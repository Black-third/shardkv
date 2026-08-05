package store

// Bit operations on the string type.
//
// A bitmap here is not a data type of its own: it is a plain string, addressed a bit at
// a time. That is exactly what Redis does, and it is what makes the whole family
// interoperate with the string commands -- SETBIT grows a value that STRLEN measures in
// bytes, APPEND extends the bitmap, GETRANGE reads a slice of it, and SET replaces it.
// Keeping one representation is the point: a separate bitmap type would have to decide
// what STRLEN means for it, and any answer would be wrong for somebody.
//
// # Bit numbering
//
// Bit 0 is the *most* significant bit of byte 0, which is Redis's numbering and the
// only one under which a bitmap read back with GETRANGE is in the order it was written.
// So bit n lives in byte n/8 at mask 0x80>>(n%8).
//
// # The size cap
//
// The largest addressable offset is maxStringLen*8 - 1: a bit past that would grow the
// string beyond the largest bulk string the protocol can carry, so the value could
// never be read back. Redis refuses the same offsets with the same reasoning.

import (
	"errors"
	"math"
	"math/bits"
	"time"
)

// Bit-operation sentinel errors.
var (
	// ErrBitOffset is returned for a bit offset that is negative or past the largest
	// string the protocol can carry.
	ErrBitOffset = errors.New("bit offset is not an integer or out of range")
	// ErrBitValue is returned by SetBit for a value that is not 0 or 1.
	ErrBitValue = errors.New("bit is not an integer or out of range")
)

// maxBitOffset is the largest bit a command may address.
const maxBitOffset = int64(maxStringLen)*8 - 1

// stringForWrite resolves a key for a bit write, creating an empty string value when it
// is missing and preserving the TTL when it is not. It returns the entry so the caller
// can grow e.str in place.
//
// A wrong-type key is refused rather than overwritten, which is what makes SETBIT on a
// list an error instead of a silent replacement.
func (s *Store) stringForWrite(sh *shard, key string, now time.Time) (*entry, error) {
	e := sh.liveEntry(key, now)
	if e != nil {
		if e.kind != kindString {
			return nil, ErrWrongType
		}
		return e, nil
	}
	e = &entry{kind: kindString}
	sh.data[key] = e
	return e, nil
}

// growTo extends the value to at least n bytes, zero-padding.
func growString(e *entry, n int) {
	if len(e.str) >= n {
		return
	}
	grown := make([]byte, n)
	copy(grown, e.str)
	e.str = grown
}

// SetBit sets or clears the bit at offset and returns its previous value. A missing key
// is treated as an empty string and grown as needed; the TTL of an existing key is kept.
func (s *Store) SetBit(key string, offset int64, on bool) (old int, err error) {
	if offset < 0 || offset > maxBitOffset {
		return 0, ErrBitOffset
	}
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, err := s.stringForWrite(sh, key, now)
	if err != nil {
		return 0, err
	}
	byteIdx := int(offset / 8)
	mask := byte(0x80 >> (offset % 8))
	growString(e, byteIdx+1)
	if e.str[byteIdx]&mask != 0 {
		old = 1
	}
	if on {
		e.str[byteIdx] |= mask
	} else {
		e.str[byteIdx] &^= mask
	}
	s.touch(e, now)
	return old, nil
}

// GetBit returns the bit at offset, which is 0 for any offset past the end of the value
// (and for a missing key) -- a bitmap is conceptually infinite and zero-filled.
func (s *Store) GetBit(key string, offset int64) (int, error) {
	if offset < 0 || offset > maxBitOffset {
		return 0, ErrBitOffset
	}
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return 0, nil
	}
	if e.kind != kindString {
		return 0, ErrWrongType
	}
	byteIdx := int(offset / 8)
	if byteIdx >= len(e.str) {
		return 0, nil
	}
	if e.str[byteIdx]&byte(0x80>>(offset%8)) != 0 {
		return 1, nil
	}
	return 0, nil
}

// BitRange is a BITCOUNT/BITPOS range: two inclusive indexes that count either bytes or
// bits, with Redis's negative-from-the-end rule.
type BitRange struct {
	Start, End int64
	// Bits selects BIT indexing rather than BYTE. The distinction matters because a byte
	// range can only ever name whole bytes, and a client counting set bits in a field that
	// is not byte-aligned needs the bit form.
	Bits    bool
	Present bool
}

// resolveByteRange clamps a byte range into [0, n) and reports whether anything is left.
func resolveByteRange(start, end int64, n int64) (int64, int64, bool) {
	if start < 0 {
		start += n
	}
	if end < 0 {
		end += n
	}
	start = max(start, 0)
	if end >= n {
		end = n - 1
	}
	if n == 0 || start > end {
		return 0, 0, false
	}
	return start, end, true
}

// BitCount counts the set bits in the value, optionally within a range.
func (s *Store) BitCount(key string, r BitRange) (int64, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return 0, nil
	}
	if e.kind != kindString {
		return 0, ErrWrongType
	}
	str := e.str
	if !r.Present {
		return countBits(str), nil
	}
	if r.Bits {
		start, end, ok := resolveByteRange(r.Start, r.End, int64(len(str))*8)
		if !ok {
			return 0, nil
		}
		return countBitsInBitRange(str, start, end), nil
	}
	start, end, ok := resolveByteRange(r.Start, r.End, int64(len(str)))
	if !ok {
		return 0, nil
	}
	return countBits(str[start : end+1]), nil
}

func countBits(b []byte) int64 {
	var n int64
	for _, c := range b {
		n += int64(bits.OnesCount8(c))
	}
	return n
}

// countBitsInBitRange counts set bits between two bit indexes inclusive. The ends are
// masked bit by bit and the middle counted a byte at a time, so an unaligned range costs
// no more than a handful of extra operations.
func countBitsInBitRange(b []byte, startBit, endBit int64) int64 {
	var n int64
	firstWhole := (startBit + 7) / 8
	lastWhole := (endBit+1)/8 - 1
	if firstWhole > lastWhole {
		// The whole range is inside one byte (or spans no whole byte at all).
		for i := startBit; i <= endBit; i++ {
			if b[i/8]&byte(0x80>>(i%8)) != 0 {
				n++
			}
		}
		return n
	}
	for i := startBit; i < firstWhole*8; i++ {
		if b[i/8]&byte(0x80>>(i%8)) != 0 {
			n++
		}
	}
	n += countBits(b[firstWhole : lastWhole+1])
	for i := (lastWhole + 1) * 8; i <= endBit; i++ {
		if b[i/8]&byte(0x80>>(i%8)) != 0 {
			n++
		}
	}
	return n
}

// BitPos returns the position of the first bit equal to bit, or -1.
//
// The subtle case is the one Redis documents: with no explicit end, a search for a 0 bit
// that finds only 1s reports the first bit *past* the value, because a string is
// conceptually followed by infinitely many zeros. With an explicit end, the search is
// confined to the range and reports -1 instead -- the range is a statement that nothing
// outside it exists.
func (s *Store) BitPos(key string, bit int, r BitRange, hasEnd bool) (int64, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		if bit == 0 {
			return 0, nil // an empty bitmap is all zeros, so the first 0 is at 0
		}
		return -1, nil
	}
	if e.kind != kindString {
		return 0, ErrWrongType
	}
	str := e.str
	totalBits := int64(len(str)) * 8

	startBit, endBit := int64(0), totalBits-1
	if r.Present {
		unit := int64(len(str))
		if r.Bits {
			unit = totalBits
		}
		s2, e2, ok := resolveByteRange(r.Start, r.End, unit)
		if !ok {
			return -1, nil
		}
		if r.Bits {
			startBit, endBit = s2, e2
		} else {
			startBit, endBit = s2*8, e2*8+7
		}
	}
	for i := startBit; i <= endBit && i < totalBits; i++ {
		set := str[i/8]&byte(0x80>>(i%8)) != 0
		if set == (bit == 1) {
			return i, nil
		}
	}
	// Only an unbounded search for a zero bit continues past the value.
	if bit == 0 && !hasEnd {
		return totalBits, nil
	}
	return -1, nil
}

// BitOpKind names a BITOP operation.
type BitOpKind int

// The BITOP operations.
const (
	BitOpAnd BitOpKind = iota
	BitOpOr
	BitOpXor
	BitOpNot
)

// BitOp applies a bitwise operation across the source keys and stores the result in
// dst, returning the result's length in bytes. A zero-length result deletes dst, as in
// Redis.
//
// The result is as long as the longest source, with shorter sources treated as
// zero-padded -- which is the only definition under which AND, OR and XOR stay
// associative over keys of different lengths.
//
// Every key involved is locked at once, in shard-index order, so the whole operation
// sees one consistent cut of its inputs. Two BITOPs naming the same keys in opposite
// orders therefore cannot deadlock.
func (s *Store) BitOp(op BitOpKind, dst string, srcs []string) (int, error) {
	keys := make([]string, 0, len(srcs)+1)
	keys = append(keys, dst)
	keys = append(keys, srcs...)
	unlock := s.lockKeys(keys...)
	defer unlock()

	now := s.clock()
	values := make([][]byte, 0, len(srcs))
	maxLen := 0
	for _, k := range srcs {
		sh := s.getShard(k)
		e := sh.liveEntry(k, now)
		var v []byte
		if e != nil {
			if e.kind != kindString {
				return 0, ErrWrongType
			}
			v = e.str
		}
		if len(v) > maxLen {
			maxLen = len(v)
		}
		values = append(values, v)
	}
	if maxLen > maxStringLen {
		return 0, ErrStringTooLong
	}

	out := make([]byte, maxLen)
	if maxLen > 0 {
		switch op {
		case BitOpNot:
			// NOT takes exactly one source; the caller has already checked that.
			for i := range out {
				out[i] = ^byteAt(values[0], i)
			}
		default:
			copy(out, values[0])
			for _, v := range values[1:] {
				for i := range out {
					switch op {
					case BitOpAnd:
						out[i] &= byteAt(v, i)
					case BitOpOr:
						out[i] |= byteAt(v, i)
					case BitOpXor:
						out[i] ^= byteAt(v, i)
					case BitOpNot:
					}
				}
			}
		}
	}

	dsh := s.getShard(dst)
	if len(out) == 0 {
		delete(dsh.data, dst)
		return 0, nil
	}
	// The destination is replaced outright, TTL and all: BITOP computes a new value
	// rather than editing one, so carrying over the old key's expiry would attach a
	// deadline that described different data.
	ne := &entry{kind: kindString, str: out}
	s.touch(ne, now)
	dsh.data[dst] = ne
	return len(out), nil
}

// byteAt reads a byte from a value that may be shorter than the result, treating the
// missing tail as zeros.
func byteAt(v []byte, i int) byte {
	if i < len(v) {
		return v[i]
	}
	return 0
}

// --- BITFIELD -----------------------------------------------------------------

// BitFieldOverflow selects what happens when a BITFIELD INCRBY or SET exceeds the range
// of its integer type.
type BitFieldOverflow int

// The OVERFLOW behaviours.
const (
	// OverflowWrap wraps modulo the type's range, which is C's unsigned arithmetic and
	// two's-complement signed arithmetic. It is Redis's default.
	OverflowWrap BitFieldOverflow = iota
	// OverflowSat saturates at the type's minimum or maximum.
	OverflowSat
	// OverflowFail refuses the operation and reports nothing for it.
	OverflowFail
)

// BitFieldOpKind names a BITFIELD subcommand.
type BitFieldOpKind int

// The BITFIELD subcommands.
const (
	BitFieldGet BitFieldOpKind = iota
	BitFieldSet
	BitFieldIncrBy
)

// BitFieldOp is one BITFIELD operation: a typed integer field at a bit offset, and what
// to do with it.
type BitFieldOp struct {
	Kind     BitFieldOpKind
	Signed   bool
	Bits     int   // 1..64 for unsigned, 1..64 for signed (Redis caps unsigned at 63)
	Offset   int64 // in bits
	Value    int64 // the value to set, or the increment
	Overflow BitFieldOverflow
}

// BitFieldResult is one operation's outcome. Present is false for an operation that
// OVERFLOW FAIL refused, which the caller reports as a null.
type BitFieldResult struct {
	Value   int64
	Present bool
}

// BitField applies a sequence of typed-integer operations to one key under a single
// acquisition of its shard lock, so the whole sequence is atomic: a client using
// BITFIELD to implement a counter with a saturating ceiling sees no intermediate state.
//
// changed reports whether any operation modified the value, so a read-only BITFIELD (all
// GETs) propagates nothing.
func (s *Store) BitField(key string, ops []BitFieldOp) (results []BitFieldResult, changed bool, err error) {
	sh := s.getShard(key)
	now := s.clock()

	// A sequence of nothing but GETs must not create the key, so the lock taken depends
	// on what the sequence contains.
	readOnly := true
	for _, op := range ops {
		if op.Kind != BitFieldGet {
			readOnly = false
			break
		}
	}
	if readOnly {
		sh.mu.RLock()
		defer sh.mu.RUnlock()
		e := s.readEntry(sh, key, now)
		if e != nil && e.kind != kindString {
			return nil, false, ErrWrongType
		}
		var str []byte
		if e != nil {
			str = e.str
		}
		results = make([]BitFieldResult, 0, len(ops))
		for _, op := range ops {
			results = append(results, BitFieldResult{Value: getBitField(str, op), Present: true})
		}
		return results, false, nil
	}

	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, err := s.stringForWrite(sh, key, now)
	if err != nil {
		return nil, false, err
	}
	results = make([]BitFieldResult, 0, len(ops))
	for _, op := range ops {
		switch op.Kind {
		case BitFieldGet:
			results = append(results, BitFieldResult{Value: getBitField(e.str, op), Present: true})
		case BitFieldSet:
			growString(e, bitFieldBytes(op))
			old := getBitField(e.str, op)
			v, ok := clampBitField(op, op.Value)
			if !ok {
				results = append(results, BitFieldResult{})
				continue
			}
			setBitField(e.str, op, v)
			changed = true
			results = append(results, BitFieldResult{Value: old, Present: true})
		case BitFieldIncrBy:
			growString(e, bitFieldBytes(op))
			cur := getBitField(e.str, op)
			v, ok := incrBitField(op, cur)
			if !ok {
				results = append(results, BitFieldResult{})
				continue
			}
			setBitField(e.str, op, v)
			changed = true
			results = append(results, BitFieldResult{Value: v, Present: true})
		}
	}
	if changed {
		s.touch(e, now)
	} else if len(e.str) == 0 {
		// Nothing was written and the key was created by this call: leave no empty value
		// behind, so a BITFIELD that only failed is indistinguishable from one never run.
		delete(sh.data, key)
	}
	return results, changed, nil
}

// bitFieldBytes is how many bytes a field at this offset needs.
func bitFieldBytes(op BitFieldOp) int {
	return int((op.Offset + int64(op.Bits) + 7) / 8)
}

// getBitField reads the field, sign-extending a signed one. Bits past the end of the
// value read as zero, so a GET beyond the string is 0 rather than an error.
func getBitField(str []byte, op BitFieldOp) int64 {
	var v uint64
	for i := 0; i < op.Bits; i++ {
		bit := op.Offset + int64(i)
		v <<= 1
		idx := int(bit / 8)
		if idx < len(str) && str[idx]&byte(0x80>>(bit%8)) != 0 {
			v |= 1
		}
	}
	if op.Signed && op.Bits < 64 && v&(1<<(op.Bits-1)) != 0 {
		// Sign-extend: set every bit above the field's width.
		v |= ^uint64(0) << op.Bits
	}
	return int64(v)
}

// setBitField writes the field's low Bits bits.
func setBitField(str []byte, op BitFieldOp, value int64) {
	v := uint64(value)
	for i := 0; i < op.Bits; i++ {
		bit := op.Offset + int64(i)
		mask := byte(0x80 >> (bit % 8))
		if v&(1<<(op.Bits-1-i)) != 0 {
			str[bit/8] |= mask
		} else {
			str[bit/8] &^= mask
		}
	}
}

// bitFieldBounds is the inclusive range of the operation's integer type.
func bitFieldBounds(op BitFieldOp) (lo, hi int64) {
	if op.Signed {
		if op.Bits == 64 {
			return math.MinInt64, math.MaxInt64
		}
		hi = int64(1)<<(op.Bits-1) - 1
		return -hi - 1, hi
	}
	if op.Bits >= 63 {
		return 0, math.MaxInt64
	}
	return 0, int64(1)<<op.Bits - 1
}

// clampBitField applies the overflow policy to a value being SET. ok is false only for
// OVERFLOW FAIL on a value outside the type.
func clampBitField(op BitFieldOp, v int64) (int64, bool) {
	lo, hi := bitFieldBounds(op)
	if v >= lo && v <= hi {
		return v, true
	}
	switch op.Overflow {
	case OverflowFail:
		return 0, false
	case OverflowSat:
		if v < lo {
			return lo, true
		}
		return hi, true
	default:
		return wrapBitField(op, v), true
	}
}

// incrBitField adds the increment under the overflow policy. The arithmetic is done in
// uint64 so the addition itself cannot overflow undetectably; whether it *did* overflow
// the field's type is then decided by comparing against the bounds.
func incrBitField(op BitFieldOp, cur int64) (int64, bool) {
	lo, hi := bitFieldBounds(op)
	sum := int64(uint64(cur) + uint64(op.Value)) //nolint:gosec // wraparound is inspected below
	overflowed := false
	switch {
	case op.Value > 0 && sum < cur:
		overflowed = true // wrapped past the int64 ceiling
	case op.Value < 0 && sum > cur:
		overflowed = true
	case sum < lo || sum > hi:
		overflowed = true
	}
	if !overflowed {
		return sum, true
	}
	switch op.Overflow {
	case OverflowFail:
		return 0, false
	case OverflowSat:
		// Which end it saturates at is decided by the direction of the increment, not by
		// the wrapped sum -- the sum has already lost that information.
		if op.Value > 0 {
			return hi, true
		}
		return lo, true
	default:
		return wrapBitField(op, sum), true
	}
}

// wrapBitField reduces a value into the field's type modulo its range, which is what
// the hardware would do if the field were a machine word of that width.
func wrapBitField(op BitFieldOp, v int64) int64 {
	if op.Bits == 64 {
		return v
	}
	masked := uint64(v) & (uint64(1)<<op.Bits - 1)
	if op.Signed && masked&(1<<(op.Bits-1)) != 0 {
		masked |= ^uint64(0) << op.Bits
	}
	return int64(masked)
}
