package store

// HyperLogLog: probabilistic cardinality estimation, in Redis's own string format.
//
// # Wire compatibility
//
// A HyperLogLog here is a plain string whose bytes are byte-for-byte a Redis HLL: the
// 16-byte "HYLL" header, the same 14-bit register addressing, the same 6-bit dense
// packing, the same sparse opcodes, and the same MurmurHash64A with the same seed. A
// value written here can be read by real Redis and counted to the same number, and a
// value produced by Redis can be read here. That portability is the whole point -- an
// HLL is a *sketch*, so it is the one value a client is likely to want to move between
// servers, and a "Redis-compatible" server whose HLL strings were private would be a
// trap.
//
// The layout, exactly as in Redis:
//
//	bytes 0..3    "HYLL"
//	byte  4       encoding: 0 = dense, 1 = sparse
//	bytes 5..7    unused
//	bytes 8..15   cached cardinality, little endian; bit 7 of byte 15 set = stale
//	bytes 16..    the registers, dense or sparse
//
// # The one deliberate difference, and why it is invisible
//
// Redis patches a sparse representation in place, splitting and merging opcode runs
// around the register it is updating. This implementation decodes the sparse form to a
// register array, applies the update, and re-encodes canonically. The *format* is
// identical -- the output is a valid sparse HLL that Redis reads -- but the byte
// sequence for a given set of registers is the canonical one rather than whatever
// history of in-place patches produced it. Two servers that saw the same additions in
// different orders therefore agree byte for byte here, where Redis would agree only on
// the count. The cost is O(16384) per sparse update instead of O(sparse length), which
// is bounded: sparse is only used while the sketch is small, and it is promoted to dense
// as soon as the encoding exceeds the configured hll-sparse-max-bytes.
//
// The cached cardinality is always written *stale*. Computing it on a write would put an
// O(16384) pass on the PFADD path, and computing it on a read would make PFCOUNT modify
// the value it is reading -- which would either write on a replica or leave master and
// replica holding different bytes for the same sketch. Redis's own PFADD invalidates the
// cache the same way, so a Redis client reading our string simply recomputes, as it
// would after any Redis write.
//
// # The estimator
//
// The count is Ertl's tau/sigma estimator, which is what Redis has used since 5.0. It
// replaced the original bias-corrected harmonic mean (and its lookup table of empirical
// bias values) with a closed form that is accurate across the whole range, including the
// small- and large-cardinality ends the raw estimator was worst at -- and, unlike the
// table, it needs no magic numbers. hllTau and hllSigma are the two series it is built
// from; both converge in a handful of iterations.

import (
	"encoding/binary"
	"errors"
	"math"
	"strconv"
	"time"
)

// ErrNotHLL is returned when a key holds a string that is not a HyperLogLog.
var ErrNotHLL = errors.New("key is not a valid HyperLogLog string value")

// The HyperLogLog geometry, all matching Redis's compile-time constants.
const (
	// hllP is the number of bits of the hash used to address a register, so there are
	// 2^hllP registers. 14 gives 16384 registers and a standard error of 0.81%.
	hllP = 14
	// hllRegisters is 2^hllP.
	hllRegisters = 1 << hllP
	// hllBits is the width of one dense register. 6 bits hold 0..63, and the largest
	// value a register can take is hllQ+1 = 51.
	hllBits = 6
	// hllQ is how many hash bits are left to count leading zeros in, so a register's
	// value is at most hllQ+1.
	hllQ = 64 - hllP
	// hllRegisterMax is the mask of one dense register.
	hllRegisterMax = (1 << hllBits) - 1
	// hllHdrSize is the header, including the cached-cardinality field.
	hllHdrSize = 16
	// hllDenseSize is the length of a dense HLL string.
	hllDenseSize = hllHdrSize + (hllRegisters*hllBits+7)/8
	// hllSparseMaxBytes is how large the sparse encoding may grow before the value is
	// promoted to dense. It is Redis's hll-sparse-max-bytes default, and the value
	// PFSELFTEST checks the encodings against; a live store reads the configured one
	// (see encodingDefaults, which repeats it as HLLSparseMaxBytes).
	hllSparseMaxBytes = 3000

	hllDense  = 0
	hllSparse = 1

	// The sparse opcode limits.
	hllSparseXZeroMaxLen = 16384
	hllSparseZeroMaxLen  = 64
	hllSparseValMaxValue = 32
	hllSparseValMaxLen   = 4

	// hllAlphaInf is the alpha constant in the limit, 0.5/ln(2).
	hllAlphaInf = 0.7213475204444817
	// hllMurmurSeed is the seed Redis hashes elements with. It has to be this exact
	// value or the registers a Redis-written sketch holds would mean nothing here.
	hllMurmurSeed = 0xadc83b19
)

var hllMagic = [4]byte{'H', 'Y', 'L', 'L'}

// --- hashing ------------------------------------------------------------------

// murmurHash64A is the 64-bit MurmurHash2 variant Redis uses for HyperLogLog. It is
// reproduced rather than replaced by a standard-library hash because the register a
// given element lands in *is* the format: a different hash makes a byte-compatible
// sketch that counts a different set.
func murmurHash64A(data []byte, seed uint64) uint64 {
	const m = 0xc6a4a7935bd1e995
	const r = 47

	h := seed ^ (uint64(len(data)) * m)
	n := len(data) - len(data)%8
	for i := 0; i < n; i += 8 {
		k := binary.LittleEndian.Uint64(data[i : i+8])
		k *= m
		k ^= k >> r
		k *= m
		h ^= k
		h *= m
	}
	tail := data[n:]
	if len(tail) > 0 {
		// The tail bytes are folded in from the most significant down, exactly as the
		// switch-with-fallthrough in the C original does.
		for i := len(tail) - 1; i >= 0; i-- {
			h ^= uint64(tail[i]) << (8 * uint(i))
		}
		h *= m
	}
	h ^= h >> r
	h *= m
	h ^= h >> r
	return h
}

// hllPatLen reports which register an element belongs to and the length of the run of
// leading zeros (plus one) that register should record.
//
// Setting bit hllQ before counting is Redis's termination trick: it guarantees the loop
// ends and bounds the count at hllQ+1, so a register always fits in 6 bits.
func hllPatLen(element []byte) (index int, count uint8) {
	hash := murmurHash64A(element, hllMurmurSeed)
	index = int(hash & (hllRegisters - 1))
	hash >>= hllP
	hash |= uint64(1) << hllQ
	bit := uint64(1)
	count = 1
	for hash&bit == 0 {
		count++
		bit <<= 1
	}
	return index, count
}

// --- the string representation ------------------------------------------------

// hllInvalidateCache marks the cached cardinality stale, which is the state every write
// leaves it in. See the note at the top of the file.
func hllInvalidateCache(b []byte) {
	b[15] |= 1 << 7
}

// hllValidCache reports whether the cached cardinality can be trusted. Nothing here
// relies on it -- the count is always recomputed -- but it is what a Redis client reads,
// so the bit has to mean what Redis means by it.
func hllValidCache(b []byte) bool { return b[15]&(1<<7) == 0 }

// isHLL reports whether a string is a well-formed HyperLogLog.
//
// The checks are exactly the ones that make the rest of this file safe: the magic, a
// header, a known encoding, and -- for dense -- the exact length its geometry requires.
// A sparse value's length is not fixed, so it is validated by decoding it.
func isHLL(b []byte) bool {
	if len(b) < hllHdrSize || string(b[:4]) != string(hllMagic[:]) {
		return false
	}
	switch b[4] {
	case hllDense:
		return len(b) == hllDenseSize
	case hllSparse:
		_, ok := sparseRegisters(b[hllHdrSize:])
		return ok
	}
	return false
}

// --- dense registers ----------------------------------------------------------

// denseGet reads register i out of the packed 6-bit array.
//
// Registers are packed low-bit first within each byte, which is Redis's packing. The
// last register in the array ends exactly on the final byte boundary, so unlike the C
// macro -- which reads one byte past and relies on the result being masked away -- this
// takes the second byte only when the register genuinely spans two.
func denseGet(regs []byte, i int) uint8 {
	bit := i * hllBits
	b := bit / 8
	fb := bit % 8
	v := uint16(regs[b]) >> fb
	if fb > 8-hllBits {
		v |= uint16(regs[b+1]) << (8 - fb)
	}
	return uint8(v & hllRegisterMax)
}

// denseSet writes register i.
func denseSet(regs []byte, i int, val uint8) {
	bit := i * hllBits
	b := bit / 8
	fb := bit % 8
	regs[b] &= ^(byte(hllRegisterMax) << fb)
	regs[b] |= val << fb
	if fb > 8-hllBits {
		regs[b+1] &= ^(byte(hllRegisterMax) >> (8 - fb))
		regs[b+1] |= val >> (8 - fb)
	}
}

// denseRegisters expands a dense HLL's registers into a plain array.
func denseRegisters(regs []byte) []uint8 {
	out := make([]uint8, hllRegisters)
	for i := range out {
		out[i] = denseGet(regs, i)
	}
	return out
}

// encodeDense builds a dense HLL string from a register array.
func encodeDense(regs []uint8) []byte {
	out := make([]byte, hllDenseSize)
	copy(out, hllMagic[:])
	out[4] = hllDense
	hllInvalidateCache(out)
	body := out[hllHdrSize:]
	for i, v := range regs {
		if v != 0 {
			denseSet(body, i, v)
		}
	}
	return out
}

// --- sparse registers ---------------------------------------------------------

// xzeroOpcode encodes a run of len zero registers as an XZERO pair (1..16384).
func xzeroOpcode(runLen int) []byte {
	n := runLen - 1
	return []byte{byte(0x40 | (n >> 8)), byte(n & 0xff)}
}

// sparseRegisters decodes a sparse register body into a register array. ok is false for
// a body that is malformed or does not describe exactly hllRegisters registers, which is
// what validates a sparse value a client handed us.
func sparseRegisters(body []byte) ([]uint8, bool) {
	out := make([]uint8, 0, hllRegisters)
	for i := 0; i < len(body); {
		op := body[i]
		switch op & 0xc0 {
		case 0x00: // ZERO: 00pppppp
			runLen := int(op&0x3f) + 1
			if len(out)+runLen > hllRegisters {
				return nil, false
			}
			out = append(out, make([]uint8, runLen)...)
			i++
		case 0x40: // XZERO: 01pppppp qqqqqqqq
			if i+1 >= len(body) {
				return nil, false
			}
			runLen := (int(op&0x3f)<<8 | int(body[i+1])) + 1
			if len(out)+runLen > hllRegisters {
				return nil, false
			}
			out = append(out, make([]uint8, runLen)...)
			i += 2
		default: // VAL: 1vvvvvpp
			val := ((op >> 2) & 0x1f) + 1
			runLen := int(op&0x03) + 1
			if len(out)+runLen > hllRegisters {
				return nil, false
			}
			for j := 0; j < runLen; j++ {
				out = append(out, val)
			}
			i++
		}
	}
	if len(out) != hllRegisters {
		return nil, false
	}
	return out, true
}

// encodeSparse renders a register array in the sparse encoding, or reports that it
// cannot: a register above hllSparseValMaxValue has no sparse opcode, and an encoding
// past maxBytes (the configured hll-sparse-max-bytes) is not worth keeping sparse.
//
// The output is canonical -- the shortest run-length encoding of the register array --
// which is what makes two servers that saw the same additions in different orders hold
// identical bytes.
func encodeSparse(regs []uint8, maxBytes int64) ([]byte, bool) {
	body := make([]byte, 0, 64)
	for i := 0; i < len(regs); {
		v := regs[i]
		// The length of the run of identical values starting here.
		run := 1
		for i+run < len(regs) && regs[i+run] == v {
			run++
		}
		if v == 0 {
			for run > 0 {
				switch {
				case run > hllSparseZeroMaxLen:
					take := min(run, hllSparseXZeroMaxLen)
					body = append(body, xzeroOpcode(take)...)
					run -= take
				default:
					body = append(body, byte(run-1))
					run = 0
				}
			}
			i += 1
			// The run was consumed above; advance past all of it.
			for i < len(regs) && regs[i] == 0 {
				i++
			}
			continue
		}
		if v > hllSparseValMaxValue {
			return nil, false
		}
		for run > 0 {
			take := min(run, hllSparseValMaxLen)
			body = append(body, 0x80|((v-1)<<2)|byte(take-1))
			run -= take
		}
		// Advance past the whole run of this value.
		for i < len(regs) && regs[i] == v {
			i++
		}
	}
	if int64(hllHdrSize+len(body)) > maxBytes {
		return nil, false
	}
	out := make([]byte, hllHdrSize, hllHdrSize+len(body))
	copy(out, hllMagic[:])
	out[4] = hllSparse
	hllInvalidateCache(out)
	return append(out, body...), true
}

// encodeHLL renders a register array in whichever encoding fits: sparse while it is
// small, dense once it is not. That is the same rule Redis applies, so a sketch built
// here changes representation at the same point one built there does -- including when
// hll-sparse-max-bytes moves that point, which is how a caller asks for the dense
// encoding from the first element.
func encodeHLL(regs []uint8, sparseMaxBytes int64) []byte {
	if b, ok := encodeSparse(regs, sparseMaxBytes); ok {
		return b
	}
	return encodeDense(regs)
}

// hllRegistersOf decodes any HLL string into a register array.
func hllRegistersOf(b []byte) ([]uint8, bool) {
	if len(b) < hllHdrSize || string(b[:4]) != string(hllMagic[:]) {
		return nil, false
	}
	switch b[4] {
	case hllDense:
		if len(b) != hllDenseSize {
			return nil, false
		}
		return denseRegisters(b[hllHdrSize:]), true
	case hllSparse:
		return sparseRegisters(b[hllHdrSize:])
	}
	return nil, false
}

// --- the estimator ------------------------------------------------------------

// hllTau is the correction series for the registers at the top of the range. It is
// Ertl's tau, transcribed from Redis: an iteration on a square root that converges when
// the accumulated correction stops changing, which is a handful of steps in double
// precision.
func hllTau(x float64) float64 {
	if x == 0 || x == 1 {
		return 0
	}
	var zPrime float64
	y := 1.0
	z := 1 - x
	for {
		x = math.Sqrt(x)
		zPrime = z
		y *= 0.5
		d := 1 - x
		z -= d * d * y
		if zPrime == z {
			break
		}
	}
	return z / 3
}

// hllSigma is the correction series for the registers at the bottom of the range --
// Ertl's sigma, which is what replaces the old small-range linear counting.
func hllSigma(x float64) float64 {
	if x == 1 {
		return math.Inf(1)
	}
	var zPrime float64
	y := 1.0
	z := x
	for {
		x *= x
		zPrime = z
		z += x * y
		y += y
		if zPrime == z {
			break
		}
	}
	return z
}

// hllCount estimates the cardinality the registers describe.
//
// This is Redis's hllCount: a histogram of register values, then the tau/sigma estimator
// over it. The histogram is what makes it O(registers) with no floating-point work per
// register, and it is also why the estimator needs no bias-correction table -- the two
// series handle the ends of the range analytically.
func hllCount(regs []uint8) uint64 {
	const m = float64(hllRegisters)
	var histo [64]int
	for _, v := range regs {
		histo[v]++
	}
	z := m * hllTau((m-float64(histo[hllQ+1]))/m)
	for j := hllQ; j >= 1; j-- {
		z += float64(histo[j])
		z *= 0.5
	}
	z += m * hllSigma(float64(histo[0])/m)
	return uint64(math.Round(hllAlphaInf * m * m / z))
}

// --- store operations ---------------------------------------------------------

// PFAdd adds elements to the HyperLogLog at key and reports whether any register
// changed -- which is what PFADD returns, and what decides whether the write is
// propagated at all.
//
// A missing key is created. The command is a pure function of its arguments and the
// value it reads (the hash is deterministic and seeded by a constant), so it propagates
// verbatim: a replica applying the same PFADD sets the same registers.
func (s *Store) PFAdd(key string, elements [][]byte) (updated bool, err error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e := sh.liveEntry(key, now)
	if e != nil && e.kind != kindString {
		return false, ErrWrongType
	}
	var regs []uint8
	created := false
	if e == nil {
		regs = make([]uint8, hllRegisters)
		created = true
	} else {
		var ok bool
		regs, ok = hllRegistersOf(e.str)
		if !ok {
			return false, ErrNotHLL
		}
	}

	for _, el := range elements {
		idx, count := hllPatLen(el)
		if count > regs[idx] {
			regs[idx] = count
			updated = true
		}
	}
	if !updated && !created {
		return false, nil
	}
	// Re-encode rather than patch. See the note at the top of the file: the format is
	// Redis's, the byte sequence is the canonical encoding of the registers.
	ne := &entry{kind: kindString, str: encodeHLL(regs, s.encoding[HLLSparseMaxBytes].Load()), rawString: true}
	if e != nil {
		ne.expireAt = e.expireAt
	}
	s.touch(ne, now)
	sh.data[key] = ne
	return true, nil
}

// PFCount estimates the combined cardinality of the given keys.
//
// With one key it counts that sketch; with several it counts their union, by taking the
// per-register maximum first -- which is exact, in the sense that the union of two
// sketches is a sketch of the union of their inputs. That is the property that makes
// HyperLogLog useful for counting distinct visitors across shards.
//
// It never writes. Redis caches the computed cardinality back into the value here; doing
// that would make a read modify the dataset, which on this server would either be
// refused on a replica or leave master and replica holding different bytes for the same
// sketch. The estimate is one pass over 16384 registers, so recomputing costs a few
// microseconds.
func (s *Store) PFCount(keys []string) (int64, error) {
	unlock := s.rlockKeys(keys...)
	defer unlock()
	now := s.clock()

	var merged []uint8
	for _, k := range keys {
		sh := s.getShard(k)
		e := sh.liveEntry(k, now)
		if e == nil {
			s.keyspaceMisses.Add(1)
			continue
		}
		s.keyspaceHits.Add(1)
		if e.kind != kindString {
			return 0, ErrWrongType
		}
		regs, ok := hllRegistersOf(e.str)
		if !ok {
			return 0, ErrNotHLL
		}
		if merged == nil {
			merged = regs
			continue
		}
		for i, v := range regs {
			if v > merged[i] {
				merged[i] = v
			}
		}
	}
	if merged == nil {
		return 0, nil
	}
	return int64(hllCount(merged)), nil
}

// PFMerge merges the source sketches into dst, which is included in the union if it
// already exists. changed reports whether dst's bytes moved, so a merge that adds
// nothing propagates nothing.
//
// Like PFAdd it is deterministic -- a per-register maximum -- so it propagates verbatim.
func (s *Store) PFMerge(dst string, srcs []string) (changed bool, err error) {
	keys := make([]string, 0, len(srcs)+1)
	keys = append(keys, dst)
	keys = append(keys, srcs...)
	unlock := s.lockKeys(keys...)
	defer unlock()

	now := s.clock()
	merged := make([]uint8, hllRegisters)
	var expireAt time.Time
	dsh := s.getShard(dst)
	if e := dsh.liveEntry(dst, now); e != nil {
		if e.kind != kindString {
			return false, ErrWrongType
		}
		regs, ok := hllRegistersOf(e.str)
		if !ok {
			return false, ErrNotHLL
		}
		copy(merged, regs)
		expireAt = e.expireAt
	}
	for _, k := range srcs {
		sh := s.getShard(k)
		e := sh.liveEntry(k, now)
		if e == nil {
			continue
		}
		if e.kind != kindString {
			return false, ErrWrongType
		}
		regs, ok := hllRegistersOf(e.str)
		if !ok {
			return false, ErrNotHLL
		}
		for i, v := range regs {
			if v > merged[i] {
				merged[i] = v
			}
		}
	}
	out := encodeHLL(merged, s.encoding[HLLSparseMaxBytes].Load())
	if e := dsh.liveEntry(dst, now); e != nil && string(e.str) == string(out) {
		return false, nil
	}
	ne := &entry{kind: kindString, str: out, rawString: true, expireAt: expireAt}
	s.touch(ne, now)
	dsh.data[dst] = ne
	return true, nil
}

// PFRegisters returns the sketch's registers, for PFDEBUG GETREG.
func (s *Store) PFRegisters(key string) ([]uint8, bool, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return nil, false, nil
	}
	if e.kind != kindString {
		return nil, false, ErrWrongType
	}
	regs, ok := hllRegistersOf(e.str)
	if !ok {
		return nil, false, ErrNotHLL
	}
	return regs, true, nil
}

// PFEncoding reports whether the sketch is stored sparse or dense, for PFDEBUG.
func (s *Store) PFEncoding(key string) (string, bool, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return "", false, nil
	}
	if e.kind != kindString {
		return "", false, ErrWrongType
	}
	if !isHLL(e.str) {
		return "", false, ErrNotHLL
	}
	if e.str[4] == hllDense {
		return "dense", true, nil
	}
	return "sparse", true, nil
}

// PFToDense converts a sparse sketch to the dense encoding, for PFDEBUG TODENSE.
// changed is false when it was dense already.
func (s *Store) PFToDense(key string) (changed, ok bool, err error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e := sh.liveEntry(key, now)
	if e == nil {
		return false, false, nil
	}
	if e.kind != kindString {
		return false, false, ErrWrongType
	}
	regs, valid := hllRegistersOf(e.str)
	if !valid {
		return false, false, ErrNotHLL
	}
	if e.str[4] == hllDense {
		return false, true, nil
	}
	e.str = encodeDense(regs)
	s.touch(e, now)
	return true, true, nil
}

// HLLSelfTest checks the parts of the implementation that a wrong answer would
// otherwise hide: the dense packing round-trips every register value at every position,
// and the sparse encoding round-trips a register array. It backs PFSELFTEST.
//
// It is here rather than in a test file because PFSELFTEST is a command: the point of it
// is to be runnable against a server that is already running, which is exactly when a
// packing bug would matter and exactly when a Go test cannot help.
func HLLSelfTest() error {
	// Dense: write a distinct value into every register and read them all back. This is
	// the check that catches an off-by-one in the 6-bit packing, which would otherwise
	// only show up as a count that is quietly wrong.
	regs := make([]uint8, hllRegisters)
	for i := range regs {
		regs[i] = uint8(i%(hllQ+1)) + 1
	}
	dense := encodeDense(regs)
	if !isHLL(dense) {
		return errors.New("hll: the dense encoding is not recognized as a HyperLogLog")
	}
	got := denseRegisters(dense[hllHdrSize:])
	for i := range regs {
		if got[i] != regs[i] {
			return errors.New("hll: dense register " + strconv.Itoa(i) + " round-tripped incorrectly")
		}
	}
	// Sparse: a register array within the sparse limits must round-trip too, and the
	// encoding must be shorter than the dense one or there would be no point to it.
	sparseRegs := make([]uint8, hllRegisters)
	for i := 0; i < 100; i++ {
		sparseRegs[i*7] = uint8(i%hllSparseValMaxValue) + 1
	}
	sparse, ok := encodeSparse(sparseRegs, hllSparseMaxBytes)
	if !ok {
		return errors.New("hll: a small register array could not be encoded sparsely")
	}
	if len(sparse) >= hllDenseSize {
		return errors.New("hll: the sparse encoding is no smaller than the dense one")
	}
	decoded, ok := sparseRegisters(sparse[hllHdrSize:])
	if !ok {
		return errors.New("hll: the sparse encoding does not decode")
	}
	for i := range sparseRegs {
		if decoded[i] != sparseRegs[i] {
			return errors.New("hll: sparse register " + strconv.Itoa(i) + " round-tripped incorrectly")
		}
	}
	// And an empty sketch counts zero, which is the one answer that must be exact.
	if n := hllCount(make([]uint8, hllRegisters)); n != 0 {
		return errors.New("hll: an empty sketch counts " + strconv.Itoa(int(n)) + ", not 0")
	}
	return nil
}
