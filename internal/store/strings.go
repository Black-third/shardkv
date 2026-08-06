package store

import (
	"math"
	"strconv"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
)

// This file holds the string operations beyond the plain Get/Set pair in store.go
// and the small helpers in moreops.go: the option-driven SET, the range editors,
// and the float increment.

// SetOptions is the parsed option tail of the SET command.
//
// Deadline is only consulted when HasDeadline is set; the caller resolves a
// relative EX/PX operand into an absolute instant against Store.Now first, so the
// deadline written to memory is the same one the server propagates. With neither
// HasDeadline nor KeepTTL, SET clears any existing TTL, as Redis does.
type SetOptions struct {
	NX          bool // only set when the key does not exist
	XX          bool // only set when the key already exists
	Get         bool // report the previous string value
	KeepTTL     bool // retain the existing TTL instead of clearing it
	HasDeadline bool
	Deadline    time.Time
}

// SetWithOptions is SET with its NX/XX/GET/KEEPTTL/expiry options applied as one
// atomic step. set reports whether the value was actually written (false when NX
// found the key present or XX found it absent); old/oldOK carry the previous
// string value and are only populated when Get is set.
//
// ErrWrongType is returned -- and nothing is written -- when Get is requested on
// a key holding another data type, matching Redis: the GET half of the command
// cannot be answered, so the SET half does not happen either. Without Get, SET
// replaces a value of any type as usual.
func (s *Store) SetWithOptions(key string, val []byte, o SetOptions) (old []byte, oldOK, set bool, err error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e := sh.liveEntry(key, now)
	if e != nil && o.Get {
		if e.kind != kindString {
			return nil, false, false, ErrWrongType
		}
		old, oldOK = copyBytes(e.str), true
	}
	if (o.NX && e != nil) || (o.XX && e == nil) {
		return old, oldOK, false, nil
	}

	var expireAt time.Time
	switch {
	case o.HasDeadline:
		expireAt = o.Deadline
	case o.KeepTTL && e != nil:
		expireAt = e.expireAt
	}
	ne := &entry{kind: kindString, str: copyBytes(val), expireAt: expireAt}
	s.touch(ne, now)
	sh.data[key] = ne
	return old, oldOK, true, nil
}

// GetEx reads the string at key and optionally rewrites its expiry in the same
// locked step, which is what GETEX needs: the value it reports and the TTL it
// leaves behind must describe one state.
//
// apply says whether to touch the expiry at all (GETEX with no option is a plain
// read); with apply set, a zero deadline persists the key and any other deadline
// replaces its TTL. changed reports whether the expiry actually moved, so the
// caller propagates nothing for a no-op.
func (s *Store) GetEx(key string, deadline time.Time, apply bool) (val []byte, ok, changed bool, err error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e := sh.liveEntry(key, now)
	if e == nil {
		return nil, false, false, nil
	}
	if e.kind != kindString {
		return nil, false, false, ErrWrongType
	}
	out := copyBytes(e.str)
	if apply && !e.expireAt.Equal(deadline) {
		e.expireAt = deadline
		changed = true
	}
	s.touch(e, now)
	return out, true, changed, nil
}

// SetRange overwrites the string at key from offset with val, zero-padding any
// gap, and returns the resulting length. A missing key is treated as an empty
// string; any existing TTL is preserved.
//
// An empty val is a no-op that reports the current length (0 for a missing key)
// without creating anything, and an offset that would grow the value past the
// largest string the protocol can carry is refused rather than allocated.
func (s *Store) SetRange(key string, offset int, val []byte) (int, error) {
	if offset < 0 {
		return 0, ErrOffset
	}
	// Written as two comparisons rather than as offset+len(val) > maxStringLen, because
	// that sum overflows for an offset near the top of int64 and wraps to a negative --
	// which passed the check and then panicked slicing at the offset. A single integer
	// operand from any client was enough to take the process down.
	if offset > maxStringLen || len(val) > maxStringLen-offset {
		return 0, ErrStringTooLong
	}
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e := sh.liveEntry(key, now)
	if e != nil && e.kind != kindString {
		return 0, ErrWrongType
	}
	var base []byte
	var expireAt time.Time
	if e != nil {
		base, expireAt = e.str, e.expireAt
	}
	if len(val) == 0 {
		return len(base), nil
	}

	n := max(len(base), offset+len(val))
	nv := make([]byte, n)
	copy(nv, base)
	copy(nv[offset:], val)
	// Written into rather than stored whole, so it stays a plain buffer: see
	// entry.rawString and OBJECT ENCODING.
	ne := &entry{kind: kindString, str: nv, rawString: true, expireAt: expireAt}
	s.touch(ne, now)
	sh.data[key] = ne
	return n, nil
}

// GetRange returns the substring of the string at key covered by the inclusive
// [start, end] range, with Redis's index rules: negative indexes count from the
// end and both bounds are then clamped into the string, so an out-of-range pair
// yields an empty result rather than an error. A missing key reads as an empty
// string.
func (s *Store) GetRange(key string, start, end int) ([]byte, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return nil, nil
	}
	if e.kind != kindString {
		return nil, ErrWrongType
	}
	s.touch(e, now)

	n := len(e.str)
	if start < 0 {
		start += n
	}
	if end < 0 {
		end += n
	}
	start = max(start, 0)
	end = max(end, 0)
	if end >= n {
		end = n - 1
	}
	if start > end || n == 0 {
		return nil, nil
	}
	return copyBytes(e.str[start : end+1]), nil
}

// IncrByFloat adds delta to the float string at key and returns the result,
// stored in the shortest round-trippable form. A missing key counts as 0 and any
// existing TTL is preserved.
//
// ErrNotFloat reports a stored value that is not a number; ErrNaN reports a
// result that is not finite (adding to a huge value, or opposite infinities),
// which Redis refuses rather than storing an "inf" a later increment could never
// recover from.
func (s *Store) IncrByFloat(key string, delta float64) (float64, string, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e := sh.liveEntry(key, now)
	if e != nil && e.kind != kindString {
		return 0, "", ErrWrongType
	}
	var cur float64
	var expireAt time.Time
	if e != nil {
		parsed, ok := resp.ParseDouble(string(e.str))
		if !ok || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
			return 0, "", ErrNotFloat
		}
		cur, expireAt = parsed, e.expireAt
	}
	sum := cur + delta
	if math.IsNaN(sum) || math.IsInf(sum, 0) {
		return 0, "", ErrNaN
	}
	text := formatIncrFloat(sum)
	ne := &entry{kind: kindString, str: []byte(text), expireAt: expireAt}
	s.touch(ne, now)
	sh.data[key] = ne
	return sum, text, nil
}

// formatFloat renders a *score*: the text ZSCORE and a snapshot both spell it with.
// It delegates to the one formatter that decides how a double appears on the wire, so a
// score written into a Dump replays to the same text ZSCORE reports.
//
// It must not have its own implementation, and briefly did: this used
// strconv.FormatFloat(f, 'g', -1, 64), which switches to an exponent far earlier than
// Redis does. resp imports nothing from this package, so the dependency is acyclic.
func formatFloat(f float64) string { return resp.FormatDouble(f) }

// formatIncrFloat renders the result of INCRBYFLOAT or HINCRBYFLOAT, which Redis spells
// differently from a score -- and the difference is measurable, not theoretical.
//
// Redis formats a score with d2string, which uses an exponent outside roughly 1e-6..1e18,
// and an increment result with ld2string in its "human" mode, which never does. Measured
// against redis:7.2: ZSCORE of 1e-7 answers "1e-7", while INCRBYFLOAT reaching 2e-7
// answers and stores "0.0000002". Using one formatter for both got the increment wrong.
//
// Getting this wrong was not cosmetic. INCRBYFLOAT propagates its result as `SET key
// <text>`, so the value the master stored and the value the replica stored came from
// different formatters: `INCRBYFLOAT` replied "17179869185.5", `GET` on the master
// answered "1.71798691855e+10", and the replica held the decimal form. Master and replica
// disagreed about the bytes of a key, silently, which is what invariant 4 exists to
// prevent.
//
// 'f' with precision -1 is the shortest round-trippable fixed form: never an exponent,
// and no trailing-zero noise. A very large value renders long, which is what Redis's
// %.17Lf does too.
func formatIncrFloat(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}
