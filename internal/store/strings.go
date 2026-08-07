package store

import (
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
	charged := s.charge(sh, key)
	defer sh.mu.Unlock()
	defer s.settle(sh, key, charged)

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
	s.putString(sh, key, val, expireAt, now)
	return old, oldOK, true, nil
}

// putString stores val as key's whole string value, reusing the entry already at key when
// it is one this can overwrite in place. The caller holds sh's write lock and has taken its
// charge; the settle that follows re-measures the value either way, so the accounting does
// not care which path this took.
//
// # Why in place
//
// An entry is ~128 bytes -- six type-specific fields, a deadline and two atomic counters --
// and a SET over a key that already held a string was allocating a fresh one and dropping
// the old one on the floor. That was 128 of the 264 bytes one pipelined SET allocated, i.e.
// the largest single item on the write path and about half of everything a pipelined SET
// costs the collector. Garbage collection is paced by bytes allocated, and the tail latency
// this project loses to Redis under pipelining is GC cost (measured: with the collector off,
// the p99 of a 32-connection `-P 16` SET load was the lower of the two in 5 of 5 paired
// runs, 0.25-0.66x, over 30-38 collections per 3-second run). So bytes here are tail
// milliseconds there.
//
// Every other write in this package already mutates the entry it found -- LPUSH, HSET, ZADD
// -- so this brings the string path into line with them rather than inventing a new
// discipline. It is safe on the same terms: a reader holds the shard's shared lock and
// copies the value out before releasing it, so no reader can be looking at an entry while
// this writer holds the exclusive lock, and nothing in this package retains an *entry across
// an unlock.
//
// # Why every field is reset
//
// The reused entry must end up indistinguishable from the fresh one it replaces, or the
// optimization becomes a behaviour change nobody asked for. That includes the two counters
// the eviction sampler reads: a fresh entry starts with a zero access time and a zero LFU
// counter, so this zeroes both before touch records the access.
//
// Real Redis does the opposite with the LFU counter -- dbSetValue copies the old object's
// counter onto the new value, so an overwrite does not make a hot key look cold. That is
// arguably the better behaviour and it is deliberately *not* adopted here: changing what
// OBJECT FREQ reports is a decision about eviction, and smuggling it in as a side effect of
// an allocation change is exactly the silent drift this project's notes are about. It stays
// available as its own change, with its own test.
func (s *Store) putString(sh *shard, key string, val []byte, expireAt, now time.Time) {
	s.putOwnedString(sh, key, copyBytes(val), strWholeValue, expireAt, now)
}

// putOwnedString is putString for a value the caller has already allocated and does not
// share with anyone -- the digits Incr formatted, say -- so there is nothing to copy. origin
// is what OBJECT ENCODING will report the value as; see strOrigin.
func (s *Store) putOwnedString(sh *shard, key string, val []byte, origin strOrigin, expireAt, now time.Time) {
	if e := sh.data[key]; e != nil && e.plainString() {
		e.str = val
		e.expireAt = expireAt
		e.strOrigin = origin
		e.elemBytes = 0
		e.atime.Store(0)
		e.freq.Store(0)
		s.touch(e, now)
		return
	}
	ne := &entry{kind: kindString, str: val, strOrigin: origin, expireAt: expireAt}
	s.touch(ne, now)
	sh.data[key] = ne
}

// plainString reports whether e is a string entry carrying no other type's state, and so
// can be overwritten in place by putString.
//
// The nil checks are not redundant paranoia: kind alone would be enough if nothing ever set
// a field it does not name, but a stale map or skip list left behind by a type conversion
// would then be reachable from a value reporting itself as a string -- a value whose kind
// disagrees with its contents, which is the one failure this package must not be able to
// produce. Checking is five loads; being wrong is silent corruption.
func (e *entry) plainString() bool {
	return e.kind == kindString && e.list == nil && e.dict == nil &&
		e.set == nil && e.zset == nil && e.stream == nil
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
	charged := s.charge(sh, key)
	defer sh.mu.Unlock()
	defer s.settle(sh, key, charged)

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
	charged := s.charge(sh, key)
	defer sh.mu.Unlock()
	defer s.settle(sh, key, charged)

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
	// strOrigin and OBJECT ENCODING. That holds whether the key existed or not --
	// Redis's setrangeCommand builds the new value with createObject over a raw sds and
	// never runs tryObjectEncoding, unlike APPEND's create path.
	ne := &entry{kind: kindString, str: nv, strOrigin: strMutatedBuffer, expireAt: expireAt}
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

// IncrByFloat adds delta to the float string at key and returns the text it stored --
// which is also the text the server replies and the text it propagates, so there is one
// spelling of the new value and not three. A missing key counts as 0 and any existing TTL
// is preserved.
//
// The arithmetic is long double, not float64, because that is what the value's bytes are
// the decimal text of: see longdouble.go, which is also where the delta was parsed. The
// caller passes the parsed operand rather than a float64 for that reason -- rounding the
// operand to a float64 on the way in threw away precision the addition needed, and
// `INCRBYFLOAT k 1e-17` on a stored 1 answered "1" instead of "1.00000000000000001".
//
// ErrNotFloat reports a stored value that is not a number -- including one outside long
// double's range, which is what Redis's own parse refuses. ErrNaN reports a result that is
// not finite (adding to a huge value, or an infinite operand), which Redis refuses rather
// than storing an "inf" a later increment could never recover from.
func (s *Store) IncrByFloat(key string, delta LongDouble) (string, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	charged := s.charge(sh, key)
	defer sh.mu.Unlock()
	defer s.settle(sh, key, charged)

	e := sh.liveEntry(key, now)
	if e != nil && e.kind != kindString {
		return "", ErrWrongType
	}
	var cur LongDouble
	var expireAt time.Time
	if e != nil {
		parsed, ok := ParseLongDouble(string(e.str))
		if !ok {
			return "", ErrNotFloat
		}
		cur, expireAt = parsed, e.expireAt
	}
	sum, ok := cur.add(delta)
	if !ok {
		return "", ErrNaN
	}
	text := sum.Text()
	// Built whole, but *not* through the integer encoding: Redis's incrbyfloatCommand
	// hands the formatted text to createStringObject and stops there, where SET runs
	// tryObjectEncoding first. So an integral result reads `embstr` here and `int` after a
	// SET of the same digits -- see strOrigin, and note that returning strWholeValue
	// instead is the bug this state was added to fix.
	ne := &entry{kind: kindString, str: []byte(text), strOrigin: strPlainObject, expireAt: expireAt}
	s.touch(ne, now)
	sh.data[key] = ne
	return text, nil
}

// formatFloat renders a *score*: the text ZSCORE and a snapshot both spell it with.
// It delegates to the one formatter that decides how a double appears on the wire, so a
// score written into a Dump replays to the same text ZSCORE reports.
//
// It must not have its own implementation, and briefly did: this used
// strconv.FormatFloat(f, 'g', -1, 64), which switches to an exponent far earlier than
// Redis does. resp imports nothing from this package, so the dependency is acyclic.
func formatFloat(f float64) string { return resp.FormatDouble(f) }
