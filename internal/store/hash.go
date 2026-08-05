package store

import (
	"math"
	"math/rand"
	"strconv"
	"time"
)

// This file extends the hash type whose HSet/HGet/HDel core lives in types.go
// with the field-wise increments, projections, and the random pick.

// HKeys returns every field name in the hash (nil if the key is missing).
func (s *Store) HKeys(key string) ([]string, error) {
	return hashProject(s, key, func(f string, v []byte) string { return f })
}

// HVals returns every value in the hash (nil if the key is missing).
func (s *Store) HVals(key string) ([][]byte, error) {
	return hashProject(s, key, func(f string, v []byte) []byte { return copyBytes(v) })
}

// hashProject collects one projection of every field/value pair under a single
// acquisition of the shard lock. HKEYS and HVALS differ only in what they keep,
// and neither should copy the other half of the pair.
func hashProject[T any](s *Store, key string, pick func(string, []byte) T) ([]T, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return nil, nil
	}
	if e.kind != kindHash {
		return nil, ErrWrongType
	}
	s.touch(e, now)
	out := make([]T, 0, len(e.dict))
	for f, v := range e.dict {
		out = append(out, pick(f, v))
	}
	return out, nil
}

// HExists reports whether field is present in the hash at key.
func (s *Store) HExists(key, field string) (bool, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return false, nil
	}
	if e.kind != kindHash {
		return false, ErrWrongType
	}
	_, ok := e.dict[field]
	return ok, nil
}

// HMGet returns the values of the given fields in order, with a nil entry for
// every absent field (and for every field of a missing key), so the reply always
// has one element per requested field.
func (s *Store) HMGet(key string, fields ...string) ([][]byte, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	out := make([][]byte, len(fields))
	e := s.readEntry(sh, key, now)
	if e == nil {
		return out, nil
	}
	if e.kind != kindHash {
		return nil, ErrWrongType
	}
	s.touch(e, now)
	for i, f := range fields {
		if v, ok := e.dict[f]; ok {
			out[i] = copyBytes(v)
		}
	}
	return out, nil
}

// HStrLen returns the length of the value at field, or 0 when either the key or
// the field is absent -- the same reply, as in Redis, since neither holds bytes.
func (s *Store) HStrLen(key, field string) (int, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return 0, nil
	}
	if e.kind != kindHash {
		return 0, ErrWrongType
	}
	return len(e.dict[field]), nil
}

// HSetNX sets field=val only when the field does not already exist, creating the
// hash if needed. It reports whether the field was written.
func (s *Store) HSetNX(key, field string, val []byte) (bool, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e := sh.liveEntry(key, now)
	if e != nil && e.kind != kindHash {
		return false, ErrWrongType
	}
	if e == nil {
		e = &entry{kind: kindHash, dict: make(map[string][]byte)}
		sh.data[key] = e
	}
	if _, exists := e.dict[field]; exists {
		return false, nil
	}
	e.dict[field] = copyBytes(val)
	s.touch(e, now)
	return true, nil
}

// HIncrBy adds delta to the integer at field and returns the result, creating the
// hash and the field (from 0) as needed. ErrHashNotInteger reports a field that
// does not hold a base-10 integer and ErrOverflow a result outside int64, which
// Redis refuses rather than wrapping.
func (s *Store) HIncrBy(key, field string, delta int64) (int64, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, err := s.hashForWrite(sh, key, now)
	if err != nil {
		return 0, err
	}
	var n int64
	if cur, ok := e.dict[field]; ok {
		parsed, perr := strconv.ParseInt(string(cur), 10, 64)
		if perr != nil {
			return 0, ErrHashNotInteger
		}
		n = parsed
	}
	if (delta > 0 && n > math.MaxInt64-delta) || (delta < 0 && n < math.MinInt64-delta) {
		return 0, ErrOverflow
	}
	n += delta
	e.dict[field] = []byte(strconv.FormatInt(n, 10))
	s.touch(e, now)
	return n, nil
}

// HIncrByFloat adds delta to the float at field and returns the result, stored in
// the shortest round-trippable form. ErrHashNotFloat reports a field that is not a
// number and ErrNaN a result that is not finite.
func (s *Store) HIncrByFloat(key, field string, delta float64) (float64, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, err := s.hashForWrite(sh, key, now)
	if err != nil {
		return 0, err
	}
	var cur float64
	if have, ok := e.dict[field]; ok {
		parsed, perr := strconv.ParseFloat(string(have), 64)
		if perr != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
			return 0, ErrHashNotFloat
		}
		cur = parsed
	}
	sum := cur + delta
	if math.IsNaN(sum) || math.IsInf(sum, 0) {
		return 0, ErrNaN
	}
	e.dict[field] = []byte(formatFloat(sum))
	s.touch(e, now)
	return sum, nil
}

// hashForWrite returns the hash at key in an already-locked shard, creating it if
// absent. The caller holds sh's write lock.
func (s *Store) hashForWrite(sh *shard, key string, now time.Time) (*entry, error) {
	e := sh.liveEntry(key, now)
	if e != nil {
		if e.kind != kindHash {
			return nil, ErrWrongType
		}
		return e, nil
	}
	e = &entry{kind: kindHash, dict: make(map[string][]byte)}
	sh.data[key] = e
	return e, nil
}

// HRandField returns count field names from the hash at key, with their values
// when withValues is set (flattened as field, value, ...).
//
// A negative count allows the same field to be returned more than once and always
// yields exactly -count names; a positive count returns distinct names and is
// capped at the hash size. The pick is random, so the caller must never propagate
// the command itself: a replica would draw different fields.
func (s *Store) HRandField(key string, count int, withValues bool) ([][]byte, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return nil, nil
	}
	if e.kind != kindHash {
		return nil, ErrWrongType
	}
	s.touch(e, now)

	fields := make([]string, 0, len(e.dict))
	for f := range e.dict {
		fields = append(fields, f)
	}
	picked := pickMembers(fields, count)
	out := make([][]byte, 0, len(picked)*2)
	for _, f := range picked {
		out = append(out, []byte(f))
		if withValues {
			out = append(out, copyBytes(e.dict[f]))
		}
	}
	return out, nil
}

// pickMembers draws count elements from pool: with repetition when count is
// negative (exactly -count of them), without repetition when it is positive
// (at most len(pool), the whole pool in random order once count reaches it).
// It never mutates pool's backing array beyond the copy it makes.
func pickMembers(pool []string, count int) []string {
	if len(pool) == 0 || count == 0 {
		return nil
	}
	if count < 0 {
		out := make([]string, 0, -count)
		for i := 0; i < -count; i++ {
			out = append(out, pool[rand.Intn(len(pool))])
		}
		return out
	}
	shuffled := make([]string, len(pool))
	copy(shuffled, pool)
	rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	return shuffled[:min(count, len(shuffled))]
}
