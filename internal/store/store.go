// Package store implements a sharded, concurrency-safe in-memory key-value
// store with per-key TTL expiration.
//
// The keyspace is partitioned across a fixed number of shards, each guarded by
// its own sync.RWMutex. Because independent keys usually hash to different
// shards, concurrent operations rarely contend on the same lock -- this is the
// mechanism that lets the server scale across CPU cores instead of serializing
// every request behind one global lock.
package store

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"
)

// ErrNotInteger is returned by Incr when the stored value is not a base-10
// integer.
var ErrNotInteger = errors.New("value is not an integer")

type entry struct {
	value    []byte
	expireAt time.Time // zero means the key never expires
}

func (e entry) expired(now time.Time) bool {
	return !e.expireAt.IsZero() && !now.Before(e.expireAt)
}

type shard struct {
	mu   sync.RWMutex
	data map[string]entry
}

// Store is a sharded key-value store safe for concurrent use by many
// goroutines.
type Store struct {
	shards []*shard
	mask   uint64           // numShards-1; numShards is always a power of two
	clock  func() time.Time // injectable so tests can control time
}

// New returns a Store with at least numShards shards, rounded up to the next
// power of two so shard selection can use a cheap bitmask instead of a modulo.
func New(numShards int) *Store {
	n := nextPow2(numShards)
	s := &Store{
		shards: make([]*shard, n),
		mask:   uint64(n - 1),
		clock:  time.Now,
	}
	for i := range s.shards {
		s.shards[i] = &shard{data: make(map[string]entry)}
	}
	return s
}

func nextPow2(n int) int {
	if n < 1 {
		return 1
	}
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

// fnv1a is an inlined, allocation-free FNV-1a 64-bit hash. Avoiding
// hash/fnv.New64a keeps shard selection off the heap on the hot path.
func fnv1a(key string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= prime64
	}
	return h
}

func (s *Store) getShard(key string) *shard {
	return s.shards[fnv1a(key)&s.mask]
}

// Get returns a copy of the value stored at key. ok is false if the key is
// missing or has expired. Expired keys encountered here are removed lazily.
func (s *Store) Get(key string) (value []byte, ok bool) {
	sh := s.getShard(key)
	now := s.clock()

	sh.mu.RLock()
	e, found := sh.data[key]
	sh.mu.RUnlock()

	if !found {
		return nil, false
	}
	if !e.expired(now) {
		// Return a copy so callers cannot mutate the stored bytes.
		out := make([]byte, len(e.value))
		copy(out, e.value)
		return out, true
	}

	// Expired: delete under the write lock, re-checking in case another
	// goroutine refreshed the key in the meantime.
	sh.mu.Lock()
	if e2, ok := sh.data[key]; ok && e2.expired(s.clock()) {
		delete(sh.data, key)
	}
	sh.mu.Unlock()
	return nil, false
}

// Set stores value at key. A ttl of zero means the key never expires. The
// value is copied so the caller's buffer can be reused safely.
func (s *Store) Set(key string, value []byte, ttl time.Duration) {
	var expireAt time.Time
	if ttl > 0 {
		expireAt = s.clock().Add(ttl)
	}
	v := make([]byte, len(value))
	copy(v, value)

	sh := s.getShard(key)
	sh.mu.Lock()
	sh.data[key] = entry{value: v, expireAt: expireAt}
	sh.mu.Unlock()
}

// Del removes key and reports whether a live key was actually removed.
func (s *Store) Del(key string) bool {
	sh := s.getShard(key)
	now := s.clock()

	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, found := sh.data[key]
	if !found {
		return false
	}
	delete(sh.data, key)
	return !e.expired(now) // report true only if it had not already expired
}

// Exists reports whether key is present and unexpired.
func (s *Store) Exists(key string) bool {
	sh := s.getShard(key)
	now := s.clock()

	sh.mu.RLock()
	e, found := sh.data[key]
	sh.mu.RUnlock()
	return found && !e.expired(now)
}

// Incr atomically adds delta to the integer value at key and returns the
// result. A missing or expired key is treated as 0. Any existing TTL is
// preserved. Returns ErrNotInteger if the current value is not a base-10 int.
func (s *Store) Incr(key string, delta int64) (int64, error) {
	sh := s.getShard(key)
	now := s.clock()

	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, found := sh.data[key]
	live := found && !e.expired(now)

	var n int64
	if live {
		parsed, err := strconv.ParseInt(string(e.value), 10, 64)
		if err != nil {
			return 0, ErrNotInteger
		}
		n = parsed
	}
	n += delta

	expireAt := time.Time{}
	if live {
		expireAt = e.expireAt // keep the original deadline
	}
	sh.data[key] = entry{value: []byte(strconv.FormatInt(n, 10)), expireAt: expireAt}
	return n, nil
}

// Expire sets a new TTL on an existing live key. It reports false if the key is
// missing or already expired.
func (s *Store) Expire(key string, ttl time.Duration) bool {
	sh := s.getShard(key)
	now := s.clock()

	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, found := sh.data[key]
	if !found || e.expired(now) {
		return false
	}
	e.expireAt = now.Add(ttl)
	sh.data[key] = e
	return true
}

// TTL reports the remaining time to live for key. ok is false when the key is
// missing or expired; hasTTL is false when the key exists but never expires.
func (s *Store) TTL(key string) (remaining time.Duration, hasTTL, ok bool) {
	sh := s.getShard(key)
	now := s.clock()

	sh.mu.RLock()
	e, found := sh.data[key]
	sh.mu.RUnlock()

	if !found || e.expired(now) {
		return 0, false, false
	}
	if e.expireAt.IsZero() {
		return 0, false, true
	}
	return e.expireAt.Sub(now), true, true
}

// Len returns the number of live keys. It scans every shard, so it is O(n) and
// intended for diagnostics (DBSIZE), not the hot path.
func (s *Store) Len() int {
	now := s.clock()
	total := 0
	for _, sh := range s.shards {
		sh.mu.RLock()
		for _, e := range sh.data {
			if !e.expired(now) {
				total++
			}
		}
		sh.mu.RUnlock()
	}
	return total
}

// Keys returns all live keys. Like Len it scans every shard and is meant for
// diagnostics rather than the hot path.
func (s *Store) Keys() []string {
	now := s.clock()
	var keys []string
	for _, sh := range s.shards {
		sh.mu.RLock()
		for k, e := range sh.data {
			if !e.expired(now) {
				keys = append(keys, k)
			}
		}
		sh.mu.RUnlock()
	}
	return keys
}

// FlushAll removes every key from every shard.
func (s *Store) FlushAll() {
	for _, sh := range s.shards {
		sh.mu.Lock()
		sh.data = make(map[string]entry)
		sh.mu.Unlock()
	}
}

// Janitor periodically reclaims memory held by expired keys until ctx is
// canceled. Lazy expiration on read already keeps results correct; the janitor
// exists so keys that are written-then-never-read don't leak memory.
func (s *Store) Janitor(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.sweep()
		}
	}
}

func (s *Store) sweep() {
	now := s.clock()
	for _, sh := range s.shards {
		sh.mu.Lock()
		for k, e := range sh.data {
			if e.expired(now) {
				delete(sh.data, k)
			}
		}
		sh.mu.Unlock()
	}
}
