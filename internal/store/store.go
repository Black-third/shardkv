// Package store implements a sharded, concurrency-safe in-memory data store
// supporting Redis-style data types: strings, lists, hashes, sets, and sorted
// sets.
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

// Sentinel errors returned by type-specific operations.
var (
	// ErrNotInteger is returned by Incr when a string value is not a base-10 int.
	ErrNotInteger = errors.New("value is not an integer")
	// ErrWrongType is returned when an operation is applied to a key holding a
	// value of the wrong data type.
	ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

type kind uint8

const (
	kindString kind = iota
	kindList
	kindHash
	kindSet
	kindZSet
)

func (k kind) String() string {
	switch k {
	case kindString:
		return "string"
	case kindList:
		return "list"
	case kindHash:
		return "hash"
	case kindSet:
		return "set"
	case kindZSet:
		return "zset"
	default:
		return "none"
	}
}

// entry is a single stored value. Exactly one of the type fields is populated
// according to kind. The list/dict/set/zset fields are reference types, so a
// handler can fetch the entry once and mutate the referenced structure in place
// without re-storing it.
type entry struct {
	kind     kind
	str      []byte
	list     *deque
	dict     map[string][]byte
	set      map[string]struct{}
	zset     *zset
	expireAt time.Time // zero means the key never expires
}

func (e entry) expired(now time.Time) bool {
	return !e.expireAt.IsZero() && !now.Before(e.expireAt)
}

type shard struct {
	mu   sync.RWMutex
	data map[string]entry
}

// Store is a sharded data store safe for concurrent use by many goroutines.
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

func copyBytes(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// --- generic key operations (any type) ---------------------------------------

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
	return !e.expired(now)
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

// Type returns the data-type name of key (string/list/hash/set/zset) or
// ("none", false) if the key is missing or expired.
func (s *Store) Type(key string) (string, bool) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	e, found := sh.data[key]
	sh.mu.RUnlock()
	if !found || e.expired(now) {
		return "none", false
	}
	return e.kind.String(), true
}

// Expire sets a new TTL on an existing live key. Reports false if the key is
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

// Keys returns all live keys. Like Len it scans every shard.
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

// --- string operations -------------------------------------------------------

// Get returns a copy of the string value at key. ok is false if the key is
// missing, expired, or holds a non-string type. Expired keys are removed
// lazily.
func (s *Store) Get(key string) (value []byte, ok bool) {
	sh := s.getShard(key)
	now := s.clock()

	sh.mu.RLock()
	e, found := sh.data[key]
	sh.mu.RUnlock()

	if !found {
		return nil, false
	}
	if e.expired(now) {
		s.dropIfExpired(sh, key)
		return nil, false
	}
	if e.kind != kindString {
		return nil, false
	}
	return copyBytes(e.str), true
}

// Set stores a string value at key, replacing any existing value of any type. A
// ttl of zero means the key never expires.
func (s *Store) Set(key string, value []byte, ttl time.Duration) {
	var expireAt time.Time
	if ttl > 0 {
		expireAt = s.clock().Add(ttl)
	}
	sh := s.getShard(key)
	sh.mu.Lock()
	sh.data[key] = entry{kind: kindString, str: copyBytes(value), expireAt: expireAt}
	sh.mu.Unlock()
}

// Incr atomically adds delta to the integer string at key and returns the
// result. A missing/expired key is treated as 0; any existing TTL is preserved.
// Returns ErrWrongType if the key holds a non-string, ErrNotInteger if the
// string is not a base-10 integer.
func (s *Store) Incr(key string, delta int64) (int64, error) {
	sh := s.getShard(key)
	now := s.clock()

	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, found := sh.data[key]
	live := found && !e.expired(now)
	if live && e.kind != kindString {
		return 0, ErrWrongType
	}

	var n int64
	expireAt := time.Time{}
	if live {
		parsed, err := strconv.ParseInt(string(e.str), 10, 64)
		if err != nil {
			return 0, ErrNotInteger
		}
		n = parsed
		expireAt = e.expireAt
	}
	n += delta
	sh.data[key] = entry{kind: kindString, str: []byte(strconv.FormatInt(n, 10)), expireAt: expireAt}
	return n, nil
}

func (s *Store) dropIfExpired(sh *shard, key string) {
	sh.mu.Lock()
	if e, ok := sh.data[key]; ok && e.expired(s.clock()) {
		delete(sh.data, key)
	}
	sh.mu.Unlock()
}

// --- background expiration ----------------------------------------------------

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
