// Package store implements a sharded, concurrency-safe in-memory data store
// supporting Redis-style data types: strings, lists, hashes, sets, and sorted
// sets.
//
// The keyspace is partitioned across a fixed number of shards, each guarded by
// its own sync.RWMutex. Because independent keys usually hash to different
// shards, concurrent operations rarely contend on the same lock -- this is the
// mechanism that lets the store scale across CPU cores instead of serializing
// every request behind one global lock.
//
// Entries are stored by pointer so that a key's last-access time can be recorded
// atomically on the read path (under a shared read lock) to drive approximate
// LRU eviction, without upgrading reads to exclusive locks.
package store

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Sentinel errors returned by type-specific operations.
var (
	// ErrNotInteger is returned by Incr when a string value is not a base-10 int.
	ErrNotInteger = errors.New("value is not an integer")
	// ErrWrongType is returned when an operation is applied to a key holding a
	// value of the wrong data type.
	ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	// ErrOverflow is returned by Incr when the result would exceed int64 range.
	ErrOverflow = errors.New("increment or decrement would overflow")
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
// according to kind. Entries are always referenced by pointer and never copied
// (atime is an atomic that must not be copied).
type entry struct {
	kind     kind
	str      []byte
	list     *deque
	dict     map[string][]byte
	set      map[string]struct{}
	zset     *zset
	expireAt time.Time    // zero means the key never expires
	atime    atomic.Int64 // last-access time (unix nanos) for LRU eviction
}

func (e *entry) expired(now time.Time) bool {
	return !e.expireAt.IsZero() && !now.Before(e.expireAt)
}

// touch records a key's access time for LRU eviction. It is a no-op when
// eviction is disabled, so the default (unbounded) configuration keeps the read
// path free of the extra atomic write.
func (s *Store) touch(e *entry, now time.Time) {
	if s.maxKeys.Load() > 0 {
		e.atime.Store(now.UnixNano())
	}
}

type shard struct {
	mu   sync.RWMutex
	data map[string]*entry
}

// Store is a sharded data store safe for concurrent use by many goroutines.
type Store struct {
	shards []*shard
	mask   uint64           // numShards-1; numShards is always a power of two
	clock  func() time.Time // injectable so tests can control time

	maxKeys atomic.Int64 // 0 = unbounded; else the approximate live-key cap (global)
	evicted atomic.Int64
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
		s.shards[i] = &shard{data: make(map[string]*entry)}
	}
	return s
}

// SetMaxKeys bounds the store to approximately maxKeys live keys (0 = unbounded).
// Enforcement happens on the background janitor pass (see EvictToLimit), so the
// store may transiently exceed the cap between passes.
func (s *Store) SetMaxKeys(maxKeys int) {
	if maxKeys < 0 {
		maxKeys = 0
	}
	s.maxKeys.Store(int64(maxKeys))
}

// Evicted reports how many keys have been removed by the eviction policy.
func (s *Store) Evicted() int64 { return s.evicted.Load() }

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

// EvictToLimit removes approximate-LRU keys until the store is within maxKeys.
// It is invoked by the janitor; it locks one shard at a time (never two at
// once), so it composes safely with concurrent operations.
func (s *Store) EvictToLimit() {
	maxKeys := int(s.maxKeys.Load())
	if maxKeys <= 0 {
		return
	}
	excess := s.Len() - maxKeys
	for i := 0; i < excess; i++ {
		if !s.evictOneLRU() {
			break
		}
	}
}

// evictOneLRU removes a single key: it picks a random non-empty shard, samples
// up to 16 of its keys, and evicts the one with the oldest access time. Sampling
// is Redis's approach to approximate LRU without maintaining a global ordering.
func (s *Store) evictOneLRU() bool {
	for try := 0; try < 2*len(s.shards); try++ {
		sh := s.shards[rand.Intn(len(s.shards))]
		sh.mu.Lock()
		if len(sh.data) == 0 {
			sh.mu.Unlock()
			continue
		}
		var victim string
		var oldest int64 = math.MaxInt64
		sampled := 0
		for k, e := range sh.data {
			if a := e.atime.Load(); a < oldest {
				oldest = a
				victim = k
			}
			if sampled++; sampled >= 16 {
				break
			}
		}
		delete(sh.data, victim)
		sh.mu.Unlock()
		s.evicted.Add(1)
		return true
	}
	return false
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
	defer sh.mu.RUnlock()
	e, found := sh.data[key]
	return found && !e.expired(now)
}

// Type returns the data-type name of key, or ("none", false) if missing/expired.
func (s *Store) Type(key string) (string, bool) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e, found := sh.data[key]
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
	return true
}

// TTL reports the remaining time to live for key. ok is false when the key is
// missing or expired; hasTTL is false when the key exists but never expires.
func (s *Store) TTL(key string) (remaining time.Duration, hasTTL, ok bool) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e, found := sh.data[key]
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
		sh.data = make(map[string]*entry)
		sh.mu.Unlock()
	}
}

// --- string operations -------------------------------------------------------

// Get returns a copy of the string value at key. ok is false if the key is
// missing, expired, or holds a non-string type. Expired keys are removed lazily.
func (s *Store) Get(key string) (value []byte, ok bool) {
	sh := s.getShard(key)
	now := s.clock()

	sh.mu.RLock()
	e, found := sh.data[key]
	if !found {
		sh.mu.RUnlock()
		return nil, false
	}
	if e.expired(now) {
		sh.mu.RUnlock()
		s.dropIfExpired(sh, key)
		return nil, false
	}
	if e.kind != kindString {
		sh.mu.RUnlock()
		return nil, false
	}
	out := copyBytes(e.str)
	s.touch(e, now)
	sh.mu.RUnlock()
	return out, true
}

// Set stores a string value at key, replacing any existing value of any type. A
// ttl of zero means the key never expires.
func (s *Store) Set(key string, value []byte, ttl time.Duration) {
	now := s.clock()
	var expireAt time.Time
	if ttl > 0 {
		expireAt = now.Add(ttl)
	}
	e := &entry{kind: kindString, str: copyBytes(value), expireAt: expireAt}
	s.touch(e, now)

	sh := s.getShard(key)
	sh.mu.Lock()
	sh.data[key] = e
	sh.mu.Unlock()
}

// Incr atomically adds delta to the integer string at key and returns the
// result. A missing/expired key is treated as 0; any existing TTL is preserved.
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
	// Reject signed overflow rather than silently wrapping (Redis semantics).
	if (delta > 0 && n > math.MaxInt64-delta) || (delta < 0 && n < math.MinInt64-delta) {
		return 0, ErrOverflow
	}
	n += delta
	ne := &entry{kind: kindString, str: []byte(strconv.FormatInt(n, 10)), expireAt: expireAt}
	s.touch(ne, now)
	sh.data[key] = ne
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
			s.EvictToLimit()
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
