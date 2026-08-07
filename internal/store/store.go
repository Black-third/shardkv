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
	"slices"
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
	// ErrNotFloat is returned by IncrByFloat when a string value is not a float.
	ErrNotFloat = errors.New("value is not a valid float")
	// ErrHashNotInteger is returned by HIncrBy when a field is not a base-10 int.
	ErrHashNotInteger = errors.New("hash value is not an integer")
	// ErrHashNotFloat is returned by HIncrByFloat when a field is not a float.
	ErrHashNotFloat = errors.New("hash value is not a float")
	// ErrNaN is returned by the float increments when the result is not finite.
	ErrNaN = errors.New("increment would produce NaN or Infinity")
	// ErrScoreNaN is returned by ZIncrBy when the resulting score is NaN (the only
	// way to reach it is adding opposite infinities).
	ErrScoreNaN = errors.New("resulting score is not a number (NaN)")
	// ErrOffset is returned by SetRange for a negative offset.
	ErrOffset = errors.New("offset is out of range")
	// ErrStringTooLong is returned by SetRange when the result would exceed the
	// largest string the protocol can carry.
	ErrStringTooLong = errors.New("string exceeds maximum allowed size (proto-max-bulk-len)")
	// ErrNoSuchKey is returned by LSet when the key does not exist.
	ErrNoSuchKey = errors.New("no such key")
	// ErrIndexOutOfRange is returned by LSet for an index past the list bounds.
	ErrIndexOutOfRange = errors.New("index out of range")
	// ErrSameObject is returned by Copy when source and destination are one key.
	ErrSameObject = errors.New("source and destination objects are the same")
	// ErrNotString is returned by LCS when either key holds something other than a
	// string. It is separate from ErrWrongType because Redis words this one in terms of
	// the values rather than the operation: LCS needs two strings and does not report
	// which of the two was not one.
	ErrNotString = errors.New("The specified keys must contain string values")
)

// maxStringLen bounds a value SetRange may grow a string to. It matches Redis's
// proto-max-bulk-len default, which is also the largest bulk string the protocol
// can carry, so a string that could never be read back is refused up front.
const maxStringLen = 512 << 20

type kind uint8

const (
	kindString kind = iota
	kindList
	kindHash
	kindSet
	kindZSet
	kindStream
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
	case kindStream:
		return "stream"
	default:
		return "none"
	}
}

// strOrigin says how a string value came to be, which is the only thing that decides
// the name OBJECT ENCODING gives it beyond its length. It exists so the encoding names
// the *representation* rather than being re-derived from the content, because Redis's
// representation depends on which constructor ran and not on what the bytes now read as.
//
// The three states are exhaustive: every string entry was either stored whole by a
// command that offers the integer encoding, built whole by one that does not, or
// produced by editing a buffer in place. They are named for that distinction rather
// than for the commands, because the commands are only evidence of it.
//
// Getting this wrong is not cosmetic: `assert_encoding` runs throughout Redis's own test
// suite, and memory-analysis tools read the field to decide how a value is stored.
type strOrigin uint8

const (
	// strWholeValue: stored whole by a command that runs Redis's tryObjectEncoding --
	// SET and its variants, MSET, GETSET, INCR/INCRBY/DECR, RESTORE, and APPEND when it
	// *creates* the key. The integer encoding is attempted first, so `SET k 1` reads
	// `int`; otherwise the length decides embstr from raw.
	strWholeValue strOrigin = iota

	// strPlainObject: built whole as a plain string object by a command that never
	// attempts the integer encoding. INCRBYFLOAT is the case: Redis's
	// incrbyfloatCommand calls createStringObject, so a result of "2" reads `embstr`
	// where `SET k 2` reads `int`. The length still decides embstr from raw.
	strPlainObject

	// strMutatedBuffer: produced by appending to or writing into an existing buffer --
	// APPEND onto a key that already existed, SETRANGE, SETBIT, BITFIELD, BITOP's
	// destination, and the HyperLogLog commands. Redis unshares the value into a plain
	// sds and never re-encodes it, so `SET k 1` then `APPEND k 2` reads "12" and is
	// still `raw`. Always raw, whatever the length.
	strMutatedBuffer
)

// entry is a single stored value. Exactly one of the type fields is populated
// according to kind. Entries are always referenced by pointer and never copied
// (atime is an atomic that must not be copied).
type entry struct {
	kind kind
	str  []byte
	// strOrigin is meaningful only for kindString; see the type. The zero value is
	// strWholeValue, which is what a plain SET produces, so a construction site that
	// stores a whole value needs to say nothing.
	strOrigin strOrigin
	list      *deque
	dict      map[string][]byte
	set       map[string]struct{}
	zset      *zset
	stream    *stream
	expireAt  time.Time    // zero means the key never expires
	atime     atomic.Int64 // last-access time (unix nanos) for LRU eviction

	// elemBytes is the maintained payload size of a hash or a set -- both bare Go maps
	// with no wrapper that could carry the count -- and the last measured payload size
	// of a stream. The list and sorted-set types keep their own (deque.bytes,
	// zset.bytes) and a string's is its buffer's capacity. It is guarded by the shard's
	// write lock, like the value it describes. See memtrack.go.
	elemBytes int64

	// freq is the LFU access counter and the minute it was last decayed, packed as
	// Redis packs them. Atomic for the same reason atime is: it is written from the read
	// path, which holds only the shard's shared lock. See evict.go.
	freq atomic.Uint32
}

func (e *entry) expired(now time.Time) bool {
	return !e.expireAt.IsZero() && !now.Before(e.expireAt)
}

// touch records what the eviction sampler needs to rank this key: an access instant
// under an LRU policy, or a decaying access counter under an LFU one.
//
// It is one atomic load and a branch when nothing is configured to evict, which is the
// default: no cap, no byte budget, so nothing would ever read the value and the read path
// does not pay to write it. That gate is refreshTracking's job, so that "which of the
// three settings is in force" is decided when one of them changes rather than on every
// read.
func (s *Store) touch(e *entry, now time.Time) {
	switch accessTracking(s.access.Load()) {
	case trackAccessTime:
		e.atime.Store(now.UnixNano())
	case trackFrequency:
		e.freq.Store(lfuAccess(e.freq.Load(), lfuMinute(now),
			s.lfuDecayTime.Load(), s.lfuLogFactor.Load()))
	}
}

type shard struct {
	mu   sync.RWMutex
	data map[string]*entry

	// mem is the sum of entryCharge over this shard's entries, guarded by mu. It is a
	// plain integer because every path that changes it already holds the write lock, and
	// it exists so one shard can be reconciled against the store's total while the other
	// shards keep taking writes. See memtrack.go.
	mem int64

	// volatile is how many of this shard's entries carry a deadline, maintained by the
	// same settle that maintains mem. It exists for the eviction sampler: a volatile-*
	// policy may only evict a key with a TTL, and without a count it could not tell "this
	// shard has no candidates" from "I did not sample far enough" -- so it would refuse
	// writes while candidates existed, or scan hopeless shards forever looking for them.
	// See shard.pickVictim.
	volatile int
}

// Store is a sharded data store safe for concurrent use by many goroutines.
type Store struct {
	shards []*shard
	mask   uint64           // numShards-1; numShards is always a power of two
	clock  func() time.Time // injectable so tests can control time

	maxKeys atomic.Int64 // 0 = unbounded; else the approximate live-key cap (global)
	evicted atomic.Int64

	// memTrack gates the whole byte accounting. While it is false, every mutation pays one
	// atomic load and nothing else -- no map lookup, no arithmetic, no counter -- which is
	// what keeps the default configuration (no byte budget, nobody asking) exactly as fast
	// as it was before any of this existed. It is raised by SetMaxMemory or by the first
	// reader of UsedMemory, either of which also derives the totals from the dataset so the
	// counter never starts from a partial history. See Store.TrackMemory.
	memTrack atomic.Bool

	// mem is the running byte total behind used_memory and the maxmemory budget: the sum
	// of every shard's mem. It is a single atomic rather than 256 integers summed on
	// demand because of which side is hot -- the total is read before every write once a
	// budget is configured, where summing the shards would be 256 loads per command,
	// against one uncontended atomic add per mutation on a path that has already taken a
	// mutex. See memtrack.go for what it counts.
	mem atomic.Int64

	// The eviction configuration. maxMemory is the server-wide byte budget as this
	// database was told it (see SetMaxMemory for why every database holds the same copy),
	// policy is Redis's maxmemory-policy and policySet whether anything chose it
	// explicitly. evictSamples is Redis's maxmemory-samples, and lfuLogFactor /
	// lfuDecayTime its lfu-log-factor / lfu-decay-time. access caches what the read path
	// must record under all of that, so touch decides with one atomic load. See evict.go.
	maxMemory    atomic.Int64
	policy       atomic.Int32
	policySet    atomic.Bool
	evictSamples atomic.Int64
	lfuLogFactor atomic.Int64
	lfuDecayTime atomic.Int64
	access       atomic.Int32

	// activeExpire gates the janitor's expiry sweep. It defaults to on; see
	// SetActiveExpire for why anything would turn it off.
	activeExpire atomic.Bool

	// encoding holds the representation thresholds OBJECT ENCODING reports from and
	// the HyperLogLog's sparse/dense switchover. See encoding.go.
	encoding encodingLimits

	// keyspaceHits and keyspaceMisses count how many key lookups made by *read* paths
	// found a live value and how many did not. They back INFO's keyspace_hits and
	// keyspace_misses, i.e. the cache hit rate an operator sizes the dataset from.
	//
	// They are maintained here, next to the lookup, rather than in the server: the
	// answer is decided by the map probe, and a counter kept anywhere else would have
	// to guess it back from the reply. See readEntry for why they are not in liveEntry.
	keyspaceHits   atomic.Int64
	keyspaceMisses atomic.Int64

	// onRemoved, if set, is called after a key is removed by the store itself
	// rather than by a client command: evicted is true for an LRU eviction and
	// false for a TTL expiration. It is always invoked without any shard lock
	// held, so the callback may take other locks. The server uses it to
	// invalidate WATCHers (both cases) and to propagate a DEL for evictions
	// (expired keys converge via their absolute deadlines). Set before serving.
	onRemoved func(key string, evicted bool)
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
	s.activeExpire.Store(true)
	s.encoding.reset()
	s.evictSamples.Store(EvictionSamples)
	s.lfuLogFactor.Store(defaultLFULogFactor)
	s.lfuDecayTime.Store(defaultLFUDecayTime)
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
	s.refreshTracking()
}

// MaxKeys reports the configured eviction cap (0 = unbounded), for CONFIG GET.
func (s *Store) MaxKeys() int { return int(s.maxKeys.Load()) }

// SetRemovalHook registers a callback invoked when the store removes a key on
// its own (expiration or eviction). It must be set before serving starts.
func (s *Store) SetRemovalHook(fn func(key string, evicted bool)) { s.onRemoved = fn }

// SetClock overrides the store's time source, which defaults to time.Now. It
// exists so tests can drive TTL logic deterministically instead of with sleeps,
// and must be set before serving starts.
func (s *Store) SetClock(fn func() time.Time) { s.clock = fn }

// Now reports the store's current time. Callers outside the store read time
// through it rather than calling time.Now directly, so that under an injected
// clock the deadline a command writes to memory and the deadline it propagates to
// the AOF and replicas come from the same clock.
func (s *Store) Now() time.Time { return s.clock() }

func (s *Store) notifyRemoved(key string, evicted bool) {
	if s.onRemoved != nil {
		s.onRemoved(key, evicted)
	}
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

// lockKeys write-locks every distinct shard the given keys hash to and returns
// the matching unlock function.
//
// The shards are locked in ascending index order, which is what makes a
// multi-key write safe: two commands naming the same keys in opposite order
// (COPY a b and COPY b a) would deadlock if each locked in argument order, and
// no total order over the keys themselves survives duplicates or aliasing. It is
// the same discipline Rename applies by hand for its two shards.
func (s *Store) lockKeys(keys ...string) func() {
	idx := s.shardIndexes(keys)
	for _, i := range idx {
		s.shards[i].mu.Lock()
	}
	return func() {
		for _, i := range idx {
			s.shards[i].mu.Unlock()
		}
	}
}

// rlockKeys is lockKeys for a multi-key read (SINTER and friends): it takes the
// shared lock on each distinct shard, in the same ascending order, so the whole
// computation sees one consistent cut of every input.
func (s *Store) rlockKeys(keys ...string) func() {
	idx := s.shardIndexes(keys)
	for _, i := range idx {
		s.shards[i].mu.RLock()
	}
	return func() {
		for _, i := range idx {
			s.shards[i].mu.RUnlock()
		}
	}
}

// shardIndexes returns the distinct shard indexes the keys hash to, ascending.
func (s *Store) shardIndexes(keys []string) []int {
	idx := make([]int, 0, len(keys))
	for _, k := range keys {
		i := int(fnv1a(k) & s.mask)
		if !slices.Contains(idx, i) {
			idx = append(idx, i)
		}
	}
	slices.Sort(idx)
	return idx
}

// liveEntry returns the unexpired entry for key held in an already-locked shard,
// or nil. It is the shared preamble of the multi-key operations, which cannot
// call the single-key helpers without taking a shard lock they already hold.
func (sh *shard) liveEntry(key string, now time.Time) *entry {
	e, ok := sh.data[key]
	if !ok || e.expired(now) {
		return nil
	}
	return e
}

// readEntry is liveEntry for a *read*: it looks the key up in an already-locked
// shard and records the outcome as a keyspace hit or miss.
//
// It is deliberately a second helper rather than accounting inside liveEntry. Every
// mutating operation in this package resolves its key through liveEntry too, so a
// counter there would report an LPUSH that created a list as a cache miss and an
// HSET that overwrote a field as a cache hit -- which would make keyspace_hits mean
// "key lookups" instead of "reads that were served from memory", and quietly turn
// the hit *rate* into a number no operator could act on. The alternative -- counting
// in each read handler in the server -- would put the decision one layer away from
// the map probe that actually makes it, and would be silently incomplete for every
// read command added afterwards.
//
// Every method that calls it holds only the shard's read lock, which is what makes
// "this is a read" a structural property rather than a convention: a write cannot
// reach this function without first taking the exclusive lock the read methods do
// not take.
//
// The two counters are atomics, so accounting costs one uncontended atomic add on an
// already-locked path and allocates nothing.
func (s *Store) readEntry(sh *shard, key string, now time.Time) *entry {
	e := sh.liveEntry(key, now)
	if e == nil {
		s.keyspaceMisses.Add(1)
		return nil
	}
	s.keyspaceHits.Add(1)
	return e
}

// KeyspaceHits and KeyspaceMisses report the read-lookup counters INFO publishes.
func (s *Store) KeyspaceHits() int64   { return s.keyspaceHits.Load() }
func (s *Store) KeyspaceMisses() int64 { return s.keyspaceMisses.Load() }

// ResetKeyspaceStats zeroes the hit/miss counters, for CONFIG RESETSTAT.
func (s *Store) ResetKeyspaceStats() {
	s.keyspaceHits.Store(0)
	s.keyspaceMisses.Store(0)
}

func copyBytes(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// EvictionSamples is the default sample size: how many keys the eviction sampler looks
// at in the shard it picked before choosing the best victim among them. It is Redis's
// maxmemory-samples, the same "approximated LRU" trade-off where a larger sample is a
// better choice for more work, and it is settable at runtime -- see
// Store.SetEvictionSamples, and note that CONFIG GET must report what the sampler
// actually uses rather than a constant.
//
// The default is 16 rather than Redis's 5 because a sample here is drawn from one shard
// rather than from one global table, so it sees 1/256th of the keyspace and a wider
// sample buys back some of that narrowness.
const EvictionSamples = 16

// EvictToLimit removes keys until the store is within maxKeys. It is invoked by the
// janitor; it locks one shard at a time (never two at once), so it composes safely with
// concurrent operations.
//
// The excess is measured with approxLen rather than Len: the janitor calls this
// on every tick, and an O(live keys) scan per tick is a real cost on a large
// store, whereas the cap is documented as approximate anyway.
//
// The key cap is a separate mechanism from the byte budget, and it evicts under its own
// terms: it uses the configured policy's victim selection when that policy evicts at all,
// and approximate LRU when the policy is noeviction. A cap is an instruction to bound the
// keyspace, so answering it with OOM errors -- which is what noeviction means for
// maxmemory -- would silently retire a documented feature.
func (s *Store) EvictToLimit() {
	maxKeys := int(s.maxKeys.Load())
	if maxKeys <= 0 {
		return
	}
	policy := s.EvictionPolicy()
	if !policy.Evicts() {
		policy = PolicyAllKeysLRU
	}
	excess := s.approxLen() - maxKeys
	for i := 0; i < excess; i++ {
		if !s.EvictOne(policy) {
			break
		}
	}
}

// approxLen returns the number of stored entries, counting keys whose TTL has
// elapsed but that no read or sweep has reclaimed yet. It reads each shard's map
// length instead of examining every entry, so it costs O(shards) rather than
// O(keys). The janitor sweeps expired keys immediately before evicting, so at
// that point the approximation equals the live count.
func (s *Store) approxLen() int {
	total := 0
	for _, sh := range s.shards {
		sh.mu.RLock()
		total += len(sh.data)
		sh.mu.RUnlock()
	}
	return total
}

// --- generic key operations (any type) ---------------------------------------

// Del removes key and reports whether a live key was actually removed.
//
// Deleting a key whose TTL had already elapsed reports false but still fires the
// removal hook: the entry disappears here, so no later read or sweep can report
// the expiration, and a WATCHer of that key would otherwise never learn it
// expired.
func (s *Store) Del(key string) bool {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	e, found := sh.data[key]
	if found {
		delete(sh.data, key)
		s.uncharge(sh, key, e)
	}
	sh.mu.Unlock()

	if !found {
		return false
	}
	if e.expired(now) {
		s.notifyRemoved(key, false) // expiration, observed by DEL
		return false
	}
	return true
}

// Exists reports whether key is present and unexpired.
func (s *Store) Exists(key string) bool {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	return s.readEntry(sh, key, now) != nil
}

// Type returns the data-type name of key, or ("none", false) if missing/expired.
func (s *Store) Type(key string) (string, bool) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e := s.readEntry(sh, key, now)
	if e == nil {
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
//
// A time.Duration spans only ~292 years, so a deadline further out than that
// saturates. Callers that report a TTL to a client should use TTLMillis, which
// cannot saturate.
func (s *Store) TTL(key string) (remaining time.Duration, hasTTL, ok bool) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e := s.readEntry(sh, key, now)
	if e == nil {
		return 0, false, false
	}
	if e.expireAt.IsZero() {
		return 0, false, true
	}
	return e.expireAt.Sub(now), true, true
}

// TTLMillis reports the remaining time to live for key in milliseconds, with the
// same ok/hasTTL semantics as TTL. It subtracts two millisecond timestamps rather
// than producing a time.Duration, whose int64 nanoseconds overflow for any
// deadline more than ~292 years out -- a deadline a client can set directly with
// PEXPIREAT, and which made TTL report a large negative number.
func (s *Store) TTLMillis(key string) (remaining int64, hasTTL, ok bool) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e := s.readEntry(sh, key, now)
	if e == nil {
		return 0, false, false
	}
	if e.expireAt.IsZero() {
		return 0, false, true
	}
	return e.expireAt.UnixMilli() - now.UnixMilli(), true, true
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

// Scan iterates the keyspace incrementally. Starting from cursor 0, it returns a
// batch of live keys and the next cursor; a returned cursor of 0 means iteration
// is complete. It walks whole shards at a time, accumulating until at least
// count keys are gathered, which gives SCAN's guarantee that a key present for
// the entire iteration is returned at least once, without ever blocking the
// whole store. count <= 0 defaults to 10.
func (s *Store) Scan(cursor uint64, count int) (keys []string, next uint64) {
	if count <= 0 {
		count = 10
	}
	now := s.clock()
	idx := int(cursor)
	if idx < 0 || idx >= len(s.shards) {
		return nil, 0
	}
	for idx < len(s.shards) {
		sh := s.shards[idx]
		sh.mu.RLock()
		for k, e := range sh.data {
			if !e.expired(now) {
				keys = append(keys, k)
			}
		}
		sh.mu.RUnlock()
		idx++
		if len(keys) >= count {
			break
		}
	}
	if idx >= len(s.shards) {
		return keys, 0 // wrapped: iteration complete
	}
	return keys, uint64(idx)
}

// FlushAll removes every key from every shard.
func (s *Store) FlushAll() {
	for _, sh := range s.shards {
		sh.mu.Lock()
		sh.data = make(map[string]*entry)
		s.mem.Add(-sh.mem)
		sh.mem = 0
		sh.volatile = 0
		sh.mu.Unlock()
	}
}

// --- string operations -------------------------------------------------------

// Get returns a copy of the string value at key. ok is false if the key is
// missing, expired, or holds a non-string type. Expired keys are removed lazily.
// It is the form MGET wants, which reports a wrong-type key as a miss; GetString
// distinguishes the two.
func (s *Store) Get(key string) (value []byte, ok bool) {
	v, ok, _ := s.GetString(key)
	return v, ok
}

// GetString returns a copy of the string value at key, distinguishing a miss
// from a type mismatch: ok is false when the key is missing or expired, and
// ErrWrongType is returned when it holds another data type. Unlike a Type-then-Get
// pair it answers both questions under one acquisition of the shard lock, so the
// hot read path locks once and can never observe two different states.
func (s *Store) GetString(key string) (value []byte, ok bool, err error) {
	sh := s.getShard(key)
	now := s.clock()

	sh.mu.RLock()
	e, found := sh.data[key]
	if !found {
		s.keyspaceMisses.Add(1)
		sh.mu.RUnlock()
		return nil, false, nil
	}
	if e.expired(now) {
		s.keyspaceMisses.Add(1)
		sh.mu.RUnlock()
		s.dropIfExpired(sh, key)
		return nil, false, nil
	}
	s.keyspaceHits.Add(1)
	if e.kind != kindString {
		sh.mu.RUnlock()
		return nil, false, ErrWrongType
	}
	out := copyBytes(e.str)
	s.touch(e, now)
	sh.mu.RUnlock()
	return out, true, nil
}

// Set stores a string value at key, replacing any existing value of any type. A
// ttl of zero means the key never expires.
func (s *Store) Set(key string, value []byte, ttl time.Duration) {
	now := s.clock()
	var expireAt time.Time
	if ttl > 0 {
		expireAt = now.Add(ttl)
	}
	sh := s.getShard(key)
	sh.mu.Lock()
	charged := s.charge(sh, key)
	defer sh.mu.Unlock()
	defer s.settle(sh, key, charged)
	// The value is copied under the lock rather than before it, which is where
	// SetWithOptions -- the path an actual SET command takes -- has always copied it. See
	// putString for why the entry is reused when there is one to reuse.
	s.putString(sh, key, value, expireAt, now)
}

// Incr atomically adds delta to the integer string at key and returns the
// result. A missing/expired key is treated as 0; any existing TTL is preserved.
func (s *Store) Incr(key string, delta int64) (int64, error) {
	sh := s.getShard(key)
	now := s.clock()

	sh.mu.Lock()
	charged := s.charge(sh, key)
	defer sh.mu.Unlock()
	defer s.settle(sh, key, charged)

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
	// AppendInt onto a nil slice rather than []byte(FormatInt(...)): the latter allocates
	// the digits as a string and then copies them into a second allocation, and INCR is one
	// of the commands the vs-redis benchmark measures. The buffer is this function's own, so
	// putOwnedString can adopt it without a further copy.
	s.putOwnedString(sh, key, strconv.AppendInt(nil, n, 10), strWholeValue, expireAt, now)
	return n, nil
}

func (s *Store) dropIfExpired(sh *shard, key string) {
	sh.mu.Lock()
	removed := false
	if e, ok := sh.data[key]; ok && e.expired(s.clock()) {
		delete(sh.data, key)
		s.uncharge(sh, key, e)
		removed = true
	}
	sh.mu.Unlock()
	if removed {
		s.notifyRemoved(key, false) // lazy expiration
	}
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
			// One walk, two jobs: reclaim what has expired (unless the sweep is turned
			// off) and refresh the byte accounting for the values this package cannot
			// track incrementally. They share the walk because both are O(live keys) over
			// exactly the same entries, and doing them separately would double a cost the
			// janitor pays on every tick.
			s.maintain(s.activeExpire.Load())
			s.EvictToLimit()
		}
	}
}

// SetActiveExpire turns the janitor's expiry sweep on or off. Eviction is
// unaffected.
//
// It exists for testing lazy expiration deterministically: with the sweep running, a
// key that has passed its deadline may be reclaimed by the background pass before a
// test can observe that a *read* is what removed it, so the two mechanisms cannot be
// told apart. Redis exposes the same switch as DEBUG SET-ACTIVE-EXPIRE, and its test
// suite relies on it. Turning it off never makes a stale value visible: correctness
// comes from the lazy check on every read, and the sweep only reclaims memory.
func (s *Store) SetActiveExpire(on bool) { s.activeExpire.Store(on) }

// ActiveExpire reports whether the janitor's sweep is enabled.
func (s *Store) ActiveExpire() bool { return s.activeExpire.Load() }

func (s *Store) sweep() { s.maintain(true) }

// maintain walks every shard once. When expire is set it removes the entries whose
// deadline has passed, uncharging each from the byte accounting; either way it refreshes
// the measured payload of every value whose size the mutation paths in this package
// cannot maintain themselves.
//
// That second job is the stream type and only the stream type: its mutations live in
// stream.go, so a stream's payload is re-measured here instead of at the moment it
// changes. The consequence is bounded and stated: used_memory lags a stream-only write
// burst by at most one sweep interval and converges exactly, where every other type is
// exact at every instant.
//
// The shard's total is then re-derived from the per-entry charges rather than adjusted by
// a delta. It costs one addition per entry on a walk that was already O(live keys), and
// it is what makes the pass self-healing at the map level too: a stream key created or
// deleted by a path that does not report it is corrected here, not carried forever.
func (s *Store) maintain(expire bool) {
	now := s.clock()
	tracking := s.memTrack.Load()
	var removed []string
	for _, sh := range s.shards {
		sh.mu.Lock()
		var total int64
		volatile := 0
		for k, e := range sh.data {
			if expire && e.expired(now) {
				delete(sh.data, k)
				removed = append(removed, k)
				continue
			}
			if !tracking {
				continue // nothing is counting, so there is nothing to reconcile
			}
			if e.kind == kindStream {
				e.elemBytes = e.stream.memorySize()
			}
			total += entryCharge(k, e)
			if !e.expireAt.IsZero() {
				volatile++
			}
		}
		if tracking {
			if d := total - sh.mem; d != 0 {
				sh.mem = total
				s.mem.Add(d)
			}
			sh.volatile = volatile
		}
		sh.mu.Unlock()
	}
	for _, k := range removed {
		s.notifyRemoved(k, false) // janitor expiration: outside the shard lock
	}
}

// Shards reports how many lock shards the keyspace is partitioned across. It is the
// one piece of the store's geometry an operator may want to see (INFO reports it),
// since it is what bounds how many writes can proceed in parallel.
func (s *Store) Shards() int { return len(s.shards) }
