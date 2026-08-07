package store

import (
	"math/rand"
	"strconv"
	"time"
)

// This file holds the type-agnostic key operations added beyond the ones in
// store.go and moreops.go: the random pick, the cross-shard copy, the conditional
// expire, and the introspection OBJECT reports.

// ExpireCond is the set of conditions an expire command applies its new deadline
// under: the NX/XX/GT/LT flags of EXPIRE and its siblings. It is a bit set rather
// than an enumeration because Redis accepts the compatible combinations (XX GT is
// "only shorten an existing TTL"), rejecting only NX with any other and GT with LT.
type ExpireCond uint8

// The expire conditions, combinable. A key with no TTL is treated as expiring
// infinitely far in the future, which is what makes GT never fire on a persistent
// key while LT always does -- Redis's rule.
const (
	ExpireNX ExpireCond = 1 << iota // only when the key has no TTL
	ExpireXX                        // only when the key already has a TTL
	ExpireGT                        // only when the new deadline is later
	ExpireLT                        // only when the new deadline is earlier
)

// ExpireAlways is the empty condition set: an expire command with no flag.
const ExpireAlways ExpireCond = 0

// ExpireAtCond sets an absolute expiry on an existing live key subject to cond,
// reporting whether the deadline was actually applied. A missing or already
// expired key always reports false, as does a condition that rejected the new
// deadline.
func (s *Store) ExpireAtCond(key string, deadline time.Time, cond ExpireCond) bool {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	charged := s.charge(sh, key)
	defer sh.mu.Unlock()
	defer s.settle(sh, key, charged)

	e := sh.liveEntry(key, now)
	if e == nil {
		return false
	}
	hasTTL := !e.expireAt.IsZero()
	switch {
	case cond&ExpireNX != 0 && hasTTL:
		return false
	case cond&ExpireXX != 0 && !hasTTL:
		return false
	// A persistent key has no deadline to beat, so GT can never apply to one.
	case cond&ExpireGT != 0 && (!hasTTL || !deadline.After(e.expireAt)):
		return false
	case cond&ExpireLT != 0 && hasTTL && !deadline.Before(e.expireAt):
		return false
	}
	e.expireAt = deadline
	return true
}

// ExpireTimeMillis reports the absolute instant key expires at, as a Unix timestamp in
// milliseconds. ok is false when the key is missing or expired; hasTTL is false when it
// exists and never expires. It is what EXPIRETIME and PEXPIRETIME answer.
//
// It reads the deadline rather than deriving one from a remaining TTL, which is the whole
// point of the two commands: a caller that has to reconstruct an absolute deadline from
// `TTL` plus its own clock gets an answer that is wrong by however long the round trip
// took, and wrong again if the two clocks disagree.
func (s *Store) ExpireTimeMillis(key string) (unixMillis int64, hasTTL, ok bool) {
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
	return e.expireAt.UnixMilli(), true, true
}

// RandomKey returns an arbitrary live key. ok is false only when the store holds
// no live keys at all.
//
// The pick walks shards from a random start and takes the first live key of the
// first non-empty shard, so it costs O(shards) rather than the O(keys) a uniform
// draw would. Like Redis's RANDOMKEY the distribution is approximate; unlike a
// uniform draw it never stalls the store. Callers must not propagate it: two
// copies would pick different keys.
func (s *Store) RandomKey() (string, bool) {
	now := s.clock()
	start := rand.Intn(len(s.shards))
	for i := 0; i < len(s.shards); i++ {
		sh := s.shards[(start+i)&int(s.mask)]
		sh.mu.RLock()
		for k, e := range sh.data {
			if !e.expired(now) {
				sh.mu.RUnlock()
				return k, true
			}
		}
		sh.mu.RUnlock()
	}
	return "", false
}

// Copy duplicates the value and TTL at src into dst, reporting whether anything
// was copied: false when src is missing/expired, or when dst already exists and
// replace is not set. ErrSameObject reports src == dst.
//
// The copy is deep, so a later mutation of one key is never visible through the
// other, and it locks both shards in index order (like Rename) so two concurrent
// copies with swapped arguments cannot deadlock.
func (s *Store) Copy(src, dst string, replace bool) (bool, error) {
	if src == dst {
		return false, ErrSameObject
	}
	now := s.clock()
	unlock := s.lockKeys(src, dst)
	defer s.trackedKeys(unlock, src, dst)()

	ssh, dsh := s.getShard(src), s.getShard(dst)
	e := ssh.liveEntry(src, now)
	if e == nil {
		return false, nil
	}
	if !replace && dsh.liveEntry(dst, now) != nil {
		return false, nil
	}
	ne := e.clone()
	s.touch(ne, now)
	dsh.data[dst] = ne
	return true, nil
}

// RenameNX moves src to dst only when dst does not already exist. renamed reports
// whether the move happened; srcFound is false when src is missing or expired,
// which the caller reports as "no such key".
//
// Renaming a key onto itself is not an error: it reports found but unchanged, the
// 0 reply Redis gives.
func (s *Store) RenameNX(src, dst string) (renamed, srcFound bool) {
	now := s.clock()
	unlock := s.lockKeys(src, dst)
	defer s.trackedKeys(unlock, src, dst)()

	ssh, dsh := s.getShard(src), s.getShard(dst)
	e := ssh.liveEntry(src, now)
	if e == nil {
		return false, false
	}
	if src == dst || dsh.liveEntry(dst, now) != nil {
		return false, true
	}
	delete(ssh.data, src)
	dsh.data[dst] = e
	return true, true
}

// Touch marks key as recently used for the eviction sampler and reports whether
// it is live. It is what TOUCH needs: a read that refreshes the access time
// without returning the value.
func (s *Store) Touch(key string) bool {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e := s.readEntry(sh, key, now)
	if e == nil {
		return false
	}
	s.touch(e, now)
	return true
}

// IdleSeconds reports how long ago key was last accessed, for OBJECT IDLETIME.
// ok is false when the key is missing or expired.
//
// Access times are only recorded while eviction is enabled (that is what keeps the
// read path free of an atomic write in the default configuration), so an untracked
// key reports 0 rather than an age measured from the epoch.
func (s *Store) IdleSeconds(key string) (int64, bool) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e := s.readEntry(sh, key, now)
	if e == nil {
		return 0, false
	}
	atime := e.atime.Load()
	if atime == 0 {
		return 0, true
	}
	idle := now.UnixNano() - atime
	if idle < 0 {
		return 0, true
	}
	return idle / int64(time.Second), true
}

// SetIdleSeconds backdates key's access time so that it reports idle for secs, which
// is what RESTORE's IDLETIME option asks for: a key that arrives from another node
// should carry the age it had there rather than look freshly written to the eviction
// sampler. It reports whether the key was live.
//
// The access time is stored unconditionally, unlike touch, which records nothing while
// eviction is disabled. That costs one atomic store on a command that already rebuilt
// a whole value, and it means OBJECT IDLETIME reports back what RESTORE was told even
// on a server with no cap configured.
func (s *Store) SetIdleSeconds(key string, secs int64) bool {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e := sh.liveEntry(key, now)
	if e == nil {
		return false
	}
	e.atime.Store(now.UnixNano() - secs*int64(time.Second))
	return true
}

// Encoding reports the Redis encoding name OBJECT ENCODING would give the value
// at key. ok is false when the key is missing or expired.
//
// The internals here are not Redis's, so the name is chosen by the same size and
// content thresholds Redis switches its representations at: a client that asks in
// order to decide whether a value is still cheap to scan gets the answer it
// expects, even though this store uses one representation per type throughout.
//
// The thresholds are the configured ones (see encoding.go), not constants, because
// they are configurable in Redis and a caller that lowers one expects the reported
// name to follow.
func (s *Store) Encoding(key string) (string, bool) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return "", false
	}
	switch e.kind {
	case kindString:
		// The name follows how the value was produced, not what its bytes now read as:
		// see strOrigin. A buffer that was edited in place stays raw whatever its length;
		// only a value stored *whole by a command that offers the integer encoding* can
		// read `int`; everything else is embstr or raw by length alone.
		switch e.strOrigin {
		case strMutatedBuffer:
			return "raw", true
		case strWholeValue:
			// The length test comes first because it is a comparison rather than a parse,
			// and because it is also Redis's own precondition (string2ll on a longer
			// buffer cannot yield a long long).
			if len(e.str) <= 20 {
				if _, err := strconv.ParseInt(string(e.str), 10, 64); err == nil {
					return "int", true
				}
			}
		}
		if len(e.str) <= 44 {
			return "embstr", true
		}
		return "raw", true
	case kindList:
		var bytes int64
		for el := e.list.l.Front(); el != nil; el = el.Next() {
			bytes += int64(len(el.Value.([]byte)))
		}
		if listExceedsListpack(s.encoding[ListMaxListpackSize].Load(), e.list.len(), bytes) {
			return "quicklist", true
		}
		return "listpack", true
	case kindHash:
		maxVal := s.encoding[HashMaxListpackValue].Load()
		for f, v := range e.dict {
			if int64(len(f)) > maxVal || int64(len(v)) > maxVal {
				return "hashtable", true
			}
		}
		if int64(len(e.dict)) <= s.encoding[HashMaxListpackEntries].Load() {
			return "listpack", true
		}
		return "hashtable", true
	case kindSet:
		allInts := true
		var longest int64
		for m := range e.set {
			if _, err := strconv.ParseInt(m, 10, 64); err != nil {
				allInts = false
			}
			if int64(len(m)) > longest {
				longest = int64(len(m))
			}
		}
		switch {
		case allInts && int64(len(e.set)) <= s.encoding[SetMaxIntsetEntries].Load():
			return "intset", true
		case int64(len(e.set)) <= s.encoding[SetMaxListpackEntries].Load() &&
			longest <= s.encoding[SetMaxListpackValue].Load():
			return "listpack", true
		}
		return "hashtable", true
	case kindZSet:
		if int64(len(e.zset.dict)) <= s.encoding[ZSetMaxListpackEntries].Load() {
			maxVal := s.encoding[ZSetMaxListpackValue].Load()
			for m := range e.zset.dict {
				if int64(len(m)) > maxVal {
					return "skiplist", true
				}
			}
			return "listpack", true
		}
		return "skiplist", true
	case kindStream:
		// Redis reports "stream" whatever the size, because a stream has only one
		// representation from a client's point of view.
		return "stream", true
	}
	return "", false
}

// clone returns a deep copy of the entry, sharing nothing mutable with it, so a
// copied key and its source can be modified independently. The access time is
// deliberately not carried over: the copy is a brand new key.
//
// strOrigin *is* carried over, because Redis's COPY duplicates the object rather than
// re-storing its bytes (dupStringObject preserves the encoding). Dropping it made
// `SET k 1; APPEND k 2; COPY k d` report `int` for d and `raw` for k -- two encodings
// for one byte sequence, which is the answer that cannot be right.
func (e *entry) clone() *entry {
	ne := &entry{kind: e.kind, strOrigin: e.strOrigin, expireAt: e.expireAt}
	switch e.kind {
	case kindString:
		ne.str = copyBytes(e.str)
	case kindList:
		ne.list = newDeque()
		for el := e.list.l.Front(); el != nil; el = el.Next() {
			ne.list.rpush(copyBytes(el.Value.([]byte)))
		}
	case kindHash:
		ne.dict = make(map[string][]byte, len(e.dict))
		for f, v := range e.dict {
			// Through hashPut, not a raw map write: the copy's buffers have their own
			// capacities, so the clone's byte count has to be built from them rather
			// than copied from the original's.
			hashPut(ne, f, copyBytes(v))
		}
	case kindSet:
		ne.set = make(map[string]struct{}, len(e.set))
		for m := range e.set {
			setPut(ne, m)
		}
	case kindZSet:
		ne.zset = newZSet()
		node := e.zset.sl.head.next[0].forward
		for node != nil {
			ne.zset.add(node.member, node.score)
			node = node.next[0].forward
		}
	case kindStream:
		ne.stream = e.stream.clone()
	}
	return ne
}
