package store

import (
	"math/rand"
	"sync/atomic"
)

// zsetSeed gives every sorted set an independent, deterministic RNG seed for its
// skip list. Each zset's skip list is only ever touched under its shard's lock,
// so a per-zset *rand.Rand never sees concurrent access.
var zsetSeed atomic.Int64

// zset is a sorted set: a member->score map for O(1) score lookup plus a skip
// list ordered by (score, member) for O(log n) rank and range queries.
type zset struct {
	dict map[string]float64
	sl   *skipList
}

func newZSet() *zset {
	seed := zsetSeed.Add(1)
	return &zset{
		dict: make(map[string]float64),
		sl:   newSkipList(rand.New(rand.NewSource(seed))),
	}
}

// add inserts or updates member. added is true only for a newly-created member
// (for the ZADD reply count); changed is true if the set was modified at all
// (new member or a different score) so callers can avoid propagating a no-op.
func (z *zset) add(member string, score float64) (added, changed bool) {
	if old, ok := z.dict[member]; ok {
		if old == score {
			return false, false
		}
		z.sl.delete(member, old)
		z.sl.insert(member, score)
		z.dict[member] = score
		return false, true
	}
	z.dict[member] = score
	z.sl.insert(member, score)
	return true, true
}

func (z *zset) remove(member string) bool {
	old, ok := z.dict[member]
	if !ok {
		return false
	}
	delete(z.dict, member)
	z.sl.delete(member, old)
	return true
}

// --- store-level sorted-set operations ---------------------------------------

// ZAdd adds or updates member with the given score. added is the number of
// newly-created members (0 or 1); changed reports whether the set was modified
// (false for a same-score no-op, so the caller can skip propagation).
func (s *Store) ZAdd(key string, member string, score float64) (added int, changed bool, err error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, found := sh.data[key]
	live := found && !e.expired(now)
	if live && e.kind != kindZSet {
		return 0, false, ErrWrongType
	}
	if !live {
		e = &entry{kind: kindZSet, zset: newZSet()}
		sh.data[key] = e
	}
	isNew, didChange := e.zset.add(member, score)
	if isNew {
		added = 1
	}
	s.touch(e, now)
	return added, didChange, nil
}

// ZScore returns the score of member. ok is false if key or member is absent.
func (s *Store) ZScore(key, member string) (float64, bool, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e, found := sh.data[key]
	if !found || e.expired(now) {
		return 0, false, nil
	}
	if e.kind != kindZSet {
		return 0, false, ErrWrongType
	}
	score, ok := e.zset.dict[member]
	return score, ok, nil
}

// ZRem removes member and reports whether it was present. Deletes the key when
// the set becomes empty.
func (s *Store) ZRem(key, member string) (bool, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, found := sh.data[key]
	if !found || e.expired(now) {
		return false, nil
	}
	if e.kind != kindZSet {
		return false, ErrWrongType
	}
	removed := e.zset.remove(member)
	if len(e.zset.dict) == 0 {
		delete(sh.data, key)
	}
	return removed, nil
}

// ZCard returns the cardinality of the sorted set (0 if missing).
func (s *Store) ZCard(key string) (int, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e, found := sh.data[key]
	if !found || e.expired(now) {
		return 0, nil
	}
	if e.kind != kindZSet {
		return 0, ErrWrongType
	}
	return len(e.zset.dict), nil
}

// ZRank returns the 0-based rank of member in ascending score order. ok is false
// if key or member is absent.
func (s *Store) ZRank(key, member string) (int, bool, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e, found := sh.data[key]
	if !found || e.expired(now) {
		return 0, false, nil
	}
	if e.kind != kindZSet {
		return 0, false, ErrWrongType
	}
	score, ok := e.zset.dict[member]
	if !ok {
		return 0, false, nil
	}
	return e.zset.sl.rank(member, score), true, nil
}

// ZMember is a member/score pair returned by range queries.
type ZMember struct {
	Member string
	Score  float64
}

// ZRange returns members in the inclusive rank range [start, stop] in ascending
// score order, using Redis index semantics (negatives count from the end).
func (s *Store) ZRange(key string, start, stop int) ([]ZMember, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e, found := sh.data[key]
	if !found || e.expired(now) {
		return nil, nil
	}
	if e.kind != kindZSet {
		return nil, ErrWrongType
	}

	n := e.zset.sl.length
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop || n == 0 {
		return nil, nil
	}

	out := make([]ZMember, 0, stop-start+1)
	node := e.zset.sl.nodeByRank(start)
	for i := start; i <= stop && node != nil; i++ {
		out = append(out, ZMember{Member: node.member, Score: node.score})
		node = node.next[0].forward
	}
	return out, nil
}
