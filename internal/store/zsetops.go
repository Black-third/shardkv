package store

import "math"

// This file extends the sorted set whose ZAdd/ZScore/ZRange core lives in zset.go
// with the option-driven add, the score and lexicographic range queries, the
// range removals, and the pops.

// ZAddOptions is the parsed flag set of the ZADD command. NX/XX gate on the
// member's existence, GT/LT on its current score, CH changes what the reply
// counts, and Incr turns the score operand into a delta.
type ZAddOptions struct {
	NX   bool // only add new members, never update
	XX   bool // only update existing members, never add
	GT   bool // only update when the new score is greater
	LT   bool // only update when the new score is lower
	CH   bool // report members changed rather than members added
	Incr bool // treat the score as an increment
}

// allows reports whether the NX/XX flags admit a member, given whether it is
// already in the set.
func (o ZAddOptions) allows(present bool) bool {
	switch {
	case o.NX && present:
		return false
	case o.XX && !present:
		return false
	}
	return true
}

// ZAddMulti adds or updates several members under one acquisition of the shard
// lock, so a concurrent reader never sees half of a multi-pair ZADD. added counts
// newly-created members and changed counts every member whose score moved plus
// every one created, which is what the CH flag reports.
//
// The whole command is validated by the caller before it reaches here; the options
// are then applied per member, as Redis does (one rejected member does not abort
// the rest).
func (s *Store) ZAddMulti(key string, o ZAddOptions, members []ZMember) (added, changed int, err error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	charged := s.charge(sh, key)
	defer sh.mu.Unlock()
	defer s.settle(sh, key, charged)

	e := sh.liveEntry(key, now)
	if e != nil && e.kind != kindZSet {
		return 0, 0, ErrWrongType
	}
	if e == nil {
		if o.XX {
			return 0, 0, nil // nothing to update, and XX forbids creating
		}
		e = &entry{kind: kindZSet, zset: newZSet()}
		sh.data[key] = e
	}
	for _, m := range members {
		cur, present := e.zset.dict[m.Member]
		if !o.allows(present) || !gateScore(o, m.Score, cur, present) {
			continue
		}
		isNew, didChange := e.zset.add(m.Member, m.Score)
		if isNew {
			added++
		}
		if didChange {
			changed++
		}
	}
	if len(e.zset.dict) == 0 {
		delete(sh.data, key) // XX on a key we had to create: leave nothing behind
	} else {
		s.touch(e, now)
	}
	return added, changed, nil
}

// gateScore applies the GT/LT flags: they only ever block an update to an existing
// member, so a member being created passes regardless. That is Redis's rule, and
// it is what makes "ZADD key GT score member" a safe high-water-mark update.
func gateScore(o ZAddOptions, score, cur float64, present bool) bool {
	if !present {
		return true
	}
	switch {
	case o.GT:
		return score > cur
	case o.LT:
		return score < cur
	}
	return true
}

// ZIncrBy adds delta to the score of member (creating it from 0 if absent) and
// returns the new score. applied is false when the options rejected the update, in
// which case the score is unchanged and ZADD INCR replies nil.
//
// ErrScoreNaN reports a result that is not a number, which is only reachable by
// adding opposite infinities; Redis refuses it rather than storing a member that
// can never be ordered.
func (s *Store) ZIncrBy(key, member string, delta float64, o ZAddOptions) (score float64, applied bool, err error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	charged := s.charge(sh, key)
	defer sh.mu.Unlock()
	defer s.settle(sh, key, charged)

	e := sh.liveEntry(key, now)
	if e != nil && e.kind != kindZSet {
		return 0, false, ErrWrongType
	}
	var cur float64
	var present bool
	if e != nil {
		cur, present = e.zset.dict[member]
	}
	if !o.allows(present) {
		return 0, false, nil
	}
	score = cur + delta
	if math.IsNaN(score) {
		return 0, false, ErrScoreNaN
	}
	if !gateScore(o, score, cur, present) {
		return 0, false, nil
	}
	if e == nil {
		e = &entry{kind: kindZSet, zset: newZSet()}
		sh.data[key] = e
	}
	e.zset.add(member, score)
	s.touch(e, now)
	return score, true, nil
}

// ZRemMulti removes the given members and returns how many were present. The key
// is deleted once the set is empty.
func (s *Store) ZRemMulti(key string, members ...string) (int, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	charged := s.charge(sh, key)
	defer sh.mu.Unlock()
	defer s.settle(sh, key, charged)

	e := sh.liveEntry(key, now)
	if e == nil {
		return 0, nil
	}
	if e.kind != kindZSet {
		return 0, ErrWrongType
	}
	removed := 0
	for _, m := range members {
		if e.zset.remove(m) {
			removed++
		}
	}
	if len(e.zset.dict) == 0 {
		delete(sh.data, key)
	} else if removed > 0 {
		s.touch(e, now)
	}
	return removed, nil
}

// ZRevRange returns members in the inclusive rank range [start, stop] counted from
// the highest score down, i.e. ZRANGE's indexes applied to the reversed order.
func (s *Store) ZRevRange(key string, start, stop int) ([]ZMember, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return nil, nil
	}
	if e.kind != kindZSet {
		return nil, ErrWrongType
	}
	n := e.zset.sl.length
	from, to, ok := normalizeRange(n, start, stop)
	if !ok {
		return nil, nil
	}
	// Rank r from the top is rank n-1-r from the bottom, so walk the ascending
	// range and reverse it.
	out := make([]ZMember, 0, to-from+1)
	node := e.zset.sl.nodeByRank(n - 1 - to)
	for i := n - 1 - to; i <= n-1-from && node != nil; i++ {
		out = append(out, ZMember{Member: node.member, Score: node.score})
		node = node.next[0].forward
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, nil
}

// ZRevRank returns the 0-based rank of member counted from the highest score down.
// ok is false if the key or the member is absent.
func (s *Store) ZRevRank(key, member string) (int, bool, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return 0, false, nil
	}
	if e.kind != kindZSet {
		return 0, false, ErrWrongType
	}
	score, ok := e.zset.dict[member]
	if !ok {
		return 0, false, nil
	}
	return e.zset.sl.length - 1 - e.zset.sl.rank(member, score), true, nil
}

// ZMScore returns the score of each member in order. present[i] is false for a
// member the set does not hold, and for every member of a missing key, so the
// reply always has one element per requested member.
func (s *Store) ZMScore(key string, members ...string) (scores []float64, present []bool, err error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	scores = make([]float64, len(members))
	present = make([]bool, len(members))
	e := s.readEntry(sh, key, now)
	if e == nil {
		return scores, present, nil
	}
	if e.kind != kindZSet {
		return nil, nil, ErrWrongType
	}
	for i, m := range members {
		scores[i], present[i] = e.zset.dict[m]
	}
	return scores, present, nil
}

// ZRandMember returns count members of the sorted set, with their scores. A
// negative count allows repeats and yields exactly -count members; a positive
// count returns distinct members capped at the set size. The draw is random, so a
// caller must never propagate the command itself.
func (s *Store) ZRandMember(key string, count int) ([]ZMember, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return nil, nil
	}
	if e.kind != kindZSet {
		return nil, ErrWrongType
	}
	s.touch(e, now)
	pool := make([]string, 0, len(e.zset.dict))
	for m := range e.zset.dict {
		pool = append(pool, m)
	}
	picked := pickMembers(pool, count)
	out := make([]ZMember, 0, len(picked))
	for _, m := range picked {
		out = append(out, ZMember{Member: m, Score: e.zset.dict[m]})
	}
	return out, nil
}

// ZPop removes and returns up to count members from the low-score end (fromMin) or
// the high-score end, in the order they were popped. A missing key yields nothing;
// the key is deleted once it is empty.
func (s *Store) ZPop(key string, count int, fromMin bool) ([]ZMember, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	charged := s.charge(sh, key)
	defer sh.mu.Unlock()
	defer s.settle(sh, key, charged)

	e := sh.liveEntry(key, now)
	if e == nil {
		return nil, nil
	}
	if e.kind != kindZSet {
		return nil, ErrWrongType
	}
	out := make([]ZMember, 0, min(count, e.zset.sl.length))
	for i := 0; i < count; i++ {
		node := e.zset.sl.head.next[0].forward
		if !fromMin {
			node = e.zset.sl.tail
		}
		if node == nil {
			break
		}
		out = append(out, ZMember{Member: node.member, Score: node.score})
		e.zset.remove(node.member)
	}
	if len(e.zset.dict) == 0 {
		delete(sh.data, key)
	} else if len(out) > 0 {
		s.touch(e, now)
	}
	return out, nil
}

// ZRemRangeByRank removes the members in the inclusive rank range [start, stop]
// (ascending score order, negative indexes counting from the end) and returns how
// many went.
func (s *Store) ZRemRangeByRank(key string, start, stop int) (int, error) {
	return s.zRemRange(key, func(z *zset) []string {
		from, to, ok := normalizeRange(z.sl.length, start, stop)
		if !ok {
			return nil
		}
		var doomed []string
		node := z.sl.nodeByRank(from)
		for i := from; i <= to && node != nil; i++ {
			doomed = append(doomed, node.member)
			node = node.next[0].forward
		}
		return doomed
	})
}

// ZRemRangeByScore removes every member whose score falls in r and returns how
// many went.
func (s *Store) ZRemRangeByScore(key string, r ScoreRange) (int, error) {
	return s.zRemRange(key, func(z *zset) []string {
		var doomed []string
		for node := z.firstInScoreRange(r); node != nil; node = node.next[0].forward {
			if !r.belowMax(node.score) {
				break
			}
			doomed = append(doomed, node.member)
		}
		return doomed
	})
}

// zRemRange removes whichever members select returns, under one acquisition of the
// shard lock: the members are collected before any is unlinked, since removing a
// node while walking the skip list would drop the walk's own successor pointer.
func (s *Store) zRemRange(key string, pick func(*zset) []string) (int, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	charged := s.charge(sh, key)
	defer sh.mu.Unlock()
	defer s.settle(sh, key, charged)

	e := sh.liveEntry(key, now)
	if e == nil {
		return 0, nil
	}
	if e.kind != kindZSet {
		return 0, ErrWrongType
	}
	doomed := pick(e.zset)
	for _, m := range doomed {
		e.zset.remove(m)
	}
	if len(e.zset.dict) == 0 {
		delete(sh.data, key)
	} else if len(doomed) > 0 {
		s.touch(e, now)
	}
	return len(doomed), nil
}

// --- score and lexicographic ranges -------------------------------------------

// ScoreRange is the score interval ZRANGEBYSCORE and its relatives select, with
// each end optionally exclusive (Redis's "(" prefix) or infinite.
type ScoreRange struct {
	Min, Max         float64
	MinExcl, MaxExcl bool
}

// aboveMin reports whether score satisfies the range's lower bound.
func (r ScoreRange) aboveMin(score float64) bool {
	if r.MinExcl {
		return score > r.Min
	}
	return score >= r.Min
}

// belowMax reports whether score satisfies the range's upper bound.
func (r ScoreRange) belowMax(score float64) bool {
	if r.MaxExcl {
		return score < r.Max
	}
	return score <= r.Max
}

// firstInScoreRange returns the first node satisfying the range's lower bound, or
// nil when no member reaches it. It descends the skip list, so finding the start of
// a range costs O(log n) rather than a scan from the head.
func (z *zset) firstInScoreRange(r ScoreRange) *slNode {
	x := z.sl.head
	for i := z.sl.level - 1; i >= 0; i-- {
		for x.next[i].forward != nil && !r.aboveMin(x.next[i].forward.score) {
			x = x.next[i].forward
		}
	}
	return x.next[0].forward
}

// ZRangeByScore returns the members whose scores fall in r, ascending (or
// descending when rev is set). offset skips that many results and count bounds how
// many are returned, with a negative count meaning "all that remain" -- the LIMIT
// clause. A negative offset returns nothing, as Redis does.
func (s *Store) ZRangeByScore(key string, r ScoreRange, offset, count int, rev bool) ([]ZMember, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return nil, nil
	}
	if e.kind != kindZSet {
		return nil, ErrWrongType
	}
	if offset < 0 {
		return nil, nil
	}
	s.touch(e, now)

	var in []ZMember
	for node := e.zset.firstInScoreRange(r); node != nil; node = node.next[0].forward {
		if !r.belowMax(node.score) {
			break
		}
		in = append(in, ZMember{Member: node.member, Score: node.score})
	}
	if rev {
		for i, j := 0, len(in)-1; i < j; i, j = i+1, j-1 {
			in[i], in[j] = in[j], in[i]
		}
	}
	if offset >= len(in) {
		return nil, nil
	}
	in = in[offset:]
	if count >= 0 && count < len(in) {
		in = in[:count]
	}
	return in, nil
}

// ZCount returns how many members have a score inside r.
func (s *Store) ZCount(key string, r ScoreRange) (int, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return 0, nil
	}
	if e.kind != kindZSet {
		return 0, ErrWrongType
	}
	n := 0
	for node := e.zset.firstInScoreRange(r); node != nil; node = node.next[0].forward {
		if !r.belowMax(node.score) {
			break
		}
		n++
	}
	return n, nil
}

// LexBound is one end of a lexicographic range: either an infinity -- Redis's "-" for the
// smallest possible member and "+" for the largest -- or a member with an inclusive
// ("[member") or exclusive ("(member") comparison.
//
// Both infinities are legal at *either* end, which is why this is a bound rather than a
// pair of "unbounded" flags. `ZLEXCOUNT key + -` is a well-formed query whose answer is 0
// (nothing is both above the largest member and below the smallest), and Redis answers 0
// rather than rejecting it -- so a range has to be able to express a lower bound of
// positive infinity, and its own test suite asks for exactly that.
type LexBound struct {
	NegInf bool // "-": below every member
	PosInf bool // "+": above every member
	Excl   bool
	Member string
}

// LexRange is the member interval the lexicographic commands select.
type LexRange struct {
	Min, Max LexBound
}

func (r LexRange) aboveMin(m string) bool {
	switch {
	case r.Min.NegInf:
		return true
	case r.Min.PosInf:
		return false
	case r.Min.Excl:
		return m > r.Min.Member
	}
	return m >= r.Min.Member
}

func (r LexRange) belowMax(m string) bool {
	switch {
	case r.Max.PosInf:
		return true
	case r.Max.NegInf:
		return false
	case r.Max.Excl:
		return m < r.Max.Member
	}
	return m <= r.Max.Member
}

// ZLexCount returns how many members fall inside the lexicographic range r.
//
// As in Redis the answer is only meaningful when every member shares one score:
// the index is ordered by (score, member), so members are in lexicographic order
// only within a score. It walks in order and stops at the first member past the
// upper bound.
func (s *Store) ZLexCount(key string, r LexRange) (int, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return 0, nil
	}
	if e.kind != kindZSet {
		return 0, ErrWrongType
	}
	n := 0
	for node := e.zset.sl.head.next[0].forward; node != nil; node = node.next[0].forward {
		if !r.belowMax(node.member) {
			break
		}
		if r.aboveMin(node.member) {
			n++
		}
	}
	return n, nil
}

// ZRangeByScoreMulti returns the members falling in any of the given score ranges,
// ascending, under a single acquisition of the shard lock.
//
// It exists for GEOSEARCH, which resolves a circle or a box into a handful of
// geohash cells and so needs several disjoint score ranges answered as one query. Doing
// it with several ZRangeByScore calls would take the lock several times, and a
// concurrent GEOADD in between could move a member from a cell already scanned into one
// not yet scanned -- so the search would miss a member that was present throughout. One
// lock gives the whole search one consistent cut, which is the same reason the multi-key
// set operations take rlockKeys.
//
// Ranges are walked in the order given and the results concatenated; the caller sorts by
// whatever it is ordering on (distance, for GEOSEARCH).
func (s *Store) ZRangeByScoreMulti(key string, ranges []ScoreRange) ([]ZMember, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return nil, nil
	}
	if e.kind != kindZSet {
		return nil, ErrWrongType
	}
	s.touch(e, now)

	var out []ZMember
	for _, r := range ranges {
		for node := e.zset.firstInScoreRange(r); node != nil; node = node.next[0].forward {
			if !r.belowMax(node.score) {
				break
			}
			out = append(out, ZMember{Member: node.member, Score: node.score})
		}
	}
	return out, nil
}
