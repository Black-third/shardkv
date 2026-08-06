package store

import (
	"math"
	"sort"
	"time"
)

// The sorted-set algebra -- union, intersection and difference -- plus the
// lexicographic range queries and the range-into-a-key store.
//
// Two things here are load-bearing and easy to get wrong.
//
// A *plain set* is a legal input to every one of the combining operations, and each of
// its members counts with a score of 1. That is Redis's rule, and it is what makes
// `ZUNIONSTORE dest 2 someset somezset` mean anything; a server that answered WRONGTYPE
// for a set would refuse a command a great deal of code sends. See scoredMembers.
//
// And every one of them reads *all* of its inputs under one acquisition of the shard
// locks (rlockKeys / lockKeys, in shard-index order -- invariant 8). Reading the inputs
// one at a time would let a concurrent ZADD move a member between two of them, so the
// result could contain a member that was in neither input at any single instant, or miss
// one that was in both throughout.

// ZAggregate selects how a member's scores from several inputs are combined.
type ZAggregate int

// The aggregations, under Redis's names. Sum is the default.
const (
	ZAggSum ZAggregate = iota
	ZAggMin
	ZAggMax
)

// ZCombineKind is which of the three set operations to perform.
type ZCombineKind int

const (
	ZCombineUnion ZCombineKind = iota
	ZCombineInter
	ZCombineDiff
)

// ZSetOp is one input to a combining operation: the key and the weight its scores are
// multiplied by before aggregation. A weight of 1 leaves them alone.
type ZSetOp struct {
	Key    string
	Weight float64
}

// scoredMembers reads key from an already-locked shard as a member -> score map.
//
// A sorted set yields its own scores; a plain set yields 1 for every member, which is
// Redis's convention and the whole reason the algebra accepts both types. A missing or
// expired key yields nil, which every operation treats as the empty set. Anything else is
// ErrWrongType.
//
// The map is a fresh one rather than the entry's own, so the caller may aggregate into it
// without mutating the input.
func (sh *shard) scoredMembers(key string, now time.Time) (map[string]float64, error) {
	e := sh.liveEntry(key, now)
	if e == nil {
		return nil, nil
	}
	switch e.kind {
	case kindZSet:
		out := make(map[string]float64, len(e.zset.dict))
		for m, sc := range e.zset.dict {
			out[m] = sc
		}
		return out, nil
	case kindSet:
		out := make(map[string]float64, len(e.set))
		for m := range e.set {
			out[m] = 1
		}
		return out, nil
	}
	return nil, ErrWrongType
}

// weighted applies an input's weight, folding the NaN that 0 * infinity produces back to
// zero -- Redis's convention, and the only one that keeps a member orderable.
func weighted(score, weight float64) float64 {
	v := score * weight
	if math.IsNaN(v) {
		return 0
	}
	return v
}

// aggregate folds val into target under the chosen aggregation. Summing +inf and -inf
// gives NaN, which Redis maps to 0 for the same reason weighted does.
func aggregate(target, val float64, agg ZAggregate) float64 {
	switch agg {
	case ZAggMin:
		if val < target {
			return val
		}
		return target
	case ZAggMax:
		if val > target {
			return val
		}
		return target
	}
	v := target + val
	if math.IsNaN(v) {
		return 0
	}
	return v
}

// ZCombine computes a union, intersection or difference over the given inputs and returns
// the result ordered by (score, member) -- the order a sorted set is in, so the reply is
// what ZRANGE would give if the result had been stored and read back.
//
// Difference ignores weights and the aggregation, as Redis does: ZDIFF has neither
// option, and the scores it reports are the first input's own.
func (s *Store) ZCombine(kind ZCombineKind, ops []ZSetOp, agg ZAggregate) ([]ZMember, error) {
	if len(ops) == 0 {
		return nil, nil
	}
	keys := make([]string, len(ops))
	for i, op := range ops {
		keys[i] = op.Key
	}
	now := s.clock()
	unlock := s.rlockKeys(keys...)
	defer unlock()

	acc, err := s.zcombineLocked(kind, ops, agg, now)
	if err != nil {
		return nil, err
	}
	return sortedMembers(acc), nil
}

// zcombineLocked is the computation itself, with every input's shard already locked.
func (s *Store) zcombineLocked(kind ZCombineKind, ops []ZSetOp, agg ZAggregate, now time.Time) (map[string]float64, error) {
	first, err := s.getShard(ops[0].Key).scoredMembers(ops[0].Key, now)
	if err != nil {
		return nil, err
	}

	switch kind {
	case ZCombineUnion:
		acc := make(map[string]float64, len(first))
		for m, sc := range first {
			acc[m] = weighted(sc, ops[0].Weight)
		}
		for _, op := range ops[1:] {
			in, err := s.getShard(op.Key).scoredMembers(op.Key, now)
			if err != nil {
				return nil, err
			}
			for m, sc := range in {
				v := weighted(sc, op.Weight)
				if cur, seen := acc[m]; seen {
					acc[m] = aggregate(cur, v, agg)
					continue
				}
				acc[m] = v
			}
		}
		return acc, nil

	case ZCombineInter:
		// The other inputs are all read before anything is folded, because an intersection
		// with a missing (or wrong-typed) key is empty and a type error has to be reported
		// whether or not the first input happened to be empty.
		others := make([]map[string]float64, 0, len(ops)-1)
		for _, op := range ops[1:] {
			in, err := s.getShard(op.Key).scoredMembers(op.Key, now)
			if err != nil {
				return nil, err
			}
			others = append(others, in)
		}
		acc := make(map[string]float64)
	members:
		for m, sc := range first {
			v := weighted(sc, ops[0].Weight)
			for i, in := range others {
				other, ok := in[m]
				if !ok {
					continue members
				}
				v = aggregate(v, weighted(other, ops[i+1].Weight), agg)
			}
			acc[m] = v
		}
		return acc, nil
	}

	// Difference.
	others := make([]map[string]float64, 0, len(ops)-1)
	for _, op := range ops[1:] {
		in, err := s.getShard(op.Key).scoredMembers(op.Key, now)
		if err != nil {
			return nil, err
		}
		others = append(others, in)
	}
	acc := make(map[string]float64, len(first))
diff:
	for m, sc := range first {
		for _, in := range others {
			if _, ok := in[m]; ok {
				continue diff
			}
		}
		acc[m] = sc
	}
	return acc, nil
}

// sortedMembers renders an accumulator as a sorted-set ordering: ascending by score, and
// by member for equal scores. Sorting is what makes the reply deterministic -- the
// accumulator is a Go map, so without it two servers given the same inputs would answer
// in different orders and a replica's ZRANGESTORE would disagree with its master's.
func sortedMembers(acc map[string]float64) []ZMember {
	out := make([]ZMember, 0, len(acc))
	for m, sc := range acc {
		out = append(out, ZMember{Member: m, Score: sc})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score < out[j].Score
		}
		return out[i].Member < out[j].Member
	})
	return out
}

// ZCombineStore computes the operation and replaces dest with the result, returning the
// number of members stored. An empty result deletes dest, which is Redis's rule and the
// reason the reply is a count rather than an acknowledgement: 0 means "dest is now gone".
//
// dest is locked together with the inputs, in shard-index order, so the whole command is
// one atomic step even when dest is also one of the inputs -- `ZUNIONSTORE k 2 k other`
// is a legal and common way to accumulate into a running total.
func (s *Store) ZCombineStore(dest string, kind ZCombineKind, ops []ZSetOp, agg ZAggregate) (int, error) {
	if len(ops) == 0 {
		return 0, nil
	}
	keys := make([]string, 0, len(ops)+1)
	keys = append(keys, dest)
	for _, op := range ops {
		keys = append(keys, op.Key)
	}
	now := s.clock()
	unlock := s.lockKeys(keys...)
	defer unlock()

	acc, err := s.zcombineLocked(kind, ops, agg, now)
	if err != nil {
		return 0, err
	}
	return s.replaceZSetLocked(dest, sortedMembers(acc), now), nil
}

// replaceZSetLocked overwrites dest with exactly these members, deleting it when there are
// none. The caller holds dest's shard lock.
func (s *Store) replaceZSetLocked(dest string, members []ZMember, now time.Time) int {
	dsh := s.getShard(dest)
	if len(members) == 0 {
		delete(dsh.data, dest)
		return 0
	}
	z := newZSet()
	for _, m := range members {
		z.add(m.Member, m.Score)
	}
	// A store replaces the destination outright, so any TTL it had goes with the old value.
	// Redis does the same: the key is a new object.
	e := &entry{kind: kindZSet, zset: z}
	s.touch(e, now)
	dsh.data[dest] = e
	return len(members)
}

// ZInterCard reports how many members the intersection would contain, stopping once limit
// have been found (0 = no limit).
//
// It exists because the count is often all a caller wants, and computing it does not
// require building the intersection: with a limit it can stop early, which is what makes
// "do these two large sets overlap at all" cheap.
func (s *Store) ZInterCard(keys []string, limit int) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	now := s.clock()
	unlock := s.rlockKeys(keys...)
	defer unlock()

	first, err := s.getShard(keys[0]).scoredMembers(keys[0], now)
	if err != nil {
		return 0, err
	}
	others := make([]map[string]float64, 0, len(keys)-1)
	for _, k := range keys[1:] {
		in, err := s.getShard(k).scoredMembers(k, now)
		if err != nil {
			return 0, err
		}
		others = append(others, in)
	}
	n := 0
members:
	for m := range first {
		for _, in := range others {
			if _, ok := in[m]; !ok {
				continue members
			}
		}
		n++
		if limit > 0 && n >= limit {
			break
		}
	}
	return n, nil
}

// --- lexicographic ranges ------------------------------------------------------

// ZRangeByLex returns the members inside the lexicographic range r, ascending (or
// descending when rev is set), with the LIMIT clause applied after the ordering.
//
// As with ZLEXCOUNT the answer is only meaningful when every member shares one score: the
// index is ordered by (score, member), so members are in lexicographic order only within
// a score. That is Redis's caveat too, and it is a property of the data rather than of the
// implementation -- which is why this walks the index in order rather than sorting, and so
// reports exactly what a Redis walking its own skip list would.
func (s *Store) ZRangeByLex(key string, r LexRange, offset, count int, rev bool) ([]ZMember, error) {
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

	in := lexMembers(e.zset, r)
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

// lexMembers collects the members inside r, ascending. The walk stops at the first member
// past the upper bound rather than scanning the whole set.
func lexMembers(z *zset, r LexRange) []ZMember {
	var out []ZMember
	for node := z.sl.head.next[0].forward; node != nil; node = node.next[0].forward {
		if !r.belowMax(node.member) {
			break
		}
		if r.aboveMin(node.member) {
			out = append(out, ZMember{Member: node.member, Score: node.score})
		}
	}
	return out
}

// ZRemRangeByLex removes every member inside the lexicographic range r and returns how
// many went.
func (s *Store) ZRemRangeByLex(key string, r LexRange) (int, error) {
	return s.zRemRange(key, func(z *zset) []string {
		picked := lexMembers(z, r)
		doomed := make([]string, 0, len(picked))
		for _, m := range picked {
			doomed = append(doomed, m.Member)
		}
		return doomed
	})
}

// --- ZRANGESTORE ---------------------------------------------------------------

// ZRangeSelector describes which slice of a sorted set ZRANGESTORE is to copy: exactly
// the selection ZRANGE's own options express. Only one of the three bound forms is used,
// chosen by By.
type ZRangeSelector struct {
	By     ZRangeBy
	Start  int // rank bounds, for ZRangeByRank
	Stop   int
	Score  ScoreRange
	Lex    LexRange
	Offset int
	Count  int // negative means "everything from the offset on"
	Rev    bool
}

// ZRangeBy is which kind of bound a range selection uses.
type ZRangeBy int

const (
	ZRangeByRank ZRangeBy = iota
	ZRangeByScore
	ZRangeByLex
)

// ZRangeStore copies the selected slice of src into dest, replacing it, and returns how
// many members were stored. An empty selection deletes dest.
//
// Both keys are locked together, in shard-index order, which is what makes the command
// atomic when they are the same key -- `ZRANGESTORE k k 0 5` is a legal way to truncate a
// set in place, and reading it after having already replaced it would produce nothing.
func (s *Store) ZRangeStore(dest, src string, sel ZRangeSelector) (int, error) {
	now := s.clock()
	unlock := s.lockKeys(dest, src)
	defer unlock()

	e := s.getShard(src).liveEntry(src, now)
	if e != nil && e.kind != kindZSet {
		return 0, ErrWrongType
	}
	var picked []ZMember
	if e != nil {
		picked = selectRange(e.zset, sel)
	}
	return s.replaceZSetLocked(dest, picked, now), nil
}

// ZRangeSelect returns the slice of key the selector names, which is what the general
// ZRANGE answers. It shares its selection with ZRangeStore, deliberately: one description
// of "what these options select", read by both the command that returns it and the command
// that stores it.
func (s *Store) ZRangeSelect(key string, sel ZRangeSelector) ([]ZMember, error) {
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
	return selectRange(e.zset, sel), nil
}

// selectRange applies a selector to a sorted set, with the shard lock already held. It is
// the shared body of ZRANGESTORE and of the read-side range commands' bounds, so the two
// cannot disagree about what a given set of options selects.
func selectRange(z *zset, sel ZRangeSelector) []ZMember {
	var in []ZMember
	switch sel.By {
	case ZRangeByScore:
		for node := z.firstInScoreRange(sel.Score); node != nil; node = node.next[0].forward {
			if !sel.Score.belowMax(node.score) {
				break
			}
			in = append(in, ZMember{Member: node.member, Score: node.score})
		}
	case ZRangeByLex:
		in = lexMembers(z, sel.Lex)
	default:
		// Rank bounds are applied to the requested *direction*, so REV's indexes count from
		// the high-score end -- and, unlike the score and lex forms, the reversal happens
		// before the indexes are resolved rather than after.
		n := z.sl.length
		all := make([]ZMember, 0, n)
		for node := z.sl.head.next[0].forward; node != nil; node = node.next[0].forward {
			all = append(all, ZMember{Member: node.member, Score: node.score})
		}
		if sel.Rev {
			for i, j := 0, len(all)-1; i < j; i, j = i+1, j-1 {
				all[i], all[j] = all[j], all[i]
			}
		}
		from, to, ok := normalizeRange(n, sel.Start, sel.Stop)
		if !ok {
			return nil
		}
		return all[from : to+1]
	}
	if sel.Rev {
		for i, j := 0, len(in)-1; i < j; i, j = i+1, j-1 {
			in[i], in[j] = in[j], in[i]
		}
	}
	if sel.Offset < 0 || sel.Offset >= len(in) {
		return nil
	}
	in = in[sel.Offset:]
	if sel.Count >= 0 && sel.Count < len(in) {
		in = in[:sel.Count]
	}
	return in
}

// --- SORT's source and destination ---------------------------------------------

// SortSource reads the collection SORT is to order as a flat list of members, and reports
// which type it was.
//
// The order it returns is the collection's own -- a list's insertion order, a sorted set's
// score order, a set's hash-table order -- because that is what `SORT ... BY nosort`
// answers with. A missing key is an empty result rather than an error, which is what makes
// SORT of a queue that has drained reply with nothing instead of failing.
func (s *Store) SortSource(key string) (elems []string, kind string, err error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return nil, "none", nil
	}
	switch e.kind {
	case kindList:
		out := make([]string, 0, e.list.len())
		for el := e.list.l.Front(); el != nil; el = el.Next() {
			out = append(out, string(el.Value.([]byte)))
		}
		return out, "list", nil
	case kindSet:
		out := make([]string, 0, len(e.set))
		for m := range e.set {
			out = append(out, m)
		}
		return out, "set", nil
	case kindZSet:
		out := make([]string, 0, e.zset.sl.length)
		for node := e.zset.sl.head.next[0].forward; node != nil; node = node.next[0].forward {
			out = append(out, node.member)
		}
		return out, "zset", nil
	}
	return nil, "", ErrWrongType
}

// ReplaceList overwrites key with exactly these elements as a list, returning its new
// length; an empty list deletes the key. It is what SORT ... STORE needs, and it replaces
// rather than appending because SORT's destination is the result of the sort and not a
// queue being added to.
func (s *Store) ReplaceList(key string, vals [][]byte) (int, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if len(vals) == 0 {
		delete(sh.data, key)
		return 0, nil
	}
	d := newDeque()
	for _, v := range vals {
		d.rpush(copyBytes(v))
	}
	e := &entry{kind: kindList, list: d}
	s.touch(e, now)
	sh.data[key] = e
	return d.len(), nil
}
