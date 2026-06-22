package store

import "math/rand"

// skipList is an ordered collection of (member, score) pairs, ordered by score
// and then by member lexicographically. It is the index that backs sorted sets
// and provides O(log n) insertion, deletion, and rank queries.
//
// The implementation mirrors the classic Redis zskiplist: each node carries a
// span at every level (the number of nodes it skips over) so that the rank of a
// member can be computed in O(log n) instead of O(n).
//
// A skipList is not safe for concurrent use; callers (the owning shard) hold a
// lock around it.
const (
	slMaxLevel = 32
	slP        = 0.25
)

type slNode struct {
	member string
	score  float64
	next   []slLevel
	prev   *slNode // level-0 backward pointer, for reverse iteration
}

type slLevel struct {
	forward *slNode
	span    int // nodes between this node and forward at this level
}

type skipList struct {
	head   *slNode
	tail   *slNode
	length int
	level  int // current highest level in use (>=1)
	rng    *rand.Rand
}

func newSkipList(rng *rand.Rand) *skipList {
	return &skipList{
		head:  &slNode{next: make([]slLevel, slMaxLevel)},
		level: 1,
		rng:   rng,
	}
}

func (sl *skipList) randomLevel() int {
	lvl := 1
	for lvl < slMaxLevel && sl.rng.Float64() < slP {
		lvl++
	}
	return lvl
}

// less reports whether (scoreA, memberA) sorts before (scoreB, memberB).
func less(scoreA float64, memberA string, scoreB float64, memberB string) bool {
	if scoreA != scoreB {
		return scoreA < scoreB
	}
	return memberA < memberB
}

// insert adds a new (member, score) pair. The caller must ensure member is not
// already present (sorted-set semantics update via delete+insert).
func (sl *skipList) insert(member string, score float64) {
	var update [slMaxLevel]*slNode
	var rank [slMaxLevel]int

	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		if i == sl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for x.next[i].forward != nil &&
			less(x.next[i].forward.score, x.next[i].forward.member, score, member) {
			rank[i] += x.next[i].span
			x = x.next[i].forward
		}
		update[i] = x
	}

	lvl := sl.randomLevel()
	if lvl > sl.level {
		for i := sl.level; i < lvl; i++ {
			rank[i] = 0
			update[i] = sl.head
			update[i].next[i].span = sl.length
		}
		sl.level = lvl
	}

	x = &slNode{member: member, score: score, next: make([]slLevel, lvl)}
	for i := 0; i < lvl; i++ {
		x.next[i].forward = update[i].next[i].forward
		update[i].next[i].forward = x
		x.next[i].span = update[i].next[i].span - (rank[0] - rank[i])
		update[i].next[i].span = (rank[0] - rank[i]) + 1
	}
	for i := lvl; i < sl.level; i++ {
		update[i].next[i].span++
	}

	if update[0] != sl.head {
		x.prev = update[0]
	}
	if x.next[0].forward != nil {
		x.next[0].forward.prev = x
	} else {
		sl.tail = x
	}
	sl.length++
}

// delete removes the node matching (member, score). It reports whether a node
// was removed.
func (sl *skipList) delete(member string, score float64) bool {
	var update [slMaxLevel]*slNode
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for x.next[i].forward != nil &&
			less(x.next[i].forward.score, x.next[i].forward.member, score, member) {
			x = x.next[i].forward
		}
		update[i] = x
	}
	x = x.next[0].forward
	if x == nil || x.score != score || x.member != member {
		return false
	}

	for i := 0; i < sl.level; i++ {
		if update[i].next[i].forward == x {
			update[i].next[i].span += x.next[i].span - 1
			update[i].next[i].forward = x.next[i].forward
		} else {
			update[i].next[i].span--
		}
	}
	if x.next[0].forward != nil {
		x.next[0].forward.prev = x.prev
	} else {
		sl.tail = x.prev
	}
	for sl.level > 1 && sl.head.next[sl.level-1].forward == nil {
		sl.level--
	}
	sl.length--
	return true
}

// rank returns the 0-based rank of (member, score), or -1 if absent.
func (sl *skipList) rank(member string, score float64) int {
	var r int
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for x.next[i].forward != nil &&
			!less(score, member, x.next[i].forward.score, x.next[i].forward.member) {
			r += x.next[i].span
			x = x.next[i].forward
		}
		if x != sl.head && x.member == member {
			return r - 1
		}
	}
	return -1
}

// nodeByRank returns the node at the given 0-based rank, or nil if out of range.
func (sl *skipList) nodeByRank(rank int) *slNode {
	traversed := 0
	x := sl.head
	target := rank + 1 // ranks are 0-based; spans count from 1
	for i := sl.level - 1; i >= 0; i-- {
		for x.next[i].forward != nil && traversed+x.next[i].span <= target {
			traversed += x.next[i].span
			x = x.next[i].forward
		}
		if traversed == target {
			return x
		}
	}
	return nil
}
