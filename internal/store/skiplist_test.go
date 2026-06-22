package store

import (
	"math/rand"
	"sort"
	"testing"
)

func newTestSkipList() *skipList {
	return newSkipList(rand.New(rand.NewSource(1)))
}

func TestSkipListInsertRankOrder(t *testing.T) {
	sl := newTestSkipList()
	pairs := map[string]float64{
		"a": 3, "b": 1, "c": 2, "d": 5, "e": 4,
	}
	for m, s := range pairs {
		sl.insert(m, s)
	}
	if sl.length != len(pairs) {
		t.Fatalf("length = %d; want %d", sl.length, len(pairs))
	}

	// Expected order by score: b(1) c(2) a(3) e(4) d(5)
	want := []string{"b", "c", "a", "e", "d"}
	for i, m := range want {
		n := sl.nodeByRank(i)
		if n == nil || n.member != m {
			t.Fatalf("rank %d = %v; want %q", i, n, m)
		}
		if r := sl.rank(m, pairs[m]); r != i {
			t.Fatalf("rank(%q) = %d; want %d", m, r, i)
		}
	}
}

func TestSkipListDelete(t *testing.T) {
	sl := newTestSkipList()
	sl.insert("a", 1)
	sl.insert("b", 2)
	sl.insert("c", 3)

	if !sl.delete("b", 2) {
		t.Fatal("delete(b) returned false")
	}
	if sl.delete("b", 2) {
		t.Fatal("second delete(b) returned true")
	}
	if sl.length != 2 {
		t.Fatalf("length = %d; want 2", sl.length)
	}
	if sl.rank("c", 3) != 1 {
		t.Fatalf("rank(c) after delete = %d; want 1", sl.rank("c", 3))
	}
	if n := sl.nodeByRank(0); n.member != "a" {
		t.Fatalf("rank 0 = %q; want a", n.member)
	}
}

func TestSkipListTiesBrokenByMember(t *testing.T) {
	sl := newTestSkipList()
	// Same score: order must fall back to member string.
	for _, m := range []string{"banana", "apple", "cherry"} {
		sl.insert(m, 1.0)
	}
	want := []string{"apple", "banana", "cherry"}
	for i, m := range want {
		if n := sl.nodeByRank(i); n.member != m {
			t.Fatalf("rank %d = %q; want %q", i, n.member, m)
		}
	}
}

// TestSkipListFuzzAgainstSort cross-checks the skip list's ordering and rank
// queries against a trivially-correct sorted slice over many random operations.
func TestSkipListFuzzAgainstSort(t *testing.T) {
	sl := newTestSkipList()
	rng := rand.New(rand.NewSource(42))
	scores := map[string]float64{}

	for i := 0; i < 4000; i++ {
		m := string(rune('a' + rng.Intn(26)))
		if old, ok := scores[m]; ok {
			sl.delete(m, old)
			delete(scores, m)
		}
		s := float64(rng.Intn(50))
		sl.insert(m, s)
		scores[m] = s
	}

	type pair struct {
		m string
		s float64
	}
	var want []pair
	for m, s := range scores {
		want = append(want, pair{m, s})
	}
	sort.Slice(want, func(i, j int) bool {
		return less(want[i].s, want[i].m, want[j].s, want[j].m)
	})

	if sl.length != len(want) {
		t.Fatalf("length = %d; want %d", sl.length, len(want))
	}
	for i, p := range want {
		n := sl.nodeByRank(i)
		if n == nil || n.member != p.m || n.score != p.s {
			t.Fatalf("rank %d = %v; want %+v", i, n, p)
		}
		if r := sl.rank(p.m, p.s); r != i {
			t.Fatalf("rank(%q,%v) = %d; want %d", p.m, p.s, r, i)
		}
	}
}
