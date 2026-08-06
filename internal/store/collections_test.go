package store

import (
	"sort"
	"strconv"
	"strings"
	"testing"
)

// TestListIndexWalksFromTheNearerEnd pins deque.at over a list long enough that the
// two directions differ: an index in the second half is reached by walking back from
// the tail, so an off-by-one there would only show up away from the head.
func TestListIndexWalksFromTheNearerEnd(t *testing.T) {
	s := New(4)
	const n = 101
	for i := 0; i < n; i++ {
		if _, err := s.RPush("l", []byte(strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < n; i++ {
		v, ok, err := s.LIndex("l", i)
		if err != nil || !ok || string(v) != strconv.Itoa(i) {
			t.Fatalf("LIndex(%d) = %q, %v, %v", i, v, ok, err)
		}
		// The same element from the other direction.
		v, ok, err = s.LIndex("l", i-n)
		if err != nil || !ok || string(v) != strconv.Itoa(i) {
			t.Fatalf("LIndex(%d) = %q, %v, %v; want element %d", i-n, v, ok, err, i)
		}
	}
	for _, bad := range []int{n, n + 1, -n - 1} {
		if _, ok, _ := s.LIndex("l", bad); ok {
			t.Errorf("LIndex(%d) found an element outside the list", bad)
		}
	}

	// LSet through the same walk, at both ends and in the middle.
	for _, i := range []int{0, n / 2, n - 1, -1, -n} {
		if err := s.LSet("l", i, []byte("set")); err != nil {
			t.Fatalf("LSet(%d): %v", i, err)
		}
		v, _, _ := s.LIndex("l", i)
		if string(v) != "set" {
			t.Errorf("LSet(%d) wrote to the wrong element: %q", i, v)
		}
	}
	if err := s.LSet("l", n, []byte("x")); err != ErrIndexOutOfRange {
		t.Errorf("LSet past the end = %v; want ErrIndexOutOfRange", err)
	}
	if err := s.LSet("ghost", 0, []byte("x")); err != ErrNoSuchKey {
		t.Errorf("LSet on a missing key = %v; want ErrNoSuchKey", err)
	}
}

// TestLTrimBoundaries covers the trim arithmetic across the whole range of index
// pairs on a small list, cross-checked against a plain slice.
func TestLTrimBoundaries(t *testing.T) {
	elems := []string{"a", "b", "c", "d", "e"}
	cases := []struct {
		start, stop int
		want        string
	}{
		{0, -1, "a,b,c,d,e"},
		{0, 0, "a"},
		{1, 3, "b,c,d"},
		{-2, -1, "d,e"},
		{-100, 100, "a,b,c,d,e"},
		{2, 100, "c,d,e"},
		{3, 1, ""},
		{5, 10, ""},
		{-1, -2, ""},
	}
	for _, tc := range cases {
		s := New(4)
		for _, e := range elems {
			s.RPush("l", []byte(e))
		}
		if _, err := s.LTrim("l", tc.start, tc.stop); err != nil {
			t.Fatalf("LTrim(%d, %d): %v", tc.start, tc.stop, err)
		}
		got, _ := s.LRange("l", 0, -1)
		if join(got) != tc.want {
			t.Errorf("LTrim(%d, %d) left %q; want %q", tc.start, tc.stop, join(got), tc.want)
		}
		// An emptied list takes its key with it.
		if tc.want == "" && s.Exists("l") {
			t.Errorf("LTrim(%d, %d) left an empty list behind", tc.start, tc.stop)
		}
	}
}

// TestPopCountsCapAtCollectionSize covers the count forms of the pops when the count
// exceeds what is there: they return everything and delete the key, never padding or
// blocking.
func TestPopCountsCapAtCollectionSize(t *testing.T) {
	s := New(8)
	s.RPush("l", []byte("a"), []byte("b"))
	s.SAdd("s", "a", "b")
	s.ZAdd("z", "a", 1)
	s.ZAdd("z", "b", 2)

	if vals, ok, _ := s.LPopCount("l", 10, true); !ok || len(vals) != 2 {
		t.Errorf("LPopCount(10) = %d elements, ok %v; want 2", len(vals), ok)
	}
	if s.Exists("l") {
		t.Error("drained list still exists")
	}
	if _, ok, _ := s.LPopCount("l", 1, true); ok {
		t.Error("LPopCount reported a missing key as present")
	}

	members, ok, _ := s.SPop("s", 10)
	if !ok || len(members) != 2 {
		t.Errorf("SPop(10) = %d members, ok %v; want 2", len(members), ok)
	}
	if s.Exists("s") {
		t.Error("drained set still exists")
	}
	if _, ok, _ := s.SPop("s", 1); ok {
		t.Error("SPop reported a missing key as present")
	}

	popped, _ := s.ZPop("z", 10, true)
	if len(popped) != 2 || popped[0].Member != "a" || popped[1].Member != "b" {
		t.Errorf("ZPop(min, 10) = %v; want a then b", popped)
	}
	if s.Exists("z") {
		t.Error("drained sorted set still exists")
	}
	if got, _ := s.ZPop("z", 1, true); len(got) != 0 {
		t.Errorf("ZPop on a missing key = %v; want nothing", got)
	}
}

// TestSCombineIgnoresKeyRepetition checks the combinators against a key named twice,
// where an intersection is the set itself and a difference is empty.
func TestSCombineIgnoresKeyRepetition(t *testing.T) {
	s := New(8)
	s.SAdd("a", "x", "y")

	cases := []struct {
		op   SetOp
		keys []string
		want string
	}{
		{SetInter, []string{"a", "a"}, "x,y"},
		{SetUnion, []string{"a", "a"}, "x,y"},
		{SetDiff, []string{"a", "a"}, ""},
		{SetInter, []string{"a"}, "x,y"},
	}
	for _, tc := range cases {
		got, err := s.SCombine(tc.op, 0, tc.keys...)
		if err != nil {
			t.Fatalf("SCombine(%v, %v): %v", tc.op, tc.keys, err)
		}
		sort.Strings(got)
		if strings.Join(got, ",") != tc.want {
			t.Errorf("SCombine(%v, %v) = %v; want %q", tc.op, tc.keys, got, tc.want)
		}
	}

	// A store into one of its own sources reads the source before overwriting it.
	if n, changed, err := s.SCombineStore(SetInter, "a", "a", "a"); err != nil || n != 2 || !changed {
		t.Errorf("SCombineStore into its own source = %d, %v, %v; want 2, true, nil", n, changed, err)
	}
	if n, _ := s.SCard("a"); n != 2 {
		t.Errorf("set after storing into itself has %d members; want 2", n)
	}
	// An empty result deletes the destination rather than storing an empty set, and
	// only counts as a change when there was something there to delete.
	s.SAdd("b", "z")
	if n, changed, _ := s.SCombineStore(SetInter, "dst", "a", "b"); n != 0 || changed {
		t.Errorf("empty intersection over a missing destination = %d, changed %v; want 0, false", n, changed)
	}
	if s.Exists("dst") {
		t.Error("an empty result left an empty set behind")
	}
	s.SAdd("dst", "leftover")
	if _, changed, _ := s.SCombineStore(SetInter, "dst", "a", "b"); !changed {
		t.Error("an empty intersection that deleted the destination reported no change")
	}
	if s.Exists("dst") {
		t.Error("the destination survived an empty result")
	}
}

// TestZRangeByScoreLimit covers the score range with its LIMIT clause in both
// directions, which is where the offset is applied *after* the reversal.
func TestZRangeByScoreLimit(t *testing.T) {
	s := New(4)
	for i, m := range []string{"a", "b", "c", "d"} {
		if _, _, err := s.ZAdd("z", m, float64(i+1)); err != nil {
			t.Fatal(err)
		}
	}
	all := ScoreRange{Min: 1, Max: 4}

	cases := []struct {
		offset, count int
		rev           bool
		want          string
	}{
		{0, -1, false, "a,b,c,d"},
		{1, -1, false, "b,c,d"},
		{1, 2, false, "b,c"},
		{0, 0, false, ""},
		{10, -1, false, ""},
		{-1, -1, false, ""},
		{0, -1, true, "d,c,b,a"},
		{1, 2, true, "c,b"},
	}
	for _, tc := range cases {
		got, err := s.ZRangeByScore("z", all, tc.offset, tc.count, tc.rev)
		if err != nil {
			t.Fatalf("ZRangeByScore: %v", err)
		}
		names := make([]string, 0, len(got))
		for _, m := range got {
			names = append(names, m.Member)
		}
		if strings.Join(names, ",") != tc.want {
			t.Errorf("ZRangeByScore(offset %d, count %d, rev %v) = %v; want %q",
				tc.offset, tc.count, tc.rev, names, tc.want)
		}
	}

	// Exclusive bounds, and a range no member reaches.
	if got, _ := s.ZRangeByScore("z", ScoreRange{Min: 1, MinExcl: true, Max: 4, MaxExcl: true}, 0, -1, false); len(got) != 2 {
		t.Errorf("exclusive range returned %d members; want 2", len(got))
	}
	if got, _ := s.ZRangeByScore("z", ScoreRange{Min: 10, Max: 20}, 0, -1, false); len(got) != 0 {
		t.Errorf("range above every score returned %v", got)
	}
	if got, _ := s.ZRangeByScore("ghost", all, 0, -1, false); len(got) != 0 {
		t.Errorf("range over a missing key returned %v", got)
	}
}

// TestFloatIncrementsRejectAnUnderscoredValue covers the stored-value half of the float
// parse. A client can plant any string with SET or HSET, so the text these two read back
// is not necessarily text this server wrote -- and Go's float grammar accepts a spelling
// Redis refuses.
//
// Measured on redis:7.2-alpine: `SET a 1_0` then `INCRBYFLOAT a 1` answers
// "ERR value is not a valid float", and `HSET h f 1_0` then `HINCRBYFLOAT h f 1` answers
// "ERR hash value is not a float". Before ParseDouble both answered 11 here: the
// underscore was silently dropped, the key was overwritten with a number a hundred times
// too small, and nothing reported it.
func TestFloatIncrementsRejectAnUnderscoredValue(t *testing.T) {
	s := New(4)

	for _, planted := range []string{"1_0", "1_000.5", "1_", "_1"} {
		s.Set("k", []byte(planted), 0)
		if _, _, err := s.IncrByFloat("k", 1); err != ErrNotFloat {
			t.Errorf("IncrByFloat over a stored %q: err = %v; want ErrNotFloat", planted, err)
		}
		// The refusal must leave the value alone rather than half-applying.
		if got, _, _ := s.GetString("k"); string(got) != planted {
			t.Errorf("a refused IncrByFloat over %q left %q", planted, got)
		}

		if _, err := s.HSet("h", [2][]byte{[]byte("f"), []byte(planted)}); err != nil {
			t.Fatalf("HSet: %v", err)
		}
		if _, _, err := s.HIncrByFloat("h", "f", 1); err != ErrHashNotFloat {
			t.Errorf("HIncrByFloat over a stored %q: err = %v; want ErrHashNotFloat", planted, err)
		}
		if got, _, _ := s.HGet("h", "f"); string(got) != planted {
			t.Errorf("a refused HIncrByFloat over %q left %q", planted, got)
		}
	}

	// The spellings Redis does accept still work, so the check rejects underscores rather
	// than anything that merely looks unusual.
	for _, planted := range []string{"5", "+5", ".5", "5.", "1e1"} {
		s.Set("ok", []byte(planted), 0)
		if _, _, err := s.IncrByFloat("ok", 0); err != nil {
			t.Errorf("IncrByFloat over a stored %q: %v", planted, err)
		}
	}
}
