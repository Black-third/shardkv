package store

import (
	"sort"
	"strconv"
	"sync"
	"testing"
)

func TestListOps(t *testing.T) {
	s := New(8)

	if n, _ := s.RPush("l", []byte("b"), []byte("c")); n != 2 {
		t.Fatalf("RPush len = %d; want 2", n)
	}
	if n, _ := s.LPush("l", []byte("a")); n != 3 {
		t.Fatalf("LPush len = %d; want 3", n)
	}
	// list is now a, b, c
	got, _ := s.LRange("l", 0, -1)
	if join(got) != "a,b,c" {
		t.Fatalf("LRange = %q; want a,b,c", join(got))
	}
	if v, ok, _ := s.LPop("l"); !ok || string(v) != "a" {
		t.Fatalf("LPop = %q,%v; want a", v, ok)
	}
	if v, ok, _ := s.RPop("l"); !ok || string(v) != "c" {
		t.Fatalf("RPop = %q,%v; want c", v, ok)
	}
	if n, _ := s.LLen("l"); n != 1 {
		t.Fatalf("LLen = %d; want 1", n)
	}
	// Drain to empty -> key removed.
	s.LPop("l")
	if s.Exists("l") {
		t.Fatal("emptied list should be deleted")
	}
}

func TestHashOps(t *testing.T) {
	s := New(8)
	if n, _ := s.HSet("h", [2][]byte{[]byte("f1"), []byte("v1")}, [2][]byte{[]byte("f2"), []byte("v2")}); n != 2 {
		t.Fatalf("HSet created = %d; want 2", n)
	}
	if n, _ := s.HSet("h", [2][]byte{[]byte("f1"), []byte("v1b")}); n != 0 {
		t.Fatalf("HSet update created = %d; want 0", n)
	}
	if v, ok, _ := s.HGet("h", "f1"); !ok || string(v) != "v1b" {
		t.Fatalf("HGet f1 = %q,%v; want v1b", v, ok)
	}
	if n, _ := s.HLen("h"); n != 2 {
		t.Fatalf("HLen = %d; want 2", n)
	}
	if n, _ := s.HDel("h", "f1", "nope"); n != 1 {
		t.Fatalf("HDel = %d; want 1", n)
	}
}

func TestSetOps(t *testing.T) {
	s := New(8)
	if n, _ := s.SAdd("s", "a", "b", "a"); n != 2 {
		t.Fatalf("SAdd = %d; want 2", n)
	}
	if ok, _ := s.SIsMember("s", "b"); !ok {
		t.Fatal("SIsMember b = false; want true")
	}
	if n, _ := s.SCard("s"); n != 2 {
		t.Fatalf("SCard = %d; want 2", n)
	}
	m, _ := s.SMembers("s")
	sort.Strings(m)
	if join2(m) != "a,b" {
		t.Fatalf("SMembers = %q; want a,b", join2(m))
	}
	if n, _ := s.SRem("s", "a"); n != 1 {
		t.Fatalf("SRem = %d; want 1", n)
	}
}

func TestZSetOps(t *testing.T) {
	s := New(8)
	s.ZAdd("z", "a", 3)
	s.ZAdd("z", "b", 1)
	s.ZAdd("z", "c", 2)
	if n, _ := s.ZCard("z"); n != 3 {
		t.Fatalf("ZCard = %d; want 3", n)
	}
	// Ascending by score: b(1) c(2) a(3)
	r, _ := s.ZRange("z", 0, -1)
	if zjoin(r) != "b,c,a" {
		t.Fatalf("ZRange = %q; want b,c,a", zjoin(r))
	}
	if rank, ok, _ := s.ZRank("z", "a"); !ok || rank != 2 {
		t.Fatalf("ZRank a = %d,%v; want 2", rank, ok)
	}
	// Update score moves rank: a -> 0
	s.ZAdd("z", "a", 0)
	if rank, _, _ := s.ZRank("z", "a"); rank != 0 {
		t.Fatalf("ZRank a after update = %d; want 0", rank)
	}
	if sc, ok, _ := s.ZScore("z", "a"); !ok || sc != 0 {
		t.Fatalf("ZScore a = %v,%v; want 0", sc, ok)
	}
}

func TestWrongType(t *testing.T) {
	s := New(8)
	s.Set("k", []byte("v"), 0)
	if _, err := s.LPush("k", []byte("x")); err != ErrWrongType {
		t.Fatalf("LPush on string = %v; want ErrWrongType", err)
	}
	if _, err := s.SAdd("k", "x"); err != ErrWrongType {
		t.Fatalf("SAdd on string = %v; want ErrWrongType", err)
	}
	if _, err := s.HSet("k", [2][]byte{[]byte("f"), []byte("v")}); err != ErrWrongType {
		t.Fatalf("HSet on string = %v; want ErrWrongType", err)
	}
	if _, _, err := s.ZAdd("k", "m", 1); err != ErrWrongType {
		t.Fatalf("ZAdd on string = %v; want ErrWrongType", err)
	}
	// And the reverse: GET on a list returns not-ok.
	s.RPush("l", []byte("x"))
	if _, ok := s.Get("l"); ok {
		t.Fatal("Get on list should be not-ok")
	}
	if typ, _ := s.Type("l"); typ != "list" {
		t.Fatalf("Type(l) = %q; want list", typ)
	}
}

// TestConcurrentZSet hammers a sorted set from many goroutines; correctness is
// checked via final cardinality. Run with -race.
func TestConcurrentZSet(t *testing.T) {
	s := New(16)
	const goroutines, perG = 16, 500
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				m := "m" + strconv.Itoa(g*perG+i)
				s.ZAdd("z", m, float64(i))
			}
		}(g)
	}
	wg.Wait()
	if n, _ := s.ZCard("z"); n != goroutines*perG {
		t.Fatalf("ZCard = %d; want %d", n, goroutines*perG)
	}
}

func join(b [][]byte) string {
	parts := make([]string, len(b))
	for i, x := range b {
		parts[i] = string(x)
	}
	return joinStrings(parts)
}

func join2(s []string) string { return joinStrings(s) }

func zjoin(zs []ZMember) string {
	parts := make([]string, len(zs))
	for i, z := range zs {
		parts[i] = z.Member
	}
	return joinStrings(parts)
}

func joinStrings(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += ","
		}
		out += p
	}
	return out
}
