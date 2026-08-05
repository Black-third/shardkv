package store

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

// keysOnDifferentShards returns two keys with the given prefix that hash to
// different shards, so a two-key operation on them exercises the two-lock path
// rather than the trivial same-shard one.
func keysOnDifferentShards(s *Store, prefix string) (string, string) {
	first := prefix + "0"
	for i := 1; i < 1000; i++ {
		cand := prefix + strconv.Itoa(i)
		if s.getShard(first) != s.getShard(cand) {
			return first, cand
		}
	}
	panic("no two keys with prefix " + prefix + " land on different shards")
}

// TestCrossShardWritesCannotDeadlock hammers every two-key write from many
// goroutines, half of them naming the keys in one order and half in the other.
//
// Locking in argument order would deadlock here almost immediately: one goroutine
// holds shard A waiting for B while another holds B waiting for A. Locking in shard
// *index* order cannot, whatever order the caller named the keys in. Run with -race,
// which also catches any access that escaped the locks.
func TestCrossShardWritesCannotDeadlock(t *testing.T) {
	s := New(16)
	strA, strB := keysOnDifferentShards(s, "str")
	setA, setB := keysOnDifferentShards(s, "set")
	listA, listB := keysOnDifferentShards(s, "list")
	dstA, dstB := keysOnDifferentShards(s, "dst")

	reset := func() {
		s.Set(strA, []byte("a"), 0)
		s.Set(strB, []byte("b"), 0)
		s.SAdd(setA, "x", "y")
		s.SAdd(setB, "y", "z")
		s.RPush(listA, []byte("x"), []byte("y"))
		s.RPush(listB, []byte("y"), []byte("z"))
	}
	reset()

	const goroutines, iterations = 16, 200
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			// Odd goroutines name every pair the other way round.
			src, dst := strA, strB
			ssrc, sdst := setA, setB
			lsrc, ldst := listA, listB
			out := dstA
			if g%2 == 1 {
				src, dst = dst, src
				ssrc, sdst = sdst, ssrc
				lsrc, ldst = ldst, lsrc
				out = dstB
			}
			for i := 0; i < iterations; i++ {
				s.Copy(src, dst, true)
				s.RenameNX(src, dst)
				s.SMove(ssrc, sdst, "x")
				s.SMove(sdst, ssrc, "z")
				s.LMove(lsrc, ldst, true, false)
				s.LMove(ldst, lsrc, false, true)
				s.SCombineStore(SetUnion, out, ssrc, sdst)
				s.SCombineStore(SetInter, out, sdst, ssrc)
				s.SCombine(SetDiff, 0, ssrc, sdst)
				if i%50 == 0 {
					reset() // keep the collections from draining away entirely
				}
			}
		}(g)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("two-key writes with swapped arguments deadlocked: the shards are not being locked in index order")
	}

	// Nothing may have leaked out of the sets the moves shuffle members between.
	for _, key := range []string{setA, setB} {
		members, err := s.SMembers(key)
		if err != nil {
			t.Fatalf("SMembers(%q): %v", key, err)
		}
		for _, m := range members {
			if m != "x" && m != "y" && m != "z" {
				t.Errorf("set %q holds unexpected member %q", key, m)
			}
		}
	}
}

// TestCopyIsADeepCopy pins that a copied key shares nothing mutable with its source:
// a shallow copy would make a later push or field write visible through both keys.
func TestCopyIsADeepCopy(t *testing.T) {
	s := New(8)
	s.Set("str", []byte("v"), 0)
	s.RPush("list", []byte("a"))
	s.HSet("hash", [2][]byte{[]byte("f"), []byte("v")})
	s.SAdd("set", "m")
	s.ZAdd("zset", "m", 1)

	for _, key := range []string{"str", "list", "hash", "set", "zset"} {
		if ok, err := s.Copy(key, key+"-copy", false); !ok || err != nil {
			t.Fatalf("Copy(%q) = %v, %v", key, ok, err)
		}
	}

	s.Append("str", []byte("!"))
	s.RPush("list", []byte("b"))
	s.HSet("hash", [2][]byte{[]byte("f"), []byte("changed")})
	s.SAdd("set", "another")
	s.ZAdd("zset", "m", 99)

	if v, _ := s.Get("str-copy"); string(v) != "v" {
		t.Errorf("copied string = %q; want v", v)
	}
	if items, _ := s.LRange("list-copy", 0, -1); len(items) != 1 {
		t.Errorf("copied list has %d elements; want 1", len(items))
	}
	if v, _, _ := s.HGet("hash-copy", "f"); string(v) != "v" {
		t.Errorf("copied hash field = %q; want v", v)
	}
	if n, _ := s.SCard("set-copy"); n != 1 {
		t.Errorf("copied set has %d members; want 1", n)
	}
	if score, _, _ := s.ZScore("zset-copy", "m"); score != 1 {
		t.Errorf("copied zset score = %v; want 1", score)
	}

	// The copy carries the TTL, and refuses an existing destination without replace.
	s.Set("vol", []byte("v"), 30*time.Second)
	if ok, _ := s.Copy("vol", "vol-copy", false); !ok {
		t.Fatal("Copy of a volatile key failed")
	}
	if _, hasTTL, ok := s.TTL("vol-copy"); !ok || !hasTTL {
		t.Error("copied key lost its TTL")
	}
	if ok, _ := s.Copy("str", "vol-copy", false); ok {
		t.Error("Copy overwrote an existing destination without replace")
	}
	if ok, _ := s.Copy("str", "vol-copy", true); !ok {
		t.Error("Copy with replace did not overwrite")
	}
	if _, err := s.Copy("str", "str", false); err != ErrSameObject {
		t.Errorf("Copy onto itself = %v; want ErrSameObject", err)
	}
}
