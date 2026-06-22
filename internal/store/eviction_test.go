package store

import (
	"strconv"
	"testing"
)

func TestEvictionEnforcesCap(t *testing.T) {
	s := New(256) // default-like shard count; cap is well below it
	s.SetMaxKeys(8)

	for i := 0; i < 200; i++ {
		s.Set("k"+strconv.Itoa(i), []byte("v"), 0)
	}
	s.EvictToLimit() // normally driven by the janitor

	if got := s.Len(); got != 8 {
		t.Fatalf("Len = %d; want 8 (global cap enforced)", got)
	}
	if s.Evicted() == 0 {
		t.Fatal("Evicted = 0; want > 0 after inserting 200 keys into an 8-key store")
	}
}

func TestEvictionPicksLRU(t *testing.T) {
	s := New(1)     // single shard so keys collide deterministically
	s.SetMaxKeys(3) // perShardCap = 3

	s.Set("a", []byte("1"), 0)
	s.Set("b", []byte("2"), 0)
	s.Set("c", []byte("3"), 0)

	// Make "a" the least-recently-used, "c" the most-recently-used.
	sh := s.shards[0]
	sh.data["a"].atime.Store(10)
	sh.data["b"].atime.Store(20)
	sh.data["c"].atime.Store(30)

	// A 4th key exceeds the cap; eviction must remove the LRU key ("a").
	s.Set("d", []byte("4"), 0)
	s.EvictToLimit()

	if s.Exists("a") {
		t.Fatal("a was least-recently-used and should have been evicted")
	}
	if !s.Exists("d") {
		t.Fatal("d (just inserted) should exist")
	}
	if !s.Exists("c") {
		t.Fatal("c (most-recently-used) should survive")
	}
	if got := s.Len(); got != 3 {
		t.Fatalf("Len = %d; want 3", got)
	}
	if s.Evicted() != 1 {
		t.Fatalf("Evicted = %d; want 1", s.Evicted())
	}
}

// TestNoEvictionWhenUnbounded confirms the default (no cap) keeps everything.
func TestNoEvictionWhenUnbounded(t *testing.T) {
	s := New(4)
	for i := 0; i < 100; i++ {
		s.Set("k"+strconv.Itoa(i), []byte("v"), 0)
	}
	if got := s.Len(); got != 100 {
		t.Fatalf("Len = %d; want 100 (no cap set)", got)
	}
	if s.Evicted() != 0 {
		t.Fatalf("Evicted = %d; want 0", s.Evicted())
	}
}
