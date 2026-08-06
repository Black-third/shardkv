package store

import (
	"strconv"
	"testing"
	"time"
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

// TestApproxLenCountsStoredEntries pins the cheap size estimate EvictToLimit runs
// on. It reads each shard's map length rather than examining every entry, so it is
// O(shards) instead of O(keys) -- the janitor calls it on every tick -- and it
// therefore still counts a key whose TTL elapsed but which nothing has reclaimed.
func TestApproxLenCountsStoredEntries(t *testing.T) {
	s := New(4)
	cur := time.Unix(1000, 0)
	s.SetClock(func() time.Time { return cur })

	for i := 0; i < 10; i++ {
		s.Set("live"+strconv.Itoa(i), []byte("v"), 0)
	}
	for i := 0; i < 6; i++ {
		s.Set("vol"+strconv.Itoa(i), []byte("v"), 5*time.Second)
	}
	if got := s.approxLen(); got != 16 {
		t.Fatalf("approxLen = %d; want 16", got)
	}

	cur = cur.Add(10 * time.Second) // the 6 volatile keys are now expired
	if got := s.Len(); got != 10 {
		t.Fatalf("Len = %d; want 10 (expired keys are not live)", got)
	}
	if got := s.approxLen(); got != 16 {
		t.Fatalf("approxLen before a sweep = %d; want 16 (entries still stored)", got)
	}
	// The janitor sweeps before it evicts, which is what makes the estimate exact
	// at the point EvictToLimit reads it.
	s.sweep()
	if got := s.approxLen(); got != 10 {
		t.Fatalf("approxLen after a sweep = %d; want 10", got)
	}
}

// BenchmarkEvictToLimitAtCap measures a janitor tick on a store already at its cap
// -- the common case, where no key needs evicting and the whole cost is the size
// estimate. It is the reason that estimate is not an O(keys) scan.
func BenchmarkEvictToLimitAtCap(b *testing.B) {
	s := New(256)
	const keys = 50000
	for i := 0; i < keys; i++ {
		s.Set("key:"+strconv.Itoa(i), []byte("value"), 0)
	}
	s.SetMaxKeys(keys)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.EvictToLimit()
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
