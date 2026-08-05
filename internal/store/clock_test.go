package store

import (
	"testing"
	"time"
)

// TestSetClockDrivesNow checks that the injected clock is readable from outside
// the package. The server resolves its own expire deadlines and its propagation
// rewrites through Now, so both come from this one clock rather than one of them
// reading time.Now directly.
func TestSetClockDrivesNow(t *testing.T) {
	s := New(4)
	cur := time.Unix(1000, 0)
	s.SetClock(func() time.Time { return cur })

	if got := s.Now(); !got.Equal(cur) {
		t.Fatalf("Now = %v; want the injected %v", got, cur)
	}
	cur = cur.Add(time.Hour)
	if got := s.Now(); !got.Equal(cur) {
		t.Fatalf("Now after advancing the clock = %v; want %v", got, cur)
	}

	// And the injected clock is the one TTL logic uses, not the wall clock.
	s.Set("k", []byte("v"), 30*time.Second)
	d, hasTTL, ok := s.TTL("k")
	if !ok || !hasTTL || d != 30*time.Second {
		t.Fatalf("TTL = %v,%v,%v; want 30s,true,true", d, hasTTL, ok)
	}
}

// TestNowDefaultsToWallClock confirms production behavior is unchanged: a store
// nobody injected a clock into reads real time.
func TestNowDefaultsToWallClock(t *testing.T) {
	s := New(4)
	before := time.Now()
	got := s.Now()
	after := time.Now()
	if got.Before(before) || got.After(after) {
		t.Fatalf("Now = %v; want a reading between %v and %v", got, before, after)
	}
}

// TestGetStringSeparatesMissFromWrongType covers the single-lock typed read that
// replaced GET's Type-then-Get pair: one shard-lock acquisition has to answer both
// "is it there" and "is it a string".
func TestGetStringSeparatesMissFromWrongType(t *testing.T) {
	s := New(4)
	s.Set("str", []byte("v"), 0)
	s.RPush("list", []byte("x"))

	if v, ok, err := s.GetString("str"); err != nil || !ok || string(v) != "v" {
		t.Fatalf("GetString(str) = %q,%v,%v; want \"v\",true,nil", v, ok, err)
	}
	if v, ok, err := s.GetString("missing"); err != nil || ok || v != nil {
		t.Fatalf("GetString(missing) = %q,%v,%v; want nil,false,nil", v, ok, err)
	}
	if _, ok, err := s.GetString("list"); err != ErrWrongType || ok {
		t.Fatalf("GetString(list) = _,%v,%v; want false,ErrWrongType", ok, err)
	}
	// Get keeps reporting a wrong-type key as a plain miss, which is what MGET wants.
	if _, ok := s.Get("list"); ok {
		t.Error("Get(list) reported ok; want a miss")
	}
}

// TestGetStringExpiresLazily keeps the lazy-expiration side effect intact: reading
// an expired key drops it and reports the removal.
func TestGetStringExpiresLazily(t *testing.T) {
	s := New(4)
	cur := time.Unix(1000, 0)
	s.SetClock(func() time.Time { return cur })

	var removed []string
	s.SetRemovalHook(func(k string, _ bool) { removed = append(removed, k) })

	s.Set("k", []byte("v"), 5*time.Second)
	cur = cur.Add(10 * time.Second)

	if _, ok, err := s.GetString("k"); ok || err != nil {
		t.Fatalf("GetString of an expired key = _,%v,%v; want false,nil", ok, err)
	}
	if len(removed) != 1 || removed[0] != "k" {
		t.Fatalf("lazy removal hook = %v; want [k]", removed)
	}
}
