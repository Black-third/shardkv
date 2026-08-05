package store

import (
	"strconv"
	"testing"
	"time"
)

func TestRemovalHookOnJanitorExpiry(t *testing.T) {
	s := New(4)
	cur := time.Unix(1000, 0)
	s.clock = func() time.Time { return cur }

	var removed []string
	s.SetRemovalHook(func(k string, _ bool) { removed = append(removed, k) })

	s.Set("k", []byte("v"), 5*time.Second)
	cur = cur.Add(10 * time.Second) // k is now expired
	s.sweep()

	if len(removed) != 1 || removed[0] != "k" {
		t.Fatalf("janitor removal hook = %v; want [k]", removed)
	}
}

func TestRemovalHookOnLazyExpiry(t *testing.T) {
	s := New(4)
	cur := time.Unix(0, 0)
	s.clock = func() time.Time { return cur }

	var removed []string
	s.SetRemovalHook(func(k string, _ bool) { removed = append(removed, k) })

	s.Set("k", []byte("v"), 5*time.Second)
	cur = cur.Add(10 * time.Second)
	s.Get("k") // lazy expiration path must also fire the hook

	if len(removed) != 1 || removed[0] != "k" {
		t.Fatalf("lazy removal hook = %v; want [k]", removed)
	}
}

// TestRemovalHookOnDelOfExpiredKey covers the one path that used to remove an
// entry silently: DEL on a key whose TTL had already elapsed dropped it from the
// shard and reported "nothing live removed", which meant no read or sweep could
// ever report the expiration afterwards -- so a WATCHer of that key never learned
// it expired and a pending EXEC committed against a key that was already gone.
func TestRemovalHookOnDelOfExpiredKey(t *testing.T) {
	s := New(4)
	cur := time.Unix(1000, 0)
	s.clock = func() time.Time { return cur }

	var removed []string
	var evictedFlags []bool
	s.SetRemovalHook(func(k string, evicted bool) {
		removed = append(removed, k)
		evictedFlags = append(evictedFlags, evicted)
	})

	s.Set("k", []byte("v"), 5*time.Second)
	cur = cur.Add(10 * time.Second) // k is expired but still present

	if s.Del("k") {
		t.Fatal("Del of an already-expired key reported a live removal")
	}
	if len(removed) != 1 || removed[0] != "k" {
		t.Fatalf("removal hook after DEL of expired key = %v; want [k]", removed)
	}
	if evictedFlags[0] {
		t.Error("DEL of an expired key reported an eviction; want an expiration")
	}
	// The entry is gone, so a later sweep must not report it a second time.
	removed = nil
	s.sweep()
	if len(removed) != 0 {
		t.Errorf("sweep re-reported an already-removed key: %v", removed)
	}
}

// TestRemovalHookNotFiredForLiveDel confirms the hook stays reserved for removals
// the store makes on its own: a DEL of a live key is the client's own write, and
// dispatch already invalidates WATCHers and propagates it.
func TestRemovalHookNotFiredForLiveDel(t *testing.T) {
	s := New(4)
	var removed []string
	s.SetRemovalHook(func(k string, _ bool) { removed = append(removed, k) })

	s.Set("k", []byte("v"), 0)
	if !s.Del("k") {
		t.Fatal("Del of a live key reported no removal")
	}
	if len(removed) != 0 {
		t.Fatalf("removal hook fired for a client DEL: %v", removed)
	}
}

func TestRemovalHookOnEviction(t *testing.T) {
	s := New(1)
	s.SetMaxKeys(2)
	var removed []string
	s.SetRemovalHook(func(k string, _ bool) { removed = append(removed, k) })

	for i := 0; i < 5; i++ {
		s.Set("k"+strconv.Itoa(i), []byte("v"), 0)
	}
	s.EvictToLimit()

	if len(removed) == 0 {
		t.Fatal("eviction did not fire the removal hook")
	}
	if got := s.Len() + 0; got != 2 {
		t.Fatalf("Len after eviction = %d; want 2", got)
	}
}
