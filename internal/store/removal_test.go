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
