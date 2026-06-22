package store

import (
	"strconv"
	"testing"
)

func TestScanFullIteration(t *testing.T) {
	s := New(16)
	want := map[string]bool{}
	for i := 0; i < 200; i++ {
		k := "k" + strconv.Itoa(i)
		s.Set(k, []byte("v"), 0)
		want[k] = true
	}

	seen := map[string]bool{}
	var cursor uint64
	iterations := 0
	for {
		keys, next := s.Scan(cursor, 7)
		for _, k := range keys {
			seen[k] = true
		}
		cursor = next
		if iterations++; iterations > 10000 {
			t.Fatal("SCAN did not terminate")
		}
		if cursor == 0 {
			break
		}
	}

	if len(seen) != len(want) {
		t.Fatalf("SCAN saw %d distinct keys; want %d", len(seen), len(want))
	}
	for k := range want {
		if !seen[k] {
			t.Fatalf("SCAN missed key %q", k)
		}
	}
}

func TestScanEmptyAndOutOfRange(t *testing.T) {
	s := New(4)
	if keys, next := s.Scan(0, 10); len(keys) != 0 || next != 0 {
		t.Fatalf("Scan of empty store = %v,%d; want [],0", keys, next)
	}
	if keys, next := s.Scan(99999, 10); len(keys) != 0 || next != 0 {
		t.Fatalf("Scan with out-of-range cursor = %v,%d; want [],0", keys, next)
	}
}
