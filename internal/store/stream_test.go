package store

import (
	"errors"
	"testing"
)

// TestStreamNextIDCarriesIntoTheMillisecond covers the auto-generated id at the two edges
// of the counter.
//
// A full sequence counter is not exhaustion: it carries into the next millisecond, and only
// an id at the maximum in *both* halves has nowhere left to go. Treating a full counter as
// exhaustion refused every later `XADD *` on a stream whose last id named a future
// millisecond -- which Redis's own suite does deliberately, with a timestamp in 2051, and
// which is how this was found.
func TestStreamNextIDCarriesIntoTheMillisecond(t *testing.T) {
	fields := [][]byte{[]byte("f"), []byte("v")}

	s := New(4)
	// A millisecond far in the future, so the clock cannot overtake it, with the sequence
	// counter already at its maximum.
	const future = "2577343934890-18446744073709551615"
	id, ok := ParseStreamID(future, 0)
	if !ok {
		t.Fatalf("ParseStreamID(%q) failed", future)
	}
	if _, _, _, err := s.XAdd("x", XAddOptions{ID: id}, fields); err != nil {
		t.Fatalf("XADD with an explicit future id: %v", err)
	}
	got, _, _, err := s.XAdd("x", XAddOptions{Auto: true}, fields)
	if err != nil {
		t.Fatalf("XADD * after a full sequence counter: %v; want it to carry into the next millisecond", err)
	}
	if want := "2577343934891-0"; got.String() != want {
		t.Errorf("XADD * = %s; want %s", got, want)
	}

	// The genuinely last possible id, where both halves are at the maximum, is exhaustion.
	s2 := New(4)
	last, ok := ParseStreamID("18446744073709551615-18446744073709551615", 0)
	if !ok {
		t.Fatal("ParseStreamID of the last possible id failed")
	}
	if _, _, _, err := s2.XAdd("x", XAddOptions{ID: last}, fields); err != nil {
		t.Fatalf("XADD of the last possible id: %v", err)
	}
	if _, _, _, err := s2.XAdd("x", XAddOptions{Auto: true}, fields); !errors.Is(err, ErrStreamIDExhausted) {
		t.Errorf("XADD * past the last possible id = %v; want ErrStreamIDExhausted", err)
	}

	// The "<ms>-*" form cannot carry, because the caller fixed the millisecond: a full
	// counter inside it really has nowhere to go.
	s3 := New(4)
	mid, _ := ParseStreamID("5-18446744073709551615", 0)
	if _, _, _, err := s3.XAdd("x", XAddOptions{ID: mid}, fields); err != nil {
		t.Fatalf("XADD of a full counter inside millisecond 5: %v", err)
	}
	// The refusal is "equal or smaller", not "exhausted": the stream has plenty of room left,
	// it is the *millisecond the caller named* that has none. Redis words it the same way.
	if _, _, _, err := s3.XAdd("x", XAddOptions{ID: StreamID{Ms: 5}, AutoSeq: true}, fields); !errors.Is(err, ErrStreamIDSmaller) {
		t.Errorf("XADD 5-* past a full counter = %v; want ErrStreamIDSmaller", err)
	}
}
