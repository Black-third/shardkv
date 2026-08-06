package store

import (
	"errors"
	"testing"
	"time"
)

// TestSetRangeEdges covers the SetRange cases a client cannot express over the
// inline protocol: an empty value, and an offset that would grow the string past
// the largest one the protocol can carry.
func TestSetRangeEdges(t *testing.T) {
	s := New(8)

	// An empty value never creates a key, and only reports the current length.
	if n, err := s.SetRange("missing", 0, nil); n != 0 || err != nil {
		t.Fatalf("SetRange with an empty value on a missing key = %d, %v; want 0, nil", n, err)
	}
	if s.Exists("missing") {
		t.Error("SetRange with an empty value created the key")
	}
	s.Set("k", []byte("hello"), 0)
	if n, err := s.SetRange("k", 100, nil); n != 5 || err != nil {
		t.Fatalf("SetRange with an empty value = %d, %v; want 5, nil", n, err)
	}
	if v, _ := s.Get("k"); string(v) != "hello" {
		t.Errorf("value after an empty SetRange = %q; want hello", v)
	}

	// The size limit is checked before anything is allocated, so an absurd offset is
	// cheap to refuse rather than an attempt to allocate half a gigabyte.
	if _, err := s.SetRange("k", maxStringLen, []byte("x")); err != ErrStringTooLong {
		t.Errorf("SetRange past the size limit = %v; want ErrStringTooLong", err)
	}
	if _, err := s.SetRange("k", -1, []byte("x")); err != ErrOffset {
		t.Errorf("SetRange with a negative offset = %v; want ErrOffset", err)
	}
	if v, _ := s.Get("k"); string(v) != "hello" {
		t.Errorf("value after a rejected SetRange = %q; want hello", v)
	}

	// The gap is zero-padded, and the TTL survives.
	s.Set("vol", []byte("ab"), 30*time.Second)
	if n, _ := s.SetRange("vol", 4, []byte("z")); n != 5 {
		t.Fatalf("SetRange length = %d; want 5", n)
	}
	if v, _ := s.Get("vol"); string(v) != "ab\x00\x00z" {
		t.Errorf("padded value = %q; want ab\\x00\\x00z", v)
	}
	if _, hasTTL, ok := s.TTL("vol"); !ok || !hasTTL {
		t.Error("SetRange cleared the TTL")
	}
}

// TestGetExChangedReporting pins the flag that decides whether GETEX propagates: it
// must report a change only when the expiry actually moved.
func TestGetExChangedReporting(t *testing.T) {
	s := New(8)
	cur := time.Unix(1000, 0)
	s.SetClock(func() time.Time { return cur })
	s.Set("k", []byte("v"), 0)

	deadline := cur.Add(time.Minute)
	if v, ok, changed, err := s.GetEx("k", deadline, true); !ok || changed != true || err != nil || string(v) != "v" {
		t.Fatalf("GetEx setting a TTL = %q, %v, %v, %v", v, ok, changed, err)
	}
	// Setting the same deadline again is not a change.
	if _, _, changed, _ := s.GetEx("k", deadline, true); changed {
		t.Error("GetEx reported a change for an unchanged deadline")
	}
	// A plain read never changes anything.
	if _, _, changed, _ := s.GetEx("k", time.Time{}, false); changed {
		t.Error("GetEx without apply reported a change")
	}
	if _, hasTTL, _ := s.TTL("k"); !hasTTL {
		t.Error("GetEx without apply removed the TTL")
	}
	// A zero deadline persists the key -- once.
	if _, _, changed, _ := s.GetEx("k", time.Time{}, true); !changed {
		t.Error("GetEx did not report removing the TTL")
	}
	if _, _, changed, _ := s.GetEx("k", time.Time{}, true); changed {
		t.Error("GetEx reported persisting an already-permanent key")
	}
	// A missing key reports nothing at all.
	if _, ok, changed, _ := s.GetEx("ghost", deadline, true); ok || changed {
		t.Error("GetEx on a missing key reported a value or a change")
	}
	// The wrong type is an error, not a miss.
	s.RPush("list", []byte("x"))
	if _, _, _, err := s.GetEx("list", deadline, true); err != ErrWrongType {
		t.Errorf("GetEx on a list = %v; want ErrWrongType", err)
	}
}

// TestSetWithOptions covers the option matrix of SET at the store level, where the
// distinction between "wrote nothing" and "wrote" is the return value the server
// turns into +OK or a null.
func TestSetWithOptions(t *testing.T) {
	s := New(8)

	if _, _, set, _ := s.SetWithOptions("k", []byte("v1"), SetOptions{NX: true}); !set {
		t.Fatal("NX on a missing key did not write")
	}
	if _, _, set, _ := s.SetWithOptions("k", []byte("v2"), SetOptions{NX: true}); set {
		t.Error("NX on an existing key wrote")
	}
	if v, _ := s.Get("k"); string(v) != "v1" {
		t.Errorf("value = %q; want v1", v)
	}
	if _, _, set, _ := s.SetWithOptions("absent", []byte("v"), SetOptions{XX: true}); set {
		t.Error("XX on a missing key wrote")
	}
	if s.Exists("absent") {
		t.Error("XX created the key")
	}
	// GET reports the previous value even when the condition refused the write.
	old, oldOK, set, _ := s.SetWithOptions("k", []byte("v3"), SetOptions{NX: true, Get: true})
	if !oldOK || string(old) != "v1" || set {
		t.Errorf("NX GET = %q, %v, %v; want v1, true, false", old, oldOK, set)
	}
	// KEEPTTL retains an expiry that a plain SET clears.
	s.Set("vol", []byte("v"), time.Minute)
	s.SetWithOptions("vol", []byte("v2"), SetOptions{KeepTTL: true})
	if _, hasTTL, _ := s.TTL("vol"); !hasTTL {
		t.Error("KEEPTTL cleared the TTL")
	}
	s.SetWithOptions("vol", []byte("v3"), SetOptions{})
	if _, hasTTL, _ := s.TTL("vol"); hasTTL {
		t.Error("a plain SET kept the TTL")
	}
	// GET against another type refuses the whole command.
	s.RPush("list", []byte("x"))
	if _, _, set, err := s.SetWithOptions("list", []byte("v"), SetOptions{Get: true}); err != ErrWrongType || set {
		t.Errorf("GET on a list = %v, set=%v; want ErrWrongType, false", err, set)
	}
	if typ, _ := s.Type("list"); typ != "list" {
		t.Errorf("type after a rejected SET ... GET = %q; want list", typ)
	}
	// Without GET, SET replaces a value of any type.
	if _, _, set, err := s.SetWithOptions("list", []byte("v"), SetOptions{}); !set || err != nil {
		t.Errorf("SET over a list = set %v, %v; want true, nil", set, err)
	}
}

// TestIncrByFloatFormatting pins the text an increment stores: the value the store keeps
// is the one a later increment reads back, the one the server replies, and the one it
// propagates.
//
// Redis spells an increment result with its "human" formatter, which never uses an
// exponent -- unlike a sorted-set score, which does outside roughly 1e-6..1e18. Every
// expectation below was read off redis:7.2 rather than reasoned about; the small-value
// case previously asserted "2e-06", which is what Go's 'g' verb produces and not what
// Redis stores.
func TestIncrByFloatFormatting(t *testing.T) {
	s := New(8)
	cases := []struct {
		start string
		delta float64
		want  string
	}{
		{"", 3, "3"},
		{"10.5", 0.1, "10.6"},
		{"5", -5, "0"},
		{"3.0e3", 200, "3200"},
		// redis:7.2: SET a 0.000001; INCRBYFLOAT a 0.000001 -> "0.000002", and GET agrees.
		{"0.000001", 0.000001, "0.000002"},
		// And below the exponent threshold a score would use, an increment still does not.
		// redis:7.2: SET b 0.0000001; INCRBYFLOAT b 0.0000001 -> "0.0000002".
		{"0.0000001", 0.0000001, "0.0000002"},
		// A large value stays decimal too -- this is the case that was reaching the AOF as
		// "1.71798691855e+10" while the reply said otherwise.
		{"17179869184", 1.5, "17179869185.5"},
	}
	for _, tc := range cases {
		s.Del("k")
		if tc.start != "" {
			s.Set("k", []byte(tc.start), 0)
		}
		// The store returns the text it stored, and that same text is what the server
		// replies and propagates -- so asserting on it here covers all three at once.
		// Re-formatting the float instead is what let the reply and the stored bytes
		// drift apart.
		_, text, err := s.IncrByFloat("k", tc.delta)
		if err != nil {
			t.Fatalf("IncrByFloat(%q, %v): %v", tc.start, tc.delta, err)
		}
		if text != tc.want {
			t.Errorf("IncrByFloat(%q, %v) = %s; want %s", tc.start, tc.delta, text, tc.want)
		}
		v, _ := s.Get("k")
		if string(v) != tc.want {
			t.Errorf("stored value after IncrByFloat(%q, %v) = %q; want %q", tc.start, tc.delta, v, tc.want)
		}
	}

	// A value that is not a number, and a result that is not finite, are refused.
	s.Set("bad", []byte("hello"), 0)
	if _, _, err := s.IncrByFloat("bad", 1); !errors.Is(err, ErrNotFloat) {
		t.Errorf("IncrByFloat on a non-number = %v; want ErrNotFloat", err)
	}
	s.Set("huge", []byte("1e308"), 0)
	if _, _, err := s.IncrByFloat("huge", 1e308); !errors.Is(err, ErrNaN) {
		t.Errorf("IncrByFloat overflowing = %v; want ErrNaN", err)
	}
	if v, _ := s.Get("huge"); string(v) != "1e308" {
		t.Errorf("value after a refused increment = %q; want 1e308", v)
	}
}
