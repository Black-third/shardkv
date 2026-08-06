package store

import (
	"strconv"
	"testing"
	"time"
)

// TestExpireAtCond is the table for the conditional expire flags, including the
// combinations Redis allows together. The asymmetry worth pinning is that a
// persistent key counts as expiring infinitely far out, so GT can never beat it and
// LT always can.
func TestExpireAtCond(t *testing.T) {
	cur := time.Unix(1000, 0)
	early, late := cur.Add(time.Minute), cur.Add(time.Hour)

	cases := []struct {
		name     string
		startTTL time.Duration // 0 = a persistent key
		deadline time.Time
		cond     ExpireCond
		want     bool
	}{
		{"no condition, persistent", 0, late, ExpireAlways, true},
		{"no condition, volatile", time.Minute, late, ExpireAlways, true},
		{"NX on a persistent key", 0, late, ExpireNX, true},
		{"NX on a volatile key", time.Minute, late, ExpireNX, false},
		{"XX on a persistent key", 0, late, ExpireXX, false},
		{"XX on a volatile key", time.Minute, late, ExpireXX, true},
		{"GT with a later deadline", time.Minute, late, ExpireGT, true},
		{"GT with an earlier deadline", time.Hour, early, ExpireGT, false},
		{"GT with the same deadline", time.Minute, cur.Add(time.Minute), ExpireGT, false},
		{"GT on a persistent key", 0, late, ExpireGT, false},
		{"LT with an earlier deadline", time.Hour, early, ExpireLT, true},
		{"LT with a later deadline", time.Minute, late, ExpireLT, false},
		{"LT on a persistent key", 0, late, ExpireLT, true},
		{"XX GT together, both hold", time.Minute, late, ExpireXX | ExpireGT, true},
		{"XX GT together, GT fails", time.Hour, early, ExpireXX | ExpireGT, false},
		{"XX GT on a persistent key", 0, late, ExpireXX | ExpireGT, false},
	}
	for _, tc := range cases {
		s := New(4)
		s.SetClock(func() time.Time { return cur })
		s.Set("k", []byte("v"), tc.startTTL)

		if got := s.ExpireAtCond("k", tc.deadline, tc.cond); got != tc.want {
			t.Errorf("%s: ExpireAtCond = %v; want %v", tc.name, got, tc.want)
			continue
		}
		// When it applied, the stored deadline is the one asked for; when it did not,
		// the original TTL is untouched.
		ms, hasTTL, _ := s.TTLMillis("k")
		switch {
		case tc.want:
			if want := tc.deadline.UnixMilli() - cur.UnixMilli(); !hasTTL || ms != want {
				t.Errorf("%s: remaining = %d (hasTTL %v); want %d", tc.name, ms, hasTTL, want)
			}
		case tc.startTTL == 0:
			if hasTTL {
				t.Errorf("%s: a refused expire made the key volatile", tc.name)
			}
		default:
			if want := int64(tc.startTTL / time.Millisecond); ms != want {
				t.Errorf("%s: remaining = %d; want the original %d", tc.name, ms, want)
			}
		}
	}

	// A missing or already expired key is never touched.
	s := New(4)
	s.SetClock(func() time.Time { return cur })
	if s.ExpireAtCond("ghost", late, ExpireAlways) {
		t.Error("ExpireAtCond reported success on a missing key")
	}
	s.Set("gone", []byte("v"), time.Millisecond)
	cur = cur.Add(time.Second)
	if s.ExpireAtCond("gone", late, ExpireNX) {
		t.Error("ExpireAtCond revived an expired key")
	}
}

// TestRandomKey covers the arbitrary pick: nothing on an empty store, always a live
// key otherwise, and never an expired one.
func TestRandomKey(t *testing.T) {
	cur := time.Unix(1000, 0)
	s := New(8)
	s.SetClock(func() time.Time { return cur })

	if _, ok := s.RandomKey(); ok {
		t.Error("RandomKey found a key in an empty store")
	}

	want := map[string]bool{}
	for i := 0; i < 50; i++ {
		k := "k" + strconv.Itoa(i)
		s.Set(k, []byte("v"), 0)
		want[k] = true
	}
	// Every draw is one of the live keys, and over many draws more than one turns up.
	seen := map[string]bool{}
	for i := 0; i < 200; i++ {
		k, ok := s.RandomKey()
		if !ok {
			t.Fatal("RandomKey found nothing in a populated store")
		}
		if !want[k] {
			t.Fatalf("RandomKey returned %q, which is not in the store", k)
		}
		seen[k] = true
	}
	if len(seen) < 2 {
		t.Errorf("RandomKey returned only %d distinct key(s) over 200 draws", len(seen))
	}

	// An expired key is not a candidate.
	s2 := New(4)
	s2.SetClock(func() time.Time { return cur })
	s2.Set("gone", []byte("v"), time.Millisecond)
	cur = cur.Add(time.Second)
	if k, ok := s2.RandomKey(); ok {
		t.Errorf("RandomKey returned the expired key %q", k)
	}
}

// TestIdleSecondsTracksAccess covers OBJECT IDLETIME's source. Access times are only
// recorded while eviction is enabled -- that is what keeps the default read path free
// of an atomic write -- so an untracked key must report 0 rather than an age measured
// from the epoch.
func TestIdleSecondsTracksAccess(t *testing.T) {
	cur := time.Unix(1_600_000_000, 0)

	untracked := New(4)
	untracked.SetClock(func() time.Time { return cur })
	untracked.Set("k", []byte("v"), 0)
	if idle, ok := untracked.IdleSeconds("k"); !ok || idle != 0 {
		t.Errorf("IdleSeconds without eviction = %d, %v; want 0, true", idle, ok)
	}

	tracked := New(4)
	tracked.SetClock(func() time.Time { return cur })
	tracked.SetMaxKeys(1000) // turns access-time tracking on
	tracked.Set("k", []byte("v"), 0)
	cur = cur.Add(90 * time.Second)
	if idle, ok := tracked.IdleSeconds("k"); !ok || idle != 90 {
		t.Errorf("IdleSeconds after 90s = %d, %v; want 90, true", idle, ok)
	}
	// Reading the key resets the clock on it.
	tracked.Get("k")
	if idle, _ := tracked.IdleSeconds("k"); idle != 0 {
		t.Errorf("IdleSeconds after a read = %d; want 0", idle)
	}
	if _, ok := tracked.IdleSeconds("ghost"); ok {
		t.Error("IdleSeconds reported a missing key as present")
	}
}

// TestEncodingThresholds covers OBJECT ENCODING's answers. The internals here are one
// representation per type, so what the report promises is Redis's size and content
// thresholds -- which is what a client asking the question is deciding on.
func TestEncodingThresholds(t *testing.T) {
	s := New(8)
	long := make([]byte, 50)
	for i := range long {
		long[i] = 'x'
	}

	s.Set("int", []byte("12345"), 0)
	s.Set("embstr", []byte("hello"), 0)
	s.Set("raw", long, 0)
	s.RPush("smalllist", []byte("a"))
	s.HSet("smallhash", [2][]byte{[]byte("f"), []byte("v")})
	s.HSet("bigvalhash", [2][]byte{[]byte("f"), append(long, long...)})
	s.SAdd("intset", "1", "2", "3")
	s.SAdd("strset", "a", "b")
	s.ZAdd("smallzset", "m", 1)

	// 200 elements is over the sorted set's and the plain set's default of 128 but under
	// the hash's default of 512, and 200 small integers total far less than the 8 KB a
	// listpack may occupy -- so the list and the hash here are *listpacks*, which is what
	// a real redis:7.2 reports for exactly this data. The oversized cases below are what
	// cross the thresholds.
	for i := 0; i < 200; i++ {
		v := strconv.Itoa(i)
		s.RPush("midlist", []byte(v))
		s.HSet("midhash", [2][]byte{[]byte(v), []byte(v)})
		s.SAdd("bigset", "m"+v)
		s.ZAdd("bigzset", "m"+v, float64(i))
	}
	// A list crosses its default threshold by *bytes*, not by element count: the default
	// list-max-listpack-size is -2, meaning "8 KB". 200 fifty-byte elements is 10 KB.
	for i := 0; i < 200; i++ {
		s.RPush("biglist", long)
	}
	for i := 0; i < 600; i++ {
		v := strconv.Itoa(i)
		s.HSet("bighash", [2][]byte{[]byte(v), []byte(v)})
	}

	cases := []struct{ key, want string }{
		{"int", "int"},
		{"embstr", "embstr"},
		{"raw", "raw"},
		{"smalllist", "listpack"},
		{"midlist", "listpack"},
		{"biglist", "quicklist"},
		{"smallhash", "listpack"},
		{"midhash", "listpack"},
		{"bigvalhash", "hashtable"},
		{"bighash", "hashtable"},
		{"intset", "intset"},
		{"strset", "listpack"},
		{"bigset", "hashtable"},
		{"smallzset", "listpack"},
		{"bigzset", "skiplist"},
	}
	for _, tc := range cases {
		got, ok := s.Encoding(tc.key)
		if !ok {
			t.Errorf("Encoding(%q) reported the key missing", tc.key)
			continue
		}
		if got != tc.want {
			t.Errorf("Encoding(%q) = %q; want %q", tc.key, got, tc.want)
		}
	}
	if _, ok := s.Encoding("ghost"); ok {
		t.Error("Encoding reported a missing key as present")
	}
}

// TestEncodingThresholdsAreConfigured checks that the thresholds are configuration and
// not constants: lowering one has to change the reported encoding of a value that has
// not itself changed. That is the whole point of them being settable -- Redis's own type
// tests are `foreach encoding {listpack hashtable}` loops that set a threshold and then
// assert what OBJECT ENCODING says.
func TestEncodingThresholdsAreConfigured(t *testing.T) {
	s := New(8)
	long := make([]byte, 100)
	for i := range long {
		long[i] = 'x'
	}

	s.HSet("h", [2][]byte{[]byte("f1"), []byte("v")})
	s.HSet("h", [2][]byte{[]byte("f2"), []byte("v")})
	s.RPush("l", []byte("a"), []byte("b"), []byte("c"))
	s.SAdd("s", "a", "b", "c")
	s.SAdd("si", "1", "2", "3")
	s.ZAdd("z", "a", 1)
	s.ZAdd("z", "b", 2)

	// Defaults first, so a later failure distinguishes "the threshold did not apply"
	// from "the value was never a listpack".
	for _, k := range []string{"h", "l", "s", "z"} {
		if got, _ := s.Encoding(k); got != "listpack" {
			t.Fatalf("Encoding(%q) = %q before any threshold moved; want listpack", k, got)
		}
	}

	s.SetEncodingLimit(HashMaxListpackEntries, 1)
	s.SetEncodingLimit(ListMaxListpackSize, 2)
	s.SetEncodingLimit(SetMaxListpackEntries, 2)
	s.SetEncodingLimit(SetMaxIntsetEntries, 2)
	s.SetEncodingLimit(ZSetMaxListpackEntries, 1)

	want := map[string]string{"h": "hashtable", "l": "quicklist", "s": "hashtable", "z": "skiplist", "si": "hashtable"}
	for k, exp := range want {
		if got, _ := s.Encoding(k); got != exp {
			t.Errorf("Encoding(%q) after lowering the entry thresholds = %q; want %q", k, got, exp)
		}
	}
	// An all-integer set past set-max-intset-entries falls back to the listpack, not
	// straight to the hash table -- so raising only the listpack threshold moves it,
	// which is the two-stage rule Redis applies to sets and to nothing else.
	s.SetEncodingLimit(SetMaxListpackEntries, 8)
	if got, _ := s.Encoding("si"); got != "listpack" {
		t.Errorf("Encoding of an integer set over set-max-intset-entries = %q; want listpack", got)
	}

	// And by value length, which is the other half of Redis's rule: a member longer than
	// the configured maximum forces the general representation whatever the count is.
	s2 := New(8)
	s2.SAdd("s", string(long))
	s2.ZAdd("z", string(long), 1)
	s2.HSet("h", [2][]byte{[]byte("f"), long})
	for k, exp := range map[string]string{"s": "hashtable", "z": "skiplist", "h": "hashtable"} {
		if got, _ := s2.Encoding(k); got != exp {
			t.Errorf("Encoding(%q) with a 100-byte member = %q; want %q", k, got, exp)
		}
	}
	s2.SetEncodingLimit(SetMaxListpackValue, 128)
	s2.SetEncodingLimit(ZSetMaxListpackValue, 128)
	s2.SetEncodingLimit(HashMaxListpackValue, 128)
	for _, k := range []string{"s", "z", "h"} {
		if got, _ := s2.Encoding(k); got != "listpack" {
			t.Errorf("Encoding(%q) after raising the value threshold = %q; want listpack", k, got)
		}
	}

	// A negative list-max-listpack-size is a byte budget, so a list of many tiny
	// elements stays a listpack while one holding more than the budget does not.
	s3 := New(8)
	for i := 0; i < 500; i++ {
		s3.RPush("l", []byte("x"))
	}
	if got, _ := s3.Encoding("l"); got != "listpack" {
		t.Errorf("Encoding of 500 one-byte elements under the default -2 = %q; want listpack", got)
	}
	s3.SetEncodingLimit(ListMaxListpackSize, -1) // 4 KB
	for i := 0; i < 5000; i++ {
		s3.RPush("l", []byte("x"))
	}
	if got, _ := s3.Encoding("l"); got != "quicklist" {
		t.Errorf("Encoding of 5500 bytes of elements under -1 (4 KB) = %q; want quicklist", got)
	}
}

// TestRenameNXAndTouch covers the remaining keyspace helpers.
func TestRenameNXAndTouch(t *testing.T) {
	s := New(8)
	s.Set("a", []byte("1"), 0)
	s.Set("b", []byte("2"), 0)

	if renamed, found := s.RenameNX("a", "b"); renamed || !found {
		t.Errorf("RenameNX onto an existing key = %v, %v; want false, true", renamed, found)
	}
	if v, _ := s.Get("b"); string(v) != "2" {
		t.Errorf("destination was overwritten: %q", v)
	}
	if renamed, found := s.RenameNX("a", "c"); !renamed || !found {
		t.Errorf("RenameNX = %v, %v; want true, true", renamed, found)
	}
	if s.Exists("a") {
		t.Error("source survived the rename")
	}
	if _, found := s.RenameNX("ghost", "x"); found {
		t.Error("RenameNX reported a missing source as found")
	}
	// Renaming onto itself is not an error, just nothing to do.
	if renamed, found := s.RenameNX("b", "b"); renamed || !found {
		t.Errorf("RenameNX onto itself = %v, %v; want false, true", renamed, found)
	}
	if v, _ := s.Get("b"); string(v) != "2" {
		t.Errorf("value after a self-rename = %q; want 2", v)
	}

	if !s.Touch("b") {
		t.Error("Touch reported a live key as absent")
	}
	if s.Touch("ghost") {
		t.Error("Touch reported a missing key as present")
	}
}

// TestEncodingReportsRepresentationNotContent covers what OBJECT ENCODING names.
//
// The field describes how a value is *stored*, not what its bytes happen to look like.
// Redis tries an integer encoding when a value is stored whole and does not re-encode one
// it has appended to, so `SET k 1` then `APPEND k 2` leaves a value that reads "12" and
// is still a plain buffer. Deriving the answer from the content instead reports `int`
// both times -- wrong in a way that matters, because assert_encoding runs throughout
// Redis's own test suite and memory-analysis tools read the field.
func TestEncodingReportsRepresentationNotContent(t *testing.T) {
	s := New(8)

	s.Set("k", []byte("1"), 0)
	if enc, _ := s.Encoding("k"); enc != "int" {
		t.Errorf("a whole numeric value encodes as %q; want int", enc)
	}
	if _, err := s.Append("k", []byte("2")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if v, _ := s.Get("k"); string(v) != "12" {
		t.Fatalf("Append produced %q; want 12", v)
	}
	if enc, _ := s.Encoding("k"); enc != "raw" {
		t.Errorf("an appended value encodes as %q; want raw (it reads as a number but is a buffer)", enc)
	}
	// Storing it whole again re-encodes, as Redis re-encodes on SET.
	s.Set("k", []byte("12"), 0)
	if enc, _ := s.Encoding("k"); enc != "int" {
		t.Errorf("re-storing the value whole encodes as %q; want int", enc)
	}
	// INCR re-encodes too: it writes a fresh integer.
	if _, err := s.Incr("k", 1); err != nil {
		t.Fatalf("Incr: %v", err)
	}
	if enc, _ := s.Encoding("k"); enc != "int" {
		t.Errorf("after INCR the encoding is %q; want int", enc)
	}

	// SETRANGE writes into a value, so it is a buffer for the same reason.
	s.Set("r", []byte("9"), 0)
	if _, err := s.SetRange("r", 1, []byte("9")); err != nil {
		t.Fatalf("SetRange: %v", err)
	}
	if enc, _ := s.Encoding("r"); enc != "raw" {
		t.Errorf("a value written into by SETRANGE encodes as %q; want raw", enc)
	}

	// A bitmap is a string that was never stored whole.
	if _, _, err := s.SetBit("b", 7, true); err != nil {
		t.Fatalf("SetBit: %v", err)
	}
	if enc, _ := s.Encoding("b"); enc != "raw" {
		t.Errorf("a bitmap encodes as %q; want raw", enc)
	}
}
