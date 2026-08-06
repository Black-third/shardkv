package server

import (
	"strconv"
	"strings"
	"testing"
)

func TestGlobMatch(t *testing.T) {
	cases := []struct {
		pattern, s string
		want       bool
	}{
		{"*", "anything", true},
		{"", "", true},
		{"", "x", false},
		{"user:*", "user:42", true},
		{"user:*", "admin:42", false},
		{"k?", "k1", true},
		{"k?", "k12", false},
		{"abc", "abc", true},
		{"a*c", "abbbc", true},
		{"a*c", "abbbd", false},
		{"*suffix", "has-suffix", true},
		{"a*b*c", "axxbyyc", true},
	}
	for _, c := range cases {
		if got := globMatch(c.pattern, c.s); got != c.want {
			t.Errorf("globMatch(%q, %q) = %v; want %v", c.pattern, c.s, got, c.want)
		}
	}
}

func TestScanCommand(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	for i := 0; i < 20; i++ {
		c.cmd("SET k" + strconv.Itoa(i) + " v")
	}
	c.cmd("SET other 1")

	// A single SCAN with a large COUNT returns everything and cursor 0.
	full := c.cmd("SCAN 0 COUNT 1000")
	if !contains(full, "[0 [") {
		t.Fatalf("SCAN reply not [cursor [keys]] with cursor 0: %s", full)
	}
	for _, want := range []string{"k0", "k19", "other"} {
		if !contains(full, want) {
			t.Errorf("SCAN result missing %q: %s", want, full)
		}
	}

	// MATCH filters out non-matching keys.
	matched := c.cmd("SCAN 0 MATCH k* COUNT 1000")
	if !contains(matched, "k5") {
		t.Errorf("SCAN MATCH k* missing k5: %s", matched)
	}
	if contains(matched, "other") {
		t.Errorf("SCAN MATCH k* should exclude 'other': %s", matched)
	}
}

// TestScanOptionErrors covers malformed option lists. The parser used to step over
// the arguments two at a time while requiring a following value, which silently
// dropped a trailing lone argument: a COUNT with no value, or one stray extra
// token, was accepted as if the client had written a valid command.
func TestScanOptionErrors(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("SET k v")

	cases := []struct{ cmd, want string }{
		// Truncated option lists.
		{"SCAN 0 COUNT", "-ERR syntax error"},
		{"SCAN 0 MATCH", "-ERR syntax error"},
		{"SCAN 0 MATCH k* EXTRA", "-ERR syntax error"},
		{"SCAN 0 MATCH k* COUNT 10 EXTRA", "-ERR syntax error"},
		// Unknown options are rejected whether or not they carry a value.
		{"SCAN 0 BOGUS 1", "-ERR syntax error"},
		{"SCAN 0 BOGUS", "-ERR syntax error"},
		// COUNT must be a number, and at least 1.
		{"SCAN 0 COUNT abc", "-ERR value is not an integer or out of range"},
		{"SCAN 0 COUNT 1.5", "-ERR value is not an integer or out of range"},
		{"SCAN 0 COUNT 0", "-ERR syntax error"},
		{"SCAN 0 COUNT -1", "-ERR syntax error"},
		// An unparsable cursor still reports its own error.
		{"SCAN notacursor", "-ERR invalid cursor"},
		// Well-formed forms keep working.
		{"SCAN 0 COUNT 1000", "[0 [k]]"},
		{"SCAN 0 MATCH k COUNT 10", "[0 [k]]"},
		{"SCAN 0 count 10 match k", "[0 [k]]"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}
}

// TestScanLargeCountIsClamped checks a COUNT beyond int range is accepted (it just
// means "as much as possible") rather than wrapping to a negative batch size.
func TestScanLargeCountIsClamped(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("SET k v")
	if got := c.cmd("SCAN 0 COUNT 9223372036854775807"); got != "[0 [k]]" {
		t.Errorf("SCAN with a MaxInt64 COUNT = %q; want [0 [k]]", got)
	}
}

// TestCollectionScanCommands covers HSCAN/SSCAN/ZSCAN. They share SCAN's contract
// -- a [cursor, elements] reply, MATCH and COUNT options -- and return a collection
// in a single batch with a next cursor of 0, which is what Redis does for a
// compactly encoded collection.
func TestCollectionScanCommands(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("HSET h f1 v1")
	c.cmd("SADD s only")
	c.cmd("ZADD z 1 m")
	c.cmd("SET str v")

	cases := []struct{ cmd, want string }{
		// HSCAN and ZSCAN flatten pairs; SSCAN lists members.
		{"HSCAN h 0", "[0 [f1 v1]]"},
		{"SSCAN s 0", "[0 [only]]"},
		{"ZSCAN z 0", "[0 [m 1]]"},
		{"HSCAN h 0 COUNT 100", "[0 [f1 v1]]"},
		// A missing key is an empty batch, not an error.
		{"HSCAN missing 0", "[0 []]"},
		{"SSCAN missing 0", "[0 []]"},
		{"ZSCAN missing 0", "[0 []]"},
		// A non-zero cursor means the iteration already finished.
		{"HSCAN h 99", "[0 []]"},
		{"SSCAN s 1", "[0 []]"},
		// MATCH filters on the field or member, never on the value or score.
		{"HSCAN h 0 MATCH f*", "[0 [f1 v1]]"},
		{"HSCAN h 0 MATCH nope*", "[0 []]"},
		{"HSCAN h 0 MATCH v1", "[0 []]"},
		{"ZSCAN z 0 MATCH m", "[0 [m 1]]"},
		{"ZSCAN z 0 MATCH 1", "[0 []]"},
		{"SSCAN s 0 MATCH on*", "[0 [only]]"},
		// The same option-list validation SCAN applies.
		{"HSCAN h notacursor", "-ERR invalid cursor"},
		{"HSCAN h 0 COUNT", "-ERR syntax error"},
		{"HSCAN h 0 BOGUS 1", "-ERR syntax error"},
		{"HSCAN h 0 COUNT 0", "-ERR syntax error"},
		{"HSCAN h 0 COUNT abc", "-ERR value is not an integer or out of range"},
		{"HSCAN h 0 MATCH f* EXTRA", "-ERR syntax error"},
		{"HSCAN h", "-ERR wrong number of arguments for 'hscan' command"},
		{"SSCAN s", "-ERR wrong number of arguments for 'sscan' command"},
		{"ZSCAN z", "-ERR wrong number of arguments for 'zscan' command"},
		// Wrong type, one per command.
		{"HSCAN str 0", wrongType},
		{"SSCAN str 0", wrongType},
		{"ZSCAN str 0", wrongType},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	// A whole collection comes back in one call, cursor 0.
	for i := 0; i < 50; i++ {
		c.cmd("SADD big m" + strconv.Itoa(i))
	}
	reply := c.cmd("SSCAN big 0")
	if !contains(reply, "[0 [") {
		t.Fatalf("SSCAN reply not [cursor [members]] with cursor 0: %s", reply)
	}
	// Flatten the nested array so a member at either end is a whole token.
	flat := strings.NewReplacer("[", " ", "]", " ").Replace(reply)
	for _, want := range []string{"m0", "m25", "m49"} {
		if !containsKey(flat, want) {
			t.Errorf("SSCAN result missing %q: %s", want, reply)
		}
	}
}
