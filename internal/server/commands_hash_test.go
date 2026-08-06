package server

import (
	"sort"
	"strings"
	"testing"
)

// arrayFields returns the elements of a flattened "[a b c]" array reply, sorted, so
// a reply whose order comes from a Go map can be compared exactly.
func arrayFields(reply string) string {
	inner := reply
	if len(inner) >= 2 && inner[0] == '[' {
		inner = inner[1 : len(inner)-1]
	}
	f := splitSpace(inner)
	sort.Strings(f)
	return strings.Join(f, ",")
}

// TestHashProjectionCommands covers the read-only hash commands. HKEYS/HVALS come
// out in map order, so they are compared as sets.
func TestHashProjectionCommands(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		{"HSET h f1 v1 f2 v2 f3 v3", ":3"},
		{"HEXISTS h f1", ":1"},
		{"HEXISTS h nope", ":0"},
		{"HEXISTS missing f", ":0"},
		{"HSTRLEN h f1", ":2"},
		{"HSTRLEN h nope", ":0"},
		{"HSTRLEN missing f", ":0"},
		// HMGET answers one element per field, a null for each absent one.
		{"HMGET h f1 nope f3", "[v1 (nil) v3]"},
		{"HMGET missing a b", "[(nil) (nil)]"},
		{"HKEYS missing", "[]"},
		{"HVALS missing", "[]"},
		{"HEXISTS h", "-ERR wrong number of arguments for 'hexists' command"},
		{"HMGET h", "-ERR wrong number of arguments for 'hmget' command"},
		{"HSTRLEN h", "-ERR wrong number of arguments for 'hstrlen' command"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}
	if got := arrayFields(c.cmd("HKEYS h")); got != "f1,f2,f3" {
		t.Errorf("HKEYS = %q; want the three fields", got)
	}
	if got := arrayFields(c.cmd("HVALS h")); got != "v1,v2,v3" {
		t.Errorf("HVALS = %q; want the three values", got)
	}

	c.cmd("SET str v")
	for _, cmd := range []string{"HKEYS str", "HVALS str", "HEXISTS str f", "HMGET str f", "HSTRLEN str f"} {
		if got := c.cmd(cmd); got != wrongType {
			t.Errorf("%q on a string = %q; want %q", cmd, got, wrongType)
		}
	}
}

// TestHashWriteCommands covers HSETNX and the two field increments, whose error
// strings are hash-specific ("hash value is not an integer", not the plain one).
func TestHashWriteCommands(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		{"HSET h f1 v1", ":1"},
		{"HSETNX h f1 other", ":0"},
		{"HGET h f1", "v1"},
		{"HSETNX h f2 v2", ":1"},
		{"HGET h f2", "v2"},
		{"HSETNX fresh f v", ":1"}, // creates the hash
		{"HGET fresh f", "v"},
		{"HSETNX h f", "-ERR wrong number of arguments for 'hsetnx' command"},
		// HINCRBY starts a missing field (and a missing hash) from zero.
		{"HINCRBY counter f 5", ":5"},
		{"HINCRBY counter f -2", ":3"},
		{"HGET counter f", "3"},
		{"HSET bad f abc", ":1"},
		{"HINCRBY bad f 1", "-ERR hash value is not an integer"},
		{"HINCRBY counter f abc", "-ERR value is not an integer or out of range"},
		{"HSET big f 9223372036854775807", ":1"},
		{"HINCRBY big f 1", "-ERR increment or decrement would overflow"},
		{"HGET big f", "9223372036854775807"},
		{"HINCRBY counter f", "-ERR wrong number of arguments for 'hincrby' command"},
		// HINCRBYFLOAT, including the non-finite result Redis refuses.
		{"HINCRBYFLOAT fl f 10.5", "10.5"},
		{"HINCRBYFLOAT fl f 0.1", "10.6"},
		{"HINCRBYFLOAT fl f -0.6", "10"},
		{"HINCRBYFLOAT bad f 1", "-ERR hash value is not a float"},
		{"HINCRBYFLOAT fl f nan", "-ERR value is not a valid float"},
		// An infinite operand parses; the infinity is reported against the result, as
		// Redis reports it. See TestIncrByFloat for the same distinction on the string side.
		// An infinite operand is refused as an operand, which is where HINCRBYFLOAT's message
		// differs from INCRBYFLOAT's -- Redis words the two differently and this follows it.
		{"HINCRBYFLOAT fl f inf", "-ERR value is NaN or Infinity"},
		{"HSET fbig f 1e308", ":1"},
		{"HINCRBYFLOAT fbig f 1e308", "-ERR increment would produce NaN or Infinity"},
		{"HGET fbig f", "1e308"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	c.cmd("SET str v")
	for _, cmd := range []string{"HSETNX str f v", "HINCRBY str f 1", "HINCRBYFLOAT str f 1"} {
		if got := c.cmd(cmd); got != wrongType {
			t.Errorf("%q on a string = %q; want %q", cmd, got, wrongType)
		}
	}
}

// TestHRandField covers HRANDFIELD's three reply shapes and the sign of count,
// which decides whether repeats are allowed.
func TestHRandField(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("HSET one f v")

	cases := []struct{ cmd, want string }{
		{"HRANDFIELD one", "f"},
		{"HRANDFIELD one 3", "[f]"},      // distinct: capped at the hash size
		{"HRANDFIELD one -3", "[f f f]"}, // with repeats: exactly three
		{"HRANDFIELD one 1 WITHVALUES", "[f v]"},
		{"HRANDFIELD one 0", "[]"},
		{"HRANDFIELD missing", "(nil)"},
		{"HRANDFIELD missing 3", "[]"},
		{"HRANDFIELD one abc", "-ERR value is not an integer or out of range"},
		{"HRANDFIELD one 1 BOGUS", "-ERR syntax error"},
		{"HRANDFIELD one 1 WITHVALUES EXTRA", "-ERR syntax error"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	// With several fields, a positive count returns distinct ones.
	c.cmd("HSET many a 1 b 2 c 3")
	if got := arrayFields(c.cmd("HRANDFIELD many 3")); got != "a,b,c" {
		t.Errorf("HRANDFIELD many 3 = %q; want all three distinct fields", got)
	}
	if got := arrayFields(c.cmd("HRANDFIELD many 10")); got != "a,b,c" {
		t.Errorf("HRANDFIELD many 10 = %q; want the whole hash", got)
	}

	c.cmd("SET str v")
	if got := c.cmd("HRANDFIELD str"); got != wrongType {
		t.Errorf("HRANDFIELD on a string = %q; want %q", got, wrongType)
	}
}
