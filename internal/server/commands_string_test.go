package server

import "testing"

const wrongType = "-WRONGTYPE Operation against a key holding the wrong kind of value"

// TestSetOptions covers SET's NX/XX/GET/KEEPTTL/expiry options and the
// combinations Redis rejects. The reply distinguishes three outcomes that all look
// like "nothing to say": +OK for a write, a null for a condition that did not fire,
// and the previous value for GET.
func TestSetOptions(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		// NX only writes a missing key; XX only an existing one.
		{"SET k v1 NX", "+OK"},
		{"SET k v2 NX", "(nil)"},
		{"GET k", "v1"},
		{"SET k v2 XX", "+OK"},
		{"GET k", "v2"},
		{"SET absent v XX", "(nil)"},
		{"EXISTS absent", ":0"},
		// GET reports the previous value, or a null when there was none.
		{"SET k v3 GET", "v2"},
		{"GET k", "v3"},
		{"SET fresh v GET", "(nil)"},
		{"SET k v4 XX GET", "v3"},
		// NX with GET still reports the old value even though it wrote nothing.
		{"SET k v5 NX GET", "v4"},
		{"GET k", "v4"},
		// KEEPTTL retains an expiry a plain SET would clear.
		{"SET tk v EX 100", "+OK"},
		{"TTL tk", ":100"},
		{"SET tk v2 KEEPTTL", "+OK"},
		{"TTL tk", ":100"},
		{"SET tk v3", "+OK"},
		{"TTL tk", ":-1"},
		// EXAT takes an absolute second timestamp.
		{"SET ek v EXAT 4102444800", "+OK"},
		{"EXISTS ek", ":1"},
		// Incompatible and malformed option lists.
		{"SET k v NX XX", "-ERR syntax error"},
		{"SET k v XX NX", "-ERR syntax error"},
		{"SET k v EX 10 KEEPTTL", "-ERR syntax error"},
		{"SET k v KEEPTTL EX 10", "-ERR syntax error"},
		{"SET k v EX 10 PX 10000", "-ERR syntax error"},
		{"SET k v EX", "-ERR syntax error"},
		{"SET k v BOGUS", "-ERR syntax error"},
		{"SET k v EX abc", "-ERR value is not an integer or out of range"},
		{"SET k v EX 0", "-ERR invalid expire time in 'set' command"},
		{"SET k v EX -1", "-ERR invalid expire time in 'set' command"},
		{"SET k v PXAT 0", "-ERR invalid expire time in 'set' command"},
		{"SET k", "-ERR wrong number of arguments for 'set' command"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	// GET against another data type is an error, and it must abort the SET too.
	c.cmd("RPUSH wl a")
	if got := c.cmd("SET wl v GET"); got != wrongType {
		t.Errorf("SET ... GET on a list = %q; want %q", got, wrongType)
	}
	if got := c.cmd("LRANGE wl 0 -1"); got != "[a]" {
		t.Errorf("list after a rejected SET ... GET = %q; want [a]", got)
	}
}

// TestSetExAndGetEx covers the TTL-carrying string commands, including the
// distinction GETEX needs: it replies with the value whether or not it changed an
// expiry.
func TestSetExAndGetEx(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		{"SETEX k 100 v", "+OK"},
		{"GET k", "v"},
		{"TTL k", ":100"},
		{"PSETEX pk 100000 v", "+OK"},
		{"TTL pk", ":100"},
		{"SETEX k 0 v", "-ERR invalid expire time in 'setex' command"},
		{"SETEX k -5 v", "-ERR invalid expire time in 'setex' command"},
		{"PSETEX k 0 v", "-ERR invalid expire time in 'psetex' command"},
		{"SETEX k abc v", "-ERR value is not an integer or out of range"},
		{"SETEX k 9223372036854775807 v", "-ERR invalid expire time in 'setex' command"},
		{"SETEX k 10", "-ERR wrong number of arguments for 'setex' command"},
		// GETEX reads, and optionally rewrites the expiry.
		{"SET g v", "+OK"},
		{"GETEX g", "v"},
		{"TTL g", ":-1"},
		{"GETEX g EX 100", "v"},
		{"TTL g", ":100"},
		{"GETEX g PERSIST", "v"},
		{"TTL g", ":-1"},
		{"GETEX g PXAT 4102444800000", "v"},
		{"EXISTS g", ":1"},
		{"GETEX missing", "(nil)"},
		{"GETEX missing EX 100", "(nil)"},
		{"GETEX g EX 0", "-ERR invalid expire time in 'getex' command"},
		{"GETEX g EX", "-ERR syntax error"},
		{"GETEX g BOGUS", "-ERR syntax error"},
		{"GETEX g PERSIST EXTRA", "-ERR syntax error"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	c.cmd("RPUSH wl a")
	if got := c.cmd("GETEX wl EX 10"); got != wrongType {
		t.Errorf("GETEX on a list = %q; want %q", got, wrongType)
	}
}

// TestStringRangeCommands covers SETRANGE/GETRANGE, whose index rules are the
// fiddly part: negative bounds count from the end, out-of-range bounds clamp to an
// empty result rather than erroring, and SETRANGE zero-pads a gap.
func TestStringRangeCommands(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		// SETRANGE creates the key, overwrites in place, and extends.
		{"SETRANGE sr 0 hello", ":5"},
		{"GET sr", "hello"},
		{"SETRANGE sr 1 xy", ":5"},
		{"GET sr", "hxylo"},
		{"SETRANGE sr 6 z", ":7"},
		{"STRLEN sr", ":7"},
		{"SETRANGE sr -1 x", "-ERR offset is out of range"},
		{"SETRANGE sr abc x", "-ERR value is not an integer or out of range"},
		{"SETRANGE sr 0", "-ERR wrong number of arguments for 'setrange' command"},
		// GETRANGE index rules.
		{"SET gr Thisisastring", "+OK"},
		{"GETRANGE gr 0 3", "This"},
		{"GETRANGE gr -3 -1", "ing"},
		{"GETRANGE gr 0 -1", "Thisisastring"},
		{"GETRANGE gr 10 100", "ing"},
		{"GETRANGE gr 0 -100", "T"},
		{"GETRANGE gr 100 200", ""},
		{"GETRANGE gr 5 2", ""},
		{"GETRANGE missing 0 -1", ""},
		{"SUBSTR gr 0 3", "This"},
		{"GETRANGE gr 0 abc", "-ERR value is not an integer or out of range"},
		{"GETRANGE gr 0", "-ERR wrong number of arguments for 'getrange' command"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	// The zero-padded gap is real NUL bytes, not spaces.
	if got := c.cmd("GETRANGE sr 5 5"); got != "\x00" {
		t.Errorf("SETRANGE gap byte = %q; want a NUL", got)
	}

	c.cmd("RPUSH wl a")
	for _, cmd := range []string{"SETRANGE wl 0 x", "GETRANGE wl 0 1"} {
		if got := c.cmd(cmd); got != wrongType {
			t.Errorf("%q on a list = %q; want %q", cmd, got, wrongType)
		}
	}
}

// TestIncrByFloat covers the float increment, whose failure modes are the point:
// a non-numeric value or operand, and a result that is no longer finite.
func TestIncrByFloat(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		{"SET f 10.5", "+OK"},
		{"INCRBYFLOAT f 0.1", "10.6"},
		{"INCRBYFLOAT f -5", "5.6"},
		{"INCRBYFLOAT fresh 3.0e3", "3000"},
		{"GET fresh", "3000"},
		// A missing key starts from zero.
		{"INCRBYFLOAT zero 1.5", "1.5"},
		// Neither the stored value nor the operand may be non-numeric.
		{"SET s hello", "+OK"},
		{"INCRBYFLOAT s 1", "-ERR value is not a valid float"},
		{"INCRBYFLOAT f abc", "-ERR value is not a valid float"},
		{"INCRBYFLOAT f nan", "-ERR value is not a valid float"},
		{"INCRBYFLOAT f inf", "-ERR value is not a valid float"},
		{"INCRBYFLOAT f", "-ERR wrong number of arguments for 'incrbyfloat' command"},
		// A result that leaves the finite range is refused, value untouched.
		{"SET big 1e308", "+OK"},
		{"INCRBYFLOAT big 1e308", "-ERR increment would produce NaN or Infinity"},
		{"GET big", "1e308"},
		// The TTL survives an increment.
		{"SETEX tf 100 1", "+OK"},
		{"INCRBYFLOAT tf 1", "2"},
		{"TTL tf", ":100"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	c.cmd("RPUSH wl a")
	if got := c.cmd("INCRBYFLOAT wl 1"); got != wrongType {
		t.Errorf("INCRBYFLOAT on a list = %q; want %q", got, wrongType)
	}
}
