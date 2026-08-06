package server

import (
	"strconv"
	"testing"
	"time"

	"github.com/Black-third/shardkv/internal/store"
)

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
		{"INCRBYFLOAT f", "-ERR wrong number of arguments for 'incrbyfloat' command"},
		// A result that leaves the finite range is refused, value untouched. The finite
		// range is long double's, though, which reaches four thousand decades past a
		// float64's: this case used to assert that 1e308 + 1e308 overflowed, which was
		// pinning the float64 implementation rather than Redis. Measured on redis:7.2
		// (amd64), that sum answers a 309-digit number -- see
		// TestIncrByFloatMatchesLongDouble -- and the overflow is out at 1e4932.
		{"SET big 1e4932", "+OK"},
		{"INCRBYFLOAT big 1e4932", "-ERR increment would produce NaN or Infinity"},
		{"GET big", "1e4932"},
		// The TTL survives an increment.
		{"SETEX tf 100 1", "+OK"},
		{"INCRBYFLOAT tf 1", "2"},
		{"TTL tf", ":100"},
		// An infinite *operand* parses -- "inf" is a valid float, and Redis's string2ld
		// accepts it while rejecting NaN -- so the error names the result that cannot be
		// represented rather than blaming an operand that was well formed. Verified against
		// redis:7.2, which answers exactly this.
		{"SET inf1 1", "+OK"},
		{"INCRBYFLOAT inf1 inf", "-ERR increment would produce NaN or Infinity"},
		{"GET inf1", "1"},
		{"INCRBYFLOAT inf1 -inf", "-ERR increment would produce NaN or Infinity"},
		// NaN is refused at parse time, as Redis refuses it.
		{"INCRBYFLOAT inf1 nan", "-ERR value is not a valid float"},
		{"INCRBYFLOAT inf1 notanumber", "-ERR value is not a valid float"},
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

// TestMSetNX covers the all-or-nothing guarantee, which is the whole command: a single
// existing key has to refuse the entire batch, leaving every other key untouched.
func TestMSetNX(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		{"MSETNX a 1 b 2", ":1"},
		{"GET a", "1"},
		{"GET b", "2"},
		// One key already present refuses the batch, and c must not have been written.
		{"MSETNX b 22 c 3", ":0"},
		{"GET b", "2"},
		{"GET c", "(nil)"},
		{"MSETNX c 3 d 4", ":1"},
		{"GET c", "3"},
		{"MSETNX x 1 y", "-ERR wrong number of arguments for 'msetnx' command"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}
	// An expired key does not count as existing.
	c.cmd("SET gone v PX 1")
	waitFor(t, "the key to expire", func() bool { return c.cmd("GET gone") == "(nil)" })
	if got := c.cmd("MSETNX gone fresh"); got != ":1" {
		t.Errorf("MSETNX over an expired key = %q; want :1", got)
	}
}

// TestMSetNXIsWatchedOnEveryKey covers invariant 7 for a command that writes several
// keys: WATCH has to see a conflict on any of them, not just the first.
func TestMSetNXIsWatchedOnEveryKey(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	watcher := dialTx(t, addr)
	defer watcher.close()
	writer := dialTx(t, addr)
	defer writer.close()

	watcher.cmd("WATCH second")
	watcher.cmd("MULTI")
	watcher.cmd("SET other 1")
	// The write lands on the *second* key of the MSETNX, which is the one a
	// first-argument-only key extraction would miss.
	if got := writer.cmd("MSETNX first 1 second 2"); got != ":1" {
		t.Fatalf("MSETNX = %q; want :1", got)
	}
	if got := watcher.cmd("EXEC"); got != "(nil)" {
		t.Errorf("EXEC = %q; want a null array: MSETNX changed a watched key", got)
	}
}

// TestTimeReadsTheStoreClock covers TIME, and that it shares the clock every other
// server-side "now" reads -- under an injected clock a client comparing TIME against a
// TTL it just set must see one timeline.
func TestTimeReadsTheStoreClock(t *testing.T) {
	st := store.New(8)
	fixed := time.Date(2030, 3, 4, 5, 6, 7, 891011000, time.UTC)
	st.SetClock(func() time.Time { return fixed })
	_, addr, stop := startServer(t, st)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	got := c.cmd("TIME")
	want := "[" + strconv.FormatInt(fixed.Unix(), 10) + " 891011]"
	if got != want {
		t.Errorf("TIME = %q; want %q", got, want)
	}
}

// TestDebugSetActiveExpire covers the switch Redis's own test suite needs to observe
// lazy expiration deterministically. Turning the sweep off must not make a stale value
// readable: the lazy check on every read is what enforces the deadline.
func TestDebugSetActiveExpire(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	if got := c.cmd("DEBUG SET-ACTIVE-EXPIRE 0"); got != "+OK" {
		t.Fatalf("DEBUG SET-ACTIVE-EXPIRE 0 = %q", got)
	}
	c.cmd("SET k v PX 1")
	waitFor(t, "the key to read as gone", func() bool { return c.cmd("GET k") == "(nil)" })
	if got := c.cmd("EXISTS k"); got != ":0" {
		t.Errorf("EXISTS on an expired key with the sweep off = %q; want :0", got)
	}
	if got := c.cmd("DEBUG SET-ACTIVE-EXPIRE 1"); got != "+OK" {
		t.Errorf("DEBUG SET-ACTIVE-EXPIRE 1 = %q", got)
	}
	if got := c.cmd("DEBUG SET-ACTIVE-EXPIRE 2"); !contains(got, "Invalid argument") {
		t.Errorf("DEBUG SET-ACTIVE-EXPIRE 2 = %q; want an argument error", got)
	}
}
