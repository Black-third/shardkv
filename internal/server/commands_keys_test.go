package server

import (
	"strconv"
	"testing"
	"time"
)

// TestKeysFiltersByPattern covers KEYS honouring its glob argument. It used to
// ignore the pattern entirely and return the whole keyspace, so a client asking
// for one namespace got every key in the store.
func TestKeysFiltersByPattern(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	for _, k := range []string{"user:1", "user:2", "admin:1", "u", "user"} {
		c.cmd("SET " + k + " v")
	}

	cases := []struct {
		pattern string
		want    []string
		absent  []string
	}{
		{"user:*", []string{"user:1", "user:2"}, []string{"admin:1", "u"}},
		{"*:1", []string{"user:1", "admin:1"}, []string{"user:2"}},
		{"user", []string{"user"}, []string{"user:1", "u"}},
		{"u*", []string{"u", "user", "user:1", "user:2"}, []string{"admin:1"}},
		{"*", []string{"user:1", "user:2", "admin:1", "u", "user"}, nil},
		{"nomatch*", nil, []string{"user:1", "admin:1", "u"}},
	}
	for _, tc := range cases {
		got := c.cmd("KEYS " + tc.pattern)
		for _, k := range tc.want {
			if !containsKey(got, k) {
				t.Errorf("KEYS %s missing %q; got %s", tc.pattern, k, got)
			}
		}
		for _, k := range tc.absent {
			if containsKey(got, k) {
				t.Errorf("KEYS %s should exclude %q; got %s", tc.pattern, k, got)
			}
		}
	}

	// A pattern matching nothing is an empty array, not the whole keyspace.
	if got := c.cmd("KEYS nomatch*"); got != "[]" {
		t.Errorf("KEYS nomatch* = %s; want []", got)
	}
}

// containsKey reports whether a flattened "[a b c]" array reply contains exactly
// the element k, so that "user" does not match inside "user:1".
func containsKey(reply, k string) bool {
	inner := reply
	if len(inner) >= 2 && inner[0] == '[' {
		inner = inner[1 : len(inner)-1]
	}
	for _, f := range splitSpace(inner) {
		if f == k {
			return true
		}
	}
	return false
}

func splitSpace(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ' ' {
			if i > start {
				out = append(out, s[start:i])
			}
			start = i + 1
		}
	}
	return out
}

// TestExpireRejectsOverflowingOperand covers the expire family against operands
// whose arithmetic overflows int64. Unchecked, the multiplication into nanoseconds
// or milliseconds wraps into the past, so a far-future expiry silently deleted the
// key instead of preserving it. Redis reports these as an invalid expire time.
func TestExpireRejectsOverflowingOperand(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		// MaxInt64 seconds/ms: the unit conversion overflows.
		{"SET k v", "+OK"},
		{"EXPIRE k 9223372036854775807", "-ERR invalid expire time in 'expire' command"},
		{"TTL k", ":-1"},
		{"EXISTS k", ":1"},
		{"EXPIREAT k 9223372036854775807", "-ERR invalid expire time in 'expireat' command"},
		{"EXISTS k", ":1"},
		{"PEXPIRE k 9223372036854775807", "-ERR invalid expire time in 'pexpire' command"},
		{"EXISTS k", ":1"},
		{"SET s v EX 9223372036854775807", "-ERR invalid expire time in 'set' command"},
		// Beyond int64 entirely is a range error, as before.
		{"EXPIRE k 99999999999999999999", "-ERR value is not an integer or out of range"},
		{"PEXPIREAT k notanumber", "-ERR value is not an integer or out of range"},
		// A millisecond deadline well past int32 still works: it is a real timestamp.
		{"PEXPIREAT k 4102444800000", ":1"},
		{"EXISTS k", ":1"},
		{"PERSIST k", ":1"},
		// And so does an absolute deadline supplied to SET.
		{"SET pxat v PXAT 4102444800000", "+OK"},
		{"EXISTS pxat", ":1"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}
}

// TestMillisecondDeadlineSurvivesRoundTrip checks that a PEXPIREAT deadline past
// the 32-bit range is stored exactly, which is what parseInt64 (rather than an
// int-wide Atoi) guarantees on every build width.
func TestMillisecondDeadlineSurvivesRoundTrip(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	const deadlineMs int64 = 4102444800000 // 2100-01-01, far past MaxInt32
	c.cmd("SET k v")
	if got := c.cmd("PEXPIREAT k " + strconv.FormatInt(deadlineMs, 10)); got != ":1" {
		t.Fatalf("PEXPIREAT = %q; want :1", got)
	}

	// PTTL must report a positive remaining time in the right ballpark, i.e. the
	// deadline was not truncated into the past.
	reply := c.cmd("PTTL k")
	if len(reply) < 2 || reply[0] != ':' {
		t.Fatalf("PTTL = %q; want an integer reply", reply)
	}
	got, err := strconv.ParseInt(reply[1:], 10, 64)
	if err != nil {
		t.Fatalf("PTTL = %q: %v", reply, err)
	}
	if got <= 0 {
		t.Fatalf("PTTL = %d; want > 0 (a truncated deadline reads as already expired)", got)
	}
}

// TestTTLBeyondDurationRange covers a deadline further out than a time.Duration
// can express. PEXPIREAT accepts any millisecond timestamp, but computing the
// remainder as a Duration saturates at MaxInt64 nanoseconds (~292 years), and
// TTL's round-to-nearest-second then added 500ms to that and wrapped -- so TTL
// answered a large negative number for a key that expires in the year 5138.
func TestTTLBeyondDurationRange(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	const deadlineMs int64 = 99999999999999 // ~year 5138, ~9.8e13 ms out
	c.cmd("SET k v")
	if got := c.cmd("PEXPIREAT k " + strconv.FormatInt(deadlineMs, 10)); got != ":1" {
		t.Fatalf("PEXPIREAT = %q; want :1", got)
	}

	nowMs := time.Now().UnixMilli()
	for _, tc := range []struct {
		cmd         string
		wantAtLeast int64
	}{
		{"PTTL k", deadlineMs - nowMs - 5000},
		{"TTL k", (deadlineMs - nowMs - 5000) / 1000},
	} {
		reply := c.cmd(tc.cmd)
		if len(reply) < 2 || reply[0] != ':' {
			t.Fatalf("%s = %q; want an integer reply", tc.cmd, reply)
		}
		got, err := strconv.ParseInt(reply[1:], 10, 64)
		if err != nil {
			t.Fatalf("%s = %q: %v", tc.cmd, reply, err)
		}
		if got < tc.wantAtLeast {
			t.Errorf("%s = %d; want >= %d (a saturated Duration reads as negative or truncated)",
				tc.cmd, got, tc.wantAtLeast)
		}
	}
}

// TestDecrByMinInt64 covers the one delta that cannot be negated: -MinInt64 wraps
// back to MinInt64, so DECRBY by it used to subtract nothing and instead decrement
// by the full negative range.
func TestDecrByMinInt64(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("SET n 0")
	if got := c.cmd("DECRBY n -9223372036854775808"); got != "-ERR increment or decrement would overflow" {
		t.Errorf("DECRBY by MinInt64 = %q; want an overflow error", got)
	}
	if got := c.cmd("GET n"); got != "0" {
		t.Errorf("GET n = %q; want 0 (the rejected DECRBY must not apply)", got)
	}
	// The mirror image still works: a MaxInt64 delta is representable.
	if got := c.cmd("DECRBY n 9223372036854775807"); got != ":-9223372036854775807" {
		t.Errorf("DECRBY n MaxInt64 = %q", got)
	}
}
