package server

import (
	"context"
	"strings"
	"testing"

	"github.com/Black-third/shardkv/internal/store"
)

// The increment family is the only place in the server where a number is computed in long
// double rather than float64, because Redis computes it there and the *stored bytes* are
// the decimal text of that computation. store/longdouble.go covers which long double and
// why; this file is the wire view: the replies, the exact error strings, and the two copies
// of a key agreeing after a master has incremented it.
//
// Every expectation here was read off a live redis:7.2 (amd64, glibc) on the same command
// sequence, not derived. Two of them were derived in an earlier draft and both were wrong.

// TestIncrByFloatMatchesLongDouble is the precision half, over the wire: the reply and the
// bytes GET reads back afterwards must be one text, and it must be the one Redis produces.
//
// A float64 implementation answered a different number for every row in the first group --
// not a rounding difference in a hidden digit, but a different reply.
func TestIncrByFloatMatchesLongDouble(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ set, incr, want string }{
		{"0.1", "0.2", "0.3"},                             // float64: 0.30000000000000004
		{"0.7", "0.1", "0.8"},                             // float64: 0.7999999999999999
		{"1e17", "1", "100000000000000001"},               // float64: 100000000000000000
		{"9007199254740992", "1", "9007199254740993"},     // float64: 9007199254740992
		{"1", "1e-17", "1.00000000000000001"},             // float64: 1
		{"1", "1e-16", "1.0000000000000001"},              //
		{"123456789012345678", "1", "123456789012345679"}, // float64: 123456789012345680
		{"1e20", "1", "100000000000000000000"},            // beyond float64 entirely
		{"1e30", "1e-30", "1000000000000000000024696061952"},
		// strtold takes a C99 hex float literal, so INCRBYFLOAT does.
		{"0x10", "1", "17"},
		{"0", "0X1.8p3", "12"},
		// The increment formatter never uses an exponent, unlike a score's.
		{"0.0000001", "0.0000001", "0.0000002"},
		{"17179869184", "1.5", "17179869185.5"},
		// Negative zero is spelled "0".
		{"-1e-30", "0", "0"},
	}
	for i, tc := range cases {
		key := "k" + string(rune('a'+i))
		if got := c.cmd("SET " + key + " " + tc.set); got != "+OK" {
			t.Fatalf("SET %s %s = %q", key, tc.set, got)
		}
		if got := c.cmd("INCRBYFLOAT " + key + " " + tc.incr); got != tc.want {
			t.Errorf("INCRBYFLOAT over %s by %s = %q; want %q", tc.set, tc.incr, got, tc.want)
		}
		// The reply and the stored bytes are the same text, which is also the text the
		// propagated SET carries. Reading it back is how all three are checked at once.
		if got := c.cmd("GET " + key); got != tc.want {
			t.Errorf("GET after %s += %s = %q; want %q", tc.set, tc.incr, got, tc.want)
		}
		// The hash side must answer identically for the same numbers.
		if got := c.cmd("HSET h" + key + " f " + tc.set); got != ":1" {
			t.Fatalf("HSET h%s f %s = %q", key, tc.set, got)
		}
		if got := c.cmd("HINCRBYFLOAT h" + key + " f " + tc.incr); got != tc.want {
			t.Errorf("HINCRBYFLOAT over %s by %s = %q; want %q", tc.set, tc.incr, got, tc.want)
		}
	}

	// 1e308 + 1e308 is a 309-digit number, not an overflow: measured on redis:7.2, both
	// commands answer this and STRLEN agrees.
	c.cmd("SET big 1e308")
	got := c.cmd("INCRBYFLOAT big 1e308")
	if len(got) != 309 || !strings.HasPrefix(got, "19999999999999999999") ||
		!strings.HasSuffix(got, "1514636284849020905456510092687857156096") {
		t.Errorf("INCRBYFLOAT 1e308 by 1e308 = %.40s… (%d digits); want the measured 309-digit value", got, len(got))
	}
	if n := c.cmd("STRLEN big"); n != ":309" {
		t.Errorf("STRLEN after the huge increment = %q; want :309", n)
	}
	if v := c.cmd("GET big"); v != got {
		t.Error("the reply and the stored bytes of the huge increment differ")
	}
}

// TestIncrByFloatErrorStrings pins the exact refusals and, as much as it can, their
// ordering. The two commands word an infinite operand differently -- Redis does, and its
// own hash test asserts on it -- so a single message for both would be wrong on one side.
func TestIncrByFloatErrorStrings(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	const notFloat = "-ERR value is not a valid float"
	const notFinite = "-ERR increment would produce NaN or Infinity"
	const hashNotFloat = "-ERR hash value is not a float"
	const operandInf = "-ERR value is NaN or Infinity"

	cases := []struct{ cmd, want string }{
		// A malformed operand is refused before the store is touched, and the syntax is
		// C's, not Go's: "1_0" is a valid Go float literal and must not be read as 10.
		{"SET s 1", "+OK"},
		{"INCRBYFLOAT s abc", notFloat},
		{"INCRBYFLOAT s 1_0", notFloat},
		{"INCRBYFLOAT s nan", notFloat},
		{"INCRBYFLOAT s 0x", notFloat},
		{"INCRBYFLOAT s 1e", notFloat},
		{"GET s", "1"}, // none of them wrote anything

		// An operand past long double's range is refused as an operand, because that is
		// what strtold reports: an overflow, not an infinity.
		{"INCRBYFLOAT s 1e5000", notFloat},
		{"INCRBYFLOAT s 1e-5000", notFloat},
		{"INCRBYFLOAT s 1e-4951", notFloat},
		// One decade lower and it is a surviving subnormal, which is accepted and adds 0.
		{"INCRBYFLOAT s 1e-4950", "1"},

		// An infinite operand *parses* -- string2ld accepts the infinities and rejects
		// only NaN -- so INCRBYFLOAT reports it against the result.
		{"INCRBYFLOAT s inf", notFinite},
		{"INCRBYFLOAT s -inf", notFinite},
		{"INCRBYFLOAT s INFINITY", notFinite},
		{"GET s", "1"},
		// A stored infinity parses the same way, and is caught the same way.
		{"SET i inf", "+OK"},
		{"INCRBYFLOAT i 1", notFinite},
		{"GET i", "inf"},
		// A stored value that does not parse names the value.
		{"SET bad hello", "+OK"},
		{"INCRBYFLOAT bad 1", notFloat},
		{"SET bad2 1_0", "+OK"},
		{"INCRBYFLOAT bad2 1", notFloat},

		// HINCRBYFLOAT refuses an infinite operand as an operand, which is the one place
		// the wording differs. The malformed-operand message is the same as INCRBYFLOAT's,
		// and it still comes first: "nan" is a parse failure, not an infinity.
		{"HSET h f 1", ":1"},
		{"HINCRBYFLOAT h f inf", operandInf},
		{"HINCRBYFLOAT h f -inf", operandInf},
		{"HINCRBYFLOAT h f Infinity", operandInf},
		{"HINCRBYFLOAT h f nan", notFloat},
		{"HINCRBYFLOAT h f 1_0", notFloat},
		{"HINCRBYFLOAT h f 1e5000", notFloat},
		{"HGET h f", "1"},
		// A field that does not parse gets the hash-side message, and an infinite one
		// still reports against the result.
		{"HSET h bad abc", ":1"},
		{"HINCRBYFLOAT h bad 1", hashNotFloat},
		{"HSET h inf inf", ":1"},
		{"HINCRBYFLOAT h inf 1", notFinite},
		{"HGET h inf", "inf"},

		// Arity and wrong type, unchanged by any of this.
		{"INCRBYFLOAT s", "-ERR wrong number of arguments for 'incrbyfloat' command"},
		{"HINCRBYFLOAT h f", "-ERR wrong number of arguments for 'hincrbyfloat' command"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	c.cmd("RPUSH list x")
	for _, cmd := range []string{"INCRBYFLOAT list 1", "HINCRBYFLOAT list f 1"} {
		if got := c.cmd(cmd); got != wrongType {
			t.Errorf("%q on a list = %q; want %q", cmd, got, wrongType)
		}
	}
	// A wrong type is reported after the operand is parsed, so a bad operand on a
	// wrong-typed key still names the operand -- which is the order Redis reports in.
	if got := c.cmd("INCRBYFLOAT list nan"); got != notFloat {
		t.Errorf("INCRBYFLOAT on a list with a bad operand = %q; want %q", got, notFloat)
	}
}

// TestIncrByFloatReplicaConvergence is invariant 4's actual claim: after a run of
// increments on the master, the two copies hold *the same bytes*, not merely the same
// number.
//
// That is not automatic. The increments propagate their result as `SET key <text>`, so the
// text the master stored is the text the replica is told to store -- and if the two sides
// ever computed or formatted the value separately, they would disagree in a digit and
// nothing would report it. This is also where the arithmetic being deterministic pays off:
// a real Redis master and replica on different architectures cannot pass this test, because
// long double differs between them.
func TestIncrByFloatReplicaConvergence(t *testing.T) {
	_, masterAddr, stopM := startServer(t, store.New(8))
	defer stopM()
	mc := dialTx(t, masterAddr)
	defer mc.close()

	replica, replicaAddr, stopR := startServer(t, store.New(8))
	defer stopR()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	replica.ReplicaOf(ctx, masterAddr)

	rc := dialTx(t, replicaAddr)
	defer rc.close()

	mc.cmd("SET sentinel 1")
	waitFor(t, "the replica to attach", func() bool { return rc.cmd("GET sentinel") == "1" })

	// Operands chosen so a float64 would answer differently at nearly every step, and so
	// the accumulated value keeps growing in significant digits.
	mc.cmd("SETEX ttl 100 0.1")
	for _, delta := range []string{"0.2", "1e-17", "0x1.8p3", "1e17", "-0.3", "1e-10",
		"123456789012345678", "1e-16", "-1e-30", "9007199254740992"} {
		mc.cmd("INCRBYFLOAT ttl " + delta)
		mc.cmd("HINCRBYFLOAT hash field " + delta)
	}
	// And the huge magnitudes a float64 could not hold at all.
	mc.cmd("SET huge 1e308")
	mc.cmd("INCRBYFLOAT huge 1e308")
	mc.cmd("HSET hhuge f 1e4000")
	mc.cmd("HINCRBYFLOAT hhuge f 1e4000")

	mc.cmd("SET fence done")
	waitFor(t, "the write stream to drain", func() bool { return rc.cmd("GET fence") == "done" })

	for _, cmd := range []string{
		"GET ttl", "HGET hash field", "GET huge", "STRLEN huge", "HGET hhuge f",
	} {
		m, r := mc.cmd(cmd), rc.cmd(cmd)
		if m != r {
			t.Errorf("%q diverged:\n  master  %.60s… (%d bytes)\n  replica %.60s… (%d bytes)",
				cmd, m, len(m), r, len(r))
		}
	}
	// The propagated form carries KEEPTTL, so the replica's key is still volatile.
	if got := rc.cmd("TTL ttl"); got == ":-1" {
		t.Error("the increment cleared the replica's TTL; the propagated SET needs KEEPTTL")
	}
	// A further increment must reach the same next value on both copies, which is what
	// "the same bytes" is for: a value that read back differently would diverge again from
	// here on. The probe uses a value small enough for 1e-17 to be visible in it -- the
	// accumulated key above has grown to 18 integer digits, where a 1e-17 increment
	// changes nothing at all, on this server and on Redis alike (measured: `SET q
	// 232463988267086682; INCRBYFLOAT q 1e-17` answers 232463988267086682).
	mc.cmd("SET probe 1")
	mc.cmd("INCRBYFLOAT probe 1e-17")
	after := mc.cmd("GET probe")
	if after != "1.00000000000000001" {
		t.Errorf("1 += 1e-17 = %q; want 1.00000000000000001", after)
	}
	waitFor(t, "the last increment to replicate", func() bool { return rc.cmd("GET probe") == after })
}
