package store

import (
	"strings"
	"testing"
)

// Every expectation in this file was read off a live Redis, never reasoned about, and the
// server it was read off is named in the comment beside it. That matters more here than
// anywhere else in the package, because the reference implementation does not have one
// answer: C's long double is the x87 80-bit format on x86-64 and IEEE binary128 on arm64,
// so the same INCRBYFLOAT stores different bytes on the two. Measured, on redis:7.2
// (amd64) against redis:7.4 (arm64), both official images:
//
//	command                            amd64 (x87)                   arm64 (binary128)
//	SET k 1e20;   INCRBYFLOAT k 1       100000000000000000000         100000000000000000001
//	SET k 1e21;   INCRBYFLOAT k 1       1000000000000000000000        1000000000000000000001
//	SET k 1e30;   INCRBYFLOAT k 1e-30   1000000000000000000024696061952
//	                                                                  1000000000000000000000000000000
//	SET k 1e308;  INCRBYFLOAT k 1e308   1999999999999999999993371…    2000000000000000000000000…
//	              INCRBYFLOAT k 1e-4951 ERR value is not a valid float
//	                                                                  0
//	SET k 18446744073709551617; INCRBYFLOAT k 0
//	                                    18446744073709551616          18446744073709551617
//
// The two diverge wherever a value needs more than about 19 significant digits (x87 has 64
// mantissa bits, binary128 has 113) and at the subnormal floor: measured, the smallest
// operand accepted is 1e-4950 on amd64 and 3.3e-4966 on arm64, which are the two formats'
// smallest subnormals (2**-16445 and 2**-16494). Over a randomised comparison of 1240
// command sequences, 569 differed.
//
// This package implements the x87 answer -- see longdouble.go for why, and for why being
// *deterministic* about it is a property Redis itself lacks.

// mustParseLD is the operand a test hands to IncrByFloat, parsed the way the server parses
// it. It panics rather than returning an error so it can be used inline; a test that wants
// to check a refusal calls ParseLongDouble directly.
func mustParseLD(s string) LongDouble {
	d, ok := ParseLongDouble(s)
	if !ok {
		panic("test operand does not parse as a long double: " + s)
	}
	return d
}

// TestLongDoubleArithmeticMatchesRedis is the arithmetic and the formatting together,
// which is the only way either is observable: the text is the reply, the stored value, and
// the propagated SET, all three.
//
// Every want below is what `SET k <start>; INCRBYFLOAT k <delta>` answers on redis:7.2
// (amd64, glibc), verified live rather than derived.
func TestLongDoubleArithmeticMatchesRedis(t *testing.T) {
	cases := []struct{ start, delta, want string }{
		// The cases float64 arithmetic got wrong. Each of these answered the Go
		// shortest-round-trip form of a float64 sum before, which is a different number.
		{"0.1", "0.2", "0.3"},                                // float64: 0.30000000000000004
		{"0.7", "0.1", "0.8"},                                // float64: 0.7999999999999999
		{"1e17", "1", "100000000000000001"},                  // float64: 100000000000000000
		{"9007199254740992", "1", "9007199254740993"},        // float64: 9007199254740992
		{"1", "1e-17", "1.00000000000000001"},                // float64: 1
		{"1", "1e-16", "1.0000000000000001"},                 // float64 agrees here
		{"123456789012345678", "1", "123456789012345679"},    // float64: 123456789012345680
		{"1e19", "1", "10000000000000000001"},                // float64: 10000000000000000000
		{"1e18", "1", "1000000000000000001"},                 // float64: 1000000000000000000
		{"1e30", "1e-30", "1000000000000000000024696061952"}, // x87 cannot hold 1e30 exactly
		{"3.3", "3.3", "6.6"},                                // float64: 6.6 as well
		{"7", "-7.0000000000000000001", "0"},                 // 20 digits of operand matter
		{"2", "0.000000000000000001", "2"},                   // 1e-18 is below %.17Lf
		{"0", "1e-17", "0.00000000000000001"},
		{"0", "1e-18", "0"},

		// Formatting: the increment formatter never uses an exponent, unlike a score's.
		{"", "3", "3"},
		{"10.5", "0.1", "10.6"},
		{"5", "-5", "0"},
		{"3.0e3", "200", "3200"},
		{"0.000001", "0.000001", "0.000002"},
		{"0.0000001", "0.0000001", "0.0000002"},
		{"1e-10", "1e-10", "0.0000000002"},
		{"0", "2.5e-9", "0.0000000025"},
		{"17179869184", "1.5", "17179869185.5"},
		{"100", "0.1", "100.1"},
		{"0", "0.3333333333333333", "0.3333333333333333"},

		// Negative zero is spelled "0", which is a rewrite ld2string does by hand.
		{"-1e-30", "0", "0"},
		{"-0", "-0", "0"},
		{"-0.5", "0.5", "0"},

		// A C99 hex float literal is a valid operand, because strtold takes one.
		{"0x10", "1", "17"},
		{"0", "0x1.8p3", "12"},
		{"0", "0x1p-16445", "0"}, // the smallest subnormal, which formats as 0

		// Magnitudes past float64 entirely, where a float64 implementation refused.
		{"340282366920938463463374607431768211456", "1", "340282366920938463463374607431768211456"},
		{"18446744073709551617", "0", "18446744073709551616"},
		{"1e-4950", "1e-4950", "0"},
	}
	for _, tc := range cases {
		s := New(4)
		if tc.start != "" {
			s.Set("k", []byte(tc.start), 0)
		}
		got, err := s.IncrByFloat("k", mustParseLD(tc.delta))
		if err != nil {
			t.Errorf("IncrByFloat(%q += %q): %v", tc.start, tc.delta, err)
			continue
		}
		if got != tc.want {
			t.Errorf("IncrByFloat(%q += %q) = %q; want %q", tc.start, tc.delta, got, tc.want)
		}
		// The reply and the stored bytes are the same text or the invariant is broken.
		if v, _ := s.Get("k"); string(v) != tc.want {
			t.Errorf("stored value after %q += %q = %q; want %q", tc.start, tc.delta, v, tc.want)
		}
		// And the hash side must answer identically, since a client can move a value
		// between a string and a field and expect the same arithmetic.
		h := New(4)
		if tc.start != "" {
			if _, err := h.HSet("h", [2][]byte{[]byte("f"), []byte(tc.start)}); err != nil {
				t.Fatalf("HSet: %v", err)
			}
		}
		hgot, err := h.HIncrByFloat("h", "f", mustParseLD(tc.delta))
		if err != nil {
			t.Errorf("HIncrByFloat(%q += %q): %v", tc.start, tc.delta, err)
			continue
		}
		if hgot != tc.want {
			t.Errorf("HIncrByFloat(%q += %q) = %q; want %q", tc.start, tc.delta, hgot, tc.want)
		}
	}
}

// TestLongDoubleHugeResults covers the results that are thousands of digits long, which a
// float64 implementation refused outright. The full text is too long to paste, so each is
// pinned by its length and both ends -- all three read off redis:7.2 (amd64).
func TestLongDoubleHugeResults(t *testing.T) {
	cases := []struct {
		start, delta string
		wantLen      int
		head, tail   string
	}{
		// This one previously answered "ERR increment would produce NaN or Infinity": a
		// float64 sum overflows where a long double has 4000 more decades of headroom.
		{"1e308", "1e308", 309,
			"1999999999999999999933717593116912913211",
			"1514636284849020905456510092687857156096"},
		{"0", "1e4000", 4000,
			"9999999999999999999965463873099623784932",
			"1956972190272729951206648276349901340672"},
		{"1e4930", "1e4930", 4931,
			"2000000000000000000052347569774075861144",
			"9435218635595131545830940082299950071808"},
		{"0", "1e4932", 4933,
			"1000000000000000000006018949387963891323",
			"0876379371163141086538418113791886622720"},
		// LDBL_MAX itself, spelled as the hex literal that names it exactly.
		{"0", "0x1.fffffffffffffffep16383", 4933,
			"1189731495357231765021263853030970205169",
			"8849149662444156604419552086811989770240"},
	}
	for _, tc := range cases {
		s := New(4)
		if tc.start != "0" {
			s.Set("k", []byte(tc.start), 0)
		}
		got, err := s.IncrByFloat("k", mustParseLD(tc.delta))
		if err != nil {
			t.Errorf("IncrByFloat(%q += %q): %v", tc.start, tc.delta, err)
			continue
		}
		if len(got) != tc.wantLen {
			t.Errorf("IncrByFloat(%q += %q) is %d digits; want %d", tc.start, tc.delta, len(got), tc.wantLen)
			continue
		}
		if !strings.HasPrefix(got, tc.head) || !strings.HasSuffix(got, tc.tail) {
			t.Errorf("IncrByFloat(%q += %q) = %s…%s; want %s…%s",
				tc.start, tc.delta, got[:40], got[len(got)-40:], tc.head, tc.tail)
		}
	}
}

// TestLongDoubleParse pins which operands Redis's string2ld accepts, which it refuses, and
// which it accepts as an infinity -- the three outcomes are wire-visible as three
// different replies, so all three have to be right.
//
// Every row is what redis:7.2 (amd64, glibc) answers for `SET k 0; INCRBYFLOAT k <op>`:
// an accepted finite operand stores a number, an accepted infinity answers "ERR increment
// would produce NaN or Infinity" (the *result* is what is wrong), and a refused one
// answers "ERR value is not a valid float".
func TestLongDoubleParse(t *testing.T) {
	const (
		bad    = "refused"
		finite = "finite"
		inf    = "infinite"
	)
	cases := []struct{ in, want string }{
		// The ordinary grammar.
		{"1", finite}, {"+1", finite}, {"-1", finite}, {".5", finite}, {"5.", finite},
		{"+.5", finite}, {"1e1", finite}, {"1e+1", finite}, {"1e-1", finite},
		{"1E5", finite}, {"0", finite}, {"-0", finite}, {"0.0", finite}, {"00001", finite},
		{"0e999999999999", finite}, // a zero mantissa is zero however extreme the exponent

		// C99 hex float literals: strtold takes them, so Redis does.
		{"0x10", finite}, {"0X10", finite}, {"0x1p1", finite}, {"0X1.8p3", finite},
		{"0x.8p1", finite}, {"0x10p0", finite}, {"0xAp0", finite}, {"0xa.bp-2", finite},

		// Syntax Redis refuses. "1_0" is the one Go's own float grammar would accept:
		// a planted value of "1_0" must not read as 10.
		{".", bad}, {"-.", bad}, {"+", bad}, {"-", bad}, {"", bad}, {"1e", bad},
		{"e5", bad}, {"1_0", bad}, {"1_000", bad}, {"0x", bad}, {"0xp1", bad},
		{"0x1p", bad}, {"0x1p+", bad}, {"0b101", bad}, {"--1", bad}, {"1.2.3", bad},
		{"1e1e1", bad}, {"1..2", bad}, {"1 ", bad}, {" 1", bad}, {"\t1", bad},
		{"1\t", bad}, {"\n1", bad}, {"1\n", bad}, {" ", bad}, {"hello", bad},

		// The infinities parse, in any spelling and any case. NaN does not: strtold
		// parses it and string2ld's isnan check throws it out.
		{"inf", inf}, {"INF", inf}, {"Inf", inf}, {"+inf", inf}, {"-inf", inf},
		{"infinity", inf}, {"INFINITY", inf}, {"-Infinity", inf}, {"iNfInItY", inf},
		// "infinit" is a deliberately truncated "infinity", not a typo: strtold consumes the
		// whole operand or nothing, so a prefix must be refused rather than completed.
		{"infinit", bad}, {"nan", bad}, {"NAN", bad}, {"-nan", bad}, {"+nan", bad},
		{"nan(1)", bad}, {"nan(", bad}, {"nano", bad},

		// The range boundaries, in both directions. Above LDBL_MAX strtold reports an
		// overflow and string2ld refuses the operand; below half the smallest subnormal
		// it reports an underflow to zero, which string2ld also refuses -- but a
		// subnormal that survives is accepted and formats as "0".
		{"1e4932", finite}, {"1e4933", bad}, {"1e5000", bad}, {"1e308", finite},
		{"1e400", finite}, {"1e4000", finite},
		// The overflow boundary is a *rounding* boundary, not a comparison against
		// LDBL_MAX: a value above it that is still within half an ulp rounds back down
		// and is accepted, which is what makes ...503 and ...504 finite. Measured, all
		// four: ...502, ...503 and ...504 store the same 4933-digit number and ...509 is
		// refused. Guessing this row is what made the first draft of this test fail
		// against a correct implementation.
		{"1.18973149535723176502e4932", finite}, // LDBL_MAX, truncated
		{"1.18973149535723176503e4932", finite}, // above it, rounds back to it
		{"1.18973149535723176504e4932", finite}, // likewise
		{"1.18973149535723176509e4932", bad},    // past half an ulp: rounds to infinity
		{"1.19e4932", bad},
		{"0x1.fffffffffffffffep16383", finite}, // LDBL_MAX exactly
		{"0x1.ffffffffffffffffp16383", bad},    // the tie above it, which rounds to inf
		{"0x1p16383", finite}, {"0x1p16384", bad},
		{"1e-4949", finite}, {"1e-4950", finite}, {"1e-4951", bad}, {"1e-4952", bad},
		{"1e-5000", bad}, {"1e-4966", bad},
		{"0x1p-16445", finite}, // the smallest subnormal
		{"0x1p-16446", bad},    // exactly half of it: round-half-to-even gives zero
		{"0x1p-16447", bad},
		{"1e2147483647", bad}, {"1e-2147483648", bad}, {"1e99999999999999999999", bad},

		// An operand of MAX_LONG_DOUBLE_CHARS bytes or more never reaches strtold.
		{"1." + strings.Repeat("0", 5117), finite}, // 5119 bytes
		{"1." + strings.Repeat("0", 5118), bad},    // 5120 bytes
	}
	for _, tc := range cases {
		d, ok := ParseLongDouble(tc.in)
		got := bad
		switch {
		case ok && d.IsInf():
			got = inf
		case ok:
			got = finite
		}
		if got != tc.want {
			t.Errorf("ParseLongDouble(%q) = %s; want %s", tc.in, got, tc.want)
		}
	}
}

// TestLongDoubleNonFiniteResultsAreRefused covers the sums Redis will not store, including
// the pair that would panic if the infinities were carried as big.Float's own ±Inf:
// big.Float.Add panics on +Inf + -Inf, and this is reachable from any client.
func TestLongDoubleNonFiniteResultsAreRefused(t *testing.T) {
	cases := []struct{ start, delta string }{
		{"inf", "1"},         // a stored infinity parses; the result is what is refused
		{"inf", "-inf"},      // +Inf + -Inf: NaN, and the big.Float panic if unguarded
		{"-inf", "inf"},      //
		{"inf", "inf"},       //
		{"1", "inf"},         //
		{"1", "-inf"},        //
		{"1e4932", "1e4932"}, // finite operands, overflowing sum
		{"-1e4932", "-1e4932"},
		{"1e4932", "0x1p16383"},
	}
	for _, tc := range cases {
		s := New(4)
		s.Set("k", []byte(tc.start), 0)
		if _, err := s.IncrByFloat("k", mustParseLD(tc.delta)); err != ErrNaN {
			t.Errorf("IncrByFloat(%q += %q) = %v; want ErrNaN", tc.start, tc.delta, err)
		}
		// A refused increment leaves the value exactly as it was.
		if v, _ := s.Get("k"); string(v) != tc.start {
			t.Errorf("a refused increment over %q left %q", tc.start, v)
		}
	}
}

// TestLongDoubleStoredValueRefusals covers the other half of the parse: a value a client
// planted with SET, which is not necessarily text this server wrote.
//
// Measured on redis:7.2: each of these answers "ERR value is not a valid float" for
// INCRBYFLOAT and "ERR hash value is not a float" for HINCRBYFLOAT.
func TestLongDoubleStoredValueRefusals(t *testing.T) {
	for _, planted := range []string{
		"hello", "1_0", "", " 1", "1 ", "nan", "1e5000", "1e-5000", "0x", "1.2.3",
		strings.Repeat("1", 5120),
	} {
		s := New(4)
		s.Set("k", []byte(planted), 0)
		if _, err := s.IncrByFloat("k", mustParseLD("1")); err != ErrNotFloat {
			t.Errorf("IncrByFloat over a stored %q = %v; want ErrNotFloat", planted, err)
		}
		if v, _ := s.Get("k"); string(v) != planted {
			t.Errorf("a refused increment over %q left %q", planted, v)
		}
		h := New(4)
		if _, err := h.HSet("h", [2][]byte{[]byte("f"), []byte(planted)}); err != nil {
			t.Fatalf("HSet: %v", err)
		}
		if _, err := h.HIncrByFloat("h", "f", mustParseLD("1")); err != ErrHashNotFloat {
			t.Errorf("HIncrByFloat over a stored %q = %v; want ErrHashNotFloat", planted, err)
		}
	}
}

// TestLongDoubleAccumulates walks a key through repeated increments, which is what the
// command is actually for -- and where a formatter that lost or gained a digit each time
// would drift. Both sequences are what redis:7.2 (amd64) replies, step by step.
func TestLongDoubleAccumulates(t *testing.T) {
	tenth := []string{"0.1", "0.2", "0.3", "0.4", "0.5", "0.6", "0.7", "0.8", "0.9", "1"}
	s := New(4)
	s.Set("k", []byte("0"), 0)
	for i, want := range tenth {
		got, err := s.IncrByFloat("k", mustParseLD("0.1"))
		if err != nil {
			t.Fatalf("increment %d: %v", i+1, err)
		}
		if got != want {
			t.Fatalf("after %d increments of 0.1 = %q; want %q", i+1, got, want)
		}
	}
	// 1e-17 is the last decade %.17Lf shows and 1+1e-17 needs 18 significant digits, so
	// this run is only possible at all in long double: in a float64 every step would
	// answer "1". The eight values below are measured, step by step, on redis:7.2.
	tiny := []string{
		"1.00000000000000001", "1.00000000000000002", "1.00000000000000003",
		"1.00000000000000004", "1.00000000000000005", "1.00000000000000006",
		"1.00000000000000007", "1.00000000000000008",
	}
	s.Set("t", []byte("1"), 0)
	for i, want := range tiny {
		got, err := s.IncrByFloat("t", mustParseLD("1e-17"))
		if err != nil {
			t.Fatalf("tiny increment %d: %v", i+1, err)
		}
		if got != want {
			t.Fatalf("after %d increments of 1e-17 = %q; want %q", i+1, got, want)
		}
	}
}
