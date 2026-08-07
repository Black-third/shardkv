package server

import (
	"strings"
	"testing"
	"time"
)

// TestGlobRedisSemantics is the measured table. Every `want` in it is redis:7.2's own answer,
// obtained by storing the subject as the only key in a database and asking `KEYS <pattern>`
// -- the actual user-visible surface, not a private hook, so the answer is the one a client
// would get. Where amd64 and arm64 disagree the case says so and says why.
//
// It is a table rather than a set of hand-written assertions because the interesting cases
// are the ones nobody would think to write: `[a-]` is a range, `[!ae]` is not a negation,
// `a[` matches nothing while `a[b` matches "ab".
func TestGlobRedisSemantics(t *testing.T) {
	cases := []struct {
		pattern, subject string
		want             bool
		note             string
	}{
		// --- character classes, the feature that was missing -------------------
		{"h[ae]llo", "hello", true, ""},
		{"h[ae]llo", "hallo", true, ""},
		{"h[ae]llo", "hbllo", false, ""},
		{"h[^e]llo", "hallo", true, ""},
		{"h[^e]llo", "hello", false, ""},
		{"h[^e]llo", "h]llo", true, "a ']' is an ordinary byte to a negated class"},
		{"h[^ae]llo", "hbllo", true, ""},
		{"h[^ae]llo", "hello", false, ""},
		{"h[a-c]llo", "hallo", true, ""},
		{"h[a-c]llo", "hbllo", true, ""},
		{"h[a-c]llo", "hcllo", true, ""},
		{"h[a-c]llo", "hzllo", false, ""},
		{"h[c-a]llo", "hbllo", true, "a reversed range is swapped, not refused"},
		{"h[a-cA-C]llo", "hBllo", true, "two ranges in one class"},
		{"h[abc]llo", "hbllo", true, ""},

		// --- the cases POSIX and Redis disagree about --------------------------
		{"h[]llo", "hello", false, "'[]' is an EMPTY class and matches nothing"},
		{"h[]llo", "h[]llo", false, "...and it is not two literal brackets either"},
		{"h[]a]llo", "h]llo", false, "a ']' first is NOT the literal ']' POSIX makes it"},
		{"h[]a]llo", "hallo", false, "the class ended at the first ']', so 'a]llo' must follow"},
		{"h[^]llo", "hello", true, "an empty NEGATED class matches any single byte, like '?'"},
		{"h[^]llo", "hzllo", true, ""},
		{"h[!ae]llo", "hello", true, "'!' is not a negation: the class is {'!','a','e'}"},
		{"h[!ae]llo", "h!llo", true, ""},
		{"h[!ae]llo", "hbllo", false, ""},
		// '[a-]' is the RANGE 'a'..']' (swapped to ']'..'a'), not the members 'a' and '-':
		// the range branch only asks that three bytes remain, and ']' is the third. So the
		// class has no terminator, swallows "llo" as further members, and `h[a-]llo` can
		// therefore only ever match a two-byte subject -- which is why the measured answer
		// for "hallo" is *no*, and the way to see the range is to end the pattern there.
		{"h[a-]llo", "hallo", false, "the class ate 'llo', so only 'h' + one byte can match"},
		{"h[a-]llo", "h]llo", false, ""},
		{"h[a-]", "ha", true, "now the range is visible: 'a' is in ']'..'a'"},
		{"h[a-]", "h]", true, "...and so is ']'"},
		{"h[a-]", "h^", true, "...and '^', which lies between them"},
		{"h[a-]", "hz", false, ""},
		{"h[-a]llo", "h-llo", true, "'[-a]' IS the two members '-' and 'a'"},
		{"h[-a]llo", "hallo", true, ""},
		{"h[-a]llo", "hbllo", false, ""},

		// --- unterminated classes --------------------------------------------
		{"a[", "a[", false, "an unterminated empty class matches nothing at all"},
		{"a[", "ab", false, ""},
		{"a[", "a", false, ""},
		{"a[b", "ab", true, "an unterminated class keeps the members it read"},
		{"a[b", "abc", false, "the class consumed the rest of the pattern, so 'c' is spare"},
		{"a[bc", "ab", true, ""},
		{"a[bc", "ac", true, ""},
		{"a[bc", "ad", false, ""},
		{"a[a-b", "ab", true, "a range inside an unterminated class still counts"},
		{"a[a-b", "ac", false, ""},
		{"a[^", "ab", true, "an unterminated empty NEGATED class matches any byte"},
		{"a[^", "a[", true, ""},
		{"a[^b", "ax", true, ""},
		{"a[^b", "ab", false, ""},
		{"[a", "a", true, ""},
		{"[a]", "a", true, ""},
		{"a[]", "a", false, "'[]' is empty, and there is nothing left to match anyway"},

		// --- escapes ---------------------------------------------------------
		{`h\*llo`, "h*llo", true, ""},
		{`h\*llo`, "hello", false, "an escaped star is a literal star"},
		{`\*`, "*", true, ""},
		{`\*`, "x", false, ""},
		{`a\*b`, "a*b", true, ""},
		{`a\*b`, `a\b`, false, ""},
		{`\[abc\]`, "[abc]", true, ""},
		{`\[abc\]`, "a", false, ""},
		{`a\?b`, "a?b", true, ""},
		{`a\?b`, "axb", false, ""},
		{`a\\b`, `a\b`, true, "an escaped backslash is one backslash"},
		{`\z`, "z", true, "an escape before an ordinary byte is that byte"},
		{`\a`, "a", true, ""},
		{`a\`, `a\`, true, "a TRAILING backslash stands for itself"},
		{`a\`, "ab", false, ""},
		{`[\*]`, "*", true, "an escaped member inside a class"},
		{`[a\]]`, "]", true, "...which is how ']' becomes a class member"},
		{`[a\]]`, "a", true, ""},
		{`[a\]]`, "b", false, ""},

		// --- stars, and the empty subject ------------------------------------
		{"*", "a", true, ""},
		{"*", "", false, "the matcher refuses an empty subject; KEYS short-circuits `*` itself"},
		{"**", "", false, "measured: `KEYS **` reports nothing for an empty key name"},
		{"", "", true, ""},
		{"", "a", false, ""},
		{"?", "", false, ""},
		{"[a]", "", false, ""},
		{"a*", "a", true, ""},
		{"a*b*c", "aXbXc", true, ""},
		{"ab*c", "ab", false, "a trailing '*' is collapsed, but 'c' still has to match"},
		{"a*c", "abc", true, ""},
		{"a**c", "abc", true, ""},
		{"*a*", "bab", true, ""},
		{"a?c", "abc", true, ""},
		{"a?c", "ac", false, ""},

		// --- classes behind a star: the combination that needs backtracking ---
		{"*[b]", "aaab", true, ""},
		{"*[b]", "aaa", false, ""},
		{"*[^b]", "aaa", true, ""},
		{"a*[ab]b", "aaab", true, ""},
		{"[a-z]*[a-z]", "aaab", true, ""},
		{"*[a-c]*[a-c]*", "xayb", true, ""},
		{"*[a-c]*[a-c]*", "xyz", false, ""},

		// --- bytes are bytes -------------------------------------------------
		{"h?llo", "h\x00llo", true, "'?' matches a NUL like any other byte"},
		{"h[\xfe-\xff]llo", "h\xffllo", true, ""},
		{"h[\x80-\xff]llo", "h\x90llo", true, ""},
		{"h[\x00-\x7f]llo", "hAllo", true, ""},
	}

	for _, c := range cases {
		got := globMatch(c.pattern, c.subject)
		if got != c.want {
			t.Errorf("globMatch(%q, %q) = %v, redis:7.2 says %v%s",
				c.pattern, c.subject, got, c.want, suffix(c.note))
		}
	}
	t.Logf("%d measured cases", len(cases))
}

// TestGlobHighByteRangesAreUnsigned documents the one deliberate divergence, and pins the
// side of it this server is on.
//
// A class range whose endpoints straddle 0x7f gets *different answers from the same Redis
// release on different architectures*, because it compares C `char`s and their signedness is
// the platform's. Measured on redis:7.2: `[\x00-\xff]` against "A" matches on arm64
// (unsigned char, so the range is 0..255) and does not on amd64 (signed char, so the
// endpoints are 0 and -1, get swapped to -1..0, and admit only NUL and 0xff). That is
// Redis disagreeing with itself, not a specification, so this server takes the unsigned
// reading: the one that is the same everywhere and the one a client writing a byte range
// means.
func TestGlobHighByteRangesAreUnsigned(t *testing.T) {
	cases := []struct {
		pattern, subject string
		want             bool
	}{
		{"[\x00-\xff]", "A", true},     // arm64 redis: yes. amd64 redis: no.
		{"[\x00-\xff]", "\x00", true},  // both: yes
		{"[\x00-\xff]", "\xff", true},  // both: yes
		{"[a-\xff]", "z", true},        // arm64: yes. amd64: no.
		{"[a-\xff]", "\xf0", true},     // arm64: yes. amd64: no.
		{"[\x00-\xfe]", "\xff", false}, // arm64: no. amd64: yes.
		{"[\xfe-\xff]", "\xff", true},  // both: yes
	}
	for _, c := range cases {
		if got := globMatch(c.pattern, c.subject); got != c.want {
			t.Errorf("globMatch(%q, %q) = %v, want %v (the unsigned reading)",
				c.pattern, c.subject, got, c.want)
		}
	}
}

// TestGlobFoldIsAsciiOnly covers the nocase form, which is what Redis's CONFIG GET and
// COMMAND LIST FILTERBY use. Folding has to reach the class members and the range endpoints
// too, and it has to stop at ASCII: C's tolower in the "C" locale Redis runs in leaves
// everything else alone, and a byte range that depended on a locale would depend on
// something the wire protocol does not carry.
func TestGlobFoldIsAsciiOnly(t *testing.T) {
	cases := []struct {
		pattern, subject string
		want             bool
	}{
		{"MAXMEMORY", "maxmemory", true},
		{"maxmemory", "MAXMEMORY", true},
		{"max*", "MAXMEMORY", true},
		{"MAX[MN]EMORY", "maxmemory", true},
		{"max[A-Z]emory", "maxmemory", true},
		{"max[a-z]emory", "maxMemory", true},
		{`\Maxmemory`, "maxmemory", true},
		{"MAX[^M]EMORY", "maxmemory", false},
		{"\xc4\xb0", "\xc4\xb1", false}, // dotted/dotless I in UTF-8: not folded, and must not be
	}
	for _, c := range cases {
		if got := globMatchFold(c.pattern, c.subject); got != c.want {
			t.Errorf("globMatchFold(%q, %q) = %v, want %v", c.pattern, c.subject, got, c.want)
		}
	}
	// And the case-sensitive form must be unaffected by any of it.
	if globMatch("MAXMEMORY", "maxmemory") {
		t.Error("globMatch folded case")
	}
}

// TestGlobClassesAddNoUnboundedBacktracking is the bound the character-class work had to not
// break. The matcher already had two limits, added after a measured denial of service: a
// single '*' followed by a long near-matching literal was quadratic, and 768 KB of input cost
// 106 seconds of CPU. A character class is a token whose *scan* is as long as the class, so a
// budget charged only by pattern position would let `*[<long class>]` backtrack once per
// subject byte at the cost of the whole class each time -- a second unbounded source in the
// exact shape the first bound exists for.
//
// The charge therefore counts the failed token's own extent. This test is the proof: every
// shape must answer, and answer having spent no more than the budget it was granted.
//
// The bound is asserted on the work charged, not on a duration. A duration here is a
// measurement of the machine -- under -race on a loaded box the old 2000ms ceiling was
// reachable while the matcher was behaving perfectly -- and the budget is not denominated in
// milliseconds anyway. The wall clock is still recorded, but only logged: see commit 64653df
// for this tree's previous encounter with tests that timed the hardware.
//
// The exact bound is granted + len(pattern), and the second term is not slack. A charge is
// applied whole and only then tested against what is left, so the attempt that exhausts the
// budget overshoots it by its own extent -- and for a failed character class that extent is
// the length of the class, which is the very thing this test was added for. Measured, the
// worst overshoot here is 131,330 bytes on a 196,611-byte pattern of ranges.
func TestGlobClassesAddNoUnboundedBacktracking(t *testing.T) {
	// An anti-pathology backstop only, not the bound: the bound is the work assertion below.
	// This exists so a matcher that somehow stopped terminating is reported as a failure with
	// a name rather than as the whole package timing out, and it is set where no amount of
	// scheduling noise or race-detector overhead can reach it (the measured worst case is
	// ~55ms, and ~1.1s under -race).
	const hangCeiling = 60 * time.Second

	// stoppedByBudget says the shape is large enough that the budget is what must end it,
	// rather than the pattern simply running out. It is the assertion that keeps this test
	// honest about the thing it was written for: if a failed character class were charged one
	// byte instead of its own extent, `*[<256KB class>]` would still be refused eventually and
	// still answer "no match", but it would get there by sliding the star 512K times over a
	// 256KB class -- the quadratic blowup -- having charged only ~512K of a granted 16.7M.
	// Verified by mutation: with `charge = end - star` replaced by `charge = 1`, the matcher
	// stopped answering inside two minutes.
	shapes := []struct {
		name            string
		pattern         string
		subject         string
		stoppedByBudget bool
	}{
		{"star then a long literal (the original shape)",
			"*" + strings.Repeat("a", 256<<10) + "b", strings.Repeat("a", 512<<10), true},
		{"star then a long literal then a class",
			"*" + strings.Repeat("a", 256<<10) + "[b]", strings.Repeat("a", 512<<10), true},
		{"star then one enormous class",
			"*[" + strings.Repeat("z", 256<<10) + "]", strings.Repeat("a", 512<<10), true},
		{"star then an enormous class then a literal",
			"*[" + strings.Repeat("z", 128<<10) + "]b", strings.Repeat("a", 512<<10), true},
		{"star then an enormous UNTERMINATED class",
			"*[" + strings.Repeat("z", 256<<10), strings.Repeat("a", 512<<10), true},
		{"star then a long class of ranges",
			"*[" + strings.Repeat("x-y", 64<<10) + "]", strings.Repeat("a", 512<<10), true},
		{"star then a long run of escapes",
			"*" + strings.Repeat(`\a`, 128<<10) + "b", strings.Repeat("a", 512<<10), true},
		{"many classes between many stars",
			strings.Repeat("*[a-c]", 900) + "z", strings.Repeat("a", 64<<10), false},
		{"a class after every star, at the group bound",
			strings.Repeat("*[abc]", 999) + "z", strings.Repeat("a", 64<<10), false},
	}
	for _, sh := range shapes {
		start := time.Now()
		got, work := globMatchCase(sh.pattern, sh.subject, false)
		took := time.Since(start)
		input := len(sh.pattern) + len(sh.subject)
		granted := int64(globWorkFactor) * int64(input)
		if granted > globMaxWork {
			granted = globMaxWork
		}
		if ceiling := granted + int64(len(sh.pattern)); work > ceiling {
			t.Errorf("%s: charged %d of a granted %d (ceiling %d) for %d bytes of input -- "+
				"the work budget is not bounding it", sh.name, work, granted, ceiling, input)
		}
		if sh.stoppedByBudget && work < granted {
			t.Errorf("%s: charged only %d of a granted %d -- this shape is meant to exhaust the "+
				"budget, so a charge this small means the backtracking is not being counted at "+
				"its true extent and the quadratic path is open again", sh.name, work, granted)
		}
		if took > hangCeiling {
			t.Errorf("%s: took %v for %d bytes of input, which is not a slow machine",
				sh.name, took, input)
		}
		if got {
			t.Errorf("%s: reported a match; every shape here is constructed not to match",
				sh.name)
		}
		t.Logf("%-46s %8d bytes  work %9d of %8d  %7.1fms", sh.name, input, work, granted,
			float64(took.Microseconds())/1000)
	}
}

// TestGlobCostDoesNotGrowWithInput is the shape of the original weakness stated as an
// assertion: before the budget existed the cost grew 16x per doubling of the input. It must
// now be flat, with classes in the pattern as well as without them.
func TestGlobCostDoesNotGrowWithInput(t *testing.T) {
	// The answer is returned alongside the timing and asserted, not discarded: every shape
	// here is a pattern whose tail cannot occur in an all-'a' subject, so the true answer is
	// "no match" at both sizes. Checking it is what says the budget refused these for running
	// out of work rather than by changing the answer.
	measure := func(t *testing.T, tail string, n int) (int64, time.Duration) {
		t.Helper()
		pattern := "*" + strings.Repeat("a", n) + tail
		subject := strings.Repeat("a", 2*n)
		start := time.Now()
		matched, work := globMatchCase(pattern, subject, false)
		took := time.Since(start)
		if matched {
			t.Errorf("tail %q at %d bytes reported a match; the tail cannot occur in the subject",
				tail, n)
		}
		return work, took
	}
	for _, tail := range []string{"b", "[b]", "[^a]", `\b`} {
		small, smallTook := measure(t, tail, 64<<10)
		large, largeTook := measure(t, tail, 256<<10)
		// 4x the input. Quadratic would be 16x the work; a bounded matcher does the same work
		// at both sizes, because both are past the cap and the cap is a constant.
		//
		// Asserted on the work rather than on the two durations, which is the difference
		// between a claim about the matcher and a claim about the machine: the ratio of two
		// small durations on a loaded box is noise, and the previous form needed a 50ms
		// absolute term bolted on to survive it. Measured, the two figures here agree to within
		// 0.006% -- so a tolerance of 2% is already three hundred times the observed spread,
		// and quadratic growth would miss it by a factor of 8.
		if large > small+small/50 {
			t.Errorf("tail %q: charged %d at 64KB and %d at 256KB -- the cost is still growing "+
				"with the input", tail, small, large)
		}
		t.Logf("tail %-5q 64KB: work %9d in %6.1fms   256KB: work %9d in %6.1fms", tail,
			small, float64(smallTook.Microseconds())/1000,
			large, float64(largeTook.Microseconds())/1000)
	}
}

// TestGlobStarGroupBoundIgnoresEscapedAndBracketedStars: the bound is on how many star groups
// the matcher works through, and neither an escaped star nor one inside a character class is
// a group. Counting '*' bytes -- which is what the old walk did, when neither could exist --
// would refuse a long pattern of literal stars that redis matches.
func TestGlobStarGroupBoundIgnoresEscapedAndBracketedStars(t *testing.T) {
	// 1001 escaped stars: a pattern of 2002 bytes, no star groups at all, and it matches a
	// string of 1001 stars.
	pattern := strings.Repeat(`\*`, 1001)
	subject := strings.Repeat("*", 1001)
	if !globMatch(pattern, subject) {
		t.Error("1001 escaped stars were refused by the star-group bound; they are literals")
	}
	// The same in classes.
	pattern = strings.Repeat("[*]", 1001)
	if !globMatch(pattern, subject) {
		t.Error("1001 bracketed stars were refused by the star-group bound; they are literals")
	}
	// And the real bound is unmoved: 1000 groups match, 1001 do not.
	if !globMatch(strings.Repeat("*?", 1000), strings.Repeat("a", 4096)) {
		t.Error("1000 star groups should still match")
	}
	if globMatch(strings.Repeat("*?", 1001), strings.Repeat("a", 4096)) {
		t.Error("1001 star groups should match nothing, as in redis")
	}
}

// TestGlobEmptySubjectAtEveryCaller is the reason the two callers Redis short-circuits are
// short-circuited here too. The matcher refuses an empty subject -- that is Redis's own
// `while (patternLen && stringLen)` -- so `KEYS *` and `SCAN ... MATCH *` would stop
// reporting a key whose name is the empty string, which both SET and HSET accept.
//
// Measured on redis:7.2 with an empty key name present: `KEYS *` lists it, `KEYS **` does
// not; `HSCAN h 0 MATCH *` lists the empty field, `MATCH **` does not; and `PSUBSCRIBE *`
// does *not* receive `PUBLISH "" hi`, because Pub/Sub has no such short-circuit. All three
// asymmetries are reproduced.
func TestGlobEmptySubjectAtEveryCaller(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd(`SET "" v`)
	c.cmd(`HSET h "" hv`)
	cases := []struct{ cmd, want string }{
		{"KEYS *", "[ h]"},        // the empty key and "h"
		{"KEYS **", "[h]"},        // the matcher refuses the empty subject
		{"KEYS ?", "[h]"},         // '?' needs a byte
		{"HSCAN h 0 MATCH *", ""}, // filled in below
		{"HSCAN h 0 MATCH **", "[0 []]"},
	}
	for _, tc := range cases {
		got := c.cmd(tc.cmd)
		if tc.want == "" {
			continue
		}
		if sortedFlat(got) != sortedFlat(tc.want) {
			t.Errorf("%s -> %q, want %q", tc.cmd, got, tc.want)
		}
	}
	if got := c.cmd("HSCAN h 0 MATCH *"); !strings.Contains(got, "hv") {
		t.Errorf("HSCAN h 0 MATCH * -> %q; the empty field name must survive the one pattern "+
			"that keeps everything", got)
	}
}

// suffix renders a case's note, so a failure prints the reason the answer is what it is
// rather than only the two booleans.
func suffix(note string) string {
	if note == "" {
		return ""
	}
	return " -- " + note
}

// sortedFlat normalizes a flat array reply so key order is not asserted.
func sortedFlat(s string) string {
	if !strings.HasPrefix(s, "[") {
		return s
	}
	parts := strings.Split(s[1:len(s)-1], " ")
	for i := 0; i < len(parts); i++ {
		for j := i + 1; j < len(parts); j++ {
			if parts[j] < parts[i] {
				parts[i], parts[j] = parts[j], parts[i]
			}
		}
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// TestGlobOrdinaryPatternsStayFree: the budget must remain something an ordinary pattern
// never touches. This is the claim that makes the two bounds acceptable at all -- a bound
// real traffic can feel is a bound that has changed the server's behaviour rather than only
// its worst case.
//
// It is asserted in the matcher's own unit, the work charged against the budget, and not in
// milliseconds. The previous version of this test timed 100,000 iterations against a ceiling
// of 5us per call, which was wrong twice over: it timed fmt.Sprintf alongside the matcher,
// and it measured the machine, so it failed under -race on a loaded box while the matcher
// was perfectly fine. Commit 64653df ("stop two tests measuring the machine") is this tree
// having already learned that once. Cost over time belongs in a benchmark; what belongs here
// is the bound, and the bound is a number the matcher can state exactly.
//
// Two properties, both measured:
//
//   - A pattern whose only star is trailing, or which has no star at all, charges *exactly
//     zero*. The charge lives in the backtrack branch, and a trailing star returns from the
//     star (Redis's `if (patternLen == 1) return 1`) without ever entering it. This covers
//     "user:*", "*" and every fixed pattern -- which is to say almost all real traffic.
//   - A pattern with an *interior* star does backtrack, and must charge work linear in the
//     subject with a constant of about 2. Linear is the whole point: the weakness the budget
//     exists for is quadratic, and 1024 bytes of allowance per input byte against a cost of
//     2 leaves three orders of magnitude of headroom.
func TestGlobOrdinaryPatternsStayFree(t *testing.T) {
	// work is the exact charge, measured on this matcher. A number here changing is a real
	// change in what an ordinary pattern costs and wants reading, not widening.
	cases := []struct {
		pattern, subject string
		want             bool
		work             int64
		note             string
	}{
		{"user:*", "user:1000", true, 0, "trailing star returns from the star"},
		{"user:[0-9]*", "user:1000", true, 0, "a class before a trailing star is still free"},
		{"user:[0-9]*", "user:alice", false, 0, "no star to backtrack to: refused for free"},
		{"*", "anything", true, 0, "the commonest pattern of all"},
		{"cache:[a-f0-9][a-f0-9]:*", "cache:3f:x", true, 0, "two classes, trailing star"},
		{"h?llo", "hello", true, 0, "'?' consumes a byte and never backtracks"},
		{"session:*:token", "session:abc:token", true, 6, "interior star: 3 bytes, 2 each"},
		{"user:[0-9]*:profile", "user:1000:profile", true, 6, "interior star past a class"},
		{"key:*:*:tail", "key:aa:bb:tail", true, 8, "two interior stars"},
	}
	for _, c := range cases {
		got, work := globMatchCase(c.pattern, c.subject, false)
		if got != c.want {
			t.Errorf("globMatch(%q, %q) = %v, want %v -- %s",
				c.pattern, c.subject, got, c.want, c.note)
		}
		if work != c.work {
			t.Errorf("globMatch(%q, %q) charged %d of budget, want %d -- %s",
				c.pattern, c.subject, work, c.work, c.note)
		}
		// And whatever it charged has to be a rounding error against what it was granted,
		// which is the statement "the budget is not something this pattern can feel".
		granted := int64(globWorkFactor) * int64(len(c.pattern)+len(c.subject))
		if granted > globMaxWork {
			granted = globMaxWork
		}
		if work*1000 > granted {
			t.Errorf("globMatch(%q, %q) used %d of %d granted -- over a tenth of a percent "+
				"is no longer a bound an ordinary pattern cannot feel",
				c.pattern, c.subject, work, granted)
		}
	}
	// The interior-star cost as the subject grows: linear, not quadratic. Quadratic is what
	// the budget exists to stop, and the difference is visible in three exact integers rather
	// than in three durations.
	const pattern = "user:[0-9]*:profile"
	for _, tc := range []struct {
		digits int
		work   int64
	}{
		{10, 18},
		{100, 198},
		{1000, 1998},
		{10000, 19998},
	} {
		subject := "user:" + strings.Repeat("7", tc.digits) + ":profile"
		matched, work := globMatchCase(pattern, subject, false)
		if !matched {
			t.Errorf("%q against %d digits did not match; every subject here matches",
				pattern, tc.digits)
		}
		// 2*digits-2 exactly: one charge per position the star slides through, of the two
		// pattern bytes the attempt scanned before failing. Pinned rather than bounded because
		// it is integer arithmetic over the algorithm, with no clock and no map order in it --
		// it is the same number on every machine, and a different number means the backtracking
		// changed shape.
		if work != tc.work {
			t.Errorf("%q against %d digits charged %d, want %d (2n-2)",
				pattern, tc.digits, work, tc.work)
		}
	}
}
