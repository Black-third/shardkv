package server

import (
	"strings"
	"testing"
	"time"
)

// TestGlobStarGroupBound pins the boundary real Redis draws, which shardkv did not draw at
// all before: a pattern with more than 1000 '*' *groups* matches nothing, and a run of
// adjacent stars is one group however long it is.
//
// Every case here was measured against redis:7.2 on amd64/glibc and redis:7.2-alpine on
// arm64, which agreed on all of them, so this is Redis's behaviour and not an
// architecture's. The cases with a short subject are the ones that make the bound visible
// as a *bound*: a "*?"x1001 pattern needs 1001 bytes to match anything at all, so a short
// subject answers "no match" for the ordinary reason and proves nothing.
func TestGlobStarGroupBound(t *testing.T) {
	long := strings.Repeat("a", 2500)
	cases := []struct {
		name    string
		pattern string
		s       string
		want    bool
	}{
		{"1000 groups of *?, long subject", strings.Repeat("*?", 1000), long, true},
		{"1001 groups of *?, long subject", strings.Repeat("*?", 1001), long, false},
		{"1000 groups of *a, long subject", strings.Repeat("*a", 1000), long, true},
		{"1001 groups of *a, long subject", strings.Repeat("*a", 1001), long, false},
		{"5000 groups of *?, long subject", strings.Repeat("*?", 5000), strings.Repeat("a", 12000), false},
		// Adjacent stars collapse into one group, which is the half that is easy to get
		// wrong: bounding the star *count* would refuse these, and Redis matches them.
		{"1001 adjacent stars", strings.Repeat("*", 1001) + "a", "a", true},
		{"100000 adjacent stars", strings.Repeat("*", 100000), strings.Repeat("a", 50), true},
		{"1001 groups written **?", strings.Repeat("**?", 1001), long, false},
		{"1000 groups written **?", strings.Repeat("**?", 1000), long, true},
		// No stars at all: the bound is about groups, so a pattern of 1001 '?' is untouched.
		{"1001 question marks", strings.Repeat("?", 1001), strings.Repeat("a", 1001), true},
		{"a plain star, long subject", "*", long, true},
	}
	for _, c := range cases {
		if got := globMatch(c.pattern, c.s); got != c.want {
			t.Errorf("%s: globMatch(<%dB pattern>, <%dB subject>) = %v; want %v",
				c.name, len(c.pattern), len(c.s), got, c.want)
		}
	}
}

// TestGlobWorkBudget covers the second bound, which Redis does not have: the quadratic
// backtracking a single star followed by a long literal produces.
//
// The measured attack this stops is `KEYS *<256KB of 'a'>b` against one 512KB key, which
// took 1m46s of CPU for 768KB of input before the budget existed -- and 4m22s on real
// redis:7.2, so the shape is not a shardkv weakness, only one it is cheap enough to fix.
func TestGlobWorkBudget(t *testing.T) {
	// The hostile shape, at a size whose unbounded cost is ~7.6s. It must now be fast, and
	// it must still answer what it answered before: no match.
	k := 64000
	pattern := "*" + strings.Repeat("a", k) + "b"
	subject := strings.Repeat("a", 2*k)
	start := time.Now()
	got := globMatch(pattern, subject)
	el := time.Since(start)
	if got {
		t.Errorf("globMatch(%q..., a{%d}) = true; want false", pattern[:8], 2*k)
	}
	// Two orders of magnitude of headroom over the ~90ms the budget allows at this size, so
	// this is a check that the bound exists at all rather than a timing assertion that would
	// flake on a loaded machine.
	if el > 5*time.Second {
		t.Errorf("the hostile pattern took %v; the work budget did not bound it", el)
	}

	// And the property that makes the budget safe to apply at all: the most work a match can
	// possibly do is len(pattern)*len(subject), so wherever that product is at or under the
	// budget the answer is provably unchanged. These are the two corners of the envelope the
	// constants are documented as covering, each in the shape that maximises backtracking --
	// a star followed by a run that nearly matches from every offset -- so each really does
	// spend the work rather than merely being long.
	//
	// A 1 KiB pattern against a 16 KiB subject: 1024*16384 = 2^24 = globMaxWork.
	kib := strings.Repeat("z", 1023)
	if !globMatch("*"+kib, strings.Repeat("z", 16*1024-1023)+kib) {
		t.Error("a 1KiB pattern against a 16KiB subject was truncated by the work budget")
	}
	// A 256-byte pattern against a 64 KiB subject: 256*65536 = 2^24 again.
	quarter := strings.Repeat("z", 255)
	if !globMatch("*"+quarter, strings.Repeat("z", 64*1024-255)+quarter) {
		t.Error("a 256-byte pattern against a 64KiB subject was truncated by the work budget")
	}
	// A 64-byte pattern against a 256 KiB subject: 64*262144 = 2^24 once more, and here the
	// per-byte factor allows 1024*(2^18+64) too, so both halves have to hold.
	short := strings.Repeat("z", 63)
	if !globMatch("*"+short, strings.Repeat("z", 256*1024-63)+short) {
		t.Error("a 64-byte pattern against a 256KiB subject was truncated by the work budget")
	}
	// And the ordinary end of it: nothing a client would ever send is near the envelope.
	if !globMatch("*"+strings.Repeat("z", 40), strings.Repeat("z", 4000)) {
		t.Error("a 40-byte pattern against a 4KB subject was truncated by the work budget")
	}
}

// TestGlobOrdinaryPatternsUnaffected is the regression guard for the bounds' cost and
// meaning: the patterns clients actually send must answer exactly what they answered
// before, and the paths that carry them -- KEYS, SCAN MATCH, HSCAN, PUBSUB CHANNELS,
// pattern subscriptions -- all go through this one function.
func TestGlobOrdinaryPatternsUnaffected(t *testing.T) {
	cases := []struct {
		pattern, s string
		want       bool
	}{
		{"*", "user:1234:profile", true},
		{"", "", true},
		{"", "a", false},
		{"*", "", true},
		{"user:*", "user:1234:profile", true},
		{"user:*", "session:1", false},
		{"user:*:profile", "user:1234:profile", true},
		{"user:*:profile", "user:1234:token", false},
		{"*:profile", "user:1234:profile", true},
		{"*1234*", "user:1234:profile", true},
		{"user:????", "user:1234", true},
		{"user:????", "user:12345", false},
		{"?", "a", true},
		{"?", "", false},
		{"*a*b*c*", "xaybzc!", true},
		{"*a*b*c*", "xaybz", false},
		{"__keyspace@0__:*", "__keyspace@0__:k", true},
		{"h?llo", "hello", true},
		{"h*llo", "heeeello", true},
		{"h*llo", "hallox", false},
	}
	for _, c := range cases {
		if got := globMatch(c.pattern, c.s); got != c.want {
			t.Errorf("globMatch(%q, %q) = %v; want %v", c.pattern, c.s, got, c.want)
		}
	}
}

// BenchmarkGlobMatch is what the bounds' cost is judged on. The ordinary cases are the ones
// that matter: the matcher runs once per key of a KEYS and once per element of an HSCAN, so
// a cost added there is multiplied by the keyspace.
func BenchmarkGlobMatch(b *testing.B) {
	key := "user:1234:profile"
	long := strings.Repeat("k", 512)
	cases := []struct {
		name, pattern, s string
	}{
		{"star", "*", key},
		{"prefix", "user:*", key},
		{"middle", "user:*:profile", key},
		{"suffix", "*profile", key},
		{"nomatch", "session:*", key},
		{"questions", "user:????:profile", key},
		{"multistar", "*a*b*c*", "xaybzc!"},
		{"longkey", "*:v1", long + ":v1"},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if globMatch(c.pattern, c.s) == (i < 0) {
					b.Fatal("unreachable")
				}
			}
		})
	}
}
