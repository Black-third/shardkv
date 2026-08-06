package server

import (
	"errors"
	"math"
	"strconv"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

// writeStoreErr translates a store error into the matching RESP error reply.
//
// ErrWrongType already carries its RESP prefix (WRONGTYPE, not ERR); every other
// sentinel is a bare message that Redis prefixes with ERR, and the default case is
// what keeps a newly added one from reaching a client without a prefix.
func writeStoreErr(w *resp.Writer, err error) {
	switch {
	case errors.Is(err, store.ErrWrongType):
		w.WriteError(err.Error())
	case errors.Is(err, store.ErrNotInteger):
		w.WriteError("ERR value is not an integer or out of range")
	case errors.Is(err, store.ErrOverflow):
		w.WriteError("ERR increment or decrement would overflow")
	default:
		w.WriteError("ERR " + err.Error())
	}
}

// parseScore parses a sorted-set score. It admits the
// infinities, which Redis uses as the extreme scores, but still rejects NaN: the
// skip list orders by score, and a NaN compares false against everything.
func parseScore(b []byte) (float64, bool) {
	f, ok := parseFloat(b)
	if !ok || math.IsNaN(f) {
		return 0, false
	}
	return f, true
}

// emptyIfNil turns a nil slice into an empty one, so a reply that means "the empty
// string" is written as $0 rather than as the null bulk string $-1. GETRANGE past
// the end of a value is empty, not missing.
func emptyIfNil(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	return b
}

// parseInt parses an operand that indexes into a collection (a list or sorted-set
// rank), where the store's own API is int-wide. Operands that are timestamps,
// millisecond durations, or counters must use parseInt64 instead.
func parseInt(b []byte) (int, bool) {
	n, err := strconv.Atoi(string(b))
	return n, err == nil
}

// parseInt64 parses a base-10 signed 64-bit operand. Every timestamp, duration,
// and counter goes through it rather than parseInt, whose int is only 32 bits wide
// on a 32-bit build -- far too narrow for the millisecond deadlines PEXPIREAT,
// PEXPIRE and SET ... PXAT carry, which would otherwise be silently truncated.
func parseInt64(b []byte) (int64, bool) {
	n, err := strconv.ParseInt(string(b), 10, 64)
	return n, err == nil
}

// maxDrawWithReplacement bounds how many draws a negative RANDMEMBER count may ask for.
// It is the protocol's own multibulk limit: an array longer than this is one no RESP
// reader will accept, including this server's own.
const maxDrawWithReplacement = resp.MaxMultiBulk

// parseRandomCount parses the count operand of the RANDMEMBER family -- SRANDMEMBER,
// HRANDFIELD, ZRANDMEMBER -- whose negative form asks for that many draws *with*
// replacement. errMsg is the RESP error to reply with, or "".
//
// pairs says the reply carries two elements per draw (WITHSCORES/WITHVALUES), which
// halves the range Redis accepts.
//
// Three refusals, each with the message real Redis sends:
//
//   - not a number: the usual "not an integer or out of range".
//   - exactly the smallest int64: Redis's accepted range is [-LONG_MAX, LONG_MAX], so
//     the single value whose negation does not fit is refused by name. It has to be --
//     negating it wraps back to itself, and a slice of negative length is a panic in Go,
//     which is one operand from any client taking the whole process down. That is what it
//     did before this check existed.
//   - beyond half the range with pairs, which Redis rejects with its shorter
//     "value is out of range".
//
// And one refusal Redis does not make: a repetition count past the protocol's multibulk
// limit. Redis materializes it and dies of it; a reply that no reader can parse is not
// worth the heap it would take to build, and refusing before allocating is the same
// discipline SETRANGE applies to a string too long to carry. The message is Redis's
// "value is out of range", so a client (or a test) matching on that text sees what it
// expects.
func parseRandomCount(b []byte, pairs bool) (int, string) {
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return 0, "ERR value is not an integer or out of range"
	}
	if n == math.MinInt64 {
		return 0, "ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807"
	}
	if pairs && (n < -math.MaxInt64/2 || n > math.MaxInt64/2) {
		return 0, "ERR value is out of range"
	}
	if n < -maxDrawWithReplacement {
		return 0, "ERR value is out of range"
	}
	// A positive count is bounded by the collection's own size, so it needs no cap of its
	// own; it is clamped to int width for the 32-bit build.
	if n > math.MaxInt32 {
		n = math.MaxInt32
	}
	return int(n), ""
}

func parseFloat(b []byte) (float64, bool) {
	f, err := strconv.ParseFloat(string(b), 64)
	return f, err == nil
}

// formatFloat renders a score the way Redis does: shortest round-trippable form,
// with the infinities spelled the way Redis spells them (and accepts them back)
// rather than Go's "+Inf".
//
// The formatting itself lives in the resp package, because a double's text is part
// of the wire encoding and RESP2 (bulk string) and RESP3 (,<value>) must spell the
// same score identically.
func formatFloat(f float64) string { return resp.FormatDouble(f) }

// deadlineMs converts an expire-family operand into the absolute millisecond
// deadline it means. unitMs is the operand's unit expressed in milliseconds (1000
// for the second-granularity commands, 1 for the millisecond ones) and rel says
// whether the operand counts from nowMs or is already absolute.
//
// ok is false when the arithmetic would overflow int64. Unchecked, the
// multiplication and addition wrap into the past, so a far-future expiry would
// silently delete the key instead of preserving it; Redis reports such an operand
// as an invalid expire time, and so do the callers.
func deadlineMs(nowMs, n, unitMs int64, rel bool) (int64, bool) {
	if unitMs != 1 {
		if n > math.MaxInt64/unitMs || n < math.MinInt64/unitMs {
			return 0, false
		}
		n *= unitMs
	}
	if !rel {
		return n, true
	}
	if (n > 0 && nowMs > math.MaxInt64-n) || (n < 0 && nowMs < math.MinInt64-n) {
		return 0, false
	}
	return nowMs + n, true
}

// The two bounds on the glob matcher below. Both exist because a pattern is an operand
// the client chooses, and the server then re-runs it once per key (KEYS, SCAN ... MATCH),
// once per element (HSCAN/SSCAN/ZSCAN), or -- worst -- once per subscribed pattern on
// every PUBLISH, under the global Pub/Sub lock. The matcher's cost is therefore something
// a client can multiply, so it has to be something a client cannot make large.
const (
	// globMaxStarGroups is the number of '*' *groups* a pattern may contain and still
	// match anything. A run of adjacent stars is one group, because a run means exactly
	// what a single star means.
	//
	// This is Redis's own bound, reproduced so that the boundary is in the same place.
	// Redis's stringmatchlen recurses once per star group and refuses at
	// `if (nesting > 1000) return 0`, and since a match has to consume the whole pattern,
	// a pattern with more than 1000 groups can never match there. Measured against
	// redis:7.2 on amd64 and on arm64: 1000 groups of "*?" against a long enough key
	// matches, 1001 groups matches nothing, and 100000 *adjacent* stars still match --
	// which is the part that is easy to get wrong. shardkv reported a match in all three
	// cases before this bound existed.
	//
	// Note what it does *not* bound: Redis collapses adjacent stars, so the bound says
	// nothing about the cost of a single star followed by a long literal, which is the
	// shape that is actually expensive (see globWorkFactor).
	globMaxStarGroups = 1000

	// globWorkFactor and globMaxWork bound the matcher's *backtracking* work: the budget is
	// globWorkFactor bytes of backtracking per byte the client sent, and never more than
	// globMaxWork in total. Everything else the matcher does is one pass over the pattern
	// and one over the subject, so backtracking is the only term that is not linear in the
	// input -- and it is quadratic.
	//
	// The measured shape: `KEYS *<256KB of 'a'>b` against one 512KB key took 1m46s of CPU
	// for 768KB of input, growing exactly 16x per doubling of its size. Real redis:7.2 takes
	// 2.5x longer on the same input, so the weakness is not shardkv's invention -- but Redis
	// serializes it on its one thread while here every connection has its own, and a
	// subscribed pattern is re-evaluated on every PUBLISH under the global Pub/Sub lock,
	// where the cost lands on every other publisher too.
	//
	// Both numbers are chosen from the same fact rather than from a feeling about how big is
	// big: the work a match can possibly do is at most len(pattern)*len(subject), so a
	// budget is provably *unreachable* -- the answer provably unchanged -- wherever
	// len(pattern)*len(subject) is at or below it. That gives an envelope, and these two
	// numbers put it here: any pattern up to 1 KiB against any subject up to 16 KiB, any
	// pattern up to 256 bytes against any subject up to 64 KiB, and any pattern up to 64
	// bytes against any subject up to 256 KiB. Real patterns are tens of bytes and real keys
	// hundreds, so the margin over anything plausible is several hundredfold; measured over
	// a corpus of realistic patterns the worst backtracking-work-to-input ratio was 7, where
	// the hostile shapes start at 333 and climb with their own size (21,333 at 192KB of
	// input).
	//
	// What is left is bounded rather than unbounded, and the cap is what binds: measured, one
	// match costs a flat ~27ms of CPU at worst and that figure does not move as the input
	// grows -- 66KB of input and 5MB of input cost the same. Before the budget, 768KB of
	// input cost 1m46s. The per-byte factor is what keeps small inputs from being given a
	// budget out of proportion to themselves, and it is the binding half only below ~16 KiB
	// of input.
	//
	// The cap is deliberately not larger: the Pub/Sub path pays this per subscribed pattern
	// per publish while holding the global registry lock, so it is a cost every other
	// publisher waits for, not only the client that asked for it.
	//
	// Past the budget the pattern reports no match, which is the answer Redis gives past
	// its own bound rather than an error invented here.
	globWorkFactor = 1024
	globMaxWork    = 1 << 24 // 16,777,216 byte comparisons
)

// globMatch reports whether s matches a glob pattern supporting '*' (any run)
// and '?' (any single byte), as Redis's MATCH does for the common cases. It backs
// both KEYS and SCAN's MATCH option.
//
// It is iterative with a single backtrack point rather than recursive as Redis's is, so
// the star-count blowup Redis's bound exists to stop is not reachable here at all (1000
// adjacent stars cost 2.6us) -- the bound is reproduced for the boundary's sake, not for
// the cost's. What *is* reachable is quadratic backtracking, which the work budget stops.
// See globMaxStarGroups and globWorkFactor.
func globMatch(pattern, s string) bool {
	// A '*' group needs at least one byte, so a pattern no longer than the bound cannot
	// exceed it. Testing that here rather than inside the helper is what keeps the bound off
	// the cost of every ordinary pattern: for "user:*" the whole check is one comparison
	// against a constant, with no call to make and nothing to count.
	if len(pattern) > globMaxStarGroups && globStarGroupsExceeded(pattern) {
		return false
	}
	// int64, because on a 32-bit build int is 32 bits wide and the product of the factor
	// with two operands the reader accepts at up to 512 MiB each would wrap -- into a
	// negative budget, which would refuse every pattern (invariant 15).
	budget := int64(globWorkFactor) * int64(len(pattern)+len(s))
	if budget > globMaxWork {
		budget = globMaxWork
	}
	pi, si := 0, 0
	star, starS := -1, 0
	for si < len(s) {
		switch {
		case pi < len(pattern) && (pattern[pi] == '?' || pattern[pi] == s[si]):
			pi++
			si++
		case pi < len(pattern) && pattern[pi] == '*':
			// A star that ends the pattern absorbs whatever is left of the subject, so the
			// answer is already known and there is nothing to backtrack over. This is Redis's
			// own `if (patternLen == 1) return 1` after it collapses a run of stars, and it is
			// what makes the two commonest patterns cheap: without it "*" backtracks once per
			// byte of the subject, and "user:*" once per byte after the prefix.
			if pi == len(pattern)-1 {
				return true
			}
			star, starS = pi, si
			pi++
		case star != -1:
			// Charge the attempt that just failed: pi-star bytes of pattern were compared
			// after the star and led nowhere. Charging here, rather than counting every
			// iteration, is what makes the budget free for an ordinary pattern -- one that
			// never has to backtrack never reaches this branch. It also guarantees
			// termination on its own, since every visit costs at least 1.
			budget -= int64(pi - star)
			if budget < 0 {
				return false
			}
			pi = star + 1
			starS++
			si = starS
		default:
			return false
		}
	}
	for pi < len(pattern) && pattern[pi] == '*' {
		pi++
	}
	return pi == len(pattern)
}

// globStarGroupsExceeded reports whether pattern holds more '*' groups than a pattern may
// hold and still match anything -- a run of adjacent stars counting as one group. See
// globMaxStarGroups. Its caller has already established that the pattern is long enough for
// the answer to be able to be yes.
func globStarGroupsExceeded(pattern string) bool {
	groups, prevStar := 0, false
	for i := 0; i < len(pattern); i++ {
		if pattern[i] != '*' {
			prevStar = false
			continue
		}
		if !prevStar {
			groups++
			if groups > globMaxStarGroups {
				return true
			}
		}
		prevStar = true
	}
	return false
}
