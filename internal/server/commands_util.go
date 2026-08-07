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

// globMatch reports whether s matches a glob pattern, using Redis's stringmatchlen
// semantics: '*' matches any run of bytes, '?' any single byte, '[...]' a character class
// (with '^' negation and 'a-c' ranges), and a backslash escapes the byte that follows it.
// It backs KEYS, every SCAN family MATCH, CONFIG GET and pattern subscriptions.
//
// globMatchFold is the same matcher with ASCII case folding, which is the nocase form
// Redis uses for the names in CONFIG GET and COMMAND LIST FILTERBY.
//
// The class and escape syntax is reproduced from Redis's implementation rather than from
// the POSIX glob most readers expect, because they disagree in ways a client can see. The
// disagreements, each measured against redis:7.2 (see the table in TestGlobRedisSemantics):
//
//   - '[]' is an empty class and matches nothing, so a ']' as the first member is not the
//     literal ']' POSIX makes it: `[]abc]` never matches. `[^]` is an empty *negated*
//     class and therefore matches any single byte, exactly like '?'.
//   - '!' is not a negation. `[!ae]` is the three-member class {'!','a','e'}.
//   - a reversed range is silently swapped, so `[c-a]` is `[a-c]`.
//   - `[a-]` is a *range* from 'a' to ']', because the range branch only asks that three
//     bytes remain; `[-a]` is the two-member class {'-','a'}, because a leading '-' is not
//     followed by one.
//   - an unterminated class swallows the rest of the pattern and keeps the members it read
//     before running out, so `a[b` matches "ab" and `a[a-b` matches "ab" -- while `a[`
//     matches nothing at all, its class being empty.
//   - an escape before an ordinary byte is that byte (`\z` matches "z"), and a *trailing*
//     backslash is the literal backslash, which is what Redis's fall-through from its
//     escape case into its literal case does.
//
// One deliberate difference from redis on amd64, and it is Redis that is inconsistent
// with itself rather than this: the range comparison here is on unsigned bytes. Redis
// compares C `char`s, whose signedness is the platform's, so a range with an endpoint at
// or above 0x80 gets different answers from the same Redis release on different
// architectures. Measured on redis:7.2: `[\x00-\xff]` matches "A" on arm64 (unsigned
// char, 0..255) and does not on amd64 (signed char: 0 and -1, swapped to -1..0, which
// admits only NUL and 0xff). This server takes the unsigned reading -- the one that is
// the same everywhere and the one a client writing a byte range means.
func globMatch(pattern, s string) bool {
	matched, _ := globMatchCase(pattern, s, false)
	return matched
}

// globMatchFold is globMatch with ASCII case folding (Redis's nocase form).
func globMatchFold(pattern, s string) bool {
	matched, _ := globMatchCase(pattern, s, true)
	return matched
}

// globMatchCase is the matcher itself. It is iterative with a single backtrack point
// rather than recursive as Redis's is, so the star-count blowup Redis's nesting bound
// exists to stop is not reachable here at all (1000 adjacent stars cost 2.6us) -- the
// bound is reproduced for the boundary's sake, not for the cost's. What *is* reachable is
// quadratic backtracking, which the work budget stops. See globMaxStarGroups and
// globWorkFactor.
//
// The single backtrack point is sound because every token except '*' consumes exactly one
// subject byte: each segment between two stars is therefore matched at its earliest
// possible position, and a later failure can only be repaired by sliding the *last* star.
// That is the same answer Redis's recursion reaches, and for the same reason -- its
// skipLongerMatches flag exists precisely to stop it from re-trying an earlier star.
//
// The second return value is the backtracking work charged against the budget, which is
// the number the two bounds above are denominated in. It exists so a test can assert what
// an ordinary pattern costs *in the matcher's own units* rather than in milliseconds on
// whatever machine happens to be running it -- see TestGlobOrdinaryPatternsStayFree, and
// commit 64653df for what wall-clock assertions have already cost this tree. Both wrappers
// discard it, so it is one int64 in a register on every real call and never a load or a
// store.
func globMatchCase(pattern, s string, fold bool) (bool, int64) {
	// A '*' group needs at least one byte, so a pattern no longer than the bound cannot
	// exceed it. Testing that here rather than inside the helper is what keeps the bound off
	// the cost of every ordinary pattern: for "user:*" the whole check is one comparison
	// against a constant, with no call to make and nothing to count.
	if len(pattern) > globMaxStarGroups && globStarGroupsExceeded(pattern) {
		return false, 0
	}
	// Redis's outer loop is `while (patternLen && stringLen)`, so an empty subject never
	// enters it and never reaches the trailing-star collapse *inside* it: for an empty
	// subject the answer is "only an empty pattern", and `*` does not match "". Measured:
	// with an empty key name present, redis:7.2 answers `KEYS **` with nothing. `KEYS *`
	// does report it -- but because KEYS short-circuits the single-star pattern before the
	// matcher sees it, which cmdKeys reproduces, not because the matcher matched.
	if len(s) == 0 {
		return len(pattern) == 0, 0
	}
	// int64, because on a 32-bit build int is 32 bits wide and the product of the factor
	// with two operands the reader accepts at up to 512 MiB each would wrap -- into a
	// negative budget, which would refuse every pattern (invariant 15).
	budget := int64(globWorkFactor) * int64(len(pattern)+len(s))
	if budget > globMaxWork {
		budget = globMaxWork
	}
	// Kept so the work charged can be reported without counting it a second time in the
	// loop: the reported figure is the difference, so the backtrack path stays exactly the
	// one subtraction and one comparison it was.
	granted := budget
	pi, si := 0, 0
	star, starS := -1, 0
	for si < len(s) {
		// charge is how much pattern the attempt that is about to be abandoned scanned. It
		// is set by the branches below and spent in the backtrack, so an ordinary pattern --
		// one that never backtracks -- never pays anything at all.
		var charge int
		if pi < len(pattern) {
			p := pattern[pi]
			// The hot path, written out rather than routed through globToken, and ordered for
			// the commonest outcome: a literal byte of pattern that matches the subject byte.
			// That costs two loads, one comparison, and the three tests that say this byte is
			// not a metacharacter.
			//
			// The duplication is deliberate. This loop runs once per charged byte of
			// backtracking -- up to globMaxWork of them -- and globToken cannot be inlined,
			// because it switches into a class parser that loops. Measured on the hostile shape
			// the work budget exists for (one '*' then 256 KiB of near-matching literal against
			// a 512 KiB subject), routing literals through that call cost 7x for an identical
			// answer: 17ms became 125ms. A class and an escape still go through globToken,
			// because they have to, and they are not what the hostile shapes are made of.
			if p == s[si] && !globMeta[p] {
				pi++
				si++
				continue
			}
			switch {
			case p == '?':
				pi++
				si++
				continue

			case p == '*':
				// A star that ends the pattern absorbs whatever is left of the subject, so the
				// answer is already known and there is nothing to backtrack over. This is Redis's
				// own `if (patternLen == 1) return 1` after it collapses a run of stars, and it is
				// what makes the two commonest patterns cheap: without it "*" backtracks once per
				// byte of the subject, and "user:*" once per byte after the prefix.
				if pi == len(pattern)-1 {
					return true, granted - budget
				}
				star, starS = pi, si
				pi++
				continue

			case p == '[' || p == '\\':
				end, ok := globToken(pattern, pi, s[si], fold)
				if ok {
					pi, si = end, si+1
					continue
				}
				// The failed token counts too, not just the pattern before it. A character class
				// leaves pi where it was however long the class is, so charging pi-star alone
				// would let `*[<a long class>]` against a long subject backtrack once per subject
				// byte at a cost of the whole class each time -- a second unbounded backtracking
				// source, in a matcher whose first one is what these bounds exist for.
				charge = end - star

			case fold && lowerByte(p) == lowerByte(s[si]):
				// A literal that matches only case-insensitively. Off the hot path on purpose:
				// only CONFIG GET and COMMAND LIST FILTERBY fold, and neither is on a data path.
				pi++
				si++
				continue

			default:
				charge = pi + 1 - star // a literal that did not match: one byte of pattern
			}
		} else {
			charge = pi - star
		}
		if star < 0 {
			return false, granted - budget
		}
		// Charging only here, rather than counting every iteration, is what makes the budget
		// free for an ordinary pattern. It also guarantees termination on its own, since a
		// visit always costs at least 1: end is past the token that failed, and pi is past
		// the star.
		budget -= int64(charge)
		if budget < 0 {
			return false, granted - budget
		}
		pi = star + 1
		starS++
		si = starS
	}
	// The subject ran out. Redis collapses a trailing run of stars here and then requires
	// the pattern to be exhausted too, which is why `ab*c` does not match "ab".
	for pi < len(pattern) && pattern[pi] == '*' {
		pi++
	}
	return pi == len(pattern), granted - budget
}

// globMeta marks the four pattern bytes that are not literals: '*' and '?' consume subject
// without naming it, '[' opens a class, and '\\' escapes what follows. The hot path in
// globMatchCase tests membership with one indexed load instead of four comparisons, which is
// worth it because that test runs once per byte of pattern per backtracking attempt -- up to
// globMaxWork times. The table is 256 bytes and stays resident.
var globMeta = [256]bool{'*': true, '?': true, '[': true, '\\': true}

// globToken matches the single subject byte c against the one pattern token that begins at
// pattern[pi] -- a literal, an escape, '?', or a character class -- and reports where that
// token ends.
//
// end is returned whether or not the token matched, because the caller charges its
// backtracking budget by how much pattern the failed attempt scanned, and for a class that
// is the length of the class rather than one byte.
//
// '*' is not a token: it consumes an unbounded run and belongs to the caller.
func globToken(pattern string, pi int, c byte, fold bool) (end int, matched bool) {
	switch pattern[pi] {
	case '?':
		return pi + 1, true
	case '[':
		return globClass(pattern, pi, c, fold)
	case '\\':
		// An escape passes the next byte through as a literal, whatever it is. A *trailing*
		// backslash has no next byte and stands for itself, which is what Redis's
		// fall-through from its escape case into its literal case does.
		if pi+1 < len(pattern) {
			return pi + 2, eqByteFold(pattern[pi+1], c, fold)
		}
	}
	return pi + 1, eqByteFold(pattern[pi], c, fold)
}

// globClass matches c against the character class beginning at pattern[pi] (which is a
// '['), and reports the index just past the class.
//
// The walk is a transcription of Redis's, including the two things about it that are
// surprising: the class ends at the first ']' whatever its position, so an empty class
// (`[]`, or `[^]` before negation) is a real thing that matches nothing; and an
// unterminated class ends at the end of the *pattern*, keeping whichever members it
// managed to read. See globMatch for the measured consequences.
func globClass(pattern string, pi int, c byte, fold bool) (end int, matched bool) {
	j := pi + 1
	neg := j < len(pattern) && pattern[j] == '^'
	if neg {
		j++
	}
	match := false
	for {
		switch {
		case j+1 < len(pattern) && pattern[j] == '\\':
			// An escaped member is a literal, so ']' and '-' can be class members.
			j++
			if eqByteFold(pattern[j], c, fold) {
				match = true
			}
			j++

		case j < len(pattern) && pattern[j] == ']':
			return j + 1, match != neg

		case j >= len(pattern):
			// Unterminated. Redis backs its pattern pointer up by one byte here, which its own
			// post-switch increment then undoes, so the class consumes the rest of the pattern.
			return j, match != neg

		case j+2 < len(pattern) && pattern[j+1] == '-':
			// A range needs three bytes left, counted from here rather than to the ']' -- which
			// is why `[a-]` is the range 'a'..']' and not the two members 'a' and '-'.
			lo, hi := pattern[j], pattern[j+2]
			if lo > hi {
				lo, hi = hi, lo // Redis swaps a reversed range rather than refusing it
			}
			b := c
			if fold {
				lo, hi, b = lowerByte(lo), lowerByte(hi), lowerByte(b)
			}
			if b >= lo && b <= hi {
				match = true
			}
			j += 3

		default:
			if eqByteFold(pattern[j], c, fold) {
				match = true
			}
			j++
		}
	}
}

// lowerByte folds one ASCII byte to lower case, and only ASCII: that is what C's tolower
// does in the "C" locale Redis runs in, and folding anything else would make a byte range
// depend on a locale the wire protocol does not carry.
func lowerByte(b byte) byte {
	if b >= 'A' && b <= 'Z' {
		return b + ('a' - 'A')
	}
	return b
}

func eqByteFold(a, b byte, fold bool) bool {
	return a == b || (fold && lowerByte(a) == lowerByte(b))
}

// globStarGroupsExceeded reports whether pattern holds more '*' groups than a pattern may
// hold and still match anything -- a run of adjacent stars counting as one group. See
// globMaxStarGroups. Its caller has already established that the pattern is long enough for
// the answer to be able to be yes.
//
// It walks the pattern with globToken rather than counting '*' bytes, so an escaped star
// and a star inside a character class are the literals they are and count towards nothing.
// Redis's bound is on how many star groups the *matcher* recurses through, and it never
// recurses for either of those.
func globStarGroupsExceeded(pattern string) bool {
	groups, prevStar := 0, false
	for i := 0; i < len(pattern); {
		if pattern[i] == '*' {
			if !prevStar {
				groups++
				if groups > globMaxStarGroups {
					return true
				}
			}
			prevStar = true
			i++
			continue
		}
		prevStar = false
		// The subject byte is immaterial here; only the token's extent is being asked for.
		i, _ = globToken(pattern, i, 0, false)
	}
	return false
}
