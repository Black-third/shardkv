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

// parseFloatStrict parses a float operand that has to be finite. Go's parser
// accepts "inf" and "nan", but the increment family rejects them outright rather
// than storing a value no later increment could recover from, and a NaN score would
// have no place in the sorted set's ordering at all.
func parseFloatStrict(b []byte) (float64, bool) {
	f, ok := parseFloat(b)
	if !ok || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, false
	}
	return f, true
}

// parseScore parses a sorted-set score. Unlike parseFloatStrict it admits the
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

// globMatch reports whether s matches a glob pattern supporting '*' (any run)
// and '?' (any single byte), as Redis's MATCH does for the common cases. It backs
// both KEYS and SCAN's MATCH option.
func globMatch(pattern, s string) bool {
	pi, si := 0, 0
	star, starS := -1, 0
	for si < len(s) {
		switch {
		case pi < len(pattern) && (pattern[pi] == '?' || pattern[pi] == s[si]):
			pi++
			si++
		case pi < len(pattern) && pattern[pi] == '*':
			star, starS = pi, si
			pi++
		case star != -1:
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
