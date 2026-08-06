package store

import (
	"math/big"
	"strings"
)

// This file is the arithmetic INCRBYFLOAT and HINCRBYFLOAT are defined in terms of.
// Everywhere else in the server a floating-point value is a float64; these two commands
// are the exception, because Redis computes them in C's long double and both the reply
// and the *stored bytes* are the decimal text of that computation. A float64 gets a
// visibly different answer: measured against redis:7.2, `SET k 0.1; INCRBYFLOAT k 0.2`
// answers 0.3 while float64 arithmetic answers 0.30000000000000004, and
// `SET k 1e17; INCRBYFLOAT k 1` answers 100000000000000001 where float64 cannot
// represent that value at all and answers 100000000000000000.
//
// # Which long double
//
// C's long double is architecture-dependent, so *real Redis does not have one answer
// here*. Measured, on the same two commands, with two live servers:
//
//	                             redis:7.2 amd64 (x87, 64-bit mantissa)
//	                             redis:7.4 arm64 (IEEE binary128, 113-bit mantissa)
//	SET k 1e20;   INCRBYFLOAT k 1     100000000000000000000   vs  100000000000000000001
//	SET k 1e308;  INCRBYFLOAT k 1e308 1999999999999999999993…  vs  2000000000000000000000…
//	SET k 1e30;   INCRBYFLOAT k 1e-30 1000000000000000000024696061952
//	                                                          vs  1000000000000000000000000000000
//	              INCRBYFLOAT k 1e-4951   refused             vs  accepted, stores 0
//
// The x87 form is what this package implements, for two reasons. The first is
// compatibility: x86-64 is the architecture the overwhelming majority of Redis
// deployments run on, so it is the answer a client is most likely to be comparing
// against. The second matters more, and is a property Redis itself does not have:
// **shardkv's answer is deterministic where Redis's is not.** big.Float at a fixed
// precision computes identically on every platform Go targets, so a shardkv master on
// arm64 and its replica on amd64 store the same bytes for the same key. A real Redis
// master and replica across those two architectures cannot, because INCRBYFLOAT
// propagates its *result* (invariant 4) -- the master ships text the replica would never
// have produced, and every later increment of that key compounds the disagreement,
// silently.
//
// So the compatibility claim is exactly this and no more: byte-identical to Redis on
// x86-64, deliberately architecture-independent, and therefore different from a Redis
// built where long double is binary128. Two other measured deviations, both narrow:
//
//   - An operand between the two formats' subnormal floors is refused here and accepted
//     there: below x87's tie of 2**-16446 (1.8e-4951) but at or above binary128's of
//     2**-16495 (3.2e-4966). Measured, the smallest accepted operand is 1e-4950 on amd64
//     and 3.3e-4966 on arm64.
//   - A *hex* literal that overflows long double ("0x1p16384") is refused here as an
//     unparseable operand, which is what glibc's strtold reports and what redis:7.2 on
//     glibc answers. An alpine (musl) build instead accepts it as an infinity and refuses
//     it one step later, against the result. Both error either way; only the message
//     differs, and only for a hex literal -- "1e5000" is refused by both libcs.
//
// # What the C code is
//
// Redis parses with string2ld (strtold plus a whole-string and range check), adds in
// long double, refuses a non-finite result, and formats with ld2string in LD_STR_HUMAN
// mode ("%.17Lf" with trailing zeros and any trailing "." removed). ParseLongDouble,
// LongDouble.add and LongDouble.Text are those three steps.

const (
	// ldPrec is the x87 long double's mantissa width. big.Float at this precision with
	// ToNearestEven is x87 arithmetic -- verified against a live redis:7.2 on amd64
	// across the formatting, precision and boundary cases in longdouble_test.go.
	ldPrec = 64

	// ldMaxExp is the largest big.Float exponent a finite long double has. big.Float's
	// own exponent range is effectively unbounded, so without this check 1e5000 + 0
	// would succeed here and be refused by Redis. LDBL_MAX is (1 - 2**-64) * 2**16384,
	// whose MantExp exponent is 16384; anything that rounds above it is an infinity.
	ldMaxExp = 16384

	// ldZeroTieShift is the negated exponent of the tie below which strtold underflows
	// all the way to zero -- which string2ld refuses (ERANGE with a zero result), so it
	// decides whether an operand parses at all. The smallest positive x87 subnormal is
	// 2**-16445, so a magnitude at or below half of it, 2**-16446, rounds to zero: at
	// exactly the tie, round-half-to-even prefers zero's even mantissa. Measured:
	// INCRBYFLOAT with 0x1p-16445 stores 0, with 0x1p-16446 answers "value is not a
	// valid float".
	ldZeroTieShift = 16446

	// ldMaxTextLen mirrors MAX_LONG_DOUBLE_CHARS, the stack buffer string2ld copies into
	// before calling strtold: a longer operand is refused without being looked at.
	ldMaxTextLen = 5 * 1024
)

// LongDouble is a value in the x87 long double format: a finite value carried as a
// big.Float of exactly ldPrec bits, or an infinity.
//
// The infinities are held as a flag rather than as big.Float's own ±Inf because
// big.Float.Add *panics* on +Inf + -Inf, and that panic would be reachable from any
// client with `SET k inf; INCRBYFLOAT k -inf` -- taking the process down and every
// connection with it, the hazard invariant 15 is about. Keeping the flag separate means
// non-finiteness is decided before anything is added, so there is no addition to panic.
//
// The zero value is +0. A LongDouble is immutable once returned; nothing here writes
// through the pointer.
type LongDouble struct {
	f   *big.Float // finite value at ldPrec bits; nil means +0
	inf int8       // 0 finite, +1 for +Inf, -1 for -Inf
}

// IsInf reports whether d is an infinity. HINCRBYFLOAT refuses an infinite *operand* and
// names it as one, which is the one place the two increments word their errors
// differently -- see cmdHIncrByFloat.
func (d LongDouble) IsInf() bool { return d.inf != 0 }

// float returns d's finite value. The nil case is the zero-value LongDouble.
func (d LongDouble) float() *big.Float {
	if d.f == nil {
		return new(big.Float).SetPrec(ldPrec)
	}
	return d.f
}

// add returns d + o, and ok=false when the result is not finite -- which is exactly the
// isnan/isinf check Redis applies to the sum before storing anything.
//
// Every case involving an infinity answers false: an infinity plus a finite value is
// infinite, like signs are infinite, and opposite signs are NaN. Deciding that here,
// before touching big.Float, is what keeps +Inf + -Inf from panicking.
func (d LongDouble) add(o LongDouble) (LongDouble, bool) {
	if d.inf != 0 || o.inf != 0 {
		return LongDouble{}, false
	}
	sum := new(big.Float).SetPrec(ldPrec).SetMode(big.ToNearestEven)
	sum.Add(d.float(), o.float())
	if ldExponentOverflows(sum) {
		return LongDouble{}, false
	}
	// A sum can land below the smallest subnormal without being zero, where a real x87
	// would have flushed further precision away. It is not observable: Text renders
	// every magnitude under 5e-18 as "0", so the two spellings cannot differ.
	return LongDouble{f: sum}, true
}

// Text renders d the way Redis's ld2string does in LD_STR_HUMAN mode: "%.17Lf", then
// trailing zeros and any trailing "." removed, then negative zero spelled "0".
//
// This is *not* how a sorted-set score is spelled, and the difference is measurable
// rather than theoretical: a score uses d2string, which switches to an exponent outside
// roughly 1e-6..1e18, while this never does. Measured against redis:7.2, ZSCORE of 1e-7
// answers "1e-7" while an increment reaching 2e-7 answers and stores "0.0000002". Using
// one formatter for both got the increment wrong -- and not only in the reply, because
// the increment propagates its result as `SET key <text>`: the master stored the text one
// formatter produced and the replica the text of the other, so the two disagreed about
// the bytes of a key with nothing to report it.
func (d LongDouble) Text() string {
	switch d.inf {
	case 1:
		return "inf"
	case -1:
		return "-inf"
	}
	s := d.float().Text('f', 17)
	// The guard is C's: strip only what follows a decimal point, so an integral value's
	// own trailing zeros are never eaten. 'f' with 17 digits always writes the point.
	if strings.IndexByte(s, '.') >= 0 {
		s = strings.TrimRight(s, "0")
		s = strings.TrimSuffix(s, ".")
	}
	// A magnitude that rounds to zero keeps its sign through printf, and Redis rewrites
	// that one case: measured, `SET k -1e-30; INCRBYFLOAT k 0` stores "0", not "-0".
	if s == "-0" {
		return "0"
	}
	return s
}

// ParseLongDouble parses one operand or stored value exactly as Redis's string2ld does,
// reporting ok=false for everything string2ld refuses. That is a wider grammar than Go's:
// strtold is a C library function, so it takes C99 hex float literals ("0x10" is 16,
// "0X1.8p3" is 12) and the "inf"/"infinity" spellings in any case, and it is narrower in
// one respect that matters -- Go's float syntax accepts digit separators, and a planted
// value of "1_0" must be refused rather than read as 10.
//
// The five refusals that are not simply a syntax error, all measured against redis:7.2:
//
//   - NaN in any spelling. strtold parses it; string2ld's isnan check rejects it.
//   - A magnitude above LDBL_MAX ("1e5000"), which strtold reports as an overflow.
//     An *explicit* infinity ("inf") is accepted, and refused later against the result.
//   - A magnitude that underflows to zero ("1e-4951"), which strtold also reports.
//     A subnormal that survives ("1e-4950") is accepted and formats as "0".
//   - Anything the parse does not consume whole: leading space, trailing space, "1.2.3",
//     "1e", "0x", a truncated "infinit". (Not a typo: the point of that last one is that a
//     prefix of "infinity" is refused rather than rounded up to it.)
//   - An operand of ldMaxTextLen bytes or more, which never reaches strtold at all.
func ParseLongDouble(s string) (LongDouble, bool) {
	if len(s) == 0 || len(s) >= ldMaxTextLen {
		return LongDouble{}, false
	}
	// string2ld checks the first byte itself, because strtold would happily skip leading
	// whitespace and report a successful whole-string parse of " 1".
	if isCSpace(s[0]) {
		return LongDouble{}, false
	}
	body, neg := s, false
	if body[0] == '+' || body[0] == '-' {
		neg = body[0] == '-'
		body = body[1:]
	}
	if body == "" {
		return LongDouble{}, false
	}
	if strings.EqualFold(body, "inf") || strings.EqualFold(body, "infinity") {
		if neg {
			return LongDouble{inf: -1}, true
		}
		return LongDouble{inf: 1}, true
	}
	// NaN parses in C and is then refused, rather than failing the parse. The outcome is
	// the same refusal either way; it is spelled out so the reason is not lost.
	if len(body) >= 3 && strings.EqualFold(body[:3], "nan") {
		return LongDouble{}, false
	}
	if len(body) > 2 && body[0] == '0' && (body[1] == 'x' || body[1] == 'X') {
		return parseHexLongDouble(body[2:], neg)
	}
	return parseDecLongDouble(body, neg)
}

// parseDecLongDouble parses the unsigned decimal form: digits with an optional point and
// an optional exponent, at least one digit somewhere.
func parseDecLongDouble(body string, neg bool) (LongDouble, bool) {
	i := 0
	intStart := i
	for i < len(body) && isDigit(body[i]) {
		i++
	}
	digits := body[intStart:i]
	fracLen := 0
	if i < len(body) && body[i] == '.' {
		i++
		fracStart := i
		for i < len(body) && isDigit(body[i]) {
			i++
		}
		fracLen = i - fracStart
		digits += body[fracStart:i]
	}
	if len(digits) == 0 {
		return LongDouble{}, false
	}
	exp10, ok := scanExponent(body, &i, 'e', 'E')
	if !ok || i != len(body) {
		return LongDouble{}, false
	}
	exp10 = satAdd(exp10, -fracLen)

	mant, zero := digitsToInt(digits, 10)
	if zero {
		return signedZero(neg), true
	}
	// 10**(d-1) <= |value| < 10**d, which is enough to settle every magnitude except the
	// two that straddle a boundary, and keeps the big.Int work below bounded.
	d := satAdd(len(mantDigits(digits)), exp10)
	switch {
	case d > 4933:
		return LongDouble{}, false // above 10**4933 > LDBL_MAX
	case d <= -4951:
		return LongDouble{}, false // below 10**-4951 < 2**-16446, underflows to zero
	case d == -4950:
		// The one ambiguous band around the tie: decide it exactly.
		// |value| <= 2**-16446  <=>  digits * 2**16446 <= 10**-exp10
		lhs := new(big.Int).Lsh(mant, ldZeroTieShift)
		rhs := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-exp10)), nil)
		if lhs.Cmp(rhs) <= 0 {
			return LongDouble{}, false
		}
	}
	return finiteFromInt(mant, 10, exp10, neg)
}

// parseHexLongDouble parses the unsigned C99 hex form, body being what follows "0x":
// hex digits with an optional point and an optional binary exponent.
func parseHexLongDouble(body string, neg bool) (LongDouble, bool) {
	i := 0
	intStart := i
	for i < len(body) && isHexDigit(body[i]) {
		i++
	}
	digits := body[intStart:i]
	fracLen := 0
	if i < len(body) && body[i] == '.' {
		i++
		fracStart := i
		for i < len(body) && isHexDigit(body[i]) {
			i++
		}
		fracLen = i - fracStart
		digits += body[fracStart:i]
	}
	if len(digits) == 0 {
		return LongDouble{}, false
	}
	// The binary exponent is optional -- strtold's subject sequence allows "0x10" -- but
	// a "p" that is present must carry digits. Without them strtold's parse stops before
	// the "p", leaving text unconsumed, which string2ld refuses.
	exp2, ok := scanExponent(body, &i, 'p', 'P')
	if !ok || i != len(body) {
		return LongDouble{}, false
	}
	exp2 = satAdd(exp2, -4*fracLen)

	mant, zero := digitsToInt(digits, 16)
	if zero {
		return signedZero(neg), true
	}
	// 2**(hi-1) <= |value| < 2**hi.
	hi := satAdd(mant.BitLen(), exp2)
	switch {
	case hi > ldMaxExp+1:
		return LongDouble{}, false
	case hi <= -ldZeroTieShift:
		return LongDouble{}, false
	}
	// |value| <= 2**-16446  <=>  mant <= 2**k. Compared through bit lengths so k never
	// has to be materialised for an absurd exponent.
	if k := -ldZeroTieShift - exp2; k >= 0 {
		switch bl := mant.BitLen(); {
		case bl <= k:
			return LongDouble{}, false
		case bl == k+1 && mant.Cmp(new(big.Int).Lsh(big.NewInt(1), uint(k))) == 0:
			return LongDouble{}, false
		}
	}
	return finiteFromInt(mant, 2, exp2, neg)
}

// finiteFromInt builds mant * base**exp rounded to ldPrec once, and reports ok=false if
// that lands outside the finite range.
//
// Rounding exactly once is the point. The scale factor is built as an exact integer and
// applied with a single big.Float operation, each of which is correctly rounded to the
// destination's precision, so the result is what x87 would hold. Rounding the mantissa
// first and the product afterwards would round twice and could differ in the last bit.
func finiteFromInt(mant *big.Int, base int, exp int, neg bool) (LongDouble, bool) {
	f := new(big.Float).SetPrec(ldPrec).SetMode(big.ToNearestEven)
	switch {
	case base == 2:
		// A power of two is an exponent shift, so the only rounding is of the mantissa.
		f.SetInt(mant)
		f.SetMantExp(f, exp)
	case exp == 0:
		f.SetInt(mant)
	default:
		// Exact numerator and denominator, one rounded division or multiplication.
		e := exp
		if e < 0 {
			e = -e
		}
		pow := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(int64(base)), big.NewInt(int64(e)), nil))
		num := new(big.Float).SetInt(mant)
		if exp > 0 {
			f.Mul(num, pow)
		} else {
			f.Quo(num, pow)
		}
	}
	if ldExponentOverflows(f) {
		return LongDouble{}, false
	}
	if neg {
		f.Neg(f)
	}
	return LongDouble{f: f}, true
}

// ldExponentOverflows reports whether f is beyond LDBL_MAX, which is the bound big.Float
// does not have and long double does.
func ldExponentOverflows(f *big.Float) bool {
	// big.Float turns an exponent past int32 into an infinity, and reports MantExp of an
	// infinity as 0 -- which would read as "in range". Nothing here can reach that with
	// the magnitudes already bounded, but the check costs nothing and the alternative
	// failure is an infinity stored as text.
	if f.IsInf() {
		return true
	}
	if f.Sign() == 0 {
		return false
	}
	return f.MantExp(nil) > ldMaxExp
}

// signedZero is the zero a mantissa of nothing but zeros parses to, keeping its sign.
// Text spells both spellings "0", so the sign is invisible; it is kept because "-0" plus
// "-0" is "-0" in every other implementation and there is no reason for this one to be
// the exception.
func signedZero(neg bool) LongDouble {
	f := new(big.Float).SetPrec(ldPrec).SetMode(big.ToNearestEven)
	if neg {
		f.Neg(f)
	}
	return LongDouble{f: f}
}

// scanExponent consumes an exponent introduced by either of two markers, advancing *i
// past it. A missing marker is not an error and leaves the exponent at 0; a marker with
// no digits after it is an error, because strtold would leave it unconsumed and
// string2ld refuses a parse that does not reach the end of the string.
//
// The value saturates rather than wrapping: an exponent of 10**19 digits is nobody's
// valid operand, but wrapping it would turn an overflow into a small number.
func scanExponent(body string, i *int, lower, upper byte) (int, bool) {
	j := *i
	if j >= len(body) || (body[j] != lower && body[j] != upper) {
		return 0, true
	}
	j++
	neg := false
	if j < len(body) && (body[j] == '+' || body[j] == '-') {
		neg = body[j] == '-'
		j++
	}
	start := j
	exp := 0
	for j < len(body) && isDigit(body[j]) {
		if exp < 1<<30 {
			exp = exp*10 + int(body[j]-'0')
		}
		j++
	}
	if j == start {
		return 0, false
	}
	*i = j
	if neg {
		exp = -exp
	}
	return exp, true
}

// satAdd adds without wrapping, so a saturated exponent stays saturated.
func satAdd(a, b int) int {
	const lim = 1 << 40
	sum := a + b
	switch {
	case sum > lim:
		return lim
	case sum < -lim:
		return -lim
	}
	return sum
}

// digitsToInt reads the digit run as an integer, reporting zero=true when every digit is
// a zero -- the one case that is neither an overflow nor an underflow however extreme the
// exponent is ("0e999999999999" is 0, and Redis accepts it).
func digitsToInt(digits string, base int) (n *big.Int, zero bool) {
	d := mantDigits(digits)
	if d == "" {
		return nil, true
	}
	n, ok := new(big.Int).SetString(d, base)
	if !ok { // unreachable: every byte was validated as a digit of this base
		return nil, true
	}
	return n, false
}

// mantDigits strips leading zeros, so the remaining length is the digit count the
// magnitude estimate needs. An all-zero run comes back empty.
func mantDigits(digits string) string {
	i := 0
	for i < len(digits) && digits[i] == '0' {
		i++
	}
	return digits[i:]
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func isHexDigit(c byte) bool {
	return isDigit(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// isCSpace is C's isspace over the bytes strtold would skip.
func isCSpace(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\v', '\f', '\r':
		return true
	}
	return false
}
