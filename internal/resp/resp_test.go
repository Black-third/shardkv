package resp

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"math"
	"strconv"
	"strings"
	"testing"
)

func TestReadCommandArray(t *testing.T) {
	// *3 SET foo bar  as a Redis client would send it.
	in := "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
	args, err := NewReader(strings.NewReader(in)).ReadCommand()
	if err != nil {
		t.Fatalf("ReadCommand: %v", err)
	}
	got := make([]string, len(args))
	for i, a := range args {
		got[i] = string(a)
	}
	want := []string{"SET", "foo", "bar"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("args = %v; want %v", got, want)
	}
}

func TestReadCommandRejectsHugeHeaders(t *testing.T) {
	// These previously caused an overflow panic (bulk length near MaxInt64) or a
	// multi-GB allocation. They must now be rejected as protocol errors without
	// allocating.
	cases := []string{
		"*1\r\n$9223372036854775806\r\n", // length+2 overflows to negative
		"*2000000000\r\n",                // ~48GB of slice headers
		"*1\r\n$2000000000\r\n",          // ~2GB bulk
	}
	for _, in := range cases {
		_, err := NewReader(strings.NewReader(in)).ReadCommand()
		// errors.Is rather than ==: each violation is now a distinct error carrying the
		// detail the server reports to the client, and each wraps ErrProtocol so this
		// general test still holds.
		if !errors.Is(err, ErrProtocol) {
			t.Errorf("ReadCommand(%q) err = %v; want a protocol error", in, err)
		}
		// And each must name what was wrong, because the whole point of reporting it is
		// that the caller can act on it.
		if ProtocolErrorText(err) == "" {
			t.Errorf("ReadCommand(%q) err = %v has no client-facing detail", in, err)
		}
	}
}

func TestReadCommandInline(t *testing.T) {
	args, err := NewReader(strings.NewReader("PING hello\r\n")).ReadCommand()
	if err != nil {
		t.Fatalf("ReadCommand: %v", err)
	}
	if len(args) != 2 || string(args[0]) != "PING" || string(args[1]) != "hello" {
		t.Fatalf("inline parse = %q", args)
	}
}

func TestReadCommandMultiple(t *testing.T) {
	in := "*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n"
	r := NewReader(strings.NewReader(in))
	for i := 0; i < 2; i++ {
		args, err := r.ReadCommand()
		if err != nil {
			t.Fatalf("command %d: %v", i, err)
		}
		if string(args[0]) != "PING" {
			t.Fatalf("command %d = %q", i, args)
		}
	}
}

func TestWriterTypes(t *testing.T) {
	cases := []struct {
		name string
		fn   func(*Writer) error
		want string
	}{
		{"simple", func(w *Writer) error { return w.WriteSimple("OK") }, "+OK\r\n"},
		{"error", func(w *Writer) error { return w.WriteError("ERR nope") }, "-ERR nope\r\n"},
		{"int", func(w *Writer) error { return w.WriteInt(42) }, ":42\r\n"},
		{"bulk", func(w *Writer) error { return w.WriteBulk([]byte("hi")) }, "$2\r\nhi\r\n"},
		{"null", func(w *Writer) error { return w.WriteBulk(nil) }, "$-1\r\n"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf)
			if err := c.fn(w); err != nil {
				t.Fatal(err)
			}
			w.Flush()
			if buf.String() != c.want {
				t.Fatalf("got %q; want %q", buf.String(), c.want)
			}
		})
	}
}

func TestWriteArray(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.WriteArrayHeader(2)
	w.WriteBulk([]byte("a"))
	w.WriteBulk([]byte("bb"))
	w.Flush()
	want := "*2\r\n$1\r\na\r\n$2\r\nbb\r\n"
	if buf.String() != want {
		t.Fatalf("got %q; want %q", buf.String(), want)
	}
}

// Round-trip: what the Writer produces for a bulk string is what the Reader
// expects to consume as an argument.
func TestRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.WriteArrayHeader(2)
	w.WriteBulk([]byte("GET"))
	w.WriteBulk([]byte("k"))
	w.Flush()

	args, err := (&Reader{r: bufio.NewReader(&buf)}).ReadCommand()
	if err != nil {
		t.Fatalf("ReadCommand: %v", err)
	}
	if len(args) != 2 || string(args[0]) != "GET" || string(args[1]) != "k" {
		t.Fatalf("round-trip = %q", args)
	}
}

// TestProtoDefaultsToRESP2 pins the default. Every writer the server creates for
// something other than a client connection -- the sink an AOF replay discards
// replies into, the one a replica feed encodes commands with -- relies on it, so a
// changed default would silently rewrite those streams.
func TestProtoDefaultsToRESP2(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if w.Proto() != ProtoRESP2 {
		t.Fatalf("default proto = %d; want %d", w.Proto(), ProtoRESP2)
	}
}

// TestVersionedWriters checks each RESP3 type against its RESP2 fallback. The
// expected bytes are the ones real Redis emits for the same value (captured from
// redis:7-alpine via DEBUG PROTOCOL), so this is a compatibility test and not merely
// a self-consistency one.
func TestVersionedWriters(t *testing.T) {
	cases := []struct {
		name         string
		write        func(*Writer)
		resp2, resp3 string
	}{
		{"null", func(w *Writer) { w.WriteNull() }, "$-1\r\n", "_\r\n"},
		{"null array", func(w *Writer) { w.WriteNullArray() }, "*-1\r\n", "_\r\n"},
		{"true", func(w *Writer) { w.WriteBool(true) }, ":1\r\n", "#t\r\n"},
		{"false", func(w *Writer) { w.WriteBool(false) }, ":0\r\n", "#f\r\n"},
		{"double", func(w *Writer) { w.WriteDouble(3.141) }, "$5\r\n3.141\r\n", ",3.141\r\n"},
		{"double integral", func(w *Writer) { w.WriteDouble(2) }, "$1\r\n2\r\n", ",2\r\n"},
		{"double inf", func(w *Writer) { w.WriteDouble(math.Inf(1)) }, "$3\r\ninf\r\n", ",inf\r\n"},
		{"double -inf", func(w *Writer) { w.WriteDouble(math.Inf(-1)) }, "$4\r\n-inf\r\n", ",-inf\r\n"},
		{"bignum", func(w *Writer) { w.WriteBigNumber("123456789012345678901234567890") },
			"$30\r\n123456789012345678901234567890\r\n", "(123456789012345678901234567890\r\n"},
		{"verbatim", func(w *Writer) { w.WriteVerbatim("txt", []byte("a\nb")) },
			"$3\r\na\nb\r\n", "=7\r\ntxt:a\nb\r\n"},
		{"map", func(w *Writer) {
			w.WriteMapHeader(1)
			w.WriteBulk([]byte("k"))
			w.WriteBulk([]byte("v"))
		}, "*2\r\n$1\r\nk\r\n$1\r\nv\r\n", "%1\r\n$1\r\nk\r\n$1\r\nv\r\n"},
		{"set", func(w *Writer) {
			w.WriteSetHeader(1)
			w.WriteBulk([]byte("m"))
		}, "*1\r\n$1\r\nm\r\n", "~1\r\n$1\r\nm\r\n"},
		{"push", func(w *Writer) {
			w.WritePushHeader(2)
			w.WriteBulk([]byte("message"))
			w.WriteBulk([]byte("x"))
		}, "*2\r\n$7\r\nmessage\r\n$1\r\nx\r\n", ">2\r\n$7\r\nmessage\r\n$1\r\nx\r\n"},
		// An attribute has no RESP2 encoding: the header writes nothing at all, which is
		// what lets a caller emit the pairs only when Proto() says to.
		{"attribute header", func(w *Writer) { w.WriteAttributeHeader(1) }, "", "|1\r\n"},
	}
	for _, tc := range cases {
		for _, proto := range []int{ProtoRESP2, ProtoRESP3} {
			var buf bytes.Buffer
			w := NewWriter(&buf)
			w.SetProto(proto)
			tc.write(w)
			if err := w.Flush(); err != nil {
				t.Fatalf("%s: flush: %v", tc.name, err)
			}
			want := tc.resp2
			if proto == ProtoRESP3 {
				want = tc.resp3
			}
			if got := buf.String(); got != want {
				t.Errorf("%s under RESP%d = %q; want %q", tc.name, proto, got, want)
			}
		}
	}
}

// TestFormatDouble pins the text both protocols share. RESP2 puts it in a bulk
// string and RESP3 after a comma, so a disagreement here would report two different
// scores for one value.
func TestFormatDouble(t *testing.T) {
	// Every expectation below was read off redis:7.2 rather than reasoned about: these are
	// ZSCORE replies for the same values. Go's 'g' verb disagrees with several of them --
	// it renders 17179869185.5 as "1.71798691855e+10" and 1e-7 as "1e-07" -- and the
	// disagreement is not cosmetic, because INCRBYFLOAT propagates its result as text via
	// SET, so this formatting reaches the AOF and every replica.
	cases := map[float64]string{
		0: "0", 1: "1", 1.5: "1.5", -2.25: "-2.25",
		math.Inf(1): "inf", math.Inf(-1): "-inf",

		// Fixed notation, which is where Go's 'g' broke first.
		17179869185.5:    "17179869185.5",
		3.1415926535:     "3.1415926535",
		0.005:            "0.005",
		3.0e-5:           "0.00003",
		1.0e-6:           "0.000001",
		1.0e17:           "100000000000000000",
		1.0e18:           "1000000000000000000",
		3479099956230698: "3479099956230698", // a 52-bit geohash score
		200:              "200",

		// Outside the fixed range Redis uses an exponent, unpadded.
		1.0e19:  "1e+19",
		1.0e20:  "1e+20",
		1.0e30:  "1e+30",
		1.0e100: "1e+100",
		1.0e-7:  "1e-7",
		1.0e-10: "1e-10",
		1.5e300: "1.5e+300",
		3.0e300: "3e+300",
	}
	for in, want := range cases {
		if got := FormatDouble(in); got != want {
			t.Errorf("FormatDouble(%v) = %q; want %q", in, got, want)
		}
	}
	if got := FormatDouble(math.NaN()); got != "nan" {
		t.Errorf("FormatDouble(NaN) = %q; want %q", got, "nan")
	}
	// Whatever the notation, the text has to read back as the same float. A formatter that
	// round-trips is the property that actually matters for propagation.
	for _, f := range []float64{17179869185.5, 3.1415926535, 1e-7, 1e19, 1.5e300, 0.005} {
		back, err := strconv.ParseFloat(FormatDouble(f), 64)
		if err != nil || back != f {
			t.Errorf("FormatDouble(%v) = %q does not round-trip (%v, %v)", f, FormatDouble(f), back, err)
		}
	}
}

// TestParseDouble pins the reading half against the same reference the formatting half
// was pinned against. The expectations below were measured on redis:7.2-alpine, each
// through a command that parses a double (ZADD for the operand cases, INCRBYFLOAT on a
// planted value for the stored-value ones).
//
// The underscore rows are the reason the function exists. strconv.ParseFloat implements
// Go's float *literal* grammar, in which "1_0" is a well-formed 10, so every parse site
// that reached for it accepted a spelling Redis rejects -- and dropped the separator
// silently, turning a mistyped thousands separator into a value two orders of magnitude
// out.
func TestParseDouble(t *testing.T) {
	cases := []struct {
		in   string
		want float64
		ok   bool
	}{
		// Redis: "ERR value is not a valid float" for every underscored form.
		{"1_0", 0, false},
		{"1_000.5", 0, false},
		{"1_", 0, false},
		{"_1", 0, false},
		{"1__0", 0, false},
		{"1e_5", 0, false},
		// Everything Redis's strtod accepts and Go's grammar spells the same way.
		{"1", 1, true},
		{"+5", 5, true},
		{".5", 0.5, true},
		{"5.", 5, true},
		{"0.0", 0, true},
		{"-0", 0, true},
		{"1e-7", 1e-7, true},
		{"17179869185.5", 17179869185.5, true},
		{"0x1p3", 8, true}, // the hex form both accept
		{"inf", math.Inf(1), true},
		{"-inf", math.Inf(-1), true},
		{"infinity", math.Inf(1), true},
		// Malformed, rejected by both.
		{"", 0, false},
		{"1e", 0, false},
		{" 1", 0, false},
		{"1 ", 0, false},
		{"abc", 0, false},
		{"1e400", 0, false}, // overflow: Redis rejects on ERANGE, Go on ErrRange
	}
	for _, tc := range cases {
		got, ok := ParseDouble(tc.in)
		if ok != tc.ok {
			t.Errorf("ParseDouble(%q) ok = %v; want %v", tc.in, ok, tc.ok)
			continue
		}
		if ok && got != tc.want {
			t.Errorf("ParseDouble(%q) = %v; want %v", tc.in, got, tc.want)
		}
	}
	// NaN parses -- the callers reject it, since which error they report differs by
	// command -- but it must be a NaN rather than silently becoming a number.
	if got, ok := ParseDouble("nan"); !ok || !math.IsNaN(got) {
		t.Errorf("ParseDouble(%q) = %v, %v; want NaN, true", "nan", got, ok)
	}
	// Whatever FormatDouble spells, ParseDouble must read back unchanged: the two halves
	// are one decision, and a value that does not survive the round trip is a value a
	// replica reconstructs differently from its master.
	for _, f := range []float64{17179869185.5, 3.1415926535, 1e-7, 1e19, 1.5e300, 0.005, 0, -2.5, 1 << 52} {
		back, ok := ParseDouble(FormatDouble(f))
		if !ok || back != f {
			t.Errorf("ParseDouble(FormatDouble(%v) = %q) = %v, %v", f, FormatDouble(f), back, ok)
		}
	}
	for _, f := range []float64{math.Inf(1), math.Inf(-1)} {
		if back, ok := ParseDouble(FormatDouble(f)); !ok || back != f {
			t.Errorf("ParseDouble(FormatDouble(%v) = %q) = %v, %v", f, FormatDouble(f), back, ok)
		}
	}
}

// TestReadCommandInlineQuoting covers the inline parser's quoting.
//
// This was the worst bug found in the client-facing parser, because it was silent: the
// splitter divided on whitespace and kept the quote and escape characters as literal
// bytes, so `set "a\x41b" v` over a telnet or nc session wrote a key named
// `"a\x41b"` -- eight bytes including the quotes -- and answered +OK. The caller was
// told its write succeeded and got a different key than it asked for, with nothing
// anywhere reporting a problem. An unterminated quote was worse still: `set "oops v`
// created a key called `"oops` instead of being refused.
//
// Expectations below match Redis's sdssplitargs, verified against redis:7.2.
func TestReadCommandInlineQuoting(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{`PING`, []string{"PING"}},
		{`set foo bar`, []string{"set", "foo", "bar"}},
		// A quoted argument loses its quotes.
		{`set "foo bar" v`, []string{"set", "foo bar", "v"}},
		{`set 'foo bar' v`, []string{"set", "foo bar", "v"}},
		// \xHH is a byte, not four characters.
		{`set "a\x41b" v`, []string{"set", "aAb", "v"}},
		// The C escapes Redis recognises inside double quotes.
		{`set "a\nb" v`, []string{"set", "a\nb", "v"}},
		{`set "a\tb" v`, []string{"set", "a\tb", "v"}},
		{`set "a\"b" v`, []string{"set", `a"b`, "v"}},
		// Single quotes take only \' -- everything else is literal.
		{`set 'a\nb' v`, []string{"set", `a\nb`, "v"}},
		{`set 'a\'b' v`, []string{"set", "a'b", "v"}},
		// An empty quoted argument is still an argument.
		{`set k ""`, []string{"set", "k", ""}},
		// Tabs separate arguments as spaces do.
		{"set\tfoo\tbar", []string{"set", "foo", "bar"}},
		// Runs of separators collapse.
		{`set   foo    bar`, []string{"set", "foo", "bar"}},
	}
	for _, tc := range cases {
		args, err := NewReader(strings.NewReader(tc.in + "\r\n")).ReadCommand()
		if err != nil {
			t.Errorf("ReadCommand(%q) unexpected error %v", tc.in, err)
			continue
		}
		if len(args) != len(tc.want) {
			t.Errorf("ReadCommand(%q) = %q; want %q", tc.in, args, tc.want)
			continue
		}
		for i := range args {
			if string(args[i]) != tc.want[i] {
				t.Errorf("ReadCommand(%q) arg %d = %q; want %q", tc.in, i, args[i], tc.want[i])
			}
		}
	}

	// Malformed quoting is a protocol error, never a literal. Writing the quote into the
	// keyspace is the outcome this rejects.
	for _, bad := range []string{
		`set "unterminated v`,
		`set 'unterminated v`,
		`set "closed"then v`, // a closing quote must be followed by a separator
		`set 'closed'then v`,
		`get "`,
		`get '`,
	} {
		_, err := NewReader(strings.NewReader(bad + "\r\n")).ReadCommand()
		if !errors.Is(err, ErrUnbalancedQuotes) {
			t.Errorf("ReadCommand(%q) err = %v; want unbalanced quotes", bad, err)
		}
		if ProtocolErrorText(err) != "Protocol error: unbalanced quotes in request" {
			t.Errorf("ReadCommand(%q) detail = %q", bad, ProtocolErrorText(err))
		}
	}
}

// TestReadCommandTolerantCases covers the malformed input Redis *ignores* rather than
// treating as fatal. Dropping a connection for one of these disconnects a client that
// Redis would have kept serving.
func TestReadCommandTolerantCases(t *testing.T) {
	// A negative multibulk count is the legacy null multibulk; a zero count is an empty
	// command. Both are skipped and the next request is read.
	for _, in := range []string{"*-1\r\nPING\r\n", "*-10\r\nPING\r\n", "*0\r\nPING\r\n", "\r\nPING\r\n", "\n\nPING\r\n"} {
		args, err := NewReader(strings.NewReader(in)).ReadCommand()
		if err != nil {
			t.Errorf("ReadCommand(%q) err = %v; want the following command", in, err)
			continue
		}
		if len(args) != 1 || string(args[0]) != "PING" {
			t.Errorf("ReadCommand(%q) = %q; want [PING]", in, args)
		}
	}
}

// TestProtocolErrorTextOnlyForProtocolErrors covers the guard that decides whether the
// server says anything before hanging up: an EOF or a network fault has nobody to tell.
func TestProtocolErrorTextOnlyForProtocolErrors(t *testing.T) {
	if got := ProtocolErrorText(nil); got != "" {
		t.Errorf("ProtocolErrorText(nil) = %q; want empty", got)
	}
	if got := ProtocolErrorText(io.EOF); got != "" {
		t.Errorf("ProtocolErrorText(io.EOF) = %q; want empty (nobody left to tell)", got)
	}
	// The expected-'$' case names the byte it found, which is the diagnostic value.
	_, err := NewReader(strings.NewReader("*1\r\nfoo\r\n")).ReadCommand()
	if got := ProtocolErrorText(err); got != "Protocol error: expected '$', got 'f'" {
		t.Errorf("expected-bulk detail = %q", got)
	}
}
