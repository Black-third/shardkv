package resp

import (
	"bufio"
	"bytes"
	"math"
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
		if err != ErrProtocol {
			t.Errorf("ReadCommand(%q) err = %v; want ErrProtocol", in, err)
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
	cases := map[float64]string{
		0: "0", 1: "1", 1.5: "1.5", -2.25: "-2.25",
		math.Inf(1): "inf", math.Inf(-1): "-inf",
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
}
