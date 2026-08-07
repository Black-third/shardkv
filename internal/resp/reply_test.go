package resp

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
)

// readReply decodes one reply from wire bytes.
func readReply(t *testing.T, wire string) (any, error) {
	t.Helper()
	return NewReader(strings.NewReader(wire)).ReadReply()
}

// TestReadReplyTypes covers every type the Writer can emit, in both protocol versions'
// spellings. It is the round-trip half of the writer's tests: what the writer puts on the
// wire is what this has to be able to read back.
func TestReadReplyTypes(t *testing.T) {
	for _, tc := range []struct {
		name string
		wire string
		want any
	}{
		{"simple string", "+OK\r\n", "OK"},
		{"empty simple string", "+\r\n", ""},
		{"integer", ":42\r\n", int64(42)},
		{"negative integer", ":-1\r\n", int64(-1)},
		{"bulk string", "$5\r\nhello\r\n", "hello"},
		{"empty bulk string", "$0\r\n\r\n", ""},
		{"bulk string holding CRLF", "$4\r\na\r\nb\r\n", "a\r\nb"},
		{"RESP2 null bulk", "$-1\r\n", nil},
		{"RESP2 null array", "*-1\r\n", nil},
		{"RESP3 null", "_\r\n", nil},
		{"RESP3 true", "#t\r\n", true},
		{"RESP3 false", "#f\r\n", false},
		{"RESP3 double", ",1.5\r\n", 1.5},
		{"RESP3 integral double", ",3\r\n", 3.0},
		// The infinities are spelled as words because FormatDouble spells them that way,
		// which is why the decoder goes through ParseDouble and not strconv.ParseFloat.
		{"RESP3 positive infinity", ",inf\r\n", math.Inf(1)},
		{"RESP3 negative infinity", ",-inf\r\n", math.Inf(-1)},
		// A big number is an integer that by definition does not fit an integer reply, so
		// its digits are what comes back.
		{"RESP3 big number", "(3492890328409238509324850943850943825024385\r\n",
			"3492890328409238509324850943850943825024385"},
		// A verbatim string decodes to the same value its RESP2 bulk fallback would, so a
		// caller reading INFO does not see a different value for having sent HELLO 3.
		{"RESP3 verbatim string", "=15\r\ntxt:hello world\r\n", "hello world"},
	} {
		got, err := readReply(t, tc.wire)
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%s: %q decoded to %#v (%T); want %#v", tc.name, tc.wire, got, got, tc.want)
		}
	}
}

// TestReadReplyErrorIsAValue pins the decision the EXEC reply forces: an error reply is a
// value implementing error, not the decoder's error return, so an array can carry one
// alongside the results of the commands that succeeded.
func TestReadReplyErrorIsAValue(t *testing.T) {
	got, err := readReply(t, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	if err != nil {
		t.Fatalf("an error reply must not be returned as the decoder's error: %v", err)
	}
	asErr, ok := got.(error)
	if !ok {
		t.Fatalf("an error reply decoded to %#v (%T); want a value implementing error", got, got)
	}
	if !strings.HasPrefix(asErr.Error(), "WRONGTYPE") {
		t.Errorf("the error carries %q; want the server's own sentence", asErr)
	}

	// The shape that made this necessary: EXEC's array, where one element failed and the
	// ones on either side did not.
	got, err = readReply(t, "*3\r\n:1\r\n-ERR nope\r\n:2\r\n")
	if err != nil {
		t.Fatalf("EXEC-shaped reply: %v", err)
	}
	arr, ok := got.([]any)
	if !ok || len(arr) != 3 {
		t.Fatalf("decoded to %#v; want a 3-element array", got)
	}
	if arr[0] != int64(1) || arr[2] != int64(2) {
		t.Errorf("the elements around the failure are %#v and %#v; want 1 and 2", arr[0], arr[2])
	}
	if _, isErr := arr[1].(error); !isErr {
		t.Errorf("the failed element is %#v (%T); want a value implementing error", arr[1], arr[1])
	}
}

// TestReadReplyCollections checks that the three collection headers RESP3 distinguishes
// decode alike, and the one it reshaped does not.
//
// The set and the push are tags over data RESP2 already sent as an array, so decoding them
// alike is what makes a reply come out as the same Go value whichever protocol was
// negotiated. The map is the one RESP3 genuinely changed, so it is the one that differs.
func TestReadReplyCollections(t *testing.T) {
	array := []any{"a", "b"}
	for _, tc := range []struct {
		name string
		wire string
		want any
	}{
		{"array", "*2\r\n$1\r\na\r\n$1\r\nb\r\n", array},
		{"set decodes as an array", "~2\r\n$1\r\na\r\n$1\r\nb\r\n", array},
		{"push decodes as an array", ">2\r\n$1\r\na\r\n$1\r\nb\r\n", array},
		{"empty array", "*0\r\n", []any{}},
		{"nested array", "*1\r\n*2\r\n:1\r\n:2\r\n", []any{[]any{int64(1), int64(2)}}},
		{"mixed array", "*3\r\n:1\r\n$1\r\na\r\n$-1\r\n", []any{int64(1), "a", nil}},
		{"map", "%2\r\n$1\r\na\r\n:1\r\n$1\r\nb\r\n:2\r\n",
			map[string]any{"a": int64(1), "b": int64(2)}},
		{"empty map", "%0\r\n", map[string]any{}},
	} {
		got, err := readReply(t, tc.wire)
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: %q decoded to %#v; want %#v", tc.name, tc.wire, got, tc.want)
		}
	}
}

// TestReadReplyAttributeIsDiscarded covers the one type that is metadata rather than data.
// The Writer omits an attribute entirely for a RESP2 client; the decoder has to reach the
// same value from the RESP3 stream that carries it, or the two protocols would disagree
// about what a reply *is*.
func TestReadReplyAttributeIsDiscarded(t *testing.T) {
	// |1 <one pair> then the reply the attribute describes.
	wire := "|1\r\n$3\r\nttl\r\n:100\r\n$5\r\nvalue\r\n"
	got, err := readReply(t, wire)
	if err != nil {
		t.Fatalf("a reply behind an attribute: %v", err)
	}
	if got != "value" {
		t.Errorf("decoded to %#v; want the reply behind the attribute, \"value\"", got)
	}

	// The attribute's pairs are read, not skipped by byte count, so a nested value inside
	// one consumes exactly what it occupies and the reply after it still lines up.
	wire = "|1\r\n$4\r\nkeys\r\n*2\r\n:1\r\n:2\r\n+OK\r\n"
	if got, err = readReply(t, wire); err != nil || got != "OK" {
		t.Errorf("a reply behind an attribute holding an array = %#v, %v; want \"OK\", nil", got, err)
	}
}

// TestReadReplyMalformed checks that a stream that is not a reply is reported rather than
// guessed at. The last case is the one that matters most: nesting is the only dimension a
// few bytes of header can ask for unboundedly, and the answer to it has to be an error
// rather than a stack overflow, which is not recoverable.
func TestReadReplyMalformed(t *testing.T) {
	for _, tc := range []struct {
		name string
		wire string
	}{
		{"a byte that cannot begin a reply", "?what\r\n"},
		{"an integer that is not one", ":abc\r\n"},
		{"a boolean that is neither", "#maybe\r\n"},
		{"a double that is not one", ",abc\r\n"},
		{"a bulk length that is not a number", "$abc\r\n"},
		{"a bulk length past the cap", "$536870913\r\n"},
		{"an array length that is not a number", "*abc\r\n"},
		{"an array that ends early", "*2\r\n:1\r\n"},
		{"a map with no value for its key", "%1\r\n$1\r\na\r\n"},
		{"a negative map length", "%-1\r\n"},
		{"nothing at all", ""},
		{"nesting past the depth limit", strings.Repeat("*1\r\n", maxReplyDepth+2) + ":1\r\n"},
	} {
		got, err := readReply(t, tc.wire)
		if err == nil {
			t.Errorf("%s: %q decoded to %#v; want an error", tc.name, tc.wire, got)
		}
	}
}

// TestReadReplyDepthLimitAllowsRealReplies guards the limit from the other side: it must
// be above anything this server actually produces, or a legitimate reply would be refused.
// XINFO STREAM FULL is the deepest at six levels.
func TestReadReplyDepthLimitAllowsRealReplies(t *testing.T) {
	const deepest = 8 // comfortably past XINFO STREAM FULL's six
	wire := strings.Repeat("*1\r\n", deepest) + ":1\r\n"
	if _, err := readReply(t, wire); err != nil {
		t.Fatalf("a reply nested %d deep was refused: %v", deepest, err)
	}
}

// TestReadReplyMalformedIsAProtocolError checks the sentinels wrap ErrProtocol, so a
// caller distinguishing "malformed" from "the connection failed" needs one test rather
// than a list.
func TestReadReplyMalformedIsAProtocolError(t *testing.T) {
	for _, wire := range []string{"?what\r\n", ":abc\r\n", strings.Repeat("*1\r\n", maxReplyDepth+2)} {
		_, err := readReply(t, wire)
		if !errors.Is(err, ErrProtocol) {
			t.Errorf("%q gave %v; want an error wrapping ErrProtocol", wire, err)
		}
	}
}

// TestWriterAndReaderRoundTrip is the pairing that keeps the two halves from drifting: a
// value written by the Writer in the protocol version under test must decode back to the
// value the caller asked it to write. A reply type added to the Writer without a case here
// shows up as a decode failure rather than as a value the facade silently mis-reports.
func TestWriterAndReaderRoundTrip(t *testing.T) {
	for _, proto := range []int{ProtoRESP2, ProtoRESP3} {
		for _, tc := range []struct {
			name  string
			write func(w *Writer)
			// want is per protocol, because the point of RESP3 is that two of these differ.
			want map[int]any
		}{
			{"simple string",
				func(w *Writer) { w.WriteSimple("PONG") },
				map[int]any{ProtoRESP2: "PONG", ProtoRESP3: "PONG"}},
			{"integer",
				func(w *Writer) { w.WriteInt(7) },
				map[int]any{ProtoRESP2: int64(7), ProtoRESP3: int64(7)}},
			{"bulk string",
				func(w *Writer) { w.WriteBulk([]byte("hi")) },
				map[int]any{ProtoRESP2: "hi", ProtoRESP3: "hi"}},
			{"null",
				func(w *Writer) { w.WriteNull() },
				map[int]any{ProtoRESP2: nil, ProtoRESP3: nil}},
			{"null array",
				func(w *Writer) { w.WriteNullArray() },
				map[int]any{ProtoRESP2: nil, ProtoRESP3: nil}},
			{"boolean",
				func(w *Writer) { w.WriteBool(true) },
				map[int]any{ProtoRESP2: int64(1), ProtoRESP3: true}},
			{"double",
				func(w *Writer) { w.WriteDouble(1.5) },
				map[int]any{ProtoRESP2: "1.5", ProtoRESP3: 1.5}},
			{"verbatim string",
				func(w *Writer) { w.WriteVerbatim("txt", []byte("report")) },
				map[int]any{ProtoRESP2: "report", ProtoRESP3: "report"}},
			{"big number",
				func(w *Writer) { w.WriteBigNumber("12345678901234567890123") },
				map[int]any{
					ProtoRESP2: "12345678901234567890123",
					ProtoRESP3: "12345678901234567890123",
				}},
		} {
			var buf strings.Builder
			w := NewWriter(&buf)
			w.SetProto(proto)
			tc.write(w)
			if err := w.Flush(); err != nil {
				t.Fatalf("RESP%d %s: flush: %v", proto, tc.name, err)
			}
			got, err := readReply(t, buf.String())
			if err != nil {
				t.Errorf("RESP%d %s: %q: %v", proto, tc.name, buf.String(), err)
				continue
			}
			if got != tc.want[proto] {
				t.Errorf("RESP%d %s: %q decoded to %#v (%T); want %#v",
					proto, tc.name, buf.String(), got, got, tc.want[proto])
			}
		}
	}
}

// TestWriterAndReaderRoundTripCollections is the same pairing for the headers, where RESP3
// changed a shape rather than only a tag: a set and a push come back as the array RESP2
// sent, and a map comes back as a map only where the protocol has one.
func TestWriterAndReaderRoundTripCollections(t *testing.T) {
	items := []string{"a", "b"}
	for _, proto := range []int{ProtoRESP2, ProtoRESP3} {
		var buf strings.Builder
		w := NewWriter(&buf)
		w.SetProto(proto)
		w.WriteSetHeader(len(items))
		for _, it := range items {
			w.WriteBulk([]byte(it))
		}
		if err := w.Flush(); err != nil {
			t.Fatalf("RESP%d set: flush: %v", proto, err)
		}
		got, err := readReply(t, buf.String())
		if err != nil {
			t.Fatalf("RESP%d set: %v", proto, err)
		}
		if !reflect.DeepEqual(got, []any{"a", "b"}) {
			t.Errorf("RESP%d set decoded to %#v; want []any{\"a\", \"b\"} in both protocols", proto, got)
		}

		buf.Reset()
		w = NewWriter(&buf)
		w.SetProto(proto)
		w.WriteMapHeader(1)
		w.WriteBulk([]byte("field"))
		w.WriteBulk([]byte("value"))
		if err := w.Flush(); err != nil {
			t.Fatalf("RESP%d map: flush: %v", proto, err)
		}
		if got, err = readReply(t, buf.String()); err != nil {
			t.Fatalf("RESP%d map: %v", proto, err)
		}
		want := any([]any{"field", "value"}) // RESP2 flattens a map into an array
		if proto >= ProtoRESP3 {
			want = map[string]any{"field": "value"}
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("RESP%d map decoded to %#v; want %#v", proto, got, want)
		}
	}
}
