package resp

import (
	"bufio"
	"bytes"
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
