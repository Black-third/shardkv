package resp

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
)

// FuzzReadCommand checks that the request parser never panics on arbitrary
// input, that any command it successfully parses round-trips -- re-encoding
// the parsed arguments and parsing them again yields the same arguments -- and
// that ReadCommand's two paths agree.
//
// The last of those is why the same input is parsed twice here. A frame that is
// entirely buffered is walked in place; one that is still arriving is parsed a
// line at a time, and feeding the bytes one at a time is what forces the second
// path (the buffer is empty every time ReadCommand is entered). Two pieces of
// code over one grammar is the drift risk this project's notes warn about, so
// the fuzzer is pointed at the difference rather than only at the parse.
func FuzzReadCommand(f *testing.F) {
	seeds := []string{
		"*1\r\n$4\r\nPING\r\n",
		"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
		"PING hello\r\n",
		"*2\r\n$3\r\nGET\r\n$0\r\n\r\n",
		"*-1\r\n",
		"*abc\r\n",
		"$5\r\nhello\r\n",
		"",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewReader(bytes.NewReader(data))
		args, err := r.ReadCommand() // must never panic

		slow := NewReader(&byteAtATimeReader{s: string(data)})
		slowArgs, slowErr := slow.ReadCommand()
		if (err == nil) != (slowErr == nil) ||
			(err != nil && slowErr != nil && err.Error() != slowErr.Error()) {
			t.Fatalf("buffered parse of %q gave (%q, %v), streamed gave (%q, %v)",
				data, args, err, slowArgs, slowErr)
		}
		if err == nil {
			if len(args) != len(slowArgs) {
				t.Fatalf("buffered parse of %q gave %d arguments, streamed gave %d",
					data, len(args), len(slowArgs))
			}
			for i := range args {
				if !bytes.Equal(args[i], slowArgs[i]) {
					t.Fatalf("argument %d of %q: %q buffered, %q streamed", i, data, args[i], slowArgs[i])
				}
			}
			if r.Consumed() != slow.Consumed() {
				t.Fatalf("%q consumed %d bytes buffered and %d streamed", data, r.Consumed(), slow.Consumed())
			}
		}
		if err != nil {
			return
		}

		// Re-encode and re-parse: the result must be identical.
		var buf bytes.Buffer
		w := NewWriter(&buf)
		if err := w.WriteCommand(args); err != nil {
			t.Fatalf("WriteCommand: %v", err)
		}
		w.Flush()

		r2 := &Reader{r: bufio.NewReader(&buf)}
		args2, err := r2.ReadCommand()
		if err != nil {
			t.Fatalf("re-parse failed for %q: %v", data, err)
		}
		if len(args) != len(args2) {
			t.Fatalf("arg count changed: %d -> %d", len(args), len(args2))
		}
		for i := range args {
			if !bytes.Equal(args[i], args2[i]) {
				t.Fatalf("arg %d changed: %q -> %q", i, args[i], args2[i])
			}
		}
	})
}

// TestFuzzSeedsDoNotPanic runs the seed corpus directly so the regular `go test`
// run (without -fuzz) still exercises these inputs.
func TestFuzzSeedsDoNotPanic(t *testing.T) {
	for _, s := range []string{"*\r\n", "*1\r\n$x\r\n", "*1\r\n$-5\r\n", "garbage"} {
		NewReader(strings.NewReader(s)).ReadCommand()
	}
}
