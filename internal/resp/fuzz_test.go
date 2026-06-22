package resp

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
)

// FuzzReadCommand checks that the request parser never panics on arbitrary
// input, and that any command it successfully parses round-trips: re-encoding
// the parsed arguments and parsing them again yields the same arguments.
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
