package resp

import (
	"bytes"
	"io"
	"strconv"
	"strings"
	"testing"
)

// The codec benchmarks. They exist to isolate the two halves of per-command cost that a
// pipelined client pays in full and an unpipelined one hides behind a syscall pair -- see
// internal/server/pipeline_bench_test.go for why the pipelined case is the one that
// matters, and read allocs/op rather than ns/op for the same reason.

// endlessReader repeats one buffer forever, so ReadCommand can be measured without the
// benchmark's own bookkeeping appearing in the profile between calls.
type endlessReader struct {
	buf []byte
	off int
}

func (e *endlessReader) Read(p []byte) (int, error) {
	n := 0
	for n < len(p) {
		c := copy(p[n:], e.buf[e.off:])
		n += c
		e.off += c
		if e.off == len(e.buf) {
			e.off = 0
		}
	}
	return n, nil
}

func benchReadCommand(b *testing.B, args ...string) {
	var sb strings.Builder
	sb.WriteString("*" + strconv.Itoa(len(args)) + "\r\n")
	for _, a := range args {
		sb.WriteString("$" + strconv.Itoa(len(a)) + "\r\n" + a + "\r\n")
	}
	r := NewReader(&endlessReader{buf: []byte(sb.String())})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := r.ReadCommand()
		if err != nil {
			b.Fatalf("ReadCommand: %v", err)
		}
		if len(got) != len(args) {
			b.Fatalf("ReadCommand returned %d arguments; want %d", len(got), len(args))
		}
	}
}

// BenchmarkReadCommandSet and its relatives are the shapes redis-benchmark sends.
func BenchmarkReadCommandSet(b *testing.B) { benchReadCommand(b, "SET", "key:12345", "xxx") }
func BenchmarkReadCommandGet(b *testing.B) { benchReadCommand(b, "GET", "key:12345") }

// BenchmarkReadCommandLargeValue is the case where one argument dominates: the cost has to
// be the copy of the payload and not the framing.
func BenchmarkReadCommandLargeValue(b *testing.B) {
	benchReadCommand(b, "SET", "key:12345", strings.Repeat("x", 4096))
}

// BenchmarkWriteReplyBulk and BenchmarkWriteReplyInt are the reply half: the length header
// of a bulk string and the digits of an integer are on the path of every GET and every
// INCR respectively.
func BenchmarkWriteReplyBulk(b *testing.B) {
	w := NewWriter(io.Discard)
	val := []byte("xxx")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w.WriteBulk(val)
	}
	w.Flush()
}

func BenchmarkWriteReplyInt(b *testing.B) {
	w := NewWriter(io.Discard)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w.WriteInt(int64(i))
	}
	w.Flush()
}

// BenchmarkWriteReplyArray is a multi-element reply -- an LRANGE or an HGETALL -- where the
// per-element encoding is paid as many times as there are elements.
func BenchmarkWriteReplyArray(b *testing.B) {
	w := NewWriter(io.Discard)
	items := bytes.Split([]byte("a b c d e f g h"), []byte(" "))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w.WriteArrayHeader(len(items))
		for _, it := range items {
			w.WriteBulk(it)
		}
	}
	w.Flush()
}
