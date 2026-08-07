package server

import (
	"context"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/Black-third/shardkv/internal/store"
)

// The pipelined-path benchmarks.
//
// They exist because the measured weakness in the vs-redis comparison (see the README's
// benchmark section) is *tail latency under pipelining*: shardkv's p99 was the worse of the
// two in 37 of 50 paired `-P 16` comparisons, against 27 of 50 with no pipelining -- which
// is nothing. Pipelining is what amortises the syscall pair away, so what is left is
// per-command work: the RESP parse, the dispatch, the reply encoding, and the garbage every
// one of those produces. A garbage collector's cost lands in the tail, not the median, which
// is exactly the shape of the loss.
//
// So the number these report that is worth acting on is **allocs/op**, not ns/op. Allocation
// counts are a property of the code and reproduce on a loaded machine; wall-clock latency on
// a host running a dozen other containers does not (the README documents ratio CVs of
// 30-70% on the same host). Read the allocation column; treat the timing column as
// indicative only.
//
// The client half is deliberately allocation-free -- one prebuilt request buffer, one
// prebuilt reply buffer, io.ReadFull -- because testing.B's allocation counter is
// process-wide (it reads runtime.MemStats), so anything the driver allocates would be
// charged to the server. That is what makes the figure attributable.

// benchServer starts a server on loopback with persistence and replication off, which is
// the pure-cache configuration the vs-redis benchmark measured (invariant 1: no propMu on
// the write path).
func benchServer(b *testing.B) string {
	b.Helper()
	s := New(store.New(256))
	if err := s.SetDatabases(defaultDatabases); err != nil {
		b.Fatalf("SetDatabases: %v", err)
	}
	if err := s.Listen("127.0.0.1:0"); err != nil {
		b.Fatalf("Listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Serve(ctx); close(done) }()
	b.Cleanup(func() {
		cancel()
		<-done
	})
	return s.Addr().String()
}

// respCommand renders one command in the wire form a client sends.
func respCommand(args ...string) string {
	var sb strings.Builder
	sb.WriteString("*" + strconv.Itoa(len(args)) + "\r\n")
	for _, a := range args {
		sb.WriteString("$" + strconv.Itoa(len(a)) + "\r\n" + a + "\r\n")
	}
	return sb.String()
}

// benchPipelined sends depth commands per flush and reads their replies back, which is
// what redis-benchmark -P <depth> does. cmd(i) renders the i'th command of a batch, so a
// batch spreads over depth different keys rather than hammering one shard -- the same
// mistake the vs-redis harness exists to avoid (README: `-r 100000`).
//
// replyLen is the exact byte length of one reply, so the driver can read the batch back
// with one io.ReadFull into a buffer it allocated once.
func benchPipelined(b *testing.B, depth, replyLen int, setup []string, cmd func(i int) string) {
	addr := benchServer(b)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		b.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	if len(setup) > 0 {
		var pre strings.Builder
		for _, c := range setup {
			pre.WriteString(c)
		}
		if _, err := io.WriteString(conn, pre.String()); err != nil {
			b.Fatalf("setup write: %v", err)
		}
		// Drain the setup replies before the measured loop, so their bytes cannot be
		// mistaken for a batch's.
		scratch := make([]byte, 1)
		for range setup {
			for {
				if _, err := io.ReadFull(conn, scratch); err != nil {
					b.Fatalf("setup read: %v", err)
				}
				if scratch[0] == '\n' {
					break
				}
			}
		}
	}

	var batch strings.Builder
	for i := 0; i < depth; i++ {
		batch.WriteString(cmd(i))
	}
	request := []byte(batch.String())
	replies := make([]byte, depth*replyLen)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i += depth {
		if _, err := conn.Write(request); err != nil {
			b.Fatalf("write: %v", err)
		}
		if _, err := io.ReadFull(conn, replies); err != nil {
			b.Fatalf("read: %v", err)
		}
	}
	b.StopTimer()
}

const pipelineDepth = 16 // the -P 16 the README's pipelined sweep used

func benchKey(i int) string { return "key:" + strconv.Itoa(i) }

// BenchmarkPipelinedSet is the write side: the case whose p99 was 52.9 ms against Redis's
// 18.9 ms at 128 connections.
func BenchmarkPipelinedSet(b *testing.B) {
	benchPipelined(b, pipelineDepth, len("+OK\r\n"), nil, func(i int) string {
		return respCommand("SET", benchKey(i), "xxx")
	})
}

// BenchmarkPipelinedGet is the read side. Its reply is a bulk string, so it pays the
// length-header encoding a status reply does not.
func BenchmarkPipelinedGet(b *testing.B) {
	setup := make([]string, 0, pipelineDepth)
	for i := 0; i < pipelineDepth; i++ {
		setup = append(setup, respCommand("SET", benchKey(i), "xxx"))
	}
	benchPipelined(b, pipelineDepth, len("$3\r\nxxx\r\n"), setup, func(i int) string {
		return respCommand("GET", benchKey(i))
	})
}

// BenchmarkPipelinedLRange is the multi-element reply: one bulk string per element, so
// whatever a single element costs to encode is paid as many times as there are elements.
// It is the shape LRANGE, SMEMBERS, HGETALL, ZRANGE and KEYS all share.
func BenchmarkPipelinedLRange(b *testing.B) {
	const elems = 8
	setup := make([]string, 0, pipelineDepth*elems)
	for i := 0; i < pipelineDepth; i++ {
		for j := 0; j < elems; j++ {
			setup = append(setup, respCommand("RPUSH", benchKey(i), "xxx"))
		}
	}
	replyLen := len("*8\r\n") + elems*len("$3\r\nxxx\r\n")
	benchPipelined(b, pipelineDepth, replyLen, setup, func(i int) string {
		return respCommand("LRANGE", benchKey(i), "0", "-1")
	})
}

// BenchmarkPipelinedSMembers is the other multi-element shape: the elements arrive from the
// store as Go strings rather than as byte slices, which is what a set, a hash's fields, a
// sorted set's members and KEYS all have in common. Encoding one used to convert it to a
// []byte first, and that conversion allocates -- bufio.Write's argument escapes -- so it was
// one allocation per element of every such reply. See resp.WriteBulkString.
func BenchmarkPipelinedSMembers(b *testing.B) {
	const elems = 8
	setup := make([]string, 0, pipelineDepth)
	members := make([]string, 0, elems+2)
	for j := 0; j < elems; j++ {
		members = append(members, "m"+strconv.Itoa(j))
	}
	for i := 0; i < pipelineDepth; i++ {
		setup = append(setup, respCommand(append([]string{"SADD", benchKey(i)}, members...)...))
	}
	// Every member is two bytes, so the reply's length does not depend on the order a set
	// iterates in.
	replyLen := len("*8\r\n") + elems*len("$2\r\nm0\r\n")
	benchPipelined(b, pipelineDepth, replyLen, setup, func(i int) string {
		return respCommand("SMEMBERS", benchKey(i))
	})
}

// BenchmarkPipelinedIncr is a write whose reply is an integer, so it covers the integer
// encoding as well as the write path.
func BenchmarkPipelinedIncr(b *testing.B) {
	benchPipelined(b, pipelineDepth, len(":0\r\n"), nil, func(i int) string {
		// INCRBY by 0, so the reply's byte length is the same on every iteration and the
		// driver can read a whole batch with one ReadFull. What is being measured here is the
		// integer encoding and the write path, not the arithmetic.
		return respCommand("INCRBY", benchKey(i), "0")
	})
}
