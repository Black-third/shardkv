package server

import (
	"bufio"
	"context"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/Black-third/shardkv/internal/aof"
	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

func feedTx(a *txApplier, parts ...string) {
	cmd := make([][]byte, len(parts))
	for i, p := range parts {
		cmd[i] = []byte(p)
	}
	a.feed(cmd)
}

// TestTxApplierAtomicReplay verifies all-or-nothing replay: a complete
// MULTI..EXEC applies every command, while a transaction truncated before EXEC
// (as a crash would leave the AOF) applies nothing.
func TestTxApplierAtomicReplay(t *testing.T) {
	committed := New(store.New(8))
	a := newTxApplier(committed, resp.NewWriter(io.Discard))
	feedTx(a, "MULTI")
	feedTx(a, "SET", "a", "1")
	feedTx(a, "SET", "b", "2")
	feedTx(a, "EXEC")
	if v, ok := committed.store.Get("a"); !ok || string(v) != "1" {
		t.Fatalf("committed 'a' = %q,%v; want 1", v, ok)
	}
	if v, ok := committed.store.Get("b"); !ok || string(v) != "2" {
		t.Fatalf("committed 'b' = %q,%v; want 2", v, ok)
	}

	truncated := New(store.New(8))
	b := newTxApplier(truncated, resp.NewWriter(io.Discard))
	feedTx(b, "MULTI")
	feedTx(b, "SET", "a", "1")
	feedTx(b, "SET", "b", "2") // crash: no EXEC
	if _, ok := truncated.store.Get("a"); ok {
		t.Fatal("truncated transaction applied 'a'")
	}
	if _, ok := truncated.store.Get("b"); ok {
		t.Fatal("truncated transaction applied 'b'")
	}
}

// TestTransactionReadOnExpiredKeyNoDeadlock exercises the case that would
// deadlock if expiration propagated under propMu: with the AOF on, EXEC holds
// propMu across the batch, and a queued GET lazily expires a key — which must
// only invalidate WATCHers, not take propMu again.
func TestTransactionReadOnExpiredKeyNoDeadlock(t *testing.T) {
	srv := New(store.New(8))
	logf, err := aof.Open(filepath.Join(t.TempDir(), "t.aof"), aof.SyncNo)
	if err != nil {
		t.Fatal(err)
	}
	defer logf.Close()
	srv.AttachAOF(logf) // propagating = true -> EXEC holds propMu across the batch
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { srv.Serve(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second)) // fail fast if EXEC deadlocks
	br := bufio.NewReader(conn)
	send := func(s string) string {
		conn.Write([]byte(s + "\r\n"))
		return readReply(t, br)
	}

	send("SET k v PX 1")
	time.Sleep(30 * time.Millisecond) // let k expire (lazily; no janitor here)
	send("MULTI")
	send("GET k")       // queued; expires k during EXEC while propMu is held
	send("SET other 1") // queued write
	if got := send("EXEC"); got != "[(nil) +OK]" {
		t.Fatalf("EXEC = %q; want [(nil) +OK] (and no deadlock)", got)
	}
	if got := send("GET other"); got != "1" {
		t.Fatalf("GET other = %q; want 1", got)
	}
}
