package server

import (
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

// TestSlowReplicaIsDroppedNotSkipped covers the slow-replica path. A full buffer
// used to make the master skip the command for that replica, which left it missing
// a write with nothing to ever tell it so -- permanently and silently diverged,
// despite the comment claiming it could resync. Now the feed is dropped, which ends
// the connection so the replica reconnects and takes a fresh snapshot.
func TestSlowReplicaIsDroppedNotSkipped(t *testing.T) {
	s := New(store.New(4))
	rc := newReplicaConn(1) // room for exactly one command
	s.mu.Lock()
	s.replicas[rc] = struct{}{}
	s.mu.Unlock()

	s.shipRaw(cmdArgs("SET", "a", "1")) // fits in the buffer
	select {
	case <-rc.dropped:
		t.Fatal("replica dropped while its buffer still had room")
	default:
	}

	s.shipRaw(cmdArgs("SET", "b", "2")) // buffer full -> drop rather than skip

	select {
	case <-rc.dropped:
	default:
		t.Fatal("a full replica buffer did not drop the feed (the write was silently skipped)")
	}
	s.mu.Lock()
	n := len(s.replicas)
	s.mu.Unlock()
	if n != 0 {
		t.Errorf("dropped replica still registered: %d replicas remain", n)
	}
	if got := s.replicaDrops.Load(); got != 1 {
		t.Errorf("replica_drops = %d; want 1", got)
	}

	// The command that did fit is still queued, in order: dropping the replica must
	// not disturb the stream the master already committed to.
	if got := flatten(<-rc.ch); got != "SET a 1" {
		t.Errorf("first buffered command = %q; want \"SET a 1\"", got)
	}
}

// TestDroppedReplicaFeedEnds checks the drop actually terminates the feed
// goroutine, which is what closes the connection and triggers the replica's
// reconnect-and-resync.
func TestDroppedReplicaFeedEnds(t *testing.T) {
	s := New(store.New(4))
	s.pingEvery = time.Hour // keep the keepalive out of this test

	done := make(chan struct{})
	go func() {
		// A reader that is immediately at EOF: this feed has no replica talking back,
		// which must not by itself end it (see readReplicaAcks).
		s.handleSync(context.Background(), &session{}, resp.NewReader(strings.NewReader("")),
			resp.NewWriter(io.Discard), cmdArgs("PSYNC"))
		close(done)
	}()

	// Wait for the feed to register, then drop it.
	var rc *replicaConn
	waitFor(t, "the replica feed to register", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		for r := range s.replicas {
			rc = r
		}
		return rc != nil
	})
	if got := s.fullSyncs.Load(); got != 1 {
		t.Errorf("sync_full = %d; want 1", got)
	}

	rc.drop()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleSync did not return after its feed was dropped")
	}
	s.mu.Lock()
	n := len(s.replicas)
	s.mu.Unlock()
	if n != 0 {
		t.Errorf("feed ended but the replica is still registered: %d remain", n)
	}
}

// TestMasterSendsKeepalive covers the master half of the liveness fix: an idle feed
// still carries traffic, so a replica can tell an idle master from a vanished one.
func TestMasterSendsKeepalive(t *testing.T) {
	s := New(store.New(4))
	s.pingEvery = 20 * time.Millisecond
	if err := s.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() { s.Serve(ctx); close(served) }()
	defer func() { cancel(); <-served }()

	conn, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	w := resp.NewWriter(conn)
	w.WriteCommand(cmdArgs("PSYNC"))
	if err := w.Flush(); err != nil {
		t.Fatalf("PSYNC: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	r := resp.NewReader(conn)
	for i := 0; i < 4; i++ { // an empty snapshot, so a PING should come straight away
		args, err := r.ReadCommand()
		if err != nil {
			t.Fatalf("reading the feed: %v", err)
		}
		if isKeepalive(args) {
			return
		}
	}
	t.Fatal("no keepalive arrived on an idle replica feed")
}

// TestReplicaReconnectsWhenMasterGoesSilent covers the replica half: a master that
// stops sending without closing the socket (a partition, a half-open connection, a
// hung process) used to leave the replication loop blocked in ReadCommand forever,
// serving stale data and never reconnecting.
func TestReplicaReconnectsWhenMasterGoesSilent(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	// A "master" that accepts the PSYNC and then says nothing at all, ever, while
	// holding the connection open.
	accepted := make(chan net.Conn, 8)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepted <- c
		}
	}()

	replica := New(store.New(4))
	replica.readTimeout = 50 * time.Millisecond // a real link would use seconds
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	replica.ReplicaOf(ctx, ln.Addr().String())

	// Two connections prove the replica gave up on the silent master and resynced.
	for i := 0; i < 2; i++ {
		select {
		case c := <-accepted:
			defer c.Close()
		case <-time.After(10 * time.Second):
			t.Fatalf("replica made only %d connection(s); it never gave up on the silent master", i)
		}
	}
}

// TestReplicaStaysConnectedWhileMasterPings is the false-positive guard: the read
// deadline must not tear down a link that is merely idle, because the keepalive
// keeps bytes flowing.
func TestReplicaStaysConnectedWhileMasterPings(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	accepted := make(chan net.Conn, 8)
	stopPinging := make(chan struct{})
	defer close(stopPinging)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepted <- c
			go func(c net.Conn) {
				defer c.Close()
				w := resp.NewWriter(c)
				t := time.NewTicker(10 * time.Millisecond)
				defer t.Stop()
				for {
					select {
					case <-stopPinging:
						return
					case <-t.C:
						if w.WriteCommand(keepalive) != nil || w.Flush() != nil {
							return
						}
					}
				}
			}(c)
		}
	}()

	replica := New(store.New(4))
	replica.readTimeout = 200 * time.Millisecond // 20x the ping interval
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	replica.ReplicaOf(ctx, ln.Addr().String())

	select {
	case c := <-accepted:
		defer c.Close()
	case <-time.After(5 * time.Second):
		t.Fatal("replica never connected")
	}

	// Well past the read timeout: a keepalive-fed link must not be torn down.
	select {
	case c := <-accepted:
		c.Close()
		t.Fatal("replica reconnected even though the master was sending keepalives")
	case <-time.After(600 * time.Millisecond):
	}
}

// TestKeepaliveDoesNotCorruptBufferedTransaction covers the hazard the keepalive
// introduces on the apply side. A PING can land between a MULTI and its EXEC in the
// wire stream, and the applier buffers whatever it sees; a stray command spliced
// into the group would be applied as part of the transaction.
func TestKeepaliveDoesNotCorruptBufferedTransaction(t *testing.T) {
	srv := New(store.New(8))
	a := newTxApplier(srv, resp.NewWriter(io.Discard))

	feedTx(a, "MULTI")
	feedTx(a, "SET", "a", "1")
	feedTx(a, "PING") // keepalive arriving mid-transaction
	feedTx(a, "SET", "b", "2")
	if len(a.buf) != 2 {
		t.Fatalf("buffered %d commands inside MULTI; want 2 (the keepalive must not be buffered)", len(a.buf))
	}
	feedTx(a, "EXEC")

	for _, k := range []string{"a", "b"} {
		if _, ok := srv.store.Get(k); !ok {
			t.Errorf("committed transaction is missing %q", k)
		}
	}
	if !a.inMulti && len(a.buf) == 0 {
		return
	}
	t.Errorf("applier state after EXEC: inMulti=%v buffered=%d; want false,0", a.inMulti, len(a.buf))
}

// TestKeepaliveIsNotForwardedOrPersisted checks the keepalive stays a point-to-point
// probe: it must not reach a chained replica's own downstream feed or its AOF, where
// it would be a stray command inside whatever framing happened to be open.
func TestKeepaliveIsNotForwardedOrPersisted(t *testing.T) {
	// A replica that both persists and has a downstream replica of its own.
	mid := New(store.New(4))
	next := tapReplica(t, mid)

	// Feed the middle server exactly what a master's stream looks like around a
	// keepalive, the way replicationLoop does.
	for _, args := range [][][]byte{
		cmdArgs("SET", "a", "1"),
		keepalive,
		cmdArgs("SET", "b", "2"),
	} {
		if isKeepalive(args) {
			continue // replicationLoop drops it before applying or forwarding
		}
		mid.propMu.Lock()
		mid.forward(args)
		mid.propMu.Unlock()
	}

	if got := flatten(next()); got != "SET a 1" {
		t.Fatalf("first forwarded command = %q", got)
	}
	if got := flatten(next()); got != "SET b 2" {
		t.Fatalf("second forwarded command = %q; the keepalive leaked downstream", got)
	}
}
