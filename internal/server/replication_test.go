package server

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"

	"github.com/Black-third/shardkv/internal/store"
)

func startServer(t *testing.T, st *store.Store) (*Server, string, func()) {
	t.Helper()
	s := New(st)
	if err := s.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Serve(ctx); close(done) }()
	return s, s.Addr().String(), func() {
		cancel()
		<-done
	}
}

func req(t *testing.T, conn net.Conn, br *bufio.Reader, cmd string) string {
	t.Helper()
	conn.Write([]byte(cmd + "\r\n"))
	return readReply(t, br)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 300; i++ {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

func TestReplication(t *testing.T) {
	// Master with some pre-existing data (exercises the snapshot path).
	_, masterAddr, stopM := startServer(t, store.New(8))
	defer stopM()

	mc, err := net.Dial("tcp", masterAddr)
	if err != nil {
		t.Fatalf("dial master: %v", err)
	}
	defer mc.Close()
	mbr := bufio.NewReader(mc)
	req(t, mc, mbr, "SET pre v1")
	req(t, mc, mbr, "RPUSH list a b c")

	// Replica replicating from the master.
	replica, replicaAddr, stopR := startServer(t, store.New(8))
	defer stopR()
	rctx, rcancel := context.WithCancel(context.Background())
	defer rcancel()
	replica.ReplicaOf(rctx, masterAddr)

	rc, err := net.Dial("tcp", replicaAddr)
	if err != nil {
		t.Fatalf("dial replica: %v", err)
	}
	defer rc.Close()
	rbr := bufio.NewReader(rc)

	// Snapshot should converge.
	waitFor(t, "snapshot of 'pre'", func() bool {
		return req(t, rc, rbr, "GET pre") == "v1"
	})
	waitFor(t, "snapshot of 'list'", func() bool {
		return req(t, rc, rbr, "LRANGE list 0 -1") == "[a b c]"
	})

	// Live writes on the master should stream to the replica.
	req(t, mc, mbr, "SET post v2")
	waitFor(t, "live propagation of 'post'", func() bool {
		return req(t, rc, rbr, "GET post") == "v2"
	})

	// The replica must reject client writes.
	if got := req(t, rc, rbr, "SET x y"); got != "-READONLY You can't write against a read only replica." {
		t.Fatalf("replica write = %q; want READONLY error", got)
	}

	// INFO should report the replica role.
	if got := req(t, rc, rbr, "INFO"); !contains(got, "role:replica") {
		t.Errorf("replica INFO missing role:replica; got:\n%s", got)
	}
}
