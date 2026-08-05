package server

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

// startAuthServer starts a password-protected server and returns it with its address.
func startAuthServer(t *testing.T, password string) (*Server, string, func()) {
	t.Helper()
	s := New(store.New(8))
	s.SetRequirePass(password)
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

// TestAuthGate covers the whole authentication surface of one connection: what an
// unauthenticated connection may run, the wording of each rejection, and that a
// successful AUTH opens the rest of the command set.
func TestAuthGate(t *testing.T) {
	_, addr, stop := startAuthServer(t, "s3cret")
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		// Allowed before authenticating: liveness and the handshake itself.
		{"PING", "+PONG"},
		// Everything else is refused, with the wording clients match on.
		{"GET k", "-" + errNoAuth},
		{"SET k v", "-" + errNoAuth},
		{"INFO", "-" + errNoAuth},
		{"DBSIZE", "-" + errNoAuth},
		{"SUBSCRIBE ch", "-" + errNoAuth},
		{"MULTI", "-" + errNoAuth},
		{"CONFIG GET requirepass", "-" + errNoAuth},
		// A wrong password does not authenticate. The legacy one-argument form keeps
		// the older wording; the ACL form reports the pair.
		{"AUTH wrong", "-ERR invalid password"},
		{"AUTH default wrong", "-" + errWrongPass},
		{"AUTH nobody s3cret", "-" + errWrongPass},
		{"GET k", "-" + errNoAuth},
		// The right password opens the connection.
		{"AUTH s3cret", "+OK"},
		{"SET k v", "+OK"},
		{"GET k", "v"},
		{"AUTH default s3cret", "+OK"}, // the ACL form works too
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	// A second connection starts unauthenticated: authentication is per connection.
	other := dialTx(t, addr)
	defer other.close()
	if got := other.cmd("GET k"); got != "-"+errNoAuth {
		t.Errorf("a fresh connection ran GET without authenticating: %q", got)
	}
}

// TestAuthWithoutPasswordConfigured pins the reply on an open server, which Redis
// makes an error rather than a no-op so a client cannot believe it authenticated
// against a server that has no credential at all.
func TestAuthWithoutPasswordConfigured(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	got := c.cmd("AUTH whatever")
	if !contains(got, "no password is set") {
		t.Errorf("AUTH on an open server = %q; want the \"no password is set\" error", got)
	}
	if got := c.cmd("GET k"); got != "(nil)" {
		t.Errorf("an open server refused a command after a failed AUTH: %q", got)
	}
}

// TestHelloAuthenticates covers the other way in: a client library that authenticates
// as part of its handshake rather than with a separate AUTH.
func TestHelloAuthenticates(t *testing.T) {
	_, addr, stop := startAuthServer(t, "hunter2")
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	if got := c.cmd("HELLO 2 AUTH default wrong"); got != "-"+errWrongPass {
		t.Errorf("HELLO with a bad password = %q; want %q", got, errWrongPass)
	}
	if got := c.cmd("GET k"); got != "-"+errNoAuth {
		t.Errorf("a failed HELLO AUTH authenticated the connection: %q", got)
	}
	if got := c.cmd("HELLO 2 AUTH default hunter2"); !contains(got, "proto") {
		t.Errorf("HELLO with the right password = %q; want the handshake reply", got)
	}
	if got := c.cmd("GET k"); got != "(nil)" {
		t.Errorf("HELLO AUTH did not authenticate the connection: %q", got)
	}
	// A bare HELLO on a protected connection must still refuse to hand out server
	// details before authentication.
	fresh := dialTx(t, addr)
	defer fresh.close()
	if got := fresh.cmd("HELLO"); got != "-"+errNoAuth {
		t.Errorf("bare HELLO before AUTH = %q; want %q", got, errNoAuth)
	}
}

// TestPSYNCRequiresAuth is the bypass check. PSYNC never reaches execute -- the
// connection loop diverts it into a replication feed -- so the gate has to be applied
// there too. Without it, an unauthenticated client could ask a password-protected
// master for a snapshot of its entire dataset.
func TestPSYNCRequiresAuth(t *testing.T) {
	_, addr, stop := startAuthServer(t, "letmein")
	defer stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	w := resp.NewWriter(conn)
	w.WriteCommand(cmdArgs("PSYNC", "?", "-1"))
	if err := w.Flush(); err != nil {
		t.Fatalf("PSYNC: %v", err)
	}
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	r := resp.NewReader(conn)
	if _, err := r.ReadStatus(); err == nil || !strings.Contains(err.Error(), "NOAUTH") {
		t.Fatalf("PSYNC without AUTH: err = %v; want the NOAUTH error", err)
	}

	// And no feed was set up: the connection is still an ordinary client.
	w.WriteCommand(cmdArgs("PING"))
	w.Flush()
	if status, err := r.ReadStatus(); err != nil || status != "PONG" {
		t.Fatalf("after a refused PSYNC: PING -> %q, %v; want PONG", status, err)
	}
}

// TestReplicaAuthenticatesOnEveryConnection covers the replica side, including the
// reconnect. A replica that authenticated only on its first connection would come
// back after any blip -- a dropped feed, a restarted master, a partition -- and be
// refused, then retry forever while serving data that never advances again.
func TestReplicaAuthenticatesOnEveryConnection(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	// A stand-in master that records the first command of each connection, then hangs
	// up so the replica has to reconnect.
	first := make(chan string, 4)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				args, err := resp.NewReader(conn).ReadCommand()
				if err != nil {
					return
				}
				first <- flatten(args)
			}(conn)
		}
	}()

	replica := New(store.New(4))
	replica.SetMasterAuth("replpass")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	replica.ReplicaOf(ctx, ln.Addr().String())

	for attempt := 1; attempt <= 2; attempt++ {
		select {
		case got := <-first:
			if got != "AUTH replpass" {
				t.Fatalf("connection %d began with %q; want the AUTH", attempt, got)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("the replica made only %d connection(s)", attempt-1)
		}
	}
}

// TestReplicaSyncsFromProtectedMaster is the end-to-end form: a real master with a
// password, a real replica with masterauth, and data that has to arrive.
func TestReplicaSyncsFromProtectedMaster(t *testing.T) {
	master, masterAddr, stopM := startAuthServer(t, "topsecret")
	defer stopM()

	mc := dialTx(t, masterAddr)
	defer mc.close()
	if got := mc.cmd("AUTH topsecret"); got != "+OK" {
		t.Fatalf("AUTH on the master = %q", got)
	}
	mc.cmd("SET before v1")

	replica, replicaAddr, stopR := startServer(t, store.New(8))
	defer stopR()
	replica.SetMasterAuth("topsecret")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	replica.ReplicaOf(ctx, masterAddr)

	rc := dialTx(t, replicaAddr)
	defer rc.close()
	waitFor(t, "the snapshot to arrive over an authenticated link", func() bool {
		return rc.cmd("GET before") == "v1"
	})
	mc.cmd("SET after v2")
	waitFor(t, "a live write to arrive", func() bool { return rc.cmd("GET after") == "v2" })

	// A replica with the wrong password must not sync at all.
	bad, badAddr, stopBad := startServer(t, store.New(8))
	defer stopBad()
	bad.SetMasterAuth("wrong")
	bad.ReplicaOf(ctx, masterAddr)
	badc := dialTx(t, badAddr)
	defer badc.close()
	time.Sleep(200 * time.Millisecond)
	if got := badc.cmd("GET before"); got != "(nil)" {
		t.Errorf("a replica with the wrong password received data: %q", got)
	}
	if got := master.fullSyncs.Load(); got != 1 {
		t.Errorf("master served %d full resyncs; want 1 (the unauthenticated one must not count)", got)
	}
}

// TestResetDeauthenticates pins the part of RESET that matters for a pooled
// connection: handing the socket to another caller must not hand over the
// authentication that came with it.
func TestResetDeauthenticates(t *testing.T) {
	_, addr, stop := startAuthServer(t, "pw")
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("AUTH pw")
	if got := c.cmd("SET k v"); got != "+OK" {
		t.Fatalf("SET after AUTH = %q", got)
	}
	if got := c.cmd("RESET"); got != "+RESET" {
		t.Fatalf("RESET = %q; want +RESET", got)
	}
	if got := c.cmd("GET k"); got != "-"+errNoAuth {
		t.Errorf("RESET left the connection authenticated: %q", got)
	}
}
