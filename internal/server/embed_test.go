package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Black-third/shardkv/internal/store"
)

// TestEmbeddedSessionTakesTheClientPath is the decision in embed.go that a refactor could
// most plausibly undo, so it is pinned here rather than only through the facade.
//
// An embedded caller must reach executeCommand, not dispatch. dispatch exists for an AOF
// replay and a master's stream and is deliberately never gated -- invariant 13 -- so
// routing an embedded caller through it would silently exempt it from authentication, from
// the memory budget, and in cluster mode from the redirect that stops a node writing a key
// it does not own. Each of the three gates below is unreachable from dispatch, so each one
// answering proves which path the command took.
func TestEmbeddedSessionTakesTheClientPath(t *testing.T) {
	t.Run("authentication", func(t *testing.T) {
		s := New(store.New(8))
		s.SetRequirePass("secret")
		e := s.NewEmbeddedSession(nil)
		defer e.Close()

		if got := string(e.Do(cmdArgs("GET", "k"))); !strings.HasPrefix(got, "-NOAUTH") {
			t.Fatalf("GET without AUTH answered %q; want NOAUTH, which only the client path checks", got)
		}
		if got := string(e.Do(cmdArgs("AUTH", "secret"))); got != "+OK\r\n" {
			t.Fatalf("AUTH answered %q; want +OK", got)
		}
		if got := string(e.Do(cmdArgs("SET", "k", "v"))); got != "+OK\r\n" {
			t.Fatalf("SET after AUTH answered %q; want +OK", got)
		}
	})

	t.Run("transactions queue", func(t *testing.T) {
		s := New(store.New(8))
		e := s.NewEmbeddedSession(nil)
		defer e.Close()

		if got := string(e.Do(cmdArgs("MULTI"))); got != "+OK\r\n" {
			t.Fatalf("MULTI answered %q", got)
		}
		// QUEUED rather than the command's own reply is what proves the session's
		// transaction state was consulted: dispatch has no session and would have run it.
		if got := string(e.Do(cmdArgs("INCR", "n"))); got != "+QUEUED\r\n" {
			t.Fatalf("INCR inside MULTI answered %q; want +QUEUED", got)
		}
		if got := string(e.Do(cmdArgs("EXEC"))); got != "*1\r\n:1\r\n" {
			t.Fatalf("EXEC answered %q; want the queued INCR's result", got)
		}
	})

	t.Run("cluster redirect", func(t *testing.T) {
		s, _ := twoNodeCluster(t, []string{"mine"}, []string{"theirs"})
		e := s.NewEmbeddedSession(nil)
		defer e.Close()

		if got := string(e.Do(cmdArgs("GET", "mine"))); got != "$-1\r\n" {
			t.Fatalf("a key in a slot this node owns answered %q; want the null bulk", got)
		}
		if got := string(e.Do(cmdArgs("GET", "theirs"))); !strings.HasPrefix(got, "-MOVED") {
			t.Fatalf("a key in another node's slot answered %q; want MOVED -- an embedded "+
				"caller is a client, not a master, so it must be redirected", got)
		}
	})
}

// TestEmbeddedSessionIsRegisteredAndReleased checks the bookkeeping a connection normally
// gets from its serving goroutine's defer. An embedded session has no such goroutine, so
// Close is the only thing that can do it, and a leak here would grow the client registry
// for the lifetime of the server.
func TestEmbeddedSessionIsRegisteredAndReleased(t *testing.T) {
	s := New(store.New(8))
	before := len(s.snapshotSessions())

	e := s.NewEmbeddedSession(nil)
	if got := len(s.snapshotSessions()); got != before+1 {
		t.Fatalf("after NewEmbeddedSession the registry holds %d sessions; want %d", got, before+1)
	}
	// It is a client in every sense, so CLIENT LIST describes it -- with no peer address,
	// since it has no socket.
	list := string(e.Do(cmdArgs("CLIENT", "INFO")))
	if !strings.Contains(list, "addr= ") {
		t.Errorf("CLIENT INFO reports a peer address for a session with no socket: %q", list)
	}

	e.Close()
	if got := len(s.snapshotSessions()); got != before {
		t.Fatalf("after Close the registry holds %d sessions; want %d", got, before)
	}
}

// TestEmbeddedSessionDoesNotConsumeAMaxclientsSlot pins the one bound that deliberately
// does not apply to it. maxclients exists to cap file descriptors and goroutines, both of
// which come from the accept loop; an embedded session has neither, so a program that
// tightened the limit for its socket clients must not find its own in-process client
// turned away.
func TestEmbeddedSessionDoesNotConsumeAMaxclientsSlot(t *testing.T) {
	s := New(store.New(8))
	s.SetMaxClients(1)

	first := s.NewEmbeddedSession(nil)
	defer first.Close()
	second := s.NewEmbeddedSession(nil)
	defer second.Close()

	for i, e := range []*EmbeddedSession{first, second} {
		if got := string(e.Do(cmdArgs("PING"))); got != "+PONG\r\n" {
			t.Fatalf("embedded client %d answered %q under maxclients 1; want +PONG", i, got)
		}
	}
	if got := s.RejectedConnections(); got != 0 {
		t.Errorf("%d connections were counted as refused; an embedded client is not a connection", got)
	}
}

// TestEmbeddedSessionReportsQuit covers the one piece of connection teardown that has to be
// handed back to the caller: on a socket the loop hangs up when QUIT arrives, and here
// there is no socket to hang up, so the caller is told instead.
func TestEmbeddedSessionReportsQuit(t *testing.T) {
	s := New(store.New(8))
	e := s.NewEmbeddedSession(nil)
	defer e.Close()

	if e.Quit() {
		t.Fatal("a fresh session already reports Quit")
	}
	if got := string(e.Do(cmdArgs("PING"))); got != "+PONG\r\n" {
		t.Fatalf("PING answered %q", got)
	}
	if e.Quit() {
		t.Fatal("PING set Quit")
	}
	if got := string(e.Do(cmdArgs("QUIT"))); got != "+OK\r\n" {
		t.Fatalf("QUIT answered %q; want +OK", got)
	}
	if !e.Quit() {
		t.Fatal("QUIT did not set Quit")
	}
}

// TestEmbeddedSessionKeepsItsProtocol checks that HELLO sticks, which requires the session
// to keep one Writer across commands. It is the reason no API for the protocol version is
// needed: HELLO is an ordinary command and setting it is an ordinary call.
func TestEmbeddedSessionKeepsItsProtocol(t *testing.T) {
	s := New(store.New(8))
	e := s.NewEmbeddedSession(nil)
	defer e.Close()

	if got := e.Proto(); got != 2 {
		t.Fatalf("a session that has sent no HELLO reports protocol %d; want 2", got)
	}
	e.Do(cmdArgs("HSET", "h", "f", "v"))
	if got := string(e.Do(cmdArgs("HGETALL", "h"))); !strings.HasPrefix(got, "*2\r\n") {
		t.Fatalf("under RESP2, HGETALL answered %q; want a flat array", got)
	}
	if got := string(e.Do(cmdArgs("HELLO", "3"))); !strings.HasPrefix(got, "%") {
		t.Fatalf("HELLO 3 answered %q; want a RESP3 map", got)
	}
	if got := e.Proto(); got != 3 {
		t.Fatalf("after HELLO 3 the session reports protocol %d; want 3", got)
	}
	// The command after the HELLO must be encoded in the new protocol, which is only true
	// if the Writer -- and its proto -- survived between the two calls.
	if got := string(e.Do(cmdArgs("HGETALL", "h"))); !strings.HasPrefix(got, "%1\r\n") {
		t.Fatalf("under RESP3, HGETALL answered %q; want a map", got)
	}
}

// TestEmbeddedSessionSuppressesReplies checks that CLIENT REPLY works here, which needs the
// SKIP state machine to be stepped once per command exactly as the connection loop steps
// it. A withheld reply is reported as no bytes rather than as an empty one, so a caller can
// tell it from a reply it failed to read.
func TestEmbeddedSessionSuppressesReplies(t *testing.T) {
	s := New(store.New(8))
	e := s.NewEmbeddedSession(nil)
	defer e.Close()

	if got := e.Do(cmdArgs("CLIENT", "REPLY", "SKIP")); got != nil {
		t.Fatalf("CLIENT REPLY SKIP answered %q; it has no reply of its own", got)
	}
	if got := e.Do(cmdArgs("SET", "k", "v")); got != nil {
		t.Fatalf("the skipped command answered %q; want nothing", got)
	}
	// SKIP spans exactly one command, so the next one is answered again.
	if got := string(e.Do(cmdArgs("GET", "k"))); got != "$1\r\nv\r\n" {
		t.Fatalf("the command after the skipped one answered %q; want the value", got)
	}
}

// TestServeWithNoListenerOwnsTheLifetime covers the embedded case of Serve. It is not a
// degenerate call: it is where baseCtx is set, and baseCtx is what ties replication started
// by a client's REPLICAOF to the server's lifetime rather than to context.Background.
func TestServeWithNoListenerOwnsTheLifetime(t *testing.T) {
	s := New(store.New(8))
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx) }()

	// It must block rather than return immediately, or a caller's "the server is running"
	// would be false the moment Open returned.
	select {
	case err := <-done:
		t.Fatalf("Serve with no listener returned straight away: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve with no listener: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve with no listener did not return after its context was canceled")
	}
}
