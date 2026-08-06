package server

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Black-third/shardkv/internal/store"
)

// errDebugRefused is the refusal as it appears on the wire in these tests.
var errDebugRefused = "-" + errDebugNotAllowed

// startGatedTestServer starts a server with the DEBUG gate at a chosen mode, so these tests
// see the default the shipped binary has rather than the "yes" every other test in this
// package needs from startTestServer. It binds loopback, which is what makes the "local"
// case meaningful, and it hands back the *Server so a test can move the gate mid-run.
func startGatedTestServer(t *testing.T, mode string) (string, *Server, func()) {
	t.Helper()
	s := New(store.New(4))
	if err := s.SetDatabases(defaultDatabases); err != nil {
		t.Fatalf("SetDatabases: %v", err)
	}
	if !s.SetEnableDebugCommand(mode) {
		t.Fatalf("SetEnableDebugCommand(%q) was refused", mode)
	}
	if err := s.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = s.Serve(ctx); close(done) }()
	return s.Addr().String(), s, func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("server did not shut down in time")
		}
	}
}

// TestDebugCommandGateDefaults is the one that matters: a Server nobody configured refuses
// DEBUG. If this fails, the gate ships open.
func TestDebugCommandGateDefaults(t *testing.T) {
	s := New(store.New(4))
	if got := s.EnableDebugCommand(); got != "no" {
		t.Errorf("a fresh Server reports enable-debug-command %q; want \"no\"", got)
	}
	if s.debugCommandAllowed(nil) {
		t.Error("a fresh Server allows DEBUG; the gate must default to closed")
	}
	if commandTable["DEBUG"] == nil || !commandTable["DEBUG"].protected {
		t.Error("DEBUG is not marked protected in the command table, so the gate never runs")
	}
	for _, mode := range []string{"no", "yes", "local", "NO", "Yes", "LOCAL"} {
		if !s.SetEnableDebugCommand(mode) {
			t.Errorf("SetEnableDebugCommand(%q) was refused", mode)
		}
	}
	for _, mode := range []string{"", "true", "1", "loopback", "off"} {
		if s.SetEnableDebugCommand(mode) {
			t.Errorf("SetEnableDebugCommand(%q) was accepted; only no|yes|local are Redis's values", mode)
		}
	}
	// A refused value must not have changed the setting, or a mistyped flag would silently
	// leave the gate wherever it happened to be.
	s.SetEnableDebugCommand("local")
	s.SetEnableDebugCommand("bogus")
	if got := s.EnableDebugCommand(); got != "local" {
		t.Errorf("after a refused value the setting is %q; want the previous \"local\"", got)
	}
}

// TestDebugCommandGateRefusal checks what a gated server answers, against the exact text
// redis:7.2 sends -- measured on amd64 and arm64, with enable-debug-command unset.
func TestDebugCommandGateRefusal(t *testing.T) {
	addr, _, stop := startGatedTestServer(t, "no")
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	// Every subcommand is refused, including HELP and including one that does not exist:
	// Redis takes the decision before it dispatches on the subcommand, so an unknown one
	// reports the gate rather than leaking which names the server knows.
	for _, cmd := range []string{
		"DEBUG HELP",
		"DEBUG JMAP",
		"DEBUG SLEEP 0",
		"DEBUG OBJECT nosuchkey",
		"DEBUG PROTOCOL string",
		"DEBUG SET-ACTIVE-EXPIRE 0",
		"DEBUG CHANGE-REPL-ID",
		"DEBUG QUICKLIST-PACKED-THRESHOLD 100",
		"DEBUG NO-SUCH-SUBCOMMAND",
		"debug sleep 0",
	} {
		if got := c.cmd(cmd); got != errDebugRefused {
			t.Errorf("%s = %q; want the gate's refusal", cmd, got)
		}
	}

	// The arity is checked first, so DEBUG with no subcommand answers the arity error
	// whether the gate is open or not. Measured against redis:7.2 both ways.
	if got := c.cmd("DEBUG"); got != "-ERR wrong number of arguments for 'debug' command" {
		t.Errorf("DEBUG with no subcommand = %q; want the arity error, as Redis answers", got)
	}

	// The gate must not have disabled anything else on the connection.
	if got := c.cmd("PING"); got != "+PONG" {
		t.Errorf("PING after a refused DEBUG = %q", got)
	}

	// Inside MULTI the command is refused at queue time and EXEC then aborts, which is what
	// Redis does: a transaction must not accumulate a command the connection may not run.
	c.cmd("MULTI")
	if got := c.cmd("DEBUG SLEEP 0"); got != errDebugRefused {
		t.Errorf("queued DEBUG = %q; want the refusal at queue time", got)
	}
	if got := c.cmd("EXEC"); !strings.HasPrefix(got, "-EXECABORT") {
		t.Errorf("EXEC after a refused DEBUG = %q; want EXECABORT", got)
	}
}

// TestDebugCommandGateAllowed covers the two open modes, and the fact that "local" is a
// judgement about the peer's address rather than about the host the server runs on.
func TestDebugCommandGateAllowed(t *testing.T) {
	addr, srv, stop := startGatedTestServer(t, "yes")
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	if got := c.cmd("DEBUG SLEEP 0"); got != "+OK" {
		t.Errorf("DEBUG SLEEP with the gate open = %q", got)
	}
	// The exact text redis:7.2 answers with the gate open, which for DEBUG is the longer of
	// Redis's two subcommand messages -- see the default branch of cmdDebug.
	want := "-ERR unknown subcommand or wrong number of arguments for " +
		"'NO-SUCH-SUBCOMMAND'. Try DEBUG HELP."
	if got := c.cmd("DEBUG NO-SUCH-SUBCOMMAND"); got != want {
		t.Errorf("an unknown subcommand with the gate open = %q; want %q", got, want)
	}

	// "local" admits this connection, which came from 127.0.0.1.
	srv.SetEnableDebugCommand("local")
	if got := c.cmd("DEBUG SLEEP 0"); got != "+OK" {
		t.Errorf("DEBUG from a loopback peer with the gate at \"local\" = %q", got)
	}
	if got := c.cmd("PING"); got != "+PONG" {
		t.Errorf("PING = %q", got)
	}
}

// TestDebugCommandGateLocalJudgesThePeer pins the address test itself, which is the part
// "local" stands on. It is checked directly rather than through a socket because binding a
// non-loopback address is not something a test can rely on being able to do.
func TestDebugCommandGateLocalJudgesThePeer(t *testing.T) {
	s := New(store.New(4))
	s.SetEnableDebugCommand("local")

	cases := []struct {
		addr string
		want bool
	}{
		{"127.0.0.1:34567", true},
		{"127.0.0.5:1", true},
		{"[::1]:34567", true},
		{"192.168.65.1:34567", false},
		{"10.0.0.4:6380", false},
		{"[2001:db8::1]:6380", false},
		{"not-an-address", false},
	}
	for _, c := range cases {
		sess := &session{conn: fakeConn{remote: c.addr}}
		if got := s.debugCommandAllowed(sess); got != c.want {
			t.Errorf("with the gate at \"local\", a peer at %s is allowed=%v; want %v",
				c.addr, got, c.want)
		}
	}
	// A session with no connection at all is a caller inside this process, not a client.
	if !s.debugCommandAllowed(&session{}) {
		t.Error("an in-process session was refused by \"local\"; it has no peer to judge")
	}
	// And "no" refuses even a loopback peer, or the mode would mean nothing.
	s.SetEnableDebugCommand("no")
	if s.debugCommandAllowed(&session{conn: fakeConn{remote: "127.0.0.1:1"}}) {
		t.Error("the gate at \"no\" allowed a loopback peer")
	}
}

// fakeConn is a net.Conn that only knows its peer address, which is all the gate reads.
type fakeConn struct {
	net.Conn
	remote string
}

func (c fakeConn) RemoteAddr() net.Addr { return fakeAddr(c.remote) }

type fakeAddr string

func (a fakeAddr) Network() string { return "tcp" }
func (a fakeAddr) String() string  { return string(a) }
