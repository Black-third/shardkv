package server

import (
	"bufio"
	"net"
	"testing"
)

// txConn is a small client helper for the transaction tests.
type txConn struct {
	t    *testing.T
	conn net.Conn
	br   *bufio.Reader
}

func dialTx(t *testing.T, addr string) *txConn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return &txConn{t: t, conn: conn, br: bufio.NewReader(conn)}
}

func (c *txConn) cmd(s string) string {
	c.t.Helper()
	c.conn.Write([]byte(s + "\r\n"))
	return readReply(c.t, c.br)
}

func (c *txConn) close() { c.conn.Close() }

func TestMoreStringAndKeyCommands(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		{"SETNX k v1", ":1"},
		{"SETNX k v2", ":0"}, // already exists
		{"GET k", "v1"},
		{"GETSET k v2", "v1"},
		{"GET k", "v2"},
		{"APPEND k !", ":3"},
		{"GET k", "v2!"},
		{"STRLEN k", ":3"},
		{"INCRBY c 5", ":5"},
		{"DECRBY c 2", ":3"},
		{"GETDEL k", "v2!"},
		{"EXISTS k", ":0"},
		// expiry family
		{"SET p hello", "+OK"},
		{"EXPIRE p 100", ":1"},
		{"PERSIST p", ":1"},
		{"PERSIST p", ":0"}, // nothing left to persist
		{"TTL p", ":-1"},
		{"PEXPIREAT p 99999999999999", ":1"},
		{"PERSIST p", ":1"},
		// rename
		{"SET a 1", "+OK"},
		{"RENAME a b", "+OK"},
		{"GET b", "1"},
		{"EXISTS a", ":0"},
		{"RENAME missing x", "-ERR no such key"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}
}

func TestTransactionExec(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	if got := c.cmd("MULTI"); got != "+OK" {
		t.Fatalf("MULTI = %q", got)
	}
	if got := c.cmd("SET k v1"); got != "+QUEUED" {
		t.Fatalf("queued SET = %q", got)
	}
	if got := c.cmd("INCR n"); got != "+QUEUED" {
		t.Fatalf("queued INCR = %q", got)
	}
	if got := c.cmd("INCR n"); got != "+QUEUED" {
		t.Fatalf("queued INCR = %q", got)
	}
	// EXEC returns an array of the queued replies: +OK, :1, :2
	if got := c.cmd("EXEC"); got != "[+OK :1 :2]" {
		t.Fatalf("EXEC = %q; want [+OK :1 :2]", got)
	}
	// State was actually applied.
	if got := c.cmd("GET k"); got != "v1" {
		t.Fatalf("GET k after EXEC = %q", got)
	}
	if got := c.cmd("GET n"); got != "2" {
		t.Fatalf("GET n after EXEC = %q", got)
	}
}

func TestTransactionDiscard(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("MULTI")
	c.cmd("SET k should-not-persist")
	if got := c.cmd("DISCARD"); got != "+OK" {
		t.Fatalf("DISCARD = %q", got)
	}
	if got := c.cmd("GET k"); got != "(nil)" {
		t.Fatalf("GET k after DISCARD = %q; want (nil)", got)
	}
	// EXEC without MULTI now errors.
	if got := c.cmd("EXEC"); got != "-ERR EXEC without MULTI" {
		t.Fatalf("EXEC without MULTI = %q", got)
	}
}

func TestTransactionExecAbortOnBadCommand(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("MULTI")
	c.cmd("SET k v")
	if got := c.cmd("NOTACOMMAND x"); got != "-ERR unknown command 'NOTACOMMAND'" {
		t.Fatalf("bad queued cmd = %q", got)
	}
	// A parse error in the queue makes EXEC abort entirely.
	if got := c.cmd("EXEC"); got != "-EXECABORT Transaction discarded because of previous errors." {
		t.Fatalf("EXEC = %q; want EXECABORT", got)
	}
	if got := c.cmd("GET k"); got != "(nil)" {
		t.Fatalf("GET k after EXECABORT = %q; want (nil)", got)
	}
}

func TestWatchAbortsOnConcurrentWrite(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()
	other := dialTx(t, addr)
	defer other.close()

	c.cmd("SET k 1")
	if got := c.cmd("WATCH k"); got != "+OK" {
		t.Fatalf("WATCH = %q", got)
	}
	c.cmd("MULTI")
	c.cmd("SET k from-tx")

	// A different client modifies the watched key before EXEC.
	if got := other.cmd("SET k from-other"); got != "+OK" {
		t.Fatalf("other SET = %q", got)
	}

	// EXEC must abort (null array), and the transaction's write must not apply.
	if got := c.cmd("EXEC"); got != "(nil)" {
		t.Fatalf("EXEC after watched write = %q; want (nil) null array", got)
	}
	if got := c.cmd("GET k"); got != "from-other" {
		t.Fatalf("GET k = %q; want from-other (tx aborted)", got)
	}
}

func TestWatchSucceedsWithoutConflict(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("SET k 1")
	c.cmd("WATCH k")
	c.cmd("MULTI")
	c.cmd("SET k 2")
	// No concurrent write to k -> EXEC commits.
	if got := c.cmd("EXEC"); got != "[+OK]" {
		t.Fatalf("EXEC = %q; want [+OK]", got)
	}
	if got := c.cmd("GET k"); got != "2" {
		t.Fatalf("GET k = %q; want 2", got)
	}
}
