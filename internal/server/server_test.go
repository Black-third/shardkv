package server

import (
	"bufio"
	"context"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Black-third/shardkv/internal/store"
)

// startTestServer binds to an ephemeral port, serves in the background, and
// returns the address plus a cleanup function.
func startTestServer(t *testing.T) (string, func()) {
	t.Helper()
	s := New(store.New(16))
	if err := s.SetDatabases(defaultDatabases); err != nil {
		t.Fatalf("SetDatabases: %v", err)
	}
	// DEBUG is refused by default (see commands_debug.go), and several tests here use it
	// the way Redis's own suite does -- DEBUG PROTOCOL for the RESP3 encodings, DEBUG SLEEP
	// for a command of a known duration, DEBUG SET-ACTIVE-EXPIRE to observe lazy expiry.
	// Redis's suite starts its servers with --enable-debug-command local for exactly this
	// reason; this is the same decision, made once, for every server a test starts. The
	// gate's own behaviour is covered by TestDebugCommandGate, which sets the modes itself.
	s.SetEnableDebugCommand("yes")
	if err := s.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Serve(ctx); close(done) }()
	addr := s.Addr().String()
	return addr, func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("server did not shut down in time")
		}
	}
}

// readReply parses a single RESP reply into a flat string for assertions. It
// understands both protocols: the RESP3 types render distinguishably (a map as
// {k v}, a set as ~[a b], a double as ,1.5, a push as >[...]) so a test can assert
// the *shape* of a reply and not only its contents.
func readReply(t *testing.T, br *bufio.Reader) string {
	t.Helper()
	got, err := parseReply(br)
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	return got
}

// parseReply is readReply without the testing.T, for the goroutines the blocking
// tests read replies on: t.Fatalf may only be called from the test's own goroutine.
func parseReply(br *bufio.Reader) (string, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return "", nil
	}
	switch line[0] {
	case '+', '-', ':', ',', '#', '(':
		return line, nil
	case '_':
		return "(nil)", nil // the single RESP3 null
	case '$', '=':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return "(nil)", nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(br, buf); err != nil {
			return "", err
		}
		if line[0] == '=' {
			return "=" + string(buf[:n]), nil
		}
		return string(buf[:n]), nil
	case '*', '~', '>':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return "(nil)", nil // null array (e.g. aborted EXEC)
		}
		parts := make([]string, 0, n)
		for i := 0; i < n; i++ {
			part, err := parseReply(br)
			if err != nil {
				return "", err
			}
			parts = append(parts, part)
		}
		prefix := ""
		if line[0] != '*' {
			prefix = string(line[0])
		}
		return prefix + "[" + strings.Join(parts, " ") + "]", nil
	case '%', '|':
		n, _ := strconv.Atoi(line[1:])
		parts := make([]string, 0, n*2)
		for i := 0; i < n*2; i++ {
			part, err := parseReply(br)
			if err != nil {
				return "", err
			}
			parts = append(parts, part)
		}
		out := "{" + strings.Join(parts, " ") + "}"
		if line[0] == '|' {
			// An attribute is metadata *about* the reply that follows, so the reply itself
			// still has to be read for the stream to stay in sync.
			rest, err := parseReply(br)
			if err != nil {
				return "", err
			}
			return "|" + out + rest, nil
		}
		return out, nil
	}
	return line, nil
}

func TestServerCommands(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)

	send := func(cmd string) string {
		if _, err := conn.Write([]byte(cmd + "\r\n")); err != nil {
			t.Fatalf("write: %v", err)
		}
		return readReply(t, br)
	}

	cases := []struct{ cmd, want string }{
		{"PING", "+PONG"},
		{"SET foo bar", "+OK"},
		{"GET foo", "bar"},
		{"GET missing", "(nil)"},
		{"EXISTS foo", ":1"},
		{"EXISTS foo missing", ":1"},
		{"DEL foo", ":1"},
		{"GET foo", "(nil)"},
		{"INCR n", ":1"},
		{"INCR n", ":2"},
		{"DECR n", ":1"},
		{"SET s hello", "+OK"},
		{"INCR s", "-ERR value is not an integer or out of range"},
		{"TTL n", ":-1"},
		{"TTL ghost", ":-2"},
		{"EXPIRE n 100", ":1"},
		{"FLUSHALL", "+OK"},
		{"DBSIZE", ":0"},
		{"BOGUS x", "-ERR unknown command 'BOGUS'"},
	}
	for _, c := range cases {
		if got := send(c.cmd); got != c.want {
			t.Errorf("%q -> %q; want %q", c.cmd, got, c.want)
		}
	}
}

// TestConcurrentClients verifies the server stays correct under many
// simultaneous connections incrementing a shared counter. Run with -race.
func TestConcurrentClients(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	const clients, perClient = 25, 200

	var wg sync.WaitGroup
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Errorf("dial: %v", err)
				return
			}
			defer conn.Close()
			br := bufio.NewReader(conn)
			for i := 0; i < perClient; i++ {
				conn.Write([]byte("INCR shared\r\n"))
				readReply(t, br)
			}
		}()
	}
	wg.Wait()

	conn, _ := net.Dial("tcp", addr)
	defer conn.Close()
	br := bufio.NewReader(conn)
	conn.Write([]byte("GET shared\r\n"))
	got := readReply(t, br)
	if want := strconv.Itoa(clients * perClient); got != want {
		t.Fatalf("shared counter = %s; want %s", got, want)
	}
}
