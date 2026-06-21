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

// readReply parses a single RESP reply into a flat string for assertions.
func readReply(t *testing.T, br *bufio.Reader) string {
	t.Helper()
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	line = strings.TrimRight(line, "\r\n")
	switch line[0] {
	case '+', '-', ':':
		return line
	case '$':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return "(nil)"
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(br, buf); err != nil {
			t.Fatalf("read bulk: %v", err)
		}
		return string(buf[:n])
	case '*':
		n, _ := strconv.Atoi(line[1:])
		parts := make([]string, 0, n)
		for i := 0; i < n; i++ {
			parts = append(parts, readReply(t, br))
		}
		return "[" + strings.Join(parts, " ") + "]"
	}
	return line
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
