package server

// Blocking-command tests.
//
// These run against a real listener with real concurrent TCP clients, because that is
// the only place the interesting properties exist: a blocked client is a parked
// goroutine holding a socket, and the questions worth asking -- does a push wake it,
// does the earliest waiter win, does anything leak when it times out or hangs up --
// have no meaning against an in-process handler call.

import (
	"bufio"
	"context"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Black-third/shardkv/internal/store"
)

// asyncConn is a client whose commands are issued without waiting for their replies,
// so the test can carry on while it is blocked.
//
// One reader goroutine drains everything the server sends -- replies and RESP3 push
// frames alike -- onto a channel. A reader per command would not do: a subscribed
// connection receives frames nobody asked for, and a test that blocks on a queue while
// messages arrive has to see both in the order they were written.
type asyncConn struct {
	conn net.Conn
	br   *bufio.Reader
	out  chan string
	dead chan struct{} // closed when the connection can produce nothing more
}

func dialAsync(t *testing.T, addr string) *asyncConn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	a := &asyncConn{
		conn: conn,
		br:   bufio.NewReader(conn),
		out:  make(chan string, 64),
		dead: make(chan struct{}),
	}
	go func() {
		defer close(a.dead)
		for {
			reply, err := parseReply(a.br)
			if err != nil {
				return
			}
			a.out <- reply
		}
	}()
	t.Cleanup(func() { conn.Close() })
	return a
}

// send issues a command without waiting for its reply.
func (a *asyncConn) send(cmd string) { a.conn.Write([]byte(cmd + "\r\n")) }

// sendRaw writes exactly these bytes, so a caller can put two commands on the wire in one
// write and have the server read them from a single buffer -- which is what a pipelining
// client does and what TestBlockingFairnessAgainstPipelining depends on.
func (a *asyncConn) sendRaw(raw string) { a.conn.Write([]byte(raw)) }

// reply waits for the next thing the server sends, failing the test if nothing arrives
// in time.
func (a *asyncConn) reply(t *testing.T, within time.Duration) string {
	t.Helper()
	select {
	case r := <-a.out:
		return r
	case <-a.dead:
		// The connection may have delivered a reply just before it closed.
		select {
		case r := <-a.out:
			return r
		default:
			t.Fatalf("connection closed before a reply arrived")
		}
	case <-time.After(within):
		t.Fatalf("no reply within %v", within)
	}
	return ""
}

// silentFor asserts that no reply arrives for the given time, i.e. that the client is
// genuinely still blocked.
func (a *asyncConn) silentFor(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case r := <-a.out:
		t.Fatalf("expected the client to still be blocked; got reply %q", r)
	case <-time.After(d):
	}
}

func (a *asyncConn) close() { a.conn.Close() }

// waitBlocked waits until the server reports exactly n blocked clients. Polling INFO
// rather than sleeping is what makes these tests deterministic: every assertion that
// depends on a client having reached the wait starts from here.
func waitBlocked(t *testing.T, admin *txConn, n int) {
	t.Helper()
	want := "blocked_clients:" + itoaTest(n)
	waitFor(t, "blocked_clients to reach "+itoaTest(n), func() bool {
		return contains(admin.cmd("INFO clients"), want)
	})
}

func itoaTest(n int) string {
	if n == 0 {
		return "0"
	}
	var out []byte
	for n > 0 {
		out = append([]byte{byte('0' + n%10)}, out...)
		n /= 10
	}
	return string(out)
}

// TestBlockingServedWithoutWaiting covers the fast path: data is already there, so the
// command must answer immediately and never touch the wait machinery.
func TestBlockingServedWithoutWaiting(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("RPUSH q a b c")
	c.cmd("ZADD z 1 m")
	c.cmd("RPUSH src x")

	cases := []struct{ cmd, want string }{
		{"BLPOP q 0", "[q a]"},
		{"BRPOP q 0", "[q c]"},
		{"BLPOP nokey q 0", "[q b]"},
		{"BZPOPMIN z 0", "[z m 1]"},
		{"BRPOPLPUSH src dst 0", "x"},
		{"LRANGE dst 0 -1", "[x]"},
		{"BLMOVE dst src RIGHT LEFT 0", "x"},
		{"RPUSH multi one two", ":2"},
		{"BLMPOP 0 1 multi LEFT COUNT 5", "[multi [one two]]"},
		{"ZADD zm 1 a 2 b", ":2"},
		{"BZMPOP 0 1 zm MIN COUNT 2", "[zm [[a 1] [b 2]]]"},
		// A blocked-command timeout of zero means "forever", but only when there is
		// nothing to serve; these all had something.
		{"BLPOP nokey1 nokey2 0.01", "(nil)"},
		{"BZPOPMIN nokey 0.01", "(nil)"},
		{"BLMOVE nokey dst LEFT LEFT 0.01", "(nil)"},
		{"BLMPOP 0.01 2 nokey1 nokey2 LEFT", "(nil)"},
		{"BZMPOP 0.01 1 nokey MIN", "(nil)"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}
}

// TestBlockingWokenByPush is the core wakeup test: a client blocked with no timeout
// must be served by a push from another connection, with no polling anywhere.
func TestBlockingWokenByPush(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	admin := dialTx(t, addr)
	defer admin.close()

	for _, tc := range []struct{ block, push, want string }{
		{"BLPOP q 0", "RPUSH q hello", "[q hello]"},
		{"BRPOP q 0", "LPUSH q world", "[q world]"},
		{"BZPOPMIN z 0", "ZADD z 1.5 m", "[z m 1.5]"},
		{"BZPOPMAX z 0", "ZADD z 2 n", "[z n 2]"},
		{"BLMPOP 0 1 q LEFT COUNT 3", "RPUSH q x y", "[q [x y]]"},
		{"BZMPOP 0 1 z MIN", "ZADD z 3 o", "[z [[o 3]]]"},
		{"BRPOPLPUSH src dst 0", "RPUSH src moved", "moved"},
		{"BLMOVE src2 dst2 LEFT RIGHT 0", "RPUSH src2 moved2", "moved2"},
		// A key created by a rename, not a push, must wake a waiter too: signalling is
		// driven by the same affectedKeys list WATCH uses, so anything that can make a
		// key appear is covered.
		{"BLPOP renamed 0", "RENAME ren renamed", "[renamed r1]"},
	} {
		a := dialAsync(t, addr)
		if strings.HasPrefix(tc.push, "RENAME") {
			admin.cmd("RPUSH ren r1")
		}
		a.send(tc.block)
		waitBlocked(t, admin, 1)
		admin.cmd(tc.push)
		if got := a.reply(t, 5*time.Second); got != tc.want {
			t.Errorf("%q after %q -> %q; want %q", tc.block, tc.push, got, tc.want)
		}
		waitBlocked(t, admin, 0)
		a.close()
	}
}

// TestBlockingTimeout checks that a fractional timeout elapses, replies with the
// nothing-to-serve reply, and leaves the registry empty.
func TestBlockingTimeout(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	admin := dialTx(t, addr)
	defer admin.close()

	a := dialAsync(t, addr)
	defer a.close()
	start := time.Now()
	a.send("BLPOP q 0.15")
	if got := a.reply(t, 5*time.Second); got != "(nil)" {
		t.Errorf("timed-out BLPOP = %q; want a null array", got)
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Errorf("BLPOP with a 0.15s timeout returned after %v; it did not wait", elapsed)
	}
	waitBlocked(t, admin, 0)

	// And the connection is still perfectly usable afterwards -- the disconnect
	// watchdog sets a read deadline to stop itself, and must clear it again.
	a.send("PING")
	if got := a.reply(t, 5*time.Second); got != "+PONG" {
		t.Errorf("PING after a timed-out block = %q", got)
	}
	a.send("SET k v")
	if got := a.reply(t, 5*time.Second); got != "+OK" {
		t.Errorf("SET after a timed-out block = %q", got)
	}
}

// TestBlockingFIFOFairness is the fairness test: five clients block on one key in a
// known order, then five pushes arrive one at a time. Each push must serve the
// earliest waiter, in the order they arrived.
//
// The order is known because each client is only sent after the server reports the
// previous one blocked, so registration order is the arrival order and not a race.
func TestBlockingFIFOFairness(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	admin := dialTx(t, addr)
	defer admin.close()

	const n = 5
	clients := make([]*asyncConn, n)
	for i := range clients {
		clients[i] = dialAsync(t, addr)
		clients[i].send("BLPOP fair 0")
		waitBlocked(t, admin, i+1)
	}
	if !contains(admin.cmd("INFO clients"), "total_blocking_keys:1") {
		t.Errorf("INFO should report one key with waiters:\n%s", admin.cmd("INFO clients"))
	}

	for i := 0; i < n; i++ {
		want := "[fair v" + itoaTest(i) + "]"
		admin.cmd("RPUSH fair v" + itoaTest(i))
		// The i-th push must go to the i-th client to have blocked, and to no other.
		if got := clients[i].reply(t, 5*time.Second); got != want {
			t.Fatalf("push %d was served to the wrong waiter: client %d got %q; want %q",
				i, i, got, want)
		}
		waitBlocked(t, admin, n-i-1)
	}
	for _, c := range clients {
		c.close()
	}
}

// TestBlockingOneBatchServesSeveralWaiters covers the hand-off: a single push carrying
// several elements has to serve several waiters, which only happens because a served
// waiter signals the new head of every queue it leaves.
func TestBlockingOneBatchServesSeveralWaiters(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	admin := dialTx(t, addr)
	defer admin.close()

	const n = 3
	clients := make([]*asyncConn, n)
	for i := range clients {
		clients[i] = dialAsync(t, addr)
		clients[i].send("BLPOP batch 0")
		waitBlocked(t, admin, i+1)
	}
	admin.cmd("RPUSH batch a b c")
	for i, c := range clients {
		want := "[batch " + string(rune('a'+i)) + "]"
		if got := c.reply(t, 5*time.Second); got != want {
			t.Errorf("client %d got %q; want %q", i, got, want)
		}
	}
	waitBlocked(t, admin, 0)
	for _, c := range clients {
		c.close()
	}
}

// TestBlockingNoGoroutineLeak covers both ways a wait ends without being served: the
// timeout and the client hanging up. Neither may leave a goroutine (the waiter's own,
// or the peek that watches its socket) or a registry entry behind.
func TestBlockingNoGoroutineLeak(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	admin := dialTx(t, addr)
	defer admin.close()

	settle := func() int {
		// Goroutines exit asynchronously, so the baseline is whatever the count settles
		// to rather than whatever it is at this instant.
		last, stable := -1, 0
		for i := 0; i < 200; i++ {
			n := runtime.NumGoroutine()
			if n == last {
				if stable++; stable >= 3 {
					return n
				}
			} else {
				last, stable = n, 0
			}
			time.Sleep(10 * time.Millisecond)
		}
		return runtime.NumGoroutine()
	}
	base := settle()

	// Ten clients time out.
	for i := 0; i < 10; i++ {
		a := dialAsync(t, addr)
		a.send("BLPOP leak 0.05")
		if got := a.reply(t, 5*time.Second); got != "(nil)" {
			t.Fatalf("BLPOP = %q", got)
		}
		a.close()
	}
	waitBlocked(t, admin, 0)

	// Ten clients block forever and then hang up. A blocked client is not reading its
	// own socket, so this is the case that needs the peek watchdog to notice at all.
	for i := 0; i < 10; i++ {
		a := dialAsync(t, addr)
		a.send("BLPOP leak 0")
		waitBlocked(t, admin, 1)
		a.close()
		waitBlocked(t, admin, 0)
	}

	after := settle()
	// A small allowance for the runtime's own goroutines; a leak of the kind being
	// tested for would be twenty, not two.
	if after > base+3 {
		t.Errorf("goroutines grew from %d to %d across 20 abandoned blocks", base, after)
	}
	if got := admin.cmd("INFO clients"); !contains(got, "total_blocking_keys:0") {
		t.Errorf("a wait queue was left behind:\n%s", got)
	}
}

// TestBlockingInsideMulti pins the transaction rule: inside MULTI/EXEC a blocking
// command must not block. It takes its non-blocking behaviour, because an EXEC that
// could wait would hold the batch -- and, when propagation is active, the server's
// write-ordering lock -- for the whole timeout.
func TestBlockingInsideMulti(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("MULTI")
	if got := c.cmd("BLPOP nothing 0"); got != "+QUEUED" {
		t.Fatalf("BLPOP inside MULTI = %q; want QUEUED", got)
	}
	if got := c.cmd("BZPOPMIN nothing 0"); got != "+QUEUED" {
		t.Fatalf("BZPOPMIN inside MULTI = %q; want QUEUED", got)
	}
	start := time.Now()
	if got := c.cmd("EXEC"); got != "[(nil) (nil)]" {
		t.Errorf("EXEC = %q; want two nulls", got)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("EXEC took %v: a blocking command blocked inside a transaction", elapsed)
	}

	// With data available it serves inside the transaction exactly as it would outside.
	c.cmd("RPUSH q ready")
	c.cmd("MULTI")
	c.cmd("BLPOP q 0")
	if got := c.cmd("EXEC"); got != "[[q ready]]" {
		t.Errorf("EXEC with data = %q", got)
	}
}

// TestBlockingPropagatesEffect is the divergence test. What reaches the AOF and the
// replicas must be the pop that happened, never the blocking command: a replica
// replaying BLPOP would wait forever on a connection that has no client behind it.
func TestBlockingPropagatesEffect(t *testing.T) {
	s, addr, stop := startServer(t, store.New(8))
	defer stop()
	next := tapReplica(t, s)

	admin := dialTx(t, addr)
	defer admin.close()

	// Served immediately.
	admin.cmd("RPUSH q a")
	if got := string(next()[0]); got != "RPUSH" {
		t.Fatalf("first propagated command = %q", got)
	}
	admin.cmd("BLPOP q 0")
	if got := propagatedText(next()); got != "LPOP q" {
		t.Errorf("BLPOP propagated %q; want %q", got, "LPOP q")
	}

	// Served after actually blocking, which is the path that matters: the pop happens
	// on the blocked client's goroutine, and it must still be ordered and shipped.
	a := dialAsync(t, addr)
	defer a.close()
	a.send("BLPOP q 0")
	waitBlocked(t, admin, 1)
	admin.cmd("RPUSH q b")
	if got := propagatedText(next()); got != "RPUSH q b" {
		t.Errorf("the push propagated %q", got)
	}
	if got := a.reply(t, 5*time.Second); got != "[q b]" {
		t.Fatalf("woken BLPOP = %q", got)
	}
	if got := propagatedText(next()); got != "LPOP q" {
		t.Errorf("a woken BLPOP propagated %q; want %q", got, "LPOP q")
	}

	// The other shapes, each propagating what it actually did.
	admin.cmd("ZADD z 1 m")
	next()
	admin.cmd("BZPOPMIN z 0")
	if got := propagatedText(next()); got != "ZREM z m" {
		t.Errorf("BZPOPMIN propagated %q; want %q", got, "ZREM z m")
	}
	admin.cmd("RPUSH src x")
	next()
	admin.cmd("BLMOVE src dst LEFT RIGHT 0")
	if got := propagatedText(next()); got != "LMOVE src dst LEFT RIGHT" {
		t.Errorf("BLMOVE propagated %q", got)
	}
	admin.cmd("RPUSH mq one two")
	next()
	admin.cmd("BLMPOP 0 1 mq LEFT COUNT 5")
	if got := propagatedText(next()); got != "LPOP mq 2" {
		t.Errorf("BLMPOP propagated %q; want %q", got, "LPOP mq 2")
	}
	admin.cmd("ZADD zm 1 a 2 b")
	next()
	admin.cmd("BZMPOP 0 1 zm MIN COUNT 2")
	if got := propagatedText(next()); got != "ZREM zm a b" {
		t.Errorf("BZMPOP propagated %q; want %q", got, "ZREM zm a b")
	}
}

func propagatedText(cmd [][]byte) string {
	parts := make([]string, 0, len(cmd))
	for _, a := range cmd {
		parts = append(parts, string(a))
	}
	return strings.Join(parts, " ")
}

// TestBlockingWakesWatchers checks that a pop performed by a *blocked* client
// invalidates a WATCH on the key it wrote.
//
// BLMOVE is used rather than BLPOP so the check is isolated: the WATCH is on the
// destination, which the push that unblocks the mover never names. Only the move
// itself touches it, so an aborted EXEC can only be the blocked client's work.
func TestBlockingWakesWatchers(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	admin := dialTx(t, addr)
	defer admin.close()

	watcher := dialTx(t, addr)
	defer watcher.close()
	if got := watcher.cmd("WATCH dst"); got != "+OK" {
		t.Fatalf("WATCH = %q", got)
	}

	mover := dialAsync(t, addr)
	defer mover.close()
	mover.send("BLMOVE src dst LEFT RIGHT 0")
	waitBlocked(t, admin, 1)

	admin.cmd("RPUSH src v") // touches src, never dst
	if got := mover.reply(t, 5*time.Second); got != "v" {
		t.Fatalf("BLMOVE = %q", got)
	}

	watcher.cmd("MULTI")
	watcher.cmd("GET dst")
	if got := watcher.cmd("EXEC"); got != "(nil)" {
		t.Errorf("EXEC = %q; want it aborted, because a blocked client wrote dst", got)
	}
}

// TestBlockingSurvivesFlushAllAndExpiry covers the two ways data can go away while a
// client waits. Neither may serve the client and neither may end its wait: FLUSHALL
// removes keys rather than creating them, and an expired list has nothing to pop.
func TestBlockingSurvivesFlushAllAndExpiry(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	admin := dialTx(t, addr)
	defer admin.close()

	a := dialAsync(t, addr)
	defer a.close()
	a.send("BLPOP q 0")
	waitBlocked(t, admin, 1)

	admin.cmd("FLUSHALL")
	a.silentFor(t, 200*time.Millisecond)
	waitBlocked(t, admin, 1)

	// A list that expires cannot serve anyone either: the element is gone, so the
	// waiter stays waiting rather than being woken with nothing to take.
	admin.cmd("RPUSH other v")
	admin.cmd("PEXPIRE other 1")
	a.silentFor(t, 200*time.Millisecond)

	// And it is still wakeable afterwards.
	admin.cmd("RPUSH q late")
	if got := a.reply(t, 5*time.Second); got != "[q late]" {
		t.Errorf("BLPOP after FLUSHALL and an expiry = %q", got)
	}
}

// TestBlockingIgnoresExpiredList checks the other half of expiry: a BLPOP issued after
// a list's deadline has passed must wait rather than serve the dead element.
func TestBlockingIgnoresExpiredList(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("RPUSH gone v")
	c.cmd("PEXPIRE gone 1")
	waitFor(t, "the list to expire", func() bool { return c.cmd("LLEN gone") == ":0" })
	if got := c.cmd("BLPOP gone 0.1"); got != "(nil)" {
		t.Errorf("BLPOP on an expired list = %q; want it to wait and time out", got)
	}
}

// TestBlockingUnblockedByRolePromotion covers a master that becomes a replica while
// clients are blocked. Their commands are writes, which a replica refuses, so waiting
// for one to succeed is waiting for something that can no longer happen.
func TestBlockingUnblockedByRolePromotion(t *testing.T) {
	s, addr, stop := startServer(t, store.New(8))
	defer stop()
	admin := dialTx(t, addr)
	defer admin.close()

	a := dialAsync(t, addr)
	defer a.close()
	a.send("BLPOP q 0")
	waitBlocked(t, admin, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.ReplicaOf(ctx, "127.0.0.1:1") // nothing listening; the role change is the point

	got := a.reply(t, 5*time.Second)
	if !strings.HasPrefix(got, "-UNBLOCKED") {
		t.Errorf("blocked client after promotion to replica = %q; want an -UNBLOCKED error", got)
	}
	waitBlocked(t, admin, 0)
	// And a new blocking command is refused outright rather than parked.
	if got := admin.cmd("BLPOP q 0"); !strings.HasPrefix(got, "-READONLY") {
		t.Errorf("BLPOP on a replica = %q; want READONLY", got)
	}
}

// TestClientUnblock covers CLIENT UNBLOCK in both modes.
func TestClientUnblock(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	admin := dialTx(t, addr)
	defer admin.close()

	a := dialAsync(t, addr)
	defer a.close()
	a.send("CLIENT ID")
	id := strings.TrimPrefix(a.reply(t, 5*time.Second), ":")

	a.send("BLPOP q 0")
	waitBlocked(t, admin, 1)
	if got := admin.cmd("CLIENT UNBLOCK " + id); got != ":1" {
		t.Fatalf("CLIENT UNBLOCK = %q", got)
	}
	if got := a.reply(t, 5*time.Second); got != "(nil)" {
		t.Errorf("unblocked with TIMEOUT = %q; want the timeout reply", got)
	}

	a.send("BLPOP q 0")
	waitBlocked(t, admin, 1)
	if got := admin.cmd("CLIENT UNBLOCK " + id + " ERROR"); got != ":1" {
		t.Fatalf("CLIENT UNBLOCK ERROR = %q", got)
	}
	if got := a.reply(t, 5*time.Second); !strings.HasPrefix(got, "-UNBLOCKED") {
		t.Errorf("unblocked with ERROR = %q; want an -UNBLOCKED error", got)
	}

	// A client that is not blocked reports 0, and a bad reason is a syntax error.
	if got := admin.cmd("CLIENT UNBLOCK " + id); got != ":0" {
		t.Errorf("CLIENT UNBLOCK of an unblocked client = %q; want 0", got)
	}
	if got := admin.cmd("CLIENT UNBLOCK " + id + " NONSENSE"); got != "-ERR CLIENT UNBLOCK reason should be TIMEOUT or ERROR" {
		t.Errorf("CLIENT UNBLOCK with a bad reason = %q", got)
	}
}

// TestBlockingArgumentErrors pins the error replies, which are Redis's verbatim
// because a client library matches on them. Every one of these must be reported
// immediately -- a command that parked and then reported a syntax error after its
// timeout would be useless.
func TestBlockingArgumentErrors(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("SET str v")
	c.cmd("RPUSH list a")
	for _, tc := range []struct{ cmd, want string }{
		{"BLPOP q -1", "-ERR timeout is negative"},
		{"BLPOP q abc", "-ERR timeout is not a float or out of range"},
		{"BLPOP q 99999999999999999999", "-ERR timeout is out of range"},
		{"BLPOP", "-ERR wrong number of arguments for 'blpop' command"},
		{"BLPOP q", "-ERR wrong number of arguments for 'blpop' command"},
		{"BRPOPLPUSH a b", "-ERR wrong number of arguments for 'brpoplpush' command"},
		{"BLPOP str 0", "-WRONGTYPE Operation against a key holding the wrong kind of value"},
		{"BZPOPMIN str 0", "-WRONGTYPE Operation against a key holding the wrong kind of value"},
		{"BLMOVE list d BOTH RIGHT 0", "-ERR syntax error"},
		{"BLMPOP 0 0 LEFT", "-ERR wrong number of arguments for 'blmpop' command"},
		{"BLMPOP 0 abc list LEFT", "-ERR numkeys should be greater than 0"},
		{"BLMPOP 0 2 list LEFT", "-ERR syntax error"},
		{"BLMPOP 0 1 list BOTH", "-ERR syntax error"},
		{"BLMPOP 0 1 list LEFT COUNT 0", "-ERR count should be greater than 0"},
		{"LMPOP 2 list LEFT", "-ERR syntax error"},
		{"LMPOP 1 list LEFT COUNT -1", "-ERR count should be greater than 0"},
		{"LMPOP 1 str LEFT", "-WRONGTYPE Operation against a key holding the wrong kind of value"},
		{"ZMPOP 1 list MIN", "-WRONGTYPE Operation against a key holding the wrong kind of value"},
		{"ZMPOP 2 list MIN", "-ERR syntax error"},
		// A wrong-type key is only reached if the scan gets that far, as in Redis.
		{"BLPOP list str 0", "[list a]"},
	} {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}
}

// TestBlockingRESP3Nulls checks that the nothing-to-serve reply follows the
// connection's protocol: the null array in RESP2, the single null in RESP3.
func TestBlockingRESP3Nulls(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialRaw(t, addr)

	c.expect("BLPOP q 0.01", "*-1\r\n")
	c.expect("BLMOVE a b LEFT LEFT 0.01", "*-1\r\n")
	c.expect("HELLO 3", "%7\r\n")
	// Drain the rest of the HELLO map, then check the RESP3 encodings.
	drainRaw(t, c)
	c.expect("BLPOP q 0.01", "_\r\n")
	c.expect("BLMOVE a b LEFT LEFT 0.01", "_\r\n")
	c.expect("ZADD z 1.5 m", ":1\r\n")
	c.expect("BZPOPMIN z 0", "*3\r\n$1\r\nz\r\n$1\r\nm\r\n,1.5\r\n")
}

// drainRaw reads and discards the remainder of a partially consumed map reply.
func drainRaw(t *testing.T, c *rawConn) {
	t.Helper()
	for i := 0; i < 14; i++ {
		if _, err := parseReply(c.br); err != nil {
			t.Fatalf("drain: %v", err)
		}
	}
}

// TestBlockingWhileSubscribedRESP3 covers the interaction with the Pub/Sub pump. A
// RESP3 client may be subscribed and still issue commands, so it can be blocked in
// BLPOP while messages are arriving -- which only works because the wait releases the
// connection's writer lock instead of holding it for the whole timeout.
func TestBlockingWhileSubscribedRESP3(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	admin := dialTx(t, addr)
	defer admin.close()

	a := dialAsync(t, addr)
	defer a.close()
	a.send("HELLO 3")
	a.reply(t, 5*time.Second)
	a.send("SUBSCRIBE news")
	if got := a.reply(t, 5*time.Second); got != ">[subscribe news :1]" {
		t.Fatalf("SUBSCRIBE = %q", got)
	}

	a.send("BLPOP q 0")
	waitBlocked(t, admin, 1)

	// The message must arrive while the client is still blocked. If the wait held the
	// writer lock, this would not appear until the BLPOP finished.
	admin.cmd("PUBLISH news while-blocked")
	if got := a.reply(t, 5*time.Second); got != ">[message news while-blocked]" {
		t.Errorf("message delivered to a blocked subscriber = %q", got)
	}
	admin.cmd("RPUSH q served")
	if got := a.reply(t, 5*time.Second); got != "[q served]" {
		t.Errorf("BLPOP on a subscribed connection = %q", got)
	}
}

// TestBlockingConcurrentProducersAndConsumers is the stress case: many producers and
// many blocked consumers on one queue. Every pushed element must be delivered exactly
// once, and nothing may deadlock. Run under -race, this is what says the wait
// machinery holds no lock it should not.
func TestBlockingConcurrentProducersAndConsumers(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	admin := dialTx(t, addr)
	defer admin.close()

	const consumers, items = 8, 40
	got := make(chan string, consumers*items)
	done := make(chan struct{})

	// The consumers are waited for before the test returns: a goroutine still reading
	// replies after the test function has finished cannot report a failure, and would
	// take the whole run down trying.
	var wg sync.WaitGroup
	for i := 0; i < consumers; i++ {
		wg.Add(1)
		conn := dialAsync(t, addr)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				conn.send("BLPOP work 0.2")
				select {
				case reply := <-conn.out:
					if reply != "(nil)" {
						got <- reply
					}
				case <-conn.dead:
					return
				case <-time.After(5 * time.Second):
					return
				}
			}
		}()
	}
	defer wg.Wait()

	var producers []*txConn
	for i := 0; i < 4; i++ {
		p := dialTx(t, addr)
		defer p.close()
		producers = append(producers, p)
	}
	for i := 0; i < items; i++ {
		producers[i%len(producers)].cmd("RPUSH work item" + itoaTest(i))
	}

	seen := make(map[string]int)
	failed := 0
	for i := 0; i < items; i++ {
		select {
		case reply := <-got:
			seen[reply]++
		case <-time.After(10 * time.Second):
			failed = items - i
		}
		if failed > 0 {
			break
		}
	}
	close(done)
	if failed > 0 {
		t.Fatalf("%d of %d items were never delivered", failed, items)
	}
	for i := 0; i < items; i++ {
		want := "[work item" + itoaTest(i) + "]"
		if seen[want] != 1 {
			t.Errorf("%q was delivered %d times; want exactly once", want, seen[want])
		}
	}
}

// TestBlockingFairnessAgainstPipelining is the fairness case only a pipelining client can
// create: one connection sends a push and a blocking pop of the same key in a single write.
//
// The already-blocked client must be served, not the pipelining one. Both commands arrive in
// one read on one connection, so the push's wakeup is a channel send whose recipient has not
// necessarily been scheduled by the time the second command runs its opportunistic attempt --
// and before queueAhead existed, that attempt took the element from under the client that had
// been waiting. Redis cannot have the bug (it serves blocked clients between commands) and
// its own suite pins the behaviour, so this is where the behaviour is pinned here.
func TestBlockingFairnessAgainstPipelining(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	admin := dialTx(t, addr)
	defer admin.close()

	waiting := dialAsync(t, addr)
	defer waiting.close()
	waiting.send("BLPOP pipe 0")
	waitBlocked(t, admin, 1)

	// One write carrying both commands, so the server reads them from a single buffer.
	pipelined := dialAsync(t, addr)
	defer pipelined.close()
	pipelined.sendRaw("LPUSH pipe 1\r\nBLPOP pipe 0\r\n")

	if got := waiting.reply(t, 5*time.Second); got != "[pipe 1]" {
		t.Fatalf("the client that was already blocked got %q; want [pipe 1] -- the pipelining "+
			"client took the element it pushed", got)
	}
	if got := pipelined.reply(t, 5*time.Second); got != ":1" {
		t.Fatalf("the pipelining client's LPUSH replied %q; want :1", got)
	}
	// And it is now the one waiting, so the next push serves it.
	waitBlocked(t, admin, 1)
	admin.cmd("LPUSH pipe 2")
	if got := pipelined.reply(t, 5*time.Second); got != "[pipe 2]" {
		t.Errorf("the pipelining client's BLPOP got %q; want [pipe 2]", got)
	}
}

// TestBlockingDeferenceIsTypeAware checks the other half of queueAhead: an arriving command
// only queues behind a waiter that could be served *instead of it*.
//
// A BZPOPMIN parked on a key cannot be served by a list, and the wakeup filter means it will
// never consume one and never leave its queue. An arriving BLPOP that deferred to it would
// therefore wait behind a waiter that can never be served, with the element it wanted sitting
// in plain sight.
func TestBlockingDeferenceIsTypeAware(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	admin := dialTx(t, addr)
	defer admin.close()

	zwaiter := dialAsync(t, addr)
	defer zwaiter.close()
	zwaiter.send("BZPOPMIN mixed 0")
	waitBlocked(t, admin, 1)

	// A list appears under the same key. The sorted-set waiter cannot take it.
	admin.cmd("RPUSH mixed element")
	lpop := dialAsync(t, addr)
	defer lpop.close()
	lpop.send("BLPOP mixed 0")
	if got := lpop.reply(t, 5*time.Second); got != "[mixed element]" {
		t.Errorf("BLPOP arriving behind a BZPOPMIN waiter got %q; want [mixed element] -- it "+
			"must not queue behind a waiter that cannot be served by a list", got)
	}
}
