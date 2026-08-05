package server

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Black-third/shardkv/internal/aof"
	"github.com/Black-third/shardkv/internal/store"
)

// TestHelloHandshake covers what a client library does first. The protocol version is
// the load-bearing part: 2 and 3 are both spoken, and anything else must be refused
// with NOPROTO rather than accepted and then answered in an encoding the client did
// not ask for. The RESP3 side of the handshake is pinned in resp3_test.go.
func TestHelloHandshake(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	reply := c.cmd("HELLO")
	for _, want := range []string{"server shardkv", "proto 2", "mode standalone", "role master"} {
		if !contains(reply, want) {
			t.Errorf("HELLO reply is missing %q; got %q", want, reply)
		}
	}
	if got := c.cmd("HELLO 2"); !contains(got, "proto 2") {
		t.Errorf("HELLO 2 = %q", got)
	}
	for _, bad := range []string{"HELLO 1", "HELLO 4", "HELLO abc"} {
		if got := c.cmd(bad); got != "-NOPROTO unsupported protocol version" {
			t.Errorf("%q = %q; want NOPROTO", bad, got)
		}
	}
	// A refused version must leave the connection on the one it was already speaking,
	// so the next reply is still RESP2.
	if got := c.cmd("PING"); got != "+PONG" {
		t.Errorf("PING after a refused HELLO = %q", got)
	}
	// HELLO reports the connection's own id, which must match CLIENT ID.
	id := c.cmd("CLIENT ID")
	if !contains(c.cmd("HELLO"), "id "+strings.TrimPrefix(id, ":")) {
		t.Errorf("HELLO reports a different id than CLIENT ID (%s)", id)
	}
	// SETNAME during the handshake, which is how libraries label their connections.
	c.cmd("HELLO 2 SETNAME worker-1")
	if got := c.cmd("CLIENT GETNAME"); got != "worker-1" {
		t.Errorf("CLIENT GETNAME after HELLO SETNAME = %q", got)
	}
}

// TestConfigGetSet covers the glob matching clients rely on, the settings that are
// writable, and the refusal to accept ones that are not.
func TestConfigGetSet(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	// An exact name is just a glob matching one parameter.
	if got := c.cmd("CONFIG GET maxkeys"); got != "[maxkeys 0]" {
		t.Errorf("CONFIG GET maxkeys = %q", got)
	}
	// A glob returns the matching subset, which is the form a client warming up sends.
	got := c.cmd("CONFIG GET auto-aof-rewrite-*")
	for _, want := range []string{"auto-aof-rewrite-percentage", "auto-aof-rewrite-min-size"} {
		if !contains(got, want) {
			t.Errorf("CONFIG GET auto-aof-rewrite-* is missing %q; got %q", want, got)
		}
	}
	// Several patterns at once, and a parameter matched twice reported once.
	got = c.cmd("CONFIG GET requirepass requirepass masterauth")
	if n := strings.Count(got, "requirepass"); n != 1 {
		t.Errorf("a parameter matched by two patterns was reported %d times: %q", n, got)
	}
	// A pattern matching nothing is an empty array, not an error.
	if got := c.cmd("CONFIG GET nonexistent-*"); got != "[]" {
		t.Errorf("CONFIG GET of an unknown pattern = %q; want an empty array", got)
	}

	cases := []struct{ cmd, want string }{
		{"CONFIG SET maxkeys 1000", "+OK"},
		{"CONFIG GET maxkeys", "[maxkeys 1000]"},
		{"CONFIG SET auto-aof-rewrite-percentage 50", "+OK"},
		{"CONFIG GET auto-aof-rewrite-percentage", "[auto-aof-rewrite-percentage 50]"},
		{"CONFIG SET auto-aof-rewrite-min-size 4096", "+OK"},
		{"CONFIG GET auto-aof-rewrite-min-size", "[auto-aof-rewrite-min-size 4096]"},
		{"CONFIG SET notify-keyspace-events KEA", "+OK"},
		{"CONFIG GET notify-keyspace-events", "[notify-keyspace-events AKE]"},
		{"CONFIG SET repl-backlog-size 65536", "+OK"},
		{"CONFIG GET repl-backlog-size", "[repl-backlog-size 65536]"},
		// Rejected values leave the setting alone.
		{"CONFIG SET maxkeys not-a-number", "-ERR CONFIG SET failed - argument couldn't be parsed into an integer, or is out of range, for 'maxkeys'"},
		{"CONFIG GET maxkeys", "[maxkeys 1000]"},
		{"CONFIG SET notify-keyspace-events bogus", "-ERR CONFIG SET failed - argument couldn't be parsed into an integer, or is out of range, for 'notify-keyspace-events'"},
		{"CONFIG GET notify-keyspace-events", "[notify-keyspace-events AKE]"},
		// A setting fixed at startup is refused rather than silently ignored.
		{"CONFIG SET appendonly yes", "-ERR CONFIG SET failed - can't set immutable config 'appendonly'"},
		{"CONFIG SET port 1234", "-ERR CONFIG SET failed - can't set immutable config 'port'"},
		// And an unknown one is named in the error.
		{"CONFIG SET made-up 1", "-ERR Unknown option or number of arguments for CONFIG SET - 'made-up'"},
		{"CONFIG BOGUS", "-ERR Unknown CONFIG subcommand or wrong number of arguments for 'BOGUS'"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	// requirepass round-trips through CONFIG SET, and takes effect for new connections.
	c.cmd("CONFIG SET requirepass hunter2")
	if got := c.cmd("CONFIG GET requirepass"); got != "[requirepass hunter2]" {
		t.Errorf("CONFIG GET requirepass = %q", got)
	}
	fresh := dialTx(t, addr)
	defer fresh.close()
	if got := fresh.cmd("GET k"); got != "-"+errNoAuth {
		t.Errorf("a new connection after CONFIG SET requirepass = %q; want NOAUTH", got)
	}
	// The connection that set it stays authenticated: an administrative change must not
	// disconnect the administrator.
	if got := c.cmd("GET k"); got != "(nil)" {
		t.Errorf("the connection that set requirepass was locked out: %q", got)
	}
}

// TestConfigResetStat covers the counters CONFIG RESETSTAT clears, and the ones it must
// not: a lifetime total that describes the dataset's history is not a statistic of the
// measurement window.
func TestConfigResetStat(t *testing.T) {
	s, addr, stop := startServer(t, store.New(8))
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	for i := 0; i < 5; i++ {
		c.cmd("SET k v")
	}
	if s.totalCmds.Load() == 0 {
		t.Fatal("no commands were counted; the test cannot observe the reset")
	}
	if got := c.cmd("CONFIG RESETSTAT"); got != "+OK" {
		t.Fatalf("CONFIG RESETSTAT = %q", got)
	}
	// RESETSTAT is itself counted, so the counter is small rather than exactly zero.
	if got := s.totalCmds.Load(); got > 2 {
		t.Errorf("total_commands_processed = %d after RESETSTAT; want it reset", got)
	}
	if got := s.fullSyncs.Load(); got != 0 {
		t.Errorf("sync_full = %d after RESETSTAT", got)
	}
	// The replication offset is a position in a stream, not a statistic: resetting it
	// would leave every connected replica continuing from the wrong place.
	c.cmd("SET after reset")
	s.mu.Lock()
	offset := s.replOffset
	s.mu.Unlock()
	c.cmd("CONFIG RESETSTAT")
	s.mu.Lock()
	after := s.replOffset
	s.mu.Unlock()
	if after < offset {
		t.Errorf("CONFIG RESETSTAT moved the replication offset from %d back to %d", offset, after)
	}
}

// TestClientIntrospection covers CLIENT ID/SETNAME/GETNAME/INFO/LIST, including that
// one connection can see another -- which is the point of LIST.
func TestClientIntrospection(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	a := dialTx(t, addr)
	defer a.close()
	b := dialTx(t, addr)
	defer b.close()

	idA, err := strconv.ParseInt(strings.TrimPrefix(a.cmd("CLIENT ID"), ":"), 10, 64)
	if err != nil {
		t.Fatalf("CLIENT ID: %v", err)
	}
	idB, err := strconv.ParseInt(strings.TrimPrefix(b.cmd("CLIENT ID"), ":"), 10, 64)
	if err != nil {
		t.Fatalf("CLIENT ID: %v", err)
	}
	if idA == idB {
		t.Errorf("two connections were given the same id %d", idA)
	}

	if got := a.cmd("CLIENT GETNAME"); got != "(nil)" {
		t.Errorf("an unnamed connection reports %q; want a null bulk string", got)
	}
	if got := a.cmd("CLIENT SETNAME reader"); got != "+OK" {
		t.Errorf("CLIENT SETNAME = %q", got)
	}
	if got := a.cmd("CLIENT GETNAME"); got != "reader" {
		t.Errorf("CLIENT GETNAME = %q", got)
	}
	// A name containing a space would make CLIENT LIST's one-line-per-connection format
	// unparseable, so it is refused. It takes a RESP array to even express such a name.
	if got := a.cmdRaw("CLIENT", "SETNAME", "two words"); !contains(got, "cannot contain spaces") {
		t.Errorf("CLIENT SETNAME with a space = %q", got)
	}
	if got := a.cmd("CLIENT GETNAME"); got != "reader" {
		t.Errorf("a rejected CLIENT SETNAME changed the name to %q", got)
	}

	info := a.cmd("CLIENT INFO")
	for _, want := range []string{"id=" + strconv.FormatInt(idA, 10), "name=reader", "db=0", "sub=0", "cmd=client"} {
		if !contains(info, want) {
			t.Errorf("CLIENT INFO is missing %q; got %q", want, info)
		}
	}
	list := a.cmd("CLIENT LIST")
	for _, want := range []string{"id=" + strconv.FormatInt(idA, 10), "id=" + strconv.FormatInt(idB, 10)} {
		if !contains(list, want) {
			t.Errorf("CLIENT LIST is missing %q; got %q", want, list)
		}
	}
	// A subscribed connection is flagged, so an operator can see why it is idle.
	subscribeCmd(t, b, "SUBSCRIBE ch", 1)
	waitFor(t, "the subscription to show up in CLIENT LIST", func() bool {
		return contains(a.cmd("CLIENT LIST"), "flags=P")
	})
}

// TestClientListFlagsAReplicaFeed covers the S flag. A connection that PSYNC turned
// into a replication feed used to be indistinguishable from ordinary client traffic in
// CLIENT LIST, which is how operators and monitoring identify replica links -- and the
// only way to pick the feed out in order to kill it.
func TestClientListFlagsAReplicaFeed(t *testing.T) {
	_, masterAddr, stopM := startServer(t, store.New(8))
	defer stopM()
	replica, replicaAddr, stopR := startServer(t, store.New(8))
	defer stopR()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	replica.ReplicaOf(ctx, masterAddr)

	mc := dialTx(t, masterAddr)
	defer mc.close()
	rc := dialTx(t, replicaAddr)
	defer rc.close()

	mc.cmd("SET k 1")
	waitFor(t, "the replica to attach", func() bool { return rc.cmd("GET k") == "1" })

	waitFor(t, "the replica feed to be flagged S in CLIENT LIST", func() bool {
		for _, line := range strings.Split(mc.cmd("CLIENT LIST"), "\n") {
			if contains(line, "flags=S") {
				return true
			}
		}
		return false
	})

	// The plain client asking the question is not a feed, so it must still be N --
	// flagging every connection would be as useless as flagging none.
	var ownLine string
	for _, line := range strings.Split(mc.cmd("CLIENT INFO"), "\n") {
		if contains(line, "id=") {
			ownLine = line
		}
	}
	if !contains(ownLine, "flags=N") {
		t.Errorf("the querying client reports %q; want flags=N", ownLine)
	}
}

// TestClientKill covers both forms: the positional one that names an address and
// replies +OK, and the filter form that reports a count. A killed connection has to
// actually be closed -- its goroutine is blocked in a read, and only closing the socket
// unblocks it.
func TestClientKill(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	killer := dialTx(t, addr)
	defer killer.close()
	victim := dialTx(t, addr)
	defer victim.close()

	victimID := strings.TrimPrefix(victim.cmd("CLIENT ID"), ":")
	if got := killer.cmd("CLIENT KILL ID " + victimID); got != ":1" {
		t.Fatalf("CLIENT KILL ID = %q; want :1", got)
	}
	// The victim's connection is gone: the next read fails rather than replying.
	victim.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	victim.conn.Write([]byte("PING\r\n"))
	if _, err := victim.br.ReadString('\n'); err == nil {
		t.Error("a killed connection still answered")
	}

	// The old positional form, and its error for an address nobody is using.
	other := dialTx(t, addr)
	defer other.close()
	otherAddr := clientAddrOf(t, killer, strings.TrimPrefix(other.cmd("CLIENT ID"), ":"))
	if got := killer.cmd("CLIENT KILL " + otherAddr); got != "+OK" {
		t.Errorf("CLIENT KILL <addr> = %q; want +OK", got)
	}
	if got := killer.cmd("CLIENT KILL 203.0.113.1:1"); !contains(got, "No such client") {
		t.Errorf("CLIENT KILL of an unknown address = %q", got)
	}
	// SKIPME defaults to yes, so a filter that matches everything spares the caller.
	killerAddr := clientAddrOf(t, killer, strings.TrimPrefix(killer.cmd("CLIENT ID"), ":"))
	if got := killer.cmd("CLIENT KILL ADDR " + killerAddr); got != ":0" {
		t.Errorf("CLIENT KILL ADDR <self> = %q; want :0 with SKIPME defaulting to yes", got)
	}
	if got := killer.cmd("PING"); got != "+PONG" {
		t.Errorf("the caller was killed by its own SKIPME=yes filter: %q", got)
	}
}

// clientAddrOf finds the addr= field CLIENT LIST reports for a given client id.
func clientAddrOf(t *testing.T, c *txConn, id string) string {
	t.Helper()
	for _, line := range strings.Split(c.cmd("CLIENT LIST"), "\n") {
		if !strings.HasPrefix(line, "id="+id+" ") {
			continue
		}
		for _, field := range strings.Fields(line) {
			if strings.HasPrefix(field, "addr=") {
				return strings.TrimPrefix(field, "addr=")
			}
		}
	}
	t.Fatalf("no CLIENT LIST entry for id %s", id)
	return ""
}

// TestCommandIntrospection covers COMMAND COUNT/INFO/DOCS, which client libraries use
// to decide what they may send. The transaction commands are in the table for exactly
// this reason: a client that cannot find MULTI concludes the server has no
// transactions.
func TestCommandIntrospection(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	count := c.cmd("COMMAND COUNT")
	n, err := strconv.Atoi(strings.TrimPrefix(count, ":"))
	if err != nil {
		t.Fatalf("COMMAND COUNT = %q", count)
	}
	if n != len(commandTable) {
		t.Errorf("COMMAND COUNT = %d; want %d", n, len(commandTable))
	}
	if n < 100 {
		t.Errorf("COMMAND COUNT = %d; the table looks incomplete", n)
	}

	cases := []struct{ cmd, want string }{
		// name, arity, flags, first key, last key, step.
		{"COMMAND INFO get", "[[get :2 [+readonly] :1 :1 :1]]"},
		{"COMMAND INFO set", "[[set :-3 [+write +denyoom] :1 :1 :1]]"},
		{"COMMAND INFO ping", "[[ping :-1 [+readonly] :0 :0 :0]]"},
		{"COMMAND INFO mset", "[[mset :-3 [+write +denyoom] :1 :-1 :2]]"},
		{"COMMAND INFO del", "[[del :-2 [+write +denyoom] :1 :-1 :1]]"},
		{"COMMAND INFO multi", "[[multi :1 [+fast +loading +no-auth-none] :0 :0 :0]]"},
		// An unknown command is a null entry, so a client can line the reply up with
		// what it asked about.
		{"COMMAND INFO get nosuchcommand", "[[get :2 [+readonly] :1 :1 :1] (nil)]"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	// Every command reachable through the table has an entry, transactions included.
	for _, name := range []string{"MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH", "SUBSCRIBE", "AUTH", "WAIT"} {
		if got := c.cmd("COMMAND INFO " + name); contains(got, "(nil)") {
			t.Errorf("COMMAND INFO %s = %q; the command table is missing it", name, got)
		}
	}

	// DOCS is what redis-cli asks for on connect: a name -> attributes map.
	docs := c.cmd("COMMAND DOCS get")
	for _, want := range []string{"get", "summary", "arity", "since"} {
		if !contains(docs, want) {
			t.Errorf("COMMAND DOCS get is missing %q; got %q", want, docs)
		}
	}
	if got := c.cmd("COMMAND DOCS nosuchcommand"); got != "[]" {
		t.Errorf("COMMAND DOCS of an unknown command = %q; want an empty map", got)
	}
	if got := c.cmd("COMMAND BOGUS"); !contains(got, "Unknown subcommand") {
		t.Errorf("COMMAND BOGUS = %q", got)
	}
}

// TestSelectAndDBSize covers the range check and DBSIZE's per-database answer. An index
// outside the configured range is refused rather than accepted and ignored, which would
// let a client believe it had left database 0 and then read the very keys it thought it
// had left behind. The isolation itself is covered in databases_test.go.
func TestSelectAndDBSize(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		{"SELECT 0", "+OK"},
		{"SELECT 15", "+OK"}, // the default range is 0..15, as in Redis
		{"SELECT 16", "-ERR DB index is out of range"},
		{"SELECT -1", "-ERR DB index is out of range"},
		{"SELECT abc", "-ERR value is not an integer or out of range"},
		{"SELECT 0", "+OK"},
		{"DBSIZE", ":0"},
		{"MSET a 1 b 2", "+OK"},
		{"DBSIZE", ":2"},
		{"SELECT 0", "+OK"},
		{"DBSIZE", ":2"}, // the same database, so the same keys
		{"CONFIG GET databases", "[databases 16]"},
		{"CONFIG SET databases 32", "-ERR CONFIG SET failed - can't set immutable config 'databases'"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}
}

// TestInfoSectionFiltering covers the filtering the README already advertised: INFO
// replication used to return every section, including an O(keyspace) key count that a
// monitoring agent polling for offsets had no use for.
func TestInfoSectionFiltering(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	full := c.cmd("INFO")
	for _, want := range []string{
		"# Server", "# Clients", "# Persistence", "# Stats", "# Replication", "# Keyspace",
		"shardkv_version", "role:master", "connected_slaves:0", "master_repl_offset:0",
		"sync_full:0", "sync_partial_ok:0", "sync_partial_err:0", "replica_drops:0",
		"aof_enabled:0", "aof_rewrite_in_progress:0", "aof_base_size:0",
		"aof_current_size:0", "aof_last_bgrewrite_status:ok", "db_keys:0",
		"pubsub_channels:0", "pubsub_patterns:0",
	} {
		if !contains(full, want) {
			t.Errorf("INFO is missing %q; got:\n%s", want, full)
		}
	}

	repl := c.cmd("INFO replication")
	if !contains(repl, "role:master") || !contains(repl, "master_repl_offset") {
		t.Errorf("INFO replication is missing its own fields; got:\n%s", repl)
	}
	for _, unwanted := range []string{"# Server", "# Keyspace", "db_keys", "shardkv_version"} {
		if contains(repl, unwanted) {
			t.Errorf("INFO replication returned %q, which belongs to another section:\n%s", unwanted, repl)
		}
	}
	// Two sections at once, and the aliases that mean everything.
	both := c.cmd("INFO server keyspace")
	if !contains(both, "shardkv_version") || !contains(both, "db_keys") || contains(both, "# Replication") {
		t.Errorf("INFO server keyspace = \n%s", both)
	}
	if got := c.cmd("INFO all"); !contains(got, "# Server") || !contains(got, "# Replication") {
		t.Error("INFO all did not return every section")
	}
	// An unknown section is an empty report, not an error: a client asking for a section
	// this server does not have keeps working.
	if got := c.cmd("INFO cpu"); got != "" {
		t.Errorf("INFO cpu = %q; want an empty report", got)
	}
	// The cluster section exists on every server, cluster mode or not, because
	// cluster_enabled is exactly what a client polls to find out which it is talking to.
	if got := c.cmd("INFO cluster"); !contains(got, "cluster_enabled:0") {
		t.Errorf("INFO cluster = %q; want cluster_enabled:0 on a standalone server", got)
	}
}

// TestLastSave covers LASTSAVE, whose meaning here is the last time a complete copy of
// the dataset reached the disk -- an AOF rewrite, since there is no RDB.
func TestLastSave(t *testing.T) {
	s, _, logf := newAOFServer(t, aof.SyncNo)
	c := &directClient{t: t, s: s}

	before := s.lastSave.Load()
	if before == 0 {
		t.Fatal("LASTSAVE has no starting value")
	}
	if got := c.cmd("LASTSAVE"); got != ":"+strconv.FormatInt(before, 10) {
		t.Errorf("LASTSAVE = %q; want the startup time %d", got, before)
	}

	// A rewrite is a full write-out of the dataset, so it moves LASTSAVE.
	s.lastSave.Store(before - 100)
	c.cmd("SET k v")
	if err := s.RewriteAOF(); err != nil {
		t.Fatal(err)
	}
	if got := s.lastSave.Load(); got <= before-100 {
		t.Errorf("LASTSAVE = %d after a rewrite; want it moved forward", got)
	}
	_ = logf
}

// TestShutdown covers the command that stops the server. It replies nothing on success
// -- the connection simply closes, which is Redis's behaviour and the only honest answer
// to "did it shut down?" -- and it flushes the log first unless told not to.
func TestShutdown(t *testing.T) {
	s := New(store.New(8))
	if err := s.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	served := make(chan struct{})
	go func() { s.Serve(ctx); close(served) }()

	addr := s.Addr().String()
	c := dialTx(t, addr)
	defer c.close()
	// With no hook wired the server cannot stop itself, and says so rather than
	// pretending to.
	if got := c.cmd("SHUTDOWN"); !contains(got, "Errors trying to SHUTDOWN") {
		t.Errorf("SHUTDOWN with no shutdown hook = %q", got)
	}
	if got := c.cmd("SHUTDOWN BOGUS"); got != "-ERR syntax error" {
		t.Errorf("SHUTDOWN BOGUS = %q", got)
	}

	s.SetShutdownHook(cancel)
	c2 := dialTx(t, addr)
	defer c2.close()
	c2.conn.Write([]byte("SHUTDOWN NOSAVE\r\n"))
	select {
	case <-served:
	case <-time.After(5 * time.Second):
		t.Fatal("SHUTDOWN did not stop the server")
	}
	c2.conn.SetReadDeadline(time.Now().Add(time.Second))
	if line, err := c2.br.ReadString('\n'); err == nil {
		t.Errorf("SHUTDOWN replied %q; want the connection closed with no reply", line)
	}
}

// TestQuit covers the other way a connection ends: acknowledged, then closed. The
// acknowledgement has to arrive, which is why QUIT asks the connection loop to hang up
// after the flush instead of closing the socket itself.
func TestQuit(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	if got := c.cmd("QUIT"); got != "+OK" {
		t.Fatalf("QUIT = %q; want +OK before the close", got)
	}
	c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	c.conn.Write([]byte("PING\r\n"))
	if _, err := c.br.ReadString('\n'); err == nil {
		t.Error("the connection stayed open after QUIT")
	}
}

// TestResetClearsConnectionState covers RESET across every kind of state a connection
// can accumulate, which is what makes it safe for a connection pool to reuse a socket.
func TestResetClearsConnectionState(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("CLIENT SETNAME pooled")
	c.cmd("SET k 1")
	c.cmd("WATCH k")
	c.cmd("MULTI")
	c.cmd("SET k 2")
	if got := c.cmd("RESET"); got != "+RESET" {
		t.Fatalf("RESET = %q", got)
	}
	// The transaction is gone, not committed.
	if got := c.cmd("EXEC"); got != "-ERR EXEC without MULTI" {
		t.Errorf("EXEC after RESET = %q; want no transaction to be open", got)
	}
	if got := c.cmd("GET k"); got != "1" {
		t.Errorf("GET k = %q; RESET must discard the queued write, not apply it", got)
	}
	if got := c.cmd("CLIENT GETNAME"); got != "(nil)" {
		t.Errorf("CLIENT GETNAME after RESET = %q; want the name cleared", got)
	}
}
