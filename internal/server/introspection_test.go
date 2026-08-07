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
		{"CONFIG SET maxkeys not-a-number", "-ERR CONFIG SET failed (possibly related to argument 'maxkeys') - argument couldn't be parsed into an integer, or is out of range"},
		{"CONFIG GET maxkeys", "[maxkeys 1000]"},
		{"CONFIG SET notify-keyspace-events bogus", "-ERR CONFIG SET failed (possibly related to argument 'notify-keyspace-events') - Invalid event class character. Use 'Ag$lshzxeKEtmdn'."},
		{"CONFIG GET notify-keyspace-events", "[notify-keyspace-events AKE]"},
		// A setting fixed at startup is refused rather than silently ignored.
		{"CONFIG SET appendonly yes", "-ERR CONFIG SET failed (possibly related to argument 'appendonly') - can't set immutable config"},
		{"CONFIG SET port 1234", "-ERR CONFIG SET failed (possibly related to argument 'port') - can't set immutable config"},
		// And an unknown one is named in the error.
		{"CONFIG SET made-up 1", "-ERR Unknown option or number of arguments for CONFIG SET - 'made-up'"},
		// Measured on redis:7.2. Every container command answers an unknown subcommand with
		// this one sentence; this test previously asserted the older, longer wording, which
		// no Redis 7 emits.
		{"CONFIG BOGUS", "-ERR unknown subcommand 'BOGUS'. Try CONFIG HELP."},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	// HELP is the one subcommand every container command has, so an error here means the
	// dispatch has no default arm at all -- which is how the unknown-subcommand text above
	// came to be wrong in the first place. Assert it is a listing, and that it names what
	// this server implements rather than what Redis implements.
	help := c.cmd("CONFIG HELP")
	if strings.HasPrefix(help, "-ERR") {
		t.Fatalf("CONFIG HELP = %q; want a listing", help)
	}
	for _, want := range []string{"GET <pattern>", "SET <directive> <value>", "RESETSTAT", "HELP"} {
		if !contains(help, want) {
			t.Errorf("CONFIG HELP does not mention %q: %q", want, help)
		}
	}
	// CONFIG REWRITE is not implemented here, so HELP must not advertise it. A help text
	// that lists commands the server does not have is worse than no help text.
	if contains(help, "REWRITE") {
		t.Errorf("CONFIG HELP advertises REWRITE, which this server does not implement: %q", help)
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
		// This expectation was wrong and was pinning a bug rather than catching one:
		// commandFlags used to claim denyoom for every write, and DEL is the case that
		// shows why that matters. Measured on redis 7.2, `COMMAND INFO del` reports
		// `[+write]` with no denyoom -- deleting is how an operator recovers from a full
		// keyspace, so Redis never refuses it under memory pressure, and a client library
		// that avoids denyoom commands while OOM would have been told to avoid its own way
		// out. The flag now comes from the same measured table the OOM gate reads (see
		// oomDenyOOM in maxmemory.go).
		{"COMMAND INFO del", "[[del :-2 [+write] :1 :-1 :1]]"},
		// Measured on redis 7.2 as controls for the pair above: EXPIRE only moves a
		// deadline and so is not denyoom either, while LSET can store a longer element and
		// so is. "Every write is denyoom" is wrong in both directions.
		{"COMMAND INFO expire", "[[expire :-3 [+write] :1 :1 :1]]"},
		{"COMMAND INFO lset", "[[lset :4 [+write +denyoom] :1 :1 :1]]"},
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

	// DOCS is what redis-cli and several client libraries ask for on connect: a name ->
	// attributes map. Its fields are *not* COMMAND INFO's fields, and the difference is
	// load-bearing rather than cosmetic: redis-py walks the DOCS map converting the fields
	// it knows, and an integer under a key DOCS does not define made it raise
	// `ValueError: invalid literal for int() with base 10` before the application's first
	// command. `arity` is a COMMAND INFO field; asserting it here pinned the bug.
	docs := c.cmd("COMMAND DOCS get")
	for _, want := range []string{"get", "summary", "since", "group"} {
		if !contains(docs, want) {
			t.Errorf("COMMAND DOCS get is missing %q; got %q", want, docs)
		}
	}
	if contains(docs, "arity") {
		t.Errorf("COMMAND DOCS reports arity, which is a COMMAND INFO field: %q", docs)
	}
	// The group has to be the command's actual family, or a client that buckets commands
	// by it mislabels every one of them.
	for _, tc := range []struct{ cmd, want string }{
		{"COMMAND DOCS get", "string"},
		{"COMMAND DOCS lpush", "list"},
		{"COMMAND DOCS zadd", "sorted_set"},
		{"COMMAND DOCS hset", "hash"},
		{"COMMAND DOCS sadd", "set"},
		{"COMMAND DOCS xadd", "stream"},
		{"COMMAND DOCS pfadd", "hyperloglog"},
		{"COMMAND DOCS geoadd", "geo"},
		{"COMMAND DOCS setbit", "bitmap"},
		{"COMMAND DOCS subscribe", "pubsub"},
		{"COMMAND DOCS multi", "transactions"},
		{"COMMAND DOCS echo", "connection"},
		{"COMMAND DOCS info", "server"},
		{"COMMAND DOCS cluster", "cluster"},
		{"COMMAND DOCS del", "generic"},
	} {
		if got := c.cmd(tc.cmd); !contains(got, tc.want) {
			t.Errorf("%s should report group %q; got %q", tc.cmd, tc.want, got)
		}
	}
	// arity still belongs to INFO, so the coverage the assertion above gave up is kept here.
	if got := c.cmd("COMMAND INFO get"); !contains(got, ":2") {
		t.Errorf("COMMAND INFO get should still carry the arity: %q", got)
	}
	if got := c.cmd("COMMAND DOCS nosuchcommand"); got != "[]" {
		t.Errorf("COMMAND DOCS of an unknown command = %q; want an empty map", got)
	}
	// redis:7.2 spells this exactly one way, lower-cased and naming the container to ask
	// for help. Matching on a substring of the old wording is what let it drift.
	if got := c.cmd("COMMAND BOGUS"); got != "-ERR unknown subcommand 'BOGUS'. Try COMMAND HELP." {
		t.Errorf("COMMAND BOGUS = %q", got)
	}
}

// TestEchoIsAvailable covers ECHO, which looks like a toy and is not.
//
// StackExchange.Redis issues ECHO while establishing a connection, so a server without it
// cannot be reached by any .NET client at all: the handshake fails before the application
// sends its first command. It was missing here, and nothing in the test suite noticed
// because nothing in the test suite is a .NET client.
func TestEchoIsAvailable(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	if got := c.cmd("ECHO hello"); got != "hello" {
		t.Errorf("ECHO hello = %q; want hello", got)
	}
	// An empty payload is still a bulk string, not a null.
	if got := c.cmdRaw("ECHO", ""); got != "" {
		t.Errorf("ECHO \"\" = %q; want an empty bulk string", got)
	}
	if got := c.cmd("ECHO"); !contains(got, "wrong number of arguments") {
		t.Errorf("ECHO with no argument = %q; want an arity error", got)
	}
	if got := c.cmd("ECHO one two"); !contains(got, "wrong number of arguments") {
		t.Errorf("ECHO with two arguments = %q; want an arity error", got)
	}
}

// TestScriptAndFunctionStubs covers the two subcommands that decide whether Redis's own
// test suite can run against this server at all.
//
// There is no Lua interpreter here, and EVAL is honestly absent. But the suite's setup
// path flushes scripts and functions before each unit, and an error there stops it before
// a single test executes -- so the difference between answering these and not is the
// difference between "untestable by Redis's suite" and "passes N of Redis's own tests".
// Both answers are truthful on a server that caches nothing.
func TestScriptAndFunctionStubs(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		{"SCRIPT FLUSH", "+OK"},
		{"SCRIPT FLUSH ASYNC", "+OK"},
		{"FUNCTION FLUSH", "+OK"},
		{"FUNCTION LIST", "[]"},
		// Nothing is cached, so nothing exists -- one answer per sha asked about.
		{"SCRIPT EXISTS abc", "[:0]"},
		{"SCRIPT EXISTS abc def", "[:0 :0]"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}
	// What is not supported says so, rather than pretending: SCRIPT LOAD must hand back a
	// sha the client can then EVALSHA, and there is nothing to hand back.
	if got := c.cmd("SCRIPT LOAD \"return 1\""); !contains(got, "not supported") {
		t.Errorf("SCRIPT LOAD = %q; want an explicit refusal", got)
	}
	if got := c.cmd("FUNCTION LOAD whatever"); !contains(got, "not supported") {
		t.Errorf("FUNCTION LOAD = %q; want an explicit refusal", got)
	}
}

// TestInfoReportsRedisVersionAndMemory covers the fields monitoring tools parse.
//
// redis_exporter and every Grafana dashboard built on it read redis_version and
// used_memory; a server reporting neither is invisible to all of that tooling, and a
// client library that gates features on the version either fails or assumes the oldest
// server it supports.
func TestInfoReportsRedisVersionAndMemory(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	server := c.cmd("INFO server")
	for _, want := range []string{"redis_version:", "redis_mode:standalone", "loading:0", "shardkv_version:"} {
		if !contains(server, want) {
			t.Errorf("INFO server is missing %q; got %q", want, server)
		}
	}

	mem := c.cmd("INFO memory")
	for _, want := range []string{"used_memory:", "used_memory_human:", "maxmemory:", "maxmemory_policy:"} {
		if !contains(mem, want) {
			t.Errorf("INFO memory is missing %q; got %q", want, mem)
		}
	}
	// A bare INFO has to carry them too, since that is what an agent polls.
	if all := c.cmd("INFO"); !contains(all, "redis_version:") || !contains(all, "used_memory:") {
		t.Errorf("a bare INFO omits redis_version or used_memory")
	}
}

// TestClientSetInfo covers the handshake field modern redis-py and node-redis send
// unprompted, and that CLIENT LIST reports it back -- recording a value nothing ever
// shows would be pointless.
func TestClientSetInfo(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	if got := c.cmd("CLIENT SETINFO LIB-NAME redis-py"); got != "+OK" {
		t.Errorf("CLIENT SETINFO LIB-NAME = %q", got)
	}
	if got := c.cmd("CLIENT SETINFO LIB-VER 8.1.0"); got != "+OK" {
		t.Errorf("CLIENT SETINFO LIB-VER = %q", got)
	}
	info := c.cmd("CLIENT INFO")
	for _, want := range []string{"lib-name=redis-py", "lib-ver=8.1.0"} {
		if !contains(info, want) {
			t.Errorf("CLIENT INFO is missing %q; got %q", want, info)
		}
	}
	// An unset library still reports the fields, because a client that splits the line on
	// spaces counts them.
	fresh := dialTx(t, addr)
	defer fresh.close()
	if got := fresh.cmd("CLIENT INFO"); !contains(got, "lib-name=") {
		t.Errorf("CLIENT INFO omits lib-name when unset: %q", got)
	}
	if got := c.cmd("CLIENT SETINFO BOGUS x"); !contains(got, "Unrecognized option") {
		t.Errorf("CLIENT SETINFO with an unknown attribute = %q", got)
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
		{"CONFIG SET databases 32", "-ERR CONFIG SET failed (possibly related to argument 'databases') - can't set immutable config"},
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

// TestContainerFlagReachesEveryRegistrar pins the one thing containerCommands cannot
// enforce about itself: that a command listed there actually carries the flag in the
// table.
//
// There are four registrars, and each builds a *command literal by hand. Two of them --
// registerEffect and registerBlocking -- silently omitted `container`, so XGROUP (which
// is in containerCommands *and* registered with registerEffect, because its effect ships
// concrete stream ids) filed its statistics as cmdstat_xgroup instead of
// cmdstat_xgroup|create. Nothing reported it: the command worked, the counter existed,
// and it just aggregated the wrong things. That is invariant 12's "a statistic must not
// lie", and the failure mode of a hand-copied struct literal.
//
// Asserting over the whole set rather than over XGROUP means a fifth registrar, or a new
// container command registered through the wrong one, fails here rather than in a metric
// nobody is reading.
func TestContainerFlagReachesEveryRegistrar(t *testing.T) {
	for name := range containerCommands {
		cmd, ok := commandTable[name]
		if !ok {
			t.Errorf("containerCommands lists %q, which is not in the command table", name)
			continue
		}
		if !cmd.container {
			t.Errorf("%q is a container command but its table entry has container=false, so its "+
				"statistics are filed under the bare name instead of per subcommand", name)
		}
	}
	// And the converse, so the flag cannot be set by hand on something the set does not
	// list -- that would file a per-subcommand counter for a command whose first argument
	// is data, such as SET or GEOADD.
	for name, cmd := range commandTable {
		if cmd.container && !containerCommands[name] {
			t.Errorf("%q has container=true but is not in containerCommands", name)
		}
	}
}
