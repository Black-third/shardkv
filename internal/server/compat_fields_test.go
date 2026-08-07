package server

// Wire-level coverage for four reply surfaces whose *field sets* are the compatibility
// contract rather than their values: MEMORY STATS, the CLIENT INFO/LIST line,
// CONFIG GET maxmemory*, and the OBJECT ENCODING names.
//
// Every expectation below carries the value measured against a live Redis in a comment.
// Two versions are quoted because they differ:
//
//   - redis:7.2 (7.2.15, amd64/glibc on 127.0.0.1:6502 and arm64/alpine on :6399 -- the
//     two agreed on every field name and order checked here, so none of this is
//     architecture-dependent), and
//   - redis:7.4 (7.4.10), which is the level RedisCompatVersion claims.
//
// Where they differ the 7.4 answer is the target, and the difference is named. That is
// deliberate: INFO reports redis_version 7.4.0, so a client branching on it will parse
// the 7.4 shape.

import (
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/Black-third/shardkv/internal/store"
)

// clientInfoFields is the exact field order of the CLIENT INFO / CLIENT LIST line.
//
// Measured on redis 7.4.10:
//
//	id=5 addr=... laddr=... fd=10 name= age=0 idle=0 flags=N db=9 sub=0 psub=0 ssub=0
//	multi=-1 watch=0 qbuf=26 qbuf-free=20448 argv-mem=10 multi-mem=0 rbs=16384 rbp=16384
//	obl=0 oll=0 omem=0 tot-mem=37786 events=r cmd=client|info user=default redir=-1
//	resp=2 lib-name= lib-ver=
//
// redis 7.2.15 emits the identical line without `watch`. That single insertion after
// `multi` is the only difference between the two releases, and it is why the list below
// has 31 entries rather than 30.
//
// Note what is *not* in either: there is no `tot-net-in` or `tot-net-out` in 7.2.15 or in
// 7.4.10, measured on both. Those are a later addition, and adding them here would put
// this server's line in a shape no Redis release emits.
var clientInfoFields = []string{
	"id", "addr", "laddr", "fd", "name", "age", "idle", "flags", "db", "sub", "psub",
	"ssub", "multi", "watch", "qbuf", "qbuf-free", "argv-mem", "multi-mem", "rbs", "rbp",
	"obl", "oll", "omem", "tot-mem", "events", "cmd", "user", "redir", "resp",
	"lib-name", "lib-ver",
}

// lineField returns the value of one field of a CLIENT INFO line, or "" when absent. It
// splits on '=' at the first occurrence only, because addr= and laddr= carry values that
// contain none but name= may carry anything the client chose.
func lineField(line, name string) (string, bool) {
	for _, f := range strings.Fields(line) {
		if k, v, ok := strings.Cut(f, "="); ok && k == name {
			return v, true
		}
	}
	return "", false
}

// lineFieldNames is the field order a line actually carries.
func lineFieldNames(line string) []string {
	var out []string
	for _, f := range strings.Fields(line) {
		k, _, _ := strings.Cut(f, "=")
		out = append(out, k)
	}
	return out
}

// clientLineFor finds the CLIENT LIST line belonging to a client id.
func clientLineFor(t *testing.T, c *txConn, id string) string {
	t.Helper()
	for _, line := range strings.Split(c.cmd("CLIENT LIST"), "\n") {
		if got, ok := lineField(line, "id"); ok && got == id {
			return line
		}
	}
	return ""
}

// TestClientInfoFieldSet pins the whole line: every field Redis emits is present, in
// Redis's order, on both CLIENT INFO and CLIENT LIST.
//
// The order is checked and not only the membership because the field is documented as a
// line for humans and parsed as a line by tools, some of them positionally. A missing
// field is worse than a wrong value: it shifts everything after it.
func TestClientInfoFieldSet(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()
	other := dialTx(t, addr)
	defer other.close()
	// A command on the second connection, and not only a dial. A dial returns when the TCP
	// handshake completes, which is before the server's connection goroutine has run
	// newSession and put the session in the registry CLIENT LIST reads -- so asserting "at
	// least 2 connections" straight after dialling is asserting that the accept side has been
	// scheduled. It usually has; under -race with the rest of the package running it
	// intermittently has not, which is what made this test fail for a reason that had nothing
	// to do with the field set it exists to pin. A reply proves the registration happened,
	// because newSession precedes the command loop that produced it.
	other.cmd("PING")

	info := c.cmd("CLIENT INFO")
	if got := lineFieldNames(info); !equalStrings(got, clientInfoFields) {
		t.Errorf("CLIENT INFO fields\n got: %v\nwant: %v", got, clientInfoFields)
	}
	// CLIENT LIST shares the line, so it changes with it -- and every line must carry the
	// same fields, including the ones describing a connection that is not the caller.
	list := c.cmd("CLIENT LIST")
	lines := strings.Split(strings.TrimRight(list, "\n"), "\n")
	if len(lines) < 2 {
		t.Fatalf("CLIENT LIST reported %d connections; want at least 2:\n%s", len(lines), list)
	}
	for i, line := range lines {
		if got := lineFieldNames(line); !equalStrings(got, clientInfoFields) {
			t.Errorf("CLIENT LIST line %d fields\n got: %v\nwant: %v", i, got, clientInfoFields)
		}
	}

	// The fields that must carry a real value rather than a placeholder.
	cases := []struct {
		field string
		check func(string) bool
		why   string
	}{
		// laddr is the address the connection arrived at, which for a test server is the
		// loopback address it bound. Measured shape on redis 7.2.15: laddr=172.17.0.8:6379.
		{"laddr", func(v string) bool { return strings.HasPrefix(v, "127.0.0.1:") }, "the local address"},
		// fd is the OS descriptor. Redis reports -1 only for a client with no socket, so a
		// real connection must report a non-negative one.
		{"fd", func(v string) bool { n, err := strconv.Atoi(v); return err == nil && n >= 0 }, "a real descriptor"},
		// resp=2 for a connection that has not sent HELLO. Measured: resp=2 on redis 7.2.15.
		{"resp", func(v string) bool { return v == "2" }, "the negotiated protocol"},
		// Measured on redis 7.2.15 and 7.4.10, both: user=default, redir=-1, events=r, ssub=0.
		{"user", func(v string) bool { return v == "default" }, "the ACL user"},
		{"redir", func(v string) bool { return v == "-1" }, "no tracking redirection"},
		{"events", func(v string) bool { return v == "r" }, "waiting to read"},
		{"ssub", func(v string) bool { return v == "0" }, "no shard subscriptions"},
		// multi=-1 outside a transaction. Measured on redis 7.2.15.
		{"multi", func(v string) bool { return v == "-1" }, "no transaction open"},
		{"watch", func(v string) bool { return v == "0" }, "nothing watched"},
		// tot-mem must be a positive byte count: a connection holds buffers.
		{"tot-mem", func(v string) bool { n, err := strconv.Atoi(v); return err == nil && n > 0 }, "the buffers held"},
		{"rbs", func(v string) bool { n, err := strconv.Atoi(v); return err == nil && n > 0 }, "the reply buffer size"},
		{"qbuf-free", func(v string) bool { n, err := strconv.Atoi(v); return err == nil && n > 0 }, "the query buffer's room"},
	}
	for _, tc := range cases {
		v, ok := lineField(info, tc.field)
		if !ok {
			t.Errorf("CLIENT INFO has no %s= field (%s)", tc.field, tc.why)
			continue
		}
		if !tc.check(v) {
			t.Errorf("CLIENT INFO %s=%q is not %s", tc.field, v, tc.why)
		}
	}
}

// TestClientInfoFlagsAreASet pins the flags field against the letters real Redis reports
// for each connection state, measured by putting a real connection into the state and
// reading another connection's CLIENT LIST on redis 7.2.15.
//
// The field is a *set* of letters, not one letter, and reporting only the first
// applicable one was the bug: a connection in MULTI read "N", which asserts that nothing
// notable is true of it. The S and P letters already had coverage elsewhere
// (TestClientListFlagsAReplicaFeed, TestClientIntrospection); the four here did not.
func TestClientInfoFlagsAreASet(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	watcher := dialTx(t, addr)
	defer watcher.close()

	cases := []struct {
		name  string
		setup func(*txConn)
		// flags/multi/watch as measured on redis 7.2.15 (watch from 7.4.10, which is the
		// release that has the field).
		flags, multi, watch string
	}{
		{name: "plain", setup: func(*txConn) {}, flags: "N", multi: "-1", watch: "0"},
		{
			name:  "in MULTI",
			setup: func(c *txConn) { c.cmd("MULTI") },
			flags: "x", multi: "0", watch: "0",
		},
		{
			name: "MULTI with two queued",
			setup: func(c *txConn) {
				c.cmd("MULTI")
				c.cmd("SET fk vv")
				c.cmd("GET fk")
			},
			flags: "x", multi: "2", watch: "0",
		},
		{
			name:  "watching three keys",
			setup: func(c *txConn) { c.cmd("WATCH a b c") },
			flags: "N", multi: "-1", watch: "3",
		},
		{
			// WATCH plus a concurrent write to the watched key: Redis reports "d" (dirty
			// CAS), not "N", and the transaction that follows will abort.
			name:  "dirty CAS",
			setup: func(c *txConn) { c.cmd("WATCH dk"); watcher.cmd("SET dk other") },
			flags: "d", multi: "-1", watch: "1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := dialTx(t, addr)
			defer c.close()
			id := strings.TrimPrefix(c.cmd("CLIENT ID"), ":") // before MULTI: inside one it queues
			tc.setup(c)
			line := clientLineFor(t, watcher, id)
			if line == "" {
				t.Fatalf("no CLIENT LIST line for id %s", id)
			}
			for field, want := range map[string]string{
				"flags": tc.flags, "multi": tc.multi, "watch": tc.watch,
			} {
				if got, _ := lineField(line, field); got != want {
					t.Errorf("%s = %q; want %q (redis 7.2.15/7.4.10)\nline: %s",
						field, got, want, line)
				}
			}
		})
	}
}

// TestClientInfoFlagsBlocked covers the b flag on its own, because getting a connection
// into it means leaving a command outstanding.
//
// Measured on redis 7.2.15: a client parked in BLPOP reads flags=b.
func TestClientInfoFlagsBlocked(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	watcher := dialTx(t, addr)
	defer watcher.close()
	blocked := dialTx(t, addr)
	defer blocked.close()

	id := strings.TrimPrefix(blocked.cmd("CLIENT ID"), ":")
	// Sent without reading the reply: the point is that it has not returned.
	if _, err := blocked.conn.Write([]byte("BLPOP no:such:key 5\r\n")); err != nil {
		t.Fatalf("write BLPOP: %v", err)
	}
	waitFor(t, "the blocked client to be flagged b", func() bool {
		f, _ := lineField(clientLineFor(t, watcher, id), "flags")
		return f == "b"
	})
	// And it goes away again once the client is unblocked, so the flag describes now
	// rather than ever.
	watcher.cmd("CLIENT UNBLOCK " + id)
	waitFor(t, "the flag to clear once unblocked", func() bool {
		f, _ := lineField(clientLineFor(t, watcher, id), "flags")
		return f == "N"
	})
}

// TestClientInfoRespFollowsHello checks the resp= field against the protocol the
// connection actually negotiated, including for a *different* connection's line -- which
// is the case that needs the value published on the session, since a connection's writer
// belongs to its own goroutine.
//
// Measured on redis 7.2.15: resp=2 before HELLO, resp=3 after HELLO 3, resp=2 again after
// RESET.
func TestClientInfoRespFollowsHello(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	watcher := dialTx(t, addr)
	defer watcher.close()
	c := dialTx(t, addr)
	defer c.close()

	id := strings.TrimPrefix(c.cmd("CLIENT ID"), ":")
	for _, step := range []struct {
		do   string
		want string
	}{
		{do: "", want: "2"},
		{do: "HELLO 3", want: "3"},
		{do: "HELLO 2", want: "2"},
		{do: "HELLO 3", want: "3"},
		{do: "RESET", want: "2"},
	} {
		if step.do != "" {
			c.cmd(step.do)
		}
		own, _ := lineField(c.cmd("CLIENT INFO"), "resp")
		other, _ := lineField(clientLineFor(t, watcher, id), "resp")
		if own != step.want || other != step.want {
			t.Errorf("after %q: own resp=%q, as seen by another connection resp=%q; want %q",
				step.do, own, other, step.want)
		}
	}
}

// TestMemoryStatsFieldSet pins the MEMORY STATS reply: which fields it carries, in which
// order, that the ones with no honest value are absent rather than fabricated, and that
// its numbers agree with the other two commands that report the same quantities.
//
// The field order is redis 7.2.15's, measured:
//
//	peak.allocated total.allocated startup.allocated replication.backlog clients.slaves
//	clients.normal cluster.links aof.buffer lua.caches functions.caches db.<N>
//	overhead.total keys.count keys.bytes-per-key dataset.bytes dataset.percentage
//	peak.percentage allocator.allocated allocator.active allocator.resident
//	allocator-fragmentation.ratio allocator-fragmentation.bytes allocator-rss.ratio
//	allocator-rss.bytes rss-overhead.ratio rss-overhead.bytes fragmentation
//	fragmentation.bytes
//
// redis 7.4.10 adds overhead.db.hashtable.lut, overhead.db.hashtable.rehashing,
// db.dict.rehashing.count and allocator.muzzy; all four describe its dict and allocator
// internals and are absent here for the same reason the allocator fields are.
func TestMemoryStatsFieldSet(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("SET s hello")
	c.cmd("SET big " + strings.Repeat("x", 2000))
	c.cmd("EXPIRE big 1000")
	c.cmd("RPUSH l a b c")

	// What this server emits, in this order. Every name is one redis 7.2.15 emits, and the
	// relative order is 7.2.15's -- see the checks below, which assert both against the
	// reference list rather than only against this literal.
	wantOrder := []string{
		"total.allocated", "replication.backlog", "clients.slaves", "clients.normal",
		"cluster.links", "lua.caches", "functions.caches", "db.0",
		"overhead.total", "keys.count", "keys.bytes-per-key", "dataset.bytes",
	}
	// redis 7.2.15's full order, for the ordering check. A field this server emits must
	// appear here, and the emitted subsequence must be increasing in this list.
	redis72Order := []string{
		"peak.allocated", "total.allocated", "startup.allocated", "replication.backlog",
		"clients.slaves", "clients.normal", "cluster.links", "aof.buffer", "lua.caches",
		"functions.caches", "db.0", "overhead.total", "keys.count", "keys.bytes-per-key",
		"dataset.bytes", "dataset.percentage", "peak.percentage", "allocator.allocated",
		"allocator.active", "allocator.resident", "allocator-fragmentation.ratio",
		"allocator-fragmentation.bytes", "allocator-rss.ratio", "allocator-rss.bytes",
		"rss-overhead.ratio", "rss-overhead.bytes", "fragmentation", "fragmentation.bytes",
	}

	got := c.cmd("MEMORY STATS")
	names, values := parseFlatMap(t, got)
	if !equalStrings(names, wantOrder) {
		t.Fatalf("MEMORY STATS fields\n got: %v\nwant: %v\nreply: %s", names, wantOrder, got)
	}
	// Every emitted name exists in redis 7.2.15, and in its order. Checking the order
	// against Redis's own list rather than only against wantOrder is what would catch a
	// future field inserted in the wrong place.
	prev := -1
	for _, n := range names {
		i := indexOfString(redis72Order, n)
		if i < 0 {
			t.Errorf("MEMORY STATS emits %q, which redis 7.2.15 does not", n)
			continue
		}
		if i <= prev {
			t.Errorf("MEMORY STATS emits %q out of redis 7.2.15's order", n)
		}
		prev = i
	}
	// The fields deliberately omitted must stay omitted: each would have to be invented.
	// This is the half of the change that is a claim about honesty rather than about
	// compatibility, so it is pinned.
	for _, absent := range []string{
		"peak.allocated", "startup.allocated", "aof.buffer", "dataset.percentage",
		"peak.percentage", "allocator.allocated", "allocator.active", "allocator.resident",
		"allocator-fragmentation.ratio", "allocator-rss.ratio", "rss-overhead.ratio",
		"fragmentation", "fragmentation.bytes",
	} {
		if indexOfString(names, absent) >= 0 {
			t.Errorf("MEMORY STATS reports %q, which this server cannot derive honestly", absent)
		}
	}

	// db.0 is a nested map (a nested flat array in RESP2, which is what this connection
	// speaks): exactly overhead.hashtable.main, since there is no separate expires table
	// and no slot-to-keys index to report.
	db0 := values["db.0"]
	if !strings.HasPrefix(db0, "[overhead.hashtable.main :") || !strings.HasSuffix(db0, "]") {
		t.Errorf("db.0 = %q; want a nested [overhead.hashtable.main <n>]", db0)
	}
	if strings.Contains(db0, "overhead.hashtable.expires") {
		t.Errorf("db.0 reports an expires table this store does not have: %q", db0)
	}
	// A database holding nothing gets no entry, as in Redis -- measured on 7.2.15, which
	// listed only db.9 and db.14, the two that held keys out of sixteen.
	for i := 1; i < 16; i++ {
		if _, ok := values["db."+strconv.Itoa(i)]; ok {
			t.Errorf("MEMORY STATS lists db.%d, which holds no keys", i)
		}
	}

	// keys.count is the live key count, which DBSIZE reports too: two answers to one
	// question that must not differ.
	if got, want := values["keys.count"], strings.TrimPrefix(c.cmd("DBSIZE"), ":"); got != want {
		t.Errorf("keys.count = %s but DBSIZE = %s", got, want)
	}
	// dataset.bytes is the same estimator MEMORY USAGE reports, less the keyspace
	// bookkeeping. So the two commands must add up, and this is the identity that keeps
	// dataset.bytes from becoming a second, drifting estimate.
	var usage int64
	for _, k := range []string{"s", "big", "l"} {
		usage += mustInt(t, c.cmd("MEMORY USAGE "+k))
	}
	dataset := mustInt(t, ":"+values["dataset.bytes"])
	// db.0's value stays nested, so its own integer still carries parseReply's ':' tag.
	overheadDB := mustInt(t, strings.TrimSuffix(
		strings.TrimPrefix(db0, "[overhead.hashtable.main "), "]"))
	if dataset+overheadDB != usage {
		t.Errorf("dataset.bytes(%d) + db.0 overhead(%d) = %d; the sum of MEMORY USAGE is %d",
			dataset, overheadDB, dataset+overheadDB, usage)
	}
	// keys.bytes-per-key is dataset.bytes over keys.count, and 0 for an empty keyspace
	// rather than a division by zero.
	keys := mustInt(t, ":"+values["keys.count"])
	if got, want := mustInt(t, ":"+values["keys.bytes-per-key"]), dataset/keys; got != want {
		t.Errorf("keys.bytes-per-key = %d; want dataset.bytes/keys.count = %d", got, want)
	}
	// clients.normal is the sum of exactly the tot-mem CLIENT LIST publishes, so an
	// operator cannot get two different answers for what the connections cost.
	var totMem int64
	for _, line := range strings.Split(strings.TrimRight(c.cmd("CLIENT LIST"), "\n"), "\n") {
		v, _ := lineField(line, "tot-mem")
		totMem += mustInt(t, ":"+v)
	}
	if got := mustInt(t, ":"+values["clients.normal"]); got != totMem {
		t.Errorf("clients.normal = %d; the CLIENT LIST tot-mem values sum to %d", got, totMem)
	}

	// An empty server still answers, with no db entries at all.
	c.cmd("FLUSHALL")
	names, values = parseFlatMap(t, c.cmd("MEMORY STATS"))
	if indexOfString(names, "db.0") >= 0 {
		t.Errorf("MEMORY STATS on an empty keyspace still lists db.0")
	}
	for _, f := range []string{"keys.count", "keys.bytes-per-key", "dataset.bytes"} {
		if values[f] != "0" {
			t.Errorf("on an empty keyspace %s = %s; want 0", f, values[f])
		}
	}

	// Arity and the help text.
	if got := c.cmd("MEMORY STATS extra"); got != "-ERR wrong number of arguments for 'memory|stats' command" {
		t.Errorf("MEMORY STATS with an argument = %q", got)
	}
	if got := c.cmd("MEMORY HELP"); !contains(got, "STATS") {
		t.Errorf("MEMORY HELP does not mention STATS: %q", got)
	}
}

// TestMemoryStatsIsAMapInRESP3 checks the reply shape, which is the half of the
// compatibility that RESP2 cannot show: the top level is a map and each db.N is a nested
// map. Measured on redis 7.2.15 with HELLO 3: both are maps.
func TestMemoryStatsIsAMapInRESP3(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()
	c.cmd("HELLO 3")
	c.cmd("SET k v")

	got := c.cmd("MEMORY STATS")
	if !strings.HasPrefix(got, "{") {
		t.Fatalf("MEMORY STATS in RESP3 = %q; want a map", got)
	}
	// The nested one: db.0 followed by a `{`.
	if !contains(got, "db.0 {overhead.hashtable.main :") {
		t.Errorf("db.0 is not a nested map in RESP3: %q", got)
	}
}

// TestConfigGetMaxmemoryFamily covers the parameters a client library asks for while
// warming up. An empty reply is not the same statement as "no limit", which is what made
// this a compatibility gap rather than a missing feature.
//
// Measured on redis 7.2.15, CONFIG GET maxmemory*:
//
//	maxmemory 0, maxmemory-policy noeviction, maxmemory-samples 5,
//	maxmemory-clients 0, maxmemory-eviction-tenacity 10
//
// (7.4.10 answers the same five; the order differs between servers because Redis walks a
// dict, so order is not part of this contract -- unlike the CLIENT INFO line.)
//
// The read-only half of this test was correct when nothing here measured bytes and is
// obsolete now that something does. It used to assert that every member of the family is
// refused by CONFIG SET, on the reasoning that accepting a value nothing could act on is
// how a client comes to believe it configured a limit. Bytes are measured now
// (internal/store/memtrack.go), so the values *are* acted on and refusing them would be the
// misleading answer. What replaces those assertions is the round trip -- set it, read it
// back as Redis reports it -- plus TestMaxmemoryConfigMatchesRedis for the operand forms
// and the exact refusals.
func TestConfigGetMaxmemoryFamily(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	_, got := parseFlatMap(t, c.cmd("CONFIG GET maxmemory*"))
	cases := []struct {
		param, want, why string
	}{
		// 0 is the truth, not a placeholder: no byte budget is enforced anywhere.
		// Measured: maxmemory 0 on redis 7.2.15 with no limit configured.
		{"maxmemory", "0", "no byte limit is enforced"},
		// Measured: noeviction on a redis with no maxmemory set. Here it is noeviction
		// because maxkeys is unset; see the allkeys-lru case below.
		{"maxmemory-policy", "noeviction", "nothing is evicted without a cap"},
		// Deliberately *not* Redis's 5: this is the number of keys the sampler really
		// looks at (store.EvictionSamples).
		{"maxmemory-samples", "16", "the sampler's real sample size"},
		// Measured: 0 on redis 7.2.15, which also means "no client eviction".
		{"maxmemory-clients", "0", "no client eviction"},
	}
	for _, tc := range cases {
		if v, ok := got[tc.param]; !ok {
			t.Errorf("CONFIG GET maxmemory* omits %s (%s)", tc.param, tc.why)
		} else if v != tc.want {
			t.Errorf("CONFIG GET %s = %q; want %q (%s)", tc.param, v, tc.want, tc.why)
		}
	}
	// maxmemory-eviction-tenacity is absent on purpose: there is no effort budget to tune
	// because eviction does not share a time slice with the command path here.
	if _, ok := got["maxmemory-eviction-tenacity"]; ok {
		t.Error("CONFIG GET reports maxmemory-eviction-tenacity, which tunes nothing here")
	}
	// An exact-name GET works too, because that is the form some clients send.
	for _, p := range []string{"maxmemory", "maxmemory-policy", "maxmemory-samples"} {
		if _, v := parseFlatMap(t, c.cmd("CONFIG GET "+p)); len(v) != 1 {
			t.Errorf("CONFIG GET %s did not return exactly one parameter: %v", p, v)
		}
	}

	// The policy must agree with INFO's, which is the same accessor -- a client that
	// compared them would otherwise find the server contradicting itself.
	infoPolicy := infoField(t, c, "memory", "maxmemory_policy")
	if _, v := parseFlatMap(t, c.cmd("CONFIG GET maxmemory-policy")); v["maxmemory-policy"] != infoPolicy {
		t.Errorf("CONFIG GET maxmemory-policy = %q but INFO says %q",
			v["maxmemory-policy"], infoPolicy)
	}
	if infoMax := infoField(t, c, "memory", "maxmemory"); infoMax != "0" {
		t.Errorf("INFO maxmemory = %q; want 0 to match CONFIG GET", infoMax)
	}

	// With a maxkeys cap and no policy explicitly chosen, the policy still follows what
	// eviction actually does, in both places at once -- the default is derived rather than
	// flatly noeviction. See Server.evictionPolicy.
	c.cmd("CONFIG SET maxkeys 10")
	if _, v := parseFlatMap(t, c.cmd("CONFIG GET maxmemory-policy")); v["maxmemory-policy"] != "allkeys-lru" {
		t.Errorf("with maxkeys set, maxmemory-policy = %q; want allkeys-lru", v["maxmemory-policy"])
	}
	if got := infoField(t, c, "memory", "maxmemory_policy"); got != "allkeys-lru" {
		t.Errorf("with maxkeys set, INFO maxmemory_policy = %q; want allkeys-lru", got)
	}
	c.cmd("CONFIG SET maxkeys 0")

	// An explicit policy wins over that derived default, or the parameter would read back as
	// something other than what it was set to -- the one property that makes a settable
	// config worth having.
	c.cmd("CONFIG SET maxkeys 10")
	if got := c.cmd("CONFIG SET maxmemory-policy noeviction"); got != "+OK" {
		t.Errorf("CONFIG SET maxmemory-policy noeviction = %q; want +OK", got)
	}
	if _, v := parseFlatMap(t, c.cmd("CONFIG GET maxmemory-policy")); v["maxmemory-policy"] != "noeviction" {
		t.Errorf("after setting it explicitly, maxmemory-policy = %q; want noeviction",
			v["maxmemory-policy"])
	}
	c.cmd("CONFIG SET maxkeys 0")

	// The three members this server can act on round-trip: set, then read back the value
	// Redis would report. maxmemory reports a byte count and not the operand it was given --
	// measured: redis 7.2 answers 104857600 for `CONFIG SET maxmemory 100mb`.
	roundTrip := []struct{ param, set, want string }{
		{"maxmemory", "100mb", "104857600"}, // measured on redis 7.2
		{"maxmemory", "1gb", "1073741824"},  // measured on redis 7.2
		{"maxmemory", "0", "0"},             // measured on redis 7.2
		{"maxmemory-policy", "volatile-ttl", "volatile-ttl"},
		{"maxmemory-samples", "5", "5"},
		{"lfu-log-factor", "10", "10"}, // Redis's default, measured
		{"lfu-decay-time", "1", "1"},   // Redis's default, measured
	}
	for _, tc := range roundTrip {
		if got := c.cmd("CONFIG SET " + tc.param + " " + tc.set); got != "+OK" {
			t.Errorf("CONFIG SET %s %s = %q; want +OK", tc.param, tc.set, got)
			continue
		}
		if _, v := parseFlatMap(t, c.cmd("CONFIG GET "+tc.param)); v[tc.param] != tc.want {
			t.Errorf("CONFIG SET %s %s then GET = %q; want %q",
				tc.param, tc.set, v[tc.param], tc.want)
		}
	}
	c.cmd("CONFIG SET maxmemory 0")
	c.cmd("CONFIG SET maxmemory-policy noeviction")

	// maxmemory-clients stays read-only, and for the reason it always had: nothing here
	// measures the bytes a client's buffers hold, so there is no budget to act on. A client
	// that will not read is dropped when its queue fills (invariant 6), never evicted.
	if got := c.cmd("CONFIG SET maxmemory-clients 1mb"); !contains(got, "can't set immutable config") {
		t.Errorf("CONFIG SET maxmemory-clients = %q; want the immutable-config refusal", got)
	}
}

// TestObjectEncodingOrigin is the string half of the OBJECT ENCODING matrix: the cases
// where the name depends on *which command produced the value* rather than on its bytes.
//
// Every want below was measured against redis 7.2.15 (and re-checked on 7.4.10, which
// agrees on all of them).
//
// The case this table exists for is INCRBYFLOAT: Redis's incrbyfloatCommand builds the
// result with createStringObject and never attempts the integer encoding, so an integral
// result reads embstr where the same digits from SET read int. Reporting int there was a
// real disagreement, and the fix needed a third state rather than the existing raw flag,
// which would have answered raw.
func TestObjectEncodingOrigin(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct {
		name string
		do   []string
		key  string
		want string // measured on redis 7.2.15
	}{
		// Stored whole, integer encoding attempted.
		{name: "SET integer", do: []string{"SET e 123"}, key: "e", want: "int"},
		{name: "SET short text", do: []string{"SET e hello"}, key: "e", want: "embstr"},
		{name: "SET 44 bytes", do: []string{"SET e " + strings.Repeat("a", 44)}, key: "e", want: "embstr"},
		{name: "SET 45 bytes", do: []string{"SET e " + strings.Repeat("a", 45)}, key: "e", want: "raw"},
		// 21 digits cannot be an int64, so the length rule takes over even though it looks
		// like a number.
		{name: "SET 21 digits", do: []string{"SET e " + strings.Repeat("9", 21)}, key: "e", want: "embstr"},
		{name: "INCR from missing", do: []string{"INCR e"}, key: "e", want: "int"},
		{name: "INCRBY", do: []string{"SET e 10", "INCRBY e 5"}, key: "e", want: "int"},
		{name: "GETSET", do: []string{"SET e x", "GETSET e 42"}, key: "e", want: "int"},
		{name: "SETEX", do: []string{"SETEX e 100 42"}, key: "e", want: "int"},
		{name: "MSET", do: []string{"MSET e 77"}, key: "e", want: "int"},
		{name: "SETNX", do: []string{"SETNX e 77"}, key: "e", want: "int"},

		// Built whole, integer encoding *not* attempted: the INCRBYFLOAT family.
		{name: "INCRBYFLOAT integral result", do: []string{"SET e 10.5", "INCRBYFLOAT e 0.5"}, key: "e", want: "embstr"},
		{name: "INCRBYFLOAT onto an int", do: []string{"SET e 1", "INCRBYFLOAT e 1"}, key: "e", want: "embstr"},
		{name: "INCRBYFLOAT fractional", do: []string{"SET e 1", "INCRBYFLOAT e 0.5"}, key: "e", want: "embstr"},
		{name: "INCRBYFLOAT from missing", do: []string{"INCRBYFLOAT e 5"}, key: "e", want: "embstr"},
		{
			// A 21-digit integral result: not an int64, and short enough for embstr, so the
			// two rules agree here -- which is why this case passed before the fix and is
			// kept as the control that shows the new state did not change it.
			name: "INCRBYFLOAT 21 digits", do: []string{"SET e 1", "INCRBYFLOAT e 1" + strings.Repeat("0", 20)},
			key: "e", want: "embstr",
		},
		{
			// Past 44 bytes the length rule takes over and even a plain object is raw. The
			// operand has to be large rather than tiny: an increment below long double's
			// precision is absorbed and leaves a one-character value. Measured on redis
			// 7.2.15 and 7.4.10 -- both store 45 characters here and report raw.
			name: "INCRBYFLOAT past 44 bytes",
			do:   []string{"SET e 0", "INCRBYFLOAT e 1" + strings.Repeat("0", 44)},
			key:  "e", want: "raw",
		},
		{
			// The control for the case above: an increment smaller than long double can
			// represent leaves the value at "1", which is embstr. Measured on redis 7.2.15.
			name: "INCRBYFLOAT below precision",
			do:   []string{"SET e 1", "INCRBYFLOAT e 0." + strings.Repeat("0", 44) + "1"},
			key:  "e", want: "embstr",
		},
		// An integral INCRBYFLOAT result stays a plain object, so a following INCR --
		// which does store whole -- takes it back to int.
		{name: "INCRBYFLOAT then INCR", do: []string{"SET e 1", "INCRBYFLOAT e 1", "INCR e"}, key: "e", want: "int"},

		// Edited in place: always raw.
		{name: "APPEND onto an int", do: []string{"SET e 1", "APPEND e 2"}, key: "e", want: "raw"},
		{name: "APPEND twice", do: []string{"APPEND e 1", "APPEND e 2"}, key: "e", want: "raw"},
		{name: "SETRANGE", do: []string{"SET e hello", "SETRANGE e 1 x"}, key: "e", want: "raw"},
		{name: "SETRANGE creating", do: []string{"SETRANGE e 0 12"}, key: "e", want: "raw"},
		{name: "SETBIT creating", do: []string{"SETBIT e 7 1"}, key: "e", want: "raw"},
		{name: "BITFIELD SET", do: []string{"BITFIELD e SET u8 0 255"}, key: "e", want: "raw"},
		{name: "PFADD", do: []string{"PFADD e a b"}, key: "e", want: "raw"},
		{name: "INCRBYFLOAT then APPEND", do: []string{"SET e 1", "INCRBYFLOAT e 1", "APPEND e 0"}, key: "e", want: "raw"},

		// APPEND is the one command whose answer depends on whether the key was there:
		// creating one is a whole store and runs the integer encoding, so these three
		// differ from "APPEND onto an int" above.
		{name: "APPEND creating an int", do: []string{"APPEND e 123"}, key: "e", want: "int"},
		{name: "APPEND creating text", do: []string{"APPEND e abc"}, key: "e", want: "embstr"},
		{name: "APPEND creating 45 bytes", do: []string{"APPEND e " + strings.Repeat("a", 45)}, key: "e", want: "raw"},

		// COPY duplicates the object, so the copy reports what the source reported. This is
		// the case entry.clone used to lose, which gave one byte sequence two encodings.
		{name: "COPY of an appended value", do: []string{"SET s 1", "APPEND s 2", "COPY s e"}, key: "e", want: "raw"},
		{name: "COPY of an int", do: []string{"SET s 12", "COPY s e"}, key: "e", want: "int"},
		{name: "COPY of an INCRBYFLOAT result", do: []string{"SET s 1", "INCRBYFLOAT s 1", "COPY s e"}, key: "e", want: "embstr"},
		{name: "COPY of a SETBIT value", do: []string{"SETBIT s 7 1", "COPY s e"}, key: "e", want: "raw"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c.cmd("DEL e s")
			for _, cmd := range tc.do {
				if got := c.cmd(cmd); strings.HasPrefix(got, "-") {
					t.Fatalf("%q failed: %s", cmd, got)
				}
			}
			if got := c.cmd("OBJECT ENCODING " + tc.key); got != tc.want {
				t.Errorf("OBJECT ENCODING after %v = %q; want %q (redis 7.2.15)",
					tc.do, got, tc.want)
			}
		})
	}
}

// TestObjectEncodingSurvivesDumpAndRestore checks the one path that rebuilds a value from
// its bytes rather than from a command: a DUMP fed back through RESTORE must report the
// encoding a fresh store of the same bytes would.
//
// Measured on redis 7.2.15: DUMP/RESTORE of `SET k 123` reports int, and of an appended
// value reports... raw there, because Redis's payload records the encoding. Here the
// payload does not, so a restored appended value reads int -- and that is recorded as the
// known difference rather than asserted away, because inventing an encoding field in the
// payload would change the format DUMP promises.
func TestObjectEncodingSurvivesDumpAndRestore(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	// An integer value round-trips to int, which is what a client that DUMPs and RESTOREs
	// a counter sees. Measured on redis 7.2.15: int.
	c.cmd("SET n 123")
	payload := c.cmdRaw("DUMP", "n")
	if got := c.cmdRaw("RESTORE", "n2", "0", payload); got != "+OK" {
		t.Fatalf("RESTORE: %s", got)
	}
	if got := c.cmd("OBJECT ENCODING n2"); got != "int" {
		t.Errorf("OBJECT ENCODING of a restored integer = %q; want int (redis 7.2.15)", got)
	}
	// A long value round-trips to raw by length alone, so no origin is needed for it.
	c.cmd("SET big " + strings.Repeat("z", 100))
	payload = c.cmdRaw("DUMP", "big")
	c.cmdRaw("RESTORE", "big2", "0", payload)
	if got := c.cmd("OBJECT ENCODING big2"); got != "raw" {
		t.Errorf("OBJECT ENCODING of a restored 100-byte value = %q; want raw", got)
	}
}

// --- helpers -----------------------------------------------------------------

// parseFlatMap reads a reply that parseReply rendered as "{k v k v}" (RESP3) or
// "[k v k v]" (RESP2) into its ordered names and a name->value map. Values that are
// themselves nested keep their bracketed form, which is what the db.N check reads.
func parseFlatMap(t *testing.T, reply string) ([]string, map[string]string) {
	t.Helper()
	body := reply
	if len(body) >= 2 && (body[0] == '{' || body[0] == '[') {
		body = body[1 : len(body)-1]
	} else {
		t.Fatalf("not a map or array reply: %q", reply)
	}
	fields := splitTopLevel(body)
	if len(fields)%2 != 0 {
		t.Fatalf("odd number of elements in %q: %v", reply, fields)
	}
	names := make([]string, 0, len(fields)/2)
	out := make(map[string]string, len(fields)/2)
	for i := 0; i < len(fields); i += 2 {
		// parseReply renders a RESP integer as ":N", which is how it distinguishes one from
		// a bulk string carrying digits. The distinction is not what these tests are
		// checking -- the reply shapes are pinned elsewhere -- so the tag is dropped here
		// and the values compare as plain numbers. A bulk string can never start with ':'
		// in these replies, so nothing else is stripped by accident.
		names = append(names, fields[i])
		out[fields[i]] = strings.TrimPrefix(fields[i+1], ":")
	}
	return names, out
}

// splitTopLevel splits on spaces that are not inside a nested [...] or {...}, so a nested
// reply stays one element.
func splitTopLevel(s string) []string {
	var out []string
	depth, start := 0, 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '[', '{':
			depth++
		case ']', '}':
			depth--
		case ' ':
			if depth == 0 {
				out = append(out, s[start:i])
				start = i + 1
			}
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// indexOfString is the []string form; the suite's existing indexOf searches a string for
// a substring, which is a different question with the same name.
func indexOfString(hay []string, needle string) int {
	for i, s := range hay {
		if s == needle {
			return i
		}
	}
	return -1
}

// TestClientListReadsOtherSessionsWithoutRacing is a race detector exercise, not a
// behaviour test: the CLIENT INFO line reports state that belongs to *other* connections'
// goroutines, so every field it reads has to come from an atomic, an immutable value, or a
// lock. Adding a field is exactly how that stops being true, and the failure is a data
// race rather than a wrong answer -- which no assertion about the output would catch.
//
// It drives the states the line reports (a transaction being opened and queued into, a
// WATCH being taken and released, a subscription, a blocking command) while another
// connection loops CLIENT LIST over all of them.
func TestClientListReadsOtherSessionsWithoutRacing(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	const workers = 6
	done := make(chan struct{})
	var wg sync.WaitGroup

	churn := func(id int) {
		defer wg.Done()
		c := dialTx(t, addr)
		defer c.close()
		key := "rk" + strconv.Itoa(id)
		for {
			select {
			case <-done:
				return
			default:
			}
			c.cmd("MULTI")
			c.cmd("SET " + key + " v")
			c.cmd("INCR " + key + "n")
			c.cmd("EXEC")
			c.cmd("WATCH " + key)
			c.cmd("MULTI")
			c.cmd("GET " + key)
			c.cmd("DISCARD")
			c.cmd("UNWATCH")
			c.cmd("HELLO 3")
			c.cmd("HELLO 2")
		}
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go churn(i)
	}

	// A subscriber and a blocked client, both left in their state for the duration.
	sub := dialTx(t, addr)
	defer sub.close()
	subscribeCmd(t, sub, "SUBSCRIBE rch", 1)
	blocked := dialTx(t, addr)
	defer blocked.close()
	if _, err := blocked.conn.Write([]byte("BLPOP no:such:key 3\r\n")); err != nil {
		t.Fatalf("write BLPOP: %v", err)
	}

	watcher := dialTx(t, addr)
	defer watcher.close()
	for i := 0; i < 60; i++ {
		if got := watcher.cmd("CLIENT LIST"); !contains(got, "id=") {
			t.Fatalf("CLIENT LIST returned nothing useful: %q", got)
		}
		if got := watcher.cmd("CLIENT INFO"); !contains(got, "tot-mem=") {
			t.Fatalf("CLIENT INFO is missing tot-mem: %q", got)
		}
		// MEMORY STATS sums the same per-connection accounting over every session, so it
		// reads across the same boundary and is exercised here too.
		if got := watcher.cmd("MEMORY STATS"); !contains(got, "clients.normal") {
			t.Fatalf("MEMORY STATS returned nothing useful: %q", got)
		}
	}
	close(done)
	wg.Wait()
}

// TestClientInfoWithoutASocket covers the session a test (or an internal caller) makes with
// no connection behind it. It still has to produce a parseable line, because it is in the
// registry and so appears in CLIENT LIST -- and laddr and fd are the two fields that read
// the socket.
//
// -1 is Redis's own value for a client with no descriptor; it uses it for the fake client
// that replays an AOF. So the field is always present and never a guess.
func TestClientInfoWithoutASocket(t *testing.T) {
	s := New(store.New(4))
	sess := s.newSession(nil)
	line := clientInfoLine(sess, s.clientRegistry())

	if got := lineFieldNames(line); !equalStrings(got, clientInfoFields) {
		t.Errorf("a session with no socket produced fields\n got: %v\nwant: %v", got, clientInfoFields)
	}
	if v, _ := lineField(line, "laddr"); v != "" {
		t.Errorf("laddr = %q; want empty for a session with no socket", v)
	}
	if v, _ := lineField(line, "fd"); v != "-1" {
		t.Errorf("fd = %q; want -1 for a session with no socket", v)
	}
}
