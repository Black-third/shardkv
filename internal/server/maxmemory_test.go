package server

import (
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

// keysOf lists the keyspace, parsing the array rendering readReply produces. It exists so
// the eviction tests can compare a master's key set against a replica's rather than merely
// their sizes -- two datasets of equal size that hold different keys is exactly the silent
// divergence eviction propagation exists to prevent.
func keysOf(t *testing.T, c *txConn) []string {
	t.Helper()
	reply := c.cmd("KEYS *")
	if reply == "[]" || reply == "(nil)" {
		return nil
	}
	if !strings.HasPrefix(reply, "[") || !strings.HasSuffix(reply, "]") {
		t.Fatalf("KEYS * = %q; want an array", reply)
	}
	return strings.Fields(reply[1 : len(reply)-1])
}

// TestEveryWriteIsClassifiedForOOM is the test that makes the denyoom table complete rather
// than merely populated.
//
// A map of only the *refused* names cannot distinguish "this write is safe under memory
// pressure" from "nobody has classified this write yet", so a newly added command would
// escape the byte budget silently -- it would keep being accepted while the server was over
// its limit, growing a keyspace that was supposed to be bounded, with nothing anywhere
// reporting it. Recording both answers and insisting here that every write has one turns that
// silent failure into a failing test.
func TestEveryWriteIsClassifiedForOOM(t *testing.T) {
	var missing []string
	for name, cmd := range commandTable {
		if !cmd.write {
			continue
		}
		if !oomClassified(cmd.lowerName) {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		t.Errorf("these write commands have no denyoom classification: %v\n"+
			"Add each to oomDenyOOM in maxmemory.go with the value redis 7.2 reports for it"+
			" (COMMAND INFO <name> -- look for `denyoom` in the flags array). true refuses"+
			" the command when the server is over its budget and cannot evict; false lets it"+
			" through, which is right only if it can never make the dataset larger.", missing)
	}
	// And nothing is classified that is not a command, which would be a stale entry left
	// behind by a rename.
	for name := range oomDenyOOM {
		if _, ok := commandTable[strings.ToUpper(name)]; !ok {
			t.Errorf("oomDenyOOM classifies %q, which is not a registered command", name)
		}
	}
}

// TestOOMRefusalUnderNoeviction is the headline behaviour: with a budget reached and a policy
// that will not evict, the writes that could grow the dataset are refused with Redis's exact
// text, while everything an operator needs in order to see the problem and recover from it
// keeps working.
//
// The recovery half is not a nicety. A server that refused reads would be unmonitorable, and
// one that refused DEL would be unrecoverable -- the operator's only way out of a full
// keyspace is to delete something.
func TestOOMRefusalUnderNoeviction(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	// Fill first, then set the budget below what is already held: that is the state an
	// operator reaches by lowering maxmemory, and it needs no guessing about how many keys
	// fit.
	for i := 0; i < 200; i++ {
		c.cmd("SET filler:" + strconv.Itoa(i) + " " + strings.Repeat("x", 512))
	}
	c.cmd("RPUSH mylist a b c")
	c.cmd("SADD myset a b c")
	c.cmd("HSET myhash f v")
	c.cmd("ZADD myzset 1 a")
	c.cmd("SET volatile v EX 1000")
	if got := c.cmd("CONFIG SET maxmemory-policy noeviction"); got != "+OK" {
		t.Fatalf("CONFIG SET maxmemory-policy noeviction = %q", got)
	}
	if got := c.cmd("CONFIG SET maxmemory 1kb"); got != "+OK" {
		t.Fatalf("CONFIG SET maxmemory 1kb = %q", got)
	}

	// Measured on redis 7.2 (maxmemory 3mb, noeviction, filled until it refused): this is
	// the error, byte for byte, and these are the commands that get it.
	const oom = "-OOM command not allowed when used memory > 'maxmemory'."
	refused := []string{
		"SET newkey v", "APPEND filler:1 x", "SETRANGE filler:1 0 x", "SETBIT filler:1 1 1",
		"INCR counter", "DECR counter", "INCRBYFLOAT fl 1.0", "SETNX nx v", "SETEX sx 10 v",
		"MSET a 1 b 2", "MSETNX mn 1", "GETSET filler:1 v", "COPY filler:1 copied",
		"LPUSH mylist v", "RPUSH mylist v", "LINSERT mylist BEFORE a z", "LSET mylist 0 zz",
		"RPOPLPUSH mylist other", "SADD myset m", "HSET myhash f2 v", "ZADD myzset 2 b",
		"PFADD hll a", "XADD stream * f v", "BITOP AND dest filler:1",
		"GEOADD geo 13.361389 38.115556 Palermo", "SORT mylist ALPHA STORE sorted",
	}
	for _, cmd := range refused {
		if got := c.cmd(cmd); got != oom {
			t.Errorf("%q while over the budget = %q; want %q", cmd, got, oom)
		}
	}

	// Reads keep working, so the problem stays visible.
	allowed := []struct{ cmd, wantPrefix string }{
		{"GET filler:1", "xxx"},
		{"EXISTS filler:1", ":1"},
		{"STRLEN filler:1", ":512"},
		{"LRANGE mylist 0 -1", "["},
		{"TTL volatile", ":"},
		{"DBSIZE", ":"},
		{"PING", "+PONG"},
		{"INFO memory", "used_memory"},
		// Measured on redis 7.2: each of these is allowed while over the limit, because
		// none of them can make the dataset larger. They are what recovery is made of.
		{"DEL filler:2", ":1"},
		{"UNLINK filler:3", ":1"},
		{"GETDEL filler:4", "xxx"},
		{"EXPIRE filler:5 100", ":1"},
		{"PERSIST volatile", ":1"},
		{"LPOP mylist", "a"},
		{"SREM myset a", ":1"},
		{"SPOP myset", ""},
		{"HDEL myhash f", ":1"},
		{"ZREM myzset a", ":1"},
		{"LTRIM mylist 0 0", "+OK"},
		{"RENAME filler:6 renamed", "+OK"},
		{"GETEX filler:7 EX 100", "xxx"},
		{"FLUSHDB", "+OK"},
	}
	for _, tc := range allowed {
		got := c.cmd(tc.cmd)
		if strings.HasPrefix(got, "-OOM") {
			t.Errorf("%q while over the budget was refused (%q); it must keep working or an"+
				" operator cannot recover", tc.cmd, got)
			continue
		}
		if tc.wantPrefix != "" && !strings.Contains(got, tc.wantPrefix) {
			t.Errorf("%q = %q; want something containing %q", tc.cmd, got, tc.wantPrefix)
		}
	}

	// FLUSHDB above emptied the keyspace, so the budget is met again and writes resume
	// without anything having to be reconfigured.
	if got := c.cmd("SET afterrecovery v"); got != "+OK" {
		t.Errorf("SET after recovering below the budget = %q; want +OK", got)
	}
	// And lifting the budget also lifts the refusal immediately.
	c.cmd("CONFIG SET maxmemory 1kb")
	for i := 0; i < 200; i++ {
		c.cmd("SET refill:" + strconv.Itoa(i) + " " + strings.Repeat("x", 512))
	}
	if got := c.cmd("CONFIG SET maxmemory 0"); got != "+OK" {
		t.Fatalf("CONFIG SET maxmemory 0 = %q", got)
	}
	if got := c.cmd("SET afterlifting v"); got != "+OK" {
		t.Errorf("SET after CONFIG SET maxmemory 0 = %q; want +OK", got)
	}
}

// TestOOMInsideMultiAborts covers the measured MULTI behaviour, which is stricter than the
// gate outside a transaction: queuing is itself unbounded memory growth, so Redis refuses
// *every* queued command while over the limit -- a GET included -- and lets EXEC abort.
//
// Measured on redis 7.2 over a full keyspace: MULTI is +OK, `SET x 1` and `GET k:1` both
// answer the OOM error, and EXEC answers EXECABORT.
func TestOOMInsideMultiAborts(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	for i := 0; i < 100; i++ {
		c.cmd("SET filler:" + strconv.Itoa(i) + " " + strings.Repeat("x", 512))
	}
	c.cmd("CONFIG SET maxmemory-policy noeviction")
	c.cmd("CONFIG SET maxmemory 1kb")

	if got := c.cmd("MULTI"); got != "+OK" {
		t.Fatalf("MULTI = %q; want +OK", got)
	}
	const oom = "-OOM command not allowed when used memory > 'maxmemory'."
	if got := c.cmd("SET x 1"); got != oom {
		t.Errorf("SET inside MULTI while over the budget = %q; want %q", got, oom)
	}
	if got := c.cmd("GET filler:1"); got != oom {
		t.Errorf("GET inside MULTI while over the budget = %q; want %q (queuing is itself"+
			" memory growth, so Redis refuses reads too)", got, oom)
	}
	if got := c.cmd("EXEC"); !strings.HasPrefix(got, "-EXECABORT") {
		t.Errorf("EXEC after refused queuing = %q; want EXECABORT", got)
	}
	// DISCARD is a connection-control command and runs before the gate, so an operator can
	// always abandon the batch.
	c.cmd("MULTI")
	if got := c.cmd("DISCARD"); got != "+OK" {
		t.Errorf("DISCARD while over the budget = %q; want +OK", got)
	}
}

// TestEvictionMeetsTheBudget drives the policies that do evict and checks the three things
// that make eviction observable: the keyspace comes back under the budget, evicted_keys
// counts what happened, and used_memory falls.
func TestEvictionMeetsTheBudget(t *testing.T) {
	for _, policy := range []string{
		"allkeys-lru", "allkeys-lfu", "allkeys-random", "volatile-lru", "volatile-lfu",
		"volatile-random", "volatile-ttl",
	} {
		t.Run(policy, func(t *testing.T) {
			addr, stop := startTestServer(t)
			defer stop()
			c := dialTx(t, addr)
			defer c.close()

			if got := c.cmd("CONFIG SET maxmemory-policy " + policy); got != "+OK" {
				t.Fatalf("CONFIG SET maxmemory-policy %s = %q", policy, got)
			}
			// Every key gets a TTL so the volatile-* policies have candidates. Without one
			// they would correctly refuse instead -- which is what
			// TestVolatilePolicyRefusesWithoutCandidates covers.
			for i := 0; i < 400; i++ {
				c.cmd("SET k:" + strconv.Itoa(i) + " " + strings.Repeat("x", 256) + " EX 10000")
			}
			budget := mustInt(t, c.cmd("MEMORY USAGE k:1")) * 100
			if got := c.cmd("CONFIG SET maxmemory " + strconv.FormatInt(budget, 10)); got != "+OK" {
				t.Fatalf("CONFIG SET maxmemory = %q", got)
			}
			// The budget is enforced on the write path, as in Redis, so it takes a write to
			// bring the keyspace down to it.
			if got := c.cmd("SET trigger v EX 10000"); got != "+OK" {
				t.Fatalf("SET after lowering the budget = %q; want +OK (this policy evicts)", got)
			}
			used := infoInt(t, c, "memory", "used_memory")
			if used > budget {
				t.Errorf("used_memory %d is still over the budget %d after a write", used, budget)
			}
			if evicted := infoInt(t, c, "stats", "evicted_keys"); evicted == 0 {
				t.Error("evicted_keys = 0, but the keyspace was brought down to the budget")
			}
			if n := mustInt(t, c.cmd("DBSIZE")); n >= 401 {
				t.Errorf("DBSIZE = %d; want fewer than the 401 keys written", n)
			}
		})
	}
}

// TestVolatilePolicyRefusesWithoutCandidates is the other half of the volatile-* contract,
// and the failure it guards against is the one worth spelling out: a volatile policy that
// evicted a key with no TTL would silently destroy data an operator had marked permanent,
// which is precisely what choosing a volatile policy is meant to prevent. So with nothing
// volatile to take it must refuse the write instead -- and refuse it, rather than spin
// looking for a candidate that does not exist.
func TestVolatilePolicyRefusesWithoutCandidates(t *testing.T) {
	for _, policy := range []string{"volatile-lru", "volatile-lfu", "volatile-random", "volatile-ttl"} {
		t.Run(policy, func(t *testing.T) {
			addr, stop := startTestServer(t)
			defer stop()
			c := dialTx(t, addr)
			defer c.close()

			c.cmd("CONFIG SET maxmemory-policy " + policy)
			for i := 0; i < 300; i++ {
				c.cmd("SET permanent:" + strconv.Itoa(i) + " " + strings.Repeat("x", 256))
			}
			c.cmd("CONFIG SET maxmemory 1kb")

			const oom = "-OOM command not allowed when used memory > 'maxmemory'."
			if got := c.cmd("SET newkey v"); got != oom {
				t.Errorf("SET under %s with no volatile keys = %q; want %q", policy, got, oom)
			}
			// Nothing was taken. Not "almost nothing": a permanent key is not a candidate.
			if n := mustInt(t, c.cmd("DBSIZE")); n != 300 {
				t.Errorf("DBSIZE = %d; want 300 -- %s must not evict a key with no TTL", n, policy)
			}
			if got := infoInt(t, c, "stats", "evicted_keys"); got != 0 {
				t.Errorf("evicted_keys = %d under %s with no volatile keys; want 0", got, policy)
			}

			// Give exactly one key a deadline and the sampler must find it, even though it
			// is one candidate among 300 spread over 256 shards. This is the case the
			// sampler used to lose: it drew random shards, so it missed the one shard
			// holding the candidate often enough to refuse writes while a key it was
			// allowed to take was sitting there.
			//
			// The write is still refused, and that is measured rather than a compromise:
			// on redis 7.2 in exactly this state (300 keys of 256 bytes, maxmemory 1kb,
			// volatile-lru, one key given a TTL) `SET newkey v` answers the OOM error,
			// DBSIZE falls from 300 to 299, and evicted_keys becomes 1. Redis evicts what
			// it can and then refuses if that was not enough -- freeing one key does not
			// bring 300 of them under a kilobyte. So the assertion is about the eviction,
			// not about the reply.
			c.cmd("EXPIRE permanent:7 10000")
			c.cmd("SET newkey v")
			if mustInt(t, c.cmd("EXISTS permanent:7")) != 0 {
				t.Errorf("under %s the one volatile key should have been taken; the sampler"+
					" must find a candidate that exists rather than giving up on it", policy)
			}
			if got := infoInt(t, c, "stats", "evicted_keys"); got != 1 {
				t.Errorf("evicted_keys = %d under %s with one volatile key; want 1", got, policy)
			}
			if n := mustInt(t, c.cmd("DBSIZE")); n != 299 {
				t.Errorf("DBSIZE = %d; want 299 -- exactly the one volatile key should have"+
					" gone, and no permanent key with it", n)
			}
		})
	}
}

// waitForSize waits until a server's keyspace has settled at n keys.
func waitForSize(t *testing.T, c *txConn, n int) {
	t.Helper()
	waitFor(t, "the replica to hold "+strconv.Itoa(n)+" keys", func() bool {
		return mustInt(t, c.cmd("DBSIZE")) == int64(n)
	})
}

// TestEvictionIsPropagated is the replication half. An eviction is a write: it must be
// ordered, persisted and replicated like any other, or a master and its replica silently
// hold different datasets. It is propagated as the DEL of the key that went, because the
// choice of victim is not reproducible -- a replica running the same policy over the same
// keyspace would sample different keys (invariant 4).
func TestEvictionIsPropagated(t *testing.T) {
	master, mstop := startTestServer(t)
	defer mstop()
	replica, rstop := startTestServer(t)
	defer rstop()

	mc := dialTx(t, master)
	defer mc.close()
	rc := dialTx(t, replica)
	defer rc.close()

	for i := 0; i < 200; i++ {
		mc.cmd("SET k:" + strconv.Itoa(i) + " " + strings.Repeat("x", 256))
	}
	host, port, ok := strings.Cut(master, ":")
	if !ok {
		t.Fatalf("master address %q is not host:port", master)
	}
	rc.cmd("REPLICAOF " + host + " " + port)
	waitForSize(t, rc, 200)

	mc.cmd("CONFIG SET maxmemory-policy allkeys-lru")
	budget := mustInt(t, mc.cmd("MEMORY USAGE k:1")) * 50
	mc.cmd("CONFIG SET maxmemory " + strconv.FormatInt(budget, 10))
	mc.cmd("SET trigger v")

	masterSize := mustInt(t, mc.cmd("DBSIZE"))
	if masterSize >= 201 {
		t.Fatalf("the master evicted nothing: DBSIZE = %d", masterSize)
	}
	waitForSize(t, rc, int(masterSize))

	// The replica holds exactly the master's keys, not merely the same number of them. That
	// is the property a synthetic DEL per eviction buys: had the replica evicted on its own
	// it would have sampled different victims and the two datasets would differ while both
	// looked internally consistent.
	for _, k := range keysOf(t, mc) {
		if mustInt(t, rc.cmd("EXISTS "+k)) != 1 {
			t.Errorf("the master holds %q but the replica does not", k)
		}
	}
	for _, k := range keysOf(t, rc) {
		if mustInt(t, mc.cmd("EXISTS "+k)) != 1 {
			t.Errorf("the replica holds %q but the master does not", k)
		}
	}

	// And a replica does not evict on its own account, even with a budget of its own: its
	// master drives what it holds. Redis draws the same line
	// (replica-ignore-maxmemory, on by default).
	rc.cmd("CONFIG SET maxmemory-policy allkeys-lru")
	rc.cmd("CONFIG SET maxmemory 1kb")
	before := mustInt(t, rc.cmd("DBSIZE"))
	mc.cmd("SET after-budget v")
	waitForSize(t, rc, int(before)+1)
	if got := infoInt(t, rc, "stats", "evicted_keys"); got != 0 {
		t.Errorf("the replica evicted %d keys of its own accord; a replica must follow its"+
			" master's choices or the two datasets diverge silently", got)
	}
}

// TestEvictionFiresKeyspaceNotification checks that the existing notification still fires,
// since eviction now has a second trigger. The event is "evicted" on the key that went.
func TestEvictionFiresKeyspaceNotification(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()
	c.cmd("CONFIG SET notify-keyspace-events KEA")
	c.cmd("CONFIG SET maxmemory-policy allkeys-lru")

	sub := dialTx(t, addr)
	defer sub.close()
	sub.cmd("PSUBSCRIBE __keyevent@0__:evicted")

	for i := 0; i < 200; i++ {
		c.cmd("SET k:" + strconv.Itoa(i) + " " + strings.Repeat("x", 256))
	}
	budget := mustInt(t, c.cmd("MEMORY USAGE k:1")) * 50
	c.cmd("CONFIG SET maxmemory " + strconv.FormatInt(budget, 10))
	c.cmd("SET trigger v")

	msg := nextMessage(t, sub)
	if !strings.Contains(msg, "evicted") {
		t.Errorf("first message after eviction = %q; want an __keyevent@0__:evicted delivery", msg)
	}
}

// TestMaxmemoryOperandForms pins CONFIG SET maxmemory against the operand forms redis 7.2
// accepts and refuses. Every expectation here was measured; the surprising ones are marked,
// because each of them is a value a reasonable implementation would have got wrong in the
// other direction.
func TestMaxmemoryOperandForms(t *testing.T) {
	cases := []struct {
		in   string
		want int64
		ok   bool
	}{
		// Measured on redis 7.2: the two suffix families are not the same multiplier.
		{"100mb", 100 * 1024 * 1024, true}, // 104857600
		{"1gb", 1024 * 1024 * 1024, true},  // 1073741824
		{"1kb", 1024, true},
		{"1KB", 1024, true}, // measured: case-insensitive
		{"100MB", 100 * 1024 * 1024, true},
		{"1k", 1000, true}, // measured: `k` is 1000, `kb` is 1024
		{"10m", 10000000, true},
		{"2g", 2000000000, true},
		{"1b", 1, true},
		{"1048576", 1048576, true},
		{"0", 0, true},
		// Measured, and genuinely surprising: redis 7.2 accepts the empty string and reads
		// it back as 0.
		{"", 0, true},
		{"9223372036854775807", 9223372036854775807, true},
		// Measured refusals. `-1` is the one that matters: an operator reaches for it
		// expecting "no limit", and Redis rejects it -- so reading it as 0 would remove a
		// limit with a command that looked like it had failed.
		{"-1", 0, false},
		{"abc", 0, false},
		{"100mbb", 0, false},
		{"1.5mb", 0, false},
		{"100 mb", 0, false},
		{"+100mb", 0, false},
		// Deliberate divergence, documented on parseMemorySize: redis 7.2 holds maxmemory
		// as an unsigned long long and clamps this to 18446744073709551615, where this
		// refuses anything past the int64 range. A budget above 8 exabytes is not a limit
		// any deployment sets, and refusing is the side to err on.
		{"99999999999999999999", 0, false},
	}
	for _, tc := range cases {
		got, ok := parseMemorySize(tc.in)
		if ok != tc.ok || got != tc.want {
			t.Errorf("parseMemorySize(%q) = (%d, %v); want (%d, %v)",
				tc.in, got, ok, tc.want, tc.ok)
		}
	}
}

// TestMaxmemoryConfigRefusals pins the exact refusal text, which Redis words per setting
// rather than reusing its generic integer message -- a caller told the wrong reason looks in
// the wrong place.
func TestMaxmemoryConfigRefusals(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		// Measured on redis 7.2 for each of these operands.
		{"CONFIG SET maxmemory -1",
			"-ERR CONFIG SET failed (possibly related to argument 'maxmemory') - argument must be a memory value"},
		{"CONFIG SET maxmemory abc",
			"-ERR CONFIG SET failed (possibly related to argument 'maxmemory') - argument must be a memory value"},
		{"CONFIG SET maxmemory-policy allkeys_lru",
			"-ERR CONFIG SET failed (possibly related to argument 'maxmemory-policy') - argument(s) must be one of the following: volatile-lru, volatile-lfu, volatile-random, volatile-ttl, allkeys-lru, allkeys-lfu, allkeys-random, noeviction"},
		{"CONFIG SET maxmemory-samples 0",
			"-ERR CONFIG SET failed (possibly related to argument 'maxmemory-samples') - argument must be between 1 and 2147483647 inclusive"},
		{"CONFIG SET maxmemory-samples -1",
			"-ERR CONFIG SET failed (possibly related to argument 'maxmemory-samples') - argument must be between 1 and 2147483647 inclusive"},
		// Redis distinguishes the two kinds of wrong, and this pair is why: an operator who
		// typed a letter must not be told their number was out of range. Both strings
		// measured on redis 7.2.
		{"CONFIG SET maxmemory-samples abc",
			"-ERR CONFIG SET failed (possibly related to argument 'maxmemory-samples') - argument couldn't be parsed into an integer"},
		{"CONFIG SET lfu-log-factor abc",
			"-ERR CONFIG SET failed (possibly related to argument 'lfu-log-factor') - argument couldn't be parsed into an integer"},
		{"CONFIG SET lfu-decay-time xyz",
			"-ERR CONFIG SET failed (possibly related to argument 'lfu-decay-time') - argument couldn't be parsed into an integer"},
		{"CONFIG SET lfu-log-factor -1",
			"-ERR CONFIG SET failed (possibly related to argument 'lfu-log-factor') - argument must be between 0 and 2147483647 inclusive"},
		{"CONFIG SET lfu-decay-time -1",
			"-ERR CONFIG SET failed (possibly related to argument 'lfu-decay-time') - argument must be between 0 and 2147483647 inclusive"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%s\n  got  %q\n  want %q", tc.cmd, got, tc.want)
		}
	}
	// Measured: 1000 is accepted, so the documented 1..64 range is not the real bound.
	if got := c.cmd("CONFIG SET maxmemory-samples 1000"); got != "+OK" {
		t.Errorf("CONFIG SET maxmemory-samples 1000 = %q; want +OK (redis 7.2 accepts it)", got)
	}
	c.cmd("CONFIG SET maxmemory-samples 16")
}

// TestUsedMemoryIsTheDatasetAndAgreesWithMemoryUsage ties INFO's number to the per-key
// number, because two estimates of one fact are how they drift. used_memory must be the sum
// of what MEMORY USAGE reports for every key -- the same estimator, not a second one.
func TestUsedMemoryIsTheDatasetAndAgreesWithMemoryUsage(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	if got := infoInt(t, c, "memory", "used_memory"); got != 0 {
		t.Errorf("used_memory on an empty server = %d; want 0", got)
	}
	c.cmd("SET s " + strings.Repeat("x", 1000))
	c.cmd("RPUSH l a b c d e")
	c.cmd("HSET h f1 v1 f2 v2")
	c.cmd("SADD st a b c")
	c.cmd("ZADD z 1 a 2 b")

	var sum int64
	for _, k := range keysOf(t, c) {
		sum += mustInt(t, c.cmd("MEMORY USAGE "+k))
	}
	used := infoInt(t, c, "memory", "used_memory")
	if used != sum {
		t.Errorf("used_memory = %d but the sum of MEMORY USAGE over every key is %d;"+
			" they must be the same estimator", used, sum)
	}
	// maxmemory and its policy agree between INFO and CONFIG GET, which is the property that
	// stops a client finding the server contradicting itself.
	c.cmd("CONFIG SET maxmemory 100mb")
	c.cmd("CONFIG SET maxmemory-policy volatile-ttl")
	if got := infoField(t, c, "memory", "maxmemory"); got != "104857600" {
		t.Errorf("INFO maxmemory = %q; want 104857600", got)
	}
	if got := infoField(t, c, "memory", "maxmemory_human"); got != "100.00M" {
		t.Errorf("INFO maxmemory_human = %q; want 100.00M", got)
	}
	if got := infoField(t, c, "memory", "maxmemory_policy"); got != "volatile-ttl" {
		t.Errorf("INFO maxmemory_policy = %q; want volatile-ttl", got)
	}
	_, v := parseFlatMap(t, c.cmd("CONFIG GET maxmemory-policy"))
	if v["maxmemory-policy"] != infoField(t, c, "memory", "maxmemory_policy") {
		t.Errorf("CONFIG GET maxmemory-policy = %q but INFO says %q",
			v["maxmemory-policy"], infoField(t, c, "memory", "maxmemory_policy"))
	}
	c.cmd("CONFIG SET maxmemory 0")
}

// TestObjectFreqAndIdletimeFollowThePolicy pins OBJECT FREQ and OBJECT IDLETIME against the
// measured Redis behaviour: each is answered under the policy family that tracks it and
// refused under the other, because the two share one field.
func TestObjectFreqAndIdletimeFollowThePolicy(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()
	c.cmd("CONFIG SET maxmemory 100mb")

	const freqRefusal = "-ERR An LFU maxmemory policy is not selected, access frequency not tracked. " +
		"Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust."
	const idleRefusal = "-ERR An LFU maxmemory policy is selected, idle time not tracked. " +
		"Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust."

	c.cmd("CONFIG SET maxmemory-policy allkeys-lru")
	c.cmd("SET k v")
	if got := c.cmd("OBJECT FREQ k"); got != freqRefusal {
		t.Errorf("OBJECT FREQ under allkeys-lru = %q; want the measured refusal", got)
	}
	if got := c.cmd("OBJECT IDLETIME k"); got != ":0" {
		t.Errorf("OBJECT IDLETIME under allkeys-lru = %q; want :0", got)
	}

	c.cmd("CONFIG SET maxmemory-policy allkeys-lfu")
	c.cmd("SET fresh v")
	// Measured on redis 7.2: a key just written under an LFU policy reads 5, not 0. A
	// counter starting at zero would make every new key the most attractive victim.
	if got := c.cmd("OBJECT FREQ fresh"); got != ":5" {
		t.Errorf("OBJECT FREQ on a fresh key = %q; want :5 (Redis's LFU_INIT_VAL)", got)
	}
	if got := c.cmd("OBJECT IDLETIME fresh"); got != idleRefusal {
		t.Errorf("OBJECT IDLETIME under allkeys-lfu = %q; want the measured refusal", got)
	}
	// Measured: a missing key is a null for both, not an error.
	if got := c.cmd("OBJECT FREQ nosuchkey"); got != "(nil)" {
		t.Errorf("OBJECT FREQ on a missing key = %q; want (nil)", got)
	}
	c.cmd("CONFIG SET maxmemory 0")
	c.cmd("CONFIG SET maxmemory-policy noeviction")
}

// TestMaxmemoryZeroCostsNothing is the "free path" claim, measured rather than asserted: with
// no budget configured the whole feature is one atomic load, so it allocates nothing on the
// command path.
//
// It finishes by *enabling* a budget and checking the refusal appears, so that what was
// measured is a disabled gate and not a broken one -- the same shape as
// TestObservabilityCostsNothingWhenUnused.
func TestMaxmemoryZeroCostsNothing(t *testing.T) {
	st := store.New(16)
	srv := New(st)
	if err := srv.SetDatabases(16); err != nil {
		t.Fatalf("SetDatabases: %v", err)
	}
	cmd := commandTable["SET"]
	w := resp.NewWriter(io.Discard)

	if n := testing.AllocsPerRun(200, func() {
		srv.oomGate(w, cmd, nil)
	}); n != 0 {
		t.Errorf("oomGate allocated %v times per call with maxmemory 0; want 0", n)
	}
	if srv.MaxMemory() != 0 {
		t.Fatal("the default budget should be 0")
	}
	// The gate is genuinely doing its job once a budget is set, so the zero above measured a
	// disabled path rather than a dead one.
	st.Set("k", make([]byte, 4096), 0)
	srv.SetEvictionPolicy(store.PolicyNoEviction)
	srv.SetMaxMemory(64)
	if srv.oomGate(w, cmd, nil) {
		t.Error("oomGate allowed a denyoom write while over the budget under noeviction")
	}
	if !srv.oomGate(w, commandTable["DEL"], nil) {
		t.Error("oomGate refused DEL while over the budget; recovery must stay possible")
	}
	if !srv.oomGate(w, commandTable["GET"], nil) {
		t.Error("oomGate refused a read while over the budget")
	}
}
