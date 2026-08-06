package server

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

// TestSlowlog covers the policy, the ring's bound, and the two subcommands that
// report it. The threshold is set to 0 so every command qualifies, which is what makes
// the assertions deterministic -- a test that relied on a command genuinely taking
// 10ms would be a test of the machine it runs on.
func TestSlowlog(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	// Nothing is slow by default, which is the point: the default threshold is 10ms.
	if got := c.cmd("SLOWLOG LEN"); got != ":0" {
		t.Errorf("SLOWLOG LEN on a fresh server = %q; want :0", got)
	}
	if got := c.cmd("CONFIG GET slowlog-log-slower-than"); got != "[slowlog-log-slower-than 10000]" {
		t.Errorf("CONFIG GET slowlog-log-slower-than = %q", got)
	}

	c.cmd("CONFIG SET slowlog-log-slower-than 0")
	c.cmd("SET foo bar")
	if got := c.cmd("SLOWLOG LEN"); got == ":0" {
		t.Fatal("SLOWLOG LEN is still 0 after lowering the threshold to 0")
	}
	got := c.cmd("SLOWLOG GET 1")
	// [[id timestamp usec [args...] addr name]]
	//
	// SLOWLOG GET returns newest first, so the single entry asked for is the command
	// immediately before this one -- the SLOWLOG LEN above, not the SET. Verified against
	// redis:7.2-alpine, which answers the same entry for the same sequence.
	//
	// This was two negations joined by &&, which is an OR of the negations: it failed only
	// if *neither* command appeared, and "SLOWLOG LEN" always does. A slow log that
	// returned the oldest entry, or the wrong entry, or one entry of a hundred, all passed.
	if !strings.Contains(got, "SLOWLOG LEN") {
		t.Errorf("SLOWLOG GET 1 = %q; want the most recent command (SLOWLOG LEN)", got)
	}
	if strings.Contains(got, "SET foo bar") {
		t.Errorf("SLOWLOG GET 1 = %q; want one entry, newest first, not the older SET", got)
	}
	if !strings.Contains(got, "127.0.0.1:") {
		t.Errorf("SLOWLOG GET 1 = %q; want the client address in the entry", got)
	}

	// The ring is bounded, and shrinking the bound truncates what is already held.
	c.cmd("CONFIG SET slowlog-max-len 2")
	c.cmd("SET a 1")
	c.cmd("SET b 2")
	c.cmd("SET c 3")
	if got := c.cmd("SLOWLOG LEN"); got != ":2" {
		t.Errorf("SLOWLOG LEN with max-len 2 = %q; want :2", got)
	}
	if got := c.cmd("SLOWLOG RESET"); got != "+OK" {
		t.Errorf("SLOWLOG RESET = %q", got)
	}
	// RESET is itself logged (threshold 0), so the log holds exactly it afterwards.
	if got := c.cmd("SLOWLOG LEN"); got != ":1" {
		t.Errorf("SLOWLOG LEN after RESET = %q; want :1 (the RESET itself)", got)
	}

	c.cmd("CONFIG SET slowlog-log-slower-than 10000")
	c.cmd("SLOWLOG RESET")
	if got := c.cmd("SLOWLOG HELP"); !strings.Contains(got, "GET [<count>]") {
		t.Errorf("SLOWLOG HELP = %q", got)
	}
	// redis:7.2 spells every container's unknown subcommand this one way; the old
	// per-container wording was this server's own.
	if got := c.cmd("SLOWLOG NOPE"); got != "-ERR unknown subcommand 'NOPE'. Try SLOWLOG HELP." {
		t.Errorf("SLOWLOG NOPE = %q", got)
	}
}

// TestSlowlogTruncatesLongArguments pins the rule that keeps the log from becoming a
// second copy of the dataset.
func TestSlowlogTruncatesLongArguments(t *testing.T) {
	s := New(store.New(4))
	s.SetSlowlogPolicy(0, 16)
	long := strings.Repeat("x", 500)
	s.recordSlowlog(nil, cmdArgs("SET", "k", long), time.Millisecond)

	s.slowlogMu.Lock()
	entry := s.slowlog[0]
	s.slowlogMu.Unlock()
	if len(entry.args) != 3 {
		t.Fatalf("entry has %d arguments; want 3", len(entry.args))
	}
	if len(entry.args[2]) >= len(long) {
		t.Errorf("the long argument was not truncated (%d bytes)", len(entry.args[2]))
	}
	if !strings.HasSuffix(entry.args[2], "(372 more bytes)") {
		t.Errorf("truncated argument = %q; want a trailing byte count", entry.args[2])
	}

	// And the argument count is bounded too, with a synthetic final element saying by
	// how much, so a 100-argument MSET does not pin 100 strings.
	many := make([]string, 0, 101)
	many = append(many, "MSET")
	for i := 0; i < 100; i++ {
		many = append(many, "k")
	}
	s.recordSlowlog(nil, cmdArgs(many...), time.Millisecond)
	s.slowlogMu.Lock()
	entry = s.slowlog[0]
	s.slowlogMu.Unlock()
	if len(entry.args) != slowlogMaxArgs {
		t.Fatalf("entry has %d arguments; want %d", len(entry.args), slowlogMaxArgs)
	}
	if !strings.Contains(entry.args[slowlogMaxArgs-1], "more arguments") {
		t.Errorf("last argument = %q; want a count of the dropped ones", entry.args[slowlogMaxArgs-1])
	}
}

// TestMonitorEchoesCommands checks that a monitoring connection receives what other
// clients run, and -- the part that matters -- that it never receives a credential.
func TestMonitorEchoesCommands(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	mon := dialTx(t, addr)
	defer mon.close()
	if got := mon.cmd("MONITOR"); got != "+OK" {
		t.Fatalf("MONITOR = %q; want +OK", got)
	}

	c := dialTx(t, addr)
	defer c.close()
	c.cmd("SET greeting hello")

	line := readReply(t, mon.br)
	if !strings.Contains(line, `"SET" "greeting" "hello"`) {
		t.Errorf("monitor line = %q; want the SET it echoed", line)
	}
	if !strings.Contains(line, "[0 127.0.0.1:") {
		t.Errorf("monitor line = %q; want the database and client address", line)
	}

	// A credential must never reach a monitor. Both commands that carry one are checked.
	c.cmd("AUTH hunter2")
	if line := readReply(t, mon.br); strings.Contains(line, "hunter2") {
		t.Errorf("monitor leaked an AUTH password: %q", line)
	} else if !strings.Contains(line, "(redacted)") {
		t.Errorf("AUTH monitor line = %q; want (redacted)", line)
	}
	c.cmd("HELLO 2 AUTH default hunter2")
	if line := readReply(t, mon.br); strings.Contains(line, "hunter2") {
		t.Errorf("monitor leaked a HELLO AUTH password: %q", line)
	}
	c.cmd("CONFIG SET requirepass hunter2")
	if line := readReply(t, mon.br); strings.Contains(line, "hunter2") {
		t.Errorf("monitor leaked a CONFIG SET requirepass value: %q", line)
	}
	// Leave the server open again: this connection is still authenticated (it connected
	// before the password existed), so it is the one that can undo it.
	c.cmdRaw("CONFIG", "SET", "requirepass", "")
}

// TestMonitorDropsASlowConsumer is the liveness half of the bounded-queue contract: a
// monitor that stops reading is hung up on rather than allowed to stall the command
// path. The queue is filled directly, because filling it over a socket would require
// writing monitorQueue commands faster than the pump drains them.
func TestMonitorDropsASlowConsumer(t *testing.T) {
	s := New(store.New(4))
	sess := s.newSession(nil)
	w := resp.NewWriter(io.Discard)
	cmdMonitor(s, sess, w, cmdArgs("MONITOR"))
	if s.monitorCount.Load() != 1 {
		t.Fatalf("monitorCount = %d after MONITOR; want 1", s.monitorCount.Load())
	}

	// No pump is running, so the queue fills and then the monitor is dropped -- and
	// crucially, feedMonitors never blocks while that happens.
	other := s.newSession(nil)
	for i := 0; i < monitorQueue+10; i++ {
		s.feedMonitors(other, 0, cmdArgs("SET", "k", "v"))
	}
	select {
	case <-sess.monitor.dropped:
	default:
		t.Fatal("a monitor that never drained its queue was not dropped")
	}
	if s.monitorDrops.Load() != 1 {
		t.Errorf("monitor_dropped_clients = %d; want 1", s.monitorDrops.Load())
	}

	s.detachMonitor(sess)
	if s.monitorCount.Load() != 0 {
		t.Errorf("monitorCount = %d after detach; want 0", s.monitorCount.Load())
	}
}

// TestLatencyMonitor drives the one event this server records, using DEBUG SLEEP to
// produce a spike of a known size.
func TestLatencyMonitor(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	if got := c.cmd("LATENCY HISTORY command"); got != "[]" {
		t.Errorf("LATENCY HISTORY on a fresh server = %q; want []", got)
	}
	if got := c.cmd("LATENCY LATEST"); got != "[]" {
		t.Errorf("LATENCY LATEST on a fresh server = %q; want []", got)
	}

	c.cmd("CONFIG SET latency-monitor-threshold 10")
	c.cmd("DEBUG SLEEP 0.05")

	got := c.cmd("LATENCY HISTORY command")
	if got == "[]" {
		t.Fatalf("LATENCY HISTORY = %q; want a sample after a 50ms command", got)
	}
	if latest := c.cmd("LATENCY LATEST"); !strings.HasPrefix(latest, "[[command ") {
		t.Errorf("LATENCY LATEST = %q; want a command entry", latest)
	}
	if got := c.cmd("LATENCY RESET"); got != ":1" {
		t.Errorf("LATENCY RESET = %q; want :1 (one series discarded)", got)
	}
	if got := c.cmd("LATENCY HISTORY command"); got != "[]" {
		t.Errorf("LATENCY HISTORY after RESET = %q; want []", got)
	}
	if got := c.cmd("LATENCY RESET"); got != ":0" {
		t.Errorf("a second LATENCY RESET = %q; want :0", got)
	}
}

// TestBlockedTimeIsNotSlow pins the rule that keeps every timed-out BLPOP out of the
// slow log: a client waiting is not the server being slow.
//
// The threshold is 250ms against a 500ms wait, which looks generous for something whose
// point is that a figure is *excluded*. It is deliberate. What session.blockedFor excludes
// is the park itself; what is left inside the measured window is the parse, the first
// non-blocking attempt, and -- the part that matters here -- however long this connection's
// goroutine happens to be off-CPU on either side of the park. That residue was measured on
// this test's own path: 12-137us on an idle machine, but 2.5ms at p90 and 48.9ms at worst
// with six concurrent -race runs of this package on eight cores. A 10ms threshold therefore
// failed on a loaded machine while the exclusion was working perfectly, which is a test
// reporting the scheduler rather than the server.
//
// So the margin is over the scheduler, not over the server, and it is sized from that
// measurement. It is not a weaker assertion: 500ms is still twice the threshold, so
// removing the exclusion fails the test just as it did before.
//
// The second half is the positive control TestObservabilityCostsNothingWhenUnused uses for
// the same reason -- a command that really is slow must still be recorded, or "nothing was
// logged" would pass for a slow log that had simply stopped working.
func TestBlockedTimeIsNotSlow(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("CONFIG SET slowlog-log-slower-than 250000")
	c.cmd("SLOWLOG RESET")
	if got := c.cmd("BLPOP nothing-here 0.5"); got != "(nil)" {
		t.Fatalf("BLPOP timed out with %q; want (nil)", got)
	}
	if got := c.cmd("SLOWLOG LEN"); got != ":0" {
		t.Errorf("a BLPOP that waited 500ms was recorded as slow (SLOWLOG LEN = %q)", got)
	}

	if got := c.cmd("DEBUG SLEEP 0.3"); got != "+OK" {
		t.Fatalf("DEBUG SLEEP = %q", got)
	}
	if got := c.cmd("SLOWLOG LEN"); got != ":1" {
		t.Errorf("a command that spent 300ms working was not recorded (SLOWLOG LEN = %q); "+
			"the BLPOP assertion above would have passed for a slow log that logs nothing", got)
	}
}

func TestMemoryUsageAndDoctor(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	if got := c.cmd("MEMORY USAGE missing"); got != "(nil)" {
		t.Errorf("MEMORY USAGE on a missing key = %q; want (nil)", got)
	}
	c.cmd("SET small v")
	c.cmdRaw("SET", "large", strings.Repeat("x", 4096))
	smallN := c.cmd("MEMORY USAGE small")
	largeN := c.cmd("MEMORY USAGE large")
	if !strings.HasPrefix(smallN, ":") || !strings.HasPrefix(largeN, ":") {
		t.Fatalf("MEMORY USAGE returned %q and %q; want integers", smallN, largeN)
	}
	small, large := mustInt(t, smallN), mustInt(t, largeN)
	if large <= small {
		t.Errorf("MEMORY USAGE: large=%d is not greater than small=%d", large, small)
	}
	if large < 4096 {
		t.Errorf("MEMORY USAGE large = %d; want at least the 4096 payload bytes", large)
	}
	// The estimate covers every type, and a sorted set costs more per member than a set
	// because each member sits in both the dict and the skip list.
	c.cmd("SADD st a b c d e")
	c.cmd("ZADD zt 1 a 2 b 3 c 4 d 5 e")
	if setN, zsetN := mustInt(t, c.cmd("MEMORY USAGE st")), mustInt(t, c.cmd("MEMORY USAGE zt")); zsetN <= setN {
		t.Errorf("MEMORY USAGE: zset=%d is not greater than set=%d", zsetN, setN)
	}

	if got := c.cmd("MEMORY USAGE small SAMPLES 0"); !strings.HasPrefix(got, ":") {
		t.Errorf("MEMORY USAGE ... SAMPLES 0 = %q", got)
	}
	if got := c.cmd("MEMORY USAGE small BOGUS 1"); got != "-ERR syntax error" {
		t.Errorf("MEMORY USAGE ... BOGUS = %q", got)
	}
	if got := c.cmd("MEMORY DOCTOR"); !strings.Contains(got, "Memory doctor report") {
		t.Errorf("MEMORY DOCTOR = %q", got)
	}
	if got := c.cmd("MEMORY DOCTOR"); !strings.Contains(got, "Eviction is disabled") {
		t.Errorf("MEMORY DOCTOR with no cap = %q; want the eviction warning", got)
	}
}

func mustInt(t *testing.T, reply string) int64 {
	t.Helper()
	if !strings.HasPrefix(reply, ":") {
		t.Fatalf("reply %q is not an integer", reply)
	}
	n, ok := parseInt64([]byte(reply[1:]))
	if !ok {
		t.Fatalf("reply %q is not an integer", reply)
	}
	return n
}

func TestCommandGetKeys(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		{"COMMAND GETKEYS GET foo", "[foo]"},
		{"COMMAND GETKEYS SET foo bar", "[foo]"},
		{"COMMAND GETKEYS MSET a 1 b 2", "[a b]"},
		{"COMMAND GETKEYS MGET a b c", "[a b c]"},
		{"COMMAND GETKEYS DEL a b", "[a b]"},
		{"COMMAND GETKEYS RENAME a b", "[a b]"},
		{"COMMAND GETKEYS LMOVE src dst LEFT RIGHT", "[src dst]"},
		{"COMMAND GETKEYS SINTERCARD 2 s1 s2", "[s1 s2]"},
		{"COMMAND GETKEYS LMPOP 2 l1 l2 LEFT", "[l1 l2]"},
		{"COMMAND GETKEYS BLMPOP 0 2 l1 l2 LEFT", "[l1 l2]"},
		{"COMMAND GETKEYS MEMORY USAGE k", "[k]"},
		{"COMMAND GETKEYS OBJECT ENCODING k", "[k]"},
		// The two error paths a client library relies on.
		{"COMMAND GETKEYS NOSUCHCOMMAND a", "-ERR Invalid command specified"},
		{"COMMAND GETKEYS PING", "-ERR The command has no key arguments"},
		{"COMMAND GETKEYS GET", "-ERR Invalid number of arguments specified for command"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}
}

// TestInfoCommandStats covers both of the optional sections and the rule that keeps
// them out of a bare INFO.
func TestInfoCommandStats(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	// The command table -- and so the statistics on it -- is process-global, which is
	// right for a server (one process serves one dataset) but means an earlier test in
	// this package has already called some of these. Zero them first so the assertions
	// below are about this test's own traffic.
	c.cmd("CONFIG RESETSTAT")
	c.cmd("SET k v")
	c.cmd("GET k")
	c.cmd("GET missing")
	c.cmd("INCR k") // fails: not an integer

	if got := c.cmd("INFO"); strings.Contains(got, "cmdstat_") {
		t.Error("a bare INFO includes commandstats; it must not")
	}
	stats := c.cmd("INFO commandstats")
	if !strings.Contains(stats, "cmdstat_set:calls=1") {
		t.Errorf("INFO commandstats = %q; want cmdstat_set:calls=1", stats)
	}
	if !strings.Contains(stats, "cmdstat_get:calls=2") {
		t.Errorf("INFO commandstats = %q; want cmdstat_get:calls=2", stats)
	}
	if !strings.Contains(stats, "failed_calls=1") {
		t.Errorf("INFO commandstats = %q; want the failed INCR counted", stats)
	}
	if !strings.Contains(stats, "usec_per_call=") {
		t.Errorf("INFO commandstats = %q; want usec_per_call", stats)
	}
	if lat := c.cmd("INFO latencystats"); !strings.Contains(lat, "latency_percentiles_usec_get:p50=") {
		t.Errorf("INFO latencystats = %q", lat)
	}
	// A rejected call is counted separately from a failed one.
	c.cmd("SLOWLOG")
	if stats := c.cmd("INFO commandstats"); !strings.Contains(stats, "cmdstat_slowlog:calls=0,usec=0,usec_per_call=0.00,rejected_calls=1") {
		t.Errorf("INFO commandstats = %q; want the arity rejection counted", stats)
	}
	// INFO all includes them; so does the explicit name.
	if got := c.cmd("INFO all"); !strings.Contains(got, "cmdstat_") {
		t.Error("INFO all omits commandstats")
	}
	if got := c.cmd("INFO default"); strings.Contains(got, "cmdstat_") {
		t.Error("INFO default includes commandstats; it must not")
	}
}

// TestKeyspaceHitsAndMisses is the counting-choice test: a read that found its key is a
// hit, a read that did not is a miss, and a *write* is neither.
func TestKeyspaceHitsAndMisses(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	hits, misses := keyspaceStats(t, c)
	if hits != 0 || misses != 0 {
		t.Fatalf("fresh server reports %d hits and %d misses", hits, misses)
	}

	// Writes only. If the counters lived in the shared lookup helper, these would
	// register as one miss (creating the key) and one hit (overwriting it) -- which is
	// the bug this test exists to prevent.
	c.cmd("SET k v")
	c.cmd("SET k v2")
	c.cmd("LPUSH l a")
	c.cmd("HSET h f v")
	c.cmd("SADD s m")
	c.cmd("ZADD z 1 m")
	c.cmd("DEL nothing")
	hits, misses = keyspaceStats(t, c)
	if hits != 0 || misses != 0 {
		t.Errorf("writes were counted as reads: %d hits, %d misses", hits, misses)
	}

	c.cmd("GET k")       // hit
	c.cmd("GET missing") // miss
	hits, misses = keyspaceStats(t, c)
	if hits != 1 || misses != 1 {
		t.Errorf("after one hit and one miss: %d hits, %d misses", hits, misses)
	}

	// Every type's read path is wired to the same helper.
	c.cmd("LRANGE l 0 -1")
	c.cmd("HGET h f")
	c.cmd("SMEMBERS s")
	c.cmd("ZSCORE z m")
	c.cmd("LRANGE gone 0 -1")
	hits, misses = keyspaceStats(t, c)
	if hits != 5 || misses != 2 {
		t.Errorf("after the per-type reads: %d hits, %d misses; want 5 and 2", hits, misses)
	}

	// MGET counts one lookup per key, as Redis does.
	c.cmd("MGET k missing1 missing2")
	hits, misses = keyspaceStats(t, c)
	if hits != 6 || misses != 4 {
		t.Errorf("after MGET: %d hits, %d misses; want 6 and 4", hits, misses)
	}

	// And they are statistics, so CONFIG RESETSTAT clears them.
	c.cmd("CONFIG RESETSTAT")
	if hits, misses = keyspaceStats(t, c); hits != 0 || misses != 0 {
		t.Errorf("CONFIG RESETSTAT left %d hits and %d misses", hits, misses)
	}
}

// keyspaceStats reads the two counters out of INFO stats.
func keyspaceStats(t *testing.T, c *txConn) (hits, misses int64) {
	t.Helper()
	for _, line := range strings.Split(c.cmd("INFO stats"), "\r\n") {
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		n, _ := parseInt64([]byte(value))
		switch name {
		case "keyspace_hits":
			hits = n
		case "keyspace_misses":
			misses = n
		}
	}
	return hits, misses
}

// TestObservabilityCostsNothingWhenUnused is the hot-path check, the counterpart of
// the keyspace-notification one. Every observability hook runs on every command, so
// each has to allocate nothing: the always-on part (per-command statistics) because it
// is always on, and the opt-in parts (the slow log, the latency monitor, MONITOR)
// because a server that asked for none of them must not pay for them.
func TestObservabilityCostsNothingWhenUnused(t *testing.T) {
	s := New(store.New(4))
	s.SetSlowlogPolicy(-1, 128) // disabled, as a negative threshold means
	s.SetLatencyThresholdMs(0)  // disabled, the default
	cmd := commandTable["GET"]  // any table entry; the statistics live on it
	args := cmdArgs("GET", "k") //
	w := resp.NewWriter(io.Discard)
	sess := s.newSession(nil)

	// commandstats: three atomic adds on the *command the dispatcher already holds.
	if allocs := testing.AllocsPerRun(200, func() {
		cmd.record(time.Microsecond, false)
	}); allocs != 0 {
		t.Errorf("command.record allocates %v times per call; want 0", allocs)
	}

	// The whole hook, with every opt-in feature off.
	if allocs := testing.AllocsPerRun(200, func() {
		start, errs := observeStart(w)
		s.observeCommand(sess, cmd, args, start, errs, w)
	}); allocs != 0 {
		t.Errorf("observeCommand allocates %v times per call when nothing is enabled; want 0", allocs)
	}

	// MONITOR: one atomic load with nobody attached.
	if allocs := testing.AllocsPerRun(200, func() {
		s.feedMonitors(sess, 0, args)
	}); allocs != 0 {
		t.Errorf("feedMonitors allocates %v times per call with no monitors; want 0", allocs)
	}

	// And the counters really were being maintained, which is what proves the checks
	// above measured a disabled feature rather than a broken one.
	if cmd.calls.Load() == 0 {
		t.Error("no calls were recorded, so the allocation checks measured nothing")
	}
	if s.slowlogThreshold.Load() >= 0 {
		t.Error("the slow log was not actually disabled during the check")
	}
	s.SetSlowlogPolicy(0, 128)
	start, errs := observeStart(w)
	s.observeCommand(sess, cmd, args, start, errs, w)
	s.slowlogMu.Lock()
	n := len(s.slowlog)
	s.slowlogMu.Unlock()
	if n != 1 {
		t.Errorf("enabling the slow log recorded %d entries; want 1", n)
	}
}

// TestKeyspaceReadHelperIsNotOnTheWritePath is the store-level statement of the same
// counting rule, checked directly against the store so it cannot be masked by which
// commands the server happens to route where.
func TestKeyspaceReadHelperIsNotOnTheWritePath(t *testing.T) {
	st := store.New(4)
	st.Set("k", []byte("v"), 0)
	st.LPush("l", []byte("a"))
	if _, err := st.HSet("h", [2][]byte{[]byte("f"), []byte("v")}); err != nil {
		t.Fatal(err)
	}
	if h, m := st.KeyspaceHits(), st.KeyspaceMisses(); h != 0 || m != 0 {
		t.Fatalf("writes recorded %d hits and %d misses; want none", h, m)
	}
	if _, _, err := st.GetString("k"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.GetString("nope"); err != nil {
		t.Fatal(err)
	}
	if h, m := st.KeyspaceHits(), st.KeyspaceMisses(); h != 1 || m != 1 {
		t.Errorf("one hit and one miss recorded as %d and %d", h, m)
	}
}
