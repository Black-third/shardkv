package server

import (
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Black-third/shardkv/internal/aof"
	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

// advancingClock returns a store clock that moves forward by step on *every reading*.
//
// A frozen clock cannot see the defect this file exists to pin: it makes two readings
// taken at two different moments compare equal, which is precisely the assumption that
// was wrong. Under an advancing clock any second reading differs from the first by
// exactly one step, so "the deadline in memory equals the deadline on the wire" becomes
// an exact equality rather than a measurement of how long a handler happened to take.
//
// It is local to this test on purpose: the rest of the suite freezes the clock so that
// TTL arithmetic is stable, and changing the default would break those.
func advancingClock(start time.Time, step time.Duration) func() time.Time {
	var mu sync.Mutex
	cur := start
	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		cur = cur.Add(step)
		return cur
	}
}

// clockStep is one tick of the advancing clock: the exact skew a second clock reading
// introduces.
const clockStep = 50 * time.Millisecond

// expiryClockCase is one command that carries an expiry, plus where the deadline sits in
// the command it propagates as.
type expiryClockCase struct {
	name  string
	key   string
	setup [][]string
	cmd   []string
	// deadlineAt is the argument index of the absolute deadline in the propagated form:
	// 2 for PEXPIREAT/RESTORE, 4 for SET ... PXAT.
	deadlineAt int
	// relative says the command's operand counts from now, so it is one of the forms the
	// skew could reach. The absolute forms are listed as controls and must be immune.
	relative bool
}

// TestExpiryDeadlineTakesExactlyOneClockReading is invariant 3 asserted as an equality
// instead of as a hope.
//
// A relative TTL used to be resolved against the clock twice: once by the handler, which
// stored the absolute deadline in memory, and once again afterwards by the propagation
// rewrite, which computed the deadline that went to the AOF and to the replicas. Both
// readings came from the store's clock -- which is what the code claimed made them equal
// -- but they were two *readings*, separated by the handler's own execution time, so the
// replica's copy of every key expired later than the master's by however long the write
// took. Both copies looked internally consistent and nothing reported the divergence.
//
// Under a clock that advances by a fixed step per reading, that gap is exactly one step,
// so this test fails by exactly clockStep per affected command rather than flakily.
//
// All three destinations are checked, because a deadline has three copies that must name
// the same instant: memory on the master, the command shipped to a replica, and the
// command written to the AOF. The two replaying servers run frozen clocks set well after
// the master's, which is the other half of the invariant -- an absolute deadline has to
// reconstruct the same instant however much later it is replayed.
func TestExpiryDeadlineTakesExactlyOneClockReading(t *testing.T) {
	start := time.Unix(1_600_000_000, 0)
	st := store.New(4)
	st.SetClock(advancingClock(start, clockStep))

	path := filepath.Join(t.TempDir(), "clock.aof")
	logf, err := aof.Open(path, aof.SyncAlways)
	if err != nil {
		t.Fatalf("open aof: %v", err)
	}
	defer logf.Close()

	s := New(st)
	next := tapReplica(t, s)
	s.AttachAOF(logf)
	c := &binClient{t: t, s: s}

	// A payload for the RESTORE cases, whose ttl operand is relative like EXPIRE's.
	if got := c.do("SET", "payload-src", "v"); got != "+OK" {
		t.Fatalf("SET payload-src = %q", got)
	}
	next()
	payload := c.do("DUMP", "payload-src")
	if payload == "(nil)" || strings.HasPrefix(payload, "-") {
		t.Fatalf("DUMP payload-src = %q", payload)
	}

	// Every deadline is far enough out that no key expires while the test runs, on any of
	// the three clocks.
	// Every absolute control names the same instant, spelled in whichever unit its
	// command takes, so one expected value covers all of them.
	const (
		relSec  = "100000"    // 100000 s  ~= 27.8 h
		relMs   = "100000000" // the same span in ms
		absMs   = int64(1_700_000_000_000)
		absSecS = "1700000000"
		absMsS  = "1700000000000"
	)

	cases := []expiryClockCase{
		{
			name: "EXPIRE", key: "expire", cmd: []string{"EXPIRE", "expire", relSec},
			setup: [][]string{{"SET", "expire", "v"}}, deadlineAt: 2, relative: true,
		},
		{
			name: "PEXPIRE", key: "pexpire", cmd: []string{"PEXPIRE", "pexpire", relMs},
			setup: [][]string{{"SET", "pexpire", "v"}}, deadlineAt: 2, relative: true,
		},
		{
			name: "SET EX", key: "setex-opt", cmd: []string{"SET", "setex-opt", "v", "EX", relSec},
			deadlineAt: 4, relative: true,
		},
		{
			name: "SET PX", key: "setpx-opt", cmd: []string{"SET", "setpx-opt", "v", "PX", relMs},
			deadlineAt: 4, relative: true,
		},
		{
			name: "SETEX", key: "setex", cmd: []string{"SETEX", "setex", relSec, "v"},
			deadlineAt: 4, relative: true,
		},
		{
			name: "PSETEX", key: "psetex", cmd: []string{"PSETEX", "psetex", relMs, "v"},
			deadlineAt: 4, relative: true,
		},
		{
			name: "GETEX EX", key: "getex", cmd: []string{"GETEX", "getex", "EX", relSec},
			setup: [][]string{{"SET", "getex", "v"}}, deadlineAt: 2, relative: true,
		},
		{
			name: "GETEX PX", key: "getexpx", cmd: []string{"GETEX", "getexpx", "PX", relMs},
			setup: [][]string{{"SET", "getexpx", "v"}}, deadlineAt: 2, relative: true,
		},
		{
			name: "RESTORE", key: "restored", cmd: []string{"RESTORE", "restored", relMs, payload},
			deadlineAt: 2, relative: true,
		},
		// The controls: an operand that is already absolute takes no clock reading at all,
		// so it cannot skew. Listing them keeps that a tested property rather than an
		// assumption about deadlineMs.
		{
			name: "EXPIREAT", key: "expireat", cmd: []string{"EXPIREAT", "expireat", absSecS},
			setup: [][]string{{"SET", "expireat", "v"}}, deadlineAt: 2,
		},
		{
			name: "PEXPIREAT", key: "pexpireat", cmd: []string{"PEXPIREAT", "pexpireat", absMsS},
			setup: [][]string{{"SET", "pexpireat", "v"}}, deadlineAt: 2,
		},
		{
			name: "SET EXAT", key: "setexat", cmd: []string{"SET", "setexat", "v", "EXAT", absSecS},
			deadlineAt: 4,
		},
		{
			name: "SET PXAT", key: "setpxat", cmd: []string{"SET", "setpxat", "v", "PXAT", absMsS},
			deadlineAt: 4,
		},
		{
			name: "RESTORE ABSTTL", key: "restored-abs",
			cmd:        []string{"RESTORE", "restored-abs", absMsS, payload, "ABSTTL"},
			deadlineAt: 2,
		},
	}

	keys := make([]string, 0, len(cases))
	for _, tc := range cases {
		for _, setup := range tc.setup {
			if got := c.do(setup...); strings.HasPrefix(got, "-") {
				t.Fatalf("%s setup %q = %q", tc.name, strings.Join(setup, " "), got)
			}
			next() // every setup command here is itself a dirty write
		}
		if got := c.do(tc.cmd...); strings.HasPrefix(got, "-") {
			t.Fatalf("%s = %q", tc.name, got)
		}
		shipped := next()
		keys = append(keys, tc.key)

		if tc.deadlineAt >= len(shipped) {
			t.Errorf("%s propagated %q, which carries no deadline", tc.name, fmtCmd(shipped))
			continue
		}
		wire, err := strconv.ParseInt(string(shipped[tc.deadlineAt]), 10, 64)
		if err != nil {
			t.Errorf("%s propagated a non-numeric deadline in %q", tc.name, fmtCmd(shipped))
			continue
		}
		inMemory := pexpiretime(t, c, tc.key)
		if inMemory != wire {
			t.Errorf("%s: deadline in memory %d != deadline propagated %d (skew %v); "+
				"one clock reading resolved the deadline, a second one built the wire form",
				tc.name, inMemory, wire, time.Duration(wire-inMemory)*time.Millisecond)
		}
		// And the absolute operands must land on exactly the instant they name, which is
		// what proves no reading crept in where none was needed.
		if !tc.relative && inMemory != absMs {
			t.Errorf("%s stored deadline %d; want the %d it named", tc.name, inMemory, absMs)
		}
	}

	// The same instant has to come back out of a replica fed the shipped stream, and out
	// of a replay of the AOF -- both on clocks set well after the master's, so a relative
	// operand that slipped through would land somewhere else entirely.
	if err := logf.Flush(); err != nil {
		t.Fatalf("flush aof: %v", err)
	}
	logged, err := aof.Load(path)
	if err != nil {
		t.Fatalf("load aof: %v", err)
	}
	replayed := replayOnFrozenClock(t, logged, start.Add(2*time.Hour))
	for _, key := range keys {
		want := pexpiretime(t, c, key)
		if got := pexpiretime(t, replayed, key); got != want {
			t.Errorf("AOF replay put %s's deadline at %d; the master holds %d (skew %v)",
				key, got, want, time.Duration(got-want)*time.Millisecond)
		}
	}
}

// TestTransactionExpiryTakesExactlyOneClockReading is the same equality for the EXEC
// path, which builds its batch's wire forms after every queued handler has already run.
func TestTransactionExpiryTakesExactlyOneClockReading(t *testing.T) {
	st := store.New(4)
	st.SetClock(advancingClock(time.Unix(1_600_000_000, 0), clockStep))

	s := New(st)
	next := tapReplica(t, s)
	c := &binClient{t: t, s: s}

	sess := s.newSession(nil)
	for _, cmd := range [][]string{
		{"MULTI"},
		{"SET", "tx-set", "v", "EX", "100000"},
		{"SETEX", "tx-setex", "100000", "v"},
		{"SET", "tx-exp", "v"},
		{"EXPIRE", "tx-exp", "100000"},
		{"EXEC"},
	} {
		s.execute(sess, resp.NewWriter(io.Discard), cmdArgs(cmd...))
	}

	// MULTI ... EXEC framing around four writes.
	if got := next(); string(got[0]) != "MULTI" {
		t.Fatalf("first propagated command = %q; want MULTI", fmtCmd(got))
	}
	for _, want := range []struct {
		key        string
		deadlineAt int
	}{
		{"tx-set", 4},
		{"tx-setex", 4},
		{"tx-exp", -1}, // the plain SET carries no deadline
		{"tx-exp", 2},
	} {
		shipped := next()
		if want.deadlineAt < 0 {
			continue
		}
		wire, err := strconv.ParseInt(string(shipped[want.deadlineAt]), 10, 64)
		if err != nil {
			t.Fatalf("propagated a non-numeric deadline in %q", fmtCmd(shipped))
		}
		if inMemory := pexpiretime(t, c, want.key); inMemory != wire {
			t.Errorf("EXEC: %s's deadline in memory %d != deadline propagated %d (skew %v)",
				want.key, inMemory, wire, time.Duration(wire-inMemory)*time.Millisecond)
		}
	}
	if got := next(); string(got[0]) != "EXEC" {
		t.Fatalf("final propagated command = %q; want EXEC", fmtCmd(got))
	}
}

// pexpiretime reads a key's absolute expiry instant in milliseconds. It is the one read
// that says nothing about *now*, which is what makes it usable under a clock that moves
// on every reading.
func pexpiretime(t *testing.T, c *binClient, key string) int64 {
	t.Helper()
	reply := c.do("PEXPIRETIME", key)
	if !strings.HasPrefix(reply, ":") {
		t.Fatalf("PEXPIRETIME %s = %q", key, reply)
	}
	ms, err := strconv.ParseInt(reply[1:], 10, 64)
	if err != nil {
		t.Fatalf("PEXPIRETIME %s = %q", key, reply)
	}
	if ms < 0 {
		t.Fatalf("PEXPIRETIME %s = %d; the key has no live deadline", key, ms)
	}
	return ms
}

// replayOnFrozenClock rebuilds a dataset from a recorded command stream on a clock
// frozen at `at`, standing in for a replica syncing, or a server restarting, long after
// the commands were written.
func replayOnFrozenClock(t *testing.T, cmds [][][]byte, at time.Time) *binClient {
	t.Helper()
	st := store.New(4)
	st.SetClock(func() time.Time { return at })
	s := New(st)
	s.ReplayCommands(cmds)
	return &binClient{t: t, s: s}
}
