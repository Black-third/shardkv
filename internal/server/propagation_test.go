package server

import (
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

// cmdArgs builds a command in the [][]byte form the dispatcher takes.
func cmdArgs(parts ...string) [][]byte {
	args := make([][]byte, len(parts))
	for i, p := range parts {
		args[i] = []byte(p)
	}
	return args
}

// tapReplica registers a replica feed on s and returns a function that pops the
// next command shipped to it. It is the master's own view of the replica stream,
// so it shows exactly what the AOF would have received too.
func tapReplica(t *testing.T, s *Server) func() [][]byte {
	t.Helper()
	rc := newReplicaConn(64)
	s.mu.Lock()
	s.replicas[rc] = struct{}{}
	s.mu.Unlock()
	s.propagating.Store(true)
	return func() [][]byte {
		t.Helper()
		select {
		case cmd := <-rc.ch:
			return cmd
		case <-time.After(2 * time.Second):
			t.Fatal("nothing was propagated to the replica feed")
			return nil
		}
	}
}

// TestPropagatedDeadlineMatchesStoredDeadline is the injected-clock consistency
// check. The store reads time from its clock so TTL logic is testable; the server
// used to call time.Now directly when rewriting a relative expiry for the wire, so
// under a fake clock the deadline in memory and the deadline propagated to the AOF
// and replicas came from two different clocks and disagreed.
func TestPropagatedDeadlineMatchesStoredDeadline(t *testing.T) {
	st := store.New(4)
	cur := time.Unix(1_600_000_000, 0) // a fixed instant, far from wall-clock now
	st.SetClock(func() time.Time { return cur })

	s := New(st)
	next := tapReplica(t, s)
	w := resp.NewWriter(io.Discard)

	cases := []struct {
		name string
		args [][]byte
		// wantAtMs is the absolute deadline the command means under the fake clock.
		wantAtMs int64
		wantCmd  string
	}{
		{"EXPIRE", cmdArgs("EXPIRE", "k", "30"), cur.Add(30 * time.Second).UnixMilli(), "PEXPIREAT"},
		{"PEXPIRE", cmdArgs("PEXPIRE", "k", "45000"), cur.Add(45 * time.Second).UnixMilli(), "PEXPIREAT"},
		{"EXPIREAT", cmdArgs("EXPIREAT", "k", "1600000900"), 1_600_000_900_000, "PEXPIREAT"},
		{"PEXPIREAT", cmdArgs("PEXPIREAT", "k", "1600000900000"), 1_600_000_900_000, "PEXPIREAT"},
	}

	for _, tc := range cases {
		s.dispatch(w, cmdArgs("SET", "k", "v"))
		next() // the SET itself
		s.dispatch(w, tc.args)
		got := next()

		if string(got[0]) != tc.wantCmd {
			t.Errorf("%s propagated as %q; want %s", tc.name, got[0], tc.wantCmd)
			continue
		}
		gotMs, err := strconv.ParseInt(string(got[2]), 10, 64)
		if err != nil {
			t.Errorf("%s propagated a non-numeric deadline %q", tc.name, got[2])
			continue
		}
		if gotMs != tc.wantAtMs {
			t.Errorf("%s propagated deadline %d; want %d", tc.name, gotMs, tc.wantAtMs)
		}
		// And the very same instant must be the one held in memory.
		d, hasTTL, ok := st.TTL("k")
		if !ok || !hasTTL {
			t.Errorf("%s left no TTL in memory", tc.name)
			continue
		}
		if inMemory := cur.Add(d).UnixMilli(); inMemory != gotMs {
			t.Errorf("%s: in-memory deadline %d != propagated deadline %d", tc.name, inMemory, gotMs)
		}
	}
}

// TestPropagatedSetDeadlineMatchesStoredDeadline is the same invariant for the
// SET ... EX/PX form, which propagates as SET ... PXAT.
func TestPropagatedSetDeadlineMatchesStoredDeadline(t *testing.T) {
	st := store.New(4)
	cur := time.Unix(1_600_000_000, 0)
	st.SetClock(func() time.Time { return cur })

	s := New(st)
	next := tapReplica(t, s)
	w := resp.NewWriter(io.Discard)

	for _, tc := range []struct {
		args     [][]byte
		wantAtMs int64
	}{
		{cmdArgs("SET", "k", "v", "EX", "60"), cur.Add(60 * time.Second).UnixMilli()},
		{cmdArgs("SET", "k", "v", "PX", "1500"), cur.Add(1500 * time.Millisecond).UnixMilli()},
		{cmdArgs("SET", "k", "v", "PXAT", "1600000900000"), 1_600_000_900_000},
	} {
		s.dispatch(w, tc.args)
		got := next()
		if len(got) != 5 || string(got[0]) != "SET" || string(got[3]) != "PXAT" {
			t.Fatalf("propagated form = %q; want SET k v PXAT ms", flatten(got))
		}
		gotMs, err := strconv.ParseInt(string(got[4]), 10, 64)
		if err != nil {
			t.Fatalf("propagated a non-numeric deadline %q", got[4])
		}
		if gotMs != tc.wantAtMs {
			t.Errorf("%q propagated deadline %d; want %d", flatten(tc.args), gotMs, tc.wantAtMs)
		}
		d, hasTTL, ok := st.TTL("k")
		if !ok || !hasTTL {
			t.Fatalf("%q left no TTL in memory", flatten(tc.args))
		}
		if inMemory := cur.Add(d).UnixMilli(); inMemory != gotMs {
			t.Errorf("%q: in-memory deadline %d != propagated deadline %d", flatten(tc.args), inMemory, gotMs)
		}
	}
}

// TestTransactionPropagatesStoreClockDeadline covers the EXEC path, which rewrites
// each queued write for the wire on its own.
func TestTransactionPropagatesStoreClockDeadline(t *testing.T) {
	st := store.New(4)
	cur := time.Unix(1_600_000_000, 0)
	st.SetClock(func() time.Time { return cur })

	s := New(st)
	next := tapReplica(t, s)
	w := resp.NewWriter(io.Discard)

	sess := &session{}
	s.execute(sess, w, cmdArgs("MULTI"))
	s.execute(sess, w, cmdArgs("SET", "k", "v"))
	s.execute(sess, w, cmdArgs("EXPIRE", "k", "30"))
	s.execute(sess, w, cmdArgs("EXEC"))

	// The batch ships framed in MULTI ... EXEC.
	if got := next(); string(got[0]) != "MULTI" {
		t.Fatalf("first propagated command = %q; want MULTI", flatten(got))
	}
	if got := next(); string(got[0]) != "SET" {
		t.Fatalf("second propagated command = %q; want SET", flatten(got))
	}
	got := next()
	if string(got[0]) != "PEXPIREAT" {
		t.Fatalf("third propagated command = %q; want PEXPIREAT", flatten(got))
	}
	gotMs, err := strconv.ParseInt(string(got[2]), 10, 64)
	if err != nil {
		t.Fatalf("propagated a non-numeric deadline %q", got[2])
	}
	if want := cur.Add(30 * time.Second).UnixMilli(); gotMs != want {
		t.Errorf("EXEC propagated deadline %d; want %d (the store's clock)", gotMs, want)
	}
	if got := next(); string(got[0]) != "EXEC" {
		t.Fatalf("final propagated command = %q; want EXEC", flatten(got))
	}
}

func flatten(args [][]byte) string {
	out := ""
	for i, a := range args {
		if i > 0 {
			out += " "
		}
		out += string(a)
	}
	return out
}

// TestPropagatedExpiryCommandForms pins the propagated shape of every command that
// carries a *relative* expiry, or a flag a replica must not re-evaluate. Each case
// asserts both halves of the invariant: the exact command that reaches the wire, and
// that the deadline it names is the one sitting in memory.
//
// A relative TTL shipped verbatim would be resolved against the replica's clock at
// apply time, so the two copies would expire the key at different instants; a
// conditional flag shipped verbatim could be re-evaluated against the replica's own
// TTL and reject a write the master accepted.
func TestPropagatedExpiryCommandForms(t *testing.T) {
	st := store.New(4)
	cur := time.Unix(1_600_000_000, 0)
	st.SetClock(func() time.Time { return cur })

	s := New(st)
	next := tapReplica(t, s)
	w := resp.NewWriter(io.Discard)

	at := func(d time.Duration) int64 { return cur.Add(d).UnixMilli() }
	ms := func(v int64) string { return strconv.FormatInt(v, 10) }
	const absoluteMs int64 = 1_600_000_900_000

	cases := []struct {
		name  string
		key   string
		setup []string
		cmd   string
		want  string
		// wantAtMs is the deadline that must be in memory afterwards; 0 means the key
		// must have no TTL at all.
		wantAtMs int64
	}{
		{
			name: "SETEX", key: "a", cmd: "SETEX a 30 v",
			want: "SET a v PXAT " + ms(at(30*time.Second)), wantAtMs: at(30 * time.Second),
		},
		{
			name: "PSETEX", key: "b", cmd: "PSETEX b 45000 v",
			want: "SET b v PXAT " + ms(at(45*time.Second)), wantAtMs: at(45 * time.Second),
		},
		{
			name: "SET EXAT", key: "c", cmd: "SET c v EXAT 1600000900",
			want: "SET c v PXAT " + ms(absoluteMs), wantAtMs: absoluteMs,
		},
		{
			// The NX decided the write here; the replica must not decide it again.
			name: "SET NX EX", key: "d", cmd: "SET d v NX EX 10",
			want: "SET d v PXAT " + ms(at(10*time.Second)), wantAtMs: at(10 * time.Second),
		},
		{
			name: "SET XX GET", key: "e", setup: []string{"SET e old"}, cmd: "SET e v XX GET",
			want: "SET e v", wantAtMs: 0,
		},
		{
			// KEEPTTL has to survive: without it the replica would clear its own TTL.
			name: "SET KEEPTTL", key: "f", setup: []string{"SET f v EX 100"}, cmd: "SET f v2 KEEPTTL",
			want: "SET f v2 KEEPTTL", wantAtMs: at(100 * time.Second),
		},
		{
			name: "GETEX EX", key: "g", setup: []string{"SET g v"}, cmd: "GETEX g EX 30",
			want: "PEXPIREAT g " + ms(at(30*time.Second)), wantAtMs: at(30 * time.Second),
		},
		{
			name: "GETEX PERSIST", key: "h", setup: []string{"SET h v EX 100"}, cmd: "GETEX h PERSIST",
			want: "PERSIST h", wantAtMs: 0,
		},
		{
			name: "EXPIRE NX", key: "i", setup: []string{"SET i v"}, cmd: "EXPIRE i 30 NX",
			want: "PEXPIREAT i " + ms(at(30*time.Second)), wantAtMs: at(30 * time.Second),
		},
		{
			name: "PEXPIRE XX GT", key: "j", setup: []string{"SET j v EX 10"}, cmd: "PEXPIRE j 60000 XX GT",
			want: "PEXPIREAT j " + ms(at(60*time.Second)), wantAtMs: at(60 * time.Second),
		},
		{
			name: "EXPIREAT LT", key: "k", setup: []string{"SET k v"}, cmd: "EXPIREAT k 1600000900 LT",
			want: "PEXPIREAT k " + ms(absoluteMs), wantAtMs: absoluteMs,
		},
		{
			name: "PEXPIREAT NX", key: "l", setup: []string{"SET l v"}, cmd: "PEXPIREAT l 1600000900000 NX",
			want: "PEXPIREAT l " + ms(absoluteMs), wantAtMs: absoluteMs,
		},
		{
			name: "INCRBYFLOAT", key: "m", setup: []string{"SET m 10.5 EX 100"}, cmd: "INCRBYFLOAT m 0.1",
			want: "SET m 10.6 KEEPTTL", wantAtMs: at(100 * time.Second),
		},
	}

	for _, tc := range cases {
		for _, setup := range tc.setup {
			s.dispatch(w, cmdArgs(strings.Fields(setup)...))
			next() // every setup command here is itself a dirty write
		}
		s.dispatch(w, cmdArgs(strings.Fields(tc.cmd)...))
		if got := flatten(next()); got != tc.want {
			t.Errorf("%s propagated %q; want %q", tc.name, got, tc.want)
			continue
		}
		gotMs, hasTTL, ok := st.TTLMillis(tc.key)
		if !ok {
			t.Errorf("%s: key %q is gone", tc.name, tc.key)
			continue
		}
		if tc.wantAtMs == 0 {
			if hasTTL {
				t.Errorf("%s left a TTL on %q; want none", tc.name, tc.key)
			}
			continue
		}
		if !hasTTL {
			t.Errorf("%s left no TTL on %q", tc.name, tc.key)
			continue
		}
		if inMemory := cur.UnixMilli() + gotMs; inMemory != tc.wantAtMs {
			t.Errorf("%s: in-memory deadline %d != propagated deadline %d",
				tc.name, inMemory, tc.wantAtMs)
		}
	}
}

// TestNonDeterministicWritesPropagateTheirEffect covers the commands whose result is
// drawn at random or decided by iteration order. Shipping their text would have the
// replica pick different members, so what must reach the wire is the effect the
// master had -- exactly the members it removed.
func TestNonDeterministicWritesPropagateTheirEffect(t *testing.T) {
	st := store.New(4)
	s := New(st)
	next := tapReplica(t, s)
	w := resp.NewWriter(io.Discard)

	// SPOP with a count -> SREM of the members actually popped.
	s.dispatch(w, cmdArgs("SADD", "s", "a", "b", "c", "d"))
	next()
	s.dispatch(w, cmdArgs("SPOP", "s", "2"))
	got := next()
	if string(got[0]) != "SREM" || len(got) != 4 {
		t.Fatalf("SPOP propagated %q; want SREM with the two popped members", flatten(got))
	}
	for _, m := range got[2:] {
		if in, _ := st.SIsMember("s", string(m)); in {
			t.Errorf("SPOP propagated SREM of %q, which is still in the set", m)
		}
	}

	// A bare SPOP -> SREM of the one member.
	s.dispatch(w, cmdArgs("SPOP", "s"))
	if got := next(); string(got[0]) != "SREM" || len(got) != 3 {
		t.Errorf("bare SPOP propagated %q; want a single-member SREM", flatten(got))
	}

	// ZPOPMIN/ZPOPMAX -> ZREM of the members popped, which is what disambiguates a
	// tie at the boundary score.
	s.dispatch(w, cmdArgs("ZADD", "z", "1", "a", "1", "b", "1", "c"))
	next()
	s.dispatch(w, cmdArgs("ZPOPMIN", "z", "2"))
	got = next()
	if string(got[0]) != "ZREM" || len(got) != 4 {
		t.Fatalf("ZPOPMIN propagated %q; want ZREM with the two popped members", flatten(got))
	}
	for _, m := range got[2:] {
		if _, ok, _ := st.ZScore("z", string(m)); ok {
			t.Errorf("ZPOPMIN propagated ZREM of %q, which is still in the set", m)
		}
	}
	s.dispatch(w, cmdArgs("ZPOPMAX", "z"))
	if got := next(); string(got[0]) != "ZREM" || len(got) != 3 {
		t.Errorf("ZPOPMAX propagated %q; want a single-member ZREM", flatten(got))
	}

	// HINCRBYFLOAT -> the resulting field value, not the increment.
	s.dispatch(w, cmdArgs("HSET", "h", "f", "1.5"))
	next()
	s.dispatch(w, cmdArgs("HINCRBYFLOAT", "h", "f", "0.25"))
	if got := flatten(next()); got != "HSET h f 1.75" {
		t.Errorf("HINCRBYFLOAT propagated %q; want HSET h f 1.75", got)
	}

	// The random reads must propagate nothing at all: they change no state, and a
	// replica running them would answer differently.
	for _, args := range [][]string{
		{"SRANDMEMBER", "s", "2"}, {"HRANDFIELD", "h", "2"},
		{"ZRANDMEMBER", "z", "2"}, {"RANDOMKEY"},
	} {
		s.dispatch(w, cmdArgs(args...))
	}
	s.dispatch(w, cmdArgs("SET", "fence", "1"))
	if got := flatten(next()); got != "SET fence 1" {
		t.Errorf("a random read propagated %q; want nothing before the fence", got)
	}
}

// TestTransactionPropagatesEffectsAndAbsoluteDeadlines covers the EXEC path for the
// widened surface: a queued command that propagates an effect must contribute that
// effect to the batch, and a queued relative expiry its absolute deadline, all inside
// the one MULTI/EXEC frame that makes the group all-or-nothing on replay.
func TestTransactionPropagatesEffectsAndAbsoluteDeadlines(t *testing.T) {
	st := store.New(4)
	cur := time.Unix(1_600_000_000, 0)
	st.SetClock(func() time.Time { return cur })

	s := New(st)
	next := tapReplica(t, s)
	w := resp.NewWriter(io.Discard)

	s.dispatch(w, cmdArgs("SADD", "s", "a", "b", "c"))
	next()

	sess := &session{}
	for _, cmd := range []string{
		"MULTI",
		"SPOP s 2",          // propagates as SREM of what it popped
		"SETEX k 30 v",      // propagates as SET ... PXAT
		"INCRBYFLOAT f 1.5", // propagates as SET ... KEEPTTL
		"EXPIRE s 60 NX",    // propagates as PEXPIREAT, flag dropped
		"SRANDMEMBER s 1",   // a read: propagates nothing
		"EXEC",
	} {
		s.execute(sess, w, cmdArgs(strings.Fields(cmd)...))
	}

	if got := flatten(next()); got != "MULTI" {
		t.Fatalf("first propagated command = %q; want MULTI", got)
	}
	srem := next()
	if string(srem[0]) != "SREM" || len(srem) != 4 {
		t.Errorf("SPOP in a transaction propagated %q; want a two-member SREM", flatten(srem))
	}
	want := []string{
		"SET k v PXAT " + strconv.FormatInt(cur.Add(30*time.Second).UnixMilli(), 10),
		"SET f 1.5 KEEPTTL",
		"PEXPIREAT s " + strconv.FormatInt(cur.Add(60*time.Second).UnixMilli(), 10),
		"EXEC",
	}
	for _, w := range want {
		if got := flatten(next()); got != w {
			t.Errorf("propagated %q; want %q", got, w)
		}
	}
}
