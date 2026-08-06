package server

import (
	"context"
	"io"
	"testing"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

// TestFlushAllPropagatesOnceNotPerKey pins what FLUSHALL puts on the wire. It
// clears every shard without firing the store's per-key removal hook, which is
// correct: the hook exists to tell the server about removals it did not order, and
// FLUSHALL is itself propagated. Synthesizing a DEL per key would ship an unbounded
// burst that says exactly what the single FLUSHALL already says.
func TestFlushAllPropagatesOnceNotPerKey(t *testing.T) {
	st := store.New(4)
	s := New(st)
	next := tapReplica(t, s)
	w := resp.NewWriter(io.Discard)

	for _, k := range []string{"a", "b", "c", "d"} {
		s.dispatch(w, cmdArgs("SET", k, "v"))
		next()
	}

	s.dispatch(w, cmdArgs("FLUSHALL"))
	if got := flatten(next()); got != "FLUSHALL" {
		t.Fatalf("first propagated command = %q; want FLUSHALL", got)
	}
	// Nothing else: no trailing DEL storm.
	s.dispatch(w, cmdArgs("SET", "sentinel", "1"))
	if got := flatten(next()); got != "SET sentinel 1" {
		t.Errorf("FLUSHALL propagated extra commands; next was %q", got)
	}
	if st.Len() != 1 {
		t.Errorf("store holds %d keys after FLUSHALL + one write; want 1", st.Len())
	}
}

// TestFlushAllKeepsEvictionAccounting checks FLUSHALL leaves the counters INFO
// reports alone. evicted_keys is a lifetime total of eviction-policy work, the way
// Redis reports it, so an administrative flush must not rewrite that history; and
// the cap itself still applies afterwards.
func TestFlushAllKeepsEvictionAccounting(t *testing.T) {
	st := store.New(1)
	st.SetMaxKeys(2)
	s := New(st)
	w := resp.NewWriter(io.Discard)

	for i := 0; i < 6; i++ {
		s.dispatch(w, cmdArgs("SET", "k"+string(rune('a'+i)), "v"))
	}
	st.EvictToLimit()
	before := st.Evicted()
	if before == 0 {
		t.Fatal("no evictions happened; the test cannot observe the counter")
	}

	s.dispatch(w, cmdArgs("FLUSHALL"))
	if got := st.Evicted(); got != before {
		t.Errorf("evicted_keys = %d after FLUSHALL; want the lifetime total %d", got, before)
	}
	if st.Len() != 0 {
		t.Errorf("FLUSHALL left %d keys", st.Len())
	}

	// The cap is still enforced after a flush.
	for i := 0; i < 6; i++ {
		s.dispatch(w, cmdArgs("SET", "n"+string(rune('a'+i)), "v"))
	}
	st.EvictToLimit()
	if got := st.Len(); got != 2 {
		t.Errorf("Len = %d after refilling a flushed capped store; want 2", got)
	}
}

// TestFlushAllInvalidatesWatchers pins the WATCH behavior FLUSHALL already had:
// it removes every key, so every watching session has to be marked dirty.
func TestFlushAllInvalidatesWatchers(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()
	other := dialTx(t, addr)
	defer other.close()

	c.cmd("SET k 1")
	c.cmd("WATCH k")
	c.cmd("MULTI")
	c.cmd("SET k from-tx")

	if got := other.cmd("FLUSHALL"); got != "+OK" {
		t.Fatalf("FLUSHALL = %q", got)
	}
	if got := c.cmd("EXEC"); got != "(nil)" {
		t.Fatalf("EXEC after a concurrent FLUSHALL = %q; want (nil)", got)
	}
	if got := c.cmd("EXISTS k"); got != ":0" {
		t.Errorf("EXISTS k = %q; want :0 (the aborted tx must not have written)", got)
	}
}

// TestFlushAllPropagatesDownAReplicaChain covers FLUSHALL through a two-hop chain:
// a replica applies it and re-forwards it to its own replica, so every copy ends up
// empty rather than the middle one alone.
func TestFlushAllPropagatesDownAReplicaChain(t *testing.T) {
	_, masterAddr, stopM := startServer(t, store.New(8))
	defer stopM()
	mid, midAddr, stopMid := startServer(t, store.New(8))
	defer stopMid()
	leaf, leafAddr, stopLeaf := startServer(t, store.New(8))
	defer stopLeaf()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mid.ReplicaOf(ctx, masterAddr)
	leaf.ReplicaOf(ctx, midAddr)

	mc := dialTx(t, masterAddr)
	defer mc.close()
	midc := dialTx(t, midAddr)
	defer midc.close()
	leafc := dialTx(t, leafAddr)
	defer leafc.close()

	mc.cmd("SET a 1")
	mc.cmd("SET b 2")
	mc.cmd("RPUSH l x y z")
	waitFor(t, "writes to reach the end of the chain", func() bool {
		return leafc.cmd("GET b") == "2" && leafc.cmd("LRANGE l 0 -1") == "[x y z]"
	})

	if got := mc.cmd("FLUSHALL"); got != "+OK" {
		t.Fatalf("FLUSHALL on the master = %q", got)
	}
	waitFor(t, "FLUSHALL to reach the middle replica", func() bool {
		return midc.cmd("DBSIZE") == ":0"
	})
	waitFor(t, "FLUSHALL to reach the end of the chain", func() bool {
		return leafc.cmd("DBSIZE") == ":0"
	})

	// The chain still works afterwards: a flush must not tear replication down.
	mc.cmd("SET after 1")
	waitFor(t, "a post-FLUSHALL write to reach the end of the chain", func() bool {
		return leafc.cmd("GET after") == "1"
	})
}

// TestReplicaWatchInvalidatedByMasterWrite covers WATCH on a replica. A write
// arriving from the master modifies a key exactly as a local write would, but the
// apply path skipped the watcher registry entirely, so a replica client's WATCH was
// never invalidated and its EXEC committed against data that had already changed.
func TestReplicaWatchInvalidatedByMasterWrite(t *testing.T) {
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
	poll := dialTx(t, replicaAddr) // a second connection, so rc can stay in MULTI
	defer poll.close()

	mc.cmd("SET k 1")
	waitFor(t, "the initial value to replicate", func() bool {
		return poll.cmd("GET k") == "1"
	})

	rc.cmd("WATCH k")
	rc.cmd("MULTI")
	rc.cmd("GET k") // read-only: a replica rejects queued writes at EXEC time anyway

	mc.cmd("SET k 2")
	waitFor(t, "the master's write to reach the replica", func() bool {
		return poll.cmd("GET k") == "2"
	})

	if got := rc.cmd("EXEC"); got != "(nil)" {
		t.Fatalf("EXEC = %q; want (nil): the watched key changed under it", got)
	}
}

// TestInfoReportsReplicationCounters covers the two counters added for the
// slow-replica path, so an operator can see resyncs and drops happening.
func TestInfoReportsReplicationCounters(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	reply := c.cmd("INFO")
	for _, want := range []string{"sync_full:0", "replica_drops:0"} {
		if !contains(reply, want) {
			t.Errorf("INFO missing %q; got:\n%s", want, reply)
		}
	}
}
