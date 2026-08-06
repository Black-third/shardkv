package server

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Black-third/shardkv/internal/store"
)

// infoField extracts one INFO field's value.
//
// It fatals when the field is absent rather than returning "": a renamed or dropped INFO
// field would otherwise reach the callers as an empty string, and the assertions that
// compare against "" or against a zero would report the missing field as the expected
// value. Every other helper in this suite fatals for the same reason.
func infoField(t *testing.T, c *txConn, section, field string) string {
	t.Helper()
	reply := c.cmd("INFO " + section)
	for _, line := range strings.Split(reply, "\r\n") {
		if name, value, ok := strings.Cut(line, ":"); ok && name == field {
			return value
		}
	}
	t.Fatalf("INFO %s does not report %q: %q", section, field, reply)
	return ""
}

func infoInt(t *testing.T, c *txConn, section, field string) int64 {
	t.Helper()
	n, err := strconv.ParseInt(infoField(t, c, section, field), 10, 64)
	if err != nil {
		t.Fatalf("INFO %s %s = %q: %v", section, field, infoField(t, c, section, field), err)
	}
	return n
}

// dropReplicaFeeds ends every replica feed the master has, the way it would if a
// replica fell too far behind. The connection closes, so the replica reconnects and asks
// to continue.
func dropReplicaFeeds(s *Server) int {
	s.mu.Lock()
	feeds := make([]*replicaConn, 0, len(s.replicas))
	for rc := range s.replicas {
		feeds = append(feeds, rc)
	}
	s.mu.Unlock()
	for _, rc := range feeds {
		rc.drop()
	}
	return len(feeds)
}

// TestReplicationOffsetsAdvanceTogether covers the offset accounting itself: the
// master's stream position and the replica's processed position have to converge on the
// same number, because everything built on top (partial resync, WAIT) compares them.
//
// The keepalive is what makes this non-trivial. It is written straight to one feed
// rather than through the shared stream, so a replica that counted its bytes would drift
// a little further ahead of its master every second.
func TestReplicationOffsetsAdvanceTogether(t *testing.T) {
	master, masterAddr, stopM := startServer(t, store.New(8))
	defer stopM()
	master.pingEvery = 20 * time.Millisecond // many keepalives, to expose any drift

	replica, replicaAddr, stopR := startServer(t, store.New(8))
	defer stopR()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	replica.ReplicaOf(ctx, masterAddr)

	mc := dialTx(t, masterAddr)
	defer mc.close()
	rc := dialTx(t, replicaAddr)
	defer rc.close()

	mc.cmd("SET a 1")
	waitFor(t, "the replica to attach", func() bool { return rc.cmd("GET a") == "1" })
	// Long enough for a few keepalives to ride the feed.
	time.Sleep(150 * time.Millisecond)
	for i := 0; i < 20; i++ {
		mc.cmd("SET k" + strconv.Itoa(i) + " v")
	}
	waitFor(t, "the writes to drain", func() bool { return rc.cmd("GET k19") == "v" })

	masterOffset := infoInt(t, mc, "replication", "master_repl_offset")
	if masterOffset <= 0 {
		t.Fatalf("master_repl_offset = %d; want the stream to have advanced", masterOffset)
	}
	waitFor(t, "the replica's offset to match the master's", func() bool {
		return infoInt(t, rc, "replication", "slave_repl_offset") == masterOffset
	})
	// And the master sees the acknowledgement come back on the same connection.
	waitFor(t, "the replica's acknowledgement to reach the master", func() bool {
		return contains(mc.cmd("INFO replication"), "offset="+strconv.FormatInt(masterOffset, 10))
	})
	if got := infoField(t, rc, "replication", "master_link_status"); got != "up" {
		t.Errorf("master_link_status = %q; want up", got)
	}
}

// TestPartialResyncInsideTheBacklogWindow is the point of the backlog: a replica whose
// link drops and comes back promptly continues from where it stopped instead of being
// handed the whole dataset again. The dataset still has to agree afterwards -- a
// continuation that resumed at the wrong byte would leave the two copies diverged, which
// is the failure mode a comparison is the only way to catch.
func TestPartialResyncInsideTheBacklogWindow(t *testing.T) {
	master, masterAddr, stopM := startServer(t, store.New(8))
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

	mc.cmd("SET before 1")
	waitFor(t, "the replica to attach", func() bool { return rc.cmd("GET before") == "1" })
	if got := master.fullSyncs.Load(); got != 1 {
		t.Fatalf("sync_full = %d after the initial sync; want 1", got)
	}

	// Break the link, then keep writing while it is down. The writes land in the
	// backlog, which is what the reconnect will be served from.
	if n := dropReplicaFeeds(master); n != 1 {
		t.Fatalf("dropped %d feeds; want 1", n)
	}
	for i := 0; i < 50; i++ {
		mc.cmd("SET while-down-" + strconv.Itoa(i) + " v")
	}
	mc.cmd("DEL before") // a deletion, which a snapshot would not mention at all

	waitFor(t, "the replica to reconnect and continue", func() bool {
		return master.partialOK.Load() == 1
	})
	if got := master.fullSyncs.Load(); got != 1 {
		t.Errorf("sync_full = %d; the reconnect took a full resync instead of continuing", got)
	}
	if got := master.partialErr.Load(); got != 0 {
		t.Errorf("sync_partial_err = %d; want 0", got)
	}

	// Everything written during the outage arrived, exactly once.
	mc.cmd("SET fence done")
	waitFor(t, "the stream to drain after the continuation", func() bool {
		return rc.cmd("GET fence") == "done"
	})
	for _, cmd := range []string{"DBSIZE", "GET while-down-0", "GET while-down-49", "EXISTS before"} {
		if m, r := mc.cmd(cmd), rc.cmd(cmd); m != r {
			t.Errorf("%q: master %q != replica %q", cmd, m, r)
		}
	}
	// The offsets agree too, which is what the next continuation would rely on.
	masterOffset := infoInt(t, mc, "replication", "master_repl_offset")
	waitFor(t, "the offsets to agree after a continuation", func() bool {
		return infoInt(t, rc, "replication", "slave_repl_offset") == masterOffset
	})
	if got := infoInt(t, mc, "stats", "sync_partial_ok"); got != 1 {
		t.Errorf("INFO sync_partial_ok = %d; want 1", got)
	}
}

// TestFullResyncOutsideTheBacklogWindow covers the other side of the bound: a replica
// that was away longer than the backlog retains cannot be continued, so it must fall
// back to a full resync rather than be handed a stream starting in the wrong place.
//
// It also covers why a full resync flushes first: keys the master deleted during the
// outage are not mentioned by a snapshot, so without the flush they would survive on the
// replica forever.
func TestFullResyncOutsideTheBacklogWindow(t *testing.T) {
	master, masterAddr, stopM := startServer(t, store.New(8))
	defer stopM()
	master.SetReplBacklogSize(64) // room for about one command
	replica, replicaAddr, stopR := startServer(t, store.New(8))
	defer stopR()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	replica.ReplicaOf(ctx, masterAddr)

	mc := dialTx(t, masterAddr)
	defer mc.close()
	rc := dialTx(t, replicaAddr)
	defer rc.close()

	mc.cmd("SET doomed 1")
	mc.cmd("SET kept 1")
	waitFor(t, "the replica to attach", func() bool { return rc.cmd("GET kept") == "1" })

	dropReplicaFeeds(master)
	// Far more traffic than a 64-byte backlog can retain, including a deletion.
	for i := 0; i < 100; i++ {
		mc.cmd("SET churn-" + strconv.Itoa(i) + " v")
	}
	mc.cmd("DEL doomed")

	waitFor(t, "the replica to be refused a continuation and resync fully", func() bool {
		return master.partialErr.Load() >= 1 && master.fullSyncs.Load() >= 2
	})
	if got := master.partialOK.Load(); got != 0 {
		t.Errorf("sync_partial_ok = %d; the backlog was too small to serve any continuation", got)
	}

	mc.cmd("SET fence done")
	waitFor(t, "the stream to drain after the full resync", func() bool {
		return rc.cmd("GET fence") == "done"
	})
	for _, cmd := range []string{"DBSIZE", "GET kept", "GET churn-99", "EXISTS doomed"} {
		if m, r := mc.cmd(cmd), rc.cmd(cmd); m != r {
			t.Errorf("%q: master %q != replica %q", cmd, m, r)
		}
	}
	if got := rc.cmd("EXISTS doomed"); got != ":0" {
		t.Errorf("EXISTS doomed on the replica = %q; a full resync must not leave keys the master deleted", got)
	}
	if got := infoInt(t, mc, "stats", "sync_partial_err"); got < 1 {
		t.Errorf("INFO sync_partial_err = %d; want at least 1", got)
	}
}

// TestWaitCountsAcknowledgingReplicas covers WAIT: the count it returns, the timeout
// when there are not enough replicas, and the refusal on a replica (which owns no
// stream to be acknowledged).
func TestWaitCountsAcknowledgingReplicas(t *testing.T) {
	_, masterAddr, stopM := startServer(t, store.New(8))
	defer stopM()
	mc := dialTx(t, masterAddr)
	defer mc.close()

	// With no replicas at all, WAIT returns immediately with zero.
	start := time.Now()
	if got := mc.cmd("WAIT 1 200"); got != ":0" {
		t.Errorf("WAIT with no replicas = %q; want :0", got)
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Errorf("WAIT returned after %v without waiting for its timeout", elapsed)
	}

	replica, replicaAddr, stopR := startServer(t, store.New(8))
	defer stopR()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	replica.ReplicaOf(ctx, masterAddr)

	rc := dialTx(t, replicaAddr)
	defer rc.close()
	mc.cmd("SET k 1")
	waitFor(t, "the replica to attach", func() bool { return rc.cmd("GET k") == "1" })

	// One replica, asked for one: the acknowledgement is requested and counted.
	mc.cmd("SET k 2")
	if got := mc.cmd("WAIT 1 3000"); got != ":1" {
		t.Errorf("WAIT 1 3000 with one attached replica = %q; want :1", got)
	}
	// Asking for more than exist returns what there is once the timeout elapses, rather
	// than an error: the caller wanted a number of copies, and this is the number.
	start = time.Now()
	if got := mc.cmd("WAIT 2 300"); got != ":1" {
		t.Errorf("WAIT 2 300 = %q; want :1", got)
	}
	if elapsed := time.Since(start); elapsed < 250*time.Millisecond {
		t.Errorf("WAIT 2 300 returned after %v; want it to wait out its timeout", elapsed)
	}
	// Zero replicas asked for is satisfied at once.
	if got := mc.cmd("WAIT 0 0"); got != ":1" {
		t.Errorf("WAIT 0 0 = %q; want the current count", got)
	}

	// A replica has no stream of its own to acknowledge, so it refuses.
	if got := rc.cmd("WAIT 1 100"); !contains(got, "WAIT cannot be used with replica instances") {
		t.Errorf("WAIT on a replica = %q", got)
	}
	// Bad operands are rejected rather than treated as zero.
	if got := mc.cmd("WAIT x 100"); got != "-ERR value is not an integer or out of range" {
		t.Errorf("WAIT x 100 = %q", got)
	}
	if got := mc.cmd("WAIT 1 -5"); got != "-ERR timeout is not an integer or out of range" {
		t.Errorf("WAIT 1 -5 = %q", got)
	}
}

// TestReplicaAnnouncesItsListeningPort covers REPLCONF listening-port: the peer port of
// a replication socket is ephemeral and names nothing an operator can connect to, so a
// replica announces the port it actually serves on and the master reports that.
func TestReplicaAnnouncesItsListeningPort(t *testing.T) {
	_, masterAddr, stopM := startServer(t, store.New(8))
	defer stopM()
	replica, replicaAddr, stopR := startServer(t, store.New(8))
	defer stopR()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	replica.ReplicaOf(ctx, masterAddr)

	mc := dialTx(t, masterAddr)
	defer mc.close()
	_, replicaPort, err := splitPort(replicaAddr)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the master to report the replica's announced port", func() bool {
		return contains(mc.cmd("INFO replication"), "port="+replicaPort)
	})
	info := mc.cmd("INFO replication")
	for _, want := range []string{"connected_slaves:1", "slave0:ip=127.0.0.1", "state=online"} {
		if !contains(info, want) {
			t.Errorf("INFO replication is missing %q; got:\n%s", want, info)
		}
	}
}

func splitPort(addr string) (host, port string, err error) {
	i := strings.LastIndex(addr, ":")
	if i < 0 {
		return addr, "", nil
	}
	return addr[:i], addr[i+1:], nil
}

// TestPromotedReplicaDiscardsItsContinuationPoint covers the role change. A promoted
// master accepts its own writes, so its history diverges from the one it was following:
// continuing that history later would silently skip everything that happened in between.
func TestPromotedReplicaDiscardsItsContinuationPoint(t *testing.T) {
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

	id, offset := replica.replicationTarget()
	if id == "?" || offset < 0 {
		t.Fatalf("an attached replica has no continuation point (%q, %d)", id, offset)
	}
	rc.cmd("REPLICAOF NO ONE")
	waitFor(t, "the promotion", func() bool { return contains(rc.cmd("INFO"), "role:master") })
	if id, offset := replica.replicationTarget(); id != "?" || offset != -1 {
		t.Errorf("a promoted master kept the continuation point (%q, %d); want a full resync next time", id, offset)
	}
	// And it accepts writes now.
	if got := rc.cmd("SET local 1"); got != "+OK" {
		t.Errorf("a promoted master refused a write: %q", got)
	}
}
