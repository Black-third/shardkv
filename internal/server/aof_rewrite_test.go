package server

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/Black-third/shardkv/internal/aof"
	"github.com/Black-third/shardkv/internal/store"
)

// newAOFServer returns a server persisting to a fresh log, plus the log's path.
func newAOFServer(t *testing.T, policy aof.SyncPolicy) (*Server, string, *aof.Log) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "rewrite.aof")
	logf, err := aof.Open(path, policy)
	if err != nil {
		t.Fatal(err)
	}
	s := New(store.New(8))
	s.AttachAOF(logf)
	t.Cleanup(func() { logf.Close() })
	return s, path, logf
}

// replayInto rebuilds a fresh server from the log at path.
func replayInto(t *testing.T, path string) *Server {
	t.Helper()
	cmds, err := aof.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	s := New(store.New(8))
	s.ReplayCommands(cmds)
	return s
}

// TestAOFRewriteCompactsTheLog is the bug this fixes: nothing ever called Rewrite, so
// a key written a thousand times cost a thousand records forever. After a rewrite the
// log describes the dataset, not its history, and still replays to the same state.
func TestAOFRewriteCompactsTheLog(t *testing.T) {
	// SyncNo: this test is about the log's contents, and an explicit Flush below puts
	// them on disk without paying an fsync per write.
	s, path, logf := newAOFServer(t, aof.SyncNo)
	c := &directClient{t: t, s: s}

	for i := 0; i < 500; i++ {
		c.cmd("SET counter " + strconv.Itoa(i))
	}
	c.cmd("RPUSH l a b c")
	before := logf.Size()

	if err := s.RewriteAOF(); err != nil {
		t.Fatalf("RewriteAOF: %v", err)
	}
	after := logf.Size()
	if after >= before {
		t.Errorf("the rewritten log is %d bytes, the original %d; it did not compact", after, before)
	}
	if got := logf.BaseSize(); got != after {
		t.Errorf("aof_base_size = %d after a rewrite; want the new size %d", got, after)
	}
	if err := logf.Flush(); err != nil {
		t.Fatal(err)
	}

	replayed := &directClient{t: t, s: replayInto(t, path)}
	for _, cmd := range []string{"GET counter", "LRANGE l 0 -1", "DBSIZE"} {
		if want, got := c.cmd(cmd), replayed.cmd(cmd); want != got {
			t.Errorf("%q: live %q != replayed-from-rewritten-log %q", cmd, want, got)
		}
	}
	// And the compacted file really is short: one record per key, not per write.
	records, err := aof.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) > 8 {
		t.Errorf("the rewritten log holds %d records for a 2-key dataset", len(records))
	}
}

// TestAOFRewriteUnderConcurrentWritesLosesNothing is the consistency guarantee stated
// as a test. Writers hammer the server while a rewrite runs; afterwards a replay of the
// log has to reproduce the live dataset exactly. A write landing between the snapshot
// and the file swap would be acknowledged to its client and then discarded with the old
// file, and only a comparison like this notices.
func TestAOFRewriteUnderConcurrentWritesLosesNothing(t *testing.T) {
	s, path, logf := newAOFServer(t, aof.SyncNo)

	const writers, perWriter = 8, 250
	var wg sync.WaitGroup
	for wr := 0; wr < writers; wr++ {
		wg.Add(1)
		go func(wr int) {
			defer wg.Done()
			c := &directClient{t: t, s: s}
			for i := 0; i < perWriter; i++ {
				c.cmd("SET k" + strconv.Itoa(wr) + " " + strconv.Itoa(i))
				c.cmd("INCR shared")
				if i == perWriter/2 && wr == 0 {
					if err := s.RewriteAOF(); err != nil {
						t.Errorf("RewriteAOF: %v", err)
					}
				}
			}
		}(wr)
	}
	wg.Wait()
	if err := logf.Flush(); err != nil {
		t.Fatal(err)
	}

	live := &directClient{t: t, s: s}
	replayed := &directClient{t: t, s: replayInto(t, path)}
	checks := []string{"DBSIZE", "GET shared"}
	for wr := 0; wr < writers; wr++ {
		checks = append(checks, "GET k"+strconv.Itoa(wr))
	}
	for _, cmd := range checks {
		if want, got := live.cmd(cmd), replayed.cmd(cmd); want != got {
			t.Errorf("%q: live %q != replayed %q", cmd, want, got)
		}
	}
	if want := ":" + strconv.Itoa(writers*perWriter); live.cmd("GET shared") != strconv.Itoa(writers*perWriter) {
		t.Errorf("the live counter is %q; want %s writes to have applied", live.cmd("GET shared"), want)
	}
}

// TestAOFAutoRewritePolicy covers the trigger: both conditions have to hold, and the
// size after the last rewrite is what the growth is measured against -- otherwise a
// large dataset would rewrite itself in a loop.
func TestAOFAutoRewritePolicy(t *testing.T) {
	s, _, logf := newAOFServer(t, aof.SyncNo)
	c := &directClient{t: t, s: s}

	// A minimum size the log has not reached: no rewrite, however much it grows.
	s.SetAOFRewritePolicy(1<<30, 1)
	for i := 0; i < 200; i++ {
		c.cmd("SET k v" + strconv.Itoa(i))
	}
	if s.aofRewriting.Load() {
		t.Error("a rewrite started below the minimum size")
	}
	sizeBeforePolicy := logf.Size()
	if sizeBeforePolicy == 0 {
		t.Fatal("nothing was appended; the test cannot observe the policy")
	}

	// Now a minimum size the log is already past. The log was created empty, so all of
	// it counts as growth over its base and one further write crosses the threshold.
	s.SetAOFRewritePolicy(sizeBeforePolicy/2, 100)
	c.cmd("SET k trigger")
	waitFor(t, "the automatic rewrite to finish", func() bool {
		return !s.aofRewriting.Load() && logf.BaseSize() < sizeBeforePolicy
	})
	if got := s.aofRewriteStatus(); got != "ok" {
		t.Errorf("aof_last_bgrewrite_status = %q after an automatic rewrite", got)
	}
	if got := logf.Size(); got != logf.BaseSize() {
		t.Errorf("aof_current_size %d != aof_base_size %d right after a rewrite", got, logf.BaseSize())
	}

	// The compacted size is the new baseline, and it is far below the threshold again,
	// so the next write cannot trigger a second rewrite. That is what stops a large
	// dataset from rewriting itself in a loop.
	base := logf.BaseSize()
	if base >= sizeBeforePolicy/2 {
		t.Fatalf("the compacted log (%d bytes) is not below the minimum size (%d); the check below would prove nothing",
			base, sizeBeforePolicy/2)
	}
	c.cmd("SET k one-more")
	if s.aofRewriting.Load() {
		t.Error("a second rewrite fired without the log growing past the new base")
	}
}

// TestFailedAOFRewriteLeavesTheLogUsable covers the failure path. A rewrite that cannot
// produce its temporary file must not take the working log down with it: the data
// already persisted is what a crash would replay, and losing the ability to append
// would silently stop persisting every write after it.
//
// The failure is arranged by putting a directory where the rewrite wants its temp file,
// which makes the open fail before anything is touched.
func TestFailedAOFRewriteLeavesTheLogUsable(t *testing.T) {
	s, path, logf := newAOFServer(t, aof.SyncNo)
	c := &directClient{t: t, s: s}

	c.cmd("SET before v")
	if err := os.Mkdir(path+".tmp", 0o755); err != nil {
		t.Fatal(err)
	}

	if err := s.RewriteAOF(); err == nil {
		t.Fatal("RewriteAOF succeeded although its temporary file could not be created")
	}
	if got := s.aofRewriteStatus(); got != "err" {
		t.Errorf("aof_last_bgrewrite_status = %q after a failed rewrite; want err", got)
	}
	if s.aofRewriting.Load() {
		t.Error("aof_rewrite_in_progress stayed set after a failed rewrite")
	}

	// The log still works: appends land and the whole history replays.
	c.cmd("SET after v")
	if err := logf.Flush(); err != nil {
		t.Fatalf("the log is no longer flushable after a failed rewrite: %v", err)
	}
	replayed := &directClient{t: t, s: replayInto(t, path)}
	for _, key := range []string{"before", "after"} {
		if got := replayed.cmd("GET " + key); got != "v" {
			t.Errorf("after a failed rewrite, replayed GET %s = %q; want v", key, got)
		}
	}

	// A later rewrite succeeds once the obstruction is gone, so the failure was not
	// terminal for the feature either.
	if err := os.Remove(path + ".tmp"); err != nil {
		t.Fatal(err)
	}
	if err := s.RewriteAOF(); err != nil {
		t.Fatalf("a rewrite after the obstruction was removed still failed: %v", err)
	}
	if got := s.aofRewriteStatus(); got != "ok" {
		t.Errorf("aof_last_bgrewrite_status = %q after a successful rewrite", got)
	}
}

// TestBGRewriteAOFCommand covers the client-facing command, including the reply on a
// server with no log at all.
func TestBGRewriteAOFCommand(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()
	if got := c.cmd("BGREWRITEAOF"); !contains(got, "append-only file is disabled") {
		t.Errorf("BGREWRITEAOF with no AOF = %q; want an explicit error", got)
	}

	s, path, logf := newAOFServer(t, aof.SyncNo)
	direct := &directClient{t: t, s: s}
	direct.cmd("SET k v")
	if got := direct.cmd("BGREWRITEAOF"); got != "+Background append only file rewriting started" {
		t.Errorf("BGREWRITEAOF = %q", got)
	}
	waitFor(t, "the background rewrite to finish", func() bool { return !s.aofRewriting.Load() })
	if err := logf.Flush(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte("SET")) {
		t.Errorf("the rewritten log does not describe the dataset:\n%q", data)
	}
}
