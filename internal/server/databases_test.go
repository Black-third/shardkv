package server

// Multiple-database tests.
//
// Two of these matter more than the rest. TestSelectFramesTheStream pins the
// propagation decision -- the AOF and the replica stream carry no database context, so
// the database has to appear *in* the stream as a SELECT -- and TestAOFReplayAcross
// Databases and TestReplicaConvergesAcrossDatabases close the loop on it: a stream that
// named the wrong database would put every key in the wrong keyspace, silently, which is
// exactly the failure class the invariants exist to prevent.

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Black-third/shardkv/internal/aof"
	"github.com/Black-third/shardkv/internal/store"
)

// TestDatabaseIsolation covers the point of the feature: the same key name in two
// databases is two keys.
func TestDatabaseIsolation(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		{"SET shared zero", "+OK"},
		{"SELECT 1", "+OK"},
		{"GET shared", "(nil)"}, // a different keyspace entirely
		{"SET shared one", "+OK"},
		{"DBSIZE", ":1"},
		{"SELECT 0", "+OK"},
		{"GET shared", "zero"},
		{"DBSIZE", ":1"},
		// Every type, not just strings: the whole keyspace is per-database, because each
		// database is its own set of shards.
		{"RPUSH l a", ":1"},
		{"SELECT 1", "+OK"},
		{"LRANGE l 0 -1", "[]"},
		{"RPUSH l b c", ":2"},
		{"SELECT 0", "+OK"},
		{"LRANGE l 0 -1", "[a]"},
		// TTLs are per-database too, since they live on the entry.
		{"EXPIRE shared 100", ":1"},
		{"SELECT 1", "+OK"},
		{"TTL shared", ":-1"},
		{"SELECT 0", "+OK"},
		{"TTL shared", ":100"},
		// RESET returns the connection to database 0, as in Redis.
		{"SELECT 1", "+OK"},
		{"RESET", "+RESET"},
		{"GET shared", "zero"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}
	// A second connection starts in database 0 regardless of what this one selected.
	other := dialTx(t, addr)
	defer other.close()
	if got := other.cmd("GET shared"); got != "zero" {
		t.Errorf("a new connection sees %q; want database 0", got)
	}
	if got := other.cmd("CLIENT LIST"); !contains(got, "db=0") {
		t.Errorf("CLIENT LIST should report each connection's database:\n%s", got)
	}
	c.cmd("SELECT 3")
	if got := other.cmd("CLIENT LIST"); !contains(got, "db=3") {
		t.Errorf("CLIENT LIST should report the other connection's database:\n%s", got)
	}
}

// TestFlushDBvsFlushAll pins the distinction, in both directions.
func TestFlushDBvsFlushAll(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("SET k zero")
	c.cmd("SELECT 1")
	c.cmd("SET k one")
	if got := c.cmd("FLUSHDB"); got != "+OK" {
		t.Fatalf("FLUSHDB = %q", got)
	}
	if got := c.cmd("GET k"); got != "(nil)" {
		t.Errorf("FLUSHDB left database 1 holding %q", got)
	}
	c.cmd("SELECT 0")
	if got := c.cmd("GET k"); got != "zero" {
		t.Errorf("FLUSHDB in database 1 emptied database 0: GET k = %q", got)
	}
	c.cmd("SELECT 1")
	c.cmd("SET k one")
	if got := c.cmd("FLUSHALL"); got != "+OK" {
		t.Fatalf("FLUSHALL = %q", got)
	}
	if got := c.cmd("GET k"); got != "(nil)" {
		t.Errorf("FLUSHALL left database 1 holding %q", got)
	}
	c.cmd("SELECT 0")
	if got := c.cmd("GET k"); got != "(nil)" {
		t.Errorf("FLUSHALL left database 0 holding %q", got)
	}
}

// TestSwapDBMoveAndCopy covers the three commands that reach across two databases.
func TestSwapDBMoveAndCopy(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	// SWAPDB exchanges the contents; a client keeps its index and finds the other data.
	c.cmd("SET who zero")
	c.cmd("SELECT 1")
	c.cmd("SET who one")
	c.cmd("SELECT 0")
	if got := c.cmd("SWAPDB 0 1"); got != "+OK" {
		t.Fatalf("SWAPDB = %q", got)
	}
	if got := c.cmd("GET who"); got != "one" {
		t.Errorf("after SWAPDB, database 0 holds %q; want one", got)
	}
	c.cmd("SELECT 1")
	if got := c.cmd("GET who"); got != "zero" {
		t.Errorf("after SWAPDB, database 1 holds %q; want zero", got)
	}
	c.cmd("SELECT 0")

	// MOVE takes a key out of this database and puts it in another, but only if the
	// destination does not already hold it.
	cases := []struct{ cmd, want string }{
		{"SET mover v", "+OK"},
		{"MOVE mover 2", ":1"},
		{"EXISTS mover", ":0"},
		{"MOVE mover 2", ":0"}, // gone from here, so nothing to move
		{"SET mover other", "+OK"},
		{"MOVE mover 2", ":0"}, // database 2 already has it
		{"GET mover", "other"}, // ...and a refused MOVE leaves the source alone
		{"MOVE mover 0", "-ERR source and destination objects are the same"},
		{"MOVE mover 99", "-ERR DB index is out of range"},
		{"MOVE mover abc", "-ERR value is not an integer or out of range"},
		// COPY with a DB option, with and without REPLACE, and to a different name.
		{"SET orig v1", "+OK"},
		{"COPY orig orig DB 3", ":1"},
		{"COPY orig orig DB 3", ":0"},
		{"COPY orig orig DB 3 REPLACE", ":1"},
		{"COPY orig renamed DB 3", ":1"},
		{"GET orig", "v1"}, // COPY leaves the source in place
		{"EXISTS renamed", ":0"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}
	c.cmd("SELECT 2")
	if got := c.cmd("GET mover"); got != "v" {
		t.Errorf("MOVE delivered %q to database 2; want v", got)
	}
	c.cmd("SELECT 3")
	if got := c.cmd("GET orig"); got != "v1" {
		t.Errorf("COPY DB 3 delivered %q; want v1", got)
	}
	if got := c.cmd("GET renamed"); got != "v1" {
		t.Errorf("COPY orig renamed DB 3 delivered %q; want v1", got)
	}
}

// TestSelectFramesTheStream is the propagation decision, asserted on the stream itself.
//
// A write in database 0 must ship exactly what it always shipped -- no SELECT at all --
// so an AOF written before databases existed replays identically and a replica sees no
// new commands. A write in another database must be preceded by the SELECT that puts the
// replayer there, and only when the database actually changes.
func TestSelectFramesTheStream(t *testing.T) {
	s, addr, stop := startServer(t, store.New(8))
	defer stop()
	next := tapReplica(t, s)

	c := dialTx(t, addr)
	defer c.close()

	c.cmd("SET a 1")
	if got := propagatedText(next()); got != "SET a 1" {
		t.Errorf("a database-0 write shipped %q; want no SELECT before it", got)
	}
	c.cmd("SET b 2")
	if got := propagatedText(next()); got != "SET b 2" {
		t.Errorf("a second database-0 write shipped %q", got)
	}

	c.cmd("SELECT 1")
	c.cmd("SET a in-one")
	if got := propagatedText(next()); got != "SELECT 1" {
		t.Errorf("the first write after SELECT 1 shipped %q; want a SELECT first", got)
	}
	if got := propagatedText(next()); got != "SET a in-one" {
		t.Errorf("shipped %q", got)
	}
	// The stream is now positioned in database 1, so a second write there needs nothing.
	c.cmd("SET c in-one")
	if got := propagatedText(next()); got != "SET c in-one" {
		t.Errorf("a second database-1 write shipped %q; want no repeated SELECT", got)
	}
	// And going back re-frames it.
	c.cmd("SELECT 0")
	c.cmd("SET d 4")
	if got := propagatedText(next()); got != "SELECT 0" {
		t.Errorf("returning to database 0 shipped %q; want SELECT 0", got)
	}
	if got := propagatedText(next()); got != "SET d 4" {
		t.Errorf("shipped %q", got)
	}
	// A SELECT with no write after it ships nothing: the client's database is connection
	// state, and only a write has a database to record.
	c.cmd("SELECT 5")
	c.cmd("GET a")
	c.cmd("SELECT 0")
	c.cmd("SET e 5")
	if got := propagatedText(next()); got != "SET e 5" {
		t.Errorf("a SELECT with no write shipped something: %q", got)
	}
}

// TestAOFReplayAcrossDatabases writes to several databases, replays the log into a fresh
// server, and requires every key to land back in the database it came from.
func TestAOFReplayAcrossDatabases(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dbs.aof")
	logf, err := aof.Open(path, aof.SyncAlways)
	if err != nil {
		t.Fatal(err)
	}
	live := New(store.New(8))
	if err := live.SetDatabases(4); err != nil {
		t.Fatal(err)
	}
	live.AttachAOF(logf)

	// Written through the views directly, which is what a connection's SELECT resolves
	// to, so the log records exactly what a client would have produced.
	for db := 0; db < 4; db++ {
		view := live.forDB(db)
		c := &directClient{t: t, s: view}
		c.cmd("SET key v" + itoaTest(db))
		c.cmd("RPUSH list e" + itoaTest(db))
		c.cmd("SET only-here " + itoaTest(db))
	}
	// Back to database 0 and change something, so the log ends with a SELECT that has to
	// be honoured rather than merely tolerated.
	(&directClient{t: t, s: live.forDB(0)}).cmd("SET key final")
	if err := logf.Close(); err != nil {
		t.Fatal(err)
	}

	cmds, err := aof.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	replayed := New(store.New(8))
	if err := replayed.SetDatabases(4); err != nil {
		t.Fatal(err)
	}
	replayed.ReplayCommands(cmds)

	for db := 0; db < 4; db++ {
		live, rep := live.forDB(db), replayed.forDB(db)
		lc, rc := &directClient{t: t, s: live}, &directClient{t: t, s: rep}
		for _, cmd := range []string{"GET key", "LRANGE list 0 -1", "GET only-here", "DBSIZE"} {
			if want, got := lc.cmd(cmd), rc.cmd(cmd); want != got {
				t.Errorf("database %d, %q: replay gave %q; live has %q", db, cmd, got, want)
			}
		}
	}
	// And nothing leaked into a database that was never written to.
	if got := (&directClient{t: t, s: replayed.forDB(0)}).cmd("GET only-here"); got != "0" {
		t.Errorf("database 0's own key replayed as %q", got)
	}
}

// TestReplicaConvergesAcrossDatabases takes a full resync of a master holding data in
// several databases and requires the replica to place all of it identically. It is the
// snapshot half of the same guarantee TestSelectFramesTheStream checks for the stream.
func TestReplicaConvergesAcrossDatabases(t *testing.T) {
	master, masterAddr, stopM := startServer(t, store.New(8))
	defer stopM()
	replica, replicaAddr, stopR := startServer(t, store.New(8))
	defer stopR()

	mc := dialTx(t, masterAddr)
	defer mc.close()
	// Seed several databases *before* the replica attaches, so this data can only reach
	// it through the snapshot.
	for db := 0; db < 4; db++ {
		mc.cmd("SELECT " + itoaTest(db))
		mc.cmd("SET key v" + itoaTest(db))
		mc.cmd("RPUSH list a" + itoaTest(db) + " b" + itoaTest(db))
		mc.cmd("ZADD z " + itoaTest(db+1) + " m")
	}
	mc.cmd("SELECT 2")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	replica.ReplicaOf(ctx, masterAddr)

	rc := dialTx(t, replicaAddr)
	defer rc.close()
	waitFor(t, "the snapshot to arrive", func() bool {
		rc.cmd("SELECT 3")
		return rc.cmd("GET key") == "v3"
	})

	// Now write live, from the database the master's connection is still in, so the
	// stream's position after the snapshot is exercised too.
	mc.cmd("SET live-2 yes")
	mc.cmd("SELECT 0")
	mc.cmd("SET live-0 yes")
	waitFor(t, "the live writes to arrive", func() bool {
		rc.cmd("SELECT 0")
		return rc.cmd("GET live-0") == "yes"
	})

	for db := 0; db < 4; db++ {
		mc.cmd("SELECT " + itoaTest(db))
		rc.cmd("SELECT " + itoaTest(db))
		for _, cmd := range []string{"GET key", "LRANGE list 0 -1", "ZSCORE z m", "DBSIZE"} {
			if want, got := mc.cmd(cmd), rc.cmd(cmd); want != got {
				t.Errorf("database %d, %q: replica has %q; master has %q", db, cmd, got, want)
			}
		}
	}
	rc.cmd("SELECT 2")
	if got := rc.cmd("GET live-2"); got != "yes" {
		t.Errorf("a live write into database 2 arrived as %q", got)
	}
	rc.cmd("SELECT 0")
	if got := rc.cmd("GET live-2"); got != "(nil)" {
		t.Errorf("a database-2 write leaked into database 0 on the replica")
	}
	_ = master
}

// TestWatchIsPerDatabase: a WATCH guards a key in one database, and a write to the same
// key name in another database is not a conflict.
func TestWatchIsPerDatabase(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	watcher := dialTx(t, addr)
	defer watcher.close()
	other := dialTx(t, addr)
	defer other.close()

	watcher.cmd("WATCH guarded")
	other.cmd("SELECT 1")
	other.cmd("SET guarded elsewhere")
	watcher.cmd("MULTI")
	watcher.cmd("SET guarded mine")
	if got := watcher.cmd("EXEC"); got != "[+OK]" {
		t.Errorf("EXEC = %q; a write in another database is not a conflict", got)
	}

	// The same write in the watched database does abort it.
	watcher.cmd("WATCH guarded")
	other.cmd("SELECT 0")
	other.cmd("SET guarded theirs")
	watcher.cmd("MULTI")
	watcher.cmd("SET guarded mine")
	if got := watcher.cmd("EXEC"); got != "(nil)" {
		t.Errorf("EXEC = %q; want it aborted", got)
	}

	// A MOVE into the watched database is a write to that key there, so it must conflict.
	watcher.cmd("SELECT 4")
	watcher.cmd("WATCH arriving")
	other.cmd("SELECT 5")
	other.cmd("SET arriving v")
	other.cmd("MOVE arriving 4")
	watcher.cmd("MULTI")
	watcher.cmd("GET arriving")
	if got := watcher.cmd("EXEC"); got != "(nil)" {
		t.Errorf("EXEC = %q; a MOVE into the watched database must abort it", got)
	}

	// FLUSHDB invalidates only its own database's watchers; FLUSHALL invalidates all.
	watcher.cmd("SELECT 0")
	watcher.cmd("WATCH guarded")
	other.cmd("SELECT 1")
	other.cmd("FLUSHDB")
	watcher.cmd("MULTI")
	watcher.cmd("GET guarded")
	if got := watcher.cmd("EXEC"); got == "(nil)" {
		t.Errorf("FLUSHDB in another database aborted a WATCH it should not have")
	}
	watcher.cmd("WATCH guarded")
	other.cmd("FLUSHALL")
	watcher.cmd("MULTI")
	watcher.cmd("GET guarded")
	if got := watcher.cmd("EXEC"); got != "(nil)" {
		t.Errorf("EXEC = %q; FLUSHALL must invalidate every WATCH", got)
	}
}

// TestBlockingIsPerDatabase: a client blocked on a key in one database must not be woken
// by a push to the same key name in another.
func TestBlockingIsPerDatabase(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	admin := dialTx(t, addr)
	defer admin.close()

	a := dialAsync(t, addr)
	defer a.close()
	a.send("SELECT 2")
	a.reply(t, 5*time.Second)
	a.send("BLPOP q 0")
	waitBlocked(t, admin, 1)

	admin.cmd("RPUSH q wrong-database") // database 0
	a.silentFor(t, 200*time.Millisecond)

	admin.cmd("SELECT 2")
	admin.cmd("RPUSH q right-database")
	if got := a.reply(t, 5*time.Second); got != "[q right-database]" {
		t.Errorf("woken with %q; want the push in its own database", got)
	}
	// And the push into database 0 is still sitting there untouched.
	admin.cmd("SELECT 0")
	if got := admin.cmd("LRANGE q 0 -1"); got != "[wrong-database]" {
		t.Errorf("database 0's queue = %q; the blocked client took from it", got)
	}
}

// TestKeyspaceNotificationsPerDatabase covers the channel names, which carry the
// database because the channels themselves do not: Pub/Sub has no databases.
func TestKeyspaceNotificationsPerDatabase(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	admin := dialTx(t, addr)
	defer admin.close()
	if got := admin.cmd("CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}

	sub := dialTx(t, addr)
	defer sub.close()
	sub.cmd("PSUBSCRIBE __keyevent@*")

	admin.cmd("SELECT 7")
	admin.cmd("SET tracked v")
	if got := readReply(t, sub.br); got != "[pmessage __keyevent@* __keyevent@7__:set tracked]" {
		t.Errorf("notification = %q; want the database in the channel name", got)
	}

	// MOVE reports move_from in the source database and move_to in the destination, which
	// are two different channels.
	admin.cmd("MOVE tracked 8")
	got := readReply(t, sub.br) + " " + readReply(t, sub.br)
	if !strings.Contains(got, "__keyevent@7__:move_from") ||
		!strings.Contains(got, "__keyevent@8__:move_to") {
		t.Errorf("MOVE notifications = %q", got)
	}
}

// TestInfoKeyspacePerDatabase covers the report an operator reads to find where the keys
// are, including the omission of empty databases (which is what Redis does).
func TestInfoKeyspacePerDatabase(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("MSET a 1 b 2")
	c.cmd("EXPIRE a 100")
	c.cmd("SELECT 5")
	c.cmd("SET only 1")

	got := c.cmd("INFO keyspace")
	for _, want := range []string{
		"db0:keys=2,expires=1,avg_ttl=0",
		"db5:keys=1,expires=0,avg_ttl=0",
		"db_keys:3",
		"databases:16",
	} {
		if !contains(got, want) {
			t.Errorf("INFO keyspace is missing %q:\n%s", want, got)
		}
	}
	if contains(got, "db1:") {
		t.Errorf("INFO keyspace reported an empty database:\n%s", got)
	}
}

// TestSwapDBPropagatesAndUnblocks checks the two sweeping consequences of SWAPDB: it
// reaches the stream verbatim (both indexes are absolute, so a replica swaps the same
// two), and it wakes clients blocked on keys that may now hold data.
func TestSwapDBPropagatesAndUnblocks(t *testing.T) {
	s, addr, stop := startServer(t, store.New(8))
	defer stop()
	next := tapReplica(t, s)

	admin := dialTx(t, addr)
	defer admin.close()
	admin.cmd("SELECT 1")
	admin.cmd("RPUSH q ready")
	if got := propagatedText(next()); got != "SELECT 1" {
		t.Fatalf("shipped %q", got)
	}
	if got := propagatedText(next()); got != "RPUSH q ready" {
		t.Fatalf("shipped %q", got)
	}

	// A client blocked on q in database 0, where there is nothing.
	a := dialAsync(t, addr)
	defer a.close()
	a.send("BLPOP q 0")
	waitBlocked(t, admin, 1)

	if got := admin.cmd("SWAPDB 0 1"); got != "+OK" {
		t.Fatalf("SWAPDB = %q", got)
	}
	if got := propagatedText(next()); got != "SWAPDB 0 1" {
		t.Errorf("SWAPDB shipped %q; want it verbatim", got)
	}
	if got := a.reply(t, 5*time.Second); got != "[q ready]" {
		t.Errorf("the blocked client got %q; SWAPDB must wake it", got)
	}
	// The pop it performed is propagated in its own database, so a SELECT re-frames it.
	if got := propagatedText(next()); got != "SELECT 0" {
		t.Errorf("the served pop shipped %q; want SELECT 0 first", got)
	}
	if got := propagatedText(next()); got != "LPOP q" {
		t.Errorf("the served pop shipped %q", got)
	}
}
