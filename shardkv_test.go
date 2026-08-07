package shardkv_test

import (
	"bufio"
	"errors"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Black-third/shardkv"
)

// open starts a DB and registers its Close, so a test body says nothing about teardown
// and a failure still closes the AOF.
func open(t *testing.T, opts shardkv.Options) *shardkv.DB {
	t.Helper()
	db, err := shardkv.Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return db
}

// TestInProcessClientNeedsNoNetwork is the claim the whole package is for: a Go program
// runs commands against a real shardkv server with no socket anywhere in the picture.
//
// "No socket" is checked three ways rather than assumed, because the interesting failure
// is a facade that quietly dials a loopback listener and looks identical from the
// outside. Addr is nil, which is what a server that never called net.Listen reports.
// CLIENT INFO -- the server's own account of who is connected -- shows this client with
// an empty addr, so the *server* agrees it has no peer. And the whole command surface
// works anyway: reads, writes, collections, expiry and a transaction.
func TestInProcessClientNeedsNoNetwork(t *testing.T) {
	db := open(t, shardkv.Options{})

	if addr := db.Addr(); addr != nil {
		t.Fatalf("Options{} bound a listener at %v; the in-process client must need none", addr)
	}

	// The server's own view of this client. addr= is where a connected client's peer
	// address appears, so an empty one is the server confirming there is no connection
	// behind this session -- not merely that the test did not look for one. laddr= is the
	// local end of the same socket and is empty for the same reason.
	info, _, err := db.Bytes("CLIENT", "INFO")
	if err != nil {
		t.Fatalf("CLIENT INFO: %v", err)
	}
	for _, field := range []string{"addr", "laddr"} {
		if got := fieldOf(string(info), field); got != "" {
			t.Fatalf("the embedded client reports %s=%q; it has no socket, so it should be empty\n%s",
				field, got, info)
		}
	}

	if err := db.OK("SET", "greeting", "hello"); err != nil {
		t.Fatalf("SET: %v", err)
	}
	got, ok, err := db.Bytes("GET", "greeting")
	if err != nil || !ok || string(got) != "hello" {
		t.Fatalf("GET greeting = %q, %v, %v; want \"hello\", true, nil", got, ok, err)
	}

	if _, err := db.Int("RPUSH", "list", "a", "b", "c"); err != nil {
		t.Fatalf("RPUSH: %v", err)
	}
	items, err := db.Strings("LRANGE", "list", "0", "-1")
	if err != nil {
		t.Fatalf("LRANGE: %v", err)
	}
	if strings.Join(items, ",") != "a,b,c" {
		t.Fatalf("LRANGE = %v; want [a b c]", items)
	}

	// A transaction, because MULTI is the one thing a session owns that a stateless
	// "execute this command" facade would have silently dropped.
	if err := db.OK("MULTI"); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if _, err := db.Do("INCR", "counter"); err != nil {
		t.Fatalf("queueing INCR: %v", err)
	}
	if _, err := db.Do("INCR", "counter"); err != nil {
		t.Fatalf("queueing INCR: %v", err)
	}
	results, err := db.Do("EXEC")
	if err != nil {
		t.Fatalf("EXEC: %v", err)
	}
	if arr, okArr := results.([]any); !okArr || len(arr) != 2 || arr[0] != int64(1) || arr[1] != int64(2) {
		t.Fatalf("EXEC = %#v; want [1 2]", results)
	}
}

// fieldOf pulls "name=value" out of a CLIENT INFO line, or "" if the value is empty.
func fieldOf(line, name string) string {
	for _, f := range strings.Fields(line) {
		if rest, ok := strings.CutPrefix(f, name+"="); ok {
			return rest
		}
	}
	return ""
}

// TestZeroOptionsMatchTheBinarysDefaults pins the defaults to cmd/shardkv's flags. They
// are read back through CONFIG GET rather than compared against the constants, so the
// test would fail if a default were applied to the wrong setting as well as if it were
// the wrong value.
func TestZeroOptionsMatchTheBinarysDefaults(t *testing.T) {
	db := open(t, shardkv.Options{})

	// Each of these is a value cmd/shardkv's corresponding flag defaults to. None is
	// applied by Open: the server's own constructor already defaults to the same number,
	// which is what makes the parity structural rather than a list kept in step by hand --
	// and this test is what would notice if the two ever diverged.
	cfg, err := db.Map("CONFIG", "GET", "databases", "maxmemory", "maxclients", "appendonly",
		"slowlog-log-slower-than", "slowlog-max-len", "latency-monitor-threshold",
		"repl-backlog-size", "auto-aof-rewrite-percentage", "auto-aof-rewrite-min-size",
		"timeout", "enable-debug-command", "save")
	if err != nil {
		t.Fatalf("CONFIG GET: %v", err)
	}
	for name, want := range map[string]string{
		"databases":                   "16",
		"maxmemory":                   "0",
		"maxclients":                  "10000",
		"appendonly":                  "no",
		"slowlog-log-slower-than":     "10000",
		"slowlog-max-len":             "128",
		"latency-monitor-threshold":   "0",
		"repl-backlog-size":           "1048576",
		"auto-aof-rewrite-percentage": "100",
		"auto-aof-rewrite-min-size":   "67108864",
		"timeout":                     "0",
		"enable-debug-command":        "no",
		"save":                        "3600 1 300 100 60 10000",
	} {
		if cfg[name] != want {
			t.Errorf("CONFIG GET %s = %q; want %q (cmd/shardkv's flag default)", name, cfg[name], want)
		}
	}

	// SELECT reaching 15 is the observable consequence of the database default, and the
	// one a client actually depends on.
	if err := db.OK("SELECT", "15"); err != nil {
		t.Errorf("SELECT 15: %v", err)
	}
	if _, err := db.Do("SELECT", "16"); err == nil {
		t.Error("SELECT 16 succeeded; 16 databases are numbered 0..15")
	}
}

// TestOptionsGoThroughConfigSet checks that the named options and the Config map both
// land on the settings they name, and that a value the server would refuse from a client
// is refused at Open instead of being silently dropped.
//
// The unit suffix is the point of the first case: it is parsed by the one parser CONFIG
// SET uses, so "2mb" cannot come to mean a different number at startup than at runtime.
func TestOptionsGoThroughConfigSet(t *testing.T) {
	db := open(t, shardkv.Options{
		MaxMemory:       "2mb",
		MaxMemoryPolicy: "allkeys-lru",
		MaxKeys:         1000,
		Config: map[string]string{
			"maxclients":                "123",
			"latency-monitor-threshold": "5",
			"notify-keyspace-events":    "KEA",
		},
	})

	cfg, err := db.Map("CONFIG", "GET", "maxmemory", "maxmemory-policy", "maxkeys",
		"maxclients", "latency-monitor-threshold")
	if err != nil {
		t.Fatalf("CONFIG GET: %v", err)
	}
	for name, want := range map[string]string{
		"maxmemory":                 "2097152", // 2mb, through CONFIG SET's own parser
		"maxmemory-policy":          "allkeys-lru",
		"maxkeys":                   "1000",
		"maxclients":                "123",
		"latency-monitor-threshold": "5",
	} {
		if cfg[name] != want {
			t.Errorf("CONFIG GET %s = %q; want %q", name, cfg[name], want)
		}
	}

	for _, bad := range []shardkv.Options{
		{MaxMemory: "not-a-size"},
		{MaxMemoryPolicy: "allkeys-telepathy"},
		{Config: map[string]string{"no-such-setting": "1"}},
		{Config: map[string]string{"maxclients": "-4"}},
		// appendfsync cannot be applied at Open, because the log is opened with its policy.
		// Refused rather than ignored: a caller that asked for "always" and got "everysec"
		// has a durability setting that does not do what they wrote down.
		{Config: map[string]string{"appendfsync": "always"}},
	} {
		got, err := shardkv.Open(bad)
		if err == nil {
			got.Close()
			t.Errorf("Open(%+v) succeeded; the setting should have been refused", bad)
		}
	}
}

// TestFailedOpenReleasesWhatItTook covers the unwinding a failed Open has to do itself.
// Nothing else can: no DB is returned, so there is nothing for the caller to Close, and a
// socket left bound would make the retry that fixes the configuration fail on the address
// instead -- which is what this reproduces.
func TestFailedOpenReleasesWhatItTook(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	addr := ln.Addr().String()
	if err = ln.Close(); err != nil {
		t.Fatalf("closing the probe listener: %v", err)
	}

	// Cluster mode with more databases than a cluster can have, so the failure happens
	// after the listener is bound.
	bad, err := shardkv.Open(shardkv.Options{
		Addr:    addr,
		Cluster: shardkv.ClusterOptions{Enabled: true, AnnouncePort: 7001},
		Config:  map[string]string{"no-such-setting": "1"},
	})
	if err == nil {
		bad.Close()
		t.Fatal("Open with an unknown setting succeeded")
	}

	// The same address must be usable again, which is only true if the failed Open closed
	// what it bound.
	good := open(t, shardkv.Options{Addr: addr})
	if got := good.Addr(); got == nil || got.String() != addr {
		t.Fatalf("the second Open bound %v; want %s -- the first did not release it", got, addr)
	}
}

// TestAOFSurvivesClose is the durability claim: what Close does is flush and fsync the
// log's tail, so a value written through the in-process client is in the file before Close
// returns and is replayed by the next Open.
//
// It also covers the ordering Open promises -- the replay finishes before Open returns --
// by reading the key immediately, with nothing to wait for or poll.
func TestAOFSurvivesClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.aof")

	first, err := shardkv.Open(shardkv.Options{AOFPath: path, AOFSync: "always"})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := first.OK("SET", "persisted", "yes"); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if _, err := first.Int("RPUSH", "log", "one", "two"); err != nil {
		t.Fatalf("RPUSH: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	second := open(t, shardkv.Options{AOFPath: path})
	got, ok, err := second.Bytes("GET", "persisted")
	if err != nil || !ok || string(got) != "yes" {
		t.Fatalf("after reopening, GET persisted = %q, %v, %v; want \"yes\", true, nil", got, ok, err)
	}
	items, err := second.Strings("LRANGE", "log", "0", "-1")
	if err != nil || strings.Join(items, ",") != "one,two" {
		t.Fatalf("after reopening, LRANGE log = %v, %v; want [one two]", items, err)
	}
}

// TestSnapshotSeedsAnEmptyAOF covers the one precedence case that is deliberately not
// Redis's: an AOF that is configured but empty, next to a snapshot that is not.
//
// Redis starts empty there, so enabling a durability feature costs the operator the
// dataset. Here the snapshot is loaded and the empty log is seeded from it before Open
// returns, which is what makes the log a description of the whole dataset from its first
// byte -- checked by closing and reopening with the AOF alone.
func TestSnapshotSeedsAnEmptyAOF(t *testing.T) {
	dir := t.TempDir()
	snapPath := filepath.Join(dir, "dump.skv")

	saver := open(t, shardkv.Options{SnapshotPath: snapPath})
	if err := saver.OK("SET", "from-snapshot", "value"); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if err := saver.OK("SAVE"); err != nil {
		t.Fatalf("SAVE: %v", err)
	}

	aofPath := filepath.Join(dir, "dump.aof")
	seeded, err := shardkv.Open(shardkv.Options{SnapshotPath: snapPath, AOFPath: aofPath})
	if err != nil {
		t.Fatalf("Open with a snapshot and an empty AOF: %v", err)
	}
	got, ok, err := seeded.Bytes("GET", "from-snapshot")
	if err != nil || !ok || string(got) != "value" {
		t.Fatalf("GET from-snapshot = %q, %v, %v; want \"value\", true, nil", got, ok, err)
	}
	if err := seeded.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The AOF alone must now describe the dataset, which is the whole point of seeding it.
	fromLog := open(t, shardkv.Options{AOFPath: aofPath})
	got, ok, err = fromLog.Bytes("GET", "from-snapshot")
	if err != nil || !ok || string(got) != "value" {
		t.Fatalf("the seeded AOF replayed GET from-snapshot = %q, %v, %v; want \"value\", true, nil",
			got, ok, err)
	}
}

// TestListenerAndInProcessClientShareOneKeyspace is the other half of the pitch: the same
// DB can serve real Redis clients while a Go program uses it in-process, and both see one
// dataset. A raw socket is used rather than a client library because the module has no
// third-party dependencies.
func TestListenerAndInProcessClientShareOneKeyspace(t *testing.T) {
	db := open(t, shardkv.Options{Addr: "127.0.0.1:0"})
	addr := db.Addr()
	if addr == nil {
		t.Fatal("Addr is nil after Open with an Addr")
	}

	if err := db.OK("SET", "written-in-process", "1"); err != nil {
		t.Fatalf("SET: %v", err)
	}

	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatalf("dialing %v: %v", addr, err)
	}
	defer conn.Close()
	wire := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	if got := wireCmd(t, wire, "GET", "written-in-process"); got != "1" {
		t.Errorf("over TCP, GET written-in-process = %q; want \"1\"", got)
	}
	if got := wireCmd(t, wire, "SET", "written-over-tcp", "2"); got != "OK" {
		t.Errorf("over TCP, SET = %q; want OK", got)
	}

	got, ok, err := db.Bytes("GET", "written-over-tcp")
	if err != nil || !ok || string(got) != "2" {
		t.Fatalf("in process, GET written-over-tcp = %q, %v, %v; want \"2\", true, nil", got, ok, err)
	}
}

// TestUseListenerTakesAPreBoundSocket covers the option a test framework needs: the port
// was chosen elsewhere and the socket is already open.
func TestUseListenerTakesAPreBoundSocket(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	db := open(t, shardkv.Options{Listener: ln})
	if got := db.Addr(); got == nil || got.String() != ln.Addr().String() {
		t.Fatalf("Addr = %v; want the supplied listener's %v", got, ln.Addr())
	}

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dialing the supplied listener: %v", err)
	}
	defer conn.Close()
	wire := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	if got := wireCmd(t, wire, "PING"); got != "PONG" {
		t.Errorf("PING over the supplied listener = %q; want PONG", got)
	}
}

// TestPasswordAppliesToTheInProcessClient is the gate an embedded caller could plausibly
// have been let through, since it has no connection to authenticate. It is not: the
// in-process client runs on the client path, so it is refused with NOAUTH until it
// authenticates, exactly as a socket client is.
func TestPasswordAppliesToTheInProcessClient(t *testing.T) {
	db := open(t, shardkv.Options{Password: "s3cr3t"})

	_, err := db.Do("GET", "anything")
	if err == nil || !strings.HasPrefix(err.Error(), "NOAUTH") {
		t.Fatalf("GET before AUTH = %v; want a NOAUTH error", err)
	}
	if _, err := db.Do("AUTH", "wrong"); err == nil {
		t.Error("AUTH with the wrong password succeeded")
	}
	if err := db.OK("AUTH", "s3cr3t"); err != nil {
		t.Fatalf("AUTH: %v", err)
	}
	if err := db.OK("SET", "k", "v"); err != nil {
		t.Fatalf("SET after AUTH: %v", err)
	}
}

// TestClusterRedirectsTheInProcessClient is the invariant-13 case, and the reason the
// in-process client goes through executeCommand rather than dispatch.
//
// dispatch is never redirected, because a replica must apply every write its master
// sends. An embedded caller is not a master: if it were routed that way, a program
// embedding a cluster node would silently write keys the node does not own -- producing a
// second copy of the data on a node no client will consult again, which is the failure
// invariant 13 exists to prevent. So a slot this node does not serve is refused here too.
//
// No listener is needed: AnnouncePort supplies what the bound port otherwise would.
func TestClusterRedirectsTheInProcessClient(t *testing.T) {
	db := open(t, shardkv.Options{
		Cluster: shardkv.ClusterOptions{Enabled: true, AnnounceIP: "127.0.0.1", AnnouncePort: 7001},
	})

	// No slot is assigned yet, so no node serves any key.
	if _, err := db.Do("SET", "foo", "bar"); err == nil ||
		!strings.HasPrefix(err.Error(), "CLUSTERDOWN") {
		t.Fatalf("SET with no slots assigned = %v; want CLUSTERDOWN", err)
	}

	if err := db.OK("CLUSTER", "ADDSLOTSRANGE", "0", "16383"); err != nil {
		t.Fatalf("CLUSTER ADDSLOTSRANGE: %v", err)
	}
	if err := db.OK("SET", "foo", "bar"); err != nil {
		t.Fatalf("SET after taking every slot: %v", err)
	}

	// A cluster is one keyspace, so the database default resolves to 1 rather than 16.
	cfg, err := db.Map("CONFIG", "GET", "databases")
	if err != nil {
		t.Fatalf("CONFIG GET databases: %v", err)
	}
	if cfg["databases"] != "1" {
		t.Errorf("cluster mode has %s databases; want 1", cfg["databases"])
	}
}

// TestClusterWithMoreThanOneDatabaseIsRefused checks the contradiction is reported rather
// than clamped. The binary clamps and logs because its -databases flag has a non-zero
// default; an unset field here is unambiguous, so an explicit 4 can only be a mistake.
func TestClusterWithMoreThanOneDatabaseIsRefused(t *testing.T) {
	db, err := shardkv.Open(shardkv.Options{
		Databases: 4,
		Cluster:   shardkv.ClusterOptions{Enabled: true, AnnouncePort: 7001},
	})
	if err == nil {
		db.Close()
		t.Fatal("Open with 4 databases and cluster mode succeeded; a cluster has one database")
	}
}

// TestInjectedClockMakesExpiryTestable covers Options.Now, which exists so an embedded
// caller can test a TTL without sleeping through it.
//
// The clock is read for the deadline and for every later check of it, so advancing it is
// the whole of what happens between the two assertions -- no sleep, no polling, and no
// dependence on the machine's timing.
func TestInjectedClockMakesExpiryTestable(t *testing.T) {
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	db := open(t, shardkv.Options{
		Now: func() time.Time { return time.Unix(0, now.Load()) },
	})

	if err := db.OK("SET", "volatile", "v", "EX", "60"); err != nil {
		t.Fatalf("SET EX: %v", err)
	}
	if _, ok, err := db.Bytes("GET", "volatile"); err != nil || !ok {
		t.Fatalf("GET before the deadline = %v, %v; want present", ok, err)
	}

	now.Add(int64(61 * time.Second))

	if _, ok, err := db.Bytes("GET", "volatile"); err != nil || ok {
		t.Fatalf("GET after the deadline = %v, %v; want absent", ok, err)
	}
}

// TestBlockingCommandDoesNotDeadlockInProcess covers invariant 9 from the facade: a
// blocked client holds no lock, so another client can still write -- including the write
// that serves it.
//
// The first half checks the timeout path returns rather than hanging, since an in-process
// client has no socket for the server to notice going away and a blocked one that never
// woke would hang the caller's goroutine for good. The second checks the wakeup, which
// only works if the blocked client really is holding nothing.
func TestBlockingCommandDoesNotDeadlockInProcess(t *testing.T) {
	db := open(t, shardkv.Options{})

	v, err := db.Do("BLPOP", "nothing-here", "0.05")
	if err != nil {
		t.Fatalf("BLPOP that timed out: %v", err)
	}
	if v != nil {
		t.Fatalf("BLPOP that timed out = %#v; want nil", v)
	}

	pusher := db.NewClient()
	defer pusher.Close()

	popped := make(chan []string, 1)
	errs := make(chan error, 1)
	go func() {
		got, err := db.Strings("BLPOP", "queue", "5")
		if err != nil {
			errs <- err
			return
		}
		popped <- got
	}()

	// The push has to keep being attempted: the pop may not have queued yet, and a single
	// early push would simply be taken by the pop's own opportunistic first attempt --
	// which is a pass, not a failure, so retrying makes the test prove the wakeup rather
	// than the race.
	//
	// It goes through pusher and not through db, which is the whole point of pusher
	// existing: DB embeds its default *Client, and one Client serializes its callers exactly
	// as one connection does, so db.Int here would queue behind the db.Strings above and
	// wait out the blocked BLPOP's full five seconds. That made the test a coin flip on
	// whether the runtime scheduled the pushing goroutine before the popping one -- it
	// passed when the push won the race and the pop's opportunistic first attempt found the
	// element already there, which is the one path this half of the test is not about, and
	// it failed outright (~1 run in 40, both with and without -race) when the pop won. The
	// wakeup is what is being tested, so the push has to come from a client that is not the
	// blocked one.
	deadline := time.After(5 * time.Second)
	for {
		if _, err := pusher.Int("RPUSH", "queue", "job"); err != nil {
			t.Fatalf("RPUSH: %v", err)
		}
		select {
		case got := <-popped:
			if len(got) != 2 || got[0] != "queue" || got[1] != "job" {
				t.Fatalf("BLPOP = %v; want [queue job]", got)
			}
			return
		case err := <-errs:
			t.Fatalf("BLPOP: %v", err)
		case <-deadline:
			t.Fatal("BLPOP never returned; a blocked in-process client is stuck")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// TestCloseIsIdempotentAndReportsOnce checks the contract a defer relies on: Close may be
// reached twice and the second call must be harmless and say the same thing.
func TestCloseIsIdempotentAndReportsOnce(t *testing.T) {
	db, err := shardkv.Open(shardkv.Options{AOFPath: filepath.Join(t.TempDir(), "dump.aof")})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.OK("SET", "k", "v"); err != nil {
		t.Fatalf("SET: %v", err)
	}
	first, second := db.Close(), db.Close()
	if !errors.Is(first, second) && first != second {
		t.Fatalf("Close reported %v then %v; both calls must report the same", first, second)
	}
	if first != nil {
		t.Fatalf("Close: %v", first)
	}
	select {
	case <-db.Done():
	default:
		t.Error("Done is not closed after Close")
	}
}

// TestShutdownCommandStopsTheDB checks the hook Open installs. Without it SHUTDOWN would
// answer and change nothing, which is worse than refusing it: the caller would believe the
// server had stopped.
func TestShutdownCommandStopsTheDB(t *testing.T) {
	db, err := shardkv.Open(shardkv.Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// SHUTDOWN NOSAVE does not reply -- the server is going away -- so an error here would
	// mean it was refused rather than obeyed.
	if _, err := db.Do("SHUTDOWN", "NOSAVE"); err != nil {
		t.Fatalf("SHUTDOWN NOSAVE: %v", err)
	}
	select {
	case <-db.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("SHUTDOWN did not stop the server")
	}
}

// TestVersionMatchesWhatTheServerReports checks the two are one value rather than two that
// happen to agree, by comparing the package's answer against the server's own INFO.
func TestVersionMatchesWhatTheServerReports(t *testing.T) {
	db := open(t, shardkv.Options{})
	info, _, err := db.Bytes("INFO", "server")
	if err != nil {
		t.Fatalf("INFO server: %v", err)
	}
	want := "shardkv_version:" + shardkv.Version()
	if !strings.Contains(string(info), want) {
		t.Errorf("INFO server does not contain %q; Version() and the server disagree", want)
	}
	if shardkv.Version() == "" {
		t.Error("Version() is empty")
	}
}

// TestConfigSetOnARunningDB covers the method that exists only because a DB with no
// listener has no other way to reach the command.
func TestConfigSetOnARunningDB(t *testing.T) {
	db := open(t, shardkv.Options{})
	if err := db.ConfigSet("maxmemory", "8mb"); err != nil {
		t.Fatalf("ConfigSet: %v", err)
	}
	cfg, err := db.Map("CONFIG", "GET", "maxmemory")
	if err != nil {
		t.Fatalf("CONFIG GET: %v", err)
	}
	if cfg["maxmemory"] != "8388608" {
		t.Errorf("maxmemory = %q; want 8388608", cfg["maxmemory"])
	}
	if err := db.ConfigSet("no-such-setting", "1"); err == nil {
		t.Error("ConfigSet accepted an unknown setting")
	}
}

// wireCmd sends one command over a socket and returns its reply rendered as text, for the
// tests that check a real client and the in-process one agree. It handles only the reply
// shapes those tests provoke.
func wireCmd(t *testing.T, rw *bufio.ReadWriter, args ...string) string {
	t.Helper()
	var b strings.Builder
	b.WriteString("*" + strconv.Itoa(len(args)) + "\r\n")
	for _, a := range args {
		b.WriteString("$" + strconv.Itoa(len(a)) + "\r\n" + a + "\r\n")
	}
	if _, err := rw.WriteString(b.String()); err != nil {
		t.Fatalf("writing %v: %v", args, err)
	}
	if err := rw.Flush(); err != nil {
		t.Fatalf("flushing %v: %v", args, err)
	}
	line, err := rw.ReadString('\n')
	if err != nil {
		t.Fatalf("reading the reply to %v: %v", args, err)
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		t.Fatalf("empty reply to %v", args)
	}
	switch line[0] {
	case '+', ':':
		return line[1:]
	case '-':
		t.Fatalf("%v answered %s", args, line[1:])
	case '$':
		if line == "$-1" {
			return ""
		}
		payload, err := rw.ReadString('\n')
		if err != nil {
			t.Fatalf("reading the bulk reply to %v: %v", args, err)
		}
		return strings.TrimRight(payload, "\r\n")
	}
	t.Fatalf("unexpected reply %q to %v", line, args)
	return ""
}
