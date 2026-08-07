// Command shardkv runs the sharded in-memory data-structure server.
//
// Usage:
//
//	shardkv [-addr :6380] [-shards 256] [-sweep 1s] [-maxkeys n]
//	        [-databases 16]
//	        [-cluster-enabled] [-cluster-config-file nodes.conf]
//	        [-cluster-announce-ip host] [-cluster-announce-port n]
//	        [-aof dump.aof] [-aofsync everysec|always|no]
//	        [-aof-rewrite-min-size bytes] [-aof-rewrite-percentage n]
//	        [-snapshot dump.skv] [-save "<seconds> <changes> ..."]
//	        [-replicaof host:port] [-repl-backlog-size bytes]
//	        [-requirepass secret] [-masterauth secret]
//	        [-tls-cert f] [-tls-key f] [-tls-ca f] [-tls-replication]
//	        [-notify-keyspace-events KEA]
//	        [-enable-debug-command no|yes|local]
//	        [-slowlog-log-slower-than us] [-slowlog-max-len n]
//	        [-latency-monitor-threshold ms]
//
// It speaks RESP, so any Redis client works:
//
//	redis-cli -p 6380 set foo bar
//	redis-cli -p 6380 zadd board 100 alice 200 bob
//	redis-cli -p 6380 zrange board 0 -1 withscores
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Black-third/shardkv/internal/aof"
	"github.com/Black-third/shardkv/internal/server"
	"github.com/Black-third/shardkv/internal/store"
)

// main keeps no logic of its own: run does the work and returns, so every defer
// it registered -- above all the one that flushes and closes the AOF -- has
// already executed by the time a fatal error terminates the process. Calling
// log.Fatalf from inside run would skip those defers and lose the log's buffered
// tail, silently discarding writes the client was told were durable.
func main() {
	if err := run(); err != nil {
		log.Fatalf("shardkv: %v", err)
	}
	log.Println("shardkv: shut down cleanly")
}

func run() error {
	addr := flag.String("addr", ":6380", "TCP address to listen on")
	shards := flag.Int("shards", 256, "number of lock shards (rounded up to a power of two)")
	sweep := flag.Duration("sweep", time.Second, "interval between active expiration sweeps")
	aofPath := flag.String("aof", "", "append-only file path for persistence (empty disables it)")
	aofSync := flag.String("aofsync", "everysec", "AOF sync policy: everysec|always|no")
	replicaOf := flag.String("replicaof", "", "replicate from master at host:port (empty = master)")
	maxKeys := flag.Int("maxkeys", 0, "approximate-LRU eviction cap on live keys, per database (0 = unbounded)")
	databases := flag.Int("databases", 16, "number of databases SELECT can switch between")
	requirePass := flag.String("requirepass", "", "require AUTH with this password (empty = no authentication)")
	masterAuth := flag.String("masterauth", "", "password presented to the master when replicating")
	tlsCert := flag.String("tls-cert", "", "PEM certificate for the listener (enables TLS with -tls-key)")
	tlsKey := flag.String("tls-key", "", "PEM private key for -tls-cert")
	tlsCA := flag.String("tls-ca", "", "PEM CA roots used to verify peers")
	tlsRepl := flag.Bool("tls-replication", false, "dial the master over TLS, using the same certificate material")
	backlogSize := flag.Int("repl-backlog-size", 1<<20, "bytes of replication stream retained for partial resync")
	notifyEvents := flag.String("notify-keyspace-events", "",
		"keyspace notification classes, Redis's flag characters K/E/g/$/l/s/h/z/x/e/A (empty disables)")
	aofRewriteMinSize := flag.Int64("aof-rewrite-min-size", 64<<20,
		"smallest AOF size that may trigger an automatic rewrite, in bytes")
	aofRewritePerc := flag.Int("aof-rewrite-percentage", 100,
		"growth over the size after the last rewrite that triggers one (0 disables)")
	snapPath := flag.String("snapshot", "",
		"point-in-time snapshot file for SAVE/BGSAVE (empty disables it; not an RDB file)")
	saveSpec := flag.String("save", "3600 1 300 100 60 10000",
		"automatic snapshot schedule as <seconds> <changes> pairs (empty disables it; "+
			"requires -snapshot)")
	slowlogSlower := flag.Int64("slowlog-log-slower-than", 10000,
		"microseconds a command must take to be recorded in the slow log (negative disables, 0 logs all)")
	slowlogMaxLen := flag.Int64("slowlog-max-len", 128, "how many entries the slow log retains")
	maxClients := flag.Int("maxclients", 10000,
		"maximum simultaneous client connections (further ones are refused and closed)")
	clientTimeout := flag.Int64("timeout", 0,
		"seconds a client may be idle before being disconnected (0 = never; replicas, "+
			"subscribers, monitors and blocked clients are exempt)")
	latencyThreshold := flag.Int64("latency-monitor-threshold", 0,
		"milliseconds an event must take to be recorded by the latency monitor (0 disables)")
	enableDebugCmd := flag.String("enable-debug-command", "no",
		"whether DEBUG may be run: no|yes|local (local = loopback connections only)")
	clusterEnabled := flag.Bool("cluster-enabled", false,
		"run as a cluster node: keys are routed by hash slot and foreign slots are redirected")
	clusterConfig := flag.String("cluster-config-file", "",
		"file the node id and slot map are persisted to (empty keeps them in memory)")
	clusterAnnounceIP := flag.String("cluster-announce-ip", "",
		"address other nodes and redirected clients should use (empty = 127.0.0.1)")
	clusterAnnouncePort := flag.Int("cluster-announce-port", 0,
		"port other nodes and redirected clients should use (0 = the listening port)")
	// The maxmemory family takes strings so a flag accepts exactly what CONFIG SET accepts,
	// units included, and is refused with the same sentence. The defaults shown here are
	// descriptions rather than values applied: an unpassed flag is not applied at all (see
	// below), because -maxmemory-policy's real default is derived from -maxkeys.
	flag.String("maxmemory", "0",
		"memory budget for the dataset before eviction begins; a byte count or a size with a "+
			"unit (100mb, 1gb, 512kb), 0 = unbounded")
	flag.String("maxmemory-policy", "derived from -maxkeys",
		"what happens at the budget: noeviction|allkeys-lru|allkeys-lfu|allkeys-random|"+
			"volatile-lru|volatile-lfu|volatile-random|volatile-ttl")
	flag.String("maxmemory-samples", "16",
		"keys the eviction sampler examines before choosing the victim among them (16 rather "+
			"than Redis's 5: a sample here comes from one shard, so it sees 1/256th of the keyspace)")
	flag.Parse()

	st := store.New(*shards)
	st.SetMaxKeys(*maxKeys)

	sigCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	// A cancelable child of the signal context, so SHUTDOWN can stop the server through
	// exactly the same path a signal takes: Serve returns, run unwinds, and the deferred
	// close of the AOF flushes its buffered tail.
	ctx, shutdown := context.WithCancel(sigCtx)
	defer shutdown()

	// Create the server (which registers each store's removal hook) before starting the
	// janitors that may invoke them.
	srv := server.New(st)
	// A cluster has one database, as in Redis: the slot map partitions a single keyspace
	// across nodes, so a second database would be a keyspace no slot covers and therefore
	// no node is responsible for. It is forced rather than rejected because -databases
	// has a non-zero default, so demanding the operator also pass -databases 1 would make
	// -cluster-enabled fail on its own.
	if *clusterEnabled && *databases != 1 {
		log.Printf("shardkv: cluster mode uses database 0 only; ignoring -databases %d", *databases)
		*databases = 1
	}
	if err := srv.SetDatabases(*databases); err != nil {
		return fmt.Errorf("invalid -databases %d: %w", *databases, err)
	}
	// The maxmemory family, applied through the same table CONFIG SET uses (Server.ConfigSet)
	// so a startup flag and a runtime change cannot come to mean different things.
	//
	// Only flags the operator actually passed are applied, which is what flag.Visit reports.
	// A flat default would not be harmless here: -maxmemory-policy's default is *derived*
	// (see maxmemory.go's evictionPolicy), so a server started with -maxkeys and no policy
	// evicts by allkeys-lru -- and unconditionally applying a "noeviction" default would turn
	// eviction off for every existing -maxkeys deployment, silently, on upgrade.
	//
	// After SetDatabases, because -maxmemory-samples applies to every database.
	passed := make(map[string]string)
	flag.Visit(func(f *flag.Flag) { passed[f.Name] = f.Value.String() })
	for _, name := range []string{"maxmemory", "maxmemory-policy", "maxmemory-samples"} {
		value, ok := passed[name]
		if !ok {
			continue
		}
		if err := srv.ConfigSet(name, value); err != nil {
			return fmt.Errorf("invalid -%s %q: %w", name, value, err)
		}
	}
	srv.SetRequirePass(*requirePass)
	srv.SetMasterAuth(*masterAuth)
	srv.SetReplBacklogSize(*backlogSize)
	srv.SetAOFRewritePolicy(*aofRewriteMinSize, *aofRewritePerc)
	srv.SetSlowlogPolicy(*slowlogSlower, *slowlogMaxLen)
	srv.SetLatencyThresholdMs(*latencyThreshold)
	srv.SetMaxClients(*maxClients)
	srv.SetClientTimeoutSecs(*clientTimeout)
	srv.SetShutdownHook(shutdown)
	if !srv.SetNotifyKeyspaceEvents(*notifyEvents) {
		return fmt.Errorf("invalid -notify-keyspace-events %q: use the flag characters K E g $ l s h z x e A", *notifyEvents)
	}
	// DEBUG stays shut unless it is asked for, which is Redis 7's default: two of its
	// subcommands change server-wide behaviour (the expiry sweep, the replication ID), and
	// nothing a client library does in normal operation calls DEBUG at all.
	if !srv.SetEnableDebugCommand(*enableDebugCmd) {
		return fmt.Errorf("invalid -enable-debug-command %q: use no, yes or local", *enableDebugCmd)
	}

	// TLS is opt-in: with no certificate the listener and the replication dialer stay
	// plain TCP, so an existing deployment is unaffected.
	tlsOpts := server.TLSOptions{CertFile: *tlsCert, KeyFile: *tlsKey, CAFile: *tlsCA}
	if tlsOpts.Enabled() {
		if err := srv.EnableTLS(tlsOpts); err != nil {
			return fmt.Errorf("configuring TLS: %w", err)
		}
	}
	if *tlsRepl {
		if err := srv.EnableMasterTLS(tlsOpts); err != nil {
			return fmt.Errorf("configuring replication TLS: %w", err)
		}
	}

	// Snapshots. The path and schedule are configured before anything is loaded, because
	// LoadSnapshot reads the path from the server and because an invalid schedule should
	// fail the process before it has touched the dataset.
	srv.SetSnapshotPath(*snapPath)
	if !srv.SetSaveSchedule(*saveSpec) {
		return fmt.Errorf("invalid -save %q: use whitespace-separated <seconds> <changes> pairs "+
			"(both non-negative), or \"\" to disable", *saveSpec)
	}

	// Persistence: restore the dataset, then attach the AOF for new writes.
	//
	// Precedence when both a snapshot and an AOF exist: the AOF wins, which is Redis's rule
	// (its loadDataFromDisk loads the AOF when appendonly is on and the RDB only otherwise)
	// and the right one -- the AOF records every write up to the crash, while the snapshot
	// records only up to the last save, so the AOF is by construction the more recent of the
	// two. Loading the snapshot as well would double-apply everything the AOF also holds.
	//
	// The one case that is *not* Redis's: an AOF that is configured but empty, next to a
	// snapshot that is not. Redis starts empty there and the operator loses the dataset by
	// enabling a durability feature. Here the snapshot is loaded and the AOF is then
	// rewritten from it immediately, before any client can connect, so the log describes the
	// whole dataset from its first byte. A failure of that rewrite is fatal: continuing would
	// leave an AOF that is authoritative on the next restart and describes only part of what
	// is in memory.
	fromSnapshot, err := restoreDataset(srv, *aofPath)
	if err != nil {
		return err
	}
	if *aofPath != "" {
		policy, ok := aof.ParseSyncPolicy(*aofSync)
		if !ok {
			return fmt.Errorf("invalid -aofsync %q: use always, everysec or no", *aofSync)
		}
		logf, err := aof.Open(*aofPath, policy)
		if err != nil {
			return fmt.Errorf("opening AOF: %w", err)
		}
		// Closing the log is what flushes and fsyncs its tail, so a failure here is
		// a durability failure at shutdown. run() may already be returning an error,
		// so report this one rather than replacing it.
		defer func() {
			if cerr := logf.Close(); cerr != nil {
				log.Printf("shardkv: closing AOF failed, the tail may be unpersisted: %v", cerr)
			}
		}()
		srv.AttachAOF(logf)
		if fromSnapshot {
			// The dataset came from the snapshot, so the log that is now the authority on the
			// next restart knows nothing about it yet.
			if err := srv.RewriteAOF(); err != nil {
				return fmt.Errorf("writing the loaded snapshot into the empty AOF at %s: %w", *aofPath, err)
			}
			log.Printf("shardkv: wrote the loaded snapshot into the empty AOF at %s", *aofPath)
		}
	}

	// The automatic snapshot schedule, alongside the expiry janitors below and for the same
	// reason: it is a background pass whose lifetime is the process's, and starting it where
	// the flag is read keeps "was this asked for" in one place. It is inert -- two atomic
	// loads a second -- with no snapshot path or an empty schedule.
	go srv.SnapshotScheduler(ctx)

	// Start background expiration/eviction now that the removal hooks are wired. Each
	// database is an independent keyspace with its own shards, so each gets its own
	// sweep: one janitor walking all of them would hold every database's progress
	// hostage to the largest.
	for i := 0; i < *databases; i++ {
		go srv.DB(i).Janitor(ctx, *sweep)
	}

	// Listen before announcing: the socket is bound and queueing connections from
	// here on, so the address in the log is already reachable even though the
	// accept loop only starts below in Serve.
	if err := srv.Listen(*addr); err != nil {
		return err
	}
	log.Printf("shardkv %s listening on %s", server.Version, srv.Addr())

	// Cluster mode is enabled after Listen, because a node's announced port defaults to
	// the one it actually bound -- which is only known once the listener exists, and is
	// not knowable at all for a test binding port 0.
	if *clusterEnabled {
		if err := srv.EnableCluster(server.ClusterOptions{
			ConfigFile:   *clusterConfig,
			AnnounceIP:   *clusterAnnounceIP,
			AnnouncePort: *clusterAnnouncePort,
		}); err != nil {
			return fmt.Errorf("enabling cluster mode: %w", err)
		}
		log.Printf("shardkv: cluster mode, node %s", srv.ClusterMyID())
	}

	// Replication: start as a replica if requested.
	if *replicaOf != "" {
		srv.ReplicaOf(ctx, *replicaOf)
		log.Printf("shardkv: replicating from %s", *replicaOf)
	}

	return srv.Serve(ctx)
}

// restoreDataset rebuilds the dataset from whichever of the two records exists, and reports
// whether it came from the snapshot (in which case the caller has an empty AOF to seed).
//
// The precedence, and why: an AOF is a history and a snapshot is a state, so where both hold
// data the AOF is by construction at least as recent -- it recorded every write up to the
// crash, while the snapshot stopped at the last save. Redis takes the same view
// (loadDataFromDisk loads the AOF when appendonly is on and the RDB only otherwise) and
// applying both would double-apply every write the snapshot already describes: harmless for
// SET, wrong for RPUSH, LPUSH, SADD's counts, XADD and every other command whose replay is
// not idempotent.
//
// A malformed snapshot fails the process. That is deliberate and it is the opposite of the
// AOF's behaviour, which stops at a torn record and serves what it read: the AOF's tail is
// the expected shape of a crash, whereas a snapshot is written to a temporary file and
// renamed, so it is whole or absent and anything else has been damaged after the fact.
// Serving part of it would present a subset of the dataset as the whole of it, and the next
// save would then write that subset over the good copy.
func restoreDataset(srv *server.Server, aofPath string) (fromSnapshot bool, err error) {
	if aofPath != "" {
		cmds, err := aof.Load(aofPath)
		if err != nil {
			return false, fmt.Errorf("loading AOF: %w", err)
		}
		if len(cmds) > 0 {
			srv.ReplayCommands(cmds)
			log.Printf("shardkv: replayed %d commands from %s", len(cmds), aofPath)
			if srv.SnapshotPath() != "" {
				log.Printf("shardkv: ignoring the snapshot at %s: the AOF is the more recent record",
					srv.SnapshotPath())
			}
			return false, nil
		}
	}
	keys, savedAt, err := srv.LoadSnapshot()
	if err != nil {
		return false, fmt.Errorf("loading snapshot %s: %w", srv.SnapshotPath(), err)
	}
	if keys == 0 {
		return false, nil
	}
	log.Printf("shardkv: loaded %d keys from the snapshot at %s, taken %s",
		keys, srv.SnapshotPath(), savedAt.Format(time.RFC3339))
	return true, nil
}
