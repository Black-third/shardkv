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
//	        [-replicaof host:port] [-repl-backlog-size bytes]
//	        [-requirepass secret] [-masterauth secret]
//	        [-tls-cert f] [-tls-key f] [-tls-ca f] [-tls-replication]
//	        [-notify-keyspace-events KEA]
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
	clusterEnabled := flag.Bool("cluster-enabled", false,
		"run as a cluster node: keys are routed by hash slot and foreign slots are redirected")
	clusterConfig := flag.String("cluster-config-file", "",
		"file the node id and slot map are persisted to (empty keeps them in memory)")
	clusterAnnounceIP := flag.String("cluster-announce-ip", "",
		"address other nodes and redirected clients should use (empty = 127.0.0.1)")
	clusterAnnouncePort := flag.Int("cluster-announce-port", 0,
		"port other nodes and redirected clients should use (0 = the listening port)")
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

	// Persistence: replay an existing AOF, then attach the log for new writes.
	if *aofPath != "" {
		cmds, err := aof.Load(*aofPath)
		if err != nil {
			return fmt.Errorf("loading AOF: %w", err)
		}
		if len(cmds) > 0 {
			srv.ReplayCommands(cmds)
			log.Printf("shardkv: replayed %d commands from %s", len(cmds), *aofPath)
		}
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
	}

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
