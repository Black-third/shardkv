// Command shardkv runs the sharded in-memory data-structure server.
//
// Usage:
//
//	shardkv [-addr :6380] [-shards 256] [-sweep 1s]
//	        [-aof dump.aof] [-aofsync everysec|always|no]
//	        [-replicaof host:port]
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
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Black-third/shardkv/internal/aof"
	"github.com/Black-third/shardkv/internal/server"
	"github.com/Black-third/shardkv/internal/store"
)

func main() {
	addr := flag.String("addr", ":6380", "TCP address to listen on")
	shards := flag.Int("shards", 256, "number of lock shards (rounded up to a power of two)")
	sweep := flag.Duration("sweep", time.Second, "interval between active expiration sweeps")
	aofPath := flag.String("aof", "", "append-only file path for persistence (empty disables it)")
	aofSync := flag.String("aofsync", "everysec", "AOF sync policy: everysec|always|no")
	replicaOf := flag.String("replicaof", "", "replicate from master at host:port (empty = master)")
	maxKeys := flag.Int("maxkeys", 0, "approximate-LRU eviction cap on live keys (0 = unbounded)")
	flag.Parse()

	st := store.New(*shards)
	st.SetMaxKeys(*maxKeys)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Create the server (which registers the store's removal hook) before
	// starting the janitor that may invoke it.
	srv := server.New(st)

	// Persistence: replay an existing AOF, then attach the log for new writes.
	if *aofPath != "" {
		cmds, err := aof.Load(*aofPath)
		if err != nil {
			log.Fatalf("shardkv: loading AOF: %v", err)
		}
		if len(cmds) > 0 {
			srv.ReplayCommands(cmds)
			log.Printf("shardkv: replayed %d commands from %s", len(cmds), *aofPath)
		}
		logf, err := aof.Open(*aofPath, parseSyncPolicy(*aofSync))
		if err != nil {
			log.Fatalf("shardkv: opening AOF: %v", err)
		}
		defer logf.Close()
		srv.AttachAOF(logf)
	}

	// Start background expiration/eviction now that the removal hook is wired.
	go st.Janitor(ctx, *sweep)

	if err := srv.Listen(*addr); err != nil {
		log.Fatalf("shardkv: %v", err)
	}
	log.Printf("shardkv %s listening on %s", "0.2.0", srv.Addr())

	// Replication: start as a replica if requested.
	if *replicaOf != "" {
		srv.ReplicaOf(ctx, *replicaOf)
		log.Printf("shardkv: replicating from %s", *replicaOf)
	}

	if err := srv.Serve(ctx); err != nil {
		log.Fatalf("shardkv: %v", err)
	}
	log.Println("shardkv: shut down cleanly")
}

func parseSyncPolicy(s string) aof.SyncPolicy {
	switch s {
	case "always":
		return aof.SyncAlways
	case "no":
		return aof.SyncNo
	default:
		return aof.SyncEverySec
	}
}
