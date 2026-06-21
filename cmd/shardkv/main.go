// Command shardkv runs the sharded in-memory key-value server.
//
// Usage:
//
//	shardkv [-addr :6380] [-shards 256] [-sweep 1s]
//
// It speaks RESP, so any Redis client works:
//
//	redis-cli -p 6380 set foo bar
//	redis-cli -p 6380 get foo
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Black-third/shardkv/internal/server"
	"github.com/Black-third/shardkv/internal/store"
)

func main() {
	addr := flag.String("addr", ":6380", "TCP address to listen on")
	shards := flag.Int("shards", 256, "number of lock shards (rounded up to a power of two)")
	sweep := flag.Duration("sweep", time.Second, "interval between active expiration sweeps")
	flag.Parse()

	st := store.New(*shards)

	// Cancel on Ctrl-C / SIGTERM for a clean shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go st.Janitor(ctx, *sweep)

	srv := server.New(st)
	if err := srv.Listen(*addr); err != nil {
		log.Fatalf("shardkv: %v", err)
	}
	log.Printf("shardkv listening on %s", srv.Addr())

	if err := srv.Serve(ctx); err != nil {
		log.Fatalf("shardkv: %v", err)
	}
	log.Println("shardkv: shut down cleanly")
}
