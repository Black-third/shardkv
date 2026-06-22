package server

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
)

// replBackoff is the delay between replica reconnection attempts.
const replBackoff = time.Second

// handleSync turns the calling connection into a replication feed. It registers
// the connection as a replica and takes a snapshot of the current dataset under
// propMu, then ships the snapshot and streams every subsequent write until the
// replica disconnects or the server shuts down.
//
// Because write commands also hold propMu across their mutation+propagation,
// holding it here makes the snapshot a consistent point-in-time cut: no write
// can interleave, and every write after this point is enqueued to the new
// replica exactly once. The initial sync is therefore exact (no double-apply),
// not merely eventually consistent.
//
// One narrow window remains: a master started without -aof only flips
// propagating to true here, so a write already in flight on the fast path at the
// instant of the very first PSYNC may be missed. Running a replicated master
// with -aof (propagating from startup) closes it.
func (s *Server) handleSync(ctx context.Context, w *resp.Writer) {
	rc := &replicaConn{ch: make(chan [][]byte, 1024)}

	s.propagating.Store(true)
	s.propMu.Lock()
	s.mu.Lock()
	s.replicas[rc] = struct{}{}
	snapshot := s.store.Dump()
	s.mu.Unlock()
	s.propMu.Unlock()
	defer s.removeReplica(rc)

	for _, cmd := range snapshot {
		if w.WriteCommand(cmd) != nil {
			return
		}
	}
	if w.Flush() != nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case cmd := <-rc.ch:
			if w.WriteCommand(cmd) != nil {
				return
			}
			if w.Flush() != nil {
				return
			}
		}
	}
}

func (s *Server) removeReplica(rc *replicaConn) {
	s.mu.Lock()
	delete(s.replicas, rc)
	s.mu.Unlock()
}

// ReplicaOf makes this server replicate from the master at addr. Passing an
// empty addr promotes the server back to master ("REPLICAOF NO ONE"). Any
// existing replication link is stopped first.
func (s *Server) ReplicaOf(ctx context.Context, addr string) {
	s.mu.Lock()
	if s.replCancel != nil {
		s.replCancel()
		s.replCancel = nil
	}
	if addr == "" {
		s.role = "master"
		s.masterAddr = ""
		s.mu.Unlock()
		return
	}
	cctx, cancel := context.WithCancel(ctx)
	s.replCancel = cancel
	s.role = "replica"
	s.masterAddr = addr
	s.mu.Unlock()

	go s.replicationLoop(cctx, addr)
}

// replicationLoop maintains a connection to the master, applying the command
// stream it receives. It reconnects with a fixed backoff until ctx is canceled.
func (s *Server) replicationLoop(ctx context.Context, addr string) {
	discard := resp.NewWriter(io.Discard)
	for {
		if ctx.Err() != nil {
			return
		}
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			if sleepCtx(ctx, replBackoff) {
				return
			}
			continue
		}

		// Close the connection if ctx is canceled so the read loop unblocks.
		connDone := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				conn.Close()
			case <-connDone:
			}
		}()

		hw := resp.NewWriter(conn)
		hw.WriteCommand([][]byte{[]byte("PSYNC")})
		hw.Flush()

		r := resp.NewReader(conn)
		for {
			args, err := r.ReadCommand()
			if err != nil {
				break
			}
			s.applyCommand(discard, args)
			// Re-propagate to our own replicas / AOF so chained replicas and a
			// replica's own append-only file stay in sync. The stream is already
			// in absolute/effect form, so it is forwarded verbatim.
			if s.propagating.Load() {
				s.propMu.Lock()
				s.forward(args)
				s.propMu.Unlock()
			}
		}

		close(connDone)
		conn.Close()
		if sleepCtx(ctx, replBackoff) {
			return
		}
	}
}

// sleepCtx waits for d or until ctx is canceled. It reports whether ctx was
// canceled.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return true
	case <-t.C:
		return false
	}
}
