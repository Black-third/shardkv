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
// the connection as a replica, ships a snapshot of the current dataset, then
// streams every subsequent write command until the replica disconnects or the
// server shuts down.
//
// Consistency note: the replica is registered before the snapshot is taken, so a
// write that races the snapshot may appear both in the snapshot and the live
// stream (applied twice). Replication is therefore asynchronous and eventually
// consistent; a production system would use a fork/copy-on-write snapshot or a
// replication offset/backlog to make initial sync exact.
func (s *Server) handleSync(ctx context.Context, w *resp.Writer) {
	rc := &replicaConn{ch: make(chan [][]byte, 1024)}

	s.mu.Lock()
	s.replicas[rc] = struct{}{}
	snapshot := s.store.Dump()
	s.mu.Unlock()
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
