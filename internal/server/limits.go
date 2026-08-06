package server

// The bounds on the connection table: how many clients may be connected at once, and
// how long one may sit idle before the server closes it.
//
// Without either of these, every accepted connection costs a goroutine, a reader, a
// writer and a session for as long as the peer keeps the socket open, and nothing ever
// reclaims them. A caller that opens connections and never closes them therefore
// exhausts the server's memory from the outside, with no command involved -- which is a
// remote resource exhaustion rather than a tuning gap, and the reason both of these are
// on by default the way Redis has them.
//
// The idle timeout is the half that is easy to get wrong, because the connections it
// would most obviously reap are the ones that are working correctly. A replica feed, a
// Pub/Sub subscriber, a MONITOR feed and a client parked in BLPOP are all *supposed* to
// be silent for as long as they are: none of them is going to send a command, and each
// is either waiting to be written to or waiting to be woken. Reaping any of them would
// break the feature. So the reaper exempts exactly those four, which is the same list
// Redis's clientsCronHandleTimeout exempts (its CLIENT_SLAVE flag covers monitors as
// well as replicas, which is why its comment reads "no timeout for slaves and
// monitors"). See idleExempt.

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
)

const (
	// defaultMaxClients is Redis's default. It is a count of connections rather than a
	// memory budget for the same reason maxkeys is a count of keys: it is the quantity
	// this server can actually enforce.
	defaultMaxClients = 10000
	// errMaxClients is the message Redis sends before hanging up on a connection it
	// cannot accept. It is sent *and then* the socket is closed: a client that is only
	// told "connection reset" cannot tell a full server from a crashed one.
	errMaxClients = "ERR max number of clients reached"
	// idleReapInterval is how often the reaper looks for idle connections. Redis checks
	// on its serverCron, a few times a second; once a second is enough resolution for a
	// timeout expressed in whole seconds and costs one wakeup on an idle server.
	idleReapInterval = time.Second
	// defaultStreamNodeMaxEntries is Redis's default for the inert stream parameter, so a
	// client that reads it before writing finds the number it expects.
	defaultStreamNodeMaxEntries = 100
)

// SetMaxClients bounds how many connections may be open at once. It takes effect for
// connections accepted afterwards; lowering it never disconnects a client that is
// already connected, which is Redis's behaviour and the only safe one -- a lowered
// limit that dropped existing clients would turn a tuning change into an outage.
func (s *Server) SetMaxClients(n int) {
	if n < 1 {
		n = 1
	}
	s.maxClients.Store(int64(n))
}

// MaxClients reports the limit, for CONFIG GET and INFO.
func (s *Server) MaxClients() int { return int(s.maxClients.Load()) }

// SetClientTimeoutSecs sets the idle disconnect, in seconds. Zero disables it, which is
// Redis's default: a server whose clients hold long-lived connections on purpose should
// not be guessing which ones have been abandoned.
func (s *Server) SetClientTimeoutSecs(secs int64) {
	if secs < 0 {
		secs = 0
	}
	s.clientTimeout.Store(secs)
}

// ClientTimeoutSecs reports the idle disconnect, for CONFIG GET.
func (s *Server) ClientTimeoutSecs() int64 { return s.clientTimeout.Load() }

// SetStreamNodeMaxEntries records the stream macro-node size. Nothing reads it: this
// stream is a sorted slice of entries with no macro-nodes, so there is no structure for
// the number to tune. It is stored so CONFIG GET reports back what CONFIG SET was told
// rather than a constant -- see the parameter's comment in commands_config.go for why
// that is not the same as pretending to tune something.
func (s *Server) SetStreamNodeMaxEntries(n int64) { s.streamNodeMaxEntries.Store(n) }

// StreamNodeMaxEntries reports it, for CONFIG GET.
func (s *Server) StreamNodeMaxEntries() int64 { return s.streamNodeMaxEntries.Load() }

// SetListCompressDepth and ListCompressDepth are the same arrangement for the quicklist
// compression depth: recorded, reported, and read by nothing, because a deque of
// individually allocated elements has no compressible interior nodes.
func (s *Server) SetListCompressDepth(n int64) { s.listCompressDepth.Store(n) }
func (s *Server) ListCompressDepth() int64     { return s.listCompressDepth.Load() }

// admitConn reserves a slot in the connection table for a newly accepted connection,
// reporting whether there was one. On refusal it has already told the client why and
// closed the socket.
//
// The reservation is an atomic counter rather than a look at the client registry,
// because a session is registered by the connection's own goroutine after this returns:
// counting the registry would let a burst of accepts all observe the same pre-burst
// size and overshoot the limit by however many were in flight. Only the accept loop
// increments, so the count cannot race with itself.
func (s *Server) admitConn(conn net.Conn) bool {
	if n := s.liveConns.Add(1); n <= s.maxClients.Load() {
		return true
	}
	s.liveConns.Add(-1)
	s.rejectedConns.Add(1)
	w := resp.NewWriter(conn)
	w.WriteError(errMaxClients)
	_ = w.Flush()
	conn.Close()
	return false
}

// releaseConn gives the slot back. It runs from the connection goroutine's defer, so it
// covers every way a connection can end.
func (s *Server) releaseConn() { s.liveConns.Add(-1) }

// RejectedConnections reports how many connections were refused for the limit, for
// INFO. It is a statistic an operator needs in order to tell "clients are failing to
// connect" from "clients are not trying".
func (s *Server) RejectedConnections() int64 { return s.rejectedConns.Load() }

// reapIdleClients closes connections that have been idle longer than the configured
// timeout, until ctx is canceled. It is started by Serve.
//
// It closes the socket rather than asking the connection to stop: the connection's
// goroutine is blocked in a read, which is exactly what makes it idle, so the only way
// to reach it is through the file descriptor. That is also how CLIENT KILL ends a
// connection, so the teardown path is the already-tested one.
func (s *Server) reapIdleClients(ctx context.Context) {
	t := time.NewTicker(idleReapInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			secs := s.clientTimeout.Load()
			if secs == 0 {
				continue // the whole feature is one atomic load while it is off
			}
			cutoff := time.Now().Add(-time.Duration(secs) * time.Second).UnixNano()
			for _, sess := range s.snapshotSessions() {
				if sess.lastActive.Load() > cutoff || s.idleExempt(sess) {
					continue
				}
				if sess.conn != nil {
					log.Printf("shardkv: closing client %d (%s), idle for more than %ds",
						sess.id, sess.addr, secs)
					sess.conn.Close()
				}
			}
		}
	}
}

// idleExempt reports whether a connection is silent by design and so must never be
// reaped. See the file comment: each of these is waiting for the server, not the other
// way round, and closing one would break the feature it is using.
func (s *Server) idleExempt(sess *session) bool {
	switch {
	case sess.isReplicaFeed.Load():
		// A replica reads the feed and speaks only to acknowledge an offset. Dropping it
		// costs a full resync at best.
		return true
	case sess.inSubscriberMode():
		// A subscriber's whole job is to wait for a message it did not ask for.
		return true
	case s.isMonitoring(sess):
		return true
	case s.isBlocked(sess.id):
		// A client parked in BLPOP is waiting for a push, and its wait already has a
		// timeout of its own -- the one it asked for.
		return true
	}
	return false
}

// isMonitoring reports whether the connection is an attached MONITOR feed. It reads the
// server's registry under its own lock rather than the session's own flag, which is
// written by the connection's goroutine and would be a race to read from the reaper's.
func (s *Server) isMonitoring(sess *session) bool {
	if s.monitorCount.Load() == 0 {
		return false
	}
	s.monitorMu.Lock()
	defer s.monitorMu.Unlock()
	_, ok := s.monitors[sess]
	return ok
}

// isBlocked reports whether the client with this id is parked in a blocking command. It
// is gated on the blocked-client count so the reaper does not take the queue lock on a
// server where nobody is blocked, which is the same discipline the write path applies.
func (s *Server) isBlocked(id int64) bool {
	if s.blockedCount.Load() == 0 {
		return false
	}
	s.blockMu.Lock()
	defer s.blockMu.Unlock()
	_, ok := s.blockByID[id]
	return ok
}

// --- the change counter -------------------------------------------------------

// noteDirty records one change to the dataset. It backs INFO's
// rdb_changes_since_last_save, which is what a client polls to decide whether a save is
// worth asking for, and which Redis's own test suite reads to check that a write that
// changed nothing did not count as a change.
//
// It counts commands that reported dirty, not elements: one DEL of three keys is one
// change here and three in Redis. The distinction the field exists to draw -- did
// anything change since the last full write of the dataset -- is the same either way,
// and counting commands is what the dirty flag this server already threads through every
// write path actually knows.
func (s *Server) noteDirty(n int64) { s.dirtyChanges.Add(n) }

// DirtyChanges reports the counter, for INFO.
func (s *Server) DirtyChanges() int64 { return s.dirtyChanges.Load() }

// clearDirtyChanges zeroes it, which is what a completed full write of the dataset
// means: everything the counter was counting is now on disk. Here that is a successful
// AOF rewrite, the same event LASTSAVE reports.
func (s *Server) clearDirtyChanges() { s.dirtyChanges.Store(0) }
