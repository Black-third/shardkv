package server

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
)

// session is the state of one client connection: its transaction, its
// authentication status, its Pub/Sub subscriptions, and the identity CLIENT LIST
// reports for it.
//
// The fields fall into three groups, and which group a field is in decides how it
// is synchronized:
//
//   - Owned by the connection's own goroutine (inMulti, queued, authenticated,
//     subscribed, ...). No synchronization at all; nothing else may touch them.
//   - Written by other goroutines: dirty, set when a watched key changes.
//   - Read by other connections running CLIENT LIST or CLIENT KILL: name, lastCmd,
//     lastActive and the subscription counters. Each is an atomic rather than
//     mutex-guarded, because a mutex here would funnel every connection's command
//     path through one lock purely to maintain a diagnostic.
type session struct {
	// db is the database SELECT chose for this connection, an index into the server's
	// fixed set of databases. Only the connection's own goroutine writes it (SELECT,
	// SWAPDB's clamp, RESET), but CLIENT LIST reads every connection's, so it is an
	// atomic rather than a plain int.
	db atomic.Int64

	// Transaction state (MULTI/EXEC/WATCH).
	inMulti  bool
	queued   [][][]byte
	queueErr bool // a queued command failed to parse -> EXEC aborts with EXECABORT

	// multiQueued and multiMem are the same transaction state, published for another
	// connection's CLIENT LIST: how many commands are queued and how many bytes their
	// arguments hold. inMulti and queued above belong to this connection's goroutine, and
	// CLIENT LIST reading them directly was a data race for the sake of a diagnostic.
	//
	// multiQueued holds the depth *plus one*, so that the zero value of a session means
	// "no transaction open" without an initialization step -- see session.multiDepth and
	// publishMulti, which is called from every place either field above changes.
	multiQueued atomic.Int64
	multiMem    atomic.Int64
	// watched maps each (database, key) this connection has WATCHed to whether that key was
	// live at the moment it was watched. The value is what lets EXEC tell a key that expired
	// during the transaction from one that was never there -- see watchedKeyExpired.
	watched map[dbKey]bool
	dirty   atomic.Bool

	// authenticated is true once AUTH (or HELLO ... AUTH) succeeded, or always when
	// no password is configured. See needsAuth.
	authenticated bool

	// asking is the one-shot cluster flag ASKING sets: it makes the next command
	// acceptable on a node importing that key's slot. It is consumed by the command
	// that follows (see clearAsking) because ownership has not moved -- a flag that
	// persisted would let an importing node serve a slot it does not own.
	asking bool

	// readReplica is the durable flag READONLY sets and READWRITE clears: this client
	// accepts reads served by a replica of the slot's owner instead of being redirected
	// to the master.
	readReplica bool

	// The CLIENT REPLY state (see cmdClientReply). replyOff is the durable OFF mode;
	// replySkipNext is set by CLIENT REPLY SKIP and replySkipping marks the one command
	// whose reply it suppresses. All three belong to the connection's own goroutine, which
	// is the only thing that reads or writes them.
	replyOff      bool
	replySkipNext bool
	replySkipping bool

	// quit asks the connection loop to hang up once the pending reply is flushed.
	// QUIT and SHUTDOWN set it rather than closing the socket themselves, so the
	// acknowledgement is never lost to a race with the close.
	quit bool

	conn net.Conn      // for CLIENT KILL and for hanging up on a dropped subscriber
	done chan struct{} // closed when the connection ends, to stop the message pump

	// rd is this connection's request reader, and serveDone is the server's lifetime
	// signal. Both exist for the blocking commands: a blocked client has stopped
	// reading its own socket, so something has to watch it for a disconnect (rd, see
	// watchClientGone) and for the server shutting down (serveDone). They are set once,
	// by handle, before any command runs.
	rd        *resp.Reader
	serveDone <-chan struct{}

	// wmu serializes writes to the connection's resp.Writer between the command loop
	// and the Pub/Sub pump goroutine. It is taken only once subscribed is set, so a
	// plain client never pays for a lock it cannot contend on.
	wmu        sync.Mutex
	wheld      bool // the command loop is currently holding wmu
	subscribed bool // this connection has a message pump running
	sub        *subscriber

	// MONITOR state: monitoring is set once this connection has become a monitor (the
	// command loop reads it to start the feed's pump and to take the writer lock, as it
	// does for a subscriber), and monitor is the bounded queue commands are fed into.
	monitoring bool
	monitor    *monitorSink

	// blockedFor is how long the command currently running spent parked in a blocking
	// wait. execute clears it before each command and the observability hook subtracts
	// it, so a BLPOP that waited five seconds for a push is not recorded as a
	// five-second-slow command: what the slow log is for is finding work the server did
	// slowly, and waiting for a client to push is not work. Redis excludes blocked time
	// from its own slow log for exactly this reason. Touched only by the connection's own
	// goroutine.
	blockedFor time.Duration

	// Introspection, read by other connections.
	id            int64
	addr          string
	createdAt     time.Time
	name          atomic.Pointer[string]
	lastCmd       atomic.Pointer[string]
	lastActive    atomic.Int64 // unix nanos of the last command
	nSub          atomic.Int64 // channel subscriptions
	nPSub         atomic.Int64 // pattern subscriptions
	listeningPort string       // REPLCONF listening-port, reported by INFO

	// proto is the RESP version this connection negotiated, published for CLIENT LIST's
	// resp= field. The authority for it is the connection's own resp.Writer, which
	// belongs to that connection's goroutine and so cannot be read from another; HELLO
	// and RESET are the only two things that change it, and both store it here as well.
	// Zero means no HELLO has been sent, which is RESP2.
	proto atomic.Int64

	// libName and libVer are what CLIENT SETINFO records: the client library behind this
	// connection, which CLIENT LIST then reports. Modern redis-py and node-redis send them
	// unprompted on connect, and on a server with many callers they are often the fastest
	// way to tell whose connection is misbehaving.
	libName atomic.Pointer[string]
	libVer  atomic.Pointer[string]

	// isReplicaFeed marks a connection that PSYNC turned into a replication feed.
	// It is set by handleSync and read by another connection's CLIENT LIST, so it is
	// atomic.
	isReplicaFeed atomic.Bool
}

// newSession registers a connection and returns its session. The id is a
// monotonically increasing counter, the same contract CLIENT ID offers: clients use
// it to tell one connection from another (and from a reconnect of their own).
func (s *Server) newSession(conn net.Conn) *session {
	sess := &session{
		conn:      conn,
		done:      make(chan struct{}),
		id:        s.nextID.Add(1),
		createdAt: time.Now(),
		// A connection is authenticated up front when no password is configured, so
		// the gate is a single boolean test rather than a re-read of the config.
		authenticated: s.RequirePass() == "",
	}
	if conn != nil {
		sess.addr = conn.RemoteAddr().String()
	}
	sess.lastActive.Store(sess.createdAt.UnixNano())

	s.clientMu.Lock()
	s.clients[sess.id] = sess
	s.clientMu.Unlock()
	return sess
}

// closeSession releases everything the connection held: its WATCHes, its Pub/Sub
// subscriptions, its registry entry, and the pump goroutine (via done).
func (s *Server) closeSession(sess *session) {
	s.unwatchAll(sess)
	s.unsubscribeSession(sess)
	s.detachMonitor(sess)

	s.clientMu.Lock()
	delete(s.clients, sess.id)
	s.clientMu.Unlock()

	close(sess.done)
}

// clientName reports the name set by CLIENT SETNAME, or "".
func (sess *session) clientName() string {
	if p := sess.name.Load(); p != nil {
		return *p
	}
	return ""
}

// lastCommand reports the name of the last command this connection ran, lower-cased
// the way CLIENT LIST spells it, or "NULL" for a connection that has run none.
func (sess *session) lastCommand() string {
	if p := sess.lastCmd.Load(); p != nil {
		return *p
	}
	return "NULL"
}

// inSubscriberMode reports whether the connection currently holds any subscription,
// which is what restricts it to the subscribe family (see execute). It is the live
// count, not the subscribed flag: a connection that unsubscribed from everything is
// a normal client again even though its pump is still running.
func (sess *session) inSubscriberMode() bool {
	return sess.nSub.Load()+sess.nPSub.Load() > 0
}

// snapshotSessions returns the registered connections, for CLIENT LIST/KILL. The
// slice is built under clientMu and used after releasing it, so a caller that
// closes a connection never does so while holding the registry lock.
func (s *Server) snapshotSessions() []*session {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()
	out := make([]*session, 0, len(s.clients))
	for _, sess := range s.clients {
		out = append(out, sess)
	}
	return out
}

// unlockWriter releases the connection's writer lock if the command loop is holding
// it, reporting whether it did. relockWriter takes it again.
//
// The pair exists for the blocking commands. The command loop holds the writer lock
// for the whole of a command on a subscribed connection, so that the Pub/Sub pump
// cannot interleave a delivered message with a reply; a blocking command that kept
// holding it would stall that pump for the length of its timeout. Both are called
// only from the connection's own goroutine, which is also the only writer of wheld.
func (sess *session) unlockWriter() bool {
	if !sess.wheld {
		return false
	}
	sess.wheld = false
	sess.wmu.Unlock()
	return true
}

func (sess *session) relockWriter() {
	sess.wmu.Lock()
	sess.wheld = true
}
