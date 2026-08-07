package server

// In-process command execution: a client with no socket behind it.
//
// This is the mechanism the embedded API is built on (see the root shardkv package). It
// exists here rather than there because it needs the session type and the client entry
// point, both of which are unexported and stay that way -- what leaves this package is a
// command in and reply bytes out, which is the same contract a TCP connection has.

import (
	"bytes"

	"github.com/Black-third/shardkv/internal/resp"
)

// EmbeddedSession is one in-process client: a session with no connection behind it,
// whose replies are serialized into a buffer instead of onto a socket.
//
// It runs commands through execute -- the *client* entry point -- and not through
// dispatch, and that choice is the whole of its semantics. dispatch is what an AOF
// replay and a master's stream go through, and invariant 13 says it is never
// redirected: a replica must apply every write its master sends whatever its own slot
// map says. An embedded caller is not a master. It is a client, and it has to be
// answered like one, or the embedded API would be a hole straight through every gate
// the client path exists to enforce -- authentication, MULTI queueing, the maxmemory
// budget, and in cluster mode the MOVED/ASK redirect for a slot this node does not own.
// A caller embedding a cluster node and writing a foreign key must be told, exactly as
// a socket client is.
//
// The reply is serialized and then parsed back, which is the one thing about this that
// looks wasteful and is not. What it removes is the pair of syscalls and the scheduler
// round trip a socket costs per command; what it keeps is that this server has exactly
// one command path. A path that skipped the encoding would need its own reply shapes,
// its own RESP2/RESP3 rules and its own error strings, and every disagreement with the
// real ones would be silent -- an embedded caller seeing something no socket client can
// see. Measured with -benchmem, the encode-and-parse round trip costs 10 allocations and
// 281 B for `SET k v` against 12 and 463 B for the same command over loopback TCP, so it
// is not even the more expensive of the two in the dimension that does not depend on how
// busy the machine is. (Wall-clock throughput belongs in make bench-vs-redis, which
// controls for that; a figure quoted here would not.)
//
// Not safe for concurrent use, on exactly the same terms as a connection: a session's
// transaction state, its SELECTed database and its authentication are owned by the one
// goroutine serving it (see session). Callers wanting parallelism take one of these
// each, which is what a connection pool is.
type EmbeddedSession struct {
	s    *Server
	sess *session
	buf  bytes.Buffer
	w    *resp.Writer
}

// NewEmbeddedSession registers an in-process client and returns it.
//
// serveDone is the server's lifetime signal, and passing it is what lets a blocking
// command issued in-process be released by shutdown instead of parking a goroutine past
// it (see blockUntilServed). A nil channel blocks forever, which for a server with no
// shutdown to signal is the correct answer rather than a special case.
//
// It joins the client registry, so CLIENT LIST describes it and INFO counts it in
// connected_clients -- with an empty addr, since it has no peer. That is deliberate: an
// operator connected over TCP to debug a stuck embedded caller needs to see its session,
// what it last ran and what it is blocked on, and needs CLIENT UNBLOCK to be able to
// reach it.
//
// Two bounds that apply to a connection deliberately do not apply here, and for the same
// reason in both cases: each exists to reclaim a file descriptor, and this session holds
// none. It does not consume a maxclients slot, which is counted by the accept loop -- a
// program that configured maxclients 1 for its socket clients must not find its own
// in-process client refused. And the idle reaper never touches it, because the only way
// the reaper can end a client is by closing its socket.
func (s *Server) NewEmbeddedSession(serveDone <-chan struct{}) *EmbeddedSession {
	e := &EmbeddedSession{s: s, sess: s.newSession(nil)}
	e.sess.serveDone = serveDone
	// rd stays nil, which watchClientGone already treats as "not a real connection":
	// there is no socket to watch for a hangup, so a blocked embedded client is released
	// by its timeout, by data arriving, by CLIENT UNBLOCK, or by shutdown.
	e.w = resp.NewWriter(&e.buf)
	return e
}

// Do runs one command and returns the RESP bytes of its reply. The slice is only valid
// until the next call, since the buffer is reused; the caller decodes before calling
// again.
//
// A nil return is a reply that was deliberately withheld rather than an error: CLIENT
// REPLY OFF|SKIP suppresses replies, and it suppresses them here exactly as it does on
// a socket.
func (e *EmbeddedSession) Do(args [][]byte) []byte {
	e.buf.Reset()
	e.s.execute(e.sess, e.w, args)
	// The CLIENT REPLY SKIP state machine is stepped here, before the flush, for the same
	// reason the connection loop steps it there: Resume discards whatever the suppressed
	// buffer holds, which is how the skipped reply is dropped.
	advanceReplyMode(e.sess, e.w)
	// A bytes.Buffer write cannot fail, so the flush's error is unreachable; it is
	// discarded rather than returned so that Do's signature does not offer a caller an
	// error that can never arrive.
	_ = e.w.Flush()
	if e.buf.Len() == 0 {
		return nil
	}
	return e.buf.Bytes()
}

// Quit reports whether the last command asked for the connection to be closed, which
// QUIT and SHUTDOWN both do. On a socket the loop hangs up at that point; here the
// caller is told so it can do the same.
func (e *EmbeddedSession) Quit() bool { return e.sess.quit }

// Proto reports the protocol version this session negotiated with HELLO, which decides
// the shape of the replies Do returns.
func (e *EmbeddedSession) Proto() int { return e.w.Proto() }

// Close releases everything the session held: its WATCHes, its subscriptions, its
// monitor feed and its entry in the client registry. Not calling it leaks that entry for
// the lifetime of the server, which is what a connection that is never closed would also
// do.
func (e *EmbeddedSession) Close() { e.s.closeSession(e.sess) }
