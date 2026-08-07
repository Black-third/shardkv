package server

// Connection-level commands: the handshake a client library performs on connect
// (HELLO), the connection registry it introspects (CLIENT), and the commands that
// reset or end a connection.
//
// All of them are registered with registerSession, because all of them act on one
// socket rather than on the dataset. That is also why none is a write: nothing they
// change survives the connection, so there is nothing for the AOF or a replica to
// reconstruct.

import (
	"bufio"
	"crypto/tls"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/Black-third/shardkv/internal/resp"
)

func init() {
	registerSession("QUIT", 1, cmdQuit)
	registerSession("RESET", 1, cmdReset)
	registerSession("HELLO", -1, cmdHello)
	registerSession("CLIENT", -2, cmdClient)
	registerSession("SELECT", 2, cmdSelect)
}

// cmdQuit acknowledges and then asks the connection loop to hang up. It does not
// close the socket itself: the reply is still in the write buffer at that point, and
// closing under it would lose the acknowledgement the client is waiting for.
func cmdQuit(s *Server, sess *session, w *resp.Writer, args [][]byte) {
	sess.quit = true
	w.WriteSimple("OK")
}

// cmdReset returns the connection to the state it had just after connecting: no
// transaction, no WATCHes, no subscriptions, no name, and -- on a password-protected
// server -- unauthenticated again.
//
// Dropping authentication is the point of the command rather than an oversight. RESET
// exists so a connection pool can hand a socket to an unrelated caller without
// leaking the previous one's state, and credentials are the state that matters most.
//
// The negotiated protocol goes back to RESP2 and the selected database back to 0 for
// the same reason, as they do in real Redis: the next caller has sent neither a HELLO
// nor a SELECT, so it can only be assumed to want the defaults.
func cmdReset(s *Server, sess *session, w *resp.Writer, args [][]byte) {
	sess.inMulti = false
	sess.queued = nil
	sess.queueErr = false
	publishMulti(sess, 0)
	s.unwatchAll(sess)
	s.unsubscribeSession(sess)
	sess.name.Store(nil)
	sess.authenticated = s.RequirePass() == ""
	sess.db.Store(0)
	// And replies are delivered again: RESET clears CLIENT REPLY OFF/SKIP, as it does in
	// Redis. A pooled socket handed on with its replies silenced would look hung to
	// whoever got it next.
	sess.replyOff = false
	sess.replySkipNext = false
	sess.replySkipping = false
	w.Resume()
	w.SetProto(resp.ProtoRESP2)
	sess.proto.Store(resp.ProtoRESP2)
	w.WriteSimple("RESET")
}

// cmdHello implements HELLO [protover [AUTH username password] [SETNAME name]].
//
// It is the only place a connection's protocol version changes. Both versions are
// spoken in full: a RESP2 client keeps receiving exactly the bytes it always did,
// and a RESP3 client gets the map, set, double, verbatim and push types where real
// Redis sends them (see the reply-shape notes on writeZMembers, cmdHGetAll,
// configGet and writePush).
//
// The version applies from the reply to this very command: HELLO 3 answers with a
// RESP3 map, HELLO 2 from a RESP3 connection answers with the flat array, which is
// what lets a client library negotiate down and keep parsing.
func cmdHello(s *Server, sess *session, w *resp.Writer, args [][]byte) {
	// The protocol version is optional and, when present, comes first. Anything in that
	// position that is not one of the option keywords is meant to be a version, so a
	// value that is not a version this server speaks is reported as such rather than
	// as a stray argument -- that is the error a client can act on.
	proto := w.Proto() // no version given: stay on the one already negotiated
	i := 1
	if len(args) > 1 {
		switch strings.ToUpper(string(args[1])) {
		case "AUTH", "SETNAME":
		default:
			ver, ok := parseInt64(args[1])
			if !ok || (ver != resp.ProtoRESP2 && ver != resp.ProtoRESP3) {
				w.WriteError("NOPROTO unsupported protocol version")
				return
			}
			proto = int(ver)
			i = 2
		}
	}
	for ; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "AUTH":
			if i+2 >= len(args) {
				w.WriteError("ERR Protocol error: unexpected argument")
				return
			}
			// Reuse the AUTH path so there is one place a credential is checked, and one
			// place the constant-time comparison lives.
			var buf noReply
			cmdAuth(s, sess, resp.NewWriter(&buf), [][]byte{[]byte("AUTH"), args[i+1], args[i+2]})
			if !sess.authenticated {
				w.WriteError(errWrongPass)
				return
			}
			i += 2
		case "SETNAME":
			if i+1 >= len(args) {
				w.WriteError("ERR Protocol error: unexpected argument")
				return
			}
			name := string(args[i+1])
			sess.name.Store(&name)
			i++
		default:
			w.WriteError("ERR Protocol error: unexpected argument")
			return
		}
	}
	if s.needsAuth(sess) {
		w.WriteError(errNoAuth)
		return
	}

	// Switch only once the handshake has succeeded: a HELLO that fails
	// authentication must leave the connection on the protocol it was already
	// speaking, or the client's next reply would arrive in an encoding it never
	// agreed to.
	w.SetProto(proto)
	// And published for another connection's CLIENT LIST, which cannot read this one's
	// writer. The two are set together so they cannot disagree.
	sess.proto.Store(int64(proto))

	role := "master"
	if s.isReplica() {
		role = "replica"
	}
	if proto >= resp.ProtoRESP3 {
		// RESP3: a map, with proto and id as integers rather than strings -- the shape
		// real Redis sends, which is what a RESP3 client library validates.
		w.WriteMapHeader(7)
		w.WriteBulkString("server")
		w.WriteBulkString("shardkv")
		w.WriteBulkString("version")
		w.WriteBulkString(Version)
		w.WriteBulkString("proto")
		w.WriteInt(int64(proto))
		w.WriteBulkString("id")
		w.WriteInt(sess.id)
		w.WriteBulkString("mode")
		w.WriteBulkString("standalone")
		w.WriteBulkString("role")
		w.WriteBulkString(role)
		w.WriteBulkString("modules")
		w.WriteArrayHeader(0)
		return
	}
	// RESP2 has no map type, so the reply is the flat key/value array Redis sends a
	// RESP2 client -- clients pair the elements themselves.
	fields := []string{
		"server", "shardkv",
		"version", Version,
		"proto", "2",
		"id", strconv.FormatInt(sess.id, 10),
		"mode", "standalone",
		"role", role,
	}
	w.WriteArrayHeader(len(fields) + 2)
	for _, f := range fields {
		w.WriteBulkString(f)
	}
	w.WriteBulkString("modules")
	w.WriteArrayHeader(0)
}

// noReply is a sink for a reply that is deliberately discarded (HELLO reuses AUTH's
// handler but writes its own reply). Discarding into io.Discard would work equally
// well; this keeps the intent visible at the call site.
type noReply struct{}

func (noReply) Write(p []byte) (int, error) { return len(p), nil }

// cmdClient implements CLIENT ID|GETNAME|SETNAME|LIST|INFO|KILL.
func cmdClient(s *Server, sess *session, w *resp.Writer, args [][]byte) {
	switch strings.ToUpper(string(args[1])) {
	case "ID":
		w.WriteInt(sess.id)

	case "GETNAME":
		if name := sess.clientName(); name != "" {
			w.WriteBulkString(name)
		} else {
			w.WriteNull() // unnamed is a null bulk string, not an empty one
		}

	case "SETNAME":
		if len(args) != 3 {
			w.WriteError("ERR wrong number of arguments for 'client|setname' command")
			return
		}
		name := string(args[2])
		// The name shows up in CLIENT LIST, one connection per line, so a name
		// containing a space or a newline would make that reply unparseable.
		if strings.ContainsAny(name, " \n\r") {
			w.WriteError("ERR Client names cannot contain spaces, newlines or special characters.")
			return
		}
		sess.name.Store(&name)
		w.WriteSimple("OK")

	case "SETINFO":
		// CLIENT SETINFO lib-name|lib-ver <value>, sent unprompted on connect by modern
		// redis-py and node-redis so that CLIENT LIST shows which library a connection
		// belongs to. Answering with an error is not fatal -- the clients tolerate it --
		// but it puts an error in the log of every healthy connection, and the whole point
		// of the field is to make debugging a busy server easier, which is worth having.
		if len(args) != 4 {
			w.WriteError("ERR wrong number of arguments for 'client|setinfo' command")
			return
		}
		value := string(args[3])
		if strings.ContainsAny(value, " \n\r") {
			w.WriteError("ERR lib-name cannot contain spaces, newlines or special characters.")
			return
		}
		switch strings.ToUpper(string(args[2])) {
		case "LIB-NAME":
			sess.libName.Store(&value)
		case "LIB-VER":
			sess.libVer.Store(&value)
		default:
			w.WriteError("ERR Unrecognized option '" + string(args[2]) + "'")
			return
		}
		w.WriteSimple("OK")

	case "INFO":
		// A verbatim string in RESP3, as in Redis: the line is meant to be read, not
		// escaped onto one line by the client's pretty-printer.
		w.WriteVerbatim("txt", []byte(clientInfoLine(sess, s.clientRegistry())))

	case "LIST":
		// One registry snapshot for the whole reply, not one per line: see clientRegistry.
		reg := s.clientRegistry()
		var b strings.Builder
		for _, other := range s.snapshotSessions() {
			b.WriteString(clientInfoLine(other, reg))
			b.WriteString("\n")
		}
		w.WriteVerbatim("txt", []byte(b.String()))

	case "REPLY":
		if len(args) != 3 {
			w.WriteError("ERR wrong number of arguments for 'client|reply' command")
			return
		}
		clientReply(sess, w, args)

	case "UNBLOCK":
		s.clientUnblock(w, args)

	case "KILL":
		s.clientKill(sess, w, args)

	case "HELP":
		writeSubcommandHelp(w, "CLIENT", []string{
			"ID",
			"    Return the ID of the current connection.",
			"INFO",
			"    Return information about the current client connection.",
			"LIST",
			"    Return information about all client connections.",
			"GETNAME",
			"    Return the name of the current connection.",
			"SETNAME <name>",
			"    Assign the name <name> to the current connection.",
			"SETINFO <option> <value>",
			"    Set client meta attr. Options are:",
			"    * LIB-NAME: the client lib name.",
			"    * LIB-VER: the client lib version.",
			"REPLY (ON|OFF|SKIP)",
			"    Control the replies sent to the current connection.",
			"UNBLOCK <clientid> [TIMEOUT|ERROR]",
			"    Unblock the specified blocked client.",
			"KILL <option> <value> [<option> <value> [...]]",
			"    Kill connections matching every given filter.",
		})

	default:
		writeUnknownSubcommand(w, "CLIENT", args[1])
	}
}

// clientReply implements CLIENT REPLY ON|OFF|SKIP: whether this connection wants to be
// answered at all.
//
// It exists for a client that fires a stream of writes it will not read the answers to --
// a bulk load over one socket. Without it the answers pile up in the socket buffer until
// the server blocks writing them, so the caller has to read replies it does not want.
//
// The three modes and what each replies with are Redis's, and the asymmetry is the whole
// interface: ON answers +OK (it is the one that re-enables answering, so its own answer is
// the acknowledgement), while OFF and SKIP answer with nothing at all -- an
// acknowledgement of "stop answering me" would be self-contradicting.
//
// SKIP suppresses exactly the next command's reply, so it is a two-step state machine:
// this sets replySkipNext, and advanceReplyMode -- which runs after every command --
// promotes it to replySkipping for one command and then restores delivery. SKIP while OFF
// is already in force does nothing extra, as in Redis, because there is nothing left to
// suppress.
func clientReply(sess *session, w *resp.Writer, args [][]byte) {
	switch strings.ToUpper(string(args[2])) {
	case "ON":
		sess.replyOff = false
		sess.replySkipNext = false
		sess.replySkipping = false
		w.Resume()
		w.WriteSimple("OK")
	case "OFF":
		// Flush first: the replies to the commands that came before this one in the same
		// pipeline were promised, and Suppress discards whatever the buffer holds.
		_ = w.Flush()
		sess.replyOff = true
		w.Suppress()
	case "SKIP":
		if sess.replyOff {
			return
		}
		_ = w.Flush()
		sess.replySkipNext = true
		w.Suppress()
	default:
		w.WriteError("ERR syntax error")
	}
}

// advanceReplyMode moves the CLIENT REPLY SKIP state machine on by one command. It runs
// after every command on the connection, and while no SKIP is outstanding it is two
// boolean tests.
//
// The order matters: the command that has just run may be the CLIENT REPLY SKIP itself, in
// which case suppression has to persist through the *next* one; only on the command after
// that is delivery restored -- and only if OFF is not separately in force.
func advanceReplyMode(sess *session, w *resp.Writer) {
	switch {
	case sess.replySkipNext:
		sess.replySkipNext = false
		sess.replySkipping = true
	case sess.replySkipping:
		sess.replySkipping = false
		if !sess.replyOff {
			w.Resume()
		}
	}
}

// clientRegistry is the server-wide state a CLIENT LIST needs that does not live on the
// session: which connections are blocked, which are monitors, and how many keys each has
// WATCHed. It is gathered once per command rather than once per line, so a CLIENT LIST
// over a thousand connections takes each lock once instead of a thousand times.
//
// Every map here is read under the lock that owns it and then released; none is held
// while another is taken, so this adds no edge to the lock order. Each is also skipped
// entirely when its feature is unused -- the blocked set behind blockedCount, the monitor
// set behind monitorCount, the watch counts behind an empty registry -- which is
// invariant 12's rule applied to an introspection command: a server where nobody blocks,
// monitors or watches builds none of this.
type clientRegistry struct {
	blocked  map[int64]bool
	monitors map[*session]bool
	watches  map[*session]int
}

func (s *Server) clientRegistry() clientRegistry {
	var reg clientRegistry
	if s.blockedCount.Load() > 0 {
		s.blockMu.Lock()
		reg.blocked = make(map[int64]bool, len(s.blockByID))
		for id := range s.blockByID {
			reg.blocked[id] = true
		}
		s.blockMu.Unlock()
	}
	if s.monitorCount.Load() > 0 {
		s.monitorMu.Lock()
		reg.monitors = make(map[*session]bool, len(s.monitors))
		for sess := range s.monitors {
			reg.monitors[sess] = true
		}
		s.monitorMu.Unlock()
	}
	s.watchMu.Lock()
	if len(s.watchers) > 0 {
		reg.watches = make(map[*session]int)
		for _, sessions := range s.watchers {
			for sess := range sessions {
				reg.watches[sess]++
			}
		}
	}
	s.watchMu.Unlock()
	return reg
}

// clientFlags renders the flags field: the set of single letters Redis uses, in Redis's
// own order, for the states this server can observe.
//
// Redis's field is a *set*, not one letter -- a connection in MULTI whose WATCH has been
// invalidated reads "xd" -- and reporting only the first applicable letter was the bug
// here: a client in MULTI read "N", which says "nothing special is true of this
// connection". Each letter below was measured against redis 7.2.15 by putting a real
// connection into the state and reading another connection's CLIENT LIST.
//
// The letters Redis has that this server does not report, and why:
//
//	M    the connection to a master. A replica's master link here is an outbound
//	     connection this server made, not an accepted one, so it is not in the client
//	     registry at all and has no line for a flag to appear on.
//	t R B tracking / client-side caching. Not implemented (there is no CLIENT TRACKING),
//	     so no connection can be in the state.
//	c u A the transient close-after-reply, just-unblocked and close-asap markers. Each is
//	     owned by the connection's own goroutine and is cleared within the same command
//	     that set it, so another connection could only ever read it through a data race
//	     -- and would find it false.
//	U    a unix-socket connection. This server listens on TCP only (see Serve).
//	r    the cluster READONLY flag, and e/T the no-evict/no-touch ones. All three are
//	     plain fields owned by the connection's goroutine with no atomic publication, so
//	     reading them from a CLIENT LIST would be exactly the race that multiQueued
//	     exists to avoid. They are the honest gap in this field.
func clientFlags(sess *session, reg clientRegistry) string {
	var b [8]byte
	f := b[:0]
	switch {
	case sess.isReplicaFeed.Load() && reg.monitors[sess]:
		f = append(f, 'O') // a replica that is also monitoring, as Redis flags it
	case sess.isReplicaFeed.Load():
		f = append(f, 'S') // a replication feed
	case reg.monitors[sess]:
		// A plain MONITOR connection reads 'O' on real Redis, not 'N': Redis makes every
		// monitor a CLIENT_SLAVE as well, and 'O' is the letter for the combination.
		// Measured on redis 7.2.15.
		f = append(f, 'O')
	}
	if sess.inSubscriberMode() {
		f = append(f, 'P')
	}
	if sess.multiDepth() >= 0 {
		f = append(f, 'x')
	}
	if reg.blocked[sess.id] {
		f = append(f, 'b')
	}
	if sess.dirty.Load() {
		f = append(f, 'd')
	}
	if len(f) == 0 {
		return "N"
	}
	return string(f)
}

// The per-connection buffer sizes reported by qbuf-free, rbs and tot-mem.
//
// They are derived from bufio's own default rather than written down, because that is
// where the number actually comes from: resp.NewReader and resp.NewWriter each wrap the
// socket in a default-sized bufio, so asking bufio what that size is reports the buffer
// the connection really holds and keeps reporting the right number if bufio's default
// ever changes. Writing 4096 here would be a constant that silently stopped being true.
var (
	clientReadBufSize  = int64(bufio.NewReader(strings.NewReader("")).Size())
	clientWriteBufSize = func() int64 { w := bufio.NewWriter(io.Discard); return int64(w.Available()) }()
)

// clientInfoLine renders one connection the way CLIENT LIST and CLIENT INFO do.
//
// The field names, their order and their spellings are Redis 7.4's, because client
// libraries and operators parse this line -- some of them positionally -- and a missing
// field is a parse failure or a silently-wrong number. The order was measured against a
// live redis:7.4.10 rather than taken from documentation; redis 7.2.15 emits the same
// line without `watch`, which is the only difference between the two releases here and
// the reason `watch` is present (INFO's redis_version claims 7.4).
//
// Six fields are constants, and each is a constant because the value is *true* here
// rather than because it was convenient:
//
//	ssub=0    shard channels are not implemented (there is no SSUBSCRIBE), so no
//	          connection can hold a shard subscription.
//	argv-mem=0 the argument vector is not retained past the command that used it. The
//	          only argv alive while this line is being built belongs to the CLIENT
//	          INFO/LIST building it, and charging a connection for the cost of asking
//	          about itself would be the misleading answer.
//	obl=0 oll=0 omem=0
//	          there is no reply list. A reply is encoded into the connection's fixed
//	          buffer and written to the socket; a client that will not read blocks its
//	          own writer rather than accumulating queued reply objects, so the three
//	          fields that measure that accumulation are exactly zero. The three
//	          connection kinds that *do* hold a queue -- a subscriber, a monitor and a
//	          replica feed -- are bounded by message count and dropped rather than grown
//	          (invariant 6); INFO's pubsub_dropped/monitor_dropped/replica_dropped
//	          counters are where that shows, not here.
//	rbp=0     the reply buffer's peak use is not tracked. Redis grows and shrinks a
//	          per-client reply buffer and reports the high-water mark to justify the
//	          resizing; this buffer is fixed, so there is no resizing decision for a peak
//	          to inform, and 0 is reported rather than a number invented for the field.
//	user=default
//	          there are no ACL users. The one credential this server has (requirepass)
//	          authenticates as what Redis calls the default user, which is what every
//	          connection here is.
//	redir=-1  client-side caching is not implemented, and -1 is Redis's own value for a
//	          connection that is not redirecting invalidation messages anywhere.
//	events=r  every connection is parked in a read between commands. There is no
//	          write-readiness registration to report: a reply is written synchronously and
//	          blocks the connection's own goroutine if the socket is full.
//
// And one field is honest but coarser than Redis's: qbuf is 0 because this server keeps
// no growing per-client query buffer -- each command is parsed straight out of the fixed
// read buffer and nothing is retained across commands. A *pipelining* client's
// not-yet-parsed bytes do sit in that buffer, and their count is not readable from
// another connection without a data race on bufio's cursors, so what is reported is the
// buffer's full size as qbuf-free. The number is therefore right at rest and understates
// a pipeline in flight, which is the one caveat on this line.
func clientInfoLine(sess *session, reg clientRegistry) string {
	now := time.Now()
	idle := now.Sub(time.Unix(0, sess.lastActive.Load()))
	multiMem := sess.multiMem.Load()
	// The buffers this connection actually holds. Redis's tot-mem also counts the client
	// struct itself, which has no stable size here; what is counted is the memory that
	// varies between connections and that an operator is looking for when they read the
	// field -- the two socket buffers plus whatever a queued transaction is holding.
	totMem := clientReadBufSize + clientWriteBufSize + multiMem
	return "id=" + strconv.FormatInt(sess.id, 10) +
		" addr=" + sess.addr +
		" laddr=" + sessionLocalAddr(sess) +
		" fd=" + strconv.Itoa(sessionFD(sess)) +
		" name=" + sess.clientName() +
		" age=" + strconv.Itoa(int(now.Sub(sess.createdAt).Seconds())) +
		" idle=" + strconv.Itoa(int(idle.Seconds())) +
		" flags=" + clientFlags(sess, reg) +
		" db=" + strconv.FormatInt(sess.db.Load(), 10) +
		" sub=" + strconv.FormatInt(sess.nSub.Load(), 10) +
		" psub=" + strconv.FormatInt(sess.nPSub.Load(), 10) +
		" ssub=0" +
		" multi=" + strconv.FormatInt(sess.multiDepth(), 10) +
		" watch=" + strconv.Itoa(reg.watches[sess]) +
		" qbuf=0" +
		" qbuf-free=" + strconv.FormatInt(clientReadBufSize, 10) +
		" argv-mem=0" +
		" multi-mem=" + strconv.FormatInt(multiMem, 10) +
		" rbs=" + strconv.FormatInt(clientWriteBufSize, 10) +
		" rbp=0" +
		" obl=0" +
		" oll=0" +
		" omem=0" +
		" tot-mem=" + strconv.FormatInt(totMem, 10) +
		" events=r" +
		" cmd=" + sess.lastCommand() +
		" user=default" +
		" redir=-1" +
		" resp=" + strconv.FormatInt(sessionProto(sess), 10) +
		// Reported even when empty, because Redis reports them unconditionally and a
		// parser that splits on spaces counts fields.
		" lib-name=" + loadString(&sess.libName) +
		" lib-ver=" + loadString(&sess.libVer)
}

// sessionLocalAddr is the address on this server that the connection arrived at, which is
// what Redis reports as laddr. It is read from the connection rather than stored, because
// a net.Conn's local address is fixed the moment the connection is accepted and reading it
// touches nothing another goroutine writes.
//
// A session with no connection is one a test made, and reports the empty string rather
// than inventing an address.
func sessionLocalAddr(sess *session) string {
	if sess.conn == nil {
		return ""
	}
	if a := sess.conn.LocalAddr(); a != nil {
		return a.String()
	}
	return ""
}

// sessionFD is the operating-system descriptor behind the connection, which Redis reports
// as fd and an operator uses to tie a client line to lsof or to a stack trace.
//
// -1 is Redis's own value for a client with no descriptor (it uses it for the fake client
// that replays an AOF), so it is what a session with no socket, a TLS connection whose
// transport does not expose one, or a connection already closed reports. That makes the
// field always present and never a guess.
//
// SyscallConn().Control is the supported way to look at a descriptor Go owns: it holds the
// runtime's reference to the file for the length of the call, so the descriptor cannot be
// closed and reused underneath the read. That is also why the number is not cached -- a
// cached descriptor would keep being reported after the connection closed and the number
// was handed to something else.
func sessionFD(sess *session) int {
	conn := sess.conn
	if tc, ok := conn.(*tls.Conn); ok {
		conn = tc.NetConn()
	}
	sc, ok := conn.(syscall.Conn)
	if !ok {
		return -1
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return -1
	}
	fd := -1
	if err := raw.Control(func(p uintptr) { fd = int(p) }); err != nil {
		return -1
	}
	return fd
}

// sessionProto is the RESP version the connection negotiated, for the resp= field. A
// session that has never sent HELLO has the zero value stored and is speaking RESP2,
// which is the version it started on.
func sessionProto(sess *session) int64 {
	if p := sess.proto.Load(); p != 0 {
		return p
	}
	return resp.ProtoRESP2
}

// multiDepth reports how many commands this connection has queued in a transaction, or -1
// when it has none -- the two states Redis's multi= field distinguishes. The stored value
// is the depth plus one so that a session's zero value already means "no transaction";
// see publishMulti, which is the only writer.
func (sess *session) multiDepth() int64 { return sess.multiQueued.Load() - 1 }

// publishMulti republishes the transaction state CLIENT LIST reads, and must be called
// from the connection's own goroutine after any change to inMulti or queued. memDelta is
// the argument bytes just added to the queue, or 0 for a change that did not add any.
//
// It exists because inMulti and queued belong to one goroutine while CLIENT LIST runs on
// another: the atomics are the only sanctioned way across that boundary. The delta is
// passed in rather than recomputed from the queue so that queueing the nth command stays
// O(1) -- re-summing the whole queue each time would make a large MULTI quadratic in the
// service of a diagnostic.
func publishMulti(sess *session, memDelta int64) {
	if !sess.inMulti {
		sess.multiQueued.Store(0)
		sess.multiMem.Store(0)
		return
	}
	sess.multiQueued.Store(int64(len(sess.queued)) + 1)
	if len(sess.queued) == 0 {
		sess.multiMem.Store(0)
		return
	}
	if memDelta != 0 {
		sess.multiMem.Add(memDelta)
	}
}

// queuedArgsSize is what one queued command adds to multi-mem: its argument bytes plus the
// slice header each argument is held by. It is the same accounting Redis's multi-mem does
// (the argv allocation plus each argument's own bytes) expressed in Go's terms.
func queuedArgsSize(args [][]byte) int64 {
	n := int64(unsafe.Sizeof([]byte(nil))) * int64(len(args))
	for _, a := range args {
		n += int64(len(a))
	}
	return n
}

// loadString reads an optional atomic string, returning "" when nothing was stored.
func loadString(p *atomic.Pointer[string]) string {
	if v := p.Load(); v != nil {
		return *v
	}
	return ""
}

// clientUnblock implements CLIENT UNBLOCK <id> [TIMEOUT|ERROR]: end another
// connection's blocking command from this one.
//
// TIMEOUT (the default) makes the blocked client reply as if its timeout had elapsed,
// which is indistinguishable from the ordinary outcome and therefore safe for a client
// that cannot tell the difference. ERROR makes it reply with an -UNBLOCKED error, which
// is what a tool wants when it is deliberately interrupting a client and needs that
// client to know it was interrupted rather than to conclude the queue was empty.
//
// The reply is 1 if a client was unblocked and 0 if that client was not blocked (or
// does not exist), as in Redis.
func (s *Server) clientUnblock(w *resp.Writer, args [][]byte) {
	if len(args) != 3 && len(args) != 4 {
		w.WriteError("ERR wrong number of arguments for 'client|unblock' command")
		return
	}
	id, ok := parseInt64(args[2])
	if !ok {
		w.WriteError("ERR value is not an integer or out of range")
		return
	}
	errMsg := ""
	if len(args) == 4 {
		switch strings.ToUpper(string(args[3])) {
		case "TIMEOUT":
		case "ERROR":
			errMsg = errUnblockedClient
		default:
			w.WriteError("ERR CLIENT UNBLOCK reason should be TIMEOUT or ERROR")
			return
		}
	}
	w.WriteInt(int64(boolToInt(s.unblockClient(id, errMsg))))
}

// clientKill implements both CLIENT KILL forms: the old positional
// "CLIENT KILL addr:port", which replies +OK or an error, and the filter form
// "CLIENT KILL [ID id] [ADDR addr] [SKIPME yes|no]", which replies with the number
// killed. Two shapes exist because the old one cannot express "not me".
//
// A killed connection is closed, not marked: its goroutine is blocked in a read, and
// closing the socket is what unblocks it. The current connection is closed after its
// reply is flushed, for the same reason QUIT defers the close.
func (s *Server) clientKill(sess *session, w *resp.Writer, args [][]byte) {
	var (
		byAddr string
		byID   int64
		hasID  bool
		skipMe = true
		filter = len(args) > 3 || (len(args) == 3 && !strings.Contains(string(args[2]), ":"))
	)
	if !filter {
		if len(args) != 3 {
			w.WriteError("ERR syntax error")
			return
		}
		byAddr = string(args[2])
		skipMe = false // the old form kills whatever it names, including the caller
	} else {
		if len(args)%2 != 0 {
			w.WriteError("ERR syntax error")
			return
		}
		for i := 2; i+1 < len(args); i += 2 {
			value := string(args[i+1])
			switch strings.ToUpper(string(args[i])) {
			case "ID":
				id, ok := parseInt64(args[i+1])
				if !ok {
					w.WriteError("ERR client-id should be greater than 0")
					return
				}
				byID, hasID = id, true
			case "ADDR", "LADDR":
				byAddr = value
			case "SKIPME":
				switch strings.ToUpper(value) {
				case "YES":
					skipMe = true
				case "NO":
					skipMe = false
				default:
					w.WriteError("ERR syntax error")
					return
				}
			default:
				w.WriteError("ERR syntax error")
				return
			}
		}
	}

	var victims []*session
	for _, other := range s.snapshotSessions() {
		if hasID && other.id != byID {
			continue
		}
		if byAddr != "" && other.addr != byAddr {
			continue
		}
		if !hasID && byAddr == "" {
			continue // no filter at all: refuse to kill everything by accident
		}
		if other == sess && skipMe {
			continue
		}
		victims = append(victims, other)
	}

	if !filter {
		if len(victims) == 0 {
			w.WriteError("ERR No such client address in this server")
			return
		}
		w.WriteSimple("OK")
	} else {
		w.WriteInt(int64(len(victims)))
	}
	for _, victim := range victims {
		if victim == sess {
			sess.quit = true // close after the reply reaches the wire
			continue
		}
		if victim.conn != nil {
			victim.conn.Close()
		}
	}
}

// cmdSelect implements SELECT index: it points this connection at another database.
//
// Nothing is propagated. The database a *connection* is looking at is connection state
// like its name or its protocol; what reaches the AOF and the replicas is the database
// each individual write belongs to, which the propagation path emits as its own SELECT
// (see selectOnStream). A client's SELECT and the stream's SELECT are different things
// that happen to share a name.
func cmdSelect(s *Server, sess *session, w *resp.Writer, args [][]byte) {
	n, ok := parseInt64(args[1])
	if !ok {
		w.WriteError("ERR value is not an integer or out of range")
		return
	}
	// Cluster mode has one database, so SELECT has nowhere to go. Redis answers the same
	// way, and the reason is structural rather than a restriction: the slot map
	// partitions a single keyspace, so a second database would be a keyspace no slot
	// covers and therefore no node is responsible for.
	if n != 0 && s.ClusterEnabled() {
		w.WriteError(errClusterNoDB("SELECT"))
		return
	}
	if n < 0 || n >= int64(s.Databases()) {
		w.WriteError("ERR DB index is out of range")
		return
	}
	sess.db.Store(n)
	w.WriteSimple("OK")
}
