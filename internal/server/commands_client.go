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
	"strconv"
	"strings"
	"sync/atomic"
	"time"

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
	s.unwatchAll(sess)
	s.unsubscribeSession(sess)
	sess.name.Store(nil)
	sess.authenticated = s.RequirePass() == ""
	sess.db.Store(0)
	w.SetProto(resp.ProtoRESP2)
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

	role := "master"
	if s.isReplica() {
		role = "replica"
	}
	if proto >= resp.ProtoRESP3 {
		// RESP3: a map, with proto and id as integers rather than strings -- the shape
		// real Redis sends, which is what a RESP3 client library validates.
		w.WriteMapHeader(7)
		w.WriteBulk([]byte("server"))
		w.WriteBulk([]byte("shardkv"))
		w.WriteBulk([]byte("version"))
		w.WriteBulk([]byte(Version))
		w.WriteBulk([]byte("proto"))
		w.WriteInt(int64(proto))
		w.WriteBulk([]byte("id"))
		w.WriteInt(sess.id)
		w.WriteBulk([]byte("mode"))
		w.WriteBulk([]byte("standalone"))
		w.WriteBulk([]byte("role"))
		w.WriteBulk([]byte(role))
		w.WriteBulk([]byte("modules"))
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
		w.WriteBulk([]byte(f))
	}
	w.WriteBulk([]byte("modules"))
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
			w.WriteBulk([]byte(name))
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
		w.WriteVerbatim("txt", []byte(clientInfoLine(sess)))

	case "LIST":
		var b strings.Builder
		for _, other := range s.snapshotSessions() {
			b.WriteString(clientInfoLine(other))
			b.WriteString("\n")
		}
		w.WriteVerbatim("txt", []byte(b.String()))

	case "UNBLOCK":
		s.clientUnblock(w, args)

	case "KILL":
		s.clientKill(sess, w, args)

	default:
		w.WriteError("ERR Unknown subcommand or wrong number of arguments for '" +
			string(args[1]) + "'. Try CLIENT HELP.")
	}
}

// clientInfoLine renders one connection the way CLIENT LIST and CLIENT INFO do.
// The field names are Redis's, because client libraries and operators parse them.
func clientInfoLine(sess *session) string {
	now := time.Now()
	idle := now.Sub(time.Unix(0, sess.lastActive.Load()))
	flags := "N"
	switch {
	case sess.isReplicaFeed.Load():
		flags = "S" // a replication feed, as Redis flags a connected replica
	case sess.inSubscriberMode():
		flags = "P" // as Redis flags a connection in subscriber mode
	}
	multi := "-1"
	if sess.inMulti {
		multi = strconv.Itoa(len(sess.queued))
	}
	return "id=" + strconv.FormatInt(sess.id, 10) +
		" addr=" + sess.addr +
		" name=" + sess.clientName() +
		" age=" + strconv.Itoa(int(now.Sub(sess.createdAt).Seconds())) +
		" idle=" + strconv.Itoa(int(idle.Seconds())) +
		" flags=" + flags +
		" db=" + strconv.FormatInt(sess.db.Load(), 10) +
		" sub=" + strconv.FormatInt(sess.nSub.Load(), 10) +
		" psub=" + strconv.FormatInt(sess.nPSub.Load(), 10) +
		" multi=" + multi +
		" cmd=" + sess.lastCommand() +
		// Reported even when empty, because Redis reports them unconditionally and a
		// parser that splits on spaces counts fields.
		" lib-name=" + loadString(&sess.libName) +
		" lib-ver=" + loadString(&sess.libVer)
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
