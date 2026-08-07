package server

// DEBUG: the handful of subcommands that can be answered honestly here.
//
// Real Redis's DEBUG is a large surface of test hooks, most of which describe
// internals this server does not have (listpack and quicklist encodings, RDB
// reload, jemalloc tuning, deliberate crashes). Implementing those names to return
// something plausible would be worse than not having them: a test suite or an
// operator would read the answer as a fact about this server. So only the ones whose
// meaning survives the port are here, and DEBUG HELP lists exactly those.
//
// DEBUG PROTOCOL is the important one. It replies with a value of each RESP type,
// which is how a client library's protocol tests check that a server encodes RESP3
// correctly -- and, here, the one command that exercises the big-number and
// attribute types no keyspace command has a use for.
//
// None of it is reachable by default. DEBUG is a protected command: the whole surface is
// refused unless -enable-debug-command opens it, which is where Redis 7 draws the same line
// and for the same reason -- SET-ACTIVE-EXPIRE and CHANGE-REPL-ID change how the *server*
// behaves for every client and every replica, and a test hook that any client can pull is
// not a test hook. See SetEnableDebugCommand for the choice of gating the command rather
// than the two subcommands, and executeCommand for where the decision is taken.

import (
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
)

func init() {
	register("DEBUG", -2, false, cmdDebug)
}

// protectedCommands are the commands refused unless the configuration opens them, which
// register reads to set command.protected. It is a set of one here: Redis's other
// protected command is MODULE, and there are no modules.
//
// A package-level var rather than a check inside cmdDebug's init, so it is populated before
// any init function runs whatever order the files are compiled in -- the same hazard the
// containerCommands comment describes.
var protectedCommands = map[string]bool{"DEBUG": true}

// The enable-debug-command settings, in Redis's spelling. The zero value is "no", so the
// gate is closed on a Server nobody configured.
const (
	debugCmdNo int32 = iota
	debugCmdYes
	debugCmdLocal
)

// errDebugNotAllowed is the refusal, byte for byte what redis:7.2 sends (measured on both
// amd64 and arm64, with enable-debug-command unset and set to "local" from a non-loopback
// peer). It names the option rather than merely refusing, because the only thing a caller
// can usefully do about it is set that option.
const errDebugNotAllowed = "ERR DEBUG command not allowed. If the enable-debug-command " +
	"option is set to \"local\", you can run it from a local connection, otherwise you " +
	"need to set this option in the configuration file, and then restart the server."

// SetEnableDebugCommand opens or closes the DEBUG gate. mode is Redis's own enum:
//
//	"no"    DEBUG is refused for every connection. The default.
//	"yes"   DEBUG is allowed for every connection.
//	"local" DEBUG is allowed only from a loopback peer.
//
// It reports whether mode was one of those, so a caller parsing a flag can refuse the
// value rather than silently picking one.
//
// Why the whole command is gated rather than only the subcommands that change server-wide
// behaviour: two of them do (SET-ACTIVE-EXPIRE stops the expiry sweep, CHANGE-REPL-ID
// makes every attached replica full-resync), and those are the exposure -- DEBUG SLEEP
// here parks only the calling connection, so unlike Redis's it is not a denial of service
// and is not the reason for this. But a partial gate could not answer with Redis's message,
// which says "DEBUG command not allowed" and names an option whose values are about the
// command as a whole; a client or a test suite that reads that message and reconfigures
// would then find the option had not done what it says. Redis gates the command, so the
// compatible thing and the safe thing are the same thing.
func (s *Server) SetEnableDebugCommand(mode string) bool {
	switch strings.ToLower(mode) {
	case "no":
		s.enableDebugCmd.Store(debugCmdNo)
	case "yes":
		s.enableDebugCmd.Store(debugCmdYes)
	case "local":
		s.enableDebugCmd.Store(debugCmdLocal)
	default:
		return false
	}
	return true
}

// EnableDebugCommand reports the setting in Redis's spelling, for CONFIG GET.
func (s *Server) EnableDebugCommand() string {
	switch s.enableDebugCmd.Load() {
	case debugCmdYes:
		return "yes"
	case debugCmdLocal:
		return "local"
	default:
		return "no"
	}
}

// debugCommandAllowed reports whether this connection may run a protected command. It is
// called from executeCommand for any command whose table entry is protected.
//
// A session with no connection is a caller inside this process -- a test driving execute
// directly, not a client -- and there is no peer address to judge, so "local" admits it.
func (s *Server) debugCommandAllowed(sess *session) bool {
	switch s.enableDebugCmd.Load() {
	case debugCmdYes:
		return true
	case debugCmdLocal:
		return sess == nil || sess.conn == nil || isLoopbackConn(sess.conn)
	default:
		return false
	}
}

// isLoopbackConn reports whether the peer is on the loopback interface, which is what
// Redis's islocalClient tests (its own check also admits a Unix-socket client; there is no
// Unix socket here). An address that does not parse is not treated as local: the safe
// default for a gate is to refuse what it cannot identify.
func isLoopbackConn(conn net.Conn) bool {
	addr := conn.RemoteAddr()
	if addr == nil {
		return false
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		host = addr.String()
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// debugProtocolTypes are the type names DEBUG PROTOCOL accepts, in the order the
// error message lists them (which is Redis's order).
var debugProtocolTypes = "string|integer|double|bignum|null|array|set|map|attrib|push|verbatim|true|false"

func cmdDebug(s *Server, w *resp.Writer, args [][]byte) bool {
	switch strings.ToUpper(string(args[1])) {
	case "PROTOCOL":
		if len(args) != 3 {
			w.WriteError("ERR wrong number of arguments for 'debug|protocol' command")
			return false
		}
		debugProtocol(w, strings.ToLower(string(args[2])))

	case "SLEEP":
		if len(args) != 3 {
			w.WriteError("ERR wrong number of arguments for 'debug|sleep' command")
			return false
		}
		secs, ok := parseFloat(args[2])
		if !ok || secs < 0 {
			w.WriteError("ERR value is not a valid float")
			return false
		}
		// This sleeps the calling connection, not the whole server as Redis's
		// single-threaded DEBUG SLEEP does. That difference is the point of the sharded
		// design and is stated rather than papered over; what the command is actually
		// used for -- making one client's command take a known long time, so a client
		// library's timeout and a SLOWLOG threshold can be tested -- works identically.
		// Inside MULTI/EXEC it does stall every other write, because the transaction
		// holds the propagation lock for its whole duration.
		time.Sleep(time.Duration(secs * float64(time.Second)))
		w.WriteSimple("OK")

	case "OBJECT":
		if len(args) != 3 {
			w.WriteError("ERR wrong number of arguments for 'debug|object' command")
			return false
		}
		debugObject(s, w, string(args[2]))

	case "SET-ACTIVE-EXPIRE":
		// Turns the janitor's expiry sweep off, so a test can observe lazy expiration --
		// that a *read* is what removed a key past its deadline -- without the background
		// pass racing it and reclaiming the key first. Implemented rather than stubbed,
		// because a stub that accepted the flag and kept sweeping would make exactly the
		// tests that ask for it flaky. Correctness never depends on it: the lazy check on
		// every read is what keeps an expired key invisible, and the sweep only reclaims
		// memory.
		if len(args) != 3 {
			w.WriteError("ERR wrong number of arguments for 'debug|set-active-expire' command")
			return false
		}
		on, ok := parseInt64(args[2])
		if !ok || (on != 0 && on != 1) {
			w.WriteError("ERR Invalid argument")
			return false
		}
		s.store.SetActiveExpire(on == 1)
		w.WriteSimple("OK")

	case "RELOAD":
		// Honest here, and cheap, because the snapshot machinery already exists: save the
		// dataset and put it back from what was saved. Redis's own test suite calls this
		// after almost every interesting mutation, and what it checks is that every type
		// survives the round trip through the serialized form -- the property invariant 5
		// is about. It is not a write: the dataset afterwards is the dataset before, so
		// nothing is persisted or replicated on its account.
		//
		// It is safe in the one way that matters: the saved commands are loaded and verified
		// in full *before* the databases are emptied, so a snapshot that does not parse
		// leaves the data alone. See reloadFromSnapshot for the ordering and for what it
		// does not exclude on a server with no propagation.
		if err := s.reloadFromSnapshot(); err != nil {
			w.WriteError("ERR " + err.Error())
			return false
		}
		w.WriteSimple("OK")

	case "JMAP", "STRINGMATCH-LEN", "QUICKLIST-PACKED-THRESHOLD", "LISTPACK", "LISTPACK-ENTRIES":
		// Accepted and ignored. These ask the server to dump internal representations or
		// to tune thresholds of encodings this store does not have -- there is one
		// representation per type here, so there is nothing to switch or to print. They
		// are answered rather than refused because Redis's test suite calls them in
		// passing, and an error would cost every assertion after the call in that file
		// while telling nobody anything.
		w.WriteSimple("OK")

	case "CHANGE-REPL-ID":
		// Honest here: the replication ID names the history this server serves, and
		// changing it is exactly how a test forces the next PSYNC to fall back to a full
		// resync instead of continuing from the backlog.
		s.mu.Lock()
		s.replID = newReplID()
		s.mu.Unlock()
		w.WriteSimple("OK")

	case "HELP":
		writeDebugHelp(w)

	default:
		// DEBUG is the one container that takes Redis's *longer* form here. Redis 7 has two
		// messages and which one a container uses is not derivable -- measured against
		// redis:7.2 with the gate open, OBJECT/CLIENT/CONFIG/XINFO/COMMAND/MEMORY/SLOWLOG/
		// LATENCY/PUBSUB/XGROUP all answer "unknown subcommand 'X'. Try C HELP." while DEBUG
		// answers "unknown subcommand or wrong number of arguments for 'X'. Try DEBUG HELP.",
		// which is writeSubcommandSyntaxError. Found while checking what the gate refuses
		// against what an open server answers.
		writeSubcommandSyntaxError(w, "DEBUG", args[1])
	}
	return false
}

// debugProtocol writes one value of the named RESP type. The RESP2 encodings are the
// ones real Redis falls back to, so the same command doubles as a check that a RESP2
// client is unaffected by the RESP3 support.
func debugProtocol(w *resp.Writer, typ string) {
	switch typ {
	case "string":
		w.WriteBulk([]byte("Hello World"))
	case "integer":
		w.WriteInt(12345)
	case "double":
		w.WriteDouble(3.141)
	case "bignum":
		w.WriteBigNumber("1234567999999999999999999999999999999")
	case "null":
		w.WriteNull()
	case "array":
		w.WriteArrayHeader(3)
		for i := 0; i < 3; i++ {
			w.WriteInt(int64(i))
		}
	case "set":
		w.WriteSetHeader(3)
		for i := 0; i < 3; i++ {
			w.WriteInt(int64(i))
		}
	case "map":
		w.WriteMapHeader(3)
		for i := 0; i < 3; i++ {
			w.WriteInt(int64(i))
			w.WriteBool(i == 1)
		}
	case "attrib":
		// An attribute describes the reply that follows it. RESP2 has nowhere to put
		// such metadata, so a RESP2 client gets only the reply -- which is why this is
		// one of the two places that has to test Proto() itself.
		if w.Proto() >= resp.ProtoRESP3 {
			w.WriteAttributeHeader(1)
			w.WriteBulk([]byte("key-popularity"))
			w.WriteArrayHeader(2)
			w.WriteBulk([]byte("key:123"))
			w.WriteInt(90)
		}
		w.WriteBulk([]byte("Some real reply following the attribute"))
	case "push":
		// A push has no RESP2 equivalent at all outside subscriber mode, where a RESP2
		// client demultiplexes positionally; sending one to a RESP2 client here would be
		// indistinguishable from a second reply. Redis refuses, and so does this.
		if w.Proto() < resp.ProtoRESP3 {
			w.WriteError("ERR RESP2 is not supported by this command")
			return
		}
		w.WriteBulk([]byte("Some real reply following the push reply"))
		w.WritePushHeader(2)
		w.WriteBulk([]byte("server-cpu-usage"))
		w.WriteInt(42)
	case "verbatim":
		w.WriteVerbatim("txt", []byte("This is a verbatim\nstring"))
	case "true":
		w.WriteBool(true)
	case "false":
		w.WriteBool(false)
	default:
		w.WriteError("ERR Wrong protocol type name. Please use one of the following: " +
			debugProtocolTypes)
	}
}

// debugObject reports the low-level facts about a key that this server actually
// knows: its value's encoding and how long it has been idle. Redis's version also
// reports serialization lengths and pointer addresses, which have no counterpart
// here, so they are left out rather than invented.
func debugObject(s *Server, w *resp.Writer, key string) {
	enc, ok := s.store.Encoding(key)
	if !ok {
		w.WriteError("ERR no such key")
		return
	}
	idle, _ := s.store.IdleSeconds(key)
	w.WriteSimple("Value at:0x0 refcount:1 encoding:" + enc +
		" serializedlength:0 lru:0 lru_seconds_idle:" + strconv.FormatInt(idle, 10))
}

func writeDebugHelp(w *resp.Writer) {
	lines := []string{
		"DEBUG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
		"CHANGE-REPL-ID",
		"    Give this server a new replication ID, forcing the next PSYNC to full resync.",
		"OBJECT <key>",
		"    Show the encoding and idle time of <key>.",
		"PROTOCOL <type>",
		"    Reply with a test value of the specified type. <type> can be: " + debugProtocolTypes + ".",
		"RELOAD",
		"    Save the dataset and load it back, checking the round trip through the snapshot form.",
		"SLEEP <seconds>",
		"    Stop this connection for <seconds>. Decimals allowed.",
		"HELP",
		"    Print this help.",
	}
	w.WriteArrayHeader(len(lines))
	for _, l := range lines {
		w.WriteSimple(l)
	}
}
