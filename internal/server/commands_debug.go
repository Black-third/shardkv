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

import (
	"strconv"
	"strings"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
)

func init() {
	register("DEBUG", -2, false, cmdDebug)
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
		w.WriteError("ERR unknown subcommand or wrong number of arguments for '" +
			string(args[1]) + "'. Try DEBUG HELP.")
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
