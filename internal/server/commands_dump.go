package server

// DUMP and RESTORE: the serialization that lets a key leave one node and arrive at
// another intact, and the basis of MIGRATE.
//
// # What the payload is, and what it deliberately is not
//
// It is *not* Redis's RDB object encoding. Reproducing that byte for byte would mean
// reimplementing ziplist, listpack, intset and quicklist encodings whose only purpose
// is to save memory in representations this store does not have -- and getting one of
// them subtly wrong would produce a payload a real Redis accepts and then misreads,
// which is the worst possible failure mode for a serialization. So this format is its
// own, and it says so in its first eight bytes.
//
// The layout mirrors Redis's *framing* exactly, because that framing is what makes a
// foreign payload detectable rather than misparsed:
//
//	+----------+--------------------+-----------+------------------+
//	| "SHARDKV1"| RESP command array | version   | CRC-64/ECMA      |
//	| 8 bytes   | N bytes            | 2 B LE    | 8 B LE           |
//	+----------+--------------------+-----------+------------------+
//
// Three independent gates therefore reject anything this server did not write: the
// magic (a real RDB payload fails it immediately), the version (a payload from a
// future format fails it), and the checksum (a corrupted or truncated payload fails
// it). Each answers with Redis's own message, so a client library that special-cases
// it keeps working. The reverse also holds: a real Redis rejects one of these, because
// the CRC-64 it computes over the body is the Jones-polynomial one and will not match.
//
// The body is the command sequence Store.DumpKey renders -- the same encoder the AOF
// rewrite and the replica seed use. That is the whole reason the format is cheap and
// the reason it is trustworthy: invariant 5 already guarantees that sequence
// reconstructs every one of the stored kinds exactly, chunked so no command can exceed
// the protocol's limits, with a stream's id counters, groups, consumers and
// pending-entries list included. A second encoder written just for DUMP would be a
// second thing to keep correct, and the one that drifted would drift silently.
//
// The body is read back with the package's own resp.Reader -- the parser that has a
// fuzz target -- rather than a hand-rolled one, so a malformed payload meets code that
// is already hardened against malformed input.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc64"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

func init() {
	register("DUMP", 2, false, cmdDump)
	// RESTORE's ttl operand counts from now, so it propagates the absolute ABSTTL form it
	// resolved rather than its own text. See restoreForm and propagation.go.
	registerEffect("RESTORE", -4, cmdRestore)
	// MIGRATE propagates the effect it had on *this* node's dataset, which is the DEL of
	// the keys that left. Shipping the command itself would have every replica open its
	// own connection to the destination and send the same keys again -- and an AOF replay
	// would do it once more on every restart, to a node that may no longer be there.
	registerEffect("MIGRATE", -6, cmdMigrate)
}

const (
	// dumpMagic prefixes every payload, so a payload from a real Redis (or from
	// anything else) is rejected on its first eight bytes instead of being fed to a
	// parser that might make something of it.
	dumpMagic = "SHARDKV1"
	// dumpFormatVersion is the payload format's version, carried in the same two-byte
	// little-endian slot Redis puts its RDB version in. A reader refuses anything
	// newer than it understands rather than guessing.
	dumpFormatVersion uint16 = 1
	// dumpFooterLen is the version plus the checksum: the fixed tail every payload ends
	// with.
	dumpFooterLen = 2 + 8
)

// wireError carries a message whose exact text is part of the protocol rather than a
// Go diagnostic: the client matches on it, so it is spelled the way Redis spells it,
// capital letters and all. That is also why these are a type of their own rather than
// errors.New values -- Go's convention for error strings is about text a programmer
// reads, and applying it here would silently change a reply a client library compares
// against.
type wireError string

func (e wireError) Error() string { return string(e) }

// errBadPayload is the single rejection for every way a payload can fail to be one of
// ours: wrong magic, unknown version, bad checksum, truncation, or a body that is not
// a well-formed command sequence. They share one message because they share one
// meaning -- this did not come from a DUMP on a compatible server -- and because it is
// the message Redis answers with.
const errBadPayload = wireError("DUMP payload version or checksum are wrong")

// errBadData is the failure of a payload that *is* ours and intact, but whose contents
// did not rebuild a key: a command outside the constructor set, or one the store
// refused. Redis distinguishes the two the same way, and the distinction matters when
// diagnosing -- a checksum failure means the bytes were damaged in transit, a data
// failure means they were not damaged and still did not work.
const errBadData = wireError("Bad data format")

// dumpCRC is the checksum table. CRC-64/ECMA is used because it is in the standard
// library; Redis's is CRC-64/Jones, and the difference is deliberate -- see the package
// comment on why these payloads must not be interchangeable with Redis's.
var dumpCRC = crc64.MakeTable(crc64.ECMA)

// restoreKeyPos is the set of commands a payload body may contain, mapped to the
// argument position their key occupies.
//
// It is a whitelist, not a convenience: a payload is attacker-controlled input, and
// replaying arbitrary commands out of one would let a crafted payload run FLUSHALL. It
// holds exactly the commands Store.DumpKey emits, so it is also the check that a
// payload written by a future version with more constructors is refused here rather
// than half-applied.
//
// The position is needed because RESTORE names the key and the payload does not get to
// choose it: every command is rewritten to point at the destination key before it
// runs. XGROUP is the one whose key is at argument 2 rather than 1 -- the same
// off-by-one that invariant 7 calls out for affectedKeys.
var restoreKeyPos = map[string]int{
	"SET": 1, "RPUSH": 1, "HSET": 1, "SADD": 1, "ZADD": 1,
	"XADD": 1, "XSETID": 1, "XCLAIM": 1,
	"XGROUP": 2,
}

// encodeDumpPayload serializes a key's constructor commands into a self-describing,
// version-stamped, checksummed payload. See the package comment for the layout.
func encodeDumpPayload(cmds [][][]byte) []byte {
	size := len(dumpMagic) + dumpFooterLen
	for _, c := range cmds {
		size += resp.CommandSize(c)
	}
	buf := make([]byte, 0, size)
	buf = append(buf, dumpMagic...)
	for _, c := range cmds {
		buf = append(buf, encodeCommand(c)...)
	}
	buf = binary.LittleEndian.AppendUint16(buf, dumpFormatVersion)
	return binary.LittleEndian.AppendUint64(buf, crc64.Checksum(buf, dumpCRC))
}

// decodeDumpPayload verifies a payload and returns the commands it carries.
//
// The three checks run in the order that makes a diagnosis possible: magic first (this
// is not ours at all), then version (ours, but from a newer format), then checksum
// (ours and the right format, but damaged). Only then is the body parsed, so a
// corrupted length prefix is never handed to the reader.
func decodeDumpPayload(payload []byte) ([][][]byte, error) {
	if len(payload) < len(dumpMagic)+dumpFooterLen {
		return nil, errBadPayload
	}
	if string(payload[:len(dumpMagic)]) != dumpMagic {
		return nil, errBadPayload
	}
	body := payload[len(dumpMagic) : len(payload)-dumpFooterLen]
	footer := payload[len(payload)-dumpFooterLen:]
	if v := binary.LittleEndian.Uint16(footer); v == 0 || v > dumpFormatVersion {
		return nil, errBadPayload
	}
	want := binary.LittleEndian.Uint64(footer[2:])
	if crc64.Checksum(payload[:len(payload)-8], dumpCRC) != want {
		return nil, errBadPayload
	}

	var cmds [][][]byte
	r := resp.NewReader(bytes.NewReader(body))
	for {
		args, err := r.ReadCommand()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil || len(args) == 0 {
			return nil, errBadPayload
		}
		cmds = append(cmds, args)
	}
	if len(cmds) == 0 {
		return nil, errBadPayload // a key is at least one constructor
	}
	return cmds, nil
}

// cmdDump implements DUMP key: the key's value, serialized, or a null reply when there
// is no such key. The TTL is deliberately not in the payload -- RESTORE carries it as
// its own operand, which is what lets a key be restored under a different deadline
// from the one it had.
func cmdDump(s *Server, w *resp.Writer, args [][]byte) bool {
	cmds, _, ok := s.store.DumpKey(string(args[1]))
	if !ok {
		w.WriteNull()
		return false
	}
	w.WriteBulk(encodeDumpPayload(cmds))
	return false
}

// restoreOpts are RESTORE's trailing options.
type restoreOpts struct {
	replace bool
	absTTL  bool
	idle    int64
	hasIdle bool
	freq    int64
	hasFreq bool
}

// parseRestoreOpts parses RESTORE's options, using Redis's messages for each way they
// can be wrong.
func parseRestoreOpts(args [][]byte) (restoreOpts, string) {
	var o restoreOpts
	for i := 0; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "REPLACE":
			o.replace = true
		case "ABSTTL":
			o.absTTL = true
		case "IDLETIME":
			if i+1 >= len(args) {
				return o, "ERR syntax error"
			}
			i++
			n, ok := parseInt64(args[i])
			if !ok {
				return o, "ERR value is not an integer or out of range"
			}
			if n < 0 {
				return o, "ERR Invalid IDLETIME value, must be >= 0"
			}
			o.idle, o.hasIdle = n, true
		case "FREQ":
			if i+1 >= len(args) {
				return o, "ERR syntax error"
			}
			i++
			n, ok := parseInt64(args[i])
			if !ok {
				return o, "ERR value is not an integer or out of range"
			}
			if n < 0 || n > 255 {
				return o, "ERR Invalid FREQ value, must be >= 0 and <= 255"
			}
			o.freq, o.hasFreq = n, true
		default:
			return o, "ERR syntax error"
		}
	}
	return o, ""
}

// cmdRestore implements
// RESTORE key ttl payload [REPLACE] [ABSTTL] [IDLETIME s] [FREQ f].
//
// ttl is milliseconds from now, or -- with ABSTTL -- an absolute Unix millisecond
// deadline; 0 means the key never expires. The checks run in Redis's order, so a
// client that distinguishes BUSYKEY from a bad payload sees the same one first.
//
// FREQ is accepted and validated but has no effect: it carries an LFU access counter,
// and this server's eviction sampler is LRU (see Store.EvictToLimit). Silently
// accepting an option that does nothing is worse than saying so, which is what the
// README's Cluster section does. IDLETIME, by contrast, is applied -- a key arriving
// from another node keeps the age it had there.
//
// A relative ttl is resolved once here, and the ABSTTL form built from that same value is
// what propagates (see restoreForm); a ttl of 0 or an operand already marked ABSTTL takes
// no clock reading and propagates verbatim.
func cmdRestore(s *Server, w *resp.Writer, args [][]byte) [][][]byte {
	key := string(args[1])
	ttl, ok := parseInt64(args[2])
	if !ok {
		w.WriteError("ERR value is not an integer or out of range")
		return nil
	}
	o, errMsg := parseRestoreOpts(args[4:])
	if errMsg != "" {
		w.WriteError(errMsg)
		return nil
	}
	if ttl < 0 {
		w.WriteError("ERR Invalid TTL value, must be >= 0")
		return nil
	}
	if !o.replace && s.store.Exists(key) {
		w.WriteError("BUSYKEY Target key name already exists.")
		return nil
	}
	cmds, err := decodeDumpPayload(args[3])
	if err != nil {
		w.WriteError("ERR " + err.Error())
		return nil
	}

	var deadline time.Time
	wire := args
	if ttl > 0 {
		if o.absTTL {
			deadline = time.UnixMilli(ttl)
		} else {
			atMs, valid := deadlineMs(s.store.Now().UnixMilli(), ttl, 1, true)
			if !valid {
				w.WriteError("ERR Invalid TTL value, must be >= 0")
				return nil
			}
			deadline = time.UnixMilli(atMs)
			wire = restoreForm(args, atMs)
		}
	}

	switch err := s.restoreKey(key, cmds, deadline, o); {
	case errors.Is(err, errBusyKey):
		w.WriteError("BUSYKEY Target key name already exists.")
		return nil
	case err != nil:
		w.WriteError("ERR " + err.Error())
		return nil
	}
	w.WriteSimple("OK")
	return [][][]byte{wire}
}

// errBusyKey is the commit losing a race with another connection that created the same
// key. The early check answers the ordinary case; this one closes the window between
// that check and the commit, which a single-threaded Redis does not have and a
// concurrent server does.
const errBusyKey = wireError("BUSYKEY")

// restoreKey rebuilds a payload's key and publishes it atomically.
//
// The rebuild happens in a scratch store that nothing else can reach, and only the
// finished value is moved into the real keyspace (store.AdoptKey). That is what makes
// RESTORE all-or-nothing: a payload that fails on its fourth command leaves no
// three-command remnant behind, no WATCHer is invalidated by a state that never became
// visible, and no keyspace notification escapes from the intermediate value. The
// scratch server carries its own empty serverCore precisely so those side channels are
// not merely unused but unreachable -- a handler that fires an event (XGROUP
// CREATECONSUMER does) finds notifications disabled there.
//
// The scratch store reads this server's clock, not time.Now, so a TTL in the payload's
// own commands resolves against the same instant everything else does (invariant 3).
func (s *Server) restoreKey(key string, cmds [][][]byte, deadline time.Time, o restoreOpts) error {
	scratch := store.New(1)
	scratch.SetClock(s.store.Now)
	view := &Server{serverCore: &serverCore{}, store: scratch, db: 0}
	w := resp.NewWriter(io.Discard)

	for _, cmd := range cmds {
		pos, allowed := restoreKeyPos[strings.ToUpper(string(cmd[0]))]
		if !allowed || pos >= len(cmd) {
			return errBadData
		}
		c := commandTable[strings.ToUpper(string(cmd[0]))]
		if c == nil || !arityOK(c.arity, len(cmd)) {
			return errBadData
		}
		// The payload does not get to choose the key: every command is retargeted at the
		// destination RESTORE named. The slice is copied rather than mutated in place
		// because cmd's backing array belongs to the client's request buffer.
		retargeted := make([][]byte, len(cmd))
		copy(retargeted, cmd)
		retargeted[pos] = []byte(key)

		before := w.ErrorsWritten()
		c.apply(view, w, retargeted)
		if w.ErrorsWritten() != before {
			return errBadData
		}
	}
	if !scratch.Exists(key) {
		return errBadData // the commands parsed but built nothing
	}
	if !deadline.IsZero() {
		scratch.ExpireAt(key, deadline)
	}
	if o.hasIdle {
		scratch.SetIdleSeconds(key, o.idle)
	}
	if !store.AdoptKey(s.store, scratch, key, o.replace) {
		return errBusyKey
	}
	return nil
}

// --- MIGRATE ------------------------------------------------------------------

// migrateOpts are MIGRATE's options.
type migrateOpts struct {
	copy    bool // leave the keys here as well
	replace bool // overwrite keys that already exist at the destination
	user    string
	pass    string
	hasAuth bool
}

// parseMigrateArgs parses everything after
// MIGRATE host port key destination-db timeout, returning the keys to move.
//
// The empty key at argument 3 is Redis's own signal that the keys are in a KEYS clause
// instead: MIGRATE has one key operand for the single-key form and a variadic list for
// the batch form, and the two are distinguished by that empty string rather than by
// arity. A batch is what makes a resharding tolerable -- one connection and one round
// trip for a hundred keys instead of a hundred of each.
func parseMigrateArgs(args [][]byte) (keys []string, o migrateOpts, errMsg string) {
	for i := 6; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "COPY":
			o.copy = true
		case "REPLACE":
			o.replace = true
		case "AUTH":
			if i+1 >= len(args) {
				return nil, o, "ERR syntax error"
			}
			i++
			o.pass, o.hasAuth = string(args[i]), true
		case "AUTH2":
			if i+2 >= len(args) {
				return nil, o, "ERR syntax error"
			}
			o.user, o.pass, o.hasAuth = string(args[i+1]), string(args[i+2]), true
			i += 2
		case "KEYS":
			if len(args[3]) != 0 {
				return nil, o, "ERR When using MIGRATE KEYS option, the key argument must be set to the empty string"
			}
			if i+1 >= len(args) {
				return nil, o, "ERR syntax error"
			}
			for _, k := range args[i+1:] {
				keys = append(keys, string(k))
			}
			return keys, o, ""
		default:
			return nil, o, "ERR syntax error"
		}
	}
	if len(args[3]) == 0 {
		return nil, o, "ERR When using MIGRATE KEYS option, the key argument must be set to the empty string"
	}
	return []string{string(args[3])}, o, ""
}

// migrateKeys reports the keys MIGRATE names, for commandKeys -- and so for
// COMMAND GETKEYS, for WATCH, and for the cluster redirect, all of which read the one
// extraction (invariant 7). It answers for a syntactically wrong command by reporting
// no keys, which leaves the error to the handler rather than routing on a guess.
func migrateKeys(args [][]byte) []string {
	if len(args) < 6 {
		return nil
	}
	keys, _, errMsg := parseMigrateArgs(args)
	if errMsg != "" {
		return nil
	}
	return keys
}

// cmdMigrate implements
// MIGRATE host port key|"" destination-db timeout [COPY] [REPLACE]
// [AUTH password | AUTH2 username password] [KEYS key [key ...]].
//
// It is DUMP and RESTORE joined by a socket: each key is serialized here, shipped to
// the destination as a RESTORE, and -- unless COPY was given -- deleted here once the
// destination has acknowledged it. The order is what makes it safe: a key is never
// removed from this node until the other node has confirmed it holds it, so a failure
// anywhere leaves the key where it was rather than nowhere.
//
// Each RESTORE is preceded by ASKING, because the destination is normally *importing*
// the slot and does not own it yet: without the flag it would answer MOVED and send the
// migration straight back here. That is the same one-shot flag a redirected client uses,
// which is the point -- migration is not a privileged back channel, it is the ordinary
// protocol driven by the node instead of the client.
//
// The reply is Redis's: +OK when at least one key moved, +NOKEY when none of the named
// keys existed here, and an error otherwise. The propagated effect is the DEL of what
// left (see the registration).
//
// Cost: MIGRATE holds propMu for its whole network round trip, because it is a write
// and invariant 1 orders writes against their propagation. Writes on this node are
// therefore serialized for the duration -- which is exactly what a single-threaded
// Redis does with its own MIGRATE, and why the timeout operand exists.
func cmdMigrate(s *Server, w *resp.Writer, args [][]byte) [][][]byte {
	port, ok := parseInt(args[2])
	if !ok || port <= 0 || port > 65535 {
		w.WriteError("ERR Invalid target port")
		return nil
	}
	destDB, ok := parseInt64(args[4])
	if !ok || destDB < 0 {
		w.WriteError("ERR Invalid destination database")
		return nil
	}
	timeoutMs, ok := parseInt64(args[5])
	if !ok || timeoutMs < 0 {
		w.WriteError("ERR timeout is not an integer or out of range")
		return nil
	}
	if timeoutMs == 0 {
		timeoutMs = 1000 // Redis's default when the operand is zero
	}
	keys, o, errMsg := parseMigrateArgs(args)
	if errMsg != "" {
		w.WriteError(errMsg)
		return nil
	}

	// Only the keys that are actually here are shipped. A key that is missing is not an
	// error -- a resharding loop asks for a batch and takes what is there -- and if none
	// of them exist the whole command is a NOKEY that changes nothing.
	type payload struct {
		key  string
		ttl  int64
		body []byte
	}
	var payloads []payload
	nowMs := s.store.Now().UnixMilli()
	for _, k := range keys {
		cmds, expireAtMs, found := s.store.DumpKey(k)
		if !found {
			continue
		}
		ttl := int64(0)
		if expireAtMs > 0 {
			// The destination is given the remaining life, not the deadline: RESTORE's operand
			// counts from its own now, and the two nodes' clocks are not the same clock. A
			// deadline that had already passed becomes 1ms rather than 0, since 0 means "no
			// expiry at all" and would silently make a volatile key permanent.
			ttl = max(expireAtMs-nowMs, 1)
		}
		payloads = append(payloads, payload{key: k, ttl: ttl, body: encodeDumpPayload(cmds)})
	}
	if len(payloads) == 0 {
		w.WriteSimple("NOKEY")
		return nil
	}

	addr := net.JoinHostPort(string(args[1]), strconv.Itoa(port))
	timeout := time.Duration(timeoutMs) * time.Millisecond
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		w.WriteError("IOERR error or timeout connecting to the client")
		return nil
	}
	defer func() { _ = conn.Close() }()
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		w.WriteError("IOERR error or timeout setting a deadline on the connection")
		return nil
	}
	r, cw := resp.NewReader(conn), resp.NewWriter(conn)

	if o.hasAuth {
		auth := []string{"AUTH", o.pass}
		if o.user != "" {
			auth = []string{"AUTH", o.user, o.pass}
		}
		if err := migrateStep(r, cw, auth...); err != nil {
			w.WriteError(migrateErr(err))
			return nil
		}
	}
	if err := migrateStep(r, cw, "SELECT", strconv.FormatInt(destDB, 10)); err != nil {
		w.WriteError(migrateErr(err))
		return nil
	}

	moved := make([]string, 0, len(payloads))
	for _, p := range payloads {
		// ASKING first: the destination is importing this slot and does not own it yet.
		if err := migrateStep(r, cw, "ASKING"); err != nil {
			// A destination that is not in cluster mode has no ASKING, and that is a
			// perfectly good target for a plain key move between two standalone servers.
			// Only a transport failure is fatal here.
			if !isRemoteError(err) {
				w.WriteError(migrateErr(err))
				return migrateEffect(s, moved, o)
			}
		}
		restore := []string{"RESTORE", p.key, strconv.FormatInt(p.ttl, 10), string(p.body)}
		if o.replace {
			restore = append(restore, "REPLACE")
		}
		if err := migrateStep(r, cw, restore...); err != nil {
			w.WriteError(migrateErr(err))
			// Whatever already arrived is still gone from here; report it so the effect
			// propagated matches the keyspace this node is left with.
			return migrateEffect(s, moved, o)
		}
		moved = append(moved, p.key)
	}
	w.WriteSimple("OK")
	return migrateEffect(s, moved, o)
}

// migrateEffect deletes the keys that reached the destination -- unless COPY asked for
// them to stay -- and returns the DEL to propagate.
//
// The deletion happens after the acknowledgement, never before: a key removed here
// before the other node confirmed it would be a key that exists nowhere if the
// connection dropped in between.
func migrateEffect(s *Server, moved []string, o migrateOpts) [][][]byte {
	if o.copy || len(moved) == 0 {
		return nil
	}
	del := make([][]byte, 0, len(moved)+1)
	del = append(del, []byte("DEL"))
	for _, k := range moved {
		s.store.Del(k)
		del = append(del, []byte(k))
	}
	return [][][]byte{del}
}

// migrateStep sends one command to the destination and reads its status reply.
func migrateStep(r *resp.Reader, w *resp.Writer, parts ...string) error {
	if err := w.WriteCommand(cmdBytes(parts...)); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	_, err := r.ReadStatus()
	return err
}

// isRemoteError distinguishes an error reply the destination sent from a failure of the
// connection itself. The first means the other node understood and refused; the second
// means the migration cannot continue at all.
func isRemoteError(err error) bool {
	return err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, resp.ErrProtocol) &&
		!isNetError(err)
}

func isNetError(err error) bool {
	var ne net.Error
	return errors.As(err, &ne)
}

// migrateErr renders a failure the way Redis does: a transport failure is an IOERR, and
// an error the destination replied with is quoted back so the operator sees the real
// reason (a BUSYKEY, most often, which means the destination already has the key and
// REPLACE was not given).
func migrateErr(err error) string {
	if isRemoteError(err) {
		return "ERR Target instance replied with error: " + err.Error()
	}
	return "IOERR error or timeout reading from target instance"
}

// restoreForm renders RESTORE with atMs in place of its relative TTL operand, marked
// ABSTTL so a replayer reads it as the absolute deadline it is. This is invariant 3
// applied to the one command that carries a TTL in milliseconds-from-now as a positional
// operand.
//
// Without it, a RESTORE replayed from an AOF an hour after it was written would give the
// key an hour more life than it had, and a replica would disagree with its master about
// when a migrated key disappears -- silently, since both would look internally
// consistent.
//
// atMs is the deadline cmdRestore already resolved and already gave the store: this is a
// renderer, not a second derivation, and it takes no clock. It is called only for a
// relative operand, so a ttl of 0 (no deadline) and one already marked ABSTTL never reach
// it and propagate verbatim -- there is nothing relative to rewrite.
func restoreForm(args [][]byte, atMs int64) [][]byte {
	out := make([][]byte, 0, len(args)+1)
	out = append(out, args[0], args[1], []byte(strconv.FormatInt(atMs, 10)))
	out = append(out, args[3:]...)
	return append(out, []byte("ABSTTL"))
}
