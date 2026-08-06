package server

// The stream commands.
//
// # What propagates, and why it is never the command
//
// Three of these commands decide something a replica could not decide the same way,
// so all three ship what they did rather than what they were asked to do:
//
//   - XADD with "*" takes its id from the clock. A replica replaying the literal
//     command would generate an id from *its* clock, and from that moment the two
//     copies would disagree about every id in the stream -- silently, because both
//     would look internally consistent. So XADD is registered with registerEffect and
//     ships the concrete id it assigned.
//   - XCLAIM and XAUTOCLAIM act on entries "idle for at least N milliseconds" and stamp
//     a new delivery time. Both are functions of the clock, so both ship the exact set
//     of ids they moved, with an absolute TIME and the resulting RETRYCOUNT -- the same
//     absolute-instant discipline every TTL on this server's wire follows.
//   - XREADGROUP hands out entries and records them in a pending-entries list. What it
//     hands out depends on what the group had already delivered, so it ships the PEL
//     entries it created (as XCLAIM ... FORCE JUSTID) plus the group's resulting
//     position (as XGROUP SETID ... ENTRIESREAD). A replica then holds byte-identical
//     group state, which is what makes a failover able to continue the same work.
//
// The blocking forms use the same machinery BLPOP does, so they inherit its guarantees:
// no lock is held while waiting, wakeup is FIFO per key, and the *effect* is what
// reaches the AOF and the replicas -- a replica replaying XREADGROUP BLOCK would wait
// forever on a connection with no client behind it.
//
// XREAD is the one blocking command here that is not a write: it reads a stream and
// changes nothing. It is registered with a read-only blockSpec, so it is allowed on a
// replica and never touches the write path.

import (
	"errors"
	"math"
	"strconv"
	"strings"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

func init() {
	// XADD's id is non-deterministic with "*", so it propagates its effect. See above.
	registerEffect("XADD", -5, cmdXAdd)
	register("XLEN", 2, false, cmdXLen)
	register("XRANGE", -4, false, cmdXRange)
	register("XREVRANGE", -4, false, cmdXRevRange)
	register("XDEL", -3, true, cmdXDel)
	register("XTRIM", -4, true, cmdXTrim)
	register("XSETID", -3, true, cmdXSetID)
	register("XACK", -4, true, cmdXAck)
	// XGROUP CREATE|SETID accept "$", which means "the stream's current last id" -- a
	// value only the master can resolve, so like XADD it propagates the concrete id.
	registerEffect("XGROUP", -2, cmdXGroup)
	register("XPENDING", -3, false, cmdXPending)
	register("XINFO", -2, false, cmdXInfo)
	registerEffect("XCLAIM", -6, cmdXClaim)
	registerEffect("XAUTOCLAIM", -6, cmdXAutoClaim)

	// XREAD is a read that can wait; XREADGROUP is a write that can wait. Both reuse the
	// blocking machinery in blocking.go, so both get its fairness and no-lock-while-
	// waiting guarantees for free.
	registerBlocking("XREAD", -4, &blockSpec{
		wantType: "stream",
		fn:       blockingXRead, keys: xreadBlockKeys, timeoutFn: xreadTimeout,
		empty: writeNullArray, readOnly: true,
	})
	registerBlocking("XREADGROUP", -7, &blockSpec{
		wantType:        "stream",
		wakeOnAnyChange: true,
		fn:              blockingXReadGroup, keys: xreadBlockKeys, timeoutFn: xreadTimeout,
		empty: writeNullArray,
		// Its effect is an XGROUP SETID, which must be propagated so a replica's group
		// advances identically -- but must not be reported, because Redis sends no
		// keyspace event when a consumer reads. See blockSpec.silentEffect.
		silentEffect: true,
	})
}

// Error messages Redis sends, spelled exactly, because client libraries match on them.
const (
	errInvalidStreamID = "ERR Invalid stream ID specified as stream command argument"
	errXGroupNoKey     = "ERR The XGROUP subcommand requires the key to exist. " +
		"Note that for CREATE you may want to use the MKSTREAM option to create " +
		"an empty stream automatically."
	errUnbalancedXRead = "ERR Unbalanced XREAD list of streams: " +
		"for each stream key an ID or '$' must be specified."
	errUnbalancedXReadGroup = "ERR Unbalanced XREADGROUP list of streams: " +
		"for each stream key an ID or '>' must be specified."
	errDollarInGroup = "ERR The $ ID is meaningless in the context of XREADGROUP: " +
		"you want to read the history of this consumer by specifying a proper ID, " +
		"or use the > ID to get new messages. The $ ID would just return an empty result set."
	errGTOutsideGroup = "ERR The > ID can be specified only when calling " +
		"XREADGROUP using the GROUP <group> <consumer> option."
	errPlusInGroup = "ERR The + ID is meaningless in the context of XREADGROUP: " +
		"you want to read the history of this consumer by specifying a proper ID, " +
		"or use the > ID to get new messages. The + ID would just return an empty result set."
	errXAddIDSmaller = "ERR The ID specified in XADD is equal or " +
		"smaller than the target stream top item"
	errXAddIDExhausted = "ERR The stream has exhausted the last possible ID, " +
		"unable to add more items"
	errXSetIDSmaller = "ERR The ID specified in XSETID is smaller than " +
		"the target stream top item"
	errBusyGroup = "BUSYGROUP Consumer Group name already exists"
)

// nogroupForm selects which of Redis's three NOGROUP texts a command answers with.
// There are genuinely three, measured against redis:7.2 (identically on amd64 and
// arm64), and a client that matches on the message sees the difference:
//
//	XREADGROUP                       No such key 'k' or consumer group 'g' in XREADGROUP with GROUP option
//	XPENDING, XCLAIM, XAUTOCLAIM     No such key 'k' or consumer group 'g'
//	XGROUP SETID|DELCONSUMER|...     No such consumer group 'g' for key name 'k'
//	XINFO CONSUMERS                  No such consumer group 'g' for key name 'k'
//
// This was a bool, which collapsed the middle form into the last one: the three
// commands that report on *work in flight* said "no such consumer group" where Redis
// names the key first and leaves open which of the two is missing. That is the useful
// distinction, because for those three the key may legitimately be gone.
type nogroupForm int

const (
	// nogroupRead is XREADGROUP's, which names the command because a read that finds
	// nothing and a read whose group vanished are different situations for a consumer.
	nogroupRead nogroupForm = iota
	// nogroupPending is XPENDING's, XCLAIM's and XAUTOCLAIM's.
	nogroupPending
	// nogroupAdmin is XGROUP's and XINFO CONSUMERS', where the key is known to exist --
	// a missing key is refused earlier, with "ERR no such key".
	nogroupAdmin
)

func nogroupError(key, group string, form nogroupForm) string {
	switch form {
	case nogroupRead:
		return "NOGROUP No such key '" + key + "' or consumer group '" + group +
			"' in XREADGROUP with GROUP option"
	case nogroupPending:
		return "NOGROUP No such key '" + key + "' or consumer group '" + group + "'"
	default:
		return "NOGROUP No such consumer group '" + group + "' for key name '" + key + "'"
	}
}

// writeStreamStoreErr maps the stream sentinels onto their RESP replies. Everything
// else falls through to the shared translator.
func writeStreamStoreErr(w *resp.Writer, err error, key, group string, form nogroupForm) {
	switch {
	case errors.Is(err, store.ErrNoGroup), errors.Is(err, store.ErrNoSuchStreamKey):
		w.WriteError(nogroupError(key, group, form))
	case errors.Is(err, store.ErrBusyGroup):
		w.WriteError(errBusyGroup)
	default:
		writeStoreErr(w, err)
	}
}

// --- id and range parsing -----------------------------------------------------

// parseID parses an id operand that must be complete enough to name one entry.
// seqDefault fills in a missing sequence: 0 at the start of a range, the maximum at
// the end, so "5" means "all of millisecond 5" from either side.
func parseID(b []byte, seqDefault uint64) (store.StreamID, bool) {
	return store.ParseStreamID(string(b), seqDefault)
}

// parseRangeBound parses one end of an XRANGE range, handling "-", "+" and the
// exclusive "(" prefix. The exclusive form is folded into the inclusive bound here, by
// stepping the id one place, so everything downstream deals in inclusive ranges only.
func parseRangeBound(b []byte, isStart bool) (store.StreamID, bool) {
	s := string(b)
	exclusive := strings.HasPrefix(s, "(")
	if exclusive {
		s = s[1:]
		if s == "" {
			return store.StreamID{}, false
		}
	}

	switch s {
	case "-":
		if exclusive {
			return store.StreamID{}, false // "(-" names nothing
		}
		return store.StreamID{}, true
	case "+":
		if exclusive {
			return store.StreamID{}, false
		}
		return store.StreamID{Ms: math.MaxUint64, Seq: math.MaxUint64}, true
	}
	seqDefault := uint64(0)
	if !isStart {
		seqDefault = math.MaxUint64
	}
	// An exclusive bound given as a bare millisecond excludes the whole millisecond,
	// which is what stepping from the *other* end of it achieves.
	if exclusive {
		if _, _, hasSeq := strings.Cut(s, "-"); !hasSeq {
			seqDefault = math.MaxUint64 - seqDefault
		}
	}
	id, ok := store.ParseStreamID(s, seqDefault)
	if !ok {
		return store.StreamID{}, false
	}
	if exclusive {
		if isStart {
			return id.Next(), true
		}
		return id.Prev(), true
	}
	return id, true
}

// --- XADD ---------------------------------------------------------------------

// parseTrim parses "MAXLEN|MINID [=|~] threshold" starting at args[i], returning the
// index just past it.
func parseTrim(args [][]byte, i int, o *store.TrimOptions) (int, string) {
	var strategy int
	switch {
	case strings.EqualFold(string(args[i]), "MAXLEN"):
		strategy = store.TrimMaxLen
	case strings.EqualFold(string(args[i]), "MINID"):
		strategy = store.TrimMinID
	default:
		// Nothing else can start a trim clause. Defaulting to MAXLEN here would make
		// "XTRIM key LIMIT 5" a silent trim to five entries.
		return i, "ERR syntax error"
	}
	i++
	if i >= len(args) {
		return i, "ERR syntax error"
	}
	switch string(args[i]) {
	case "~":
		o.Approx = true
		i++
	case "=":
		i++
	}
	if i >= len(args) {
		return i, "ERR syntax error"
	}
	o.Strategy = strategy
	if strategy == store.TrimMaxLen {
		n, ok := parseInt64(args[i])
		if !ok || n < 0 {
			return i, "ERR value is not an integer or out of range"
		}
		o.MaxLen = n
	} else {
		id, ok := parseID(args[i], 0)
		if !ok {
			return i, errInvalidStreamID
		}
		o.MinID = id
	}
	return i + 1, ""
}

// cmdXAdd implements
// XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold [LIMIT count]] <*|id> field value ...
func cmdXAdd(s *Server, w *resp.Writer, args [][]byte) [][][]byte {
	key := string(args[1])
	var o store.XAddOptions
	hasLimit := false
	i := 2
parse:
	for i < len(args) {
		switch {
		case strings.EqualFold(string(args[i]), "NOMKSTREAM"):
			o.NoMkStream = true
			i++
		case strings.EqualFold(string(args[i]), "MAXLEN"), strings.EqualFold(string(args[i]), "MINID"):
			next, errMsg := parseTrim(args, i, &o.Trim)
			if errMsg != "" {
				w.WriteError(errMsg)
				return nil
			}
			i = next
		case strings.EqualFold(string(args[i]), "LIMIT"):
			if i+1 >= len(args) {
				w.WriteError("ERR syntax error")
				return nil
			}
			n, ok := parseInt64(args[i+1])
			if !ok || n < 0 {
				w.WriteError("ERR value is not an integer or out of range")
				return nil
			}
			o.Trim.Limit = n
			hasLimit = true
			i += 2
		default:
			break parse
		}
	}
	if hasLimit && !o.Trim.Approx {
		w.WriteError("ERR syntax error, LIMIT cannot be used without the special ~ option")
		return nil
	}
	if i >= len(args) {
		w.WriteError("ERR wrong number of arguments for 'xadd' command")
		return nil
	}
	if !parseXAddID(args[i], &o) {
		w.WriteError(errInvalidStreamID)
		return nil
	}
	fields := args[i+1:]
	if len(fields) == 0 || len(fields)%2 != 0 {
		w.WriteError("ERR wrong number of arguments for 'xadd' command")
		return nil
	}

	id, trimmed, created, err := s.store.XAdd(key, o, fields)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrStreamIDSmaller):
			w.WriteError(errXAddIDSmaller)
		case errors.Is(err, store.ErrStreamIDExhausted):
			w.WriteError(errXAddIDExhausted)
		default:
			writeStoreErr(w, err)
		}
		return nil
	}
	if !created {
		w.WriteNull() // NOMKSTREAM and no such key
		return nil
	}
	w.WriteBulk([]byte(id.String()))

	// The effect: the concrete id, never "*", and the trim as the exact operation it
	// turned out to be. A replica running this sequence ends with the same entries under
	// the same ids, which is the whole point.
	effect := [][]byte{[]byte("XADD"), args[1], []byte(id.String())}
	effect = append(effect, fields...)
	out := [][][]byte{effect}
	if trimmed > 0 {
		out = append(out, trimEffect(args[1], o.Trim))
	}
	return out
}

// parseXAddID reads XADD's id operand: "*", "<ms>-*" or an explicit id.
func parseXAddID(b []byte, o *store.XAddOptions) bool {
	s := string(b)
	if s == "*" {
		o.Auto = true
		return true
	}
	if ms, seq, ok := strings.Cut(s, "-"); ok && seq == "*" {
		n, err := strconv.ParseUint(ms, 10, 64)
		if err != nil {
			return false
		}
		o.AutoSeq = true
		o.ID = store.StreamID{Ms: n}
		return true
	}
	id, ok := store.ParseStreamID(s, 0)
	if !ok {
		return false
	}
	o.ID = id
	return true
}

// trimEffect renders the trim a write performed as an exact XTRIM, dropping the "~"
// the client may have sent: the master has already decided what went, so the replica
// must remove exactly that and not re-derive it.
func trimEffect(key []byte, o store.TrimOptions) [][]byte {
	if o.Strategy == store.TrimMinID {
		return [][]byte{[]byte("XTRIM"), key, []byte("MINID"), []byte(o.MinID.String())}
	}
	return [][]byte{[]byte("XTRIM"), key, []byte("MAXLEN"), []byte(strconv.FormatInt(o.MaxLen, 10))}
}

// --- XLEN / XRANGE / XREVRANGE ------------------------------------------------

func cmdXLen(s *Server, w *resp.Writer, args [][]byte) bool {
	n, err := s.store.XLen(string(args[1]))
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(n)
	return false
}

func cmdXRange(s *Server, w *resp.Writer, args [][]byte) bool {
	return xrange(s, w, args, false)
}

func cmdXRevRange(s *Server, w *resp.Writer, args [][]byte) bool {
	return xrange(s, w, args, true)
}

// xrange is the shared body. XREVRANGE takes its bounds the other way round -- end
// first -- which is the only difference between the two commands.
func xrange(s *Server, w *resp.Writer, args [][]byte, rev bool) bool {
	startArg, endArg := args[2], args[3]
	if rev {
		startArg, endArg = args[3], args[2]
	}
	start, ok1 := parseRangeBound(startArg, true)
	end, ok2 := parseRangeBound(endArg, false)
	if !ok1 || !ok2 {
		w.WriteError(errInvalidStreamID)
		return false
	}
	count := 0
	if len(args) > 4 {
		if len(args) != 6 || !strings.EqualFold(string(args[4]), "COUNT") {
			w.WriteError("ERR syntax error")
			return false
		}
		n, ok := parseInt(args[5])
		if !ok {
			w.WriteError("ERR value is not an integer or out of range")
			return false
		}
		if n <= 0 {
			w.WriteArrayHeader(0)
			return false
		}
		count = n
	}
	entries, err := s.store.XRange(string(args[1]), store.StreamRange{Start: start, End: end}, count, rev)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	writeStreamEntries(w, entries)
	return false
}

// writeStreamEntries writes an array of [id, [field, value, ...]] pairs, which is the
// shape every command that returns entries shares -- in both protocols, because an
// entry's fields are an ordered list that may repeat a field name, not a map.
func writeStreamEntries(w *resp.Writer, entries []store.StreamEntry) {
	w.WriteArrayHeader(len(entries))
	for _, e := range entries {
		writeStreamEntry(w, e)
	}
}

func writeStreamEntry(w *resp.Writer, e store.StreamEntry) {
	w.WriteArrayHeader(2)
	w.WriteBulk([]byte(e.ID.String()))
	// A pending entry whose data has been deleted is reported as an id with a null field
	// list, which is how Redis says "you still owe an acknowledgement for something that
	// is gone".
	if e.Fields == nil {
		w.WriteNullArray()
		return
	}
	w.WriteArrayHeader(len(e.Fields))
	for _, f := range e.Fields {
		w.WriteBulk(f)
	}
}

// --- XDEL / XTRIM / XSETID ----------------------------------------------------

func cmdXDel(s *Server, w *resp.Writer, args [][]byte) bool {
	ids := make([]store.StreamID, 0, len(args)-2)
	for _, a := range args[2:] {
		id, ok := parseID(a, 0)
		if !ok {
			w.WriteError(errInvalidStreamID)
			return false
		}
		ids = append(ids, id)
	}
	n, err := s.store.XDel(string(args[1]), ids)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(n)
	return n > 0
}

// cmdXTrim implements XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT count].
func cmdXTrim(s *Server, w *resp.Writer, args [][]byte) bool {
	var o store.TrimOptions
	next, errMsg := parseTrim(args, 2, &o)
	if errMsg != "" {
		w.WriteError(errMsg)
		return false
	}
	if next < len(args) {
		if next+2 != len(args) || !strings.EqualFold(string(args[next]), "LIMIT") {
			w.WriteError("ERR syntax error")
			return false
		}
		n, ok := parseInt64(args[next+1])
		if !ok || n < 0 {
			w.WriteError("ERR value is not an integer or out of range")
			return false
		}
		if !o.Approx {
			w.WriteError("ERR syntax error, LIMIT cannot be used without the special ~ option")
			return false
		}
		o.Limit = n
	}
	n, err := s.store.XTrim(string(args[1]), o)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(n)
	return n > 0
}

// cmdXSetID implements XSETID key id [ENTRIESADDED n] [MAXDELETEDID id].
//
// It is what a snapshot uses to restore the counters a stream's entries do not carry,
// and it propagates verbatim: every operand is already absolute.
func cmdXSetID(s *Server, w *resp.Writer, args [][]byte) bool {
	id, ok := parseID(args[2], 0)
	if !ok {
		w.WriteError(errInvalidStreamID)
		return false
	}
	var o store.XSetIDOptions
	for i := 3; i < len(args); i += 2 {
		if i+1 >= len(args) {
			w.WriteError("ERR syntax error")
			return false
		}
		switch {
		case strings.EqualFold(string(args[i]), "ENTRIESADDED"):
			n, valid := parseInt64(args[i+1])
			if !valid || n < 0 {
				w.WriteError("ERR value is not an integer or out of range")
				return false
			}
			o.HasEntriesAdded, o.EntriesAdded = true, n
		case strings.EqualFold(string(args[i]), "MAXDELETEDID"):
			maxDeleted, valid := parseID(args[i+1], 0)
			if !valid {
				w.WriteError(errInvalidStreamID)
				return false
			}
			o.HasMaxDeleted, o.MaxDeleted = true, maxDeleted
		default:
			w.WriteError("ERR syntax error")
			return false
		}
	}
	if err := s.store.XSetID(string(args[1]), id, o); err != nil {
		switch {
		case errors.Is(err, store.ErrNoSuchStreamKey):
			w.WriteError("ERR The XSETID command requires the key to exist.")
		case errors.Is(err, store.ErrStreamSetIDSmaller):
			w.WriteError(errXSetIDSmaller)
		default:
			writeStoreErr(w, err)
		}
		return false
	}
	w.WriteSimple("OK")
	return true
}
