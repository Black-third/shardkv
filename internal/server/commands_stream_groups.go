package server

// The stream *consumer group* commands: XREAD, XREADGROUP, XACK, XGROUP, XPENDING,
// XCLAIM and XAUTOCLAIM.
//
// Split from commands_stream.go, which keeps the stream itself (XADD, the ranges, XDEL,
// XTRIM, XSETID). The line is drawn where the state is: everything here reads or mutates a
// group's position, its consumers, or its pending-entries list, and none of it changes the
// entries. That is also where the propagation rules differ -- see the package comment in
// commands_stream.go for why XREADGROUP, XCLAIM and XAUTOCLAIM ship their effects while the
// entry-level commands ship verbatim.

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

// --- XREAD / XREADGROUP -------------------------------------------------------

// xreadOpts is the parsed form of XREAD and XREADGROUP.
type xreadOpts struct {
	count    int
	blockMs  int64
	hasBlock bool
	noack    bool
	group    string
	consumer string
	keys     [][]byte
	// idIdx is where the id operands start in the original args slice, so a "$" can be
	// rewritten *in place* -- see resolveDollarIDs.
	idIdx int
	ids   [][]byte
}

// parseXRead parses both commands. isGroup selects XREADGROUP's extra GROUP/NOACK
// clauses and its different id vocabulary ("&gt;" instead of "$").
func parseXRead(args [][]byte, isGroup bool) (xreadOpts, string) {
	var o xreadOpts
	i := 1
	sawGroup := false
	for i < len(args) {
		word := strings.ToUpper(string(args[i]))
		switch {
		case word == "COUNT" && i+1 < len(args):
			n, ok := parseInt(args[i+1])
			if !ok {
				return o, "ERR value is not an integer or out of range"
			}
			o.count = max(n, 0)
			i += 2
		case word == "BLOCK" && i+1 < len(args):
			ms, ok := parseInt64(args[i+1])
			if !ok {
				return o, "ERR timeout is not an integer or out of range"
			}
			if ms < 0 {
				return o, "ERR timeout is negative"
			}
			o.blockMs, o.hasBlock = ms, true
			i += 2
		case word == "GROUP" && isGroup && i+2 < len(args):
			o.group, o.consumer = string(args[i+1]), string(args[i+2])
			sawGroup = true
			i += 3
		case word == "NOACK" && isGroup:
			o.noack = true
			i++
		case word == "STREAMS":
			rest := args[i+1:]
			if len(rest) == 0 || len(rest)%2 != 0 {
				if isGroup {
					return o, errUnbalancedXReadGroup
				}
				return o, errUnbalancedXRead
			}
			half := len(rest) / 2
			o.keys = rest[:half]
			o.ids = rest[half:]
			o.idIdx = i + 1 + half
			if isGroup && !sawGroup {
				return o, "ERR Missing GROUP keyword or consumer/group name in XREADGROUP"
			}
			return o, ""
		default:
			return o, "ERR syntax error"
		}
	}
	if isGroup {
		return o, "ERR Missing GROUP keyword or consumer/group name in XREADGROUP"
	}
	return o, "ERR syntax error"
}

// resolveDollarIDs replaces every "$" or "+" id operand with a concrete id, mutating the
// argument slice in place.
//
// It has to happen exactly once, on arrival, and the rewrite is what guarantees that.
// "$" means "entries added after I asked", so re-resolving it on each retry would move
// the goalpost every time the waiter woke and the command could never return. Writing
// the concrete id back into args is safe and is the point: the blocking machinery hands
// the same slice to every retry, so the second attempt reads the id the first one
// resolved. Nothing else uses these arguments afterwards -- a blocking command
// propagates its effect, never its own text.
//
// "+" means "the last entry", which is one place *before* the stream's last id -- XREAD
// returns what follows the id it is given, so the id it wants is the last one minus one.
// The distinction matters on a stream whose final entry has been deleted: the last id
// stays where it was, so "+" resolves behind a tombstone and the read correctly finds
// nothing, which is what Redis answers. On a stream that does not exist yet both spellings
// resolve to 0-0, so a blocking "+" waits for the first entry rather than returning
// immediately.
func (s *Server) resolveDollarIDs(args [][]byte, o *xreadOpts) {
	for i, id := range o.ids {
		spelling := string(id)
		if spelling != "$" && spelling != "+" {
			continue
		}
		last, _, err := s.store.XLastID(string(o.keys[i]))
		if err != nil {
			last = store.StreamID{}
		}
		if spelling == "+" && last != (store.StreamID{}) {
			last = last.Prev()
		}
		concrete := []byte(last.String())
		o.ids[i] = concrete
		args[o.idIdx+i] = concrete
	}
}

// xreadBlockKeys is the key list a blocked XREAD or XREADGROUP joins the queues of: the
// streams it is reading. It is only called after the command reported that it wants to
// wait, so the arguments are known to parse.
func xreadBlockKeys(args [][]byte) []string {
	isGroup := strings.EqualFold(string(args[0]), "XREADGROUP")
	o, errMsg := parseXRead(args, isGroup)
	if errMsg != "" {
		return nil
	}
	return byteStrings(o.keys)
}

// xreadTimeout reads the BLOCK operand, which is in milliseconds rather than the
// fractional seconds every other blocking command uses -- a historical difference in
// Redis's own interface that a compatible server has to keep.
func xreadTimeout(args [][]byte) (time.Duration, string) {
	isGroup := strings.EqualFold(string(args[0]), "XREADGROUP")
	o, errMsg := parseXRead(args, isGroup)
	if errMsg != "" {
		return 0, errMsg
	}
	if !o.hasBlock {
		return 0, ""
	}
	if o.blockMs > math.MaxInt64/int64(time.Millisecond) {
		return 0, "ERR timeout is out of range"
	}
	return time.Duration(o.blockMs) * time.Millisecond, ""
}

// blockingXRead is XREAD's non-blocking half.
//
// A command with no BLOCK clause must never wait, so when it finds nothing it writes
// the empty reply itself and reports that it does not want to wait. That is how one
// blockSpec serves both the blocking and the non-blocking form of the same command.
func blockingXRead(s *Server, w *resp.Writer, args [][]byte) ([][][]byte, bool) {
	o, errMsg := parseXRead(args, false)
	if errMsg != "" {
		w.WriteError(errMsg)
		return nil, false
	}
	for _, id := range o.ids {
		if string(id) == ">" {
			w.WriteError(errGTOutsideGroup)
			return nil, false
		}
	}
	s.resolveDollarIDs(args, &o)

	type result struct {
		key     []byte
		entries []store.StreamEntry
	}
	var out []result
	for i, k := range o.keys {
		after, ok := parseID(o.ids[i], 0)
		if !ok {
			w.WriteError(errInvalidStreamID)
			return nil, false
		}
		entries, err := s.store.XReadAfter(string(k), after, o.count)
		if err != nil {
			writeStoreErr(w, err)
			return nil, false
		}
		if len(entries) == 0 {
			continue
		}
		out = append(out, result{key: k, entries: entries})
	}
	if len(out) == 0 {
		if o.hasBlock {
			return nil, true // the caller may park this client
		}
		w.WriteNullArray()
		return nil, false
	}
	pairs := writeStreamsHeader(w, len(out))
	for _, r := range out {
		if pairs {
			w.WriteArrayHeader(2)
		}
		w.WriteBulk(r.key)
		writeStreamEntries(w, r.entries)
	}
	return nil, false // a read: nothing to propagate, and readOnly says so
}

// writeStreamsHeader writes the outer frame of an XREAD/XREADGROUP reply and reports
// whether each stream needs a two-element wrapper.
//
// RESP2 sends an array of [key, entries] pairs; RESP3 sends a map from key to entries,
// which is what Redis 7 sends and what lets a RESP3 client index the reply by stream
// name instead of scanning it.
func writeStreamsHeader(w *resp.Writer, n int) (pairs bool) {
	if w.Proto() >= resp.ProtoRESP3 {
		w.WriteMapHeader(n)
		return false
	}
	w.WriteArrayHeader(n)
	return true
}

// blockingXReadGroup is XREADGROUP's non-blocking half. It is a write: it advances the
// group's last-delivered id and creates pending entries, so it propagates the effect
// those amount to.
func blockingXReadGroup(s *Server, w *resp.Writer, args [][]byte) ([][][]byte, bool) {
	o, errMsg := parseXRead(args, true)
	if errMsg != "" {
		w.WriteError(errMsg)
		return nil, false
	}
	for _, id := range o.ids {
		// Both of XREAD's "where is the stream now" spellings are refused here, with the
		// messages Redis words for each: a group already tracks where its consumers are, so
		// asking for "after the current end" would return nothing and asking for "the last
		// entry" would hand out an entry the group has no record of delivering.
		switch string(id) {
		case "$":
			w.WriteError(errDollarInGroup)
			return nil, false
		case "+":
			w.WriteError(errPlusInGroup)
			return nil, false
		}
	}

	type result struct {
		key     []byte
		entries []store.StreamEntry
	}
	var out []result
	var effect [][][]byte
	sawNew := false // a ">" read is the only form that can be waited on
	for i, k := range o.keys {
		key := string(k)
		newOnly := string(o.ids[i]) == ">"
		var after store.StreamID
		if newOnly {
			sawNew = true
		} else {
			id, ok := parseID(o.ids[i], 0)
			if !ok {
				w.WriteError(errInvalidStreamID)
				return nil, false
			}
			after = id
		}
		res, err := s.store.XReadGroup(key, o.group, o.consumer, newOnly, after, o.count, o.noack)
		if err != nil {
			writeStreamStoreErr(w, err, key, o.group, nogroupRead)
			return nil, false
		}
		s.notifyConsumerCreated(res.ConsumerCreated, key)
		if len(res.Entries) == 0 {
			// The history form always answers, even with nothing outstanding: an empty PEL is
			// a fact about this consumer, not an absence of data to wait for.
			if !newOnly {
				out = append(out, result{key: k, entries: nil})
			}
			continue
		}
		out = append(out, result{key: k, entries: res.Entries})
		if newOnly {
			effect = append(effect, xreadGroupEffect(k, o, res)...)
		}
	}

	if len(out) == 0 {
		if o.hasBlock && sawNew {
			return effect, true
		}
		w.WriteNullArray()
		return effect, false
	}
	pairs := writeStreamsHeader(w, len(out))
	for _, r := range out {
		if pairs {
			w.WriteArrayHeader(2)
		}
		w.WriteBulk(r.key)
		writeStreamEntries(w, r.entries)
	}
	return effect, false
}

// xreadGroupEffect renders what one stream's delivery did, as commands a replica can
// replay to reach the identical group state: an XCLAIM per pending entry created, then
// the group's resulting position.
//
// The XCLAIMs come first because XGROUP SETID would otherwise move the group past the
// entries the claims name, and the claims are what reconstruct the PEL. FORCE creates
// the pending entry for an entry that was never pending; JUSTID keeps the delivery
// count exactly as the master recorded it instead of incrementing it again.
func xreadGroupEffect(key []byte, o xreadOpts, res store.XReadGroupResult) [][][]byte {
	var out [][][]byte
	for i, e := range res.Entries {
		if o.noack || i >= len(res.Delivery) {
			continue
		}
		d := res.Delivery[i]
		out = append(out, [][]byte{
			[]byte("XCLAIM"), key, []byte(o.group), []byte(d.Consumer),
			[]byte("0"), []byte(e.ID.String()),
			[]byte("TIME"), []byte(strconv.FormatInt(d.DeliveryMs, 10)),
			[]byte("RETRYCOUNT"), []byte(strconv.FormatInt(d.DeliveryCount, 10)),
			[]byte("FORCE"), []byte("JUSTID"),
		})
	}
	// Always emitted, including for NOACK, where it is the only thing that carries the
	// delivery: a NOACK read leaves no pending entry, so without it a replica's group
	// would re-deliver everything the master just handed out.
	out = append(out, [][]byte{
		[]byte("XGROUP"), []byte("SETID"), key, []byte(o.group),
		[]byte(res.LastDelivered.String()),
		[]byte("ENTRIESREAD"), []byte(strconv.FormatInt(res.EntriesRead, 10)),
	})
	return out
}

// --- XACK ---------------------------------------------------------------------

func cmdXAck(s *Server, w *resp.Writer, args [][]byte) bool {
	ids := make([]store.StreamID, 0, len(args)-3)
	for _, a := range args[3:] {
		id, ok := parseID(a, 0)
		if !ok {
			w.WriteError(errInvalidStreamID)
			return false
		}
		ids = append(ids, id)
	}
	n, err := s.store.XAck(string(args[1]), string(args[2]), ids)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(n)
	return n > 0
}

// --- XGROUP -------------------------------------------------------------------

// cmdXGroup implements XGROUP CREATE|SETID|DESTROY|CREATECONSUMER|DELCONSUMER|HELP.
//
// It propagates verbatim: every operand is a concrete id or a name, so a replica
// applying the same command reaches the same state. The one form that is not a client
// command at all is CREATE ... ENTRIESREAD and SETID ... ENTRIESREAD, which a snapshot
// and an XREADGROUP effect use to restore a group's read counter exactly.
func cmdXGroup(s *Server, w *resp.Writer, args [][]byte) [][][]byte {
	sub := strings.ToUpper(string(args[1]))
	if sub == "HELP" {
		writeHelp(w, "XGROUP <subcommand> [<arg> [value] [opt] ...]. Subcommands are:", []string{
			"CREATE <key> <groupname> <id|$> [MKSTREAM] [ENTRIESREAD <n>]",
			"    Create a new consumer group.",
			"CREATECONSUMER <key> <groupname> <consumer>",
			"    Create a new consumer in the specified group.",
			"DELCONSUMER <key> <groupname> <consumer>",
			"    Remove the specified consumer.",
			"DESTROY <key> <groupname>",
			"    Remove the specified group.",
			"SETID <key> <groupname> <id|$> [ENTRIESREAD <n>]",
			"    Set the current group id.",
		})
		return nil
	}
	if len(args) < 4 {
		writeUnknownSubcommand(w, "XGROUP", args[1])
		return nil
	}
	key, group := string(args[2]), string(args[3])

	switch sub {
	case "CREATE":
		return s.xgroupCreate(w, args, key, group)
	case "SETID":
		return s.xgroupSetID(w, args, key, group)

	case "DESTROY":
		destroyed, err := s.store.XGroupDestroy(key, group)
		if err != nil {
			return xgroupErr(w, err, key, group)
		}
		w.WriteInt(int64(boolToInt(destroyed)))
		if !destroyed {
			return nil
		}
		return [][][]byte{args} // fully determined by its own arguments

	case "CREATECONSUMER":
		if len(args) != 5 {
			w.WriteError("ERR wrong number of arguments for 'xgroup|createconsumer' command")
			return nil
		}
		created, err := s.store.XGroupCreateConsumer(key, group, string(args[4]))
		if err != nil {
			return xgroupErr(w, err, key, group)
		}
		w.WriteInt(int64(boolToInt(created)))
		if !created {
			return nil
		}
		return [][][]byte{args}

	case "DELCONSUMER":
		if len(args) != 5 {
			w.WriteError("ERR wrong number of arguments for 'xgroup|delconsumer' command")
			return nil
		}
		n, err := s.store.XGroupDelConsumer(key, group, string(args[4]))
		if err != nil {
			return xgroupErr(w, err, key, group)
		}
		w.WriteInt(n)
		// Always propagated, even for a consumer with nothing pending: removing it still
		// changes the group, and a replica that kept it would report a consumer the master
		// does not have.
		return [][][]byte{args}

	default:
		writeUnknownSubcommand(w, "XGROUP", args[1])
		return nil
	}
}

// xgroupErr writes the reply for a failed XGROUP subcommand and reports no effect.
func xgroupErr(w *resp.Writer, err error, key, group string) [][][]byte {
	if errors.Is(err, store.ErrNoSuchStreamKey) {
		w.WriteError(errXGroupNoKey)
		return nil
	}
	writeStreamStoreErr(w, err, key, group, nogroupAdmin)
	return nil
}

// xgroupCreate implements XGROUP CREATE key group <id|$> [MKSTREAM] [ENTRIESREAD n].
//
// What propagates is the concrete id, never the "$": a "$" replayed on a replica would
// resolve against *that* server's stream, which is only at the same position if it has
// already applied every write ahead of this one -- exactly the assumption a replicated
// "$" would be silently betting on. ENTRIESREAD is always emitted so the replica's read
// counter matches, which is what makes XINFO GROUPS lag agree on both sides.
func (s *Server) xgroupCreate(w *resp.Writer, args [][]byte, key, group string) [][][]byte {
	if len(args) < 5 {
		w.WriteError("ERR wrong number of arguments for 'xgroup|create' command")
		return nil
	}
	start, ok := s.groupStartID(key, args[4], false)
	if !ok {
		w.WriteError(errInvalidStreamID)
		return nil
	}
	mkstream := false
	// -1 is Redis's wire spelling of "the read counter is unknown", and it is what a plain
	// XGROUP CREATE leaves behind: the group has read nothing, but so has a group created
	// at "$", and reporting 0 for both would tell a client a caught-up group is behind.
	// See streamGroup.entriesRead.
	entriesRead := int64(-1)
	for i := 5; i < len(args); i++ {
		switch {
		case strings.EqualFold(string(args[i]), "MKSTREAM"):
			mkstream = true
		case strings.EqualFold(string(args[i]), "ENTRIESREAD") && i+1 < len(args):
			n, errMsg := parseEntriesRead(args[i+1])
			if errMsg != "" {
				w.WriteError(errMsg)
				return nil
			}
			entriesRead = n
			i++
		default:
			w.WriteError("ERR syntax error")
			return nil
		}
	}
	if err := s.store.XGroupCreate(key, group, start, mkstream, entriesRead, entriesRead >= 0); err != nil {
		return xgroupErr(w, err, key, group)
	}
	w.WriteSimple("OK")
	effect := [][]byte{
		[]byte("XGROUP"), []byte("CREATE"), args[2], args[3], []byte(start.String()),
	}
	if mkstream {
		// Carried through: if MKSTREAM created the stream here it has to create it there
		// too, or the replica answers the group command with "no such key".
		effect = append(effect, []byte("MKSTREAM"))
	}
	// Always carried, including the -1: the replica has to end up with the *same*
	// known-or-unknown state, or the two disagree about a group's lag from here on while
	// both look internally consistent -- invariant 4's failure shape exactly.
	effect = append(effect, []byte("ENTRIESREAD"), []byte(strconv.FormatInt(entriesRead, 10)))
	return [][][]byte{effect}
}

// xgroupSetID implements XGROUP SETID key group <id|$> [ENTRIESREAD n], propagating the
// resolved id for the reason xgroupCreate does.
func (s *Server) xgroupSetID(w *resp.Writer, args [][]byte, key, group string) [][][]byte {
	if len(args) < 5 {
		w.WriteError("ERR wrong number of arguments for 'xgroup|setid' command")
		return nil
	}
	id, ok := s.groupStartID(key, args[4], true)
	if !ok {
		w.WriteError(errInvalidStreamID)
		return nil
	}
	hasRead, entriesRead := false, int64(0)
	if len(args) > 5 {
		if len(args) != 7 || !strings.EqualFold(string(args[5]), "ENTRIESREAD") {
			w.WriteError("ERR syntax error")
			return nil
		}
		n, errMsg := parseEntriesRead(args[6])
		if errMsg != "" {
			w.WriteError(errMsg)
			return nil
		}
		hasRead, entriesRead = true, n
	}
	// hasRead means the count is *known*, not merely that the client typed ENTRIESREAD:
	// an explicit -1 asks for it to be forgotten, which is the same state a bare SETID
	// leaves. Either way the effect below reproduces it on the replica.
	if err := s.store.XGroupSetID(key, group, id, hasRead && entriesRead >= 0, entriesRead); err != nil {
		return xgroupErr(w, err, key, group)
	}
	w.WriteSimple("OK")
	effect := [][]byte{
		[]byte("XGROUP"), []byte("SETID"), args[2], args[3], []byte(id.String()),
	}
	if hasRead {
		effect = append(effect, []byte("ENTRIESREAD"), []byte(strconv.FormatInt(entriesRead, 10)))
	}
	return [][][]byte{effect}
}

// groupStartID resolves XGROUP CREATE/SETID's id operand, where "$" means "the stream's
// current last id" -- i.e. deliver only what arrives from now on.
//
// The "$" is resolved here, on the master, and the concrete id is what propagates,
// because the command itself carries no id a replica could agree on: a replica's stream
// is at the same position only if it has already applied every write ahead of this one,
// which is exactly the assumption a replicated "$" would be betting on.
func (s *Server) groupStartID(key string, arg []byte, allowExtremes bool) (store.StreamID, bool) {
	if string(arg) == "$" {
		last, _, err := s.store.XLastID(key)
		if err != nil {
			return store.StreamID{}, true // wrong type: the store call will report it
		}
		return last, true
	}
	// "-" and "+" are the smallest and largest ids that can be represented. XGROUP SETID
	// accepts them -- `XGROUP SETID k g -` is how Redis's own tests rewind a group to the
	// beginning -- and XGROUP CREATE does not, which is not an inconsistency to smooth
	// over: Redis parses CREATE's id strictly and SETID's loosely, and a compatible server
	// has to refuse where Redis refuses or a client's mistake goes unreported on one of
	// them.
	if allowExtremes {
		switch string(arg) {
		case "-":
			return store.StreamID{}, true
		case "+":
			return store.StreamID{Ms: math.MaxUint64, Seq: math.MaxUint64}, true
		}
	}
	return parseID(arg, 0)
}

// parseEntriesRead validates XGROUP's ENTRIESREAD operand: a count of entries the group
// has read, where -1 means "unknown". Any other negative value is refused, because the
// field feeds XINFO's lag arithmetic and a negative count there would produce a lag no
// consumer could act on.
func parseEntriesRead(b []byte) (int64, string) {
	n, valid := parseInt64(b)
	if !valid {
		return 0, "ERR value is not an integer or out of range"
	}
	if n < -1 {
		return 0, "ERR value for ENTRIESREAD must be positive or -1"
	}
	return n, ""
}

// --- XPENDING -----------------------------------------------------------------

// cmdXPending implements both forms:
//
//	XPENDING key group                                          -- the summary
//	XPENDING key group [IDLE ms] start end count [consumer]     -- the extended form
func cmdXPending(s *Server, w *resp.Writer, args [][]byte) bool {
	key, group := string(args[1]), string(args[2])
	if len(args) == 3 {
		sum, err := s.store.XPendingSummary(key, group)
		if err != nil {
			writeStreamStoreErr(w, err, key, group, nogroupPending)
			return false
		}
		writeXPendingSummary(w, sum)
		return false
	}

	i := 3
	minIdle := int64(0)
	if strings.EqualFold(string(args[i]), "IDLE") {
		if i+1 >= len(args) {
			w.WriteError("ERR syntax error")
			return false
		}
		n, ok := parseInt64(args[i+1])
		if !ok {
			w.WriteError("ERR value is not an integer or out of range")
			return false
		}
		minIdle = max(n, 0)
		i += 2
	}
	if len(args)-i < 3 || len(args)-i > 4 {
		w.WriteError("ERR syntax error")
		return false
	}
	start, ok1 := parseRangeBound(args[i], true)
	end, ok2 := parseRangeBound(args[i+1], false)
	if !ok1 || !ok2 {
		w.WriteError(errInvalidStreamID)
		return false
	}
	count, ok := parseInt(args[i+2])
	if !ok || count < 0 {
		w.WriteError("ERR value is not an integer or out of range")
		return false
	}
	consumer := ""
	if len(args)-i == 4 {
		consumer = string(args[i+3])
	}
	rows, err := s.store.XPendingRange(key, group,
		store.StreamRange{Start: start, End: end}, count, consumer, minIdle)
	if err != nil {
		writeStreamStoreErr(w, err, key, group, nogroupPending)
		return false
	}
	w.WriteArrayHeader(len(rows))
	for _, r := range rows {
		w.WriteArrayHeader(4)
		w.WriteBulk([]byte(r.ID.String()))
		w.WriteBulk([]byte(r.Consumer))
		w.WriteInt(r.ElapsedMs)
		w.WriteInt(r.DeliveryCount)
	}
	return false
}

// writeXPendingSummary writes [count, min-id, max-id, [[consumer, count], ...]]. The
// per-consumer counts are bulk strings rather than integers, which is what Redis sends
// -- a quirk, but one a client parses positionally.
func writeXPendingSummary(w *resp.Writer, sum store.StreamPendingSummary) {
	w.WriteArrayHeader(4)
	w.WriteInt(sum.Count)
	if sum.Count == 0 {
		w.WriteNull()
		w.WriteNull()
		w.WriteNullArray()
		return
	}
	w.WriteBulk([]byte(sum.Min.String()))
	w.WriteBulk([]byte(sum.Max.String()))
	w.WriteArrayHeader(len(sum.Consumers))
	for _, c := range sum.Consumers {
		w.WriteArrayHeader(2)
		w.WriteBulk([]byte(c.Name))
		w.WriteBulk([]byte(strconv.FormatInt(c.Count, 10)))
	}
}

// --- XCLAIM / XAUTOCLAIM ------------------------------------------------------

// cmdXClaim implements XCLAIM key group consumer min-idle-time id [id...]
// [IDLE ms] [TIME ms] [RETRYCOUNT n] [FORCE] [JUSTID] [LASTID id].
func cmdXClaim(s *Server, w *resp.Writer, args [][]byte) [][][]byte {
	key, group, consumer := string(args[1]), string(args[2]), string(args[3])
	minIdle, ok := parseInt64(args[4])
	if !ok {
		w.WriteError("ERR Invalid min-idle-time argument for XCLAIM")
		return nil
	}
	o := store.XClaimOptions{MinIdleMs: max(minIdle, 0)}

	var ids []store.StreamID
	i := 5
	for ; i < len(args); i++ {
		id, valid := parseID(args[i], 0)
		if !valid {
			break
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		w.WriteError(errInvalidStreamID)
		return nil
	}
	for ; i < len(args); i++ {
		word := strings.ToUpper(string(args[i]))
		switch {
		case word == "IDLE" && i+1 < len(args):
			n, valid := parseInt64(args[i+1])
			if !valid {
				w.WriteError("ERR Invalid IDLE option argument for XCLAIM")
				return nil
			}
			o.HasIdle, o.IdleMs = true, n
			i++
		case word == "TIME" && i+1 < len(args):
			n, valid := parseInt64(args[i+1])
			if !valid {
				w.WriteError("ERR Invalid TIME option argument for XCLAIM")
				return nil
			}
			o.HasTime, o.TimeMs = true, n
			i++
		case word == "RETRYCOUNT" && i+1 < len(args):
			n, valid := parseInt64(args[i+1])
			if !valid {
				w.WriteError("ERR Invalid RETRYCOUNT option argument for XCLAIM")
				return nil
			}
			o.HasRetryCount, o.RetryCount = true, n
			i++
		case word == "FORCE":
			o.Force = true
		case word == "JUSTID":
			o.JustID = true
		case word == "LASTID" && i+1 < len(args):
			id, valid := parseID(args[i+1], 0)
			if !valid {
				w.WriteError(errInvalidStreamID)
				return nil
			}
			o.HasLastID, o.LastID = true, id
			i++
		default:
			w.WriteError("ERR syntax error")
			return nil
		}
	}

	claimed, deleted, consumerCreated, err := s.store.XClaim(key, group, consumer, ids, o)
	if err != nil {
		writeStreamStoreErr(w, err, key, group, nogroupPending)
		return nil
	}
	s.notifyConsumerCreated(consumerCreated, key)
	writeClaimed(w, claimed, o.JustID)
	return claimEffect(args[1], group, consumer, claimed, deleted, o)
}

// writeClaimed writes the entries a claim produced, or just their ids for JUSTID.
func writeClaimed(w *resp.Writer, claimed []store.XClaimResult, justID bool) {
	w.WriteArrayHeader(len(claimed))
	for _, c := range claimed {
		if justID {
			w.WriteBulk([]byte(c.Entry.ID.String()))
			continue
		}
		writeStreamEntry(w, c.Entry)
	}
}

// claimEffect renders a claim as commands a replica can replay to the identical PEL.
//
// Each claimed id becomes an XCLAIM with an *absolute* TIME and the resulting
// RETRYCOUNT, so the replica records the same delivery instant however much later it
// applies the command -- the same reason every expiry on this server's wire is
// absolute. An id whose entry had been deleted is dropped from the PEL by the master,
// so the replica is told to drop it too, with the XACK that means exactly that.
func claimEffect(key []byte, group, consumer string, claimed []store.XClaimResult,
	deleted []store.StreamID, o store.XClaimOptions) [][][]byte {

	var out [][][]byte
	for _, c := range claimed {
		cmd := [][]byte{
			[]byte("XCLAIM"), key, []byte(group), []byte(consumer),
			[]byte("0"), []byte(c.Entry.ID.String()),
			[]byte("TIME"), []byte(strconv.FormatInt(c.DeliveryMs, 10)),
			[]byte("RETRYCOUNT"), []byte(strconv.FormatInt(c.DeliveryCount, 10)),
			[]byte("FORCE"), []byte("JUSTID"),
		}
		if o.HasLastID {
			cmd = append(cmd, []byte("LASTID"), []byte(o.LastID.String()))
		}
		out = append(out, cmd)
	}
	if len(deleted) > 0 {
		ack := [][]byte{[]byte("XACK"), key, []byte(group)}
		for _, id := range deleted {
			ack = append(ack, []byte(id.String()))
		}
		out = append(out, ack)
	}
	return out
}

// cmdXAutoClaim implements
// XAUTOCLAIM key group consumer min-idle-time start [COUNT n] [JUSTID].
func cmdXAutoClaim(s *Server, w *resp.Writer, args [][]byte) [][][]byte {
	key, group, consumer := string(args[1]), string(args[2]), string(args[3])
	minIdle, ok := parseInt64(args[4])
	if !ok {
		w.WriteError("ERR Invalid min-idle-time argument for XAUTOCLAIM")
		return nil
	}
	start, ok := parseRangeBound(args[5], true)
	if !ok {
		w.WriteError(errInvalidStreamID)
		return nil
	}
	count, justID := 100, false
	for i := 6; i < len(args); i++ {
		switch {
		case strings.EqualFold(string(args[i]), "COUNT") && i+1 < len(args):
			n, valid := parseInt(args[i+1])
			if !valid || n < 1 {
				w.WriteError("ERR COUNT must be > 0")
				return nil
			}
			count = n
			i++
		case strings.EqualFold(string(args[i]), "JUSTID"):
			justID = true
		default:
			w.WriteError("ERR syntax error")
			return nil
		}
	}

	claimed, deleted, cursor, consumerCreated, err := s.store.XAutoClaim(
		key, group, consumer, start, max(minIdle, 0), count, justID)
	if err != nil {
		writeStreamStoreErr(w, err, key, group, nogroupPending)
		return nil
	}
	s.notifyConsumerCreated(consumerCreated, key)
	// [next-cursor, [claimed entries], [ids that were dropped]] -- the third element is
	// what tells a caller that a pending entry's data had been deleted, which is
	// otherwise invisible.
	w.WriteArrayHeader(3)
	w.WriteBulk([]byte(cursor.String()))
	writeClaimed(w, claimed, justID)
	w.WriteArrayHeader(len(deleted))
	for _, id := range deleted {
		w.WriteBulk([]byte(id.String()))
	}
	return claimEffect(args[1], group, consumer, claimed, deleted, store.XClaimOptions{})
}

// xreadKeys reports the stream keys of an XREAD or XREADGROUP, for COMMAND GETKEYS.
// Their keys follow a STREAMS keyword rather than sitting at a fixed position, which is
// exactly the case COMMAND GETKEYS exists to answer.
func xreadKeys(args [][]byte) []string {
	return xreadBlockKeys(args)
}

// notifyConsumerCreated fires the keyspace event a group read or a claim owes when it
// names a consumer the group did not have.
//
// Redis reports an implicitly created consumer with the same `xgroup-createconsumer`
// event an explicit `XGROUP CREATECONSUMER` fires, and it is the *only* event
// XREADGROUP, XCLAIM and XAUTOCLAIM produce -- none of them fires an event of its own,
// which was checked against a live redis:7-alpine rather than assumed. The event has to
// come from the handler because nothing else can see it: the consumer is created as a
// side effect and the reply says nothing about it, so the command table's
// name-to-event mapping has nowhere to learn it from.
//
// It is called from inside the write path, while propMu is held, which is the same
// place and the same lock order (propMu -> subMu) that runWrite's own notification uses.
func (s *Server) notifyConsumerCreated(created bool, key string) {
	if !created {
		return
	}
	flags := notifyFlags(s.notifyFlags.Load())
	if flags == 0 {
		return
	}
	s.notifyKeyspaceEvent(flags, notifyStream, "xgroup-createconsumer", key)
}
