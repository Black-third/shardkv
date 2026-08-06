package store

// The stream type: an append-only log of field/value entries addressed by
// millisecond-sequence ids, plus the consumer groups that track who has read what.
//
// # The id, and why it is a pair
//
// An id is <ms>-<seq>: a millisecond timestamp and a counter that disambiguates
// entries added inside the same millisecond. Ids are compared as the pair, high part
// first, so they sort in insertion order and a client can name a time range by naming
// ids. The pair is unsigned 64-bit in both halves, which is what Redis uses, so every
// id Redis can produce round-trips through this server unchanged.
//
// # Monotonicity when the clock moves backwards
//
// nextID never trusts the clock to be increasing. It takes the clock's millisecond
// only when that is *strictly greater* than the last id's, and otherwise keeps the
// last id's millisecond and increments the sequence. So a clock that jumps backwards
// -- NTP stepping, a VM restoring from a snapshot, an operator setting the date --
// cannot produce an id that sorts before an id already in the stream. The cost is that
// ids can lead the wall clock for as long as it takes the clock to catch up, which is
// exactly the trade Redis makes and the only one available: the alternative, refusing
// the write, would turn a clock correction into an outage.
//
// # The ordering structure
//
// Entries live in a single slice kept sorted by id. That is not a compromise: ids are
// generated in increasing order, so an append is O(1) amortized and the slice is
// sorted by construction, never by sorting. A range query binary-searches for its
// start and then walks (O(log n + k)), which is what XRANGE and XREAD need. XDEL is
// the only operation that pays: removing an entry from the middle is an O(n) memmove.
// That is the right way round -- streams are appended to constantly and deleted from
// rarely -- and it buys a structure with no per-entry pointer overhead, no rebalancing
// and no iterator invalidation, where Redis needs a radix tree of listpacks to reach
// the same asymptotics.
//
// # Trimming
//
// MAXLEN and MINID both remove a prefix of the slice, which is O(1) in the number
// retained. Redis's `~` (approximate) exists because its entries are packed into
// macro-nodes it will not split, so it stops at a node boundary and can leave more
// entries than asked. This implementation has no macro-nodes, so `~` trims exactly,
// which satisfies the contract `~` states ("at least this many are retained") while
// also satisfying the stronger one `=` states. LIMIT is accepted and has no effect for
// the same reason: it bounds the work an approximate trim will do, and an exact
// prefix removal has no unbounded work to bound.

import (
	"errors"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Stream-specific sentinel errors.
//
// They are Go-style lower-case sentinels rather than Redis's own wording. The wording
// clients match on is capitalized ("The ID specified in XADD is ..."), which belongs on
// the wire and not in a Go error value; the server maps each sentinel to the exact text
// Redis sends (see writeStreamStoreErr and cmdXAdd).
var (
	// ErrStreamIDSmaller is returned by XAdd for an explicit id that does not sort
	// after every id already in the stream.
	ErrStreamIDSmaller = errors.New("stream id is equal or smaller than the target stream top item")
	// ErrStreamIDExhausted is returned when the sequence counter of the last
	// millisecond has no room left, which is the one case in which an append is
	// genuinely impossible.
	ErrStreamIDExhausted = errors.New("the stream has exhausted the last possible id")
	// ErrStreamSetIDSmaller is returned by XSetID for an id below the top entry.
	ErrStreamSetIDSmaller = errors.New("stream id is smaller than the target stream top item")
	// ErrNoGroup is returned when a group-scoped operation names a group that does not
	// exist (or a key that does not).
	ErrNoGroup = errors.New("no such consumer group")
	// ErrBusyGroup is returned by XGroupCreate when the group already exists.
	ErrBusyGroup = errors.New("consumer group name already exists")
	// ErrNoSuchStreamKey is returned by the group commands when the key is missing and
	// they were not asked to create it.
	ErrNoSuchStreamKey = errors.New("no such key")
)

// StreamID is an entry id: a millisecond timestamp and a sequence number.
type StreamID struct {
	Ms  uint64
	Seq uint64
}

// maxStreamID is the largest id there is, which is what "+" means in a range.
var maxStreamID = StreamID{Ms: math.MaxUint64, Seq: math.MaxUint64}

// Compare orders two ids: negative if a sorts before b, zero if equal, positive
// otherwise. The millisecond dominates; the sequence breaks the tie.
func (id StreamID) Compare(other StreamID) int {
	switch {
	case id.Ms < other.Ms:
		return -1
	case id.Ms > other.Ms:
		return 1
	case id.Seq < other.Seq:
		return -1
	case id.Seq > other.Seq:
		return 1
	}
	return 0
}

// String renders the id as <ms>-<seq>, the form clients send and receive.
func (id StreamID) String() string {
	return strconv.FormatUint(id.Ms, 10) + "-" + strconv.FormatUint(id.Seq, 10)
}

// Next returns the smallest id strictly greater than this one, which is how an
// exclusive range start becomes an inclusive one. It saturates at the maximum id
// rather than wrapping, so an exclusive range that starts at the largest id there is
// selects nothing instead of everything.
func (id StreamID) Next() StreamID {
	if id.Seq == math.MaxUint64 {
		if id.Ms == math.MaxUint64 {
			return id
		}
		return StreamID{Ms: id.Ms + 1}
	}
	return StreamID{Ms: id.Ms, Seq: id.Seq + 1}
}

// Prev returns the largest id strictly smaller than this one, for an exclusive range
// end. It saturates at zero for the same reason Next saturates at the maximum.
func (id StreamID) Prev() StreamID {
	if id.Seq == 0 {
		if id.Ms == 0 {
			return id
		}
		return StreamID{Ms: id.Ms - 1, Seq: math.MaxUint64}
	}
	return StreamID{Ms: id.Ms, Seq: id.Seq - 1}
}

// ParseStreamID parses an id operand. seqDefault is the sequence to use when only a
// millisecond was given, which differs by position: a range start means "the first
// entry in that millisecond" (0) and a range end means "the last" (max).
func ParseStreamID(s string, seqDefault uint64) (StreamID, bool) {
	ms, seq, hasSeq := strings.Cut(s, "-")
	msN, err := strconv.ParseUint(ms, 10, 64)
	if err != nil {
		return StreamID{}, false
	}
	if !hasSeq {
		return StreamID{Ms: msN, Seq: seqDefault}, true
	}
	seqN, err := strconv.ParseUint(seq, 10, 64)
	if err != nil {
		return StreamID{}, false
	}
	return StreamID{Ms: msN, Seq: seqN}, true
}

// StreamEntry is one entry: its id and its field/value pairs, flattened the way the
// protocol carries them.
type StreamEntry struct {
	ID     StreamID
	Fields [][]byte
}

func (e StreamEntry) clone() StreamEntry {
	out := StreamEntry{ID: e.ID, Fields: make([][]byte, len(e.Fields))}
	for i, f := range e.Fields {
		out.Fields[i] = copyBytes(f)
	}
	return out
}

// streamNACK is one entry a consumer has been delivered and has not acknowledged: an
// entry of the group's pending-entries list (PEL).
//
// The same *streamNACK is in the group's PEL and in its consumer's, so a claim moves
// ownership by rewiring two maps and mutating one struct -- there is no second copy
// that could disagree about the delivery count.
type streamNACK struct {
	id            StreamID
	consumer      string
	deliveryMs    int64 // when it was last delivered or claimed
	deliveryCount int64
}

// streamConsumer is one named reader inside a group.
type streamConsumer struct {
	name     string
	seenMs   int64 // last time this consumer was seen at all
	activeMs int64 // last time it was actually given an entry
	pel      map[StreamID]*streamNACK
}

// streamGroup is one consumer group: how far it has delivered, what is outstanding,
// and who its consumers are.
type streamGroup struct {
	lastDelivered StreamID
	// entriesRead counts entries this group has read over its lifetime, and
	// hasEntriesRead says whether that count is *known*. The distinction is not
	// pedantry: a group created at "0" has read nothing, but so has a group created at
	// "$", and the two are not the same situation -- the first is five entries behind and
	// the second is caught up. Redis reports a **null** entries-read until the count is
	// established, and reporting 0 for both (as this did) tells a client the second group
	// has fallen behind when it is up to date.
	//
	// It becomes known when the group actually reads, or when a caller supplies
	// ENTRIESREAD, and it goes back to unknown on an XGROUP SETID that does not supply one
	// -- because moving the position invalidates any count derived from the old one.
	entriesRead    int64
	hasEntriesRead bool
	pel            map[StreamID]*streamNACK
	consumers      map[string]*streamConsumer
}

func newStreamGroup(start StreamID, entriesRead int64, hasEntriesRead bool) *streamGroup {
	return &streamGroup{
		lastDelivered:  start,
		entriesRead:    entriesRead,
		hasEntriesRead: hasEntriesRead,
		pel:            make(map[StreamID]*streamNACK),
		consumers:      make(map[string]*streamConsumer),
	}
}

// consumerFor returns the named consumer, creating it on first use, and records that
// it was seen. created reports whether it was new, which is what XGROUP
// CREATECONSUMER reports.
func (g *streamGroup) consumerFor(name string, nowMs int64) (c *streamConsumer, created bool) {
	c = g.consumers[name]
	if c == nil {
		c = &streamConsumer{name: name, seenMs: nowMs, activeMs: nowMs, pel: make(map[StreamID]*streamNACK)}
		g.consumers[name] = c
		created = true
	}
	c.seenMs = nowMs
	return c, created
}

// stream is the value stored under a stream key.
type stream struct {
	entries []StreamEntry // sorted ascending by id, always
	last    StreamID      // the largest id ever added, whether or not it is still here
	// maxDeleted is the largest id XDEL or a trim has removed. It is reported by XINFO
	// and preserved by a snapshot, because a consumer that tracks its position by id has
	// to be able to tell "not yet written" from "written and gone".
	maxDeleted StreamID
	added      uint64 // entries added over the stream's lifetime, never decremented
	groups     map[string]*streamGroup
}

func newStream() *stream { return &stream{} }

// groupLag reports how many entries a group has still to read, and whether that number is
// knowable at all.
//
// Every branch below was derived from measuring redis:7.2 across twenty-two scenarios
// (creation at 0/$/mid, with and without reads, with ENTRIESREAD, after XDEL of the head
// and of the middle, after MAXLEN and MINID trims, after XSETID, after XGROUP SETID) and
// then checked against all of them; both the amd64 and arm64 references agreed throughout.
// It is written as an ordered cascade because the order is load-bearing -- see the third
// case, which must be tried *before* the arithmetic one.
//
// What this replaced was two lines: "added minus read, and null once anything has been
// deleted". Those were wrong in both directions. A group created at "$" is caught up, and
// reporting `added - 0` told an operator it was the whole stream behind; meanwhile a
// trimmed stream reported an unknown lag where the answer is exact.
func (st *stream) groupLag(g *streamGroup) (int64, bool) {
	// 1. Nothing left to read, whatever the history. Covers a stream that was trimmed to
	//    nothing or had every entry deleted, where Redis reports 0 rather than null.
	if len(st.entries) == 0 {
		return 0, true
	}
	// 2. The group is at or past the last id ever written, so it is caught up. This is the
	//    case a plain "added - read" got wrong for a group created at "$".
	if g.lastDelivered.Compare(st.last) >= 0 {
		return 0, true
	}
	first := st.entries[0].ID
	// 3. The group sits before the first entry that is still here, and the stream has no
	//    hole *after* that point -- either nothing was deleted, or everything deleted was
	//    older than what remains (which is what a prefix trim leaves behind). Then the
	//    group has the whole remaining stream to read, and the count of it is exact even
	//    though the read counter is not.
	//
	//    This is tried before case 4 deliberately: with `ENTRIESREAD 2` on a five-entry
	//    stream and a group still at 0-0, Redis answers 5, not 3. The position is harder
	//    evidence than a counter a caller asserted.
	if (st.maxDeleted == StreamID{} || st.maxDeleted.Compare(first) < 0) &&
		g.lastDelivered.Compare(first) < 0 {
		return int64(len(st.entries)), true
	}
	// 4. The read counter is known and no deleted id sits at or after the group's
	//    position, so nothing it has yet to read has gone missing and the arithmetic holds.
	if g.hasEntriesRead && (st.maxDeleted == StreamID{} || st.maxDeleted.Compare(g.lastDelivered) < 0) {
		if lag := int64(st.added) - g.entriesRead; lag >= 0 {
			return lag, true
		}
	}
	// 5. Otherwise the group is somewhere inside a stream with a hole ahead of it, and
	//    there is no cheap way to count what it has left. Redis will sometimes estimate
	//    here; this reports null instead, which is the honest answer and is documented as
	//    a deliberate difference. A wrong lag is worse than an absent one -- it is the
	//    number an operator pages on.
	return 0, false
}

// clone deep-copies the stream, for COPY. The consumer groups come too, including
// their pending-entries lists: a copied stream that lost its groups would silently
// hand a consumer a keyspace it had already read as though it were new.
func (st *stream) clone() *stream {
	out := &stream{last: st.last, maxDeleted: st.maxDeleted, added: st.added}
	out.entries = make([]StreamEntry, 0, len(st.entries))
	for _, e := range st.entries {
		out.entries = append(out.entries, e.clone())
	}
	if len(st.groups) == 0 {
		return out
	}
	out.groups = make(map[string]*streamGroup, len(st.groups))
	for name, g := range st.groups {
		ng := newStreamGroup(g.lastDelivered, g.entriesRead, g.hasEntriesRead)
		for cname, c := range g.consumers {
			ng.consumers[cname] = &streamConsumer{
				name: cname, seenMs: c.seenMs, activeMs: c.activeMs,
				pel: make(map[StreamID]*streamNACK, len(c.pel)),
			}
		}
		// One NACK object per entry, shared between the group's PEL and its consumer's, as
		// in the original: two copies could drift apart on the next claim.
		for id, nack := range g.pel {
			copied := &streamNACK{
				id: id, consumer: nack.consumer,
				deliveryMs: nack.deliveryMs, deliveryCount: nack.deliveryCount,
			}
			ng.pel[id] = copied
			if c := ng.consumers[nack.consumer]; c != nil {
				c.pel[id] = copied
			}
		}
		out.groups[name] = ng
	}
	return out
}

// nextID generates the id an XADD with "*" gets. See the monotonicity note at the top
// of this file.
func (st *stream) nextID(nowMs int64) (StreamID, error) {
	ms := uint64(0)
	if nowMs > 0 {
		ms = uint64(nowMs)
	}
	if ms > st.last.Ms {
		return StreamID{Ms: ms}, nil
	}
	// The clock has not moved past the last id, so the id is the one after it. A sequence
	// counter at its maximum carries into the *millisecond*: only an id that is at the
	// maximum in both halves has nowhere left to go.
	//
	// That distinction matters more than it looks. An explicit XADD may name an id whose
	// millisecond is far in the future -- Redis's own test writes
	// `2577343934890-18446744073709551615`, a timestamp in the year 2051 -- and treating a
	// full sequence counter as exhaustion refuses every later XADD on that stream, for
	// decades, over a counter that had a whole millisecond of room above it.
	if st.last.Seq == math.MaxUint64 {
		if st.last.Ms == math.MaxUint64 {
			return StreamID{}, ErrStreamIDExhausted
		}
		return StreamID{Ms: st.last.Ms + 1}, nil
	}
	return StreamID{Ms: st.last.Ms, Seq: st.last.Seq + 1}, nil
}

// seqForMs generates the id an XADD with "<ms>-*" gets: the caller fixed the
// millisecond and the sequence continues from whatever is already there.
func (st *stream) seqForMs(ms uint64) (StreamID, error) {
	// Unlike nextID this one cannot carry into the next millisecond: the caller *fixed* the
	// millisecond, so a full sequence counter inside it really has nowhere to go -- and the
	// refusal names that, rather than claiming the whole stream is exhausted. It is not: an
	// XADD naming the next millisecond, or a plain "*", still works. Redis words it the same
	// way ("equal or smaller than the target stream top item"), because from the caller's
	// point of view the id it asked for is one the stream already has.
	switch {
	case ms > st.last.Ms:
		return StreamID{Ms: ms}, nil
	case ms < st.last.Ms:
		return StreamID{}, ErrStreamIDSmaller
	case st.last.Seq == math.MaxUint64:
		return StreamID{}, ErrStreamIDSmaller
	}
	return StreamID{Ms: ms, Seq: st.last.Seq + 1}, nil
}

// firstIndexAtOrAfter is the binary search every range and read starts from.
func (st *stream) firstIndexAtOrAfter(id StreamID) int {
	return sort.Search(len(st.entries), func(i int) bool {
		return st.entries[i].ID.Compare(id) >= 0
	})
}

// find returns the index of the entry with exactly this id, or -1.
func (st *stream) find(id StreamID) int {
	i := st.firstIndexAtOrAfter(id)
	if i < len(st.entries) && st.entries[i].ID == id {
		return i
	}
	return -1
}

// firstID reports the smallest id still present.
func (st *stream) firstID() StreamID {
	if len(st.entries) == 0 {
		return StreamID{}
	}
	return st.entries[0].ID
}

// memorySize estimates the stream's footprint, for MEMORY USAGE.
func (st *stream) memorySize() int64 {
	var n int64
	for _, e := range st.entries {
		n += 48 // the StreamEntry header: the id pair and the slice header
		for _, f := range e.Fields {
			n += 24 + int64(cap(f))
		}
	}
	for name, g := range st.groups {
		n += memMapSlot + int64(len(name)) + 96
		n += int64(len(g.pel)) * (memMapSlot + 48)
		for cname := range g.consumers {
			n += memMapSlot + int64(len(cname)) + 64
		}
	}
	return n
}

// --- trimming -----------------------------------------------------------------

// Trim strategies, matching XADD/XTRIM's MAXLEN and MINID.
const (
	TrimNone = iota
	TrimMaxLen
	TrimMinID
)

// TrimOptions is the parsed trim clause of XADD and XTRIM. Approx and Limit are
// accepted and documented as having no effect here -- see the file comment.
type TrimOptions struct {
	Strategy int
	MaxLen   int64
	MinID    StreamID
	Approx   bool
	Limit    int64
}

// trim removes entries according to o and returns how many went. Both strategies
// remove a prefix, which is the only shape a stream trim can take: entries are ordered
// by id, so "the oldest n" and "everything below an id" are both prefixes.
func (st *stream) trim(o TrimOptions) int64 {
	var cut int
	switch o.Strategy {
	case TrimMaxLen:
		if o.MaxLen < 0 || int64(len(st.entries)) <= o.MaxLen {
			return 0
		}
		cut = len(st.entries) - int(o.MaxLen)
	case TrimMinID:
		cut = st.firstIndexAtOrAfter(o.MinID)
	default:
		return 0
	}
	if cut <= 0 {
		return 0
	}
	// A trim deliberately does *not* touch maxDeleted, which is what Redis does and was
	// measured: after `XTRIM s MAXLEN 2` on a five-entry stream, real Redis still reports
	// `max-deleted-entry-id 0-0` while this used to report `3-1`.
	//
	// The distinction is meaningful rather than cosmetic. `max-deleted-entry-id` records
	// ids that were *explicitly* removed, which is how a consumer tracking its position by
	// id tells "never written" from "written and gone" -- and it is also what tells the lag
	// calculation that the stream has a hole in it. Trimming leaves no hole: it removes a
	// prefix, so what remains is still contiguous, and `recorded-first-entry-id` already
	// says where it now starts. Recording a trim here made every trimmed stream look
	// fragmented, which is why a trimmed group's lag came back as "unknown" on a stream
	// Redis reports an exact lag for.
	st.entries = slices.Delete(st.entries, 0, cut)
	return int64(cut)
}

// --- XADD ---------------------------------------------------------------------

// XAddOptions is the parsed option set of XADD.
type XAddOptions struct {
	// ID is the explicit id to use. Auto means "*": generate one from the clock.
	// AutoSeq means "<ms>-*": the millisecond is given and the sequence generated.
	ID      StreamID
	Auto    bool
	AutoSeq bool
	// NoMkStream refuses to create the key, as XADD ... NOMKSTREAM does.
	NoMkStream bool
	Trim       TrimOptions
}

// XAdd appends an entry and returns the id it was given plus how many entries the
// trim clause removed. created is false when NOMKSTREAM found no key, in which case
// nothing was written.
//
// The returned id is the whole reason this signature exists: with "*" the id comes
// from the clock, so it is not reproducible, and the server has to propagate the
// concrete id rather than the command. See the effect propagation note in
// commands_stream.go.
func (s *Store) XAdd(key string, o XAddOptions, fields [][]byte) (id StreamID, trimmed int64, created bool, err error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e := sh.liveEntry(key, now)
	if e != nil && e.kind != kindStream {
		return StreamID{}, 0, false, ErrWrongType
	}
	if e == nil {
		if o.NoMkStream {
			return StreamID{}, 0, false, nil
		}
		e = &entry{kind: kindStream, stream: newStream()}
		sh.data[key] = e
	}
	st := e.stream

	switch {
	case o.Auto:
		id, err = st.nextID(now.UnixMilli())
	case o.AutoSeq:
		id, err = st.seqForMs(o.ID.Ms)
	default:
		id = o.ID
		// Strictly greater than the top item, and never the zero id, which is reserved:
		// Redis refuses 0-0 outright because it is the id a range's "-" resolves to.
		if id.Ms == 0 && id.Seq == 0 {
			err = ErrStreamIDSmaller
		} else if len(st.entries) > 0 || st.added > 0 || st.last != (StreamID{}) {
			if id.Compare(st.last) <= 0 {
				err = ErrStreamIDSmaller
			}
		}
	}
	if err != nil {
		// A stream created by this very call and then not written to must not be left
		// behind: XADD either adds an entry or changes nothing.
		if len(st.entries) == 0 && st.added == 0 && len(st.groups) == 0 {
			delete(sh.data, key)
		}
		return StreamID{}, 0, false, err
	}

	entryCopy := StreamEntry{ID: id, Fields: make([][]byte, len(fields))}
	for i, f := range fields {
		entryCopy.Fields[i] = copyBytes(f)
	}
	st.entries = append(st.entries, entryCopy)
	st.last = id
	st.added++
	trimmed = st.trim(o.Trim)
	s.touch(e, now)
	return id, trimmed, true, nil
}

// --- reads --------------------------------------------------------------------

// StreamRange is a resolved XRANGE/XREVRANGE range: two inclusive bounds, after the
// exclusive forms have been folded into them by the caller.
type StreamRange struct {
	Start, End StreamID
}

// XLen reports how many entries the stream holds (0 for a missing key).
func (s *Store) XLen(key string) (int64, error) {
	st, _, err := s.readStream(key)
	if err != nil || st == nil {
		return 0, err
	}
	return int64(len(st.entries)), nil
}

// readStream resolves a stream for reading under the shard's read lock and returns it
// with the lock released. The entries slice is never mutated in place -- a trim
// re-slices and an append may reallocate -- so the caller must copy anything it means
// to keep, which is what the range helpers do.
//
// It is a helper rather than a method on shard because every stream read needs the
// same three-line preamble and the same "a missing key is not an error" rule.
func (s *Store) readStream(key string) (*stream, time.Time, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return nil, now, nil
	}
	if e.kind != kindStream {
		return nil, now, ErrWrongType
	}
	return e.stream, now, nil
}

// XRange returns the entries in the inclusive id range, oldest first (or newest first
// when rev is set). count <= 0 means every match.
func (s *Store) XRange(key string, r StreamRange, count int, rev bool) ([]StreamEntry, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return nil, nil
	}
	if e.kind != kindStream {
		return nil, ErrWrongType
	}
	st := e.stream
	if r.Start.Compare(r.End) > 0 {
		return nil, nil
	}
	lo := st.firstIndexAtOrAfter(r.Start)
	hi := st.firstIndexAtOrAfter(r.End.Next()) // one past the last match
	if lo >= hi {
		return nil, nil
	}
	out := make([]StreamEntry, 0, min(hi-lo, maxStreamReplyLen(count, hi-lo)))
	if rev {
		for i := hi - 1; i >= lo; i-- {
			if count > 0 && len(out) == count {
				break
			}
			out = append(out, st.entries[i].clone())
		}
		return out, nil
	}
	for i := lo; i < hi; i++ {
		if count > 0 && len(out) == count {
			break
		}
		out = append(out, st.entries[i].clone())
	}
	return out, nil
}

func maxStreamReplyLen(count, available int) int {
	if count > 0 {
		return min(count, available)
	}
	return available
}

// XReadAfter returns up to count entries with ids strictly greater than after, which
// is what XREAD asks for. A missing key yields nothing and no error.
func (s *Store) XReadAfter(key string, after StreamID, count int) ([]StreamEntry, error) {
	return s.XRange(key, StreamRange{Start: after.Next(), End: maxStreamID}, count, false)
}

// XLastID reports the largest id ever added to the stream, which is what "$" resolves
// to. ok is false for a missing key, whose "$" is the zero id.
func (s *Store) XLastID(key string) (StreamID, bool, error) {
	st, _, err := s.readStream(key)
	if err != nil || st == nil {
		return StreamID{}, false, err
	}
	return st.last, true, nil
}

// --- XDEL / XTRIM / XSETID ----------------------------------------------------

// XDel removes the named entries and reports how many existed.
//
// The pending-entries lists are deliberately left alone, as in Redis: an entry that a
// consumer was delivered and has not acknowledged stays in its PEL after an XDEL, so
// XACK still works and XPENDING still reports the outstanding delivery. Silently
// acknowledging it would turn a deletion into a claim that the work was done.
func (s *Store) XDel(key string, ids []StreamID) (int64, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e := sh.liveEntry(key, now)
	if e == nil {
		return 0, nil
	}
	if e.kind != kindStream {
		return 0, ErrWrongType
	}
	st := e.stream
	var n int64
	for _, id := range ids {
		i := st.find(id)
		if i < 0 {
			continue
		}
		if id.Compare(st.maxDeleted) > 0 {
			st.maxDeleted = id
		}
		st.entries = slices.Delete(st.entries, i, i+1)
		n++
	}
	if n > 0 {
		s.touch(e, now)
	}
	return n, nil
}

// XTrim applies a trim clause on its own and reports how many entries went.
func (s *Store) XTrim(key string, o TrimOptions) (int64, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e := sh.liveEntry(key, now)
	if e == nil {
		return 0, nil
	}
	if e.kind != kindStream {
		return 0, ErrWrongType
	}
	n := e.stream.trim(o)
	if n > 0 {
		s.touch(e, now)
	}
	return n, nil
}

// XSetIDOptions is the option tail of XSETID.
type XSetIDOptions struct {
	HasEntriesAdded bool
	EntriesAdded    int64
	HasMaxDeleted   bool
	MaxDeleted      StreamID
}

// XSetID sets the stream's last id, and optionally its lifetime counters. It exists
// for two reasons: clients use it to rewind a stream, and a snapshot needs it to
// restore state that the entries alone do not carry -- a stream whose newest entries
// were all deleted still remembers the id it reached, and forgetting that would let a
// replayed stream re-issue ids a consumer has already seen.
//
// Refusing an id smaller than the largest entry present is Redis's rule, and the
// reason is the same: an id that sorts before an existing entry would make the next
// append violate the ordering the whole type rests on.
func (s *Store) XSetID(key string, id StreamID, o XSetIDOptions) error {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e := sh.liveEntry(key, now)
	if e == nil {
		return ErrNoSuchStreamKey
	}
	if e.kind != kindStream {
		return ErrWrongType
	}
	st := e.stream
	if n := len(st.entries); n > 0 && st.entries[n-1].ID.Compare(id) > 0 {
		return ErrStreamSetIDSmaller
	}
	st.last = id
	if o.HasEntriesAdded {
		st.added = uint64(max(o.EntriesAdded, 0))
	}
	if o.HasMaxDeleted {
		st.maxDeleted = o.MaxDeleted
	}
	s.touch(e, now)
	return nil
}
