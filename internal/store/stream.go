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
	entriesRead   int64
	pel           map[StreamID]*streamNACK
	consumers     map[string]*streamConsumer
}

func newStreamGroup(start StreamID, entriesRead int64) *streamGroup {
	return &streamGroup{
		lastDelivered: start,
		entriesRead:   entriesRead,
		pel:           make(map[StreamID]*streamNACK),
		consumers:     make(map[string]*streamConsumer),
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
		ng := newStreamGroup(g.lastDelivered, g.entriesRead)
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
	// The largest id being removed becomes the stream's max-deleted marker, so a
	// reader can still tell a gap from an id that was never written.
	if last := st.entries[cut-1].ID; last.Compare(st.maxDeleted) > 0 {
		st.maxDeleted = last
	}
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

// --- consumer groups ----------------------------------------------------------

// streamForGroupWrite resolves a stream for a group-scoped write. mkstream creates the
// key when it is missing, which only XGROUP CREATE ... MKSTREAM asks for.
func (s *Store) streamForGroupWrite(sh *shard, key string, now time.Time, mkstream bool) (*entry, error) {
	e := sh.liveEntry(key, now)
	if e != nil && e.kind != kindStream {
		return nil, ErrWrongType
	}
	if e == nil {
		if !mkstream {
			return nil, ErrNoSuchStreamKey
		}
		e = &entry{kind: kindStream, stream: newStream()}
		sh.data[key] = e
	}
	if e.stream.groups == nil {
		e.stream.groups = make(map[string]*streamGroup)
	}
	return e, nil
}

// XGroupCreate creates a consumer group starting from the given id. entriesRead seeds
// the group's read counter, which a snapshot restores and a client leaves at zero.
func (s *Store) XGroupCreate(key, group string, start StreamID, mkstream bool, entriesRead int64) error {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, err := s.streamForGroupWrite(sh, key, now, mkstream)
	if err != nil {
		return err
	}
	if _, exists := e.stream.groups[group]; exists {
		return ErrBusyGroup
	}
	e.stream.groups[group] = newStreamGroup(start, entriesRead)
	s.touch(e, now)
	return nil
}

// XGroupSetID moves a group's last-delivered id. entriesRead is applied only when
// hasEntriesRead is set, so a client's XGROUP SETID leaves the counter alone.
func (s *Store) XGroupSetID(key, group string, id StreamID, hasEntriesRead bool, entriesRead int64) error {
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
	g := e.stream.groups[group]
	if g == nil {
		return ErrNoGroup
	}
	g.lastDelivered = id
	if hasEntriesRead {
		g.entriesRead = entriesRead
	}
	s.touch(e, now)
	return nil
}

// XGroupDestroy removes a group and everything it tracked, reporting whether it
// existed.
func (s *Store) XGroupDestroy(key, group string) (bool, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e := sh.liveEntry(key, now)
	if e == nil {
		return false, ErrNoSuchStreamKey
	}
	if e.kind != kindStream {
		return false, ErrWrongType
	}
	if _, ok := e.stream.groups[group]; !ok {
		return false, nil
	}
	delete(e.stream.groups, group)
	s.touch(e, now)
	return true, nil
}

// XGroupCreateConsumer adds a consumer to a group, reporting whether it was new.
func (s *Store) XGroupCreateConsumer(key, group, consumer string) (bool, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e := sh.liveEntry(key, now)
	if e == nil {
		return false, ErrNoSuchStreamKey
	}
	if e.kind != kindStream {
		return false, ErrWrongType
	}
	g := e.stream.groups[group]
	if g == nil {
		return false, ErrNoGroup
	}
	_, created := g.consumerFor(consumer, now.UnixMilli())
	if created {
		s.touch(e, now)
	}
	return created, nil
}

// XGroupDelConsumer removes a consumer and returns how many pending entries it still
// held -- which is the number that is now unreachable by an XACK from that consumer,
// and therefore the number an operator needs to see before doing it.
func (s *Store) XGroupDelConsumer(key, group, consumer string) (int64, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e := sh.liveEntry(key, now)
	if e == nil {
		return 0, ErrNoSuchStreamKey
	}
	if e.kind != kindStream {
		return 0, ErrWrongType
	}
	g := e.stream.groups[group]
	if g == nil {
		return 0, ErrNoGroup
	}
	c := g.consumers[consumer]
	if c == nil {
		return 0, nil
	}
	n := int64(len(c.pel))
	for id := range c.pel {
		delete(g.pel, id)
	}
	delete(g.consumers, consumer)
	s.touch(e, now)
	return n, nil
}

// XReadGroupResult is what one XREADGROUP delivery produced: the entries served and,
// for each, the delivery metadata the effect propagation needs.
type XReadGroupResult struct {
	Entries []StreamEntry
	// Delivery parallels Entries: the delivery time and count recorded for each, which
	// the server ships in the XCLAIM it propagates so a replica's PEL matches exactly.
	Delivery []StreamDelivery
	// LastDelivered is the group's id after the read, propagated for a NOACK read where
	// there are no PEL entries to carry it.
	LastDelivered StreamID
	EntriesRead   int64
	// ConsumerCreated reports that this read named a consumer the group did not have.
	// Redis fires an xgroup-createconsumer keyspace event for exactly that, so the caller
	// has to be told; nothing else can observe it, because the consumer is created
	// implicitly and the reply says nothing about it.
	ConsumerCreated bool
}

// StreamDelivery is one PEL entry's metadata.
type StreamDelivery struct {
	Consumer      string
	DeliveryMs    int64
	DeliveryCount int64
}

// XReadGroup serves a consumer group read.
//
// newOnly selects the ">" form: entries the group has never delivered, which advances
// the group's last-delivered id and (unless noack) records each entry in the
// consumer's PEL. Otherwise it is the history form, which re-reads that one consumer's
// already-delivered entries from after the given id, changing nothing.
func (s *Store) XReadGroup(key, group, consumer string, newOnly bool, after StreamID,
	count int, noack bool) (XReadGroupResult, error) {

	var res XReadGroupResult
	sh := s.getShard(key)
	now := s.clock()
	nowMs := now.UnixMilli()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e := sh.liveEntry(key, now)
	if e == nil {
		return res, ErrNoSuchStreamKey
	}
	if e.kind != kindStream {
		return res, ErrWrongType
	}
	st := e.stream
	g := st.groups[group]
	if g == nil {
		return res, ErrNoGroup
	}
	c, created := g.consumerFor(consumer, nowMs)
	res.ConsumerCreated = created
	res.LastDelivered = g.lastDelivered
	res.EntriesRead = g.entriesRead

	if !newOnly {
		// The history form: this consumer's own outstanding entries, in id order. An id in
		// the PEL whose entry has since been deleted is reported as a null entry, which is
		// how Redis tells a consumer "you still owe an ack for something that is gone".
		ids := make([]StreamID, 0, len(c.pel))
		for id := range c.pel {
			if id.Compare(after) > 0 {
				ids = append(ids, id)
			}
		}
		slices.SortFunc(ids, func(a, b StreamID) int { return a.Compare(b) })
		for _, id := range ids {
			if count > 0 && len(res.Entries) == count {
				break
			}
			nack := c.pel[id]
			out := StreamEntry{ID: id}
			if i := st.find(id); i >= 0 {
				out = st.entries[i].clone()
			}
			res.Entries = append(res.Entries, out)
			res.Delivery = append(res.Delivery, StreamDelivery{
				Consumer: consumer, DeliveryMs: nack.deliveryMs, DeliveryCount: nack.deliveryCount,
			})
		}
		return res, nil
	}

	start := st.firstIndexAtOrAfter(g.lastDelivered.Next())
	for i := start; i < len(st.entries); i++ {
		if count > 0 && len(res.Entries) == count {
			break
		}
		ent := st.entries[i]
		g.lastDelivered = ent.ID
		g.entriesRead++
		res.Entries = append(res.Entries, ent.clone())
		c.activeMs = nowMs
		if noack {
			res.Delivery = append(res.Delivery, StreamDelivery{Consumer: consumer})
			continue
		}
		nack := &streamNACK{id: ent.ID, consumer: consumer, deliveryMs: nowMs, deliveryCount: 1}
		g.pel[ent.ID] = nack
		c.pel[ent.ID] = nack
		res.Delivery = append(res.Delivery, StreamDelivery{
			Consumer: consumer, DeliveryMs: nowMs, DeliveryCount: 1,
		})
	}
	res.LastDelivered = g.lastDelivered
	res.EntriesRead = g.entriesRead
	if len(res.Entries) > 0 {
		s.touch(e, now)
	}
	return res, nil
}

// XAck removes entries from a group's PEL and reports how many were there.
func (s *Store) XAck(key, group string, ids []StreamID) (int64, error) {
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
	g := e.stream.groups[group]
	if g == nil {
		return 0, nil // as in Redis: acknowledging into a group that is gone is not an error
	}
	var n int64
	for _, id := range ids {
		nack := g.pel[id]
		if nack == nil {
			continue
		}
		delete(g.pel, id)
		if c := g.consumers[nack.consumer]; c != nil {
			delete(c.pel, id)
		}
		n++
	}
	if n > 0 {
		s.touch(e, now)
	}
	return n, nil
}

// StreamPending is one row of XPENDING's extended form.
type StreamPending struct {
	ID            StreamID
	Consumer      string
	ElapsedMs     int64 // how long since it was last delivered
	DeliveryCount int64
}

// StreamPendingSummary is XPENDING's summary form: the count, the id range, and the
// per-consumer breakdown.
type StreamPendingSummary struct {
	Count      int64
	Min, Max   StreamID
	Consumers  []StreamConsumerCount
	StreamGone bool
}

// StreamConsumerCount is one consumer's share of a group's PEL.
type StreamConsumerCount struct {
	Name  string
	Count int64
}

// XPendingSummary answers XPENDING key group.
func (s *Store) XPendingSummary(key, group string) (StreamPendingSummary, error) {
	var out StreamPendingSummary
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return out, ErrNoGroup
	}
	if e.kind != kindStream {
		return out, ErrWrongType
	}
	g := e.stream.groups[group]
	if g == nil {
		return out, ErrNoGroup
	}
	out.Count = int64(len(g.pel))
	if out.Count == 0 {
		return out, nil
	}
	first := true
	for id := range g.pel {
		if first || id.Compare(out.Min) < 0 {
			out.Min = id
		}
		if first || id.Compare(out.Max) > 0 {
			out.Max = id
		}
		first = false
	}
	for name, c := range g.consumers {
		if len(c.pel) == 0 {
			continue
		}
		out.Consumers = append(out.Consumers, StreamConsumerCount{Name: name, Count: int64(len(c.pel))})
	}
	slices.SortFunc(out.Consumers, func(a, b StreamConsumerCount) int {
		return strings.Compare(a.Name, b.Name)
	})
	return out, nil
}

// XPendingRange answers XPENDING's extended form: the outstanding entries in an id
// range, optionally filtered to one consumer and to those idle for at least minIdleMs.
func (s *Store) XPendingRange(key, group string, r StreamRange, count int,
	consumer string, minIdleMs int64) ([]StreamPending, error) {

	sh := s.getShard(key)
	now := s.clock()
	nowMs := now.UnixMilli()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return nil, ErrNoGroup
	}
	if e.kind != kindStream {
		return nil, ErrWrongType
	}
	g := e.stream.groups[group]
	if g == nil {
		return nil, ErrNoGroup
	}
	pel := g.pel
	if consumer != "" {
		c := g.consumers[consumer]
		if c == nil {
			return nil, nil
		}
		pel = c.pel
	}
	ids := make([]StreamID, 0, len(pel))
	for id := range pel {
		if id.Compare(r.Start) < 0 || id.Compare(r.End) > 0 {
			continue
		}
		if nowMs-pel[id].deliveryMs < minIdleMs {
			continue
		}
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, b StreamID) int { return a.Compare(b) })
	out := make([]StreamPending, 0, maxStreamReplyLen(count, len(ids)))
	for _, id := range ids {
		if count > 0 && len(out) == count {
			break
		}
		nack := pel[id]
		out = append(out, StreamPending{
			ID: id, Consumer: nack.consumer,
			ElapsedMs: nowMs - nack.deliveryMs, DeliveryCount: nack.deliveryCount,
		})
	}
	return out, nil
}

// XClaimOptions is the parsed option set of XCLAIM.
type XClaimOptions struct {
	MinIdleMs int64
	// SetIdleMs / SetTimeMs set the new delivery time, relative or absolute. TIME is
	// what a replica receives, because an absolute instant replays identically however
	// much later it is applied -- the same reason every TTL on this server's wire is
	// absolute.
	HasIdle       bool
	IdleMs        int64
	HasTime       bool
	TimeMs        int64
	HasRetryCount bool
	RetryCount    int64
	Force         bool
	JustID        bool
	// LastID advances the group's last-delivered id, which is what a propagated XCLAIM
	// carries so a replica's group state matches the master's.
	HasLastID bool
	LastID    StreamID
}

// XClaimResult is one claimed entry: the entry itself (absent for JUSTID) plus the
// delivery metadata that was recorded, which the server propagates verbatim.
type XClaimResult struct {
	Entry         StreamEntry
	Present       bool // the entry still exists in the stream
	DeliveryMs    int64
	DeliveryCount int64
}

// XClaim transfers ownership of pending entries to a consumer.
//
// The claim is time-based -- it acts on entries idle for at least MinIdleMs, and it
// stamps a new delivery time -- so it is not reproducible from its own arguments. The
// server therefore propagates the concrete outcome (see commands_stream.go), which is
// why every decision this function makes is reported back in the result.
func (s *Store) XClaim(key, group, consumer string, ids []StreamID, o XClaimOptions) (
	claimed []XClaimResult, deleted []StreamID, consumerCreated bool, err error) {

	sh := s.getShard(key)
	now := s.clock()
	nowMs := now.UnixMilli()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e := sh.liveEntry(key, now)
	if e == nil {
		return nil, nil, false, ErrNoGroup
	}
	if e.kind != kindStream {
		return nil, nil, false, ErrWrongType
	}
	st := e.stream
	g := st.groups[group]
	if g == nil {
		return nil, nil, false, ErrNoGroup
	}
	c, consumerCreated := g.consumerFor(consumer, nowMs)

	for _, id := range ids {
		nack := g.pel[id]
		if nack == nil {
			if !o.Force {
				continue
			}
			// FORCE creates the PEL entry for an id that is in the stream but not pending.
			// An id that is not in the stream at all cannot be claimed: there is nothing to
			// hand over, and Redis skips it for the same reason.
			if st.find(id) < 0 {
				continue
			}
			nack = &streamNACK{id: id, consumer: consumer, deliveryMs: nowMs}
			g.pel[id] = nack
		}
		if nowMs-nack.deliveryMs < o.MinIdleMs {
			continue
		}
		// An id whose entry has been deleted is dropped from the PEL rather than claimed:
		// handing a consumer an entry that no longer exists would leave it holding an
		// acknowledgement it can never complete against any data.
		idx := st.find(id)
		if idx < 0 {
			delete(g.pel, id)
			if prev := g.consumers[nack.consumer]; prev != nil {
				delete(prev.pel, id)
			}
			deleted = append(deleted, id)
			continue
		}
		if prev := g.consumers[nack.consumer]; prev != nil && prev != c {
			delete(prev.pel, id)
		}
		nack.consumer = consumer
		switch {
		case o.HasTime:
			nack.deliveryMs = o.TimeMs
		case o.HasIdle:
			nack.deliveryMs = nowMs - o.IdleMs
		default:
			nack.deliveryMs = nowMs
		}
		switch {
		case o.HasRetryCount:
			nack.deliveryCount = o.RetryCount
		case !o.JustID:
			// A real re-delivery increments the count; JUSTID does not, because it hands over
			// ownership without delivering the data again. That is Redis's rule and the reason
			// a retry-count-based dead-letter policy still works after a JUSTID claim.
			nack.deliveryCount++
		}
		c.pel[id] = nack
		c.activeMs = nowMs
		claimed = append(claimed, XClaimResult{
			Entry: st.entries[idx].clone(), Present: true,
			DeliveryMs: nack.deliveryMs, DeliveryCount: nack.deliveryCount,
		})
	}
	if o.HasLastID && o.LastID.Compare(g.lastDelivered) > 0 {
		g.lastDelivered = o.LastID
	}
	if len(claimed) > 0 || len(deleted) > 0 || consumerCreated {
		s.touch(e, now)
	}
	return claimed, deleted, consumerCreated, nil
}

// XAutoClaim scans a group's PEL from start for entries idle at least minIdleMs and
// claims up to count of them for the consumer, returning the cursor to resume from.
func (s *Store) XAutoClaim(key, group, consumer string, start StreamID, minIdleMs int64,
	count int, justID bool) (claimed []XClaimResult, deleted []StreamID, cursor StreamID,
	consumerCreated bool, err error) {

	sh := s.getShard(key)
	now := s.clock()
	nowMs := now.UnixMilli()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e := sh.liveEntry(key, now)
	if e == nil {
		return nil, nil, StreamID{}, false, ErrNoGroup
	}
	if e.kind != kindStream {
		return nil, nil, StreamID{}, false, ErrWrongType
	}
	st := e.stream
	g := st.groups[group]
	if g == nil {
		return nil, nil, StreamID{}, false, ErrNoGroup
	}
	c, consumerCreated := g.consumerFor(consumer, nowMs)

	ids := make([]StreamID, 0, len(g.pel))
	for id := range g.pel {
		if id.Compare(start) >= 0 {
			ids = append(ids, id)
		}
	}
	slices.SortFunc(ids, func(a, b StreamID) int { return a.Compare(b) })

	// The scan is bounded by attempts rather than by successes, as Redis's is: an
	// entry skipped for being too recent still costs a step, and a caller that asked for
	// 100 must not be made to walk a million-entry PEL to find them.
	attempts := count * 10
	cursor = StreamID{}
	for i, id := range ids {
		if len(claimed)+len(deleted) >= count || i >= attempts {
			cursor = id // resume here next time
			break
		}
		nack := g.pel[id]
		if nowMs-nack.deliveryMs < minIdleMs {
			continue
		}
		idx := st.find(id)
		if idx < 0 {
			delete(g.pel, id)
			if prev := g.consumers[nack.consumer]; prev != nil {
				delete(prev.pel, id)
			}
			deleted = append(deleted, id)
			continue
		}
		if prev := g.consumers[nack.consumer]; prev != nil && prev != c {
			delete(prev.pel, id)
		}
		nack.consumer = consumer
		nack.deliveryMs = nowMs
		if !justID {
			nack.deliveryCount++
		}
		c.pel[id] = nack
		c.activeMs = nowMs
		claimed = append(claimed, XClaimResult{
			Entry: st.entries[idx].clone(), Present: true,
			DeliveryMs: nack.deliveryMs, DeliveryCount: nack.deliveryCount,
		})
	}
	if len(claimed) > 0 || len(deleted) > 0 || consumerCreated {
		s.touch(e, now)
	}
	return claimed, deleted, cursor, consumerCreated, nil
}

// --- XINFO --------------------------------------------------------------------

// StreamInfo is what XINFO STREAM reports.
type StreamInfo struct {
	Length          int64
	Groups          int64
	LastID          StreamID
	MaxDeletedID    StreamID
	EntriesAdded    int64
	RecordedFirstID StreamID
	First, Last     StreamEntry
	HasEntries      bool
}

// StreamGroupInfo is one row of XINFO GROUPS.
type StreamGroupInfo struct {
	Name            string
	Consumers       int64
	Pending         int64
	LastDelivered   StreamID
	EntriesRead     int64
	Lag             int64
	HasLag          bool
	ConsumerDetails []StreamConsumerInfo
}

// StreamConsumerInfo is one row of XINFO CONSUMERS.
type StreamConsumerInfo struct {
	Name       string
	Pending    int64
	IdleMs     int64
	InactiveMs int64
}

// XInfoStream reports the stream's own state. ok is false for a missing key.
func (s *Store) XInfoStream(key string) (StreamInfo, bool, error) {
	var out StreamInfo
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return out, false, nil
	}
	if e.kind != kindStream {
		return out, false, ErrWrongType
	}
	st := e.stream
	out = StreamInfo{
		Length:          int64(len(st.entries)),
		Groups:          int64(len(st.groups)),
		LastID:          st.last,
		MaxDeletedID:    st.maxDeleted,
		EntriesAdded:    int64(st.added),
		RecordedFirstID: st.firstID(),
	}
	if len(st.entries) > 0 {
		out.First = st.entries[0].clone()
		out.Last = st.entries[len(st.entries)-1].clone()
		out.HasEntries = true
	}
	return out, true, nil
}

// XInfoGroups reports every group on the stream, sorted by name so the reply is stable.
// withConsumers fills in each group's consumer detail, which XINFO STREAM FULL wants
// and XINFO GROUPS does not.
func (s *Store) XInfoGroups(key string, withConsumers bool) ([]StreamGroupInfo, bool, error) {
	sh := s.getShard(key)
	now := s.clock()
	nowMs := now.UnixMilli()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return nil, false, nil
	}
	if e.kind != kindStream {
		return nil, false, ErrWrongType
	}
	st := e.stream
	out := make([]StreamGroupInfo, 0, len(st.groups))
	for name, g := range st.groups {
		info := StreamGroupInfo{
			Name:          name,
			Consumers:     int64(len(g.consumers)),
			Pending:       int64(len(g.pel)),
			LastDelivered: g.lastDelivered,
			EntriesRead:   g.entriesRead,
		}
		// Lag is how many entries the group has not read. It is only knowable when nothing
		// has been deleted from underneath the group: once entries have gone, "added minus
		// read" over-counts and there is no cheap way to recover the truth, so Redis reports
		// a null lag rather than a wrong number. This does the same.
		if st.maxDeleted == (StreamID{}) {
			info.Lag = int64(st.added) - g.entriesRead
			info.HasLag = info.Lag >= 0
		}
		if withConsumers {
			info.ConsumerDetails = consumerInfos(g, nowMs)
		}
		out = append(out, info)
	}
	slices.SortFunc(out, func(a, b StreamGroupInfo) int { return strings.Compare(a.Name, b.Name) })
	return out, true, nil
}

// XInfoConsumers reports one group's consumers, sorted by name.
func (s *Store) XInfoConsumers(key, group string) ([]StreamConsumerInfo, error) {
	sh := s.getShard(key)
	now := s.clock()
	nowMs := now.UnixMilli()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return nil, ErrNoGroup
	}
	if e.kind != kindStream {
		return nil, ErrWrongType
	}
	g := e.stream.groups[group]
	if g == nil {
		return nil, ErrNoGroup
	}
	return consumerInfos(g, nowMs), nil
}

func consumerInfos(g *streamGroup, nowMs int64) []StreamConsumerInfo {
	out := make([]StreamConsumerInfo, 0, len(g.consumers))
	for name, c := range g.consumers {
		out = append(out, StreamConsumerInfo{
			Name:    name,
			Pending: int64(len(c.pel)),
			// idle is time since the consumer was last seen at all; inactive is time since it
			// was last given an entry. The two differ for a consumer that is polling an empty
			// stream, which is exactly the case an operator needs to tell apart from a
			// consumer that has died.
			IdleMs:     nowMs - c.seenMs,
			InactiveMs: nowMs - c.activeMs,
		})
	}
	slices.SortFunc(out, func(a, b StreamConsumerInfo) int { return strings.Compare(a.Name, b.Name) })
	return out
}
