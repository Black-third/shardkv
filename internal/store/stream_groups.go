package store

// Consumer groups: the state that tracks *who has read what*, and the operations that move
// it -- XGROUP, XREADGROUP, XACK, XPENDING, XCLAIM and XAUTOCLAIM.
//
// Split from stream.go, which owns the log itself. The line is the same one the server side
// draws: nothing here adds or removes an entry, and everything here touches a group's
// position, its consumers, or a pending-entries list.
//
// # Why the PEL is the delicate part
//
// A pending-entries list is the record of work *in flight*, and the same *streamNACK is held
// by both the group's PEL and the owning consumer's. That is deliberate: a claim moves
// ownership by rewiring two maps and mutating one struct, so there is no second copy that
// could disagree about the delivery count. It is also why a snapshot has to emit every
// pending entry (invariant 5): a restore that dropped them would silently re-deliver
// acknowledged work, or silently lose outstanding work, with nothing reporting it.

import (
	"slices"
	"strings"
	"time"
)

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

// XGroupCreate creates a consumer group starting from the given id.
//
// hasEntriesRead says whether the caller supplied a read counter. A client's plain
// XGROUP CREATE does not, and the group's count is then *unknown* rather than zero --
// see streamGroup.entriesRead for why that distinction is what makes lag correct. A
// snapshot replay does supply one, which is how a restored group keeps the count it had.
func (s *Store) XGroupCreate(key, group string, start StreamID, mkstream bool, entriesRead int64, hasEntriesRead bool) error {
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
	e.stream.groups[group] = newStreamGroup(start, entriesRead, hasEntriesRead)
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
	// Without an explicit ENTRIESREAD the count is *invalidated*, not left alone: it was
	// derived from the old position, and the position has just moved. Keeping it produced a
	// concrete lie -- `XGROUP SETID s g 0` after reading a whole stream left the group
	// reporting "5 read, lag 0" while sitting at the very beginning. Measured against
	// redis:7.2, which reports a null entries-read here.
	g.entriesRead, g.hasEntriesRead = entriesRead, hasEntriesRead
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
		// Reading establishes the count as well as advancing it -- and when it was unknown
		// it is seeded from the *position*, not from zero. Measured on redis:7.2: a group
		// created at "$" on a three-entry stream, given a fourth entry, reports
		// entries-read 4 after reading it, and a group created at "2-1" that reads "3-1"
		// reports 3. Both are "the ordinal of the entry just read", which is the only
		// answer consistent with the counter meaning "entries of this stream's lifetime
		// this group has consumed". Incrementing from zero would have said 1 in both cases
		// and understated every later lag by the group's starting offset.
		//
		// The ordinal is derived rather than stored: ids are assigned in increasing order
		// and `added` counts lifetime additions, so for a contiguous stream the entry at
		// slice index i is the (added - (len-1-i))'th ever added. With deletions this is an
		// estimate, as it is in Redis.
		if g.hasEntriesRead {
			g.entriesRead++
		} else {
			g.entriesRead = int64(st.added) - int64(len(st.entries)-1-i)
			g.hasEntriesRead = true
		}
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
