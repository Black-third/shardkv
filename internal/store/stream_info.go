package store

// What XINFO reports: the stream's counters, its groups, its consumers, and the full dump.
//
// Kept apart from the operations because these are pure readers, and because the subtle part
// is not the traversal but *what the numbers mean* -- see groupLag in stream.go for the lag
// cascade, and StreamGroupInfo.HasEntriesRead for why a read counter is nullable rather than
// zero. Both were derived by measuring redis:7.2 across a matrix of situations rather than
// from a specification, because a lag that is confidently wrong is worse than one reported
// as unknown.

import (
	"slices"
	"strings"
)

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
	HasEntriesRead  bool
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
			Name:           name,
			Consumers:      int64(len(g.consumers)),
			Pending:        int64(len(g.pel)),
			LastDelivered:  g.lastDelivered,
			EntriesRead:    g.entriesRead,
			HasEntriesRead: g.hasEntriesRead,
		}
		info.Lag, info.HasLag = st.groupLag(g)
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
		// A missing key is distinct from a missing group here, and the caller answers
		// them differently: XINFO CONSUMERS on an absent key is "ERR no such key" on
		// real Redis, not NOGROUP. Returning ErrNoGroup for both reported a missing
		// stream as a missing consumer group, which sends a client looking for the
		// group it just created rather than for the key that is gone.
		return nil, ErrNoSuchStreamKey
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

// --- XINFO STREAM FULL --------------------------------------------------------

// StreamFullInfo is everything XINFO STREAM ... FULL reports: the stream's counters, a
// bounded window of its entries, and every group with its consumers and both
// pending-entries lists in full.
//
// It is a separate type from StreamInfo rather than an extension of it, because the two
// forms report genuinely different field sets: FULL has no first-entry/last-entry (the
// entries themselves are there), and its "groups" is the groups rather than a count of
// them. Measured against redis:7.2, which reports nine fields here and ten there.
type StreamFullInfo struct {
	Length          int64
	EntriesAdded    int64
	LastID          StreamID
	MaxDeletedID    StreamID
	RecordedFirstID StreamID
	Entries         []StreamEntry
	Groups          []StreamFullGroupInfo
}

// StreamFullGroupInfo is one group as FULL reports it. Note it carries the pending
// entries themselves, not a count: a PEL is the record of work in flight, and the whole
// point of the FULL form is to be able to see it.
type StreamFullGroupInfo struct {
	Name           string
	LastDelivered  StreamID
	EntriesRead    int64
	HasEntriesRead bool
	Lag            int64
	HasLag         bool
	PelCount       int64
	Pending        []StreamFullPending
	Consumers      []StreamFullConsumerInfo
}

// StreamFullConsumerInfo is one consumer as FULL reports it.
//
// seen-time and active-time are absolute instants here, where XINFO CONSUMERS reports
// them as the *durations* idle and inactive. That asymmetry is Redis's, and it is the
// right way round: the summary form answers "is this consumer alive?", which is a
// duration, while the full form is a dump of state, and a dump that reported durations
// would say something different every time it ran.
type StreamFullConsumerInfo struct {
	Name     string
	SeenMs   int64
	ActiveMs int64
	PelCount int64
	Pending  []StreamFullPending
}

// StreamFullPending is one pending entry. Consumer is empty in a *consumer's* own list,
// where naming the consumer again would be redundant -- Redis omits it there, reporting
// three fields instead of four, and a client reading the reply positionally depends on
// that.
type StreamFullPending struct {
	ID            StreamID
	Consumer      string
	DeliveryMs    int64
	DeliveryCount int64
}

// XInfoStreamFull implements XINFO STREAM key FULL [COUNT n].
//
// count bounds the entries returned, because a FULL report on a large stream would
// otherwise serialise the whole thing: Redis defaults to 10 for exactly that reason and
// treats 0 as "all".
//
// It bounds **all three** lists to the same limit -- the entries, the group's
// pending-entries list, and each consumer's. That was measured rather than assumed: with a
// consumer holding five pending entries, `COUNT 2` reports two of them and `COUNT 0`
// reports all five. Bounding only the entries (as the first version of this did) left the
// PELs unbounded, which defeats the point -- a group with a million unacknowledged entries
// is exactly the situation an operator runs FULL in, and it is the PEL that is large.
//
// ok is false for a missing key.
func (s *Store) XInfoStreamFull(key string, count int) (StreamFullInfo, bool, error) {
	var out StreamFullInfo
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
	out = StreamFullInfo{
		Length:       int64(len(st.entries)),
		EntriesAdded: int64(st.added),
		LastID:       st.last,
		MaxDeletedID: st.maxDeleted,
	}
	if len(st.entries) > 0 {
		out.RecordedFirstID = st.entries[0].ID
	}

	n := len(st.entries)
	if count > 0 && count < n {
		n = count
	}
	out.Entries = make([]StreamEntry, 0, n)
	for _, ent := range st.entries[:n] {
		out.Entries = append(out.Entries, ent.clone())
	}

	out.Groups = make([]StreamFullGroupInfo, 0, len(st.groups))
	for name, g := range st.groups {
		gi := StreamFullGroupInfo{
			Name:           name,
			LastDelivered:  g.lastDelivered,
			EntriesRead:    g.entriesRead,
			HasEntriesRead: g.hasEntriesRead,
			PelCount:       int64(len(g.pel)),
			Pending:        pendingList(g.pel, true, count),
			Consumers:      make([]StreamFullConsumerInfo, 0, len(g.consumers)),
		}
		gi.Lag, gi.HasLag = st.groupLag(g)
		for cname, c := range g.consumers {
			gi.Consumers = append(gi.Consumers, StreamFullConsumerInfo{
				Name:     cname,
				SeenMs:   c.seenMs,
				ActiveMs: c.activeMs,
				PelCount: int64(len(c.pel)),
				Pending:  pendingList(c.pel, false, count),
			})
		}
		slices.SortFunc(gi.Consumers, func(a, b StreamFullConsumerInfo) int {
			return strings.Compare(a.Name, b.Name)
		})
		out.Groups = append(out.Groups, gi)
	}
	slices.SortFunc(out.Groups, func(a, b StreamFullGroupInfo) int {
		return strings.Compare(a.Name, b.Name)
	})
	return out, true, nil
}

// pendingList flattens a PEL in id order, keeping at most limit entries (0 means all).
// withConsumer distinguishes a group's list, which names the owning consumer, from a
// consumer's own, which does not.
//
// The sort is not cosmetic, and it happens *before* the limit is applied: the PEL is a map,
// so an unsorted dump would put a Go map's iteration order on the wire -- two servers
// holding identical state would answer differently, and truncating an unsorted list would
// make them disagree about *which* entries they showed. That is the same reasoning that
// makes the non-deterministic commands propagate their effects.
func pendingList(pel map[StreamID]*streamNACK, withConsumer bool, limit int) []StreamFullPending {
	out := make([]StreamFullPending, 0, len(pel))
	for id, nack := range pel {
		p := StreamFullPending{ID: id, DeliveryMs: nack.deliveryMs, DeliveryCount: nack.deliveryCount}
		if withConsumer {
			p.Consumer = nack.consumer
		}
		out = append(out, p)
	}
	slices.SortFunc(out, func(a, b StreamFullPending) int { return a.ID.Compare(b.ID) })
	if limit > 0 && limit < len(out) {
		out = out[:limit]
	}
	return out
}
