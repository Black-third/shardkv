package store

import (
	"slices"
	"strconv"
)

// dumpChunkElems bounds how many collection elements a single command emitted by
// Dump carries. One command per key is not an option: a hash, set, list, or
// sorted set with enough elements produces a RESP array longer than the
// protocol's multibulk limit, and the reader on the other end (an AOF replay or a
// replica seed) rejects the whole stream rather than the one oversized command.
// Real Redis chunks its rewrite the same way.
const dumpChunkElems = 256

// Dump returns a sequence of write commands that, when replayed against an empty
// store, reconstruct the current dataset, including TTLs (emitted as a trailing
// PEXPIREAT per key). It is used to seed a replica and to compact the
// append-only file.
//
// A collection may span several commands: the first creates the key and the rest
// append to it, which is what RPUSH/HSET/SADD/ZADD already do on replay. The
// PEXPIREAT for a key always follows the last of its chunks, so the reconstructed
// key is complete before its deadline is set.
func (s *Store) Dump() [][][]byte {
	now := s.clock()
	var cmds [][][]byte

	for _, sh := range s.shards {
		sh.mu.RLock()
		for k, e := range sh.data {
			if e.expired(now) {
				continue
			}
			key := []byte(k)
			cmds = appendValueCommands(cmds, key, e)
			// Preserve the TTL as an absolute deadline so a rewrite/replica seed
			// does not silently make a volatile key permanent.
			if !e.expireAt.IsZero() {
				ms := strconv.FormatInt(e.expireAt.UnixMilli(), 10)
				cmds = append(cmds, [][]byte{[]byte("PEXPIREAT"), key, []byte(ms)})
			}
		}
		sh.mu.RUnlock()
	}
	return cmds
}

// DumpKey renders one live key as the command sequence that reconstructs it, which is
// exactly the sub-sequence Dump would emit for that key, and reports its deadline
// separately.
//
// It is what DUMP serializes and what MIGRATE ships. Sharing the rendering with Dump is
// the point: invariant 5 -- a chunk boundary never splits a field/value pair, a stream
// carries its id counters, its groups, its consumers and its pending-entries list --
// then holds for a single key's payload for free, and cannot drift away from the
// snapshot path that the AOF rewrite and the replica seed already depend on. Every one
// of the six stored kinds (and so all ten Redis types, since a bitmap and a
// HyperLogLog are strings and a geo set is a sorted set) round-trips through it.
//
// The deadline is returned rather than emitted as a trailing PEXPIREAT because RESTORE
// carries the TTL as its own operand: a payload that set its own deadline could not be
// restored under a different one, which is what RESTORE's ttl and ABSTTL are for.
//
// ok is false for a missing or expired key, which is DUMP's null reply.
func (s *Store) DumpKey(key string) (cmds [][][]byte, expireAtMs int64, ok bool) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return nil, 0, false
	}
	if !e.expireAt.IsZero() {
		expireAtMs = e.expireAt.UnixMilli()
	}
	return appendValueCommands(nil, []byte(key), e), expireAtMs, true
}

// appendValueCommands renders an entry's value -- everything about it except its
// deadline -- as replayable commands. It is shared by Dump and DumpKey so the whole
// snapshot surface has exactly one encoder per type.
//
// The caller holds the entry's shard lock.
func appendValueCommands(cmds [][][]byte, key []byte, e *entry) [][][]byte {
	switch e.kind {
	case kindString:
		return append(cmds, [][]byte{[]byte("SET"), key, copyBytes(e.str)})

	case kindList:
		elems := make([][]byte, 0, e.list.len())
		for el := e.list.l.Front(); el != nil; el = el.Next() {
			elems = append(elems, copyBytes(el.Value.([]byte)))
		}
		return appendChunks(cmds, "RPUSH", key, elems, 1)

	case kindHash:
		elems := make([][]byte, 0, len(e.dict)*2)
		for f, v := range e.dict {
			elems = append(elems, []byte(f), copyBytes(v))
		}
		return appendChunks(cmds, "HSET", key, elems, 2)

	case kindSet:
		elems := make([][]byte, 0, len(e.set))
		for m := range e.set {
			elems = append(elems, []byte(m))
		}
		return appendChunks(cmds, "SADD", key, elems, 1)

	case kindZSet:
		elems := make([][]byte, 0, e.zset.sl.length*2)
		node := e.zset.sl.head.next[0].forward
		for node != nil {
			elems = append(elems, []byte(formatFloat(node.score)), []byte(node.member))
			node = node.next[0].forward
		}
		return appendChunks(cmds, "ZADD", key, elems, 2)

	case kindStream:
		return appendStreamCommands(cmds, key, e.stream)
	}
	return cmds
}

// appendStreamCommands renders a stream as a replayable command sequence.
//
// A stream is the one type whose value is not the whole of its state. Its entries are
// only the first part; the rest is the id counters no entry records and the consumer
// groups, and *those are data*, not caches. A group carries how far it has delivered
// and which entries a consumer was given and has not acknowledged, which is precisely
// the state that says "this work is in flight". A snapshot that dropped it would
// silently re-deliver acknowledged work, or silently drop work that was outstanding, on
// every restart and on every replica sync -- a data-loss bug that no error would ever
// report. So the sequence is, per stream:
//
//	XADD key <id> field value ...     one per entry, in id order
//	XSETID key <last-id> ENTRIESADDED <n> MAXDELETEDID <id>
//	XGROUP CREATE key <group> <last-delivered-id> ENTRIESREAD <n>
//	XGROUP CREATECONSUMER key <group> <consumer>
//	XCLAIM key <group> <consumer> 0 <id> TIME <ms> RETRYCOUNT <n> FORCE JUSTID
//
// The XADDs come first so the ids exist before anything refers to them; XSETID follows
// them, because it is refused for an id below the top entry; the groups follow that,
// because XGROUP CREATE needs the key; and each group's consumers precede its PEL
// entries, so a consumer with nothing outstanding still survives the trip.
//
// Chunking: one XADD per entry and one XCLAIM per pending entry, so no emitted command
// can approach resp.MaxMultiBulk -- an entry's own field count is already bounded by
// the XADD that created it having fitted in one command, and an XCLAIM here carries
// exactly one id. That is a command per entry rather than per 256, which is what a
// per-entry id requires: entries cannot share an XADD, because each names its own id.
//
// A PEL entry whose stream entry has since been XDELed is not reconstructed: XCLAIM
// drops an id that is not in the stream, deliberately (see XClaim). Real Redis's own
// rewrite has the same property, for the same reason -- there is nothing to hand a
// consumer -- and the alternative would be to invent a command that creates a pending
// entry for data that does not exist.
func appendStreamCommands(cmds [][][]byte, key []byte, st *stream) [][][]byte {
	for _, e := range st.entries {
		cmd := make([][]byte, 0, 3+len(e.Fields))
		cmd = append(cmd, []byte("XADD"), key, []byte(e.ID.String()))
		for _, f := range e.Fields {
			cmd = append(cmd, copyBytes(f))
		}
		cmds = append(cmds, cmd)
	}
	// Always emitted, even for an empty stream: an XSETID is the only way to restore a
	// stream that has entries-added history but no entries left, and it is what stops a
	// replayed stream from re-issuing ids a consumer has already seen.
	cmds = append(cmds, [][]byte{
		[]byte("XSETID"), key, []byte(st.last.String()),
		[]byte("ENTRIESADDED"), []byte(strconv.FormatUint(st.added, 10)),
		[]byte("MAXDELETEDID"), []byte(st.maxDeleted.String()),
	})

	// Sorted so a snapshot of the same dataset is byte-for-byte the same sequence
	// rather than one that follows map iteration order.
	groups := make([]string, 0, len(st.groups))
	for name := range st.groups {
		groups = append(groups, name)
	}
	slices.Sort(groups)
	for _, name := range groups {
		g := st.groups[name]
		cmds = append(cmds, [][]byte{
			[]byte("XGROUP"), []byte("CREATE"), key, []byte(name),
			[]byte(g.lastDelivered.String()),
			[]byte("ENTRIESREAD"), []byte(strconv.FormatInt(g.entriesRead, 10)),
		})
		consumers := make([]string, 0, len(g.consumers))
		for cname := range g.consumers {
			consumers = append(consumers, cname)
		}
		slices.Sort(consumers)
		for _, cname := range consumers {
			cmds = append(cmds, [][]byte{
				[]byte("XGROUP"), []byte("CREATECONSUMER"), key, []byte(name), []byte(cname),
			})
		}
		ids := make([]StreamID, 0, len(g.pel))
		for id := range g.pel {
			ids = append(ids, id)
		}
		slices.SortFunc(ids, func(a, b StreamID) int { return a.Compare(b) })
		for _, id := range ids {
			nack := g.pel[id]
			cmds = append(cmds, [][]byte{
				[]byte("XCLAIM"), key, []byte(name), []byte(nack.consumer),
				[]byte("0"), []byte(id.String()),
				[]byte("TIME"), []byte(strconv.FormatInt(nack.deliveryMs, 10)),
				[]byte("RETRYCOUNT"), []byte(strconv.FormatInt(nack.deliveryCount, 10)),
				[]byte("FORCE"), []byte("JUSTID"),
			})
		}
	}
	return cmds
}

// appendChunks appends "name key elems..." commands, splitting elems into runs of
// at most dumpChunkElems elements. stride is the width of one logical item (2 for
// the field/value of HSET and the score/member of ZADD, 1 otherwise) so a chunk
// boundary never falls inside an item.
func appendChunks(cmds [][][]byte, name string, key []byte, elems [][]byte, stride int) [][][]byte {
	per := dumpChunkElems - dumpChunkElems%stride
	for start := 0; start < len(elems); start += per {
		end := min(start+per, len(elems))
		cmd := make([][]byte, 0, 2+end-start)
		cmd = append(cmd, []byte(name), key)
		cmd = append(cmd, elems[start:end]...)
		cmds = append(cmds, cmd)
	}
	return cmds
}
