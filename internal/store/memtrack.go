package store

// Byte accounting: the running total behind used_memory and the maxmemory budget.
//
// # Why a maintained counter and not a walk
//
// The size of a value is a property of the value, so there is no cheaper way to learn
// it than to look at it -- which is what Footprint and MEMORY USAGE do, and why both
// are O(the thing they measure) and on no command's path but their own. A byte budget
// cannot be enforced that way: the question "are we over the limit?" is asked before
// every write, and answering it with a walk of the keyspace would make every write
// O(keyspace).
//
// So the total is maintained. The whole difficulty of a maintained total is that it has
// to be right across *every* path that changes a value's size, including the ones that
// change it in place (APPEND, SETRANGE, SETBIT, BITFIELD, the HyperLogLog commands) and
// including expiry and eviction. A counter that is updated at each of the ~90 mutation
// sites by hand is a counter that will be wrong the first time a site is added and
// nobody notices, and being wrong is not a small failure here: a number an operator
// sizes a container from must not lie.
//
// The shape below is chosen to make "did you remember?" not a question. A mutating
// method does not compute a delta; it declares that it is about to change a key:
//
//	sh.mu.Lock()
//	charged := s.charge(sh, key)
//	defer sh.mu.Unlock()
//	defer s.settle(sh, key, charged)
//
// charge reads what the key costs now, in O(1); settle re-reads it after the body has run
// and applies the difference, before the unlock releases the shard (defers run last-in
// first-out). Insert, overwrite, in-place growth, shrink and delete are therefore all one
// case, and a method cannot be half-instrumented: either it declared the key it changes or
// it did not, and the randomised drift test (TestMemoryAccountingDoesNotDrift) is what
// proves the total still equals a full recomputation over every type and every mutation.
//
// It is three plain statements rather than one helper returning a closure, and that is a
// measurement rather than a preference: the closure form escaped to the heap, so it added an
// allocation and 80 bytes to *every* write in the store -- measured at +90% on Store.Set.
// Deferred calls with ordinary arguments are open-coded by the compiler and cost nothing.
//
// The O(1) part is what makes that affordable, and it is why each container carries its
// own byte count (deque.bytes, zset.bytes, entry.elemBytes): asking a 1M-field hash how
// large it is must not be a walk of the hash, or HSET would become O(n).
//
// # What the number does and does not count
//
// It counts, for every entry the keyspace holds: the entry struct and the keyspace map's
// slot for it (memEntryOverhead), the key's own bytes, and the value's payload measured
// the same way MEMORY USAGE measures it -- exactly, for the bytes, and approximately for
// the container overhead around them (see memory.go, which owns those constants).
//
// It does *not* count: the Go runtime's own footprint, the allocator's slack and the
// per-shard map's spare capacity, the replication backlog, client input and output
// buffers, the AOF's buffer, or the goroutine stacks. Redis's used_memory excludes the
// replica output buffers from its own maxmemory arithmetic for the same reason -- a
// budget you cannot act on by evicting a key is not a budget eviction can meet -- and
// includes the rest, which it can because it measures at the allocator. There is no
// portable way to do that in Go, so the number here is the dataset's size and is
// documented as being that rather than the process's.
//
// A key whose deadline has passed but which nothing has reclaimed yet is still counted:
// it is still occupying memory, and it stops being counted when the sweep, a read, or
// eviction actually removes it.
//
// # The one gap, and it is deliberate
//
// Streams are the exception. Their mutation paths live in stream.go, which this change
// does not own, so a stream's payload is refreshed by the maintenance pass (see
// Store.maintain) rather than at the moment it changes -- which means used_memory lags a
// stream-only write burst by at most one sweep interval, and converges exactly.
// Everything else is exact at every instant. See the report accompanying this change for
// the two-line addition to stream.go that would close it.

// keyCost is everything the shard's bookkeeping needs to know about one key: what it
// costs in bytes, and whether it carries a deadline.
//
// The second field rides along with the first because it is maintained by exactly the same
// mechanism and at exactly the same moments -- every mutation that could change a key's
// size could also give it or take away a TTL -- and because the eviction sampler needs it
// to be a per-shard *count* rather than something it discovers by scanning. See
// shard.volatile.
type keyCost struct {
	bytes    int64
	volatile bool
	present  bool
}

// charge is what key currently costs the keyspace: the entry's own overhead, the key's
// bytes, and the value's payload, plus whether it is volatile. Caller holds sh's lock
// (either mode).
//
// It is the single definition of "what one key costs", read by both halves of the
// accounting, so the value added when a key appears and the value subtracted when it
// goes cannot disagree.
//
// When nothing is tracking memory it answers immediately, before touching the keyspace map.
// That early return is the whole of what a default server pays for this feature: one atomic
// load on each side of a write, no map lookup, no arithmetic. See Store.memTrack.
func (s *Store) charge(sh *shard, key string) keyCost {
	if !s.memTrack.Load() {
		return keyCost{}
	}
	return sh.chargeOf(key)
}

func (sh *shard) chargeOf(key string) keyCost {
	e, ok := sh.data[key]
	if !ok {
		return keyCost{}
	}
	return keyCost{
		bytes:    entryCharge(key, e),
		volatile: !e.expireAt.IsZero(),
		present:  true,
	}
}

func entryCharge(key string, e *entry) int64 {
	return memEntryOverhead + int64(len(key)) + entryPayload(e)
}

// entryPayload is the value's payload size in O(1), read from the count each container
// maintains as it is mutated. It is the hot-path counterpart of entrySize, which
// measures the same thing by walking the value.
//
// The two must agree exactly, which is what the drift test asserts per entry rather
// than only in total: a total that is right by luck (one key over, another under) is not
// an accounting anyone can reason about.
func entryPayload(e *entry) int64 {
	switch e.kind {
	case kindString:
		return int64(cap(e.str))
	case kindList:
		return e.list.bytes
	case kindZSet:
		return e.zset.bytes
	default:
		// Hash, set and stream. The first two are bare Go maps with no wrapper that could
		// hold a count, so theirs is maintained on the entry; a stream's is refreshed by
		// the maintenance pass.
		return e.elemBytes
	}
}

// trackedKeys settles several keys at once, for a write taken under lockKeys. Every key the
// write may touch is listed; naming one that did not change costs a comparison and nothing
// else, so the safe direction is to name them all.
//
// This one does return a closure, and unlike the single-key path it is affordable: lockKeys
// already returns one, so the multi-key commands (COPY, SMOVE, LMOVE, MSETNX, BITOP,
// PFMERGE and the STORE variants) were allocating before this change and are heavier
// commands in any case. The single-key path is the one every write goes through, which is
// why it is spelled out in plain statements instead.
func (s *Store) trackedKeys(unlock func(), keys ...string) func() {
	if !s.memTrack.Load() {
		return unlock
	}
	// Sized for the common cases (a source and a destination, or a destination and two
	// sources) so the usual multi-key write adds no allocation to a command that is
	// already touching several shards.
	var buf [4]keyCost
	before := buf[:0]
	if len(keys) > cap(before) {
		before = make([]keyCost, 0, len(keys))
	}
	for _, k := range keys {
		before = append(before, s.getShard(k).chargeOf(k))
	}
	return func() {
		for i, k := range keys {
			s.settle(s.getShard(k), k, before[i])
		}
		unlock()
	}
}

// settle applies the difference between what key cost before a mutation and what it
// costs now. Caller holds sh's write lock, which is what makes the shard's own totals
// plain integers rather than atomics.
func (s *Store) settle(sh *shard, key string, before keyCost) {
	if !s.memTrack.Load() {
		return
	}
	after := sh.chargeOf(key)
	if d := after.bytes - before.bytes; d != 0 {
		sh.mem += d
		s.mem.Add(d)
	}
	sh.volatile += countVolatile(after) - countVolatile(before)
}

func countVolatile(c keyCost) int {
	if c.present && c.volatile {
		return 1
	}
	return 0
}

// uncharge removes key's cost from the totals without looking at it again, for a caller
// that has already taken the entry out of the map. Caller holds sh's write lock.
func (s *Store) uncharge(sh *shard, key string, e *entry) {
	if !s.memTrack.Load() {
		return
	}
	d := entryCharge(key, e)
	sh.mem -= d
	s.mem.Add(-d)
	if !e.expireAt.IsZero() {
		sh.volatile--
	}
}

// UsedMemory is what the dataset in this database occupies, by the estimate this file
// documents. Once the accounting is running it is one atomic load, which is what lets the
// maxmemory check sit on the write path.
//
// Asking is what starts the accounting. A server that nobody bounds and nobody asks does
// none of it -- one atomic load on each side of a write and nothing else -- and the first
// caller to want the number pays for a full recomputation so that what it gets back is
// derived from the values themselves rather than from a counter that was not being kept.
// After that the counter is maintained and every later read is free.
//
// That is invariant 12's rule rather than a shortcut: an observer that is not watching costs
// nothing, and the cost of watching is paid by whoever asked to watch. It also means the
// number is never an extrapolation from a partial history -- there is no state in which the
// counter has been running for only part of the dataset's life.
func (s *Store) UsedMemory() int64 {
	if !s.memTrack.Load() {
		s.TrackMemory()
	}
	return s.mem.Load()
}

// TrackMemory switches the accounting on and derives the totals from the dataset. It is
// idempotent, and it is what SetMaxMemory and the first UsedMemory call go through.
//
// The flag is set *before* the recomputation, not after, and the order is what makes it
// safe: a write that lands during the walk is accounted for (the flag is already up) and
// then the walk's own figure for that shard replaces the running one under the same lock, so
// the result is exact either way. Setting the flag afterwards would silently lose every
// write that happened during the walk.
func (s *Store) TrackMemory() {
	if s.memTrack.Swap(true) {
		return // already running
	}
	s.RecomputeMemory()
}

// MemoryTracked reports whether the byte accounting is running, for tests that need to
// distinguish "the number is zero" from "nothing is counting".
func (s *Store) MemoryTracked() bool { return s.memTrack.Load() }

// exactMemory recomputes the total the slow way -- walking every value in every shard --
// and is the ground truth the maintained counter is tested against. It changes nothing.
//
// It is O(the whole dataset) and belongs to tests and to RecomputeMemory, never to a
// command path.
func (s *Store) exactMemory() int64 {
	var total int64
	for _, sh := range s.shards {
		sh.mu.RLock()
		for k, e := range sh.data {
			total += memEntryOverhead + int64(len(k)) + entrySize(e)
		}
		sh.mu.RUnlock()
	}
	return total
}

// RecomputeMemory re-derives every maintained byte count from the values themselves and
// resets the totals to match, shard by shard.
//
// It exists for two reasons. It is what makes enabling a byte budget honest: CONFIG SET
// maxmemory runs it, so the limit is compared against a number derived from the dataset
// rather than against whatever a counter had accumulated. And it is the self-healing
// path -- a mutation site that ever escapes the accounting is corrected here instead of
// drifting forever, which is the difference between an estimate and a lie.
//
// It takes one shard's write lock at a time and never two, so it composes with
// concurrent writes the way every other whole-keyspace operation here does: each shard's
// total is exact as of the moment it was locked.
func (s *Store) RecomputeMemory() {
	for _, sh := range s.shards {
		sh.mu.Lock()
		var total int64
		volatile := 0
		for k, e := range sh.data {
			reseat(e)
			total += memEntryOverhead + int64(len(k)) + entryPayload(e)
			if !e.expireAt.IsZero() {
				volatile++
			}
		}
		if d := total - sh.mem; d != 0 {
			sh.mem = total
			s.mem.Add(d)
		}
		sh.volatile = volatile
		sh.mu.Unlock()
	}
}

// reseat rewrites a value's maintained byte count from the value itself. Caller holds
// the shard's write lock.
func reseat(e *entry) {
	switch e.kind {
	case kindString:
		// Nothing to maintain: a string's payload is its buffer's capacity.
	case kindList:
		var n int64
		for el := e.list.l.Front(); el != nil; el = el.Next() {
			n += memListElem + int64(len(el.Value.([]byte)))
		}
		e.list.bytes = n
	case kindZSet:
		var n int64
		for m := range e.zset.dict {
			n += memMapSlot + memSkipNode + 2*int64(len(m))
		}
		e.zset.bytes = n
	default:
		e.elemBytes = entrySize(e)
	}
}

// --- the per-container counts -------------------------------------------------

// The helpers below are the only way the hash and set types are mutated, because those
// two are bare Go maps: there is no wrapper method that could keep the count, so the
// count lives on the entry and these keep it. A raw `e.dict[f] = v` elsewhere would be
// exactly the silent drift this file exists to prevent.

// hashPut writes field=val, adjusting the hash's byte count. val is stored as given, so
// the caller copies it if it does not own it.
func hashPut(e *entry, field string, val []byte) {
	if old, ok := e.dict[field]; ok {
		e.elemBytes += int64(cap(val)) - int64(cap(old))
	} else {
		e.elemBytes += memMapSlot + int64(len(field)) + int64(cap(val))
	}
	e.dict[field] = val
}

// hashDrop removes field and reports whether it was there.
func hashDrop(e *entry, field string) bool {
	old, ok := e.dict[field]
	if !ok {
		return false
	}
	e.elemBytes -= memMapSlot + int64(len(field)) + int64(cap(old))
	delete(e.dict, field)
	return true
}

// setPut adds member and reports whether it was new.
func setPut(e *entry, member string) bool {
	if _, ok := e.set[member]; ok {
		return false
	}
	e.elemBytes += memMapSlot + int64(len(member))
	e.set[member] = struct{}{}
	return true
}

// setDrop removes member and reports whether it was there.
func setDrop(e *entry, member string) bool {
	if _, ok := e.set[member]; !ok {
		return false
	}
	e.elemBytes -= memMapSlot + int64(len(member))
	delete(e.set, member)
	return true
}
