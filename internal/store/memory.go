package store

// Memory accounting for MEMORY USAGE.
//
// The number is an estimate and is documented as one, exactly as Redis's is: Go has
// no portable way to ask the allocator how large a live object graph is, so the size
// is computed from the value's own contents plus a fixed per-object overhead. What it
// is *good* for is the same thing Redis's is good for -- comparing keys against each
// other and spotting the one that grew -- and it is exact about the part that
// dominates, which is the payload bytes.

import "unsafe"

// Per-object overheads used by the estimate. They are the sizes the Go runtime
// actually uses for the headers involved (a map bucket entry, a slice header, an
// interface word), not invented constants, so the estimate tracks a real cost rather
// than a guess.
const (
	// memEntryOverhead covers the entry struct itself plus the keyspace map's slot for
	// it: the pointer in the bucket, the hash-tophash byte, and the string header for
	// the key.
	memEntryOverhead = int64(unsafe.Sizeof(entry{})) + 48
	// memMapSlot is the amortized per-element cost of a Go map with a string key:
	// the string header, the value, and the bucket's share of tophash and overflow.
	memMapSlot = 48
	// memListElem is one container/list element: two pointers, the list pointer, and
	// the interface holding the []byte.
	memListElem = 64
	// memSkipNode is one skip-list node: the member string header, the score, the
	// backward pointer, and on average two levels of forward pointer and span.
	memSkipNode = 96
)

// MemoryUsage estimates how many bytes of memory the value at key occupies,
// including its own key string. ok is false when the key is missing or expired.
//
// The estimate covers the payload exactly and the container overhead
// approximately, which is what makes it comparable between keys of the same type
// and honest about the thing an operator is looking for: which key is large.
func (s *Store) MemoryUsage(key string) (int64, bool) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := sh.liveEntry(key, now)
	if e == nil {
		return 0, false
	}
	// Deliberately not readEntry: MEMORY USAGE inspects a key's footprint rather than
	// reading its value, so counting it as a cache hit would inflate the hit rate with
	// introspection traffic. Redis excludes it from the counters for the same reason.
	return memEntryOverhead + int64(len(key)) + entrySize(e), true
}

// entrySize is the estimated size of one value, by type.
func entrySize(e *entry) int64 {
	switch e.kind {
	case kindString:
		return int64(cap(e.str))

	case kindList:
		var n int64
		for el := e.list.l.Front(); el != nil; el = el.Next() {
			n += memListElem + int64(len(el.Value.([]byte)))
		}
		return n

	case kindHash:
		var n int64
		for f, v := range e.dict {
			n += memMapSlot + int64(len(f)) + int64(cap(v))
		}
		return n

	case kindSet:
		var n int64
		for m := range e.set {
			n += memMapSlot + int64(len(m))
		}
		return n

	case kindZSet:
		var n int64
		for m := range e.zset.dict {
			// Every member is in both the dict and the skip list, which is exactly why a
			// sorted set costs more per member than a set does.
			n += memMapSlot + memSkipNode + 2*int64(len(m))
		}
		return n

	case kindStream:
		return e.stream.memorySize()
	}
	return 0
}
