package store

import "time"

// This file extends the set type whose SAdd/SRem/SMembers core lives in types.go
// with the random draws, the cross-key move, and the algebraic combinators.

// SPop removes and returns up to count random members. ok is false when the key is
// missing, which lets the caller distinguish SPOP's null reply (no key) from its
// empty-array reply (a count of zero). The key is deleted once it is empty.
//
// The members removed are chosen at random, so the caller must propagate the
// removal it observed (an SREM of exactly these members) rather than the command:
// a replica running SPOP itself would pick different members and diverge.
func (s *Store) SPop(key string, count int) (members []string, ok bool, err error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	charged := s.charge(sh, key)
	defer sh.mu.Unlock()
	defer s.settle(sh, key, charged)

	e := sh.liveEntry(key, now)
	if e == nil {
		return nil, false, nil
	}
	if e.kind != kindSet {
		return nil, false, ErrWrongType
	}
	if count >= len(e.set) {
		members = make([]string, 0, len(e.set))
		for m := range e.set {
			members = append(members, m)
		}
	} else {
		members = make([]string, 0, count)
		for m := range e.set {
			if len(members) == count {
				break
			}
			members = append(members, m)
		}
	}
	for _, m := range members {
		setDrop(e, m)
	}
	if len(e.set) == 0 {
		delete(sh.data, key)
	} else {
		s.touch(e, now)
	}
	return members, true, nil
}

// SRandMember returns count members without removing any. A negative count allows
// repeats and always yields exactly -count members; a positive count returns
// distinct members, capped at the set size. A missing key returns nothing.
//
// Like SPOP the draw is random, but nothing is modified, so there is nothing to
// propagate.
func (s *Store) SRandMember(key string, count int) ([]string, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return nil, nil
	}
	if e.kind != kindSet {
		return nil, ErrWrongType
	}
	s.touch(e, now)
	pool := make([]string, 0, len(e.set))
	for m := range e.set {
		pool = append(pool, m)
	}
	return pickMembers(pool, count), nil
}

// SMIsMember reports, for each member in order, whether it belongs to the set at
// key. A missing key answers false for every member.
func (s *Store) SMIsMember(key string, members ...string) ([]bool, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	out := make([]bool, len(members))
	e := s.readEntry(sh, key, now)
	if e == nil {
		return out, nil
	}
	if e.kind != kindSet {
		return nil, ErrWrongType
	}
	for i, m := range members {
		_, out[i] = e.set[m]
	}
	return out, nil
}

// SMove moves member from src to dst atomically, reporting whether it moved
// (false when src does not contain it). src is deleted once empty and dst created
// if needed; moving a member onto the set it is already in is a no-op that still
// reports success, as Redis does.
//
// Both shards are locked in index order so two concurrent moves with swapped keys
// cannot deadlock.
func (s *Store) SMove(src, dst, member string) (bool, error) {
	now := s.clock()
	unlock := s.lockKeys(src, dst)
	defer s.trackedKeys(unlock, src, dst)()

	ssh, dsh := s.getShard(src), s.getShard(dst)
	se := ssh.liveEntry(src, now)
	if se == nil {
		return false, nil
	}
	if se.kind != kindSet {
		return false, ErrWrongType
	}
	de := se
	if src != dst {
		de = dsh.liveEntry(dst, now)
		if de != nil && de.kind != kindSet {
			return false, ErrWrongType
		}
	}
	if _, ok := se.set[member]; !ok {
		return false, nil
	}
	if src == dst {
		return true, nil
	}

	setDrop(se, member)
	if len(se.set) == 0 {
		delete(ssh.data, src)
	} else {
		s.touch(se, now)
	}
	if de == nil {
		de = &entry{kind: kindSet, set: make(map[string]struct{})}
		dsh.data[dst] = de
	}
	setPut(de, member)
	s.touch(de, now)
	return true, nil
}

// SetOp names one of the algebraic set combinations.
type SetOp uint8

// The set combinations. Each is evaluated against the first key: intersection and
// difference stop early once the result can no longer grow.
const (
	SetInter SetOp = iota
	SetUnion
	SetDiff
)

// SCombine computes the intersection, union, or difference of the sets at keys and
// returns its members. limit, when positive, stops an intersection early once that
// many members have been found (SINTERCARD's LIMIT).
//
// A missing key counts as the empty set: an intersection involving one is empty
// and a difference from one is empty, both without inspecting the rest. Every
// shard involved is read-locked for the whole computation, in index order, so the
// result is a consistent cut of all the inputs rather than a stitch of several
// instants.
func (s *Store) SCombine(op SetOp, limit int, keys ...string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	now := s.clock()
	unlock := s.rlockKeys(keys...)
	defer unlock()
	return s.combineLocked(op, limit, now, keys)
}

// combineLocked does the work of SCombine with the relevant shards already locked,
// so SCombineStore can compute and store under one set of locks.
func (s *Store) combineLocked(op SetOp, limit int, now time.Time, keys []string) ([]string, error) {
	sets := make([]map[string]struct{}, len(keys))
	for i, k := range keys {
		e := s.getShard(k).liveEntry(k, now)
		if e == nil {
			continue // missing key == empty set
		}
		if e.kind != kindSet {
			return nil, ErrWrongType
		}
		s.touch(e, now) // an atomic store, safe under the shared lock a read holds
		sets[i] = e.set
	}

	var out []string
	switch op {
	case SetUnion:
		seen := make(map[string]struct{})
		for _, set := range sets {
			for m := range set {
				if _, dup := seen[m]; dup {
					continue
				}
				seen[m] = struct{}{}
				out = append(out, m)
			}
		}
	case SetInter:
		if sets[0] == nil {
			return nil, nil
		}
		for _, set := range sets[1:] {
			if set == nil {
				return nil, nil
			}
		}
	next:
		for m := range sets[0] {
			for _, set := range sets[1:] {
				if _, ok := set[m]; !ok {
					continue next
				}
			}
			out = append(out, m)
			if limit > 0 && len(out) == limit {
				break
			}
		}
	case SetDiff:
		if sets[0] == nil {
			return nil, nil
		}
	diff:
		for m := range sets[0] {
			for _, set := range sets[1:] {
				if _, ok := set[m]; ok {
					continue diff
				}
			}
			out = append(out, m)
		}
	}
	return out, nil
}

// SCombineStore computes the combination of the sets at keys and stores it at dst,
// returning the resulting cardinality. An empty result deletes dst instead of
// storing an empty set, which is Redis's behaviour and keeps "empty collection"
// and "missing key" the same state.
//
// changed reports whether dst was touched at all, so an empty result over a
// destination that did not exist propagates nothing. dst is locked together with
// the sources, in shard-index order, so the stored result is exactly the
// combination of one consistent view of the inputs -- and dst may safely be one of
// the sources.
func (s *Store) SCombineStore(op SetOp, dst string, keys ...string) (n int, changed bool, err error) {
	now := s.clock()
	unlock := s.lockKeys(append([]string{dst}, keys...)...)
	defer s.trackedKeys(unlock, dst)()

	members, err := s.combineLocked(op, 0, now, keys)
	if err != nil {
		return 0, false, err
	}
	dsh := s.getShard(dst)
	if len(members) == 0 {
		existed := dsh.liveEntry(dst, now) != nil
		delete(dsh.data, dst) // also clears an expired entry nothing reclaimed yet
		return 0, existed, nil
	}
	ne := &entry{kind: kindSet, set: make(map[string]struct{}, len(members))}
	for _, m := range members {
		setPut(ne, m)
	}
	s.touch(ne, now)
	dsh.data[dst] = ne
	return len(members), true, nil
}
