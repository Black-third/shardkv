package store

import (
	"bytes"
	"container/list"
)

// This file extends the list type whose deque and push/pop operations live in
// types.go with the indexed, positional, and cross-key operations.

// normalizeRange converts a Redis inclusive [start, stop] index pair over n
// elements into concrete bounds: negative indexes count back from the end and
// both ends are then clamped into the collection. ok is false when the range
// selects nothing, which callers report as an empty result rather than an error.
func normalizeRange(n, start, stop int) (int, int, bool) {
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	start = max(start, 0)
	if stop >= n {
		stop = n - 1
	}
	if start > stop || n == 0 {
		return 0, 0, false
	}
	return start, stop, true
}

// at returns the element at a 0-based index, negative counting from the end.
func (d *deque) at(i int) (*list.Element, bool) {
	n := d.l.Len()
	if i < 0 {
		i += n
	}
	if i < 0 || i >= n {
		return nil, false
	}
	// Walk from whichever end is closer: a list index is O(n) either way, but the
	// tail-relative access LINDEX -1 does should not traverse the whole list.
	if i <= n/2 {
		el := d.l.Front()
		for j := 0; j < i; j++ {
			el = el.Next()
		}
		return el, true
	}
	el := d.l.Back()
	for j := n - 1; j > i; j-- {
		el = el.Prev()
	}
	return el, true
}

// insertAt inserts val next to the first element equal to pivot and returns the
// new length, or -1 when pivot is absent.
func (d *deque) insertAt(pivot, val []byte, before bool) int {
	for el := d.l.Front(); el != nil; el = el.Next() {
		if !bytes.Equal(el.Value.([]byte), pivot) {
			continue
		}
		if before {
			d.l.InsertBefore(val, el)
		} else {
			d.l.InsertAfter(val, el)
		}
		return d.l.Len()
	}
	return -1
}

// removeVal deletes elements equal to val and returns how many went. A positive
// count removes at most that many scanning from the head, a negative count the
// same from the tail, and zero removes every match.
func (d *deque) removeVal(count int, val []byte) int {
	limit, fromTail := count, false
	if count < 0 {
		limit, fromTail = -count, true
	}
	removed := 0
	el := d.l.Front()
	if fromTail {
		el = d.l.Back()
	}
	for el != nil {
		next := el.Next()
		if fromTail {
			next = el.Prev()
		}
		if bytes.Equal(el.Value.([]byte), val) {
			d.l.Remove(el)
			removed++
			if count != 0 && removed == limit {
				break
			}
		}
		el = next
	}
	return removed
}

// trim keeps only the inclusive [start, stop] index range, discarding the rest.
func (d *deque) trim(start, stop int) {
	from, to, ok := normalizeRange(d.l.Len(), start, stop)
	if !ok {
		d.l.Init()
		return
	}
	for i, el := 0, d.l.Front(); i < from && el != nil; i++ {
		next := el.Next()
		d.l.Remove(el)
		el = next
	}
	for i, el := d.l.Len()-1, d.l.Back(); i > to-from && el != nil; i-- {
		prev := el.Prev()
		d.l.Remove(el)
		el = prev
	}
}

// positions returns the indexes of elements equal to val. rank selects which match
// to start from (1 = the first from the head, -1 = the first from the tail) and
// must not be zero; count bounds how many indexes to return, 0 meaning all;
// maxlen bounds how many elements to compare, 0 meaning the whole list.
func (d *deque) positions(val []byte, rank, count, maxlen int) []int {
	n := d.l.Len()
	skip := rank - 1
	fromTail := false
	if rank < 0 {
		skip, fromTail = -rank-1, true
	}

	var out []int
	compared, matched := 0, 0
	i, el := 0, d.l.Front()
	if fromTail {
		i, el = n-1, d.l.Back()
	}
	for el != nil {
		if maxlen > 0 && compared >= maxlen {
			break
		}
		compared++
		if bytes.Equal(el.Value.([]byte), val) {
			if matched >= skip {
				out = append(out, i)
				if count > 0 && len(out) == count {
					break
				}
			}
			matched++
		}
		if fromTail {
			i, el = i-1, el.Prev()
		} else {
			i, el = i+1, el.Next()
		}
	}
	return out
}

// --- store-level list operations ----------------------------------------------

// LIndex returns the element at index (negative counts from the end). ok is false
// for a missing key or an index outside the list.
func (s *Store) LIndex(key string, index int) ([]byte, bool, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return nil, false, nil
	}
	if e.kind != kindList {
		return nil, false, ErrWrongType
	}
	s.touch(e, now)
	el, ok := e.list.at(index)
	if !ok {
		return nil, false, nil
	}
	return copyBytes(el.Value.([]byte)), true, nil
}

// LSet overwrites the element at index. ErrNoSuchKey reports a missing key and
// ErrIndexOutOfRange an index outside the list -- the two distinct errors Redis
// replies with, rather than one silent no-op.
func (s *Store) LSet(key string, index int, val []byte) error {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e := sh.liveEntry(key, now)
	if e == nil {
		return ErrNoSuchKey
	}
	if e.kind != kindList {
		return ErrWrongType
	}
	el, ok := e.list.at(index)
	if !ok {
		return ErrIndexOutOfRange
	}
	el.Value = copyBytes(val)
	s.touch(e, now)
	return nil
}

// LInsert inserts val before or after the first element equal to pivot and
// returns the new length. It returns 0 for a missing key and -1 when the pivot is
// absent, which is how a client distinguishes "no list" from "no such element".
func (s *Store) LInsert(key string, before bool, pivot, val []byte) (int, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e := sh.liveEntry(key, now)
	if e == nil {
		return 0, nil
	}
	if e.kind != kindList {
		return 0, ErrWrongType
	}
	n := e.list.insertAt(pivot, copyBytes(val), before)
	if n > 0 {
		s.touch(e, now)
	}
	return n, nil
}

// LRem removes elements equal to val and returns how many were removed: count > 0
// removes at most count scanning from the head, count < 0 the same from the tail,
// and count == 0 removes every match. The key is deleted once it is empty.
func (s *Store) LRem(key string, count int, val []byte) (int, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e := sh.liveEntry(key, now)
	if e == nil {
		return 0, nil
	}
	if e.kind != kindList {
		return 0, ErrWrongType
	}
	removed := e.list.removeVal(count, val)
	if e.list.len() == 0 {
		delete(sh.data, key)
	} else if removed > 0 {
		s.touch(e, now)
	}
	return removed, nil
}

// LTrim keeps only the inclusive [start, stop] index range. A range that selects
// nothing empties the list, which deletes the key -- Redis treats an empty
// collection and a missing key as the same thing. changed reports whether
// anything was actually discarded.
func (s *Store) LTrim(key string, start, stop int) (changed bool, err error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e := sh.liveEntry(key, now)
	if e == nil {
		return false, nil
	}
	if e.kind != kindList {
		return false, ErrWrongType
	}
	before := e.list.len()
	e.list.trim(start, stop)
	if e.list.len() == 0 {
		delete(sh.data, key)
	} else {
		s.touch(e, now)
	}
	return e.list.len() != before, nil
}

// LPos returns the indexes of elements equal to val, at most count of them (0 =
// all). rank picks the match to start from: 1 is the first from the head, -1 the
// first from the tail; it must not be zero. maxlen bounds how many elements are
// compared (0 = the whole list).
func (s *Store) LPos(key string, val []byte, rank, count, maxlen int) ([]int, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return nil, nil
	}
	if e.kind != kindList {
		return nil, ErrWrongType
	}
	s.touch(e, now)
	return e.list.positions(val, rank, count, maxlen), nil
}

// LPopCount removes and returns up to n elements from the head (left) or tail
// (right) of the list, in the order they were popped. ok is false for a missing
// key, which the caller reports as a null array rather than an empty one. The key
// is deleted once it is empty.
func (s *Store) LPopCount(key string, n int, left bool) (vals [][]byte, ok bool, err error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e := sh.liveEntry(key, now)
	if e == nil {
		return nil, false, nil
	}
	if e.kind != kindList {
		return nil, false, ErrWrongType
	}
	for i := 0; i < n; i++ {
		var v []byte
		var got bool
		if left {
			v, got = e.list.lpop()
		} else {
			v, got = e.list.rpop()
		}
		if !got {
			break
		}
		vals = append(vals, v)
	}
	if e.list.len() == 0 {
		delete(sh.data, key)
	} else {
		s.touch(e, now)
	}
	return vals, true, nil
}

// LMove atomically pops an element from one end of src and pushes it onto one end
// of dst, returning the element moved. ok is false when src is missing or empty,
// in which case dst is untouched.
//
// src and dst may be the same key (a rotation). Both shards are locked in index
// order, so two concurrent moves with swapped keys cannot deadlock.
func (s *Store) LMove(src, dst string, srcLeft, dstLeft bool) (val []byte, ok bool, err error) {
	now := s.clock()
	unlock := s.lockKeys(src, dst)
	defer unlock()

	ssh, dsh := s.getShard(src), s.getShard(dst)
	se := ssh.liveEntry(src, now)
	if se == nil {
		return nil, false, nil
	}
	if se.kind != kindList {
		return nil, false, ErrWrongType
	}
	de := se
	if src != dst {
		de = dsh.liveEntry(dst, now)
		if de != nil && de.kind != kindList {
			return nil, false, ErrWrongType
		}
	}

	if srcLeft {
		val, ok = se.list.lpop()
	} else {
		val, ok = se.list.rpop()
	}
	if !ok {
		return nil, false, nil
	}
	if de == nil {
		de = &entry{kind: kindList, list: newDeque()}
		dsh.data[dst] = de
	}
	if dstLeft {
		de.list.lpush(val)
	} else {
		de.list.rpush(val)
	}
	// The pop can only empty src if nothing was pushed back into it.
	if se.list.len() == 0 {
		delete(ssh.data, src)
	} else {
		s.touch(se, now)
	}
	s.touch(de, now)
	return copyBytes(val), true, nil
}
