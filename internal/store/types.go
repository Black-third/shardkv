package store

import "container/list"

// deque is a double-ended queue of byte slices backing the list type. It wraps
// container/list so pushes and pops at both ends are O(1).
type deque struct{ l *list.List }

func newDeque() *deque { return &deque{l: list.New()} }

func (d *deque) len() int { return d.l.Len() }

func (d *deque) lpush(v []byte) { d.l.PushFront(v) }
func (d *deque) rpush(v []byte) { d.l.PushBack(v) }

func (d *deque) lpop() ([]byte, bool) {
	el := d.l.Front()
	if el == nil {
		return nil, false
	}
	d.l.Remove(el)
	return el.Value.([]byte), true
}

func (d *deque) rpop() ([]byte, bool) {
	el := d.l.Back()
	if el == nil {
		return nil, false
	}
	d.l.Remove(el)
	return el.Value.([]byte), true
}

// rangeIdx returns elements in the inclusive index range [start, stop] using
// Redis semantics: negative indexes count from the end, and the range is
// clamped to the list bounds.
func (d *deque) rangeIdx(start, stop int) [][]byte {
	n := d.l.Len()
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop || n == 0 {
		return nil
	}
	out := make([][]byte, 0, stop-start+1)
	i := 0
	for el := d.l.Front(); el != nil; el = el.Next() {
		if i > stop {
			break
		}
		if i >= start {
			out = append(out, copyBytes(el.Value.([]byte)))
		}
		i++
	}
	return out
}

// --- list operations ---------------------------------------------------------

// LPush prepends values (left) and returns the new length.
func (s *Store) LPush(key string, vals ...[]byte) (int, error) {
	return s.listPush(key, true, vals)
}

// RPush appends values (right) and returns the new length.
func (s *Store) RPush(key string, vals ...[]byte) (int, error) {
	return s.listPush(key, false, vals)
}

func (s *Store) listPush(key string, left bool, vals [][]byte) (int, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, found := sh.data[key]
	live := found && !e.expired(now)
	if live && e.kind != kindList {
		return 0, ErrWrongType
	}
	if !live {
		e = &entry{kind: kindList, list: newDeque()}
		sh.data[key] = e
	}
	for _, v := range vals {
		if left {
			e.list.lpush(copyBytes(v))
		} else {
			e.list.rpush(copyBytes(v))
		}
	}
	s.touch(e, now)
	return e.list.len(), nil
}

// LPop removes and returns the first element. ok is false if the list is empty
// or missing. The key is deleted when it becomes empty.
func (s *Store) LPop(key string) ([]byte, bool, error) { return s.listPop(key, true) }

// RPop removes and returns the last element.
func (s *Store) RPop(key string) ([]byte, bool, error) { return s.listPop(key, false) }

func (s *Store) listPop(key string, left bool) ([]byte, bool, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, found := sh.data[key]
	if !found || e.expired(now) {
		return nil, false, nil
	}
	if e.kind != kindList {
		return nil, false, ErrWrongType
	}
	var v []byte
	var ok bool
	if left {
		v, ok = e.list.lpop()
	} else {
		v, ok = e.list.rpop()
	}
	if e.list.len() == 0 {
		delete(sh.data, key)
	} else {
		s.touch(e, now)
	}
	return v, ok, nil
}

// LRange returns elements in the inclusive [start, stop] range (Redis indexing).
func (s *Store) LRange(key string, start, stop int) ([][]byte, error) {
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
	return e.list.rangeIdx(start, stop), nil
}

// LLen returns the list length (0 if missing).
func (s *Store) LLen(key string) (int, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return 0, nil
	}
	if e.kind != kindList {
		return 0, ErrWrongType
	}
	return e.list.len(), nil
}

// --- hash operations ----------------------------------------------------------

// HSet sets field=value and reports the number of newly-created fields.
func (s *Store) HSet(key string, pairs ...[2][]byte) (int, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, found := sh.data[key]
	live := found && !e.expired(now)
	if live && e.kind != kindHash {
		return 0, ErrWrongType
	}
	if !live {
		e = &entry{kind: kindHash, dict: make(map[string][]byte)}
		sh.data[key] = e
	}
	created := 0
	for _, p := range pairs {
		field := string(p[0])
		if _, ok := e.dict[field]; !ok {
			created++
		}
		e.dict[field] = copyBytes(p[1])
	}
	s.touch(e, now)
	return created, nil
}

// HGet returns the value of field. ok is false if key or field is absent.
func (s *Store) HGet(key, field string) ([]byte, bool, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return nil, false, nil
	}
	if e.kind != kindHash {
		return nil, false, ErrWrongType
	}
	s.touch(e, now)
	v, ok := e.dict[field]
	if !ok {
		return nil, false, nil
	}
	return copyBytes(v), true, nil
}

// HDel removes fields and returns the count removed. Deletes the key if emptied.
func (s *Store) HDel(key string, fields ...string) (int, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, found := sh.data[key]
	if !found || e.expired(now) {
		return 0, nil
	}
	if e.kind != kindHash {
		return 0, ErrWrongType
	}
	removed := 0
	for _, f := range fields {
		if _, ok := e.dict[f]; ok {
			delete(e.dict, f)
			removed++
		}
	}
	if len(e.dict) == 0 {
		delete(sh.data, key)
	} else {
		s.touch(e, now)
	}
	return removed, nil
}

// HGetAll returns a copy of all field/value pairs as a flat slice
// [f1, v1, f2, v2, ...].
func (s *Store) HGetAll(key string) ([][]byte, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return nil, nil
	}
	if e.kind != kindHash {
		return nil, ErrWrongType
	}
	s.touch(e, now)
	out := make([][]byte, 0, len(e.dict)*2)
	for f, v := range e.dict {
		out = append(out, []byte(f), copyBytes(v))
	}
	return out, nil
}

// HLen returns the number of fields (0 if missing).
func (s *Store) HLen(key string) (int, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return 0, nil
	}
	if e.kind != kindHash {
		return 0, ErrWrongType
	}
	return len(e.dict), nil
}

// --- set operations -----------------------------------------------------------

// SAdd adds members and returns the number newly added.
func (s *Store) SAdd(key string, members ...string) (int, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, found := sh.data[key]
	live := found && !e.expired(now)
	if live && e.kind != kindSet {
		return 0, ErrWrongType
	}
	if !live {
		e = &entry{kind: kindSet, set: make(map[string]struct{})}
		sh.data[key] = e
	}
	added := 0
	for _, m := range members {
		if _, ok := e.set[m]; !ok {
			e.set[m] = struct{}{}
			added++
		}
	}
	s.touch(e, now)
	return added, nil
}

// SRem removes members and returns the count removed. Deletes the key if emptied.
func (s *Store) SRem(key string, members ...string) (int, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, found := sh.data[key]
	if !found || e.expired(now) {
		return 0, nil
	}
	if e.kind != kindSet {
		return 0, ErrWrongType
	}
	removed := 0
	for _, m := range members {
		if _, ok := e.set[m]; ok {
			delete(e.set, m)
			removed++
		}
	}
	if len(e.set) == 0 {
		delete(sh.data, key)
	} else {
		s.touch(e, now)
	}
	return removed, nil
}

// SMembers returns all members.
func (s *Store) SMembers(key string) ([]string, error) {
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
	out := make([]string, 0, len(e.set))
	for m := range e.set {
		out = append(out, m)
	}
	return out, nil
}

// SIsMember reports whether member is in the set.
func (s *Store) SIsMember(key, member string) (bool, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return false, nil
	}
	if e.kind != kindSet {
		return false, ErrWrongType
	}
	_, ok := e.set[member]
	return ok, nil
}

// SCard returns the set cardinality (0 if missing).
func (s *Store) SCard(key string) (int, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e := s.readEntry(sh, key, now)
	if e == nil {
		return 0, nil
	}
	if e.kind != kindSet {
		return 0, ErrWrongType
	}
	return len(e.set), nil
}
