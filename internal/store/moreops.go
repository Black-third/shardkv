package store

import "time"

// --- additional string operations --------------------------------------------

// SetDeadline stores a string with an absolute expiry (zero time = persistent).
// It is the apply form of SET ... PXAT, used by propagation and replay so a TTL
// is reconstructed against a fixed deadline rather than recomputed from a later
// clock.
func (s *Store) SetDeadline(key string, val []byte, deadline time.Time) {
	now := s.clock()
	sh := s.getShard(key)
	sh.mu.Lock()
	charged := s.charge(sh, key)
	defer sh.mu.Unlock()
	defer s.settle(sh, key, charged)
	s.putString(sh, key, val, deadline, now)
}

// SetNX sets key to val only if it does not already exist. Reports whether it
// was set.
func (s *Store) SetNX(key string, val []byte) bool {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	charged := s.charge(sh, key)
	defer sh.mu.Unlock()
	defer s.settle(sh, key, charged)
	if e, found := sh.data[key]; found && !e.expired(now) {
		return false
	}
	ne := &entry{kind: kindString, str: copyBytes(val)}
	s.touch(ne, now)
	sh.data[key] = ne
	return true
}

// MSetNX sets every pair only if *none* of the keys already exists, reporting whether
// it did. Either all of them are written or none is.
//
// The all-or-nothing guarantee is the whole command, so the existence check and the
// writes have to happen under one acquisition of every shard involved -- checking first
// and writing after would let a concurrent SET land in between and turn "none existed"
// into "one did" without the caller ever being told. lockKeys orders the shards by index
// so two MSETNX calls with the keys in opposite orders cannot deadlock, and it handles
// two keys that hash to the same shard.
func (s *Store) MSetNX(pairs [][2][]byte) bool {
	keys := make([]string, len(pairs))
	for i, p := range pairs {
		keys[i] = string(p[0])
	}
	unlock := s.lockKeys(keys...)
	defer s.trackedKeys(unlock, keys...)()

	now := s.clock()
	for _, key := range keys {
		sh := s.getShard(key)
		if e, found := sh.data[key]; found && !e.expired(now) {
			return false
		}
	}
	for _, p := range pairs {
		key := string(p[0])
		ne := &entry{kind: kindString, str: copyBytes(p[1])}
		s.touch(ne, now)
		s.getShard(key).data[key] = ne
	}
	return true
}

// GetSet sets key to val and returns the previous string value (clearing any
// TTL, as Redis does). old is invalid when oldOK is false.
func (s *Store) GetSet(key string, val []byte) (old []byte, oldOK bool, err error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	charged := s.charge(sh, key)
	defer sh.mu.Unlock()
	defer s.settle(sh, key, charged)
	if e, found := sh.data[key]; found && !e.expired(now) {
		if e.kind != kindString {
			return nil, false, ErrWrongType
		}
		old = copyBytes(e.str)
		oldOK = true
	}
	s.putString(sh, key, val, time.Time{}, now)
	return old, oldOK, nil
}

// GetDel returns the string value at key and deletes it.
func (s *Store) GetDel(key string) (val []byte, ok bool, err error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	charged := s.charge(sh, key)
	defer sh.mu.Unlock()
	defer s.settle(sh, key, charged)
	e, found := sh.data[key]
	if !found || e.expired(now) {
		return nil, false, nil
	}
	if e.kind != kindString {
		return nil, false, ErrWrongType
	}
	out := copyBytes(e.str)
	delete(sh.data, key)
	return out, true, nil
}

// Append appends suffix to the string at key (creating it if absent) and returns
// the new length. Any existing TTL is preserved.
func (s *Store) Append(key string, suffix []byte) (int, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	charged := s.charge(sh, key)
	defer sh.mu.Unlock()
	defer s.settle(sh, key, charged)
	e, found := sh.data[key]
	live := found && !e.expired(now)
	if live && e.kind != kindString {
		return 0, ErrWrongType
	}
	var base []byte
	var expireAt time.Time
	if live {
		base = e.str
		expireAt = e.expireAt
	}
	nv := make([]byte, 0, len(base)+len(suffix))
	nv = append(nv, base...)
	nv = append(nv, suffix...)
	// APPEND is the one command whose origin depends on whether the key was there.
	// Appending to an existing value unshares it into a plain buffer that Redis never
	// re-encodes, so it is raw from here on whatever its bytes read as; appending to a
	// *missing* key is really a store of the whole argument, and Redis's appendCommand
	// runs tryObjectEncoding on it before dbAdd -- so `APPEND fresh 123` reads `int`,
	// measured on redis 7.2.15. Treating both as mutations reported raw for the create
	// case, which is the second half of the encoding gap strOrigin fixed.
	origin := strMutatedBuffer
	if !live {
		origin = strWholeValue
	}
	ne := &entry{kind: kindString, str: nv, strOrigin: origin, expireAt: expireAt}
	s.touch(ne, now)
	sh.data[key] = ne
	return len(nv), nil
}

// StrLen returns the length of the string at key (0 if missing).
func (s *Store) StrLen(key string) (int, error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e := s.readEntry(sh, key, now)
	if e == nil {
		return 0, nil
	}
	if e.kind != kindString {
		return 0, ErrWrongType
	}
	return len(e.str), nil
}

// --- additional key operations -----------------------------------------------

// ExpireAt sets an absolute expiry on an existing live key. A past deadline
// makes the key eligible for removal on the next access/sweep. Reports false if
// the key is missing or already expired. It is ExpireAtCond with no condition.
func (s *Store) ExpireAt(key string, deadline time.Time) bool {
	return s.ExpireAtCond(key, deadline, ExpireAlways)
}

// Persist removes the TTL from key, making it permanent. Reports whether a TTL
// was actually removed.
func (s *Store) Persist(key string) bool {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	charged := s.charge(sh, key)
	defer sh.mu.Unlock()
	defer s.settle(sh, key, charged)
	e, found := sh.data[key]
	if !found || e.expired(now) || e.expireAt.IsZero() {
		return false
	}
	e.expireAt = time.Time{}
	return true
}

// Rename moves src to dst (overwriting dst). Reports false if src is missing.
func (s *Store) Rename(src, dst string) bool {
	si := fnv1a(src) & s.mask
	di := fnv1a(dst) & s.mask
	ssh, dsh := s.shards[si], s.shards[di]
	now := s.clock()

	// Both keys are settled, and both have to be: the entry moves, so src stops costing
	// what it cost and dst starts costing the same payload under a key of a different
	// length.
	if si == di {
		ssh.mu.Lock()
		defer s.trackedKeys(ssh.mu.Unlock, src, dst)()
		e, ok := ssh.data[src]
		if !ok || e.expired(now) {
			return false
		}
		delete(ssh.data, src)
		ssh.data[dst] = e
		return true
	}

	// Lock both shards in a consistent order to avoid deadlock.
	if si < di {
		ssh.mu.Lock()
		dsh.mu.Lock()
	} else {
		dsh.mu.Lock()
		ssh.mu.Lock()
	}
	unlock := func() { ssh.mu.Unlock(); dsh.mu.Unlock() }
	defer s.trackedKeys(unlock, src, dst)()

	e, ok := ssh.data[src]
	if !ok || e.expired(now) {
		return false
	}
	delete(ssh.data, src)
	dsh.data[dst] = e
	return true
}
