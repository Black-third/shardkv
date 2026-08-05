package store

import "time"

// --- additional string operations --------------------------------------------

// SetDeadline stores a string with an absolute expiry (zero time = persistent).
// It is the apply form of SET ... PXAT, used by propagation and replay so a TTL
// is reconstructed against a fixed deadline rather than recomputed from a later
// clock.
func (s *Store) SetDeadline(key string, val []byte, deadline time.Time) {
	now := s.clock()
	e := &entry{kind: kindString, str: copyBytes(val), expireAt: deadline}
	s.touch(e, now)
	sh := s.getShard(key)
	sh.mu.Lock()
	sh.data[key] = e
	sh.mu.Unlock()
}

// SetNX sets key to val only if it does not already exist. Reports whether it
// was set.
func (s *Store) SetNX(key string, val []byte) bool {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if e, found := sh.data[key]; found && !e.expired(now) {
		return false
	}
	ne := &entry{kind: kindString, str: copyBytes(val)}
	s.touch(ne, now)
	sh.data[key] = ne
	return true
}

// GetSet sets key to val and returns the previous string value (clearing any
// TTL, as Redis does). old is invalid when oldOK is false.
func (s *Store) GetSet(key string, val []byte) (old []byte, oldOK bool, err error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if e, found := sh.data[key]; found && !e.expired(now) {
		if e.kind != kindString {
			return nil, false, ErrWrongType
		}
		old = copyBytes(e.str)
		oldOK = true
	}
	ne := &entry{kind: kindString, str: copyBytes(val)}
	s.touch(ne, now)
	sh.data[key] = ne
	return old, oldOK, nil
}

// GetDel returns the string value at key and deletes it.
func (s *Store) GetDel(key string) (val []byte, ok bool, err error) {
	sh := s.getShard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()
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
	defer sh.mu.Unlock()
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
	ne := &entry{kind: kindString, str: nv, expireAt: expireAt}
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
	defer sh.mu.Unlock()
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

	if si == di {
		ssh.mu.Lock()
		defer ssh.mu.Unlock()
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
	defer ssh.mu.Unlock()
	defer dsh.mu.Unlock()

	e, ok := ssh.data[src]
	if !ok || e.expired(now) {
		return false
	}
	delete(ssh.data, src)
	dsh.data[dst] = e
	return true
}
