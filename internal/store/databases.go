package store

// Cross-store operations, which exist because a Store is one database and a server has
// several. MOVE, COPY ... DB and SWAPDB are the only commands that reach across two of
// them.
//
// A Store deliberately knows nothing about databases -- it has no index and no notion
// of siblings -- so these are package-level functions over two stores rather than
// methods on one. Everything else in the package stays single-keyspace, which is what
// keeps the shard geometry and the lock discipline unchanged for the 130-odd commands
// that only ever touch one database.
//
// # Lock ordering
//
// Each function locks a shard (or every shard) in src and then the matching one in dst.
// Two such calls with the databases the other way round would deadlock, so the *caller*
// must serialize cross-store operations against each other; the server does that with a
// single mutex, which costs nothing because these three commands are administrative and
// rare. Ordinary single-database operations cannot participate in such a cycle at all,
// since they never hold locks in two stores.

// TransferKey moves or copies key from src to dst, both of which must be different
// stores with the same shard count.
//
// remove says whether the key is taken out of src (MOVE) or left in place (COPY).
// replace says whether an existing key in dst may be overwritten. It reports false
// without touching either store when src has no live key, or when dst already has one
// and replace is false -- the 0 reply both MOVE and COPY give.
//
// The two stores' shards for a key have the same index, since the key and the shard
// count are the same, so exactly two locks are taken and the whole transfer is atomic
// with respect to every other operation on either key.
func TransferKey(src, dst *Store, key string, remove, replace bool) bool {
	now := src.clock()
	ssh, dsh := src.getShard(key), dst.getShard(key)
	ssh.mu.Lock()
	defer ssh.mu.Unlock()
	dsh.mu.Lock()
	defer dsh.mu.Unlock()

	e := ssh.liveEntry(key, now)
	if e == nil {
		return false
	}
	if !replace && dsh.liveEntry(key, now) != nil {
		return false
	}
	// The value is cloned even for a move: an entry carries an atomic access time that
	// must not be shared between two databases, and a move that handed the pointer over
	// would leave the two keyspaces aliasing one value if the move were ever made
	// non-destructive.
	ne := e.clone()
	dst.touch(ne, now)
	dsh.data[key] = ne
	if remove {
		delete(ssh.data, key)
	}
	return true
}

// AdoptKey takes key out of src and installs it in dst, reporting whether it did.
// replace says whether a live key already in dst may be overwritten; without it the
// call reports false and moves nothing, which is RESTORE's BUSYKEY reply.
//
// It is how RESTORE commits. A payload is rebuilt command by command in a scratch
// store that no client can see, and only the finished value is published here, so a
// half-constructed key is never observable and a payload that fails partway through
// leaves the real keyspace untouched. That is also why the entry is handed over by
// pointer rather than cloned: src is discarded immediately afterwards, so there is no
// second referent to alias.
//
// The staged entry is published whether or not its deadline has already passed. A
// RESTORE carrying a deadline in the past is not an error -- Redis accepts it, and the
// key it creates is simply expired on arrival -- and treating "expired" as "nothing to
// move" would report that failure as the destination being occupied, which is a
// different thing entirely. It also matters for REPLACE: the value already there must
// be gone either way, and a caller that skipped the handover would leave it standing.
//
// src must be private to the caller -- a scratch store, never another database. That
// is what makes taking its lock second safe whatever the two stores' shard geometry:
// no other goroutine can hold it, so this cannot be one half of a cycle. TransferKey,
// which moves a key between two *live* databases, is the one that needs the server's
// crossDBMu for exactly that reason.
func AdoptKey(dst, src *Store, key string, replace bool) bool {
	now := dst.clock()
	dsh, ssh := dst.getShard(key), src.getShard(key)
	dsh.mu.Lock()
	defer dsh.mu.Unlock()
	ssh.mu.Lock()
	defer ssh.mu.Unlock()

	e, staged := ssh.data[key]
	if !staged {
		return false
	}
	if !replace && dsh.liveEntry(key, now) != nil {
		return false
	}
	delete(ssh.data, key)
	dst.touch(e, now)
	dsh.data[key] = e
	return true
}

// SwapData exchanges the entire contents of two stores, as SWAPDB does. Both must have
// the same shard count.
//
// Shard contents are swapped one shard pair at a time rather than under one giant
// critical section. That is enough for SWAPDB's contract -- a client is told the
// databases have been exchanged, not that some other client observed the exchange as a
// single instant -- and it means the operation never holds more than two locks, so a
// swap of a large dataset does not stall every shard of both databases at once.
//
// A concurrent reader of either store therefore sees, for each key, either the value
// from before the swap or the one from after it, never a torn value: the map pointer is
// replaced under the same lock every reader takes.
func SwapData(a, b *Store) {
	for i := range a.shards {
		sa, sb := a.shards[i], b.shards[i]
		sa.mu.Lock()
		sb.mu.Lock()
		sa.data, sb.data = sb.data, sa.data
		sb.mu.Unlock()
		sa.mu.Unlock()
	}
}

// LenWithExpires returns the number of live keys and how many of them have a deadline,
// which is what INFO's per-database keyspace line reports. Both come out of one pass,
// so reporting them costs no more than reporting the count alone did.
func (s *Store) LenWithExpires() (keys, expires int) {
	now := s.clock()
	for _, sh := range s.shards {
		sh.mu.RLock()
		for _, e := range sh.data {
			if e.expired(now) {
				continue
			}
			keys++
			if !e.expireAt.IsZero() {
				expires++
			}
		}
		sh.mu.RUnlock()
	}
	return keys, expires
}
