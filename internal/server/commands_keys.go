package server

import (
	"strings"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

func init() {
	register("DEL", -2, true, cmdDel)
	register("UNLINK", -2, true, cmdDel) // no lazy free here: DEL already unlinks
	register("EXISTS", -2, false, cmdExists)
	register("TOUCH", -2, false, cmdTouch)
	register("EXPIRE", -3, true, cmdExpire)
	register("PEXPIRE", -3, true, cmdPExpire)
	register("EXPIREAT", -3, true, cmdExpireAt)
	register("PEXPIREAT", -3, true, cmdPExpireAt)
	register("PERSIST", 2, true, cmdPersist)
	register("TTL", 2, false, cmdTTL)
	register("PTTL", 2, false, cmdPTTL)
	register("TYPE", 2, false, cmdType)
	register("KEYS", 2, false, cmdKeys)
	register("RENAME", 3, true, cmdRename)
	register("RENAMENX", 3, true, cmdRenameNX)
	register("COPY", -3, true, cmdCopy)
	register("RANDOMKEY", 1, false, cmdRandomKey)
	register("OBJECT", -2, false, cmdObject)
}

// parseExpireCond parses the optional NX/XX/GT/LT tail of the expire family,
// returning the RESP error message to reply with or "" when the flags are valid.
// The flags combine (XX GT only shortens an existing TTL); only NX with any other
// and GT with LT are incompatible.
func parseExpireCond(args [][]byte) (store.ExpireCond, string) {
	cond := store.ExpireAlways
	for _, a := range args[3:] {
		switch strings.ToUpper(string(a)) {
		case "NX":
			cond |= store.ExpireNX
		case "XX":
			cond |= store.ExpireXX
		case "GT":
			cond |= store.ExpireGT
		case "LT":
			cond |= store.ExpireLT
		default:
			return cond, "ERR Unsupported option " + string(a)
		}
	}
	if cond&store.ExpireNX != 0 && cond&(store.ExpireXX|store.ExpireGT|store.ExpireLT) != 0 {
		return cond, "ERR NX and XX, GT or LT options at the same time are not compatible"
	}
	if cond&store.ExpireGT != 0 && cond&store.ExpireLT != 0 {
		return cond, "ERR GT and LT options at the same time are not compatible"
	}
	return cond, ""
}

// expire applies one of the four expire commands. unitMs is the operand's unit in
// milliseconds and rel says whether it is relative to now; name only shapes the
// error reply. The deadline is computed from the store's clock -- the same clock
// propagationForm rewrites the command against -- so the instant written to memory
// and the instant shipped to the AOF and replicas are identical.
//
// A condition that rejects the new deadline replies 0 and is not dirty, so nothing
// is propagated: the flags are evaluated once, on the master, and the absolute
// PEXPIREAT that ships carries no flag for a replica to re-evaluate.
func expire(s *Server, w *resp.Writer, args [][]byte, name string, unitMs int64, rel bool) bool {
	n, ok := parseInt64(args[2])
	if !ok {
		w.WriteError("ERR value is not an integer or out of range")
		return false
	}
	cond, errMsg := parseExpireCond(args)
	if errMsg != "" {
		w.WriteError(errMsg)
		return false
	}
	atMs, ok := deadlineMs(s.store.Now().UnixMilli(), n, unitMs, rel)
	if !ok {
		w.WriteError("ERR invalid expire time in '" + name + "' command")
		return false
	}
	if s.store.ExpireAtCond(string(args[1]), time.UnixMilli(atMs), cond) {
		w.WriteInt(1)
		return true
	}
	w.WriteInt(0)
	return false
}

func cmdExpire(s *Server, w *resp.Writer, args [][]byte) bool {
	return expire(s, w, args, "expire", 1000, true)
}

func cmdPExpire(s *Server, w *resp.Writer, args [][]byte) bool {
	return expire(s, w, args, "pexpire", 1, true)
}

func cmdExpireAt(s *Server, w *resp.Writer, args [][]byte) bool {
	return expire(s, w, args, "expireat", 1000, false)
}

func cmdPExpireAt(s *Server, w *resp.Writer, args [][]byte) bool {
	return expire(s, w, args, "pexpireat", 1, false)
}

func cmdPersist(s *Server, w *resp.Writer, args [][]byte) bool {
	if s.store.Persist(string(args[1])) {
		w.WriteInt(1)
		return true
	}
	w.WriteInt(0)
	return false
}

func cmdPTTL(s *Server, w *resp.Writer, args [][]byte) bool {
	ms, hasTTL, ok := s.store.TTLMillis(string(args[1]))
	switch {
	case !ok:
		w.WriteInt(-2)
	case !hasTTL:
		w.WriteInt(-1)
	default:
		w.WriteInt(ms)
	}
	return false
}

func cmdRename(s *Server, w *resp.Writer, args [][]byte) bool {
	if s.store.Rename(string(args[1]), string(args[2])) {
		w.WriteSimple("OK")
		return true
	}
	w.WriteError("ERR no such key")
	return false
}

func cmdDel(s *Server, w *resp.Writer, args [][]byte) bool {
	count := 0
	for _, k := range args[1:] {
		if s.store.Del(string(k)) {
			count++
		}
	}
	w.WriteInt(int64(count))
	return count > 0
}

func cmdExists(s *Server, w *resp.Writer, args [][]byte) bool {
	count := 0
	for _, k := range args[1:] {
		if s.store.Exists(string(k)) {
			count++
		}
	}
	w.WriteInt(int64(count))
	return false
}

// cmdTouch reports how many of the given keys exist, refreshing each one's
// last-access time. Like Redis it is a read command: it alters only the eviction
// bookkeeping, nothing a replica or an AOF replay needs to reconstruct.
func cmdTouch(s *Server, w *resp.Writer, args [][]byte) bool {
	count := 0
	for _, k := range args[1:] {
		if s.store.Touch(string(k)) {
			count++
		}
	}
	w.WriteInt(int64(count))
	return false
}

func cmdRenameNX(s *Server, w *resp.Writer, args [][]byte) bool {
	renamed, found := s.store.RenameNX(string(args[1]), string(args[2]))
	if !found {
		w.WriteError("ERR no such key")
		return false
	}
	if renamed {
		w.WriteInt(1)
	} else {
		w.WriteInt(0)
	}
	return renamed
}

// cmdCopy implements COPY src dst [DB index] [REPLACE].
//
// With a DB option naming another database the copy crosses two keyspaces, which is
// serialized against the other cross-database commands by crossDBMu (see cmdSwapDB for
// why). Copying to the same key name in the caller's own database is the one case DB can
// express that Redis rejects, since src and dst would then be the same object.
func cmdCopy(s *Server, w *resp.Writer, args [][]byte) bool {
	replace := false
	destDB := s.db
	for i := 3; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "REPLACE":
			replace = true
		case "DB":
			if i+1 >= len(args) {
				w.WriteError("ERR syntax error")
				return false
			}
			n, ok := parseInt64(args[i+1])
			if !ok {
				w.WriteError("ERR value is not an integer or out of range")
				return false
			}
			// A copy into another database has nowhere to land in cluster mode, which has
			// only database 0. Redis reports it distinctly from an out-of-range index, since
			// the index is not what is wrong.
			if n != 0 && s.ClusterEnabled() {
				w.WriteError("ERR Copying to another database is not allowed in cluster mode")
				return false
			}
			if n < 0 || n >= int64(s.Databases()) {
				w.WriteError("ERR DB index is out of range")
				return false
			}
			destDB = int(n)
			i++
		default:
			w.WriteError("ERR syntax error")
			return false
		}
	}
	src, dst := string(args[1]), string(args[2])
	if destDB != s.db {
		if src != dst {
			// Redis's COPY copies to dst in the destination database; a different key name
			// there is not expressible by TransferKey, which works on one name.
			return copyAcrossDatabases(s, w, src, dst, destDB, replace)
		}
		s.crossDBMu.Lock()
		copied := store.TransferKey(s.store, s.dbs[destDB], src, false, replace)
		s.crossDBMu.Unlock()
		w.WriteInt(int64(boolToInt(copied)))
		return copied
	}
	copied, err := s.store.Copy(src, dst, replace)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(boolToInt(copied)))
	return copied
}

// copyAcrossDatabases handles COPY src dst DB n where src and dst are different names:
// the value is copied within the caller's database under a temporary-free two-step that
// still holds crossDBMu, then moved across under the destination name.
//
// It is expressed as a copy-then-transfer rather than as another store primitive because
// the primitive would need to know two key names in two stores, and the two shards it
// would lock are then not the same index -- a second lock-ordering rule for one command
// that is already the rarest form of the rarest family.
func copyAcrossDatabases(s *Server, w *resp.Writer, src, dst string, destDB int, replace bool) bool {
	s.crossDBMu.Lock()
	defer s.crossDBMu.Unlock()

	dstStore := s.dbs[destDB]
	if !replace && dstStore.Exists(dst) {
		w.WriteInt(0)
		return false
	}
	// Stage the value under the destination *name* in the source database, hand it over,
	// and put back whatever was standing there. The staging key is the destination name,
	// so nothing else has to be invented and no key that a client could have chosen is
	// disturbed beyond what COPY already touches.
	staged, err := s.store.Copy(src, dst, true)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	if !staged {
		w.WriteInt(0)
		return false
	}
	moved := store.TransferKey(s.store, dstStore, dst, true, replace)
	w.WriteInt(int64(boolToInt(moved)))
	return moved
}

// cmdRandomKey returns an arbitrary live key. It is a read command, so the pick
// never reaches a replica -- which matters, because a replica running RANDOMKEY
// itself would answer with a different key.
func cmdRandomKey(s *Server, w *resp.Writer, args [][]byte) bool {
	k, ok := s.store.RandomKey()
	if !ok {
		w.WriteNull()
		return false
	}
	w.WriteBulk([]byte(k))
	return false
}

// cmdObject implements OBJECT ENCODING|REFCOUNT|IDLETIME key.
//
// REFCOUNT always answers 1: values here are never shared between keys (COPY makes
// a deep copy), so the only honest answer is that one key holds the value.
func cmdObject(s *Server, w *resp.Writer, args [][]byte) bool {
	sub := strings.ToUpper(string(args[1]))
	if len(args) != 3 || (sub != "ENCODING" && sub != "REFCOUNT" && sub != "IDLETIME") {
		w.WriteError("ERR Unknown subcommand or wrong number of arguments for '" +
			string(args[1]) + "'. Try OBJECT HELP.")
		return false
	}
	key := string(args[2])
	switch sub {
	case "ENCODING":
		enc, ok := s.store.Encoding(key)
		if !ok {
			w.WriteError("ERR no such key")
			return false
		}
		w.WriteBulk([]byte(enc))
	case "REFCOUNT":
		if !s.store.Exists(key) {
			w.WriteError("ERR no such key")
			return false
		}
		w.WriteInt(1)
	case "IDLETIME":
		idle, ok := s.store.IdleSeconds(key)
		if !ok {
			w.WriteError("ERR no such key")
			return false
		}
		w.WriteInt(idle)
	}
	return false
}

func cmdTTL(s *Server, w *resp.Writer, args [][]byte) bool {
	ms, hasTTL, ok := s.store.TTLMillis(string(args[1]))
	switch {
	case !ok:
		w.WriteInt(-2) // no such key
	case !hasTTL:
		w.WriteInt(-1) // exists, no expiry
	default:
		// Round to the nearest second, as Redis does, rather than truncating.
		w.WriteInt((ms + 500) / 1000)
	}
	return false
}

func cmdType(s *Server, w *resp.Writer, args [][]byte) bool {
	typ, _ := s.store.Type(string(args[1]))
	w.WriteSimple(typ)
	return false
}

// cmdKeys implements KEYS pattern, returning every live key matching the glob.
// It is O(keyspace) and takes each shard's lock in turn; SCAN is the incremental
// alternative.
func cmdKeys(s *Server, w *resp.Writer, args [][]byte) bool {
	pattern := string(args[1])
	keys := s.store.Keys()
	matched := keys[:0]
	for _, k := range keys {
		if globMatch(pattern, k) {
			matched = append(matched, k)
		}
	}
	w.WriteArrayHeader(len(matched))
	for _, k := range matched {
		w.WriteBulk([]byte(k))
	}
	return false
}
