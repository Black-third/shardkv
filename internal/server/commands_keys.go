package server

import (
	"strconv"
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
	// The whole expire family propagates the absolute PEXPIREAT it decided on rather than
	// its own text: EXPIRE's and PEXPIRE's operands are relative to the instant the
	// handler resolved them, and all four carry NX/XX/GT/LT flags a replica must not
	// re-evaluate. See propagation.go.
	registerEffect("EXPIRE", -3, cmdExpire)
	registerEffect("PEXPIRE", -3, cmdPExpire)
	registerEffect("EXPIREAT", -3, cmdExpireAt)
	registerEffect("PEXPIREAT", -3, cmdPExpireAt)
	register("PERSIST", 2, true, cmdPersist)
	register("TTL", 2, false, cmdTTL)
	register("PTTL", 2, false, cmdPTTL)
	register("EXPIRETIME", 2, false, cmdExpireTime)
	register("PEXPIRETIME", 2, false, cmdPExpireTime)
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
// milliseconds and rel says whether it is relative to now; name only shapes the error
// reply. It takes one reading of the store's clock and hands the absolute deadline it
// resolved to both the store and the PEXPIREAT it returns as its effect, so the instant
// in memory and the instant on the wire are the same value rather than two computations
// of it. (For the already-absolute forms the reading goes unused -- deadlineMs ignores it
// when rel is false -- which is why EXPIREAT and PEXPIREAT could never skew.)
//
// A condition that rejects the new deadline replies 0 and produces no effect, so nothing
// is propagated: the flags are evaluated once, on the master, and the PEXPIREAT that
// ships carries no flag for a replica to re-evaluate.
func expire(s *Server, w *resp.Writer, args [][]byte, name string, unitMs int64, rel bool) [][][]byte {
	n, ok := parseInt64(args[2])
	if !ok {
		w.WriteError("ERR value is not an integer or out of range")
		return nil
	}
	cond, errMsg := parseExpireCond(args)
	if errMsg != "" {
		w.WriteError(errMsg)
		return nil
	}
	atMs, ok := deadlineMs(s.store.Now().UnixMilli(), n, unitMs, rel)
	if !ok {
		w.WriteError("ERR invalid expire time in '" + name + "' command")
		return nil
	}
	if s.store.ExpireAtCond(string(args[1]), time.UnixMilli(atMs), cond) {
		w.WriteInt(1)
		return [][][]byte{pexpireatForm(args[1], atMs)}
	}
	w.WriteInt(0)
	return nil
}

func cmdExpire(s *Server, w *resp.Writer, args [][]byte) [][][]byte {
	return expire(s, w, args, "expire", 1000, true)
}

func cmdPExpire(s *Server, w *resp.Writer, args [][]byte) [][][]byte {
	return expire(s, w, args, "pexpire", 1, true)
}

func cmdExpireAt(s *Server, w *resp.Writer, args [][]byte) [][][]byte {
	return expire(s, w, args, "expireat", 1000, false)
}

func cmdPExpireAt(s *Server, w *resp.Writer, args [][]byte) [][][]byte {
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
	w.WriteBulkString(k)
	return false
}

// sharedIntegerRefcount is what OBJECT REFCOUNT answers for a value Redis would have
// taken from its shared-integer table. Redis preallocates the integers 0..9999 and hands
// out the same object to every key holding one, so their refcount is meaningless and it
// reports INT_MAX rather than a number that would change as unrelated keys came and went.
//
// shardkv shares nothing -- each key owns its bytes -- so this is reported for
// compatibility rather than because anything is shared. It is not cosmetic: a client that
// uses REFCOUNT to decide whether a value is shared (which is the only thing the field is
// good for) would otherwise conclude that an integer key is private when on real Redis it
// is not.
const sharedIntegerRefcount = 2147483647

const sharedIntegerMax = 9999

// cmdObject implements OBJECT ENCODING|REFCOUNT|IDLETIME|FREQ|HELP key.
//
// A missing key is a **null**, not an error. That is measured, not assumed: real Redis
// resolves the key with objectCommandLookupOrReply, whose failure reply is
// shared.null[resp], so `OBJECT ENCODING nosuchkey` answers $-1 in RESP2 and _ in RESP3.
// Answering "ERR no such key" instead turns a routine "is this key here?" probe into an
// error a client library may raise as an exception.
func cmdObject(s *Server, w *resp.Writer, args [][]byte) bool {
	sub := strings.ToUpper(string(args[1]))
	if sub == "HELP" && len(args) == 2 {
		writeSubcommandHelp(w, "OBJECT", []string{
			"ENCODING <key>",
			"    Return the kind of internal representation used in order to store the value",
			"    associated with a <key>.",
			"FREQ <key>",
			"    Return the access frequency index of the <key>. The returned integer is",
			"    proportional to the logarithm of the recent access frequency of the key.",
			"IDLETIME <key>",
			"    Return the idle time of the <key>, that is the approximated number of",
			"    seconds elapsed since the last access to the key.",
			"REFCOUNT <key>",
			"    Return the number of references of the value associated with the specified",
			"    <key>.",
		})
		return false
	}
	switch sub {
	case "ENCODING", "REFCOUNT", "IDLETIME", "FREQ":
		// A *known* subcommand given the wrong number of arguments is an arity error
		// naming the subcommand -- "ERR wrong number of arguments for 'object|encoding'
		// command" -- not an unknown-subcommand error. Collapsing the two told a client
		// its subcommand did not exist when it had merely forgotten the key.
		if len(args) != 3 {
			w.WriteError("ERR wrong number of arguments for 'object|" +
				strings.ToLower(sub) + "' command")
			return false
		}
	default:
		writeUnknownSubcommand(w, "OBJECT", args[1])
		return false
	}
	key := string(args[2])
	switch sub {
	case "ENCODING":
		enc, ok := s.store.Encoding(key)
		if !ok {
			w.WriteNull()
			return false
		}
		w.WriteBulkString(enc)
	case "REFCOUNT":
		raw, ok, err := s.store.GetString(key)
		if err != nil || !ok {
			// err is WRONGTYPE: a live key of some other type, which shares nothing, so
			// one holder. Not a live string and no error means the key is absent.
			if err == nil && !s.store.Exists(key) {
				w.WriteNull()
				return false
			}
			w.WriteInt(1)
			return false
		}
		v := string(raw)
		if n, parseErr := strconv.ParseInt(v, 10, 64); parseErr == nil && n >= 0 && n <= sharedIntegerMax &&
			v == strconv.FormatInt(n, 10) {
			// The last clause matters: "007" parses as 7 but is not the text a shared
			// integer object would hold, and Redis stores it as a string.
			w.WriteInt(sharedIntegerRefcount)
			return false
		}
		w.WriteInt(1)
	case "IDLETIME":
		// The mirror of FREQ below, and measured the same way: under an LFU policy Redis
		// refuses IDLETIME, because the field that would hold an access time is holding the
		// access counter instead. Answering 0 would be a number an operator could read as
		// "just used".
		switch s.evictionPolicy() {
		case store.PolicyAllKeysLFU, store.PolicyVolatileLFU:
			if !s.store.Exists(key) {
				w.WriteNull()
				return false
			}
			w.WriteError("ERR An LFU maxmemory policy is selected, idle time not tracked. " +
				"Please note that when switching between policies at runtime LRU and LFU " +
				"data will take some time to adjust.")
			return false
		}
		idle, ok := s.store.IdleSeconds(key)
		if !ok {
			w.WriteNull()
			return false
		}
		w.WriteInt(idle)
	case "FREQ":
		// Redis reports access frequency only under an LFU maxmemory policy, and answers the
		// refusal below otherwise. Both halves are real here: the LFU policies maintain a
		// decaying access counter per key (see internal/store/evict.go), so under one of them
		// this reports the counter the sampler would rank by, and under any other policy
		// nothing is tracked and there is no number to report. Answering "unknown
		// subcommand" instead would tell a client the command does not exist, when the truth
		// is that the policy for it is not selected.
		if !s.store.Exists(key) {
			w.WriteNull()
			return false
		}
		switch s.evictionPolicy() {
		case store.PolicyAllKeysLFU, store.PolicyVolatileLFU:
			freq, ok := s.store.Freq(key)
			if !ok {
				w.WriteNull()
				return false
			}
			w.WriteInt(freq)
			return false
		}
		w.WriteError("ERR An LFU maxmemory policy is not selected, access frequency not " +
			"tracked. Please note that when switching between policies at runtime LRU and LFU " +
			"data will take some time to adjust.")
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

// cmdExpireTime and cmdPExpireTime report the *absolute* instant a key expires at, in
// seconds and in milliseconds, with TTL's two sentinels: -1 for a key with no expiry and
// -2 for one that is not there.
//
// The seconds form truncates rather than rounding, which is where it differs from TTL:
// TTL reports a duration and rounds to the nearest second so that a 999ms TTL does not
// read as 0, while EXPIRETIME reports a point in time, and the second a deadline falls
// in is the one it is inside -- rounding it up would name an instant after the key is
// already gone.
func cmdExpireTime(s *Server, w *resp.Writer, args [][]byte) bool {
	return expireTime(s, w, args, true)
}

func cmdPExpireTime(s *Server, w *resp.Writer, args [][]byte) bool {
	return expireTime(s, w, args, false)
}

func expireTime(s *Server, w *resp.Writer, args [][]byte, seconds bool) bool {
	ms, hasTTL, ok := s.store.ExpireTimeMillis(string(args[1]))
	switch {
	case !ok:
		w.WriteInt(-2)
	case !hasTTL:
		w.WriteInt(-1)
	case seconds:
		w.WriteInt(ms / 1000)
	default:
		w.WriteInt(ms)
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
	// A lone "*" bypasses the matcher entirely, as Redis's `allkeys` does. It is not only a
	// shortcut: the matcher refuses an empty subject (see globMatch), so without this an
	// empty key name -- which SET accepts, here and in Redis -- would be missing from
	// `KEYS *`. Measured: redis:7.2 lists it for `KEYS *` and not for `KEYS **`.
	allKeys := pattern == "*"
	matched := keys[:0]
	for _, k := range keys {
		if allKeys || globMatch(pattern, k) {
			matched = append(matched, k)
		}
	}
	w.WriteArrayHeader(len(matched))
	for _, k := range matched {
		w.WriteBulkString(k)
	}
	return false
}
