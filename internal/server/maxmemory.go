package server

// The byte budget: what used_memory is compared against, who is allowed to evict, and
// what happens when nothing can be.
//
// # Where the decision belongs
//
// maxmemory is server-wide and a store is one database, so the *comparison* is made here
// and the accounting lives there (internal/store/memtrack.go). The server sums the
// databases' totals, decides whether to evict and from which database, and decides whether
// to refuse the command; each store owns its byte count and its choice of victim.
//
// The number itself is held by the stores, one copy each, written to all of them and read
// back from database 0 -- the discipline `maxkeys` and the encoding thresholds already
// follow. That is deliberately not a server-side field shadowing a store-side one: a
// server-wide limit kept in two places is two spellings of one fact, and the copy that
// drifted would be the one enforcement reads.
//
// # Where the check sits
//
// On the client path, in executeCommand, beside the cluster redirect and for the same
// reasons. It happens before anything runs and before anything is queued, because a MULTI
// must not accumulate commands the server has already decided it cannot afford; and it is
// *not* on dispatch, so an AOF replay and a master's stream apply every write they are
// given whatever this server's limit says. A replica that evicted on its own would hold a
// different dataset from its master with nothing to report it -- Redis draws the same line
// (replica-ignore-maxmemory, on by default), and the master's eviction arrives as the DEL
// it propagated.
//
// The whole feature is one atomic load when no budget is configured, which is every
// existing deployment: the budget is read first and a zero ends the check before any
// database is consulted, before any name is compared, and before anything is allocated.
//
// # Refusing is a feature, not a failure
//
// Two states end in a refusal rather than an eviction: noeviction, and a volatile-* policy
// over a keyspace with no volatile keys. Both must refuse -- the second especially, because
// "keep looking for a candidate" is an infinite loop with a client waiting on it -- and
// both must refuse *only the commands that could grow the dataset*. A server that refused
// reads would be unmonitorable, and one that refused DEL would be unrecoverable: deleting
// something is the operator's only way out of a full keyspace. That is what oomDenyOOM
// records, and it is Redis's own denyoom flag as measured from COMMAND INFO on redis 7.2
// rather than a reconstruction of the rule behind it.

import (
	"strconv"
	"strings"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

// errOOM is Redis's refusal, byte for byte. Clients match on the OOM prefix -- several
// libraries retry, fail over, or surface a distinct exception on it -- so the wording is
// not ours to improve.
const errOOM = "OOM command not allowed when used memory > 'maxmemory'."

// SetMaxMemory sets the byte budget the whole dataset is held within (0 = unbounded).
//
// Turning a budget on re-derives every database's byte total from its values, so the first
// comparison is made against a number computed from the dataset rather than against
// whatever the counter had accumulated. That matters at exactly one moment and it is this
// one: it is when an operator will first look at used_memory and expect it to mean
// something.
func (s *Server) SetMaxMemory(n int64) {
	if n < 0 {
		n = 0
	}
	for _, db := range s.dbs {
		db.SetMaxMemory(n)
	}
}

// MaxMemory reports the configured budget, for CONFIG GET and INFO. Read from database 0,
// which is where SetMaxMemory's value is always in force because it writes all of them.
func (s *Server) MaxMemory() int64 { return s.dbs[0].MaxMemory() }

// SetEvictionPolicy selects what happens when the budget is reached.
func (s *Server) SetEvictionPolicy(p store.EvictionPolicy) {
	for _, db := range s.dbs {
		db.SetEvictionPolicy(p)
	}
}

// evictionPolicy is the policy in force, which is the configured one unless nothing has
// configured it.
//
// The default is *derived*, and this is the one place that decides it: a server with a
// maxkeys cap and no policy of its own evicts by approximate LRU over all keys, which is
// allkeys-lru; a server with neither evicts nothing, which is noeviction. Deriving it
// rather than defaulting flatly to noeviction is what keeps `maxmemory-policy` a true
// description of what this server does when it is full, which is what it was before the
// policy became settable -- and an explicit CONFIG SET still wins, so the parameter never
// reads back as something other than what it was set to.
func (s *Server) evictionPolicy() store.EvictionPolicy {
	db0 := s.dbs[0]
	if db0.EvictionPolicyConfigured() {
		return db0.EvictionPolicy()
	}
	if db0.MaxKeys() > 0 {
		return store.PolicyAllKeysLRU
	}
	return store.PolicyNoEviction
}

// maxmemoryPolicy is the name INFO's maxmemory_policy and CONFIG GET maxmemory-policy both
// report. One accessor, deliberately: two spellings of one fact drift, and a client that
// compared them would find the server disagreeing with itself.
func (s *Server) maxmemoryPolicy() string { return s.evictionPolicy().String() }

// UsedMemory is what the budget is compared against and what INFO reports as used_memory:
// the dataset's size, summed over every database. See internal/store/memtrack.go for
// exactly what it counts and what it does not.
//
// Summed over the databases because the budget is server-wide -- a limit each of sixteen
// keyspaces enforced separately would be sixteen times the limit an operator set, which is
// the kind of number that gets a container OOM-killed while every reading looks fine.
func (s *Server) UsedMemory() int64 {
	var total int64
	for _, db := range s.dbs {
		total += db.UsedMemory()
	}
	return total
}

// EvictedKeys is the lifetime count of keys removed by the eviction policy, over every
// database, for INFO's evicted_keys. Summed for the same reason keyspace_hits is: a figure
// for one keyspace out of sixteen is not the number anyone is looking at.
func (s *Server) EvictedKeys() int64 {
	var total int64
	for _, db := range s.dbs {
		total += db.Evicted()
	}
	return total
}

// oomGate is the enforcement. It reports whether the command may proceed, having written
// the refusal if not.
//
// The order of the tests is the point. The budget is read first, so a server without one
// pays a single atomic load; the total is compared next, so a server under its budget pays
// one atomic load per database and nothing else; only a server actually over its budget
// reaches the eviction loop, the role check, the command-name comparison, or any
// allocation.
func (s *Server) oomGate(w *resp.Writer, cmd *command, sess *session) bool {
	max := s.MaxMemory()
	if max == 0 {
		return true
	}
	if s.UsedMemory() <= max {
		return true
	}
	// A replica does not evict: its master does, and ships the DEL. Evicting here as well
	// would remove keys the master still holds -- a divergence with nothing to report it,
	// and the failure shape every invariant in this codebase is written against.
	if s.isReplica() {
		return true
	}
	if s.freeMemory(max) {
		return true
	}
	// Still over the limit, and nothing left the policy is allowed to remove.
	//
	// Inside a MULTI *every* command is refused, read or not, and the transaction is marked
	// so EXEC aborts with EXECABORT. That is measured, not invented: queuing is itself
	// unbounded memory growth, so Redis refuses anything but EXEC/DISCARD/QUIT/RESET while
	// a transaction is open and over the limit -- `GET` included. (Those four are
	// connection-control commands here and have already run by the time this is reached,
	// which is what leaves an operator a way to abandon the batch.)
	if sess != nil && sess.inMulti {
		sess.queueErr = true
		w.WriteError(errOOM)
		return false
	}
	// Outside a transaction only the commands that could grow the dataset are refused;
	// reads, and the deletions an operator needs in order to recover, go through.
	if cmd == nil {
		return true
	}
	// A write nobody has classified is refused rather than allowed. The completeness test
	// makes that state unreachable, and this is what makes reaching it harmless: an
	// unclassified command may well be one that grows the dataset, and letting it through
	// would be a silent hole in the budget, which is the failure this whole gate exists to
	// prevent. Being refused is visible, reported to the client, and recoverable.
	if cmd.write && !oomClassified(cmd.lowerName) {
		w.WriteError(errOOM)
		return false
	}
	if !oomDenied(cmd.lowerName) {
		return true
	}
	w.WriteError(errOOM)
	return false
}

// freeMemory evicts until the dataset is within the budget, and reports whether it got
// there. false means the policy has nothing left it may evict -- noeviction from the
// start, or a volatile-* policy over a keyspace with no volatile keys -- which is the
// caller's cue to refuse rather than to try again.
//
// It evicts from whichever database is currently largest, so a budget shared by sixteen
// keyspaces is met by taking from the one that is actually using it rather than by draining
// database 0. Each removal fires that database's removal hook, which is what propagates the
// DEL to the AOF and the replicas -- so an eviction is ordered, persisted and replicated
// exactly like any other write, and a replica's copy of the dataset follows its master's
// choices rather than making its own.
//
// That hook takes propMu, which is why this runs before the command does rather than inside
// runWrite: propMu is not reentrant, and a write holds it across both its mutation and its
// propagation. Running here also gives the stream the order it needs -- every DEL for an
// evicted key precedes the command that made room for.
func (s *Server) freeMemory(max int64) bool {
	policy := s.evictionPolicy()
	if !policy.Evicts() {
		return false
	}
	for s.UsedMemory() > max {
		db := s.largestDB()
		if db == nil || !db.EvictOne(policy) {
			return false
		}
	}
	return true
}

// largestDB is the database holding the most bytes, or nil when every database is empty.
func (s *Server) largestDB() *store.Store {
	var best *store.Store
	var most int64
	for _, db := range s.dbs {
		if n := db.UsedMemory(); n > most {
			best, most = db, n
		}
	}
	return best
}

// oomDenied reports whether a command is refused when the server is over its byte budget
// and cannot evict. It is Redis's denyoom flag, and it is the *measured* flag rather than a
// reconstruction of the rule behind it: every name below was read out of
// `COMMAND INFO <name>` on redis 7.2, because the rule has edges that are not guessable.
// LSET is denyoom (it can store a longer element) while SMOVE is not; GETEX is not (it only
// moves a deadline) while SETEX is; SORT is while SORT_RO is not.
//
// The table records both answers explicitly, and that is the point. A map of only the
// refused names cannot tell "allowed" from "nobody has classified it yet", so a new write
// command would silently escape the budget -- exactly the failure mode this codebase is
// written against. With both recorded, TestEveryWriteIsClassifiedForOOM can insist that
// every write in the command table appears here, so a new command fails a test instead of
// quietly growing an unbounded keyspace.
func oomDenied(lowerName string) bool { return oomDenyOOM[lowerName] }

// oomClassified reports whether a command has been classified at all, which is what the
// completeness test checks.
func oomClassified(lowerName string) bool { _, ok := oomDenyOOM[lowerName]; return ok }

// oomDenyOOM is `denyoom` as redis 7.2 reports it, for every command this server registers
// that writes. true means refused under memory pressure, false means allowed.
//
// SUBSCRIBE and PSUBSCRIBE carry the flag in Redis too, and are deliberately absent: they
// are connection-control commands here, so they have already run by the time the gate is
// reached. That divergence is recorded rather than papered over -- a subscription's buffers
// are not part of the dataset this budget measures anyway (see memtrack.go on what
// used_memory excludes).
var oomDenyOOM = map[string]bool{
	// --- refused: every command that can make the dataset larger --------------
	"append": true, "bitfield": true, "bitop": true, "blmove": true, "brpoplpush": true,
	"copy": true, "decr": true, "decrby": true, "geoadd": true, "georadius": true,
	"georadiusbymember": true, "geosearchstore": true, "getset": true, "hincrby": true,
	"hincrbyfloat": true, "hmset": true, "hset": true, "hsetnx": true, "incr": true,
	"incrby": true, "incrbyfloat": true, "linsert": true, "lmove": true, "lpush": true,
	"lpushx": true, "lset": true, "mset": true, "msetnx": true, "pfadd": true,
	"pfmerge": true, "psetex": true, "restore": true, "rpoplpush": true, "rpush": true,
	"rpushx": true, "sadd": true, "sdiffstore": true, "set": true, "setbit": true,
	"setex": true, "setnx": true, "setrange": true, "sinterstore": true, "sort": true,
	"sunionstore": true, "xadd": true, "xsetid": true, "zadd": true, "zdiffstore": true,
	"zincrby": true, "zinterstore": true, "zrangestore": true, "zunionstore": true,

	// --- allowed: the writes that only ever shrink the dataset, move a value, or
	// move a deadline. These are what make the refusal recoverable rather than
	// terminal: an operator with a full keyspace gets out with DEL, EXPIRE and the
	// pops, and a monitoring agent keeps reading.
	"del": false, "unlink": false, "getdel": false, "getex": false,
	"expire": false, "pexpire": false, "expireat": false, "pexpireat": false,
	"persist":  false,
	"flushall": false, "flushdb": false, "swapdb": false, "move": false,
	"rename": false, "renamenx": false, "migrate": false,
	"lpop": false, "rpop": false, "lrem": false, "ltrim": false, "lmpop": false,
	"blpop": false, "brpop": false, "blmpop": false,
	"srem": false, "spop": false, "smove": false,
	"hdel": false,
	"zrem": false, "zremrangebyscore": false, "zremrangebyrank": false,
	"zremrangebylex": false, "zpopmin": false, "zpopmax": false, "zmpop": false,
	"bzpopmin": false, "bzpopmax": false, "bzmpop": false,
	"xdel": false, "xtrim": false, "xack": false, "xclaim": false, "xautoclaim": false,
	"xgroup": false, "xreadgroup": false,
	"debug": false,
}

// --- the memory-size operand --------------------------------------------------

// parseMemorySize reads the byte counts Redis accepts wherever a memory size is
// configured: a bare integer, or one with a unit suffix.
//
// The two families of suffix are not interchangeable and Redis distinguishes them, so this
// does too: `1k` is 1000 bytes and `1kb` is 1024. Collapsing them would make `CONFIG SET
// maxmemory 100k` reserve 2.4% more than the operator asked for -- small, silent, and
// exactly the kind of thing that surfaces as an unexplained OOM kill at the top of a
// container's limit.
//
// The suffix is matched case-insensitively, as Redis's memtoull does, so `100MB` and
// `100mb` are one value. Everything else is measured against redis 7.2 rather than assumed,
// because the edges are surprising in both directions: the empty string is *accepted* and
// means 0, while `-1`, `1.5mb`, `+100mb`, `100 mb` and `100mbb` are all refused. A
// fractional or signed value that was accepted here would read back as something other than
// what was set, and `-1` in particular is the value an operator reaches for expecting "no
// limit" -- Redis refuses it, so silently reading it as 0 would be a limit removed by a
// command that looked like it had been rejected.
//
// The one deliberate divergence: Redis holds maxmemory as an unsigned long long and clamps
// an overlarge operand to 18446744073709551615, where this refuses anything above the int64
// range. A budget above 8 exabytes is not a limit any deployment sets, and refusing is the
// side to err on -- see invariant 15 on what silently wrapping an operand costs.
func parseMemorySize(v string) (int64, bool) {
	if v == "" {
		return 0, true // measured: redis 7.2 accepts the empty string and reads back 0
	}
	// A leading sign is refused, as Redis refuses it. strconv.ParseInt would accept "+100".
	if v[0] == '+' || v[0] == '-' {
		return 0, false
	}
	// Longest suffix first: "kb" must not be read as "b" with a stray "k" left in the
	// digits.
	units := []struct {
		suffix string
		mult   int64
	}{
		{"kb", 1024},
		{"mb", 1024 * 1024},
		{"gb", 1024 * 1024 * 1024},
		{"k", 1000},
		{"m", 1000 * 1000},
		{"g", 1000 * 1000 * 1000},
		{"b", 1},
	}
	digits, mult := v, int64(1)
	lower := strings.ToLower(v)
	for _, u := range units {
		if strings.HasSuffix(lower, u.suffix) {
			digits, mult = v[:len(v)-len(u.suffix)], u.mult
			break
		}
	}
	n, err := strconv.ParseInt(digits, 10, 64)
	if err != nil || n < 0 {
		return 0, false
	}
	// Refuse an overflow rather than wrapping to a small -- or negative -- budget, which
	// would silently turn "effectively no limit" into "evict everything". Invariant 15's
	// rule: the check goes before the arithmetic, not after it.
	if mult > 1 && n > (1<<63-1)/mult {
		return 0, false
	}
	return n * mult, true
}

// formatMemoryHuman renders a byte count the way INFO's *_human fields do, which is what
// dashboards display verbatim.
func formatMemoryHuman(n int64) string {
	switch {
	case n >= 1<<30:
		return strconv.FormatFloat(float64(n)/(1<<30), 'f', 2, 64) + "G"
	case n >= 1<<20:
		return strconv.FormatFloat(float64(n)/(1<<20), 'f', 2, 64) + "M"
	case n >= 1<<10:
		return strconv.FormatFloat(float64(n)/(1<<10), 'f', 2, 64) + "K"
	default:
		return strconv.FormatInt(n, 10) + "B"
	}
}
