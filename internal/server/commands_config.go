package server

// CONFIG GET/SET/RESETSTAT over the settings this server actually has.
//
// The table below is the whole surface, and it is deliberately not a free-form
// key/value store: every entry names a setting that exists and reads it from the place
// that owns it. A configuration command that accepted and remembered arbitrary
// parameters would answer CONFIG GET with settings the server does not implement,
// which is worse than answering nothing -- a client (or an operator) would tune a
// number that changes no behaviour.
//
// Parameter names are Redis's wherever the setting is Redis's, because tools match on
// them. Where the setting is this server's own -- maxkeys, which bounds the *number* of
// keys -- the name is its own too, rather than borrowing maxmemory's.
//
// The maxmemory family used to be reported read-only, with 0 as the honest answer to "what
// byte limit is enforced", because nothing here measured bytes. Bytes are measured now (see
// internal/store/memtrack.go), so every member of the family that this server can act on is
// settable and enforced: maxmemory is a real budget, maxmemory-policy really selects between
// Redis's eight policies, and maxmemory-samples really changes how many keys the sampler
// looks at. maxkeys stays beside them as a separate, simpler cap -- it is a documented
// feature and a key count is what some deployments actually want to bound.
//
// The members that remain read-only are the ones where there is still nothing to act on,
// and each says so where it is defined. The eviction policy is reported from the same
// accessor INFO reports it from, so the two can never disagree.

import (
	"errors"
	"math"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/Black-third/shardkv/internal/aof"
	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

func init() {
	register("CONFIG", -2, false, cmdConfig)
}

// configParam is one settable or readable server setting. A nil set makes the
// parameter read-only: it is reported, and an attempt to change it is refused rather
// than accepted and ignored, because several of these (the listening port, the
// certificate files, the shard count) are fixed by the time a connection exists.
type configParam struct {
	name string
	get  func(*Server) string
	set  func(*Server, string) bool // false: the value was not acceptable
	// setErr replaces the generic "couldn't be parsed into an integer" detail for a
	// parameter whose refusal is not about parsing. Redis words each of its refusals for
	// the setting that made it, and a caller that is told the wrong reason looks in the
	// wrong place.
	setErr string
	// setErrFn replaces setErr for a parameter whose refusal depends on *how* the value was
	// wrong. Redis distinguishes the two cases for its bounded integer settings -- `CONFIG
	// SET maxmemory-samples abc` says "argument couldn't be parsed into an integer" while
	// `... 0` says "argument must be between 1 and 2147483647 inclusive" -- and collapsing
	// them tells an operator who typed a letter that their number was out of range.
	setErrFn func(v string) string
}

// boundedIntErr is Redis's pair of refusals for a bounded integer setting, chosen by which
// kind of wrong the value was. Both strings are measured on redis 7.2.
func boundedIntErr(lo, hi int64) func(string) string {
	return func(v string) string {
		if _, err := strconv.ParseInt(v, 10, 64); err != nil {
			return "argument couldn't be parsed into an integer"
		}
		return "argument must be between " + strconv.FormatInt(lo, 10) + " and " +
			strconv.FormatInt(hi, 10) + " inclusive"
	}
}

var configParams = []configParam{
	{
		// Reported so a client can tell whether DEBUG is available before it tries it, which
		// is what Redis's own test suite does. Read-only for the same reason Redis makes it
		// immutable, and the reason is sharper here: a gate a client could open over the wire
		// is not a gate. Having no setter is what produces the refusal, and that refusal is
		// byte-for-byte what redis:7.2 answers for this parameter -- measured.
		//
		// `enable-module-command` is deliberately not reported alongside it, though Redis
		// reports both: there is no MODULE command here, and a config for a command that does
		// not exist is an invented fact.
		name: "enable-debug-command",
		get:  func(s *Server) string { return s.EnableDebugCommand() },
	},
	{
		name: "requirepass",
		get:  func(s *Server) string { return s.RequirePass() },
		set: func(s *Server, v string) bool {
			s.SetRequirePass(v)
			return true
		},
	},
	{
		name: "masterauth",
		get:  func(s *Server) string { return s.MasterAuth() },
		set: func(s *Server, v string) bool {
			s.SetMasterAuth(v)
			return true
		},
	},
	{
		name:   "notify-keyspace-events",
		get:    func(s *Server) string { return s.NotifyKeyspaceEvents() },
		set:    func(s *Server, v string) bool { return s.SetNotifyKeyspaceEvents(v) },
		setErr: "Invalid event class character. Use 'Ag$lshzxeKEtmdn'.",
	},
	{
		name: "appendonly",
		get:  func(s *Server) string { return yesNo(s.aof != nil) },
		// Read-only: turning persistence on needs a file to write to, which is a
		// startup decision (-aof), not a value.
	},
	{
		name: "appendfsync",
		get: func(s *Server) string {
			if s.aof == nil {
				return "everysec"
			}
			return s.aof.Policy().String()
		},
		set: func(s *Server, v string) bool {
			p, ok := aof.ParseSyncPolicy(v)
			if !ok || s.aof == nil {
				return false
			}
			s.aof.SetPolicy(p)
			return true
		},
	},
	{
		name: "auto-aof-rewrite-percentage",
		get:  func(s *Server) string { return strconv.Itoa(s.AOFRewritePercentage()) },
		set: func(s *Server, v string) bool {
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 {
				return false
			}
			s.SetAOFRewritePolicy(s.AOFRewriteMinSize(), n)
			return true
		},
	},
	{
		name: "auto-aof-rewrite-min-size",
		get:  func(s *Server) string { return strconv.FormatInt(s.AOFRewriteMinSize(), 10) },
		set: func(s *Server, v string) bool {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n < 0 {
				return false
			}
			s.SetAOFRewritePolicy(n, s.AOFRewritePercentage())
			return true
		},
	},
	{
		// The cap is per database, which is the only meaning it can have when each
		// database is an independent keyspace with its own eviction pass. CONFIG SET
		// therefore applies it to all of them, and CONFIG GET reads database 0, so the
		// value reported is always the value in force everywhere.
		//
		// It is a different mechanism from maxmemory below and stays one: a byte budget is
		// enforced on the write path and can refuse a command, while a key cap is enforced
		// by the janitor and only ever evicts. Answering a key cap with OOM errors -- which
		// is what noeviction means for maxmemory -- would silently retire a documented
		// feature, so a cap evicts by the configured policy when that policy evicts at all
		// and by approximate LRU when it does not. See Store.EvictToLimit.
		name: "maxkeys",
		get:  func(s *Server) string { return strconv.Itoa(s.DB(0).MaxKeys()) },
		set: func(s *Server, v string) bool {
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 {
				return false
			}
			for i := 0; i < s.Databases(); i++ {
				s.DB(i).SetMaxKeys(n)
			}
			return true
		},
	},
	{
		// The byte budget the whole dataset is held within, 0 for unbounded -- which is
		// still the default, so an existing deployment behaves exactly as it did.
		//
		// It is a real limit now: used_memory is maintained as values are written, and a
		// write that would leave the dataset over the budget triggers eviction or, under a
		// policy that cannot evict, the OOM refusal. See maxmemory.go.
		//
		// The value is accepted in the forms Redis accepts, suffixes included, and reported
		// back as a plain byte count exactly as Redis reports it -- `CONFIG SET maxmemory
		// 100mb` reads back `104857600`, not `100mb`. A client that round-trips the value
		// gets the same limit it set, which is the only property that makes the pair useful.
		name: "maxmemory",
		get:  func(s *Server) string { return strconv.FormatInt(s.MaxMemory(), 10) },
		set: func(s *Server, v string) bool {
			n, ok := parseMemorySize(v)
			if !ok {
				return false
			}
			s.SetMaxMemory(n)
			return true
		},
		// Redis words this refusal for the setting rather than reusing its generic integer
		// message, and a caller told the wrong reason looks in the wrong place. Measured on
		// redis 7.2 for `CONFIG SET maxmemory -1`, `abc`, `1.5mb` and `100mbb`.
		setErr: "argument must be a memory value",
	},
	{
		// Which of Redis's eight policies decides what happens when the budget is reached.
		// All eight are implemented, including the LFU pair -- see internal/store/evict.go
		// for the sampler and for the logarithmic counter LFU ranks by.
		//
		// Its default is derived rather than flatly noeviction: a server with a maxkeys cap
		// and no policy of its own evicts by approximate LRU over all keys, which is what
		// this parameter answered before it became settable. An explicit CONFIG SET always
		// wins over that, so the parameter can never read back as something other than what
		// it was set to.
		//
		// It shares maxmemoryPolicy() with INFO's maxmemory_policy on purpose: two
		// spellings of one fact drift, and a client that compared them would find the
		// server disagreeing with itself.
		name: "maxmemory-policy",
		get:  func(s *Server) string { return s.maxmemoryPolicy() },
		set: func(s *Server, v string) bool {
			p, ok := store.ParseEvictionPolicy(v)
			if !ok {
				return false
			}
			s.SetEvictionPolicy(p)
			return true
		},
		setErr: "argument(s) must be one of the following: volatile-lru, volatile-lfu, " +
			"volatile-random, volatile-ttl, allkeys-lru, allkeys-lfu, allkeys-random, noeviction",
	},
	{
		// How many keys the sampler examines before choosing a victim among them.
		//
		// Its default is 16 rather than Redis's 5, and that is a real difference rather than
		// a cosmetic one: a sample here is drawn from a single shard, so it sees 1/256th of
		// the keyspace, and a wider sample buys back some of that narrowness. Reporting 5 to
		// look familiar would have described nothing that happens here.
		//
		// It is settable, and the number it reports is the number pickVictim actually reads
		// -- that being the whole point. A knob that reported a value the sampler ignored
		// would be worse than no knob, because an operator would tune it and measure no
		// change. Redis bounds it to 1..64 and so does this: the reply to an out-of-range
		// value is its own message rather than silent clamping.
		name: "maxmemory-samples",
		get:  func(s *Server) string { return strconv.Itoa(s.DB(0).EvictionSampleCount()) },
		set: func(s *Server, v string) bool {
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 || n > math.MaxInt32 {
				return false
			}
			for i := 0; i < s.Databases(); i++ {
				s.DB(i).SetEvictionSamples(n)
			}
			return true
		},
		// Measured on redis 7.2: the bound really is 1..2147483647, not the 1..64 the
		// documentation implies -- `CONFIG SET maxmemory-samples 1000` is accepted -- and 0
		// and -1 are refused as out of range while `abc` is refused as unparseable.
		setErrFn: boundedIntErr(1, math.MaxInt32),
	},
	{
		// How slowly the LFU access counter grows. Larger means more accesses are needed to
		// tell two hot keys apart; 0 makes the counter linear in the access count. Redis's
		// default is 10.
		name: "lfu-log-factor",
		get: func(s *Server) string {
			f, _ := s.DB(0).LFUParams()
			return strconv.FormatInt(f, 10)
		},
		set:      func(s *Server, v string) bool { return s.setLFUParam(v, true) },
		setErrFn: boundedIntErr(0, math.MaxInt32),
	},
	{
		// How many idle minutes cost the LFU counter one point. 0 disables the decay
		// entirely, which makes the counter a lifetime total rather than a recent-frequency
		// estimate. Redis's default is 1.
		name: "lfu-decay-time",
		get: func(s *Server) string {
			_, d := s.DB(0).LFUParams()
			return strconv.FormatInt(d, 10)
		},
		set:      func(s *Server, v string) bool { return s.setLFUParam(v, false) },
		setErrFn: boundedIntErr(0, math.MaxInt32),
	},
	{
		// The share of maxmemory that client buffers may occupy before clients are evicted
		// to reclaim it. 0 is Redis's own default and means "no client eviction", which is
		// the truth here too: a client that will not read is dropped when its queue fills
		// (invariant 6), never evicted to free memory, and nothing measures the bytes its
		// buffers hold.
		name: "maxmemory-clients",
		get:  func(s *Server) string { return "0" },
	},
	// maxmemory-eviction-tenacity is deliberately absent rather than reported as Redis's
	// default of 10. It is the effort budget Redis spends inside one event-loop iteration
	// before returning to serving clients, and this server has no such trade-off to tune:
	// eviction runs on the janitor goroutine and drains the whole excess in one pass
	// without competing with the command path for a time slice. There is no number here
	// that a client could turn into different behaviour, so there is nothing to report.
	{
		// Microseconds, as in Redis: a negative value disables the slow log and zero logs
		// every command.
		name: "slowlog-log-slower-than",
		get:  func(s *Server) string { return strconv.FormatInt(s.SlowlogThresholdUs(), 10) },
		set: func(s *Server, v string) bool {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return false
			}
			s.SetSlowlogPolicy(n, s.SlowlogMaxLen())
			return true
		},
	},
	{
		name: "slowlog-max-len",
		get:  func(s *Server) string { return strconv.FormatInt(s.SlowlogMaxLen(), 10) },
		set: func(s *Server, v string) bool {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n < 0 {
				return false
			}
			s.SetSlowlogPolicy(s.SlowlogThresholdUs(), n)
			return true
		},
	},
	{
		// Milliseconds; 0 disables the latency monitor, which is Redis's default.
		name: "latency-monitor-threshold",
		get:  func(s *Server) string { return strconv.FormatInt(s.LatencyThresholdMs(), 10) },
		set: func(s *Server, v string) bool {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n < 0 {
				return false
			}
			s.SetLatencyThresholdMs(n)
			return true
		},
	},
	{
		name: "repl-backlog-size",
		get:  func(s *Server) string { return strconv.FormatInt(s.ReplBacklogSize(), 10) },
		set: func(s *Server, v string) bool {
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 {
				return false
			}
			// Resizing allocates a new ring, which discards the retained history: a
			// replica reconnecting immediately afterwards takes a full resync. That is
			// the honest consequence of being told to keep a different amount.
			s.SetReplBacklogSize(n)
			return true
		},
	},
	{
		name: "databases",
		get:  func(s *Server) string { return strconv.Itoa(s.Databases()) },
		// Read-only: a database is a keyspace with its own shards and its own janitor, all
		// created before serving starts, and a client may already be SELECTed into one that
		// a shrink would take away.
	},
	{
		name: "port",
		get: func(s *Server) string {
			if p := s.listeningPort(); p != "" {
				return p
			}
			return "0"
		},
	},
	{
		name: "tls-cert-file",
		get:  func(s *Server) string { opts, _ := s.TLSConfigInUse(); return opts.CertFile },
	},
	{
		name: "tls-key-file",
		get:  func(s *Server) string { opts, _ := s.TLSConfigInUse(); return opts.KeyFile },
	},
	{
		name: "tls-ca-cert-file",
		get:  func(s *Server) string { opts, _ := s.TLSConfigInUse(); return opts.CAFile },
	},
	{
		name: "tls-replication",
		get:  func(s *Server) string { _, repl := s.TLSConfigInUse(); return yesNo(repl) },
	},
	{
		name: "maxclients",
		get:  func(s *Server) string { return strconv.Itoa(s.MaxClients()) },
		set: func(s *Server, v string) bool {
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 {
				return false
			}
			s.SetMaxClients(n)
			return true
		},
	},
	{
		// Seconds a client may be idle before the server closes it; 0 disables the check,
		// which is Redis's default. See reapIdleClients for the connections it must never
		// apply to.
		name: "timeout",
		get:  func(s *Server) string { return strconv.FormatInt(s.ClientTimeoutSecs(), 10) },
		set: func(s *Server, v string) bool {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n < 0 {
				return false
			}
			s.SetClientTimeoutSecs(n)
			return true
		},
	},
	{
		// The snapshot schedule: <seconds> <changes> pairs, Redis's spelling. It used to read
		// as an empty string and refuse any non-empty value, because there was no snapshot
		// mechanism and a schedule would have been a durability promise the server could not
		// keep. There is one now (see snapshot.go), so the schedule is real -- and it only
		// ever fires on a server that was given a snapshot path, since without one there is
		// nowhere to write to and the schedule is inert.
		//
		// An invalid spec is refused rather than partly applied: half a schedule is a
		// durability setting that does not do what the operator wrote down. Measured on redis
		// 7.2, `900` alone, `abc 1` and `900 -1` are all refused with this message, while the
		// empty string is accepted and means "no snapshots".
		name:   "save",
		get:    func(s *Server) string { return s.SaveSchedule() },
		set:    func(s *Server, v string) bool { return s.SetSaveSchedule(v) },
		setErr: "Invalid save parameters",
	},
	{
		// The snapshot file's name and directory, split the way Redis splits them because
		// backup tooling reads them separately in order to reassemble the path.
		//
		// Both read empty when no snapshot path was configured, rather than the "." that
		// filepath.Base and filepath.Dir return for an empty string. "." is a real relative
		// directory, so reporting it would name a file this server has no intention of
		// writing -- and a backup script that joined the two would go looking for ./. Empty
		// is how this table already reports an unconfigured file (see tls-cert-file).
		name: "dbfilename",
		get: func(s *Server) string {
			if path := s.SnapshotPath(); path != "" {
				return filepath.Base(path)
			}
			return ""
		},
	},
	{
		name: "dir",
		get: func(s *Server) string {
			if path := s.SnapshotPath(); path != "" {
				return filepath.Dir(path)
			}
			return ""
		},
	},
	{
		// How many quicklist nodes at each end of a list are left uncompressed. This store
		// holds a list as one doubly-linked deque with no nodes and no LZF compression, so
		// like stream-node-max-entries the parameter is inert *because the structure it
		// tunes does not exist here* -- not because it is accepted and ignored. It is
		// settable and reports back what it was told, since a client that configures it
		// should not be refused and should not be lied to about what it set.
		name: "list-compress-depth",
		get:  func(s *Server) string { return strconv.FormatInt(s.ListCompressDepth(), 10) },
		set: func(s *Server, v string) bool {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n < 0 {
				return false
			}
			s.SetListCompressDepth(n)
			return true
		},
	},
	{
		// Entries per macro-node in a stream's radix tree. This stream is a sorted slice
		// of entries with no macro-nodes at all, so the parameter is genuinely inert here
		// -- inert *because the structure it tunes does not exist*, which is a different
		// claim from a threshold that is accepted and then ignored. It is settable so a
		// client that configures it is not refused, and it reports back what it was told,
		// because a value that read differently from what was set would be the misleading
		// answer.
		name: "stream-node-max-entries",
		get:  func(s *Server) string { return strconv.FormatInt(s.StreamNodeMaxEntries(), 10) },
		set: func(s *Server, v string) bool {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n < 0 {
				return false
			}
			s.SetStreamNodeMaxEntries(n)
			return true
		},
	},
}

// encodingParams are the representation thresholds. Each is a real setting: it is read
// by store.Encoding, so changing one genuinely changes what OBJECT ENCODING reports,
// which is what Redis's `foreach encoding {listpack hashtable}` type tests manipulate.
//
// alias is the pre-7.0 `ziplist` spelling Redis still answers to, where one exists.
// Redis reports both names from CONFIG GET and keeps one value behind them, so both are
// generated as separate parameters over the same accessor rather than one name silently
// rewriting the other.
var encodingParams = []struct {
	name  string
	alias string
	param store.EncodingParam
	// signed allows the negative values list-max-listpack-size uses to mean "bound the
	// listpack by bytes rather than by element count". Every other threshold is a count
	// or a length, where a negative value has no meaning.
	signed bool
}{
	{name: "hash-max-listpack-entries", alias: "hash-max-ziplist-entries", param: store.HashMaxListpackEntries},
	{name: "hash-max-listpack-value", alias: "hash-max-ziplist-value", param: store.HashMaxListpackValue},
	{name: "list-max-listpack-size", alias: "list-max-ziplist-size", param: store.ListMaxListpackSize, signed: true},
	{name: "set-max-intset-entries", param: store.SetMaxIntsetEntries},
	{name: "set-max-listpack-entries", param: store.SetMaxListpackEntries},
	{name: "set-max-listpack-value", param: store.SetMaxListpackValue},
	{name: "zset-max-listpack-entries", alias: "zset-max-ziplist-entries", param: store.ZSetMaxListpackEntries},
	{name: "zset-max-listpack-value", alias: "zset-max-ziplist-value", param: store.ZSetMaxListpackValue},
	{name: "hll-sparse-max-bytes", param: store.HLLSparseMaxBytes},
}

// init appends the encoding thresholds to the parameter table.
//
// They are applied to every database and read back from database 0, exactly as maxkeys
// is: the setting is server-wide in Redis, and each database here is an independent
// store that owns its own copy, so "the value in force" is only well defined if every
// copy holds it.
func init() {
	for _, ep := range encodingParams {
		p, signed := ep.param, ep.signed
		get := func(s *Server) string { return strconv.FormatInt(s.DB(0).EncodingLimit(p), 10) }
		set := func(s *Server, v string) bool {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || (n < 0 && !signed) {
				return false
			}
			for i := 0; i < s.Databases(); i++ {
				s.DB(i).SetEncodingLimit(p, n)
			}
			return true
		}
		configParams = append(configParams, configParam{name: ep.name, get: get, set: set})
		if ep.alias != "" {
			configParams = append(configParams, configParam{name: ep.alias, get: get, set: set})
		}
	}
}

// setLFUParam applies one of the two LFU tuning parameters to every database, leaving the
// other as it is. They are set as a pair in the store because the decay and the growth rate
// are read together on every access, so one call keeps them consistent.
func (s *Server) setLFUParam(v string, logFactor bool) bool {
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 || n > math.MaxInt32 {
		return false
	}
	for i := 0; i < s.Databases(); i++ {
		f, d := s.DB(i).LFUParams()
		if logFactor {
			f = n
		} else {
			d = n
		}
		s.DB(i).SetLFUParams(f, d)
	}
	return true
}

func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

// cmdConfig implements CONFIG GET pattern [pattern...] | SET param value | RESETSTAT.
func cmdConfig(s *Server, w *resp.Writer, args [][]byte) bool {
	switch strings.ToUpper(string(args[1])) {
	case "GET":
		if len(args) < 3 {
			w.WriteError("ERR wrong number of arguments for 'config|get' command")
			return false
		}
		configGet(s, w, args[2:])

	case "SET":
		if len(args) != 4 {
			w.WriteError("ERR wrong number of arguments for 'config|set' command")
			return false
		}
		configSet(s, w, string(args[2]), string(args[3]))

	case "RESETSTAT":
		s.resetStats()
		w.WriteSimple("OK")

	case "HELP":
		writeSubcommandHelp(w, "CONFIG", []string{
			"GET <pattern>",
			"    Return parameters matching the glob-like <pattern> and their values.",
			"SET <directive> <value>",
			"    Set the configuration <directive> to <value>.",
			"RESETSTAT",
			"    Reset statistics reported by the INFO command.",
		})

	default:
		writeUnknownSubcommand(w, "CONFIG", args[1])
	}
	return false
}

// configGet answers with the parameters matching any of the given globs, as a map of
// name to value (a RESP3 map, the flat name/value array in RESP2).
//
// The patterns are globs, not exact names, because that is what clients send: a
// library warming up asks CONFIG GET maxmemory* or save* and expects the matching
// subset rather than an error. A name with no wildcard is simply a glob that matches
// one parameter, so both forms take the same path.
func configGet(s *Server, w *resp.Writer, patterns [][]byte) {
	var out []string
	for _, p := range configParams {
		for _, pattern := range patterns {
			// Folded on both sides rather than by lower-casing the pattern: every parameter
			// name here is already lower case, so the two are equivalent today, and folding
			// is the form that stays correct if one ever is not.
			if globMatchFold(string(pattern), p.name) {
				out = append(out, p.name, p.get(s))
				break // one entry per parameter even if several patterns match it
			}
		}
	}
	writeMapStrings(w, out)
}

func configSet(s *Server, w *resp.Writer, name, value string) {
	name = strings.ToLower(name)
	for _, p := range configParams {
		if p.name != name {
			continue
		}
		if p.set == nil {
			w.WriteError("ERR CONFIG SET failed (possibly related to argument '" + name +
				"') - can't set immutable config")
			return
		}
		if !p.set(s, value) {
			w.WriteError("ERR CONFIG SET failed (possibly related to argument '" + name +
				"') - " + configSetDetail(p, value))
			return
		}
		w.WriteSimple("OK")
		return
	}
	w.WriteError("ERR Unknown option or number of arguments for CONFIG SET - '" + name + "'")
}

// configSetDetail is the reason a value was refused, in Redis's words for that setting.
// Shared by the client path and by ConfigSet so an operator who mistypes a startup flag
// reads the same sentence a client would have been sent.
func configSetDetail(p configParam, value string) string {
	switch {
	case p.setErrFn != nil:
		return p.setErrFn(value)
	case p.setErr != "":
		return p.setErr
	}
	return "argument couldn't be parsed into an integer, or is out of range"
}

// ConfigSet applies a setting from outside a client connection, which is what a command-line
// flag is.
//
// It goes through the same table, the same parsing and the same refusal text CONFIG SET uses
// -- deliberately. A flag that parsed its own value would be a second parser for the same
// setting, and the two would drift in exactly the way invariant 7 describes for key
// extraction: `-maxmemory 100mb` and `CONFIG SET maxmemory 100mb` would eventually mean
// different numbers, and nothing would report it. The value is a string for the same reason,
// so a unit suffix means at startup what it means at runtime.
//
// The error is plain rather than RESP-prefixed, since its reader is an operator looking at a
// process that would not start.
func (s *Server) ConfigSet(name, value string) error {
	name = strings.ToLower(name)
	for _, p := range configParams {
		if p.name != name {
			continue
		}
		if p.set == nil {
			return errors.New("can't set immutable config")
		}
		if !p.set(s, value) {
			return errors.New(configSetDetail(p, value))
		}
		return nil
	}
	return errors.New("unknown config parameter")
}

// resetStat clears the counters INFO reports, which is what CONFIG RESETSTAT is for:
// an operator measuring a window wants the counters to start from zero, not to be
// diffed by hand.
//
// The lifetime facts that are not statistics are left alone: the eviction total the
// store keeps (it describes the dataset's history, not this measurement window) and
// the replication offset, which is a position in a stream and would break every
// connected replica if it were reset.
func (s *Server) resetStats() {
	s.totalConns.Store(0)
	s.totalCmds.Store(0)
	s.fullSyncs.Store(0)
	s.partialOK.Store(0)
	s.partialErr.Store(0)
	s.replicaDrops.Store(0)
	s.pubsubDrops.Store(0)
	s.monitorDrops.Store(0)
	// The per-command statistics and the keyspace hit/miss counters are statistics in
	// exactly the sense the command means, so they go too. The slow log does not: it is
	// cleared by SLOWLOG RESET, which is the command an operator reaches for, and
	// discarding recorded evidence of slow commands as a side effect of resetting
	// counters would lose the one thing that cannot be recomputed.
	resetCommandStats()
	for i := 0; i < s.Databases(); i++ {
		s.DB(i).ResetKeyspaceStats()
	}
}
