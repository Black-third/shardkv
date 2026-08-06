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
// them. Where the setting is this server's own (maxkeys, which bounds keys rather than
// bytes) the name is its own too, rather than pretending to be maxmemory and reporting
// a byte count it does not enforce.

import (
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
}

var configParams = []configParam{
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
		// The RDB snapshot schedule. There is no RDB here -- persistence is the AOF and
		// nothing else -- so the schedule reads as empty, which is not a placeholder but the
		// literal truth: no periodic dataset write is configured, and none would run.
		// (A real Redis started with `--save ''` reads the same empty string.)
		//
		// It is settable, but only to a schedule that asks for nothing. "Turn snapshots
		// off" is a state this server is genuinely in and can genuinely be asked for; any
		// non-empty schedule is a durability promise it cannot keep, and accepting one
		// would be exactly the "tune a number that changes no behaviour" this table exists
		// to refuse. So the empty schedule succeeds and a non-empty one is rejected with
		// Redis's own message for a schedule it will not take.
		name:   "save",
		get:    func(s *Server) string { return "" },
		set:    func(s *Server, v string) bool { return strings.TrimSpace(v) == "" },
		setErr: "Invalid save parameters",
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
			if globMatch(strings.ToLower(string(pattern)), p.name) {
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
			detail := "argument couldn't be parsed into an integer, or is out of range"
			if p.setErr != "" {
				detail = p.setErr
			}
			w.WriteError("ERR CONFIG SET failed (possibly related to argument '" + name +
				"') - " + detail)
			return
		}
		w.WriteSimple("OK")
		return
	}
	w.WriteError("ERR Unknown option or number of arguments for CONFIG SET - '" + name + "'")
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
