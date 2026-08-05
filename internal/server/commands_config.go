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
		name: "notify-keyspace-events",
		get:  func(s *Server) string { return s.NotifyKeyspaceEvents() },
		set:  func(s *Server, v string) bool { return s.SetNotifyKeyspaceEvents(v) },
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

	default:
		w.WriteError("ERR Unknown CONFIG subcommand or wrong number of arguments for '" +
			string(args[1]) + "'")
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
			w.WriteError("ERR CONFIG SET failed - can't set immutable config '" + name + "'")
			return
		}
		if !p.set(s, value) {
			w.WriteError("ERR CONFIG SET failed - argument couldn't be parsed into an integer, " +
				"or is out of range, for '" + name + "'")
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
