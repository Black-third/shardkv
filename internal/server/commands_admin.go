package server

import (
	"fmt"
	"math"
	"net"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

// Version is the single source of truth for the server version, reported by INFO
// and logged at startup.
const Version = "0.3.0"

// RedisCompatVersion is the Redis feature level this server implements, reported as
// INFO's redis_version and by HELLO.
//
// It is deliberately a separate constant from Version. Client libraries and monitoring
// agents branch on redis_version -- to decide whether HELLO is available, whether a
// command takes a newer option, whether to expect a field in a reply -- so the value has
// to describe *what is implemented here*, not what this build is called. Raising it is a
// claim: it means the commands and reply shapes that release introduced are present.
//
// 7.4 is the level claimed: RESP3, streams with consumer groups, functions-free scripting
// absence aside, the expire flag families, OBJECT ENCODING names, cluster slots/shards
// replies. What is genuinely missing at this level is recorded in the README's
// compatibility section rather than papered over by lowering this number.
const RedisCompatVersion = "7.4.0"

func init() {
	register("PING", -1, false, cmdPing)
	register("ECHO", 2, false, cmdEcho)
	register("TIME", 1, false, cmdTime)
	register("INFO", -1, false, cmdInfo)
	register("SCRIPT", -2, false, cmdScript)
	register("FUNCTION", -2, false, cmdFunction)
	register("DBSIZE", 1, false, cmdDBSize)
	register("FLUSHALL", 1, true, cmdFlushAll)
	register("FLUSHDB", 1, true, cmdFlushDB)
	register("SWAPDB", 3, true, cmdSwapDB)
	register("MOVE", 3, true, cmdMove)
	register("COMMAND", -1, false, cmdCommand)
	register("REPLICAOF", 3, false, cmdReplicaOf)
	register("SLAVEOF", 3, false, cmdReplicaOf) // legacy alias
	registerSession("SHUTDOWN", -1, cmdShutdown)
}

func cmdPing(s *Server, w *resp.Writer, args [][]byte) bool {
	// PING takes at most one argument. The table's arity cannot express that on its own --
	// -1 means "at least one", which is every count -- so the upper bound is checked here,
	// as Redis checks it: `PING a b` is an arity error rather than an echo of the first of
	// them, which is what it would silently have been.
	if len(args) > 2 {
		w.WriteError("ERR wrong number of arguments for 'ping' command")
		return false
	}
	if len(args) > 1 {
		w.WriteBulk(args[1])
	} else {
		w.WriteSimple("PONG")
	}
	return false
}

// cmdTime returns the server's clock as a two-element array of unix seconds and the
// microseconds within that second, as Redis does.
//
// It reads the store's clock rather than time.Now, like every other server-side "now":
// under an injected clock a test that compares TIME against a TTL it just set has to see
// one timeline, not two.
func cmdTime(s *Server, w *resp.Writer, args [][]byte) bool {
	now := s.store.Now()
	w.WriteArrayHeader(2)
	w.WriteBulk([]byte(strconv.FormatInt(now.Unix(), 10)))
	w.WriteBulk([]byte(strconv.FormatInt(int64(now.Nanosecond()/1000), 10)))
	return false
}

// cmdEcho returns its argument.
//
// It looks like a toy, and it is the single most load-bearing trivial command here:
// StackExchange.Redis issues ECHO as part of establishing a connection, so a server
// without it cannot be reached by any .NET client at all -- the handshake fails before
// the application's first command. Several other libraries use it to measure round-trip
// latency and to prove a pooled connection is still the one they think it is.
func cmdEcho(s *Server, w *resp.Writer, args [][]byte) bool {
	w.WriteBulk(args[1])
	return false
}

// cmdScript answers the SCRIPT subcommands a client sends when it is *not* running a
// script. There is no Lua interpreter here (see the README's compatibility section), so
// EVAL and friends are absent and honestly so -- but SCRIPT FLUSH and SCRIPT EXISTS are
// asked by callers that are only tidying up or checking, and both have truthful answers
// on a server that caches no scripts: flushing an empty cache succeeds, and nothing
// exists.
//
// This is what lets Redis's own test suite run against this server in external mode: its
// setup path flushes scripts and functions before each unit, and an error there stops the
// suite before a single test executes. Answering these two is the difference between
// "cannot be tested by Redis's suite" and "passes N of Redis's own tests".
func cmdScript(s *Server, w *resp.Writer, args [][]byte) bool {
	switch strings.ToUpper(string(args[1])) {
	case "FLUSH":
		w.WriteSimple("OK")
	case "EXISTS":
		w.WriteArrayHeader(len(args) - 2)
		for range args[2:] {
			w.WriteInt(0)
		}
	case "HELP":
		writeScriptHelp(w)
	default:
		// LOAD would be a lie: it must return a sha the client can then EVALSHA.
		w.WriteError("ERR This Redis command is not allowed from script: " +
			"scripting is not supported by this server")
	}
	return false
}

// cmdFunction is the SCRIPT story for Redis Functions, and for the same reason: the test
// suite's setup flushes them.
func cmdFunction(s *Server, w *resp.Writer, args [][]byte) bool {
	switch strings.ToUpper(string(args[1])) {
	case "FLUSH":
		w.WriteSimple("OK")
	case "LIST":
		w.WriteArrayHeader(0)
	case "STATS":
		w.WriteMapHeader(3)
		w.WriteBulk([]byte("running_script"))
		w.WriteNull()
		w.WriteBulk([]byte("engines"))
		w.WriteMapHeader(0)
		w.WriteBulk([]byte("cluster_enabled"))
		w.WriteInt(boolToInt64(s.ClusterEnabled()))
	case "DUMP":
		w.WriteBulk(nil)
	case "HELP":
		writeFunctionHelp(w)
	default:
		w.WriteError("ERR Functions are not supported by this server")
	}
	return false
}

func writeScriptHelp(w *resp.Writer) {
	w.WriteArrayHeader(3)
	w.WriteSimple("SCRIPT FLUSH -- Flush the scripts cache (always empty here).")
	w.WriteSimple("SCRIPT EXISTS <sha1> [<sha1> ...] -- Always 0: no scripts are cached.")
	w.WriteSimple("Scripting (EVAL/EVALSHA) is not supported by this server.")
}

func writeFunctionHelp(w *resp.Writer) {
	w.WriteArrayHeader(2)
	w.WriteSimple("FUNCTION FLUSH|LIST|STATS|DUMP -- Answered for compatibility; nothing is loaded.")
	w.WriteSimple("Functions (FCALL) are not supported by this server.")
}

func boolToInt64(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// infoSection is one titled block of INFO. Splitting the report into named sections is
// not cosmetic: INFO is polled by monitoring agents, and one that wants the
// replication offsets should not have to receive (and parse past) the whole keyspace
// every few seconds.
type infoSection struct {
	name string
	// optional marks a section that a bare INFO leaves out, because it is long and
	// grows with the command table: commandstats and latencystats are one line per
	// command each. Redis excludes exactly these from its default report, so an agent
	// polling INFO every second keeps receiving what it always did, and asks for
	// "commandstats" (or "all"/"everything") when it wants them.
	optional bool
	render   func(*Server, *strings.Builder)
}

var infoSections = []infoSection{
	{name: "server", render: infoServer},
	{name: "clients", render: infoClients},
	{name: "memory", render: infoMemory},
	{name: "persistence", render: infoPersistence},
	{name: "stats", render: infoStats},
	{name: "replication", render: infoReplication},
	{name: "cluster", render: (*Server).infoCluster},
	{name: "commandstats", optional: true, render: infoCommandStats},
	{name: "latencystats", optional: true, render: infoLatencyStats},
	{name: "keyspace", render: infoKeyspace},
}

// cmdInfo implements INFO [section [section ...]].
//
// With no argument every section is reported. A named section reports just that one;
// "all", "everything" and "default" report everything, the names Redis accepts for it.
// An unknown section name yields an empty report rather than an error, which is what
// Redis does and what keeps a client that asks for a section this server does not have
// (say, "cluster") working.
//
// The report is a verbatim string for a RESP3 client and a bulk string for a RESP2
// one, as in Redis: the payload is identical, but the verbatim type tells the client
// the text is meant to be printed as a report rather than quoted and escaped onto a
// single line.
func cmdInfo(s *Server, w *resp.Writer, args [][]byte) bool {
	wanted := args[1:]
	var b strings.Builder
	for _, section := range infoSections {
		if !infoWanted(wanted, section.name, section.optional) {
			continue
		}
		fmt.Fprintf(&b, "# %s\r\n", strings.ToUpper(section.name[:1])+section.name[1:])
		section.render(s, &b)
		b.WriteString("\r\n")
	}
	w.WriteVerbatim("txt", []byte(b.String()))
	return false
}

func infoWanted(wanted [][]byte, name string, optional bool) bool {
	if len(wanted) == 0 {
		return !optional // a bare INFO is the default report
	}
	for _, arg := range wanted {
		switch asked := strings.ToLower(string(arg)); asked {
		case name, "all", "everything":
			return true
		case "default":
			if !optional {
				return true
			}
		}
	}
	return false
}

func infoServer(s *Server, b *strings.Builder) {
	// redis_version comes first and is not decoration. Every Redis monitoring agent
	// (redis_exporter and the Grafana dashboards built on it) parses this field, and
	// client libraries gate features on it -- a client that cannot find it either fails
	// or assumes the oldest server it supports. Reporting shardkv_version alone made the
	// server invisible to all of that tooling.
	//
	// The value is the Redis feature level this server implements, not a claim to be
	// Redis: shardkv_version below is the real build. Raising it means having implemented
	// what that release added.
	fmt.Fprintf(b, "redis_version:%s\r\n", RedisCompatVersion)
	fmt.Fprintf(b, "redis_mode:%s\r\n", s.redisMode())
	// loading:0 says the dataset is ready to serve. Clients poll it after connecting to a
	// server that might still be replaying an AOF; a missing field reads as unknown.
	fmt.Fprintf(b, "loading:%d\r\n", 0)
	fmt.Fprintf(b, "shardkv_version:%s\r\n", Version)
	fmt.Fprintf(b, "go_version:%s\r\n", runtime.Version())
	fmt.Fprintf(b, "os:%s %s\r\n", runtime.GOOS, runtime.GOARCH)
	fmt.Fprintf(b, "arch_bits:%d\r\n", strconv.IntSize)
	fmt.Fprintf(b, "process_id:%d\r\n", os.Getpid())
	fmt.Fprintf(b, "run_id:%s\r\n", s.replID)
	fmt.Fprintf(b, "tcp_port:%s\r\n", s.listeningPort())
	fmt.Fprintf(b, "uptime_in_seconds:%d\r\n", int(time.Since(s.startTime).Seconds()))
	fmt.Fprintf(b, "uptime_in_days:%d\r\n", int(time.Since(s.startTime).Hours()/24))
	fmt.Fprintf(b, "shards:%d\r\n", s.store.Shards())
}

// redisMode is what INFO and HELLO report for the deployment shape. Redis answers
// "cluster" or "standalone"; a client uses it to decide whether to expect redirects.
func (s *Server) redisMode() string {
	if s.ClusterEnabled() {
		return "cluster"
	}
	return "standalone"
}

// infoMemory reports the memory fields monitoring agents chart.
//
// used_memory is the dataset: the running byte total the store maintains as values are
// written, which is also what maxmemory is compared against. That is the field's whole
// purpose -- an operator sizes a container from it and alerts on it -- so it has to be the
// number eviction acts on rather than a second estimate that could disagree. What it counts
// and what it does not is documented once, on internal/store/memtrack.go, and the short
// version is: every key, its value's payload, and the keyspace's per-key overhead; not the
// Go runtime, not the allocator's slack, not the replication backlog, and not client
// buffers.
//
// used_memory_rss is the Go runtime's total reservation from the OS, which is the closest
// honest analogue of an RSS available here -- Go exposes no process resident size, and this
// server does not control its allocator the way Redis controls jemalloc. The fragmentation
// ratio is therefore that reservation over the dataset, and it reads high for the same
// reason it would on any garbage-collected runtime: the heap holds the collector's headroom
// as well as the data.
func infoMemory(s *Server, b *strings.Builder) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	used := s.UsedMemory()
	maxmem := s.MaxMemory()
	fmt.Fprintf(b, "used_memory:%d\r\n", used)
	fmt.Fprintf(b, "used_memory_human:%s\r\n", formatMemoryHuman(used))
	fmt.Fprintf(b, "used_memory_rss:%d\r\n", m.Sys)
	fmt.Fprintf(b, "used_memory_peak:%d\r\n", m.HeapSys)
	fmt.Fprintf(b, "used_memory_dataset:%d\r\n", used)
	// The Go heap, kept under its own name rather than reported as used_memory. It is a
	// real and useful number -- it is what the process is actually holding -- but it is not
	// the dataset, and a maxmemory an operator sets in bytes has to be compared against the
	// dataset or eviction would be chasing the collector.
	fmt.Fprintf(b, "used_memory_go_heap:%d\r\n", m.HeapAlloc)
	fmt.Fprintf(b, "used_memory_lua:%d\r\n", 0)
	fmt.Fprintf(b, "number_of_cached_scripts:%d\r\n", 0)
	fmt.Fprintf(b, "maxmemory:%d\r\n", maxmem)
	fmt.Fprintf(b, "maxmemory_human:%s\r\n", formatMemoryHuman(maxmem))
	// From the same accessor CONFIG GET maxmemory-policy reads, so the two can never
	// disagree -- see Server.maxmemoryPolicy.
	fmt.Fprintf(b, "maxmemory_policy:%s\r\n", s.maxmemoryPolicy())
	fmt.Fprintf(b, "mem_allocator:go-%s\r\n", runtime.Version())
	fmt.Fprintf(b, "mem_fragmentation_ratio:%.2f\r\n", float64(m.Sys)/math.Max(float64(used), 1))
}

func infoClients(s *Server, b *strings.Builder) {
	s.clientMu.Lock()
	connected := len(s.clients)
	s.clientMu.Unlock()
	fmt.Fprintf(b, "connected_clients:%d\r\n", connected)
	fmt.Fprintf(b, "maxclients:%d\r\n", s.MaxClients())
	fmt.Fprintf(b, "total_connections_received:%d\r\n", s.totalConns.Load())
	// What a caller polls to tell "clients cannot connect" from "clients are not
	// connecting": a non-zero value here is the only visible trace of the maxclients
	// refusal, since the refused connection is gone.
	fmt.Fprintf(b, "rejected_connections:%d\r\n", s.RejectedConnections())
	// What an operator polls to see whether a queue has consumers waiting on it, and
	// what a test polls to know that every client has actually reached the wait before
	// it pushes.
	fmt.Fprintf(b, "blocked_clients:%d\r\n", s.blockedClients())
	fmt.Fprintf(b, "total_blocking_keys:%d\r\n", s.blockingKeys())
}

func infoPersistence(s *Server, b *strings.Builder) {
	fmt.Fprintf(b, "aof_enabled:%d\r\n", boolToInt(s.aof != nil))
	fmt.Fprintf(b, "aof_rewrite_in_progress:%d\r\n", boolToInt(s.aofRewriting.Load()))
	var size, base int64
	if s.aof != nil {
		size, base = s.aof.Size(), s.aof.BaseSize()
	}
	fmt.Fprintf(b, "aof_base_size:%d\r\n", base)
	fmt.Fprintf(b, "aof_current_size:%d\r\n", size)
	fmt.Fprintf(b, "aof_last_bgrewrite_status:%s\r\n", s.aofRewriteStatus())
	fmt.Fprintf(b, "rdb_last_save_time:%d\r\n", s.lastSave.Load())
	// How many writes have changed the dataset since it was last written out in full.
	// Redis counts changes since its last RDB save; here the equivalent event is a
	// successful AOF rewrite, which is also what rdb_last_save_time above reports. See
	// noteDirty for what one "change" counts as.
	fmt.Fprintf(b, "rdb_changes_since_last_save:%d\r\n", s.DirtyChanges())
	// The snapshot fields, in redis 7.2's own order (measured), reading real state rather
	// than the constants they used to be: there was no snapshot mechanism when they were
	// added, so `0` and `ok` were the honest answers. There is one now, and a monitoring
	// agent that charts rdb_last_bgsave_status is watching for the value these can now take.
	fmt.Fprintf(b, "rdb_bgsave_in_progress:%d\r\n", boolToInt(s.SnapshotInProgress()))
	fmt.Fprintf(b, "rdb_last_bgsave_status:%s\r\n", s.SnapshotStatus())
	// Both durations report -1 for "not applicable", which is Redis's spelling for a save
	// that has not happened (or is not happening) rather than a zero that would read as
	// "instantaneous".
	fmt.Fprintf(b, "rdb_last_bgsave_time_sec:%d\r\n", s.SnapshotLastDurationSec())
	fmt.Fprintf(b, "rdb_current_bgsave_time_sec:%d\r\n", s.SnapshotCurrentDurationSec())
	fmt.Fprintf(b, "rdb_saves:%d\r\n", s.SnapshotSaves())
	fmt.Fprintf(b, "rdb_last_load_keys_loaded:%d\r\n", s.SnapshotLoadedKeys())
}

func infoStats(s *Server, b *strings.Builder) {
	fmt.Fprintf(b, "total_commands_processed:%d\r\n", s.totalCmds.Load())
	// Summed over every database, because a hit rate for one keyspace out of sixteen is
	// not the number anyone is looking at. The counters themselves are maintained by each
	// store, next to the lookup that decides the answer (see Store.readEntry).
	var hits, misses int64
	for _, db := range s.dbs {
		hits += db.KeyspaceHits()
		misses += db.KeyspaceMisses()
	}
	fmt.Fprintf(b, "keyspace_hits:%d\r\n", hits)
	fmt.Fprintf(b, "keyspace_misses:%d\r\n", misses)
	// Summed over every database, like keyspace_hits above and for the same reason: an
	// eviction total for one keyspace out of sixteen is not the number an operator is
	// watching. It is a lifetime figure and CONFIG RESETSTAT deliberately leaves it alone --
	// it describes the dataset's history, not a measurement window.
	fmt.Fprintf(b, "evicted_keys:%d\r\n", s.EvictedKeys())
	fmt.Fprintf(b, "sync_full:%d\r\n", s.fullSyncs.Load())
	fmt.Fprintf(b, "sync_partial_ok:%d\r\n", s.partialOK.Load())
	fmt.Fprintf(b, "sync_partial_err:%d\r\n", s.partialErr.Load())
	fmt.Fprintf(b, "replica_drops:%d\r\n", s.replicaDrops.Load())
	fmt.Fprintf(b, "monitor_dropped_clients:%d\r\n", s.monitorDrops.Load())
	b.WriteString(s.pubsubStats())
}

// infoCommandStats renders INFO commandstats: one line per command that has run,
// in Redis's format, so an existing dashboard parses it unchanged.
//
// Only commands that have actually been called are reported. A line per registered
// command would be ~180 lines of zeroes on a fresh server, and Redis omits them for
// the same reason.
func infoCommandStats(s *Server, b *strings.Builder) {
	for _, c := range statsEntries() {
		calls := c.calls.Load()
		rejected, failed := c.rejected.Load(), c.failed.Load()
		if calls == 0 && rejected == 0 {
			continue
		}
		usec := c.usec.Load()
		per := 0.0
		if calls > 0 {
			per = float64(usec) / float64(calls)
		}
		fmt.Fprintf(b, "cmdstat_%s:calls=%d,usec=%d,usec_per_call=%.2f,"+
			"rejected_calls=%d,failed_calls=%d\r\n",
			c.lowerName, calls, usec, per, rejected, failed)
	}
}

// infoLatencyStats renders INFO latencystats: the approximate latency percentiles of
// each command that has run, read out of the per-command histogram.
//
// The percentiles are bucket upper bounds, so they are exact to within a factor of two
// and never under-report. See command.percentileUs for why an approximation is the
// right trade here -- Redis's own version is one too.
func infoLatencyStats(s *Server, b *strings.Builder) {
	for _, c := range statsEntries() {
		if c.calls.Load() == 0 {
			continue
		}
		fmt.Fprintf(b, "latency_percentiles_usec_%s:p50=%.3f,p99=%.3f,p99.9=%.3f\r\n",
			c.lowerName, c.percentileUs(0.50), c.percentileUs(0.99), c.percentileUs(0.999))
	}
}

func infoReplication(s *Server, b *strings.Builder) {
	s.mu.Lock()
	role, master := s.role, s.masterAddr
	replID, offset := s.replID, s.replOffset
	slaveOffset, linkUp := s.slaveOffset, s.masterLinkUp
	backlogSize, histLen := int64(len(s.backlog.buf)), s.backlog.histLen()
	replicas := make([]*replicaConn, 0, len(s.replicas))
	for rc := range s.replicas {
		replicas = append(replicas, rc)
	}
	s.mu.Unlock()

	fmt.Fprintf(b, "role:%s\r\n", role)
	if role == "replica" {
		host, port := master, ""
		if h, p, err := net.SplitHostPort(master); err == nil {
			host, port = h, p
		}
		fmt.Fprintf(b, "master_host:%s\r\n", host)
		if port != "" {
			fmt.Fprintf(b, "master_port:%s\r\n", port)
		}
		fmt.Fprintf(b, "master_link_status:%s\r\n", upDown(linkUp))
		fmt.Fprintf(b, "slave_read_only:1\r\n")
		fmt.Fprintf(b, "slave_repl_offset:%d\r\n", slaveOffset)
	}
	fmt.Fprintf(b, "connected_slaves:%d\r\n", len(replicas))
	// Sorted so the slaveN lines are stable across INFO calls rather than following
	// the registry map's iteration order.
	sort.Slice(replicas, func(i, j int) bool { return replicas[i].port < replicas[j].port })
	for i, rc := range replicas {
		fmt.Fprintf(b, "slave%d:ip=%s,port=%s,state=online,offset=%d\r\n",
			i, rc.addr, rc.port, rc.ack.Load())
	}
	// master_replid names the stream *this* server serves, which is the ID a replica
	// must present to continue from an offset here. A replica re-encodes rather than
	// relaying its master's bytes, so its downstream stream is its own history with its
	// own offsets -- advertising the upstream ID would invite a downstream replica to
	// continue from an offset that means something else entirely.
	fmt.Fprintf(b, "master_replid:%s\r\n", replID)
	fmt.Fprintf(b, "master_repl_offset:%d\r\n", offset)
	fmt.Fprintf(b, "repl_backlog_size:%d\r\n", backlogSize)
	fmt.Fprintf(b, "repl_backlog_histlen:%d\r\n", histLen)
}

// infoKeyspace reports one line per non-empty database, in Redis's format, so an
// operator (or a monitoring agent that already parses Redis) can see where the keys
// are. An empty database is omitted, exactly as Redis omits it.
//
// db_keys is kept as the total across every database: it predates multiple databases,
// and a dashboard plotting it should not silently start plotting only database 0.
func infoKeyspace(s *Server, b *strings.Builder) {
	total := 0
	for i, db := range s.dbs {
		keys, expires := db.LenWithExpires()
		total += keys
		if keys == 0 {
			continue
		}
		fmt.Fprintf(b, "db%d:keys=%d,expires=%d,avg_ttl=0\r\n", i, keys, expires)
	}
	fmt.Fprintf(b, "db_keys:%d\r\n", total)
	fmt.Fprintf(b, "databases:%d\r\n", len(s.dbs))
}

func upDown(up bool) string {
	if up {
		return "up"
	}
	return "down"
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// cmdDBSize reports the live keys in the *calling connection's* database, which is
// what a client that has SELECTed database 3 is asking about. INFO keyspace is where
// the whole server's distribution is reported.
func cmdDBSize(s *Server, w *resp.Writer, args [][]byte) bool {
	w.WriteInt(int64(s.store.Len()))
	return false
}

// cmdFlushAll empties every database; cmdFlushDB empties only the caller's.
//
// Both propagate verbatim, and the distinction survives the trip: FLUSHDB arrives at a
// replica behind the SELECT that names the database it applied to, so the replica empties
// the same one. FLUSHALL names no database and needs none.
func cmdFlushAll(s *Server, w *resp.Writer, args [][]byte) bool {
	s.flushDatabases()
	w.WriteSimple("OK")
	return true
}

func cmdFlushDB(s *Server, w *resp.Writer, args [][]byte) bool {
	s.store.FlushAll()
	w.WriteSimple("OK")
	return true
}

// cmdSwapDB implements SWAPDB index1 index2: the two databases exchange their entire
// contents, so every client keeps its own index and finds the other dataset there.
//
// Cross-database commands are serialized against each other by crossDBMu. Each of them
// holds locks in two stores at once, so two running concurrently in opposite directions
// could deadlock; one mutex removes the possibility, and costs nothing, because these
// are administrative commands. Ordinary single-database commands cannot join such a
// cycle -- they never hold locks in two stores -- so they are unaffected.
func cmdSwapDB(s *Server, w *resp.Writer, args [][]byte) bool {
	if s.ClusterEnabled() {
		w.WriteError(errClusterNoDB("SWAPDB"))
		return false
	}
	i, ok1 := parseInt64(args[1])
	j, ok2 := parseInt64(args[2])
	if !ok1 || !ok2 {
		w.WriteError("ERR invalid first DB index")
		return false
	}
	n := int64(s.Databases())
	if i < 0 || i >= n || j < 0 || j >= n {
		w.WriteError("ERR DB index is out of range")
		return false
	}
	w.WriteSimple("OK")
	if i == j {
		return false // nothing changed, so nothing to propagate
	}
	s.crossDBMu.Lock()
	store.SwapData(s.dbs[i], s.dbs[j])
	s.crossDBMu.Unlock()
	return true
}

// cmdMove implements MOVE key db: move a key to another database, but only if the
// destination does not already hold that key.
//
// It propagates verbatim. The destination is absolute and the source is the database
// the stream is already positioned in, so a replica moves the same key between the same
// two databases.
func cmdMove(s *Server, w *resp.Writer, args [][]byte) bool {
	if s.ClusterEnabled() {
		w.WriteError(errClusterNoDB("MOVE"))
		return false
	}
	n, ok := parseInt64(args[2])
	if !ok {
		w.WriteError("ERR value is not an integer or out of range")
		return false
	}
	if n < 0 || n >= int64(s.Databases()) {
		w.WriteError("ERR DB index is out of range")
		return false
	}
	if int(n) == s.db {
		w.WriteError("ERR source and destination objects are the same")
		return false
	}
	s.crossDBMu.Lock()
	moved := store.TransferKey(s.store, s.dbs[n], string(args[1]), true, false)
	s.crossDBMu.Unlock()
	w.WriteInt(int64(boolToInt(moved)))
	return moved
}

// cmdCommand implements COMMAND [COUNT | INFO [name...] | DOCS [name...]], the
// introspection a client library performs on connect to learn what it may send.
//
// The key positions reported are coarse: this server has no per-command key
// specification, so a command with keys reports its first argument as its only key.
// That is enough for a standalone server -- what needs exact key specs is cluster key
// routing, which this server (mode "standalone") does not offer -- and it is reported
// as an approximation rather than fabricated in detail.
func cmdCommand(s *Server, w *resp.Writer, args [][]byte) bool {
	sub := ""
	if len(args) > 1 {
		sub = strings.ToUpper(string(args[1]))
	}
	switch sub {
	case "":
		// Bare COMMAND is COMMAND INFO for everything, as in Redis.
		writeCommandInfos(w, commandNames())

	case "COUNT":
		w.WriteInt(int64(len(commandTable)))

	case "INFO":
		names := commandNames()
		if len(args) > 2 {
			names = requestedNames(args[2:])
		}
		writeCommandInfos(w, names)

	case "DOCS":
		names := commandNames()
		if len(args) > 2 {
			names = requestedNames(args[2:])
		}
		writeCommandDocs(w, names)

	case "GETKEYS":
		// COMMAND GETKEYS <command> [arg...]: the keys the given command would act on.
		// A client library uses it to route or to lock, so it has to answer for the
		// commands whose keys are not at a fixed position -- which is why it consults
		// commandKeys rather than the coarse first/last/step triple COMMAND INFO reports.
		if len(args) < 3 {
			writeUnknownSubcommand(w, "COMMAND", args[1])
			return false
		}
		keys, errMsg := commandGetKeys(args[2:])
		if errMsg != "" {
			w.WriteError(errMsg)
			return false
		}
		writeStrings(w, keys)

	case "HELP":
		writeHelp(w, "COMMAND <subcommand> [<arg> [value] [opt] ...]. Subcommands are:", []string{
			"(no subcommand)",
			"    Return details about all commands.",
			"COUNT",
			"    Return the total number of commands in this server.",
			"DOCS [<command-name> ...]",
			"    Return documentation details about multiple commands.",
			"GETKEYS <full-command>",
			"    Return the keys from a full Redis command.",
			"INFO [<command-name> ...]",
			"    Return details about multiple commands.",
		})

	default:
		writeUnknownSubcommand(w, "COMMAND", args[1])
	}
	return false
}

// commandNames returns every registered command name, sorted so the reply is stable.
func commandNames() []string {
	names := make([]string, 0, len(commandTable))
	for name := range commandTable {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// requestedNames upper-cases the names a client asked about, keeping unknown ones so
// they can be reported as null entries (a client checking whether a command exists
// needs the placeholder to line up with its request).
func requestedNames(args [][]byte) []string {
	names := make([]string, 0, len(args))
	for _, a := range args {
		names = append(names, strings.ToUpper(string(a)))
	}
	return names
}

func writeCommandInfos(w *resp.Writer, names []string) {
	w.WriteArrayHeader(len(names))
	for _, name := range names {
		cmd, ok := commandTable[name]
		if !ok {
			w.WriteNullArray()
			continue
		}
		first, last, step := commandKeyRange(name, cmd)
		w.WriteArrayHeader(6)
		w.WriteBulk([]byte(cmd.lowerName))
		w.WriteInt(int64(cmd.arity))
		flags := commandFlags(cmd)
		w.WriteArrayHeader(len(flags))
		for _, f := range flags {
			w.WriteSimple(f)
		}
		w.WriteInt(int64(first))
		w.WriteInt(int64(last))
		w.WriteInt(int64(step))
	}
}

// writeCommandDocs answers COMMAND DOCS with a name -> attributes map: a real map of
// maps for a RESP3 client, flattened into name/value arrays for a RESP2 one.
// redis-cli asks for it on connect to build its help; the attributes reported are the
// ones that can be answered truthfully from the command table.
func writeCommandDocs(w *resp.Writer, names []string) {
	present := make([]string, 0, len(names))
	for _, name := range names {
		if _, ok := commandTable[name]; ok {
			present = append(present, name)
		}
	}
	w.WriteMapHeader(len(present))
	for _, name := range present {
		cmd := commandTable[name]
		w.WriteBulk([]byte(cmd.lowerName))
		// Only fields COMMAND DOCS actually defines, all string-valued.
		//
		// This previously emitted "arity" here, which belongs to COMMAND INFO, not DOCS --
		// and clients parse the two replies with different expectations. redis-py walks the
		// DOCS map converting known fields, and an integer under a key it does not expect
		// made it raise `ValueError: invalid literal for int() with base 10` before the
		// application's first command. Every field of DOCS is optional, so reporting fewer
		// honest ones is correct where inventing structure is not.
		w.WriteMapHeader(3)
		w.WriteBulk([]byte("summary"))
		w.WriteBulk([]byte(commandSummary(cmd)))
		w.WriteBulk([]byte("since"))
		w.WriteBulk([]byte(commandSince(cmd)))
		w.WriteBulk([]byte("group"))
		w.WriteBulk([]byte(commandGroup(name)))
	}
}

// commandSince is the "since" DOCS field: the version in which a command appeared.
//
// This server does not track per-command history, and inventing one would be a fiction a
// client could branch on. Reporting the compatibility level says the honest thing -- "this
// is available at the level this server implements" -- without claiming a Redis release
// history it does not have.
func commandSince(cmd *command) string { return RedisCompatVersion }

// commandGroup classifies a command the way COMMAND DOCS and COMMAND INFO's ACL
// categories do. It is derived from the name rather than stored on the table entry: the
// grouping is a documentation concern, and threading a field through 190-odd
// registrations to serve one introspection reply would put the cost on every command.
func commandGroup(name string) string {
	switch {
	case strings.HasPrefix(name, "X"):
		return "stream"
	case strings.HasPrefix(name, "PF"):
		return "hyperloglog"
	case strings.HasPrefix(name, "GEO"):
		return "geo"
	case strings.HasPrefix(name, "CLUSTER"), name == "ASKING", name == "READONLY", name == "READWRITE":
		return "cluster"
	case strings.HasPrefix(name, "SUBSCRIBE"), strings.HasPrefix(name, "PSUBSCRIBE"),
		strings.HasPrefix(name, "UNSUBSCRIBE"), strings.HasPrefix(name, "PUNSUBSCRIBE"),
		strings.HasPrefix(name, "PUBSUB"), name == "PUBLISH", name == "SPUBLISH":
		return "pubsub"
	case strings.HasPrefix(name, "Z"):
		return "sorted_set"
	case strings.HasPrefix(name, "H"):
		return "hash"
	case strings.HasPrefix(name, "SCRIPT"), strings.HasPrefix(name, "FUNCTION"),
		strings.HasPrefix(name, "EVAL"), strings.HasPrefix(name, "FCALL"):
		return "scripting"
	default:
		if group, ok := commandGroups[name]; ok {
			return group
		}
		return "generic"
	}
}

// commandGroups holds the commands whose group its name does not imply. The
// prefix rules in commandGroup cover the type families; these are the rest.
var commandGroups = map[string]string{
	"APPEND": "string", "DECR": "string", "DECRBY": "string", "GET": "string",
	"GETDEL": "string", "GETEX": "string", "GETRANGE": "string", "GETSET": "string",
	"INCR": "string", "INCRBY": "string", "INCRBYFLOAT": "string", "MGET": "string",
	"MSET": "string", "MSETNX": "string", "PSETEX": "string", "SET": "string",
	"SETEX": "string", "SETNX": "string", "SETRANGE": "string", "STRLEN": "string",
	"SUBSTR": "string", "LCS": "string",

	"SETBIT": "bitmap", "GETBIT": "bitmap", "BITCOUNT": "bitmap", "BITPOS": "bitmap",
	"BITOP": "bitmap", "BITFIELD": "bitmap", "BITFIELD_RO": "bitmap",

	"BLMOVE": "list", "BLMPOP": "list", "BLPOP": "list", "BRPOP": "list",
	"BRPOPLPUSH": "list", "LINDEX": "list", "LINSERT": "list", "LLEN": "list",
	"LMOVE": "list", "LMPOP": "list", "LPOP": "list", "LPOS": "list", "LPUSH": "list",
	"LPUSHX": "list", "LRANGE": "list", "LREM": "list", "LSET": "list", "LTRIM": "list",
	"RPOP": "list", "RPOPLPUSH": "list", "RPUSH": "list", "RPUSHX": "list",

	"SADD": "set", "SCARD": "set", "SDIFF": "set", "SDIFFSTORE": "set", "SINTER": "set",
	"SINTERCARD": "set", "SINTERSTORE": "set", "SISMEMBER": "set", "SMEMBERS": "set",
	"SMISMEMBER": "set", "SMOVE": "set", "SPOP": "set", "SRANDMEMBER": "set",
	"SREM": "set", "SSCAN": "set", "SUNION": "set", "SUNIONSTORE": "set",

	"AUTH": "connection", "CLIENT": "connection", "ECHO": "connection",
	"HELLO": "connection", "PING": "connection", "QUIT": "connection",
	"RESET": "connection", "SELECT": "connection",

	"BGREWRITEAOF": "server", "COMMAND": "server", "CONFIG": "server", "DBSIZE": "server",
	"DEBUG": "server", "FLUSHALL": "server", "FLUSHDB": "server", "INFO": "server",
	"LASTSAVE": "server", "LATENCY": "server", "MEMORY": "server", "MONITOR": "server",
	"REPLICAOF": "server", "SLAVEOF": "server", "SHUTDOWN": "server", "SLOWLOG": "server",
	"SWAPDB": "server",

	"DISCARD": "transactions", "EXEC": "transactions", "MULTI": "transactions",
	"UNWATCH": "transactions", "WATCH": "transactions",

	"PSYNC": "replication", "REPLCONF": "replication", "WAIT": "replication",
}

func commandSummary(cmd *command) string {
	switch {
	case cmd.sess != nil:
		return "Acts on the calling connection."
	case cmd.write:
		return "Modifies the dataset; persisted and replicated."
	default:
		return "Reads the dataset or reports server state."
	}
}

// commandFlags reports the flags a client cares about: whether the command writes (so
// it must not be sent to a replica) and whether it is a connection-control command
// (which a client must not pipeline behind a transaction).
//
// denyoom is reported for the writes that are actually refused when the server is over its
// byte budget and cannot evict, and withheld from the ones that are not. It used to be
// claimed for every write, which was wrong in the direction that matters: a client reading
// the flags would have believed DEL and EXPIRE could be refused under memory pressure --
// the two commands an operator recovers with -- and a well-behaved client library that
// pre-emptively avoids denyoom commands would have removed its own way out. The set is
// oomSafeWrite's complement, so the flag a client reads and the decision the server takes
// come from one place.
func commandFlags(cmd *command) []string {
	if cmd.write {
		if !oomDenied(cmd.lowerName) {
			return []string{"write"}
		}
		return []string{"write", "denyoom"}
	}
	if cmd.sess != nil {
		return []string{"fast", "loading", "no-auth-none"}
	}
	return []string{"readonly"}
}

// keylessCommands take no key argument at all, so they report a key range of zero.
// Everything else is reported as taking its first argument as a key, the approximation
// documented on cmdCommand.
var keylessCommands = map[string]bool{
	"PING": true, "INFO": true, "DBSIZE": true, "FLUSHALL": true, "COMMAND": true,
	"CONFIG": true, "CLIENT": true, "HELLO": true, "AUTH": true, "QUIT": true,
	"DEBUG": true, "FLUSHDB": true, "SWAPDB": true,
	"SLOWLOG": true, "LATENCY": true, "MONITOR": true,
	// The stream commands whose keys are not at argument 1: XREAD and XREADGROUP put
	// theirs after a STREAMS keyword, XGROUP and XINFO after a subcommand. COMMAND
	// GETKEYS answers all four exactly (see commandKeys).
	"XREAD": true, "XREADGROUP": true, "XGROUP": true, "XINFO": true,
	// MEMORY USAGE names a key, but at argument 2 rather than 1, so the fixed
	// first/last/step triple cannot express it. COMMAND GETKEYS answers it exactly.
	"MEMORY": true,
	"RESET":  true, "SELECT": true, "SHUTDOWN": true, "LASTSAVE": true,
	"BGREWRITEAOF": true, "REPLICAOF": true, "SLAVEOF": true, "REPLCONF": true,
	"WAIT": true, "SCAN": true, "RANDOMKEY": true, "KEYS": true,
	// The MPOP family carries a key count as an operand, so its keys are not at a
	// fixed position at all. Redis reports first_key 0 for exactly that reason
	// (its own flag for it is "movablekeys"), and so does this.
	"LMPOP": true, "BLMPOP": true, "ZMPOP": true, "BZMPOP": true,
	"SUBSCRIBE": true, "UNSUBSCRIBE": true, "PSUBSCRIBE": true, "PUNSUBSCRIBE": true,
	"PUBLISH": true, "PUBSUB": true,
	"MULTI": true, "EXEC": true, "DISCARD": true, "UNWATCH": true,
	// The cluster surface. CLUSTER's arguments are subcommands, slot numbers and node
	// ids, and CLUSTER KEYSLOT's operand is a key only in the sense that it is a string
	// to hash -- reporting it as a key would have the cluster redirect check redirect
	// CLUSTER KEYSLOT itself, which every node must be able to answer.
	"CLUSTER": true, "ASKING": true, "READONLY": true, "READWRITE": true,
}

func commandKeyRange(name string, cmd *command) (first, last, step int) {
	if keylessCommands[name] {
		return 0, 0, 0
	}
	switch name {
	case "MSET", "MGET", "DEL", "UNLINK", "EXISTS", "TOUCH", "WATCH":
		// Every argument is a key. MSET's values sit between them, which is exactly what
		// a step of 2 expresses.
		if name == "MSET" {
			return 1, -1, 2
		}
		return 1, -1, 1
	}
	return 1, 1, 1
}

func cmdReplicaOf(s *Server, w *resp.Writer, args [][]byte) bool {
	host := string(args[1])
	port := string(args[2])
	if strings.EqualFold(host, "NO") && strings.EqualFold(port, "ONE") {
		s.ReplicaOf(s.baseCtx, "") // promote to master
	} else {
		s.ReplicaOf(s.baseCtx, host+":"+port)
	}
	w.WriteSimple("OK")
	return false
}

// SetShutdownHook registers what SHUTDOWN calls to stop the server. It is a hook
// rather than an os.Exit inside the handler so the process unwinds through its normal
// shutdown path: the caller cancels the serve context, Serve returns, and the deferred
// close of the AOF flushes its buffered tail. Exiting from here would skip that and
// discard writes a client was told were durable.
func (s *Server) SetShutdownHook(fn func()) {
	s.mu.Lock()
	s.shutdown = fn
	s.mu.Unlock()
}

// cmdShutdown implements SHUTDOWN [NOSAVE].
//
// There is no RDB here, so "saving" means flushing the append-only log: SHUTDOWN
// flushes it, SHUTDOWN NOSAVE does not. On success there is no reply at all, because
// the server is going away -- a client sees the connection close, which is exactly
// Redis's behaviour and the only honest answer to "did it shut down?".
func cmdShutdown(s *Server, sess *session, w *resp.Writer, args [][]byte) {
	save := true
	for _, arg := range args[1:] {
		switch strings.ToUpper(string(arg)) {
		case "NOSAVE":
			save = false
		case "SAVE", "NOW", "FORCE":
			// Accepted for compatibility: this server has no lagging background child to
			// wait for or force past, so they mean the same as a plain SHUTDOWN.
		default:
			w.WriteError("ERR syntax error")
			return
		}
	}
	s.mu.Lock()
	stop := s.shutdown
	s.mu.Unlock()
	if stop == nil {
		w.WriteError("ERR Errors trying to SHUTDOWN. Check logs.")
		return
	}
	if save && s.aof != nil {
		if err := s.aof.Flush(); err != nil {
			// Refuse to stop with unflushed writes rather than lose them silently. NOSAVE
			// is how an operator says they accept that loss.
			w.WriteError("ERR Errors trying to SHUTDOWN. Check logs.")
			return
		}
	}
	sess.quit = true
	stop()
}
