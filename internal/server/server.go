// Package server exposes a store.Store over TCP using the RESP protocol, so
// standard Redis clients can talk to it. Each connection is served by its own
// goroutine; the shared store is safe for concurrent access.
//
// Commands are looked up in a self-registering table (see register). Each entry
// declares an arity and a write flag; the write flag is what drives append-only
// persistence and replica propagation, so adding a new mutating command wires it
// into durability and replication automatically. A command whose own text is not
// replayable -- one that picks members at random -- registers with registerEffect
// instead and propagates the effect it had.
package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Black-third/shardkv/internal/aof"
	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

// handlerFunc handles one command. It writes the reply to w and returns whether
// it actually modified the dataset (dirty); only dirty writes are persisted and
// replicated.
type handlerFunc func(s *Server, w *resp.Writer, args [][]byte) (dirty bool)

// effectFunc handles one command whose propagated form is not its own text. It
// writes the reply and returns the commands that must reach the AOF and replicas
// instead -- the effect it had -- or nil when the dataset did not change.
//
// A command that picks members at random (SPOP) or depends on iteration order
// cannot ship verbatim: the replica running it would draw different members and
// the two copies would silently diverge. Real Redis rewrites SPOP into the SREM of
// exactly the members it removed, and so does this.
//
// The clock is a source of non-determinism on exactly the same terms, which is why the
// commands carrying a *relative* deadline are effect producers too (SET ... EX, EXPIRE,
// SETEX, GETEX, RESTORE, and the stream commands that mint an id). Such a handler has
// already resolved the deadline it wrote to memory; returning the absolute form built
// from that one value is what keeps the wire and memory naming the same instant. Deriving
// the wire form from a second reading of the clock, however careful it is about which
// clock, skews the two by the handler's own execution time -- see propagation.go.
type effectFunc func(s *Server, w *resp.Writer, args [][]byte) (effect [][][]byte)

// sessionFunc handles a command that acts on the calling connection rather than on
// the dataset: it authenticates, renames, subscribes, or tears down that one
// connection. Such a command needs the session, which a handlerFunc deliberately
// does not get -- threading it through all ~120 dataset handlers would hand every
// one of them the ability to mutate connection state it has no business touching.
type sessionFunc func(s *Server, sess *session, w *resp.Writer, args [][]byte)

type command struct {
	// lowerName is the name as COMMAND INFO and CLIENT LIST spell it. It is stored
	// (rather than lower-cased on demand) so recording the last command a connection
	// ran is a pointer store instead of an allocation on every dispatch.
	lowerName string
	arity     int // >0: exact arg count (incl. name); <0: at least -arity
	write     bool
	fn        handlerFunc
	effect    effectFunc  // set instead of fn for a command that propagates its effect
	sess      sessionFunc // set instead of fn for a connection-control command
	block     *blockSpec  // set instead of fn for a command that can wait for data

	// container marks a command whose first argument names a subcommand -- CONFIG, CLIENT,
	// OBJECT and the rest. Statistics for those are kept per *subcommand*, because that is
	// what Redis reports (`cmdstat_config|resetstat`, never `cmdstat_config`) and because a
	// single figure for CONFIG would average a CONFIG GET against a CONFIG RESETSTAT.
	//
	// It is a plain bool read from the entry the dispatcher already holds, so a command that
	// is not a container -- every data command -- pays one branch and nothing else. That is
	// the whole reason this is a flag rather than "look up the name in a set of containers".
	container bool
	// protected marks a command that is refused unless the configuration says otherwise --
	// DEBUG, and in Redis also MODULE, which this server does not have. It is a flag on the
	// entry rather than a name the dispatcher compares against for the same reason container
	// is: the dispatcher already holds the entry, so the gate costs one bool read on the path
	// of every command instead of a string comparison. See debugCommandAllowed.
	protected bool
	// subs holds one statistics-only *command per subcommand seen, created on demand under
	// subMu. The lock and the map lookup are affordable precisely because they are only ever
	// reached for the administrative commands: nothing on a data path takes them.
	subMu sync.Mutex
	subs  map[string]*command

	// The per-command statistics INFO commandstats and INFO latencystats report. They
	// live on the table entry, as atomics, because that is what makes recording them
	// free: a dispatch already holds the *command, so accounting is two atomic adds
	// and a third into a histogram bucket, with no map lookup, no lock and no
	// allocation. See observability.go.
	calls    atomic.Int64
	usec     atomic.Int64
	rejected atomic.Int64 // refused before execution (arity, wrong role, unknown subcommand)
	failed   atomic.Int64 // executed and answered with an error
	latency  [latencyBuckets]atomic.Int64
}

// blockSpec describes a command that waits for data rather than answering "nothing
// there" -- BLPOP and its relatives. See blocking.go for the mechanism.
type blockSpec struct {
	// fn is the command's non-blocking half, attempted once on arrival and again on
	// every wakeup. It is an effect producer, because what propagates is the pop that
	// happened and never the blocking command itself: a replica replaying BLPOP would
	// wait forever on a connection with no client behind it.
	fn blockFunc
	// keys reports the keys whose arrival can serve this command, which are the queues
	// the waiter joins. It is only ever called after fn has reported that it wants to
	// wait, so it may assume the arguments parsed.
	keys func(args [][]byte) []string
	// timeout is the index of the timeout operand, or -1 for the last argument. It is
	// used only when timeoutFn is nil.
	timeout int
	// timeoutFn parses the wait for a command whose timeout is not a positional operand
	// in fractional seconds. XREAD's is neither: it sits after a BLOCK keyword that may
	// not be there at all, and it is expressed in milliseconds -- a difference in Redis's
	// own interface that a compatible server has to keep rather than normalize away.
	timeoutFn func(args [][]byte) (time.Duration, string)
	// empty writes the reply for "nothing to serve": what a timeout answers with, and
	// also what the command answers with inside MULTI, where it must not block.
	empty func(w *resp.Writer)
	// readOnly marks a command that can wait for data but changes nothing when it is
	// served -- XREAD, which reads a stream. Such a command is not refused on a replica
	// and never touches the write path: there is no effect to order, persist or
	// replicate, so "it served the client" cannot be inferred from "it produced an
	// effect" the way it can for BLPOP. See tryBlocking.
	readOnly bool
	// wantType is the data type a wakeup must find for this command to be retried: a list
	// for BLPOP, a sorted set for BZPOPMIN, a stream for XREAD.
	//
	// It exists because a waiter is woken by anything that creates one of its keys, and what
	// created it may be the wrong kind of thing entirely -- a client blocked in BZPOPMIN is
	// woken by the SET in `ZADD k 0 x; DEL k; SET k v`. Redis checks the type at the wakeup
	// and leaves such a client blocked; retrying the command instead would answer WRONGTYPE
	// for a value that is already gone by the time the client reads the error, failing a
	// command whose own arguments were never wrong. See retryBlocking.
	wantType string
	// wakeOnAnyChange makes every wakeup worth a retry, whatever the key now holds. Only
	// XREADGROUP sets it, and the reason is that its waiter depends on state *attached to*
	// the key rather than on the key's contents: a consumer group. A key that was deleted or
	// overwritten with another type has destroyed the group, so the client is not waiting for
	// data any more -- it is waiting for something that can never arrive, and it has to be
	// told (NOGROUP, or WRONGTYPE) rather than parked forever. Redis unblocks an XREADGROUP on
	// exactly these changes and no other blocking command on them.
	wakeOnAnyChange bool
	// silentEffect suppresses the keyspace notification a served effect would
	// otherwise report.
	//
	// A blocking command is normally *described* by its effect, because its own
	// arguments are a list of candidate keys and only the effect names the one that
	// changed: BLPOP's effect is an LPOP, and "lpop" is exactly the event Redis
	// reports. XREADGROUP breaks that correspondence. Its effect is an XGROUP SETID
	// -- an internal advance of the group's last-delivered id, not something the
	// client asked for -- and reporting it would emit an "xgroup-setid" event that
	// real Redis never sends for a read. The keys are still tracked for WATCH and for
	// waiter wakeups; only the event is withheld.
	silentEffect bool
}

// blockFunc is the non-blocking half of a blocking command. It returns the effect to
// propagate when it served data (having written the reply), (nil, false) when it
// rejected the arguments (having written the error), and (nil, true) when there was
// nothing to serve and nothing was written -- the only case in which the caller may
// wait.
type blockFunc func(s *Server, w *resp.Writer, args [][]byte) (effect [][][]byte, wait bool)

// apply runs the command, writes its reply, and reports whether the dataset
// changed. effect is non-nil only for a command registered with registerEffect,
// in which case the caller propagates it in place of the command itself.
func (c *command) apply(s *Server, w *resp.Writer, args [][]byte) (dirty bool, effect [][][]byte) {
	if c.block != nil {
		// No connection is waiting on this one: it is running inside a MULTI, or being
		// replayed from an AOF or a master's stream. All three take the command's
		// non-blocking behaviour, which is what Redis does inside a transaction -- an EXEC
		// that could block would hold the whole batch, and every other client behind it.
		effect, wait := c.block.fn(s, w, args)
		if wait {
			c.block.empty(w)
		}
		return len(effect) > 0, effect
	}
	if c.effect == nil {
		return c.fn(s, w, args), nil
	}
	effect = c.effect(s, w, args)
	return len(effect) > 0, effect
}

// wireForm is the sequence a dirty write ships to the AOF and replicas: the effect a
// non-deterministic handler produced, or else the command itself.
//
// It deliberately takes no clock. A command whose text depends on the clock -- anything
// carrying a relative deadline -- resolves that deadline once, in its handler, and returns
// the absolute form built from it; there is no second derivation here that could disagree
// with what the handler wrote to memory. Everything else propagates verbatim, which is
// exactly what "its own text is replayable" means.
func wireForm(args [][]byte, effect [][][]byte) [][][]byte {
	if effect != nil {
		return effect
	}
	return [][][]byte{args}
}

var commandTable = map[string]*command{}

// register adds a command to the table. Called from init() in the commands_*.go
// files so each group of commands wires itself in.
func register(name string, arity int, write bool, fn handlerFunc) {
	commandTable[name] = &command{
		lowerName: strings.ToLower(name), arity: arity, write: write, fn: fn,
		container: containerCommands[name], protected: protectedCommands[name],
	}
}

// registerEffect adds a write command that propagates the effect it had rather
// than its own text. See effectFunc.
func registerEffect(name string, arity int, fn effectFunc) {
	commandTable[name] = &command{
		lowerName: strings.ToLower(name), arity: arity, write: true, effect: fn,
		container: containerCommands[name],
	}
}

// registerBlocking adds a command that can wait for data to arrive. It is a write, so
// it is refused on a replica and its effect is persisted and replicated; what makes it
// different is that execute -- the only caller with a connection to park -- runs it
// through blockUntilServed instead of dispatch. See blockSpec.
func registerBlocking(name string, arity int, spec *blockSpec) {
	commandTable[name] = &command{
		lowerName: strings.ToLower(name), arity: arity, write: !spec.readOnly, block: spec,
		container: containerCommands[name],
	}
}

// registerSession adds a command that acts on the calling connection. Such a
// command is never a write, so it is never persisted or propagated: what it changes
// lives and dies with one client socket. See sessionFunc.
func registerSession(name string, arity int, fn sessionFunc) {
	commandTable[name] = &command{
		lowerName: strings.ToLower(name), arity: arity, sess: fn,
		container: containerCommands[name],
	}
}

// containerCommands are the commands that dispatch on a subcommand, and whose statistics are
// therefore kept per subcommand.
//
// The set is written out rather than inferred from arity, because "arity is negative and the
// first argument happens to be a word" describes plenty of commands that are not containers
// (SET, GEOADD). Getting it wrong in that direction would file every SET under its first
// *value*, which is both useless and unbounded.
//
// It is consulted by register rather than applied by a later pass, deliberately: a pass would
// have to run after every commands_*.go init, and package init order is file-name order --
// so a container registered in a file sorting after the pass (PUBSUB, in pubsub.go) would be
// silently missed. Reading the set at registration cannot be got wrong that way.
var containerCommands = map[string]bool{
	"CONFIG": true, "CLIENT": true, "COMMAND": true, "OBJECT": true, "MEMORY": true,
	"LATENCY": true, "SLOWLOG": true, "DEBUG": true, "CLUSTER": true, "PUBSUB": true,
	"XINFO": true, "XGROUP": true,
}

// statsTarget is the entry a command's measurements are recorded against: the table entry
// itself, or -- for a container command -- a per-subcommand entry created on demand.
//
// An unknown or missing subcommand falls back to the parent, which is where a rejection has
// to land: the subcommand could not be resolved, so there is nothing more specific to
// attribute it to.
func (c *command) statsTarget(args [][]byte) *command {
	if !c.container || len(args) < 2 {
		return c
	}
	sub := strings.ToLower(string(args[1]))
	c.subMu.Lock()
	defer c.subMu.Unlock()
	if e := c.subs[sub]; e != nil {
		return e
	}
	if c.subs == nil {
		c.subs = make(map[string]*command)
	}
	e := &command{lowerName: c.lowerName + "|" + sub}
	c.subs[sub] = e
	return e
}

// subStats returns the container's per-subcommand entries, sorted by name so every reply
// built from them is stable rather than following map iteration order.
func (c *command) subStats() []*command {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	out := make([]*command, 0, len(c.subs))
	for _, e := range c.subs {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].lowerName < out[j].lowerName })
	return out
}

// statsEntries lists every entry statistics are reported for, in a stable order: each
// command, and for a container its subcommands in place of it.
//
// A container's own entry is still reported when it has *rejections* -- a CONFIG with an
// unknown subcommand is a refusal that belongs to CONFIG, because nothing narrower could be
// resolved -- and skipped when it has none, which is what keeps `cmdstat_config` out of the
// reply the way Redis keeps it out.
func statsEntries() []*command {
	var out []*command
	for _, name := range commandNames() {
		c := commandTable[name]
		if !c.container {
			out = append(out, c)
			continue
		}
		if c.rejected.Load() > 0 || c.calls.Load() > 0 {
			out = append(out, c)
		}
		out = append(out, c.subStats()...)
	}
	return out
}

// errReadOnly is the reply a write gets on a replica. Every write path shares the one
// constant so a client matching on the prefix sees the same message wherever the
// refusal comes from.
const errReadOnly = "READONLY You can't write against a read only replica."

func arityOK(arity, n int) bool {
	if arity >= 0 {
		return n == arity
	}
	return n >= -arity
}

// replicaConn is the master's view of a connected replica: a buffered channel of
// commands drained by the replica's serving goroutine, plus a one-shot signal the
// master uses to end that feed.
type replicaConn struct {
	ch      chan [][]byte
	dropped chan struct{} // closed to end the feed and force the replica to resync
	once    sync.Once

	// ack is the offset the replica last reported having processed, which is what
	// WAIT counts and INFO reports. It is atomic because the feed's reader goroutine
	// writes it while other connections read it.
	ack  atomic.Int64
	addr string // the replica's peer address, for INFO
	port string // the port it announced with REPLCONF listening-port
}

func newReplicaConn(buffer int) *replicaConn {
	return &replicaConn{ch: make(chan [][]byte, buffer), dropped: make(chan struct{})}
}

// drop ends the feed. handleSync notices and returns, which closes the
// connection, so the replica's replication loop reconnects and takes a fresh full
// snapshot. Safe to call more than once.
func (rc *replicaConn) drop() { rc.once.Do(func() { close(rc.dropped) }) }

// Server is what every command handler is given, and it addresses exactly one
// database: everything shared by the whole process lives behind the embedded
// serverCore, while store and db say *which* keyspace this handler is working on.
//
// The two-part shape is what makes SELECT possible without threading a database
// index through 135 handler signatures. A connection's database is resolved once,
// in execute, to one of a fixed set of per-database views (see forDB); from then on
// `s.store` is that database's keyspace and `s.db` is its index, so a handler that
// never mentions databases operates on the right one, and a method that has to know
// -- propagation, WATCH invalidation, keyspace notifications, blocked-client wakeups
// -- reads s.db. The views are built once at startup and never allocated per
// command.
//
// The struct is deliberately copyable (a pointer, a pointer and an int); every lock
// and counter lives in serverCore, which is only ever shared.
type Server struct {
	*serverCore

	// store is this view's database and db is its index. A handler uses store; the
	// paths that must record which database a change happened in use db.
	store *store.Store
	db    int
}

// serverCore is the state shared by every database view: the listener, the
// replication and Pub/Sub registries, the configuration, and the databases
// themselves.
type serverCore struct {
	// dbs are the databases, each an independent sharded keyspace, and views[i] is the
	// Server that addresses dbs[i]. Both are fixed after startup.
	//
	// N independent keyspaces was chosen over a database dimension inside each shard so
	// that databases do not contend: a client working in database 1 takes locks that no
	// client in database 0 can ever want, which is the same reasoning that made the
	// keyspace sharded in the first place. It also leaves the store completely unaware
	// that databases exist -- SELECT is a server-level concept and stays one -- at the
	// cost of one map and mutex per shard per database, which is a few kilobytes for
	// the default 16 x 256.
	dbs   []*store.Store
	views []*Server

	// cluster is the slot map, the node table and the migration states (see cluster.go).
	// It is always allocated, so no path has to check for nil; whether any of it applies
	// is one atomic load on cluster.enabled, which is false for a standalone server.
	cluster *clusterState

	// crossDBMu serializes the three commands that touch two databases at once (SWAPDB,
	// MOVE and COPY ... DB). Each of them holds a lock in each of two stores, so two of
	// them running in opposite directions could deadlock; one mutex over all of them
	// removes the possibility. It costs nothing, because they are administrative
	// commands, and it does not affect ordinary commands at all -- a single-database
	// command never holds locks in two stores, so it cannot be part of such a cycle.
	crossDBMu sync.Mutex

	ln net.Listener
	wg sync.WaitGroup

	aof *aof.Log // nil if persistence is disabled

	// AOF rewrite state and policy (see aof_rewrite.go). aofRewriting doubles as the
	// mutual exclusion between rewrites and as what INFO reports as
	// aof_rewrite_in_progress; aofRewriteOK is the last outcome.
	aofRewriting      atomic.Bool
	aofRewriteOK      atomic.Bool
	aofRewriteMinSize atomic.Int64
	aofRewritePerc    atomic.Int64
	lastSave          atomic.Int64 // Unix time of the last full dataset write

	// snapshot is the point-in-time snapshot state: the file path, the save schedule and
	// the counters INFO persistence reports (see snapshot.go). It is one lazily allocated
	// pointer rather than nine fields here so a server that never saves costs one atomic
	// load, and so the whole feature can be read in one file.
	snapshot atomic.Pointer[snapshotState]

	// tlsConfig wraps the listener when set; masterTLS is used when dialing a master.
	// Both nil means plain TCP in both directions, which is the default. tlsOpts keeps
	// the files they were built from so CONFIG GET can report what was loaded. All are
	// set before serving starts and never change afterwards.
	tlsConfig   *tls.Config
	masterTLS   *tls.Config
	tlsOpts     TLSOptions
	masterTLSOn bool

	// shutdown is what SHUTDOWN invokes to stop the process. It is a hook rather than
	// an os.Exit so the command unwinds through the normal shutdown path -- which is
	// what flushes and closes the AOF, the difference between a clean stop and losing
	// the log's buffered tail.
	shutdown func()

	// propagating becomes true once durability or replication is active (AOF
	// attached or a replica connected). While it is true, write commands are
	// serialized through propMu across the store mutation AND the propagation, so
	// the order applied to memory is the exact order written to the AOF and the
	// replica stream. A point-in-time snapshot is taken under propMu, so an
	// initial replica sync sees a consistent cut with no double-apply. When it is
	// false (pure single-node cache), writes stay sharded-concurrent.
	propagating atomic.Bool
	propMu      sync.Mutex

	// streamDB is the database the AOF and replica stream are currently positioned in,
	// i.e. the index of the last SELECT put on the stream. It is guarded by propMu, the
	// same lock that orders the writes themselves, so the position of the stream and the
	// command that follows it are decided together or not at all. See selectOnStream.
	streamDB int

	mu         sync.Mutex
	role       string // "master" or "replica"
	masterAddr string
	replicas   map[*replicaConn]struct{}
	replCancel context.CancelFunc // cancels the active replication loop, if any

	// replOffset counts the bytes this server has put on its replication stream, and
	// backlog retains that stream's tail for partial resync. Both are guarded by mu,
	// the same lock that hands a command to the replica feeds, so a replica's
	// continuation point and the stream it continues into cannot disagree.
	//
	// replID names this server's stream. A replica presents it back in PSYNC, which is
	// how the master knows the offset it asks for refers to the same history: an ID it
	// does not recognize (a restarted master, or a different one) can only be answered
	// with a full resync.
	replOffset int64
	backlog    *replBacklog
	replID     string

	// The replica's view of its own link, also guarded by mu: the ID of the history it
	// is following, how far into it this server has got, and whether the link is
	// currently up. masterReplID empty means there is no continuation point, so the
	// next PSYNC must ask for a full resync.
	masterReplID string
	slaveOffset  int64
	masterLinkUp bool

	watchMu  sync.Mutex
	watchers map[dbKey]map[*session]struct{} // (database, key) -> sessions WATCHing it

	// The blocking-command registry (see blocking.go): per-key FIFO queues of waiting
	// clients, plus an index by client id for CLIENT UNBLOCK. blockMu is a leaf lock --
	// nothing else is acquired while it is held -- so it cannot participate in a cycle
	// with propMu, mu or watchMu. blockedCount mirrors len(blockByID) so the write path
	// can rule out the whole feature with one atomic load.
	blockMu      sync.Mutex
	blockQueues  map[dbKey][]*waiter
	blockByID    map[int64]*waiter
	blockedCount atomic.Int64

	// requirepass is the password AUTH must present; empty leaves the server open.
	// masterauth is the password this server presents when it connects to a master.
	// Both are atomic pointers rather than mutex-guarded strings because every
	// command reads requirepass on the hot path while CONFIG SET writes it rarely.
	requirepass atomic.Pointer[string]
	masterauth  atomic.Pointer[string]

	// clientMu guards the connected-client registry that backs CLIENT LIST/KILL. It
	// is a leaf lock: nothing else is acquired while it is held, so it can never
	// participate in a cycle with propMu, mu or watchMu.
	clientMu sync.Mutex
	clients  map[int64]*session
	nextID   atomic.Int64

	// The bounds on that registry (see limits.go). liveConns is the accept loop's own
	// count of open connections, which is what maxClients is compared against;
	// clientTimeout is the idle disconnect in seconds, 0 for off.
	maxClients    atomic.Int64
	clientTimeout atomic.Int64
	liveConns     atomic.Int64
	rejectedConns atomic.Int64

	// enableDebugCmd is the enable-debug-command setting: whether DEBUG may be run, and
	// by whom. Its zero value is "no", so a Server nobody configured refuses DEBUG --
	// which is the safe direction for a gate, and the direction Redis 7 defaults to. See
	// commands_debug.go.
	enableDebugCmd atomic.Int32

	// dirtyChanges counts writes that changed something since the dataset was last
	// written out in full, which is what INFO's rdb_changes_since_last_save reports.
	dirtyChanges atomic.Int64

	// streamNodeMaxEntries and listCompressDepth are stored and not read: see
	// SetStreamNodeMaxEntries.
	streamNodeMaxEntries atomic.Int64
	listCompressDepth    atomic.Int64

	// subMu guards the Pub/Sub registries: channel or pattern -> subscribed sessions.
	// Like clientMu it is a leaf lock -- a publisher collects the subscribers to drop
	// under it and closes their connections after releasing it.
	subMu       sync.Mutex
	channels    map[string]map[*session]struct{}
	patterns    map[string]map[*session]struct{}
	pubsubDrops atomic.Int64 // subscribers hung up on for falling too far behind

	// notifyFlags is the set of enabled keyspace-notification classes (see notify.go).
	// Zero means the feature is off, which the write path tests with a single atomic
	// load before doing any work at all.
	notifyFlags atomic.Int32

	// The observability state (see observability.go): the slow-log ring, the latency
	// time series, and the registry of MONITOR feeds. Each is guarded by its own leaf
	// lock, and each is gated by an atomic that lets the command path rule the whole
	// feature out without touching the lock.
	slowlogMu        sync.Mutex
	slowlog          []slowlogEntry // newest first
	slowlogNextID    atomic.Int64
	slowlogThreshold atomic.Int64 // microseconds; <0 disables, 0 logs everything
	slowlogMaxLen    atomic.Int64

	latencyMu        sync.Mutex
	latencyEvents    map[string]*latencyTimeSeries
	latencyThreshold atomic.Int64 // milliseconds; 0 disables the latency monitor

	monitorMu    sync.Mutex
	monitors     map[*session]*monitorSink
	monitorCount atomic.Int64 // mirrors len(monitors) so the command path can skip it
	monitorDrops atomic.Int64

	// Replication liveness timings. They are fields rather than constants so tests
	// can shrink them; they must be set before serving starts.
	pingEvery   time.Duration // how often each replica feed sends a keepalive
	readTimeout time.Duration // how long a replica waits on a silent master

	baseCtx      context.Context // lifetime ctx for replication started via REPLICAOF
	startTime    time.Time
	totalConns   atomic.Int64
	totalCmds    atomic.Int64
	fullSyncs    atomic.Int64 // PSYNC full resyncs served
	partialOK    atomic.Int64 // PSYNC continuations served from the backlog
	partialErr   atomic.Int64 // continuations asked for that the backlog could not serve
	replicaDrops atomic.Int64 // replicas dropped for falling too far behind
}

// dbKey names a key in a particular database. WATCH registrations and blocked-client
// queues are keyed by it rather than by the key alone, because "foo" in database 0 and
// "foo" in database 1 are different keys and a change to one must not disturb a client
// watching or waiting on the other.
type dbKey struct {
	db  int
	key string
}

// defaultDatabases is the number of databases a server has unless -databases says
// otherwise. It is Redis's default, so a client that assumes SELECT 0..15 works finds
// what it expects.
const defaultDatabases = 16

// New returns a master Server with a single database backed by st. Call SetDatabases
// to give it more. It must be called before the store's janitor goroutine starts,
// since it registers the store removal hook.
func New(st *store.Store) *Server {
	core := &serverCore{
		dbs:         []*store.Store{st},
		role:        "master",
		replicas:    make(map[*replicaConn]struct{}),
		watchers:    make(map[dbKey]map[*session]struct{}),
		blockQueues: make(map[dbKey][]*waiter),
		blockByID:   make(map[int64]*waiter),
		clients:     make(map[int64]*session),
		channels:    make(map[string]map[*session]struct{}),
		patterns:    make(map[string]map[*session]struct{}),
		pingEvery:   replPingInterval,
		readTimeout: replReadTimeout,
		baseCtx:     context.Background(),
		startTime:   time.Now(),
		backlog:     newReplBacklog(replBacklogSize),
		replID:      newReplID(),
		cluster:     newClusterState(),
	}
	s := &Server{serverCore: core, store: st, db: 0}
	core.views = []*Server{s}
	s.SetAOFRewritePolicy(defaultAOFRewriteMinSize, defaultAOFRewritePerc)
	s.SetSlowlogPolicy(defaultSlowlogThresholdUs, defaultSlowlogMaxLen)
	s.SetMaxClients(defaultMaxClients)
	s.SetStreamNodeMaxEntries(defaultStreamNodeMaxEntries)
	s.aofRewriteOK.Store(true) // no rewrite has failed yet
	s.lastSave.Store(s.startTime.Unix())
	st.SetRemovalHook(s.onKeyRemoved)
	return s
}

// SetDatabases gives the server n databases, numbered 0 to n-1. Database 0 is the one
// New was given; the rest are created here with the same shard count, clock and
// eviction cap, so a SELECTed database behaves exactly like database 0.
//
// It must be called before serving starts and before the janitors are started, since
// it creates the stores those janitors sweep and registers each one's removal hook.
// Shrinking is refused: a client could already be using a database that would vanish.
func (s *Server) SetDatabases(n int) error {
	if n < 1 {
		return fmt.Errorf("databases must be at least 1, got %d", n)
	}
	if n < len(s.dbs) {
		return fmt.Errorf("cannot reduce the number of databases from %d to %d", len(s.dbs), n)
	}
	db0 := s.dbs[0]
	for i := len(s.dbs); i < n; i++ {
		st := store.New(db0.Shards())
		// The clock is db 0's, not time.Now: a test that injected a clock did so to make
		// expiry deterministic, and a database reading a different clock would expire its
		// keys at different instants from the one the propagated deadlines were computed
		// against.
		st.SetClock(db0.Now)
		st.SetMaxKeys(db0.MaxKeys())
		view := &Server{serverCore: s.serverCore, store: st, db: i}
		st.SetRemovalHook(view.onKeyRemoved)
		s.dbs = append(s.dbs, st)
		s.views = append(s.views, view)
	}
	return nil
}

// Databases reports how many databases this server has, for CONFIG GET databases and
// for the SELECT range check.
func (s *Server) Databases() int { return len(s.dbs) }

// DB returns the store backing database i, for the janitor goroutines the caller
// starts. It panics for an out-of-range index, which can only be a programming error:
// the range is fixed before serving starts.
func (s *Server) DB(i int) *store.Store { return s.dbs[i] }

// forDB returns the view addressing database i. The views are built once by
// SetDatabases, so resolving a connection's database costs an index and no allocation.
func (s *Server) forDB(i int) *Server {
	if i < 0 || i >= len(s.views) {
		return s.views[0]
	}
	return s.views[i]
}

// onKeyRemoved handles a key the store removed on its own. It always invalidates
// any WATCH on the key and reports the expired/evicted keyspace notification -- this
// hook is the only place that learns about a key nothing ever read again. For an
// eviction it also propagates a synthetic DEL so replicas/AOF converge (an evicted key
// has no TTL, so they cannot drop it themselves); an expiration needs no DEL because
// its absolute deadline was already propagated, so replicas and AOF replay expire it
// independently. The expiration path therefore never touches propMu, which keeps it
// safe to fire from a read evaluated while a transaction holds propMu.
func (s *Server) onKeyRemoved(key string, evicted bool) {
	args := [][]byte{[]byte("DEL"), []byte(key)}
	s.touchWatchers(args)
	s.notifyRemoved(key, evicted)
	if evicted && s.propagating.Load() {
		// Shipped as-is: a DEL names no deadline, so there is nothing to rewrite. It still
		// goes through shipRaw rather than shipCommand so the stream is positioned in the
		// database the key was evicted from.
		s.propMu.Lock()
		s.shipRaw(args)
		s.propMu.Unlock()
	}
}

// AttachAOF wires an append-only log so write commands are persisted. Enabling
// persistence also turns on write serialization (propagating).
func (s *Server) AttachAOF(log *aof.Log) {
	s.aof = log
	s.propagating.Store(true)
}

// ReplayCommands applies a recorded command stream (e.g. from the AOF) to the
// store without persisting or propagating it. Used once at startup. A
// transaction is applied atomically: a MULTI..EXEC group is buffered and only
// applied on EXEC, so a crash that truncated the AOF mid-transaction (no EXEC)
// replays none of it.
func (s *Server) ReplayCommands(cmds [][][]byte) {
	a := newTxApplier(s, resp.NewWriter(io.Discard))
	for _, cmd := range cmds {
		a.feed(cmd)
	}
}

// txApplier applies a command stream with MULTI/EXEC awareness: commands between
// a MULTI and its EXEC are buffered and applied together; if the stream ends (or
// a DISCARD arrives) before EXEC, the buffer is dropped, giving all-or-nothing
// transaction replay.
type txApplier struct {
	// s is the database view the stream's SELECTs have positioned the applier in, and
	// root is the server itself, needed to resolve the next SELECT.
	s       *Server
	root    *Server
	w       *resp.Writer
	inMulti bool
	buf     [][][]byte
}

func newTxApplier(s *Server, w *resp.Writer) *txApplier {
	// A stream always begins in database 0, which is why a stream that only ever touched
	// database 0 contains no SELECT at all.
	return &txApplier{s: s.forDB(0), root: s, w: w}
}

func (a *txApplier) feed(args [][]byte) {
	if len(args) == 0 {
		return
	}
	switch strings.ToUpper(string(args[0])) {
	case "SELECT":
		// The stream's database marker. It is state for the *applier*, not a command to
		// apply: SELECT acts on a connection, and this stream has none. Directing the
		// commands that follow into the database it names is the whole of its effect.
		//
		// It is honoured immediately rather than buffered with a transaction, and inside a
		// MULTI as well as outside one: a transaction whose writes spanned two databases
		// would otherwise replay entirely into whichever one it started in.
		if len(args) == 2 {
			if n, ok := parseInt64(args[1]); ok && n >= 0 && int(n) < a.root.Databases() {
				a.s = a.root.forDB(int(n))
			}
		}
	case "MULTI":
		a.inMulti = true
		a.buf = a.buf[:0]
	case "EXEC":
		for _, c := range a.buf {
			a.s.applyCommand(a.w, c)
		}
		a.inMulti = false
		a.buf = a.buf[:0]
	case "DISCARD":
		a.inMulti = false
		a.buf = a.buf[:0]
	case "PUBLISH":
		// A message is not part of any transaction. It is delivered on arrival rather
		// than buffered, both because deferring it to the EXEC would delay every message
		// published during a long transaction and because it must not be replayed as one
		// of that transaction's writes.
		a.s.applyCommand(a.w, args)
	case "PING":
		// A master's keepalive can land between a MULTI and its EXEC. It carries no
		// state, so drop it rather than buffer it: splicing it into the group would
		// leave a stray command inside a replayed transaction.
	default:
		if a.inMulti {
			a.buf = append(a.buf, args)
		} else {
			a.s.applyCommand(a.w, args)
		}
	}
}

func (s *Server) isReplica() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.role == "replica"
}

// Listen binds the server to addr without yet accepting connections. When a TLS
// configuration has been set the listener is wrapped, so every accepted connection is
// encrypted before a single command is read.
func (s *Server) Listen(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	if s.tlsConfig != nil {
		ln = tls.NewListener(ln, s.tlsConfig)
	}
	s.ln = ln
	return nil
}

// Addr reports the bound address, or nil if Listen has not been called.
func (s *Server) Addr() net.Addr {
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// Serve accepts connections until ctx is canceled, then waits for in-flight
// connections to finish.
func (s *Server) Serve(ctx context.Context) error {
	s.mu.Lock()
	s.baseCtx = ctx // so runtime REPLICAOF ties replication to the serve lifetime
	s.mu.Unlock()

	go func() {
		<-ctx.Done()
		s.ln.Close()
	}()
	// The idle reaper (see limits.go). It exists whether or not a timeout is configured;
	// while none is, it is one atomic load per second.
	go s.reapIdleClients(ctx)

	for {
		conn, err := s.ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				s.wg.Wait()
				return nil
			default:
				return err
			}
		}
		// The connection table is bounded before a goroutine is spent on the connection,
		// which is the point: the cost this refuses is the goroutine, the reader, the writer
		// and the session, not the accept.
		if !s.admitConn(conn) {
			continue
		}
		s.totalConns.Add(1)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.releaseConn()
			s.handle(ctx, conn)
		}()
	}
}

// ListenAndServe is the convenience combination of Listen and Serve.
func (s *Server) ListenAndServe(ctx context.Context, addr string) error {
	if err := s.Listen(addr); err != nil {
		return err
	}
	return s.Serve(ctx)
}

func (s *Server) handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	r := resp.NewReader(conn)
	w := resp.NewWriter(conn)
	sess := s.newSession(conn)
	// A blocking command stops reading this socket while it waits, so it needs the
	// reader (to watch for a disconnect) and the serve context (to notice a shutdown).
	sess.rd = r
	sess.serveDone = ctx.Done()
	defer s.closeSession(sess) // release WATCHes, subscriptions and the registry entry

	// One watcher serves both jobs the connection needs done from outside its own
	// goroutine: unblock a read when the server shuts down, and let the Pub/Sub pump
	// know the connection is finished.
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-sess.done:
		}
	}()
	for {
		args, err := r.ReadCommand()
		if err != nil {
			// A malformed request gets told what was wrong with it before the connection
			// goes away. Closing silently -- which is what this did -- is what every
			// client library reports as "connection reset by peer", indistinguishable
			// from a crash or a network fault, and it withholds the one fact the caller
			// needs. Redis answers with the detail and then hangs up; so does this.
			//
			// Only a protocol violation produces a reply. An EOF or a read error has
			// nobody left to tell.
			if detail := resp.ProtocolErrorText(err); detail != "" {
				w.WriteError("ERR " + detail)
				w.Flush()
			}
			return
		}
		if len(args) == 0 {
			continue
		}
		// PSYNC turns this connection into a replication feed and never returns
		// while the replica stays connected. The authentication check has to happen
		// here too: PSYNC never reaches execute, so without it a client could ask
		// for the whole dataset as a snapshot without ever presenting a password.
		if strings.EqualFold(string(args[0]), "PSYNC") {
			if s.needsAuth(sess) {
				w.WriteError(errNoAuth)
				w.Flush()
				continue
			}
			s.handleSync(ctx, sess, r, w, args)
			return
		}
		// A subscribed connection -- or a monitoring one -- has a pump writing to the same
		// resp.Writer, so both serialize on the session's writer lock. Whether to take it
		// is decided before execute runs: the SUBSCRIBE or MONITOR that starts a pump must
		// not leave the loop unlocking a mutex it never locked.
		wasSubscribed, wasMonitoring := sess.subscribed, sess.monitoring
		locked := wasSubscribed || wasMonitoring
		if locked {
			sess.relockWriter()
		}
		s.execute(sess, w, args)
		// CLIENT REPLY SKIP spans exactly one following command, so its state machine is
		// stepped here, where one command has just finished. Two boolean tests when no SKIP
		// is outstanding, which is every connection that never sends one.
		advanceReplyMode(sess, w)
		// Coalesce replies for pipelined clients: only flush once the input is
		// drained, so a batch of pipelined commands costs one write syscall.
		var flushErr error
		if r.Buffered() == 0 || sess.quit {
			flushErr = w.Flush() // QUIT: get the acknowledgement out before hanging up
		}
		if locked {
			sess.unlockWriter()
		}
		if !wasSubscribed && sess.subscribed {
			// This connection just took its first subscription. From here on messages
			// may arrive from any publisher, so start the pump that writes them.
			go s.pumpMessages(sess, w)
		}
		if !wasMonitoring && sess.monitoring {
			// And this one just became a monitor, so start the feed that writes the
			// commands other connections run.
			go s.pumpMonitor(sess, w)
		}
		if flushErr != nil || sess.quit {
			return
		}
	}
}

// dispatch looks a command up and runs it, reporting whether it actually ran. A false
// return means the command was refused before reaching its handler -- an unknown name,
// a wrong argument count, a write on a replica -- which is what lets the caller record
// it as a rejected call rather than as a call that took no time.
func (s *Server) dispatch(w *resp.Writer, args [][]byte) (ran bool) {
	s.totalCmds.Add(1)
	name := strings.ToUpper(string(args[0]))
	cmd, ok := commandTable[name]
	if !ok {
		w.WriteError("ERR unknown command '" + string(args[0]) + "'")
		return false
	}
	if !arityOK(cmd.arity, len(args)) {
		w.WriteError("ERR wrong number of arguments for '" + strings.ToLower(name) + "' command")
		reject(cmd)
		return false
	}
	if cmd.sess != nil {
		// Only execute (which owns a session) may run these. Reaching dispatch means
		// the command arrived with no connection behind it -- from an AOF replay or a
		// master's stream -- where there is no connection for it to act on.
		w.WriteError("ERR '" + strings.ToLower(name) + "' requires a client connection")
		reject(cmd)
		return false
	}
	if cmd.write && s.isReplica() {
		w.WriteError(errReadOnly)
		reject(cmd)
		return false
	}
	if !cmd.write {
		if cmd.block != nil {
			// A read that can wait for data (XREAD) with no connection to park: it takes its
			// non-blocking behaviour, exactly as it does inside MULTI. There is nothing to
			// order, persist or replicate, so it must not go through the write path.
			cmd.apply(s, w, args)
			return true
		}
		cmd.fn(s, w, args)
		return true
	}
	s.runWrite(cmd, args, func() (bool, [][][]byte) { return cmd.apply(s, w, args) })
	return true
}

// runWrite applies a write command and publishes everything that follows from it: the
// WATCH invalidations, the keyspace notifications, the AOF and replica stream, and the
// wakeup of any client blocked on a key it created data on. It reports whether the
// dataset changed.
//
// When propagation is active the mutation and its propagation are serialized under
// propMu, so the order applied to memory is exactly the order written to the AOF and
// shipped to replicas. A pure single-node cache skips the lock entirely and keeps
// sharded-concurrent writes.
//
// apply is a parameter rather than being called directly because a blocking command's
// single non-blocking attempt has to take exactly this path -- if it served data, that
// pop is a write like any other and must be ordered, persisted and replicated
// identically -- while the decision to then wait, or not, belongs to the caller.
func (s *Server) runWrite(cmd *command, args [][]byte, apply func() (bool, [][][]byte)) bool {
	propagating := s.propagating.Load()
	if propagating {
		s.propMu.Lock()
	}
	// Which of this command's keys do not exist yet, for the "new key" notification class.
	// Nil, and free, unless that class is enabled -- see watchNewKeys.
	created := s.watchNewKeys(args)
	dirty, effect := apply()

	// What the write is *reported* as. Normally a command describes itself: SPOP's
	// effect is an SREM, and reporting "srem" to a keyspace consumer that expects
	// "spop" would be a regression. A blocking command is the exception -- its own
	// arguments are a list of candidate keys, and only its effect (LPOP <the one key it
	// served>) names the key that actually changed.
	var observed [][][]byte
	if dirty {
		s.noteDirty(1)
		observed = [][][]byte{args}
		if cmd.block != nil {
			observed = effect
		}
		// The effect names the keys that changed, which is what WATCH and the wait
		// queues need. Whether it also names the *event* is a separate question: see
		// blockSpec.silentEffect.
		notify := cmd.block == nil || !cmd.block.silentEffect
		if notify {
			s.notifyNewKeys(created)
		}
		for _, o := range observed {
			s.touchWatchers(o)
			if notify {
				s.notifyWrite(o)
			}
		}
		if propagating {
			// A multi-command effect ships framed in MULTI/EXEC, so a truncated AOF never
			// replays half of one command's consequences.
			s.propagateBatch(wireForm(args, effect))
		}
	}
	if propagating {
		s.propMu.Unlock()
	}
	// Waiters are signalled after the ordering lock is released: the data is already
	// visible (the store call returned before propagate ran), and a woken client has to
	// take propMu for its own pop, so handing it the signal while still holding that
	// lock would only make it wait on the mutex.
	if dirty {
		s.signalWaiters(observed)
	}
	return dirty
}

// applyCommand runs a command directly (no arity reply, no propagation). Used to
// replay the AOF and to apply the stream received from a master.
//
// It still invalidates WATCHers: a client can WATCH a key on a replica, and a
// write arriving from the master modifies that key just as a local write would,
// so a pending EXEC has to see the conflict. (During AOF replay there are no
// sessions yet, so this costs nothing.)
func (s *Server) applyCommand(w *resp.Writer, args [][]byte) {
	if len(args) == 0 {
		return
	}
	name := strings.ToUpper(string(args[0]))
	cmd, ok := commandTable[name]
	if !ok || !arityOK(cmd.arity, len(args)) || cmd.sess != nil {
		return // a connection-control command has no connection to act on here
	}
	created := s.watchNewKeys(args)
	dirty, _ := cmd.apply(s, w, args)
	if cmd.write && dirty {
		s.noteDirty(1)
		s.notifyNewKeys(created)
		s.touchWatchers(args)
		// A write arriving from a master is a change to this node's keyspace, so its
		// own subscribers hear about it exactly as they would a local write.
		s.notifyWrite(args)
		// And a client blocked on one of its keys is waiting for exactly this. A replica
		// refuses the blocking commands (they are writes), so in practice there is nothing
		// to wake here; it costs one atomic load to stay correct if that ever changes.
		s.signalWaiters([][][]byte{args})
	}
	w.Flush()
}

// shipRaw appends an already-formed command to the AOF and streams it to all
// connected replicas. Callers hold propMu so AOF order == replica order ==
// applied order.
func (s *Server) shipRaw(args [][]byte) {
	s.selectOnStream()
	s.shipCommand(args)
}

// selectOnStream puts a SELECT on the stream when the database this write belongs to is
// not the one the stream is already positioned in.
//
// The AOF and the replica stream carry no database context of their own: they are a flat
// sequence of commands, and a command names keys but never a database. So the database
// has to become part of the stream, which is exactly what real Redis does -- the
// alternative, tagging every command with its database, would change the wire format and
// leave an existing AOF unreplayable by the very server that wrote it.
//
// It is emitted lazily, only on a change, for two reasons. A single-database workload
// then ships byte-for-byte what it shipped before databases existed, so an AOF written
// by an older build replays identically and a replica sees no new commands. And the
// stream stays a faithful record of what happened: a SELECT appears where a client
// actually changed database, not once per write.
//
// Callers hold propMu, which is also what guards streamDB.
func (s *Server) selectOnStream() {
	if s.streamDB == s.db {
		return
	}
	s.streamDB = s.db
	s.shipCommand(selectCommand(s.db))
}

func selectCommand(db int) [][]byte {
	return [][]byte{[]byte("SELECT"), []byte(strconv.Itoa(db))}
}

// dumpAll renders every database as a replayable command sequence, each database's
// contents preceded by the SELECT that puts a replayer into it. It is what seeds a
// replica and what an AOF rewrite writes.
//
// A replay starts in database 0, so a dataset that only uses database 0 emits no SELECT
// at all and the sequence is exactly the one Store.Dump produced before databases
// existed -- which is what keeps the chunking invariant, and its tests, unchanged.
//
// The sequence ends by putting the replayer back into the database the ongoing stream is
// positioned in. Without that, a snapshot would silently redirect everything that
// follows it: the master's stream position is shared by every replica, while a snapshot
// is sent to one of them, so the snapshot has to leave that replica exactly where the
// shared stream believes it is.
//
// Callers hold propMu, both for the consistent cut and to read streamDB.
func (s *Server) dumpAll() [][][]byte {
	var out [][][]byte
	cur := 0 // a replay begins in database 0
	for i, db := range s.dbs {
		cmds := db.Dump()
		if len(cmds) == 0 {
			continue // an empty database needs no SELECT and no commands
		}
		if i != cur {
			out = append(out, selectCommand(i))
			cur = i
		}
		out = append(out, cmds...)
	}
	if cur != s.streamDB {
		out = append(out, selectCommand(s.streamDB))
	}
	return out
}

// flushDatabases empties every database. It is what FLUSHALL does, and what a replica
// does before applying a full resync: keys it holds that the master no longer has are
// not mentioned by the snapshot, in any database.
func (s *Server) flushDatabases() {
	for _, db := range s.dbs {
		db.FlushAll()
	}
}

// shipCommand appends a command to the AOF and streams it to the replicas, without any
// database framing of its own. Everything except a PUBLISH goes through shipRaw, which
// adds the framing; this is the raw funnel both share.
func (s *Server) shipCommand(args [][]byte) {
	if s.aof != nil {
		if err := s.aof.Append(args); err != nil {
			// The write was already acked to the client; surface the durability
			// failure rather than swallowing it.
			log.Printf("shardkv: AOF append failed (write not persisted): %v", err)
		}
		// Check the growth policy here rather than on a timer: this is the only place
		// the log grows, so it is the only place the answer can change. The check is two
		// atomic loads when the policy is off, and it never rewrites inline -- it starts
		// a goroutine that waits for propMu, which this caller still holds.
		s.maybeAutoRewriteAOF()
	}
	s.shipReplicas(args)
}

// shipReplicas streams an already-formed command to every connected replica,
// advancing the replication offset and recording the command in the backlog so a
// replica that reconnects soon enough can continue from where it stopped instead of
// re-reading the whole dataset.
//
// It is the single funnel for the replication stream, which is what makes the offset
// meaningful: every byte a replica is expected to have seen passed through here, in
// this order. (The per-feed keepalive deliberately does not, since it is
// point-to-point rather than part of the shared stream -- counting it would give
// every replica a different idea of the same offset.)
//
// A replica whose buffer is full is dropped, not skipped: skipping a command would
// leave that replica missing a write with nothing to ever tell it so, i.e.
// permanently and silently diverged. Dropping the feed closes its connection, so its
// replication loop reconnects -- from the backlog if it can, with a full resync if it
// cannot. A resync is the only way back to agreement.
//
// Commands that are streamed but not persisted (PUBLISH) come straight here rather
// than through shipRaw: a message belongs in a subscriber's stream, not in a log
// whose only job is to reconstruct the dataset.
func (s *Server) shipReplicas(args [][]byte) {
	enc := encodeCommand(args)

	var dropped []*replicaConn
	s.mu.Lock()
	s.replOffset += int64(len(enc))
	s.backlog.feed(enc)
	for rc := range s.replicas {
		select {
		case rc.ch <- args:
		default:
			delete(s.replicas, rc)
			dropped = append(dropped, rc)
		}
	}
	s.mu.Unlock()

	for _, rc := range dropped {
		rc.drop()
		s.replicaDrops.Add(1)
		log.Printf("shardkv: replica too far behind, dropping its feed to force a resync")
	}
}

// forward ships an already-propagation-formed command (e.g. one received from a
// master) downstream without re-rewriting it.
func (s *Server) forward(args [][]byte) { s.shipRaw(args) }

// propagateBatch ships a transaction's writes wrapped in MULTI ... EXEC so a
// crash that truncates the AOF mid-transaction, or a replica, never applies a
// partial transaction. A single-write batch needs no framing. Each command is
// already in propagation (absolute-TTL) form. Caller holds propMu.
func (s *Server) propagateBatch(cmds [][][]byte) {
	if len(cmds) == 0 {
		return
	}
	if len(cmds) == 1 {
		s.shipRaw(cmds[0])
		return
	}
	s.shipRaw([][]byte{[]byte("MULTI")})
	for _, c := range cmds {
		s.shipRaw(c)
	}
	s.shipRaw([][]byte{[]byte("EXEC")})
}
