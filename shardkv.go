// Package shardkv embeds the shardkv server in a Go program.
//
// It is the same server cmd/shardkv runs -- the same command table, the same store, the
// same AOF and replication -- reachable without a process boundary. Two things follow
// from that, and they are the reason this package exists:
//
//   - A Go program can run commands with no socket at all. DB.Do goes through the
//     server's client entry point and returns decoded values, so a test needs no port,
//     no listener, no cleanup goroutine and nothing to poll for readiness.
//   - The same DB can serve real Redis clients at the same time, by giving it an Addr.
//     An embedded cache and the redis-cli an operator debugs it with are then looking
//     at one keyspace.
//
// # Getting started
//
//	db, err := shardkv.Open(shardkv.Options{})
//	if err != nil {
//		return err
//	}
//	defer db.Close()
//
//	if err := db.OK("SET", "greeting", "hello"); err != nil {
//		return err
//	}
//	v, _, err := db.Bytes("GET", "greeting")   // v == []byte("hello")
//
// The zero Options is a complete configuration: 256 shards, 16 databases, no listener,
// no persistence. Every field is optional and every default matches the corresponding
// flag's default in cmd/shardkv.
//
// # What this package is
//
// A facade, and deliberately a thin one. Nothing here reimplements any part of the
// server: the options are applied through the same Server.ConfigSet that CONFIG SET
// goes through, so a value means at Open what it means at runtime, and commands are
// executed by the same code a TCP client reaches. No type from internal/ appears in any
// signature below, which is what lets the server be refactored without breaking a
// caller -- and what lets this package stay short enough to be read in one sitting.
//
// # What it is not
//
// It is not a Redis client library and there is no wrapper method per command. There are
// 224 commands; a hand-written mirror of them would be a second interface to keep
// correct against the first, and the mismatches would surface as an embedded caller
// unable to reach a command a socket client can. Do takes the command as its arguments,
// and the typed accessors on Client (Int, Bytes, Strings, Map, ...) convert its reply
// once for whichever shape the caller expects, so the ergonomic win generalizes to
// commands that do not exist yet.
//
// Pub/Sub is the one feature the in-process client does not serve. A subscription is a
// server-initiated stream, and delivering it needs somewhere to push to; an in-process
// client is a request/reply channel with no such place. SUBSCRIBE from one is accepted
// and then falls behind and is dropped, on the same contract as any slow subscriber. Use
// a listener and a real client for Pub/Sub.
package shardkv

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/Black-third/shardkv/internal/aof"
	"github.com/Black-third/shardkv/internal/server"
	"github.com/Black-third/shardkv/internal/store"
)

// Version reports the server version, the same string INFO and HELLO answer with.
//
// It is a function rather than a constant so that the value keeps exactly one definition
// -- the server's own -- without the internal package's name appearing in this package's
// documented interface, which is where a constant's initializer would render it. That
// matters here beyond tidiness: "no internal package appears in the contract" is the
// property that lets the server be refactored without breaking a caller, and a facade
// should not be the one place it is visibly untrue.
func Version() string { return server.Version }

// Options configures an embedded server. The zero value is valid and documented on each
// field; Open fills in the defaults cmd/shardkv's flags use.
//
// The fields fall into two groups, and which group a setting is in is decided by whether
// CONFIG SET can change it on a running server. The named fields below are the
// structural decisions that cannot be taken back -- how many shards there are, which
// files persistence uses, what the listener is -- because each of them creates or binds
// something before serving starts. Everything else is a Config entry, applied through
// the very table CONFIG SET uses: there is one parser for "256mb", and a startup value
// and a runtime one cannot come to mean different numbers.
type Options struct {
	// Addr is the TCP address to listen on, in net.Listen form (":6380", "127.0.0.1:0").
	// Empty means no listener at all: the server is reachable only in-process, which is
	// the default because it is the case that needs no port to be free.
	Addr string

	// Listener serves on a socket the caller already bound, in place of Addr. It takes
	// precedence over Addr, and Close closes it.
	//
	// It is how to use a port the test framework chose, a Unix socket, an inherited
	// descriptor, or a listener already wrapped in the caller's own TLS. A listener given
	// here is used exactly as supplied -- the TLS fields below wrap what Addr binds and do
	// not touch this.
	Listener net.Listener

	// Shards is the number of lock shards per database, rounded up to a power of two.
	// 0 means 256.
	Shards int

	// Databases is how many keyspaces SELECT can switch between. 0 means 16, or 1 when
	// Cluster.Enabled is set, since a cluster is one keyspace partitioned by slot.
	Databases int

	// SweepInterval is how often each database's janitor sweeps expired keys and evicts.
	// 0 means one second. It cannot be a Config entry because a janitor goroutine per
	// database is started with it at Open.
	SweepInterval time.Duration

	// Now replaces the clock the server reads for expiry, which is what makes a TTL
	// testable without sleeping.
	//
	// It is one function for the whole server rather than one per database, and that is
	// load-bearing: every deadline on the wire is absolute (invariant 3), so two databases
	// reading different clocks would expire keys at instants the propagated deadlines were
	// never computed against. Nil means time.Now.
	Now func() time.Time

	// AOFPath is the append-only file. Empty disables persistence. A file that exists is
	// replayed at Open, before Open returns, so a DB is ready the moment it is handed back.
	AOFPath string

	// AOFSync is the fsync policy: "everysec" (the default), "always" or "no".
	//
	// It is a field rather than a Config entry only because the policy is needed to open
	// the log, which happens before any Config entry could be applied. "appendfsync" in
	// Config is refused for that reason, rather than silently ignored.
	AOFSync string

	// SnapshotPath is the point-in-time snapshot file SAVE and BGSAVE write, and that is
	// loaded at Open when no AOF holds data. Empty disables it. It is not an RDB file.
	SnapshotPath string

	// Password is the password AUTH must present. Empty leaves the server open, which for
	// an in-process caller is the ordinary case: there is no socket to reach it on.
	Password string

	// MasterAuth is the password this server presents to a master it replicates from.
	MasterAuth string

	// MaxMemory is the budget for the dataset before eviction begins, as a byte count or
	// a size with a unit ("256mb", "1gb"). Empty or "0" is unbounded.
	MaxMemory string

	// MaxMemoryPolicy is what happens at the budget: noeviction, allkeys-lru, allkeys-lfu,
	// allkeys-random, volatile-lru, volatile-lfu, volatile-random or volatile-ttl. Empty
	// leaves it derived from MaxKeys, which is the default cmd/shardkv has: a server given
	// a key cap and no policy evicts by allkeys-lru.
	MaxMemoryPolicy string

	// MaxKeys caps live keys per database by approximate LRU. 0 is unbounded.
	MaxKeys int

	// TLS configures the listener and, separately, the dial to a master. It applies to a
	// listener bound from Addr; see Listener.
	TLS TLSOptions

	// ReplicaOf makes this server a replica of the master at host:port, in which case it
	// refuses writes. Empty makes it a master.
	ReplicaOf string

	// Cluster runs this server as a cluster node, routing keys by hash slot and
	// redirecting the ones it does not own.
	Cluster ClusterOptions

	// Config is every remaining setting, by the name CONFIG SET knows it: "maxclients",
	// "timeout", "notify-keyspace-events", "slowlog-log-slower-than",
	// "latency-monitor-threshold", "repl-backlog-size", "auto-aof-rewrite-percentage",
	// "save", "enable-debug-command", the listpack thresholds, and the rest. Values are
	// strings for the same reason CONFIG SET's are, so a unit suffix means the same thing
	// in both places.
	//
	// An unknown name, or a value the setting refuses, fails Open with the sentence a
	// client would have been sent. Applied after the named fields above, so an entry here
	// wins over one of them if both name the same setting.
	Config map[string]string
}

// TLSOptions is the certificate material for the listener and for the replication dial.
// The zero value leaves both plain TCP.
type TLSOptions struct {
	// CertFile and KeyFile are the PEM certificate and private key. Both set enables TLS
	// on the listener.
	CertFile string
	KeyFile  string
	// CAFile is the PEM roots used to verify peers. Optional.
	CAFile string
	// Replication dials the master over TLS using the same material. It is separate from
	// the listener's TLS because the two directions are independent: a replica may reach
	// an encrypted master while itself serving plain TCP on a private interface.
	Replication bool
}

func (t TLSOptions) enabled() bool { return t.CertFile != "" && t.KeyFile != "" }

// ClusterOptions runs the server as a cluster node. The zero value does not.
type ClusterOptions struct {
	// Enabled turns cluster mode on. Keys are then routed by hash slot and a key whose
	// slot belongs to another node is answered with a redirect rather than served -- which
	// applies to the in-process client too, since it is a client (see Client.Do).
	Enabled bool
	// ConfigFile is where the node id and slot map are persisted. Empty keeps them in
	// memory, so the node takes a new id on every restart.
	ConfigFile string
	// AnnounceIP is the address other nodes and redirected clients should use. Empty means
	// 127.0.0.1.
	AnnounceIP string
	// AnnouncePort is the port they should use. 0 means the port actually bound, which
	// requires a listener: a cluster node with neither is unreachable, and Open says so
	// rather than starting one.
	AnnouncePort int
}

// DB is an embedded server.
//
// The embedded *Client is the default one, so DB.Do and the typed accessors work
// directly on a DB and a program that needs nothing else never mentions Client at all.
// That client serializes its calls, exactly as one connection does; NewClient returns an
// independent one for a caller that wants commands running in parallel.
//
// A DB is safe for concurrent use. Do on the default client is safe but serialized, so
// concurrency comes from clients, which is the same shape as a connection pool.
type DB struct {
	*Client

	srv  *server.Server
	stop context.CancelFunc
	log  *aof.Log // nil when persistence is disabled

	// done is closed once the accept loop has returned, and serveErr is what it returned.
	// The close is what publishes serveErr: it is written before the close and read only
	// after it, so the channel is the synchronization and no lock is needed for either.
	done     chan struct{}
	serveErr error

	// closeOnce guards the teardown, and closeErr is what every caller of Close is given.
	// Close is idiomatically reached from a defer and from a shutdown path both, so the
	// second call has to be harmless and has to report the same thing as the first.
	closeOnce sync.Once
	closeErr  error
}

// Open starts an embedded server and returns it ready to use: any AOF or snapshot has
// already been replayed, the expiry janitors are running, and the listener -- if there is
// one -- is bound and accepting.
//
// Binding happens before Open returns, which is what removes the readiness race a test
// otherwise has to poll for: once Open has returned without an error, Addr names a socket
// that is already queueing connections.
//
// The caller must Close the result, which is what flushes and closes the AOF. Skipping it
// loses the log's buffered tail -- writes a client was told were durable.
func Open(opts Options) (*DB, error) {
	shards := opts.Shards
	if shards == 0 {
		shards = defaultShards
	}
	st := store.New(shards)
	if opts.Now != nil {
		// Before server.New and before SetDatabases: SetDatabases copies database 0's clock
		// into each database it creates, so setting it here is what makes one clock serve all
		// of them (invariant 3).
		st.SetClock(opts.Now)
	}

	srv := server.New(st)
	ctx, cancel := context.WithCancel(context.Background())
	db := &DB{srv: srv, stop: cancel, done: make(chan struct{})}
	// SHUTDOWN stops the server through the same path Close takes, so the AOF is flushed by
	// the command exactly as it is by the caller. Without the hook the command would answer
	// and change nothing.
	srv.SetShutdownHook(cancel)

	// A failure past this point has already taken resources: restore opens the log, and
	// listen binds the socket. Neither is reachable by the caller, since Open returns no DB
	// to Close, so the unwinding has to happen here -- and it has to close the log rather
	// than only drop it, because that is what flushes what the replay wrote.
	//
	// A caller-supplied listener is deliberately left alone: the caller still holds it and
	// may want to hand it to something else, whereas one bound from Addr exists only inside
	// this call and nothing else can ever reach it.
	fail := func(err error) (*DB, error) {
		cancel()
		if opts.Listener == nil {
			db.closeListener()
		}
		return nil, errors.Join(err, db.closeLog())
	}

	if err := db.configure(opts); err != nil {
		return fail(err)
	}
	if err := db.restore(opts); err != nil {
		return fail(err)
	}
	db.start(ctx, opts)
	if err := db.listen(ctx, opts); err != nil {
		return fail(err)
	}
	db.Client = db.NewClient()
	return db, nil
}

// defaults matching cmd/shardkv's flags, so an embedded server and the binary behave
// alike unless told otherwise.
const (
	defaultShards = 256
	defaultDBs    = 16
	defaultSweep  = time.Second
)

// configure applies everything that must be decided before the dataset is loaded. The
// order follows cmd/shardkv's: databases first (a per-database setting needs them to
// exist), then the settings, then the material TLS and the snapshot schedule are built
// from.
func (db *DB) configure(opts Options) error {
	dbs := opts.Databases
	switch {
	case dbs == 0 && opts.Cluster.Enabled:
		// A cluster is one keyspace partitioned across nodes by slot, so a second database
		// would be a keyspace no slot covers and therefore no node is responsible for. The
		// binary logs and clamps here because its -databases flag has a non-zero default; an
		// unset field is unambiguous, so this just resolves to the only valid value.
		dbs = 1
	case dbs == 0:
		dbs = defaultDBs
	case dbs > 1 && opts.Cluster.Enabled:
		return fmt.Errorf("shardkv: Databases %d with Cluster.Enabled: a cluster has one database", dbs)
	}
	if err := db.srv.SetDatabases(dbs); err != nil {
		return fmt.Errorf("shardkv: Databases %d: %w", dbs, err)
	}

	// Every setting below goes through Server.ConfigSet -- the table CONFIG SET uses --
	// rather than through a setter of its own, so a refusal reads as the sentence a client
	// would have been sent and a startup value cannot come to mean something a runtime one
	// does not. The named fields are applied first and the Config map second, so an entry
	// in the map wins over the field for the same setting.
	named := []struct{ name, value string }{
		{"requirepass", opts.Password},
		{"masterauth", opts.MasterAuth},
		{"maxmemory", opts.MaxMemory},
		{"maxmemory-policy", opts.MaxMemoryPolicy},
	}
	for _, s := range named {
		if s.value == "" {
			// Skipped rather than applied as a zero, for the reason cmd/shardkv only applies
			// the flags that were passed: maxmemory-policy's default is *derived* from the key
			// cap, so writing an unasked-for "noeviction" over it would turn eviction off for
			// a server configured with MaxKeys, silently.
			continue
		}
		if err := db.config(s.name, s.value); err != nil {
			return err
		}
	}
	if opts.MaxKeys != 0 {
		if err := db.config("maxkeys", strconv.Itoa(opts.MaxKeys)); err != nil {
			return err
		}
	}
	if _, ok := opts.Config["appendfsync"]; ok {
		return errors.New(`shardkv: Config["appendfsync"] cannot be applied at Open ` +
			`because the log is opened with its policy: use Options.AOFSync`)
	}
	for name, value := range opts.Config {
		if err := db.config(name, value); err != nil {
			return err
		}
	}

	if opts.TLS.enabled() {
		if err := db.srv.EnableTLS(db.tlsOptions(opts)); err != nil {
			return fmt.Errorf("shardkv: TLS: %w", err)
		}
	}
	if opts.TLS.Replication {
		if err := db.srv.EnableMasterTLS(db.tlsOptions(opts)); err != nil {
			return fmt.Errorf("shardkv: replication TLS: %w", err)
		}
	}
	// Before anything is loaded, because LoadSnapshot reads the path off the server.
	db.srv.SetSnapshotPath(opts.SnapshotPath)
	return nil
}

func (db *DB) tlsOptions(opts Options) server.TLSOptions {
	return server.TLSOptions{
		CertFile: opts.TLS.CertFile, KeyFile: opts.TLS.KeyFile, CAFile: opts.TLS.CAFile,
	}
}

func (db *DB) config(name, value string) error {
	if err := db.srv.ConfigSet(name, value); err != nil {
		return fmt.Errorf("shardkv: config %s=%q: %w", name, value, err)
	}
	return nil
}

// restore rebuilds the dataset and attaches the log new writes go to.
//
// The precedence between the two records is the binary's, and Redis's: an AOF is a
// history and a snapshot is a state, so where both hold data the AOF is by construction
// at least as recent. Applying both would double-apply every write the snapshot already
// describes -- harmless for SET, wrong for RPUSH, SADD's count and XADD.
func (db *DB) restore(opts Options) error {
	fromSnapshot, err := db.restoreDataset(opts.AOFPath)
	if err != nil {
		return err
	}
	if opts.AOFPath == "" {
		return nil
	}
	sync := opts.AOFSync
	if sync == "" {
		sync = "everysec"
	}
	policy, ok := aof.ParseSyncPolicy(sync)
	if !ok {
		return fmt.Errorf("shardkv: AOFSync %q: use always, everysec or no", sync)
	}
	logf, err := aof.Open(opts.AOFPath, policy)
	if err != nil {
		return fmt.Errorf("shardkv: opening AOF: %w", err)
	}
	db.log = logf
	db.srv.AttachAOF(logf)
	if fromSnapshot {
		// The dataset came from the snapshot, so the log that is now the authority on the
		// next restart knows nothing about it. Seeding it here -- before Open returns, so
		// before any client can connect -- is what keeps the log a description of the whole
		// dataset from its first byte. Failing is fatal: continuing would leave an
		// authoritative log describing part of what is in memory.
		if err := db.srv.RewriteAOF(); err != nil {
			return fmt.Errorf("shardkv: seeding the empty AOF at %s from the snapshot: %w",
				opts.AOFPath, err)
		}
	}
	return nil
}

func (db *DB) restoreDataset(aofPath string) (fromSnapshot bool, err error) {
	if aofPath != "" {
		cmds, err := aof.Load(aofPath)
		if err != nil {
			return false, fmt.Errorf("shardkv: loading AOF: %w", err)
		}
		if len(cmds) > 0 {
			db.srv.ReplayCommands(cmds)
			return false, nil
		}
	}
	keys, _, err := db.srv.LoadSnapshot()
	if err != nil {
		return false, fmt.Errorf("shardkv: loading snapshot %s: %w", db.srv.SnapshotPath(), err)
	}
	return keys > 0, nil
}

// start launches the background passes whose lifetime is the DB's: the snapshot schedule
// and one expiry janitor per database.
//
// One janitor per database rather than one walking all of them, for the reason
// cmd/shardkv gives: each database is an independent keyspace with its own shards, and a
// single sweep would hold every database's progress hostage to the largest.
func (db *DB) start(ctx context.Context, opts Options) {
	go db.srv.SnapshotScheduler(ctx)
	sweep := opts.SweepInterval
	if sweep == 0 {
		sweep = defaultSweep
	}
	for i := 0; i < db.srv.Databases(); i++ {
		go db.srv.DB(i).Janitor(ctx, sweep)
	}
}

// listen binds the listener (if any), enables cluster mode, starts replication, and puts
// the accept loop in the background.
//
// Serve is started even with no listener, because it is what ties replication begun at
// runtime -- a REPLICAOF from a client -- to this DB's lifetime rather than to
// context.Background.
func (db *DB) listen(ctx context.Context, opts Options) error {
	switch {
	case opts.Listener != nil:
		db.srv.UseListener(opts.Listener)
	case opts.Addr != "":
		if err := db.srv.Listen(opts.Addr); err != nil {
			return fmt.Errorf("shardkv: listening on %s: %w", opts.Addr, err)
		}
	}
	// After the listener, because a node's announced port defaults to the one it bound --
	// which is not knowable before, and not knowable at all for a DB with no listener,
	// which is why that combination needs an explicit AnnouncePort.
	if opts.Cluster.Enabled {
		if err := db.srv.EnableCluster(server.ClusterOptions{
			ConfigFile:   opts.Cluster.ConfigFile,
			AnnounceIP:   opts.Cluster.AnnounceIP,
			AnnouncePort: opts.Cluster.AnnouncePort,
		}); err != nil {
			return fmt.Errorf("shardkv: enabling cluster mode: %w", err)
		}
	}
	if opts.ReplicaOf != "" {
		db.srv.ReplicaOf(ctx, opts.ReplicaOf)
	}
	go func() {
		db.serveErr = db.srv.Serve(ctx)
		close(db.done)
	}()
	return nil
}

// Addr is the address the listener is bound to, or nil when there is none. It is how to
// find the port for Options.Addr of ":0", which is the form to prefer in a test: the
// kernel picks a free one, so nothing has to be reserved and no two tests collide.
func (db *DB) Addr() net.Addr { return db.srv.Addr() }

// Done is closed once the server has stopped: on Close, or because a client ran
// SHUTDOWN, or because the accept loop failed. Waiting on it is how an embedded caller
// notices the second and third of those, neither of which any call of its own would
// report. Close then gives the reason.
func (db *DB) Done() <-chan struct{} { return db.done }

// Close stops the server and returns the first failure it saw: the accept loop's error,
// or the AOF's.
//
// Closing the log is what flushes and fsyncs its tail, so an error here is a durability
// failure and not a cleanup detail -- it means writes acknowledged to a client did not
// reach the file. Safe to call more than once, and every call reports the same thing,
// because a defer and a shutdown path both reach it.
func (db *DB) Close() error {
	db.closeOnce.Do(func() {
		db.stop()
		if db.Client != nil {
			// Client.Close only unregisters a session and cannot fail; its error exists so
			// that the type satisfies io.Closer for a caller's defer.
			_ = db.Client.Close()
		}
		var serveErr error
		select {
		case <-db.done:
			// Read only on this side of the close, which is what makes it safe to read at
			// all: the serve goroutine wrote it before closing the channel.
			serveErr = db.serveErr
		case <-time.After(closeGrace):
			// Serve is expected back the moment the listener closes. Not waiting forever is
			// what keeps a Close in a test's defer from hanging the test rather than failing
			// it, and the log below is closed either way -- the durability step must not be
			// skipped because the accept loop is wedged.
			serveErr = errors.New("shardkv: the server did not stop within " + closeGrace.String())
		}
		db.closeErr = errors.Join(serveErr, db.closeLog())
	})
	return db.closeErr
}

// closeGrace is how long Close waits for the accept loop to return. Serve returns as
// soon as the listener closes, so this is only reached when something is wedged, and
// reporting that is more useful than blocking on it.
const closeGrace = 10 * time.Second

func (db *DB) closeLog() error {
	if db.log == nil {
		return nil
	}
	log := db.log
	db.log = nil
	if err := log.Close(); err != nil {
		return fmt.Errorf("shardkv: closing the AOF (its tail may be unpersisted): %w", err)
	}
	return nil
}

// closeListener releases a socket bound by a call that then failed. On the ordinary path
// the accept loop closes it: Serve watches the context and closes the listener when it is
// canceled, which is what makes Close enough on its own.
func (db *DB) closeListener() { _ = db.srv.CloseListener() }

// ConfigSet changes a setting on the running server, through the same table CONFIG SET
// and Options both go through. It is here because a caller that has no listener has no
// other way to reach the command.
func (db *DB) ConfigSet(name, value string) error { return db.config(name, value) }
