package server

// Point-in-time snapshots: SAVE, BGSAVE, the `save <seconds> <changes>` schedule, the load
// at startup, and DEBUG RELOAD.
//
// Why a snapshot exists at all, next to an append-only file that already persists
// everything: an AOF is a history and a snapshot is a state. A cold start on an AOF replays
// every write the server ever accepted, so start-up time grows with the *workload* rather
// than with the dataset; and there is no single file an operator can copy, because the log
// is being appended to while they copy it. A snapshot is one file, complete as of one
// instant, and small -- one command per key rather than one per write.
//
// The format is shardkv's own and is not RDB. See the internal/snapshot package comment for
// what that decision costs and why the alternative -- a partial RDB implementation that a
// real Redis might load *wrongly* -- was rejected.
//
// # What consistency a save provides
//
// The cut is **a single instant for the whole keyspace**, not per-shard-consistent, and not
// Redis's copy-on-write: propMu, crossDBMu and every shard's read lock in every database are
// all held together for the whole walk, so no write can be applied anywhere while it is being
// read. There is no window in which one shard is captured before a write and another after it
// -- which is exactly what a shard-at-a-time walk (Store.Dump's, and so the AOF rewrite's)
// would produce if propMu were not already excluding writers.
//
// Three things follow, and the third is a limit rather than a guarantee, so it is stated here
// and not left to be discovered:
//
//   - every individual write is wholly in the snapshot or wholly out of it, whatever
//     propagation mode the server is in.
//   - a command that is atomic across shards on its own -- the ones built on Store.lockKeys:
//     RENAME, COPY, SMOVE, LMOVE/RPOPLPUSH, MSETNX -- is likewise wholly in or wholly out.
//     TestSaveIsConsistentUnderConcurrentWrites pins this with LMOVE, whose two keys are in
//     different shards and whose element must never be counted twice or lost.
//   - a write that is *not* internally atomic across shards is not made atomic by the cut.
//     MSET is the case that exists today: it is a loop of independent single-key Sets with no
//     cross-shard lock, so a cut can land between its keys -- exactly as a concurrent MGET
//     can. The snapshot does not create that; it also cannot fix it. Likewise a MULTI/EXEC
//     batch is atomic against the cut only when propagation is active, because that is when
//     EXEC holds propMu (invariant 1); on a pure cache EXEC deliberately does not, and its
//     commands are applied one at a time to any observer, this one included. See
//     TestSaveDoesNotSplitATransactionWhenPropagationIsActive, which is scoped to exactly the
//     configuration where the property holds.
//
// What "background" means here is therefore narrower than in Redis, and the difference is
// stated rather than papered over:
//
//   - BGSAVE does not block the *client*: it returns as soon as the save is started.
//   - BGSAVE does not block anything for the *file write*: encoding, fsync and rename all
//     happen on the background goroutine with no lock held.
//   - BGSAVE does block *writers* for the length of the in-memory walk -- O(dataset), no
//     I/O. Readers are unaffected (the walk holds read locks). Real Redis forks and pays
//     copy-on-write page faults instead; there is no fork here, so the choice is between
//     blocking writers briefly and producing a file that never existed as a state. The
//     first is the one whose correctness can be stated in a sentence.
//   - the peak memory cost is the command stream itself, which is roughly the serialized
//     size of the dataset, held while the file is written. That is the price of not
//     forking, and it is the same order as the copy-on-write cost a fork can reach.
//
// SAVE holds nothing longer than BGSAVE does -- it takes the same cut -- but it writes the
// file on the calling connection's goroutine, so the client waits for the disk.
//
// # Lock order
//
// propMu -> crossDBMu -> shard locks, in (database, shard index) order. That is the order a
// write command already takes them in (runWrite takes propMu, the cross-database handlers
// take crossDBMu, the store takes shards by index), so a save cannot be half of a cycle.
// crossDBMu is not optional: MOVE, COPY ... DB and SWAPDB each hold locks in two stores at
// once, and no shard ordering can help across two independent stores (invariant 8) -- taking
// the same mutex they take is what makes "every shard in every database" a safe thing to
// hold. propMu is what makes the cut a point in the total order of writes when propagation
// is active; when it is not, the shard locks alone are what exclude writers.

import (
	"context"
	"errors"
	"log"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/snapshot"
)

func init() {
	// Neither is a write: a save does not change the dataset, so nothing is persisted or
	// replicated on its account. A replica saves its own snapshot, as in Redis.
	register("SAVE", 1, false, cmdSave)
	register("BGSAVE", -1, false, cmdBGSave)
}

// defaultSaveSpec is Redis's default `save` schedule, kept so CONFIG GET save reports the
// string a client expects to find. It only ever fires on a server that has a snapshot path
// configured; without one there is nowhere to save to and the schedule is inert.
const defaultSaveSpec = "3600 1 300 100 60 10000"

// saveScheduleInterval is how often the schedule is evaluated. Redis checks on its
// serverCron, several times a second; the shortest period anyone configures is measured in
// seconds, so once a second is enough resolution and costs one wakeup on an idle server.
const saveScheduleInterval = time.Second

// ErrSnapshotsDisabled is returned when a save is asked for on a server with no snapshot
// path. It is an error rather than a silent success for the same reason ErrAOFDisabled is:
// "the save finished" and "there was nowhere to save to" are different answers to an
// operator or a backup script, and only one of them means a file now exists.
var ErrSnapshotsDisabled = errors.New("server: snapshots are disabled (no snapshot file configured)")

// ErrSaveInProgress is returned when a save is already running.
var ErrSaveInProgress = errors.New("server: a save is already in progress")

// errSaveInProgressReply is Redis's message, byte for byte, for both SAVE and BGSAVE
// arriving while a save is already running.
const errSaveInProgressReply = "ERR Background save already in progress"

// saveRule is one `save <seconds> <changes>` pair: save if at least changes writes have
// changed the dataset and at least seconds have passed since the last full save.
type saveRule struct {
	seconds int64
	changes int64
}

// snapshotState is everything a snapshot needs to remember. It hangs off serverCore as one
// pointer so the fields it adds there are one line rather than nine.
type snapshotState struct {
	// path is where snapshots are written and read. Empty disables the feature. It is set
	// before serving starts and not changed afterwards, like the AOF's.
	path string

	// saving is both the mutual exclusion between saves and what INFO reports as
	// rdb_bgsave_in_progress. A single flag covers SAVE and BGSAVE together, which is what
	// makes Redis's "Background save already in progress" the right answer to either.
	saving atomic.Bool
	// ok is the outcome of the last save, for rdb_last_bgsave_status.
	ok atomic.Bool
	// saves counts completed saves (rdb_saves); lastDurMs and startedUnix back
	// rdb_last_bgsave_time_sec and rdb_current_bgsave_time_sec.
	saves       atomic.Int64
	lastDurMs   atomic.Int64
	startedUnix atomic.Int64
	// loadedKeys is how many keys the startup load put in place, for
	// rdb_last_load_keys_loaded. It is a fact an operator checks against what they expected
	// the backup to hold, which is the only way to notice a snapshot of the wrong dataset.
	loadedKeys atomic.Int64

	// rules is the parsed schedule, replaced wholesale so the ticker reads one immutable
	// generation with a single atomic load -- the same copy-on-write discipline the cluster
	// slot map uses, for the same reason (invariant 13).
	rules atomic.Pointer[[]saveRule]
	// spec is the schedule's text, so CONFIG GET save reports back what it was given
	// rather than a re-rendering of it.
	spec atomic.Pointer[string]
}

// snap returns the snapshot state, allocating it on first use so a Server that was never
// configured for snapshots costs one atomic load.
func (s *Server) snap() *snapshotState {
	if st := s.snapshot.Load(); st != nil {
		return st
	}
	fresh := &snapshotState{}
	fresh.ok.Store(true) // no save has failed yet, which is what Redis reports on a fresh server
	spec := defaultSaveSpec
	fresh.spec.Store(&spec)
	rules, _ := parseSaveSpec(spec)
	fresh.rules.Store(&rules)
	if s.snapshot.CompareAndSwap(nil, fresh) {
		return fresh
	}
	return s.snapshot.Load()
}

// SetSnapshotPath configures where SAVE and BGSAVE write. An empty path disables snapshots,
// which is the default: like the AOF, persistence here is opt-in, so an existing deployment
// that asked for neither gets neither.
func (s *Server) SetSnapshotPath(path string) { s.snap().path = path }

// SnapshotPath reports the configured path (empty if snapshots are disabled).
func (s *Server) SnapshotPath() string { return s.snap().path }

// SetSaveSchedule parses and installs a `save` specification -- whitespace-separated
// seconds/changes pairs, e.g. "3600 1 300 100 60 10000" -- reporting whether it was valid.
// An empty string is valid and disables scheduled saving, which is how Redis spells that.
//
// An invalid spec is refused rather than partly applied: half a schedule is a durability
// setting that does not do what the operator wrote down, which is worse than a startup
// failure.
func (s *Server) SetSaveSchedule(spec string) bool {
	rules, ok := parseSaveSpec(spec)
	if !ok {
		return false
	}
	st := s.snap()
	st.rules.Store(&rules)
	normalized := strings.Join(strings.Fields(spec), " ")
	st.spec.Store(&normalized)
	return true
}

// SaveSchedule reports the schedule in the spelling CONFIG GET save uses: the pairs
// separated by single spaces, or "" when nothing is scheduled.
func (s *Server) SaveSchedule() string {
	if p := s.snap().spec.Load(); p != nil {
		return *p
	}
	return ""
}

func parseSaveSpec(spec string) ([]saveRule, bool) {
	fields := strings.Fields(spec)
	if len(fields)%2 != 0 {
		return nil, false
	}
	rules := make([]saveRule, 0, len(fields)/2)
	for i := 0; i < len(fields); i += 2 {
		secs, err1 := strconv.ParseInt(fields[i], 10, 64)
		changes, err2 := strconv.ParseInt(fields[i+1], 10, 64)
		if err1 != nil || err2 != nil || secs < 0 || changes < 0 {
			return nil, false
		}
		rules = append(rules, saveRule{seconds: secs, changes: changes})
	}
	return rules, true
}

// --- the fields INFO persistence reports -------------------------------------

// SnapshotEnabled reports whether a snapshot file is configured.
func (s *Server) SnapshotEnabled() bool { return s.snap().path != "" }

// SnapshotInProgress reports whether a save is running, for rdb_bgsave_in_progress.
func (s *Server) SnapshotInProgress() bool { return s.snap().saving.Load() }

// SnapshotStatus reports the outcome of the last save in INFO's spelling, for
// rdb_last_bgsave_status. A server that has never saved reports "ok", as Redis does.
func (s *Server) SnapshotStatus() string {
	if s.snap().ok.Load() {
		return "ok"
	}
	return "err"
}

// SnapshotSaves counts completed saves, for rdb_saves.
func (s *Server) SnapshotSaves() int64 { return s.snap().saves.Load() }

// SnapshotLastDurationSec reports how long the last save took, in whole seconds, for
// rdb_last_bgsave_time_sec. It is -1 when nothing has been saved, which is Redis's spelling
// of "not applicable" in this field.
func (s *Server) SnapshotLastDurationSec() int64 {
	if s.snap().saves.Load() == 0 {
		return -1
	}
	return s.snap().lastDurMs.Load() / 1000
}

// SnapshotCurrentDurationSec reports how long the running save has been going, for
// rdb_current_bgsave_time_sec, or -1 when none is running.
func (s *Server) SnapshotCurrentDurationSec() int64 {
	st := s.snap()
	if !st.saving.Load() {
		return -1
	}
	started := st.startedUnix.Load()
	if started == 0 {
		return 0
	}
	return time.Now().Unix() - started
}

// SnapshotLoadedKeys reports how many keys the last startup load restored, for
// rdb_last_load_keys_loaded.
func (s *Server) SnapshotLoadedKeys() int64 { return s.snap().loadedKeys.Load() }

// LastSave reports the Unix time at which the dataset was last written out in full -- a
// successful snapshot save, a successful AOF rewrite, or the time in a snapshot that was
// loaded at startup. It is what LASTSAVE answers and what INFO reports as
// rdb_last_save_time.
func (s *Server) LastSave() int64 { return s.lastSave.Load() }

// --- taking the cut ----------------------------------------------------------

// snapshotCut renders every database as one replayable command stream, captured as a single
// instant. See the file comment for the guarantee and the lock order.
//
// The framing is dumpAll's -- each database's contents preceded by the SELECT that puts a
// replayer into it, and none at all for a dataset that only uses database 0 (invariant 11).
// What it does *not* copy from dumpAll is the trailing SELECT back to the ongoing stream's
// position: that exists because a replica seed is spliced into a stream shared with every
// other replica, and a file is spliced into nothing. Emitting it would put a SELECT in the
// snapshot that describes the state of a connection the file will never be replayed on.
// dirtyAtCut is the change counter as of the cut. It is read here, inside the same propMu
// section, rather than before or after: read outside it, the number would describe a
// different set of writes than the file does, and the whole use of
// rdb_changes_since_last_save is to say how far the file is behind memory.
func (s *Server) snapshotCut() (cmds [][][]byte, dirtyAtCut int64) {
	s.propMu.Lock()
	defer s.propMu.Unlock()
	s.crossDBMu.Lock()
	defer s.crossDBMu.Unlock()
	dirtyAtCut = s.dirtyChanges.Load()

	releases := make([]func(), 0, len(s.dbs))
	defer func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}()
	for _, db := range s.dbs {
		releases = append(releases, db.RLockAll())
	}

	var out [][][]byte
	cur := 0 // a replay begins in database 0
	for i, db := range s.dbs {
		part := db.DumpLocked()
		if len(part) == 0 {
			continue // an empty database needs no SELECT and no commands
		}
		if i != cur {
			out = append(out, selectCommand(i))
			cur = i
		}
		out = append(out, part...)
	}
	return out, dirtyAtCut
}

// saveSnapshot takes the cut and writes it. It owns the saving flag on entry and clears it
// on return.
//
// The dirty counter is *decremented* by what it read at the cut rather than zeroed, because
// writes that landed while the file was being written are genuinely still unsaved. Zeroing
// would report a saved dataset that is already behind, and rdb_changes_since_last_save is
// precisely the number a backup script reads to decide whether it needs to ask again.
func (s *Server) saveSnapshot() error {
	st := s.snap()
	defer st.saving.Store(false)
	if st.path == "" {
		return ErrSnapshotsDisabled
	}
	started := time.Now()
	st.startedUnix.Store(started.Unix())

	cmds, dirtyAtCut := s.snapshotCut()
	if err := snapshot.Save(st.path, cmds, started); err != nil {
		st.ok.Store(false)
		return err
	}
	st.ok.Store(true)
	st.saves.Add(1)
	st.lastDurMs.Store(time.Since(started).Milliseconds())
	s.lastSave.Store(started.Unix())
	s.dirtyChanges.Add(-dirtyAtCut)
	return nil
}

// SaveSnapshot writes a snapshot and returns when it is on disk. It is the synchronous form
// -- what SAVE runs, and what a caller that wants to know the outcome uses.
func (s *Server) SaveSnapshot() error {
	st := s.snap()
	if st.path == "" {
		return ErrSnapshotsDisabled
	}
	if !st.saving.CompareAndSwap(false, true) {
		return ErrSaveInProgress
	}
	return s.saveSnapshot()
}

// startBGSave begins a save on its own goroutine, reporting whether it started one (false
// means one was already running, or snapshots are disabled).
func (s *Server) startBGSave() bool {
	st := s.snap()
	if st.path == "" || !st.saving.CompareAndSwap(false, true) {
		return false
	}
	go func() {
		if err := s.saveSnapshot(); err != nil {
			// Reported, not swallowed: the client was already told the save started, so this
			// log line is the only place the failure appears besides
			// rdb_last_bgsave_status.
			log.Printf("shardkv: background save failed, the previous snapshot is untouched: %v", err)
		}
	}()
	return true
}

// SnapshotScheduler evaluates the `save <seconds> <changes>` schedule until ctx is done. It
// is started alongside the expiry janitors (see cmd/shardkv) rather than from Serve, for the
// same reason they are: it is a background pass whose lifetime is the process's, and wiring
// it where the flags are read keeps "was this asked for" in one place.
//
// A tick with nothing configured is two atomic loads, which is the same discipline every
// other always-on hook follows (invariant 12).
func (s *Server) SnapshotScheduler(ctx context.Context) {
	t := time.NewTicker(saveScheduleInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.maybeScheduledSave()
		}
	}
}

// maybeScheduledSave starts a background save if any rule is satisfied.
//
// The comparison is against lastSave -- the moment a *complete* copy of the dataset last
// reached the disk, which an AOF rewrite also sets -- so a server that has just been
// rewritten does not immediately snapshot as well. Both events mean the same thing to the
// question the rule is asking.
func (s *Server) maybeScheduledSave() {
	st := s.snap()
	if st.path == "" || st.saving.Load() {
		return
	}
	rulesP := st.rules.Load()
	if rulesP == nil || len(*rulesP) == 0 {
		return
	}
	dirty := s.dirtyChanges.Load()
	if dirty <= 0 {
		return
	}
	elapsed := time.Now().Unix() - s.lastSave.Load()
	for _, r := range *rulesP {
		if dirty >= r.changes && elapsed >= r.seconds {
			if s.startBGSave() {
				log.Printf("shardkv: %d changes in %ds, starting a background save", dirty, elapsed)
			}
			return
		}
	}
}

// --- loading -----------------------------------------------------------------

// LoadSnapshot replays the snapshot at the configured path into the (empty) databases and
// reports how many keys it restored. A missing file restores nothing and is not an error: it
// is a server that has never saved.
//
// A file that exists but does not check out *is* an error, and the caller is expected to
// refuse to start. Serving a partially loaded snapshot would present a subset of the dataset
// as the whole of it, and the first write would then persist that subset over the good copy.
// The commands are fully parsed and verified before any of them is applied, so a rejected
// file leaves the server exactly as it was.
func (s *Server) LoadSnapshot() (keys int64, savedAt time.Time, err error) {
	st := s.snap()
	if st.path == "" {
		return 0, time.Time{}, nil
	}
	cmds, savedAt, err := snapshot.Load(st.path)
	if err != nil {
		return 0, savedAt, err
	}
	if len(cmds) == 0 {
		return 0, savedAt, nil
	}
	s.ReplayCommands(cmds)
	for _, db := range s.dbs {
		keys += int64(db.Len())
	}
	st.loadedKeys.Store(keys)
	// A loaded snapshot is a complete copy of the dataset that is already on disk, so
	// nothing is unsaved and the last full save is the one in the file -- not the moment
	// this process started, which is what LASTSAVE would otherwise report about a backup
	// taken three days ago. Redis reports its own start time here; this is a deliberate
	// difference, and the more useful of the two answers.
	if !savedAt.IsZero() {
		s.lastSave.Store(savedAt.Unix())
	}
	s.clearDirtyChanges()
	return keys, savedAt, nil
}

// reloadFromSnapshot is DEBUG RELOAD: save the dataset, then put it back from what was
// saved. It is Redis's own test hook, and what it checks is that everything survives the
// round trip through the serialized form -- which is why the test suites that use it use it
// after every interesting mutation.
//
// The load is completed and verified *before* anything is discarded, so a snapshot that does
// not parse leaves the dataset alone rather than emptying it. That ordering is the whole
// safety of the command.
//
// propMu is held across the flush and the replay so no propagating write can interleave and
// be lost. On a server with neither an AOF nor a replica, writes do not take propMu at all,
// so a concurrent write there could be overwritten by the replay: DEBUG RELOAD is a test
// hook -- refused entirely unless -enable-debug-command opens it -- and not an online
// operation, and this is the one place that distinction matters.
// With snapshots disabled there is nowhere to write, so the round trip goes through memory
// instead of through the filesystem -- the same encoder, the same decoder, the same
// verification, just no file. Redis's DEBUG RELOAD always writes dump.rdb; refusing here
// instead would make the hook unavailable on exactly the default configuration, which would
// mean the serialized form of every type is checked only on servers that happen to persist.
func (s *Server) reloadFromSnapshot() error {
	var cmds [][][]byte
	if s.snap().path != "" {
		if err := s.SaveSnapshot(); err != nil {
			return err
		}
		loaded, _, err := snapshot.Load(s.snap().path)
		if err != nil {
			return err
		}
		cmds = loaded
	} else {
		cut, _ := s.snapshotCut()
		blob, err := snapshot.Encode(cut, time.Now())
		if err != nil {
			return err
		}
		loaded, _, err := snapshot.Decode(blob)
		if err != nil {
			return err
		}
		cmds = loaded
	}
	s.propMu.Lock()
	defer s.propMu.Unlock()
	s.flushDatabases()
	s.ReplayCommands(cmds)
	return nil
}

// --- the commands ------------------------------------------------------------

// cmdSave implements SAVE: write the snapshot now, on this connection, and answer when it is
// on disk.
//
// It blocks the calling client for the whole write and blocks every writer for the cut
// inside it. That is what SAVE is for -- an operator who wants a file before they do
// something else -- and it is why BGSAVE exists beside it.
func cmdSave(s *Server, w *resp.Writer, args [][]byte) bool {
	st := s.snap()
	if st.path == "" {
		w.WriteError("ERR " + ErrSnapshotsDisabled.Error())
		return false
	}
	if !st.saving.CompareAndSwap(false, true) {
		w.WriteError(errSaveInProgressReply)
		return false
	}
	if err := s.saveSnapshot(); err != nil {
		// Redis answers a bare "-ERR" here. The reason is named instead, because a save that
		// failed is exactly the moment an operator needs to know whether the disk is full,
		// the directory is missing or the path is not writable -- and no other reply carries
		// it.
		w.WriteError("ERR " + err.Error())
		return false
	}
	w.WriteSimple("OK")
	return false
}

// cmdBGSave implements BGSAVE [SCHEDULE]: start a save and answer immediately.
//
// SCHEDULE is accepted with Redis's meaning -- "if one is already running, arrange for
// another rather than failing". Here the request is satisfied by the running save itself
// once it has taken its cut, so the reply says scheduled and nothing is queued; there is no
// fork whose completion a second save would have to wait for.
func cmdBGSave(s *Server, w *resp.Writer, args [][]byte) bool {
	schedule := false
	if len(args) > 1 {
		if len(args) != 2 || !strings.EqualFold(string(args[1]), "SCHEDULE") {
			w.WriteError("ERR syntax error")
			return false
		}
		schedule = true
	}
	if s.snap().path == "" {
		w.WriteError("ERR " + ErrSnapshotsDisabled.Error())
		return false
	}
	if s.startBGSave() {
		w.WriteSimple("Background saving started")
		return false
	}
	if schedule {
		w.WriteSimple("Background saving scheduled")
	} else {
		w.WriteError(errSaveInProgressReply)
	}
	return false
}
