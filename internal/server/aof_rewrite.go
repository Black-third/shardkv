package server

// Compacting rewrites of the append-only file: BGREWRITEAOF and the automatic policy
// that fires one when the log has grown past a configured multiple of its
// post-rewrite size.
//
// Without this the log only ever grows: a key written a million times costs a million
// records to replay, forever, even though one record describes it. A rewrite replaces
// the history with a snapshot of the present -- Store.Dump, the same command sequence
// that seeds a replica.
//
// The consistency guarantee, and how it is obtained:
//
// The rewrite holds propMu across both Store.Dump and the file swap. Write commands
// hold that same lock across their mutation and their append, so while the rewrite
// runs no write can be applied and no record can be appended. What that buys is an
// exact log: the snapshot describes the dataset as of the instant the rewrite began,
// and every write ordered after that instant appends after the swap. No write is lost,
// none is applied twice, and no record from the discarded history survives into the new
// file. A replay of the rewritten log therefore reconstructs exactly the state a replay
// of the old one would have.
//
// The price is that writers block for the duration -- O(dataset) -- and the choice is
// deliberate. Real Redis forks and accumulates concurrent writes in a diff buffer,
// which keeps writes flowing at the cost of a copy-on-write child, a second buffer, and
// the reconciliation between them. Holding one lock is the version of this whose
// correctness can be stated in a sentence and checked by a test. BGREWRITEAOF is still
// asynchronous with respect to the *client*: the rewrite runs on its own goroutine, so
// the caller is acknowledged immediately.

import (
	"errors"
	"log"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
)

// Defaults matching Redis's auto-aof-rewrite-min-size and
// auto-aof-rewrite-percentage.
const (
	defaultAOFRewriteMinSize = 64 << 20 // 64 MB
	defaultAOFRewritePerc    = 100      // rewrite once the log has doubled
)

// ErrAOFDisabled is returned when a rewrite is asked for on a server with no
// append-only file. It is an error rather than a silent success because "the rewrite
// finished" and "there was nothing to rewrite" are different answers to an operator
// who is watching the log grow.
var ErrAOFDisabled = errors.New("server: append-only file is disabled")

// ErrRewriteInProgress is returned when a rewrite is already running.
var ErrRewriteInProgress = errors.New("server: an AOF rewrite is already in progress")

func init() {
	register("BGREWRITEAOF", 1, false, cmdBGRewriteAOF)
	register("LASTSAVE", 1, false, cmdLastSave)
}

// SetAOFRewritePolicy configures the automatic rewrite. A rewrite is triggered when
// the log is at least minSize bytes AND has grown by at least percentage percent over
// its size after the last rewrite. A percentage of zero disables the automatic policy,
// leaving BGREWRITEAOF as the only trigger.
//
// Both conditions are needed. The percentage alone would rewrite a tiny log constantly
// (a 200-byte log doubles in one command), and the minimum size alone would rewrite a
// large log that is not actually growing.
func (s *Server) SetAOFRewritePolicy(minSize int64, percentage int) {
	s.aofRewriteMinSize.Store(minSize)
	s.aofRewritePerc.Store(int64(percentage))
}

// AOFRewriteMinSize and AOFRewritePercentage report the configured policy, for
// CONFIG GET.
func (s *Server) AOFRewriteMinSize() int64  { return s.aofRewriteMinSize.Load() }
func (s *Server) AOFRewritePercentage() int { return int(s.aofRewritePerc.Load()) }

// maybeAutoRewriteAOF starts a rewrite if the growth policy calls for one. It is
// called from the append path, with propMu held, so it must never do the rewrite
// itself: it starts a goroutine that takes propMu once this write has released it.
//
// The comparison is against the size recorded at the last rewrite, not against a
// constant, so a log that has already been compacted has to grow again before the next
// one -- which is what stops a large dataset from rewriting itself in a loop.
func (s *Server) maybeAutoRewriteAOF() {
	perc := s.aofRewritePerc.Load()
	if perc <= 0 || s.aofRewriting.Load() {
		return
	}
	size, base := s.aof.Size(), s.aof.BaseSize()
	if size < s.aofRewriteMinSize.Load() {
		return
	}
	if base <= 0 {
		base = 1 // a log that started empty: any growth past the minimum qualifies
	}
	if (size-base)*100/base < perc {
		return
	}
	s.startAOFRewrite()
}

// startAOFRewrite begins a rewrite on its own goroutine, reporting whether it started
// one (false means one was already running, or there is no log to rewrite).
func (s *Server) startAOFRewrite() bool {
	if s.aof == nil || !s.aofRewriting.CompareAndSwap(false, true) {
		return false
	}
	go func() {
		defer s.aofRewriting.Store(false)
		if err := s.rewriteAOF(); err != nil {
			log.Printf("shardkv: AOF rewrite failed, the existing log is still in use: %v", err)
		}
	}()
	return true
}

// RewriteAOF compacts the append-only file and returns when it is done. It is the
// synchronous form of BGREWRITEAOF, for a caller that wants to know the outcome.
func (s *Server) RewriteAOF() error {
	if s.aof == nil {
		return ErrAOFDisabled
	}
	if !s.aofRewriting.CompareAndSwap(false, true) {
		return ErrRewriteInProgress
	}
	defer s.aofRewriting.Store(false)
	return s.rewriteAOF()
}

// rewriteAOF is the rewrite itself. The caller owns the aofRewriting flag.
//
// Dump and Rewrite are both inside the one propMu acquisition -- that is the whole
// consistency argument (see the file comment). Splitting them, even briefly, would let
// a write land between the snapshot and the swap, where it would be discarded with the
// old file while its client had already been told it succeeded.
func (s *Server) rewriteAOF() error {
	s.propMu.Lock()
	snapshot := s.dumpAll()
	err := s.aof.Rewrite(snapshot)
	s.propMu.Unlock()

	if err != nil {
		s.aofRewriteOK.Store(false)
		return err
	}
	s.aofRewriteOK.Store(true)
	s.lastSave.Store(time.Now().Unix())
	// The whole dataset is now on disk, so nothing the change counter was counting is
	// still unsaved. This is the only event here that means what an RDB save means to
	// Redis, which is why it is the only one that clears it.
	s.clearDirtyChanges()
	return nil
}

// aofRewriteStatus reports the outcome of the last rewrite in the form INFO uses.
func (s *Server) aofRewriteStatus() string {
	if s.aofRewriteOK.Load() {
		return "ok"
	}
	return "err"
}

// cmdBGRewriteAOF implements BGREWRITEAOF: it starts a compacting rewrite in the
// background and acknowledges immediately, since the rewrite can take as long as the
// dataset is large.
func cmdBGRewriteAOF(s *Server, w *resp.Writer, args [][]byte) bool {
	if s.aof == nil {
		w.WriteError("ERR " + ErrAOFDisabled.Error())
		return false
	}
	if s.startAOFRewrite() {
		w.WriteSimple("Background append only file rewriting started")
	} else {
		// One is already running. Redis answers "scheduled" here rather than failing,
		// because the caller's intent -- a compacted log soon -- is being served.
		w.WriteSimple("Background append only file rewriting scheduled")
	}
	return false
}

// cmdLastSave reports when the dataset was last written out in full, as a Unix
// timestamp. There is no RDB here, so the answer is the last successful AOF rewrite --
// the moment a complete copy of the dataset was last put on disk -- or the server's
// start time if none has happened.
func cmdLastSave(s *Server, w *resp.Writer, args [][]byte) bool {
	w.WriteInt(s.lastSave.Load())
	return false
}
