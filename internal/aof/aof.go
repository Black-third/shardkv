// Package aof implements append-only-file persistence: every write command is
// serialized (in RESP) and appended to a log that is replayed on startup to
// rebuild the dataset. This is the same durability model Redis uses for its AOF.
package aof

import (
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
)

// SyncPolicy controls how aggressively the log is flushed to disk.
type SyncPolicy int

const (
	// SyncEverySec flushes once per second in the background (good durability,
	// high throughput). Default, matching Redis's recommendation.
	SyncEverySec SyncPolicy = iota
	// SyncAlways fsyncs after every write (strongest durability, slowest).
	SyncAlways
	// SyncNo lets the OS decide when to flush (fastest, weakest durability).
	SyncNo
)

// String returns the policy's configuration-file spelling, which is also what
// CONFIG GET appendfsync reports.
func (p SyncPolicy) String() string {
	switch p {
	case SyncAlways:
		return "always"
	case SyncNo:
		return "no"
	default:
		return "everysec"
	}
}

// ParseSyncPolicy parses a policy name, reporting whether it was recognized. An
// unknown name is rejected rather than defaulted, so a typo in a durability setting
// cannot silently leave the log less durable than the operator asked for.
func ParseSyncPolicy(name string) (SyncPolicy, bool) {
	switch name {
	case "always":
		return SyncAlways, true
	case "everysec":
		return SyncEverySec, true
	case "no":
		return SyncNo, true
	}
	return SyncEverySec, false
}

// Log is a concurrency-safe append-only command log.
type Log struct {
	mu      sync.Mutex
	path    string
	f       *os.File
	w       *resp.Writer
	policy  SyncPolicy
	closed  bool
	syncing bool                 // the background flush loop is running
	logf    func(string, ...any) // surfaces background durability failures

	// size is how many bytes the log holds, and baseSize how many it held at the end
	// of the last rewrite. Their ratio is what the automatic rewrite policy compares
	// against a percentage, so the accounting is kept here, incrementally, rather than
	// stat()ed: the file is buffered, so its on-disk size lags the records the log has
	// accepted, and a policy driven by that would fire late and erratically.
	size     int64
	baseSize int64
}

// Open opens (creating if needed) the AOF at path for appending.
func Open(path string, policy SyncPolicy) (*Log, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	l := &Log{path: path, f: f, w: resp.NewWriter(f), policy: policy, logf: log.Printf}
	// A log opened on an existing file starts with that file as its base: the growth
	// the policy measures is growth since the last rewrite, and a restart is not one.
	if fi, err := f.Stat(); err == nil {
		l.size, l.baseSize = fi.Size(), fi.Size()
	}
	if policy == SyncEverySec {
		l.syncing = true
		go l.syncLoop()
	}
	return l, nil
}

// Policy reports the configured sync policy, for CONFIG GET appendfsync.
func (l *Log) Policy() SyncPolicy {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.policy
}

// SetPolicy changes the sync policy of a running log (CONFIG SET appendfsync).
//
// Switching *to* everysec has to start the background flusher, since a log opened
// with another policy never started one; switching away leaves it running, where its
// once-a-second flush is harmless (an "always" log is already flushed, and a "no" log
// is allowed to reach the disk whenever). Stopping it would need a second lifetime to
// manage for no benefit.
func (l *Log) SetPolicy(p SyncPolicy) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.policy = p
	if p == SyncEverySec && !l.syncing && !l.closed {
		l.syncing = true
		go l.syncLoop()
	}
}

// Size reports how many bytes of records the log holds.
func (l *Log) Size() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.size
}

// BaseSize reports how large the log was after the last rewrite (or when it was
// opened, if it has not been rewritten since).
func (l *Log) BaseSize() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.baseSize
}

// Flush pushes buffered records to the file and fsyncs it, making everything appended
// so far durable regardless of the sync policy. SHUTDOWN uses it: an orderly stop
// should not lose writes that a "no" or "everysec" policy still had in a buffer.
func (l *Log) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	if err := l.w.Flush(); err != nil {
		return err
	}
	return l.f.Sync()
}

// SetLogger overrides where background errors are reported (used in tests).
func (l *Log) SetLogger(logf func(string, ...any)) {
	l.mu.Lock()
	l.logf = logf
	l.mu.Unlock()
}

// Append serializes a command and writes it to the log, applying the configured
// sync policy.
func (l *Log) Append(args [][]byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	if err := l.w.WriteCommand(args); err != nil {
		return err
	}
	l.size += int64(resp.CommandSize(args))
	if l.policy == SyncAlways {
		if err := l.w.Flush(); err != nil {
			return err
		}
		return l.f.Sync()
	}
	return nil
}

func (l *Log) syncLoop() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for range t.C {
		l.mu.Lock()
		if l.closed {
			l.mu.Unlock()
			return
		}
		if err := l.w.Flush(); err != nil {
			l.logf("aof: background flush failed, data may be unpersisted: %v", err)
		} else if err := l.f.Sync(); err != nil {
			l.logf("aof: background fsync failed, data may be unpersisted: %v", err)
		}
		l.mu.Unlock()
	}
}

// Close flushes and closes the log, reporting every failure it hit rather than
// only the last one.
//
// The flush and the fsync are the steps that actually persist the tail, so
// discarding their errors would report a clean shutdown for a log whose final
// writes never reached the disk -- the one failure a caller most needs to hear
// about. Closing proceeds even after an earlier step fails, so the descriptor is
// always released.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	return errors.Join(l.w.Flush(), l.f.Sync(), l.f.Close())
}

// Rewrite atomically replaces the log with a compacted snapshot: it writes the
// given commands to a temp file and renames it over the current log, discarding
// the (potentially large) command history. Appends continue to the new file.
//
// The live file descriptor is kept valid until the new one is successfully
// opened, so a failure during the swap leaves the log usable rather than
// stranded on a closed descriptor.
func (l *Log) Rewrite(snapshot [][][]byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}

	tmp := l.path + ".tmp"
	tf, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	tw := resp.NewWriter(tf)
	var written int64
	for _, cmd := range snapshot {
		if err := tw.WriteCommand(cmd); err != nil {
			tf.Close()
			os.Remove(tmp)
			return err
		}
		written += int64(resp.CommandSize(cmd))
	}
	if err := tw.Flush(); err != nil {
		tf.Close()
		os.Remove(tmp)
		return err
	}
	if err := tf.Sync(); err != nil {
		tf.Close()
		os.Remove(tmp)
		return err
	}
	tf.Close()

	// Atomically replace the live file. On Rename failure the old descriptor is
	// untouched and the log keeps working.
	if err := os.Rename(tmp, l.path); err != nil {
		os.Remove(tmp)
		return err
	}
	syncDir(l.path) // make the rename itself durable

	newF, err := os.OpenFile(l.path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		// The compacted data is safely on disk; we just can't reopen for append.
		// Disable further appends rather than write to a stale descriptor.
		l.w.Flush()
		l.f.Close()
		l.closed = true
		l.logf("aof: reopen after rewrite failed, persistence disabled: %v", err)
		return err
	}
	// The old buffer's contents are already captured in the snapshot, so discard
	// the old file and switch to the new one.
	l.f.Close()
	l.f = newF
	l.w = resp.NewWriter(newF)
	// The compacted file is the new baseline the growth policy measures against.
	l.size, l.baseSize = written, written
	return nil
}

// syncDir fsyncs the directory containing path so a rename survives a crash.
// Best effort: errors (e.g. on platforms that disallow directory fsync) are
// ignored.
func syncDir(path string) {
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return
	}
	d.Sync()
	d.Close()
}

// Load replays the AOF at path and returns the recorded commands. A missing file
// yields no commands and no error (fresh start). A torn final record from a
// crash ends replay quietly; any other malformed record mid-file ends replay but
// logs a warning, since later valid commands are being dropped.
func Load(path string) ([][][]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	r := resp.NewReader(f)
	var cmds [][][]byte
	for {
		args, err := r.ReadCommand()
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				log.Printf("aof: stopped replay at a malformed record after %d commands: %v", len(cmds), err)
			}
			break
		}
		if len(args) > 0 {
			cmds = append(cmds, args)
		}
	}
	return cmds, nil
}
