// Package aof implements append-only-file persistence: every write command is
// serialized (in RESP) and appended to a log that is replayed on startup to
// rebuild the dataset. This is the same durability model Redis uses for its AOF.
package aof

import (
	"os"
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

// Log is a concurrency-safe append-only command log.
type Log struct {
	mu     sync.Mutex
	path   string
	f      *os.File
	w      *resp.Writer
	policy SyncPolicy
	closed bool
}

// Open opens (creating if needed) the AOF at path for appending.
func Open(path string, policy SyncPolicy) (*Log, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	l := &Log{path: path, f: f, w: resp.NewWriter(f), policy: policy}
	if policy == SyncEverySec {
		go l.syncLoop()
	}
	return l, nil
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
		l.w.Flush()
		l.f.Sync()
		l.mu.Unlock()
	}
}

// Close flushes and closes the log.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	l.w.Flush()
	l.f.Sync()
	return l.f.Close()
}

// Rewrite atomically replaces the log with a compacted snapshot: it writes the
// given commands to a temp file and renames it over the current log, discarding
// the (potentially large) command history. Appends continue to the new file.
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
	for _, cmd := range snapshot {
		if err := tw.WriteCommand(cmd); err != nil {
			tf.Close()
			return err
		}
	}
	if err := tw.Flush(); err != nil {
		tf.Close()
		return err
	}
	if err := tf.Sync(); err != nil {
		tf.Close()
		return err
	}
	tf.Close()

	// Swap files: flush current buffer, rename atomically, reopen for append.
	l.w.Flush()
	l.f.Close()
	if err := os.Rename(tmp, l.path); err != nil {
		return err
	}
	f, err := os.OpenFile(l.path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	l.f = f
	l.w = resp.NewWriter(f)
	return nil
}

// Load replays the AOF at path and returns the recorded commands. A missing file
// yields no commands and no error (fresh start). A torn final record from a
// crash simply ends replay early.
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
			break
		}
		if len(args) > 0 {
			cmds = append(cmds, args)
		}
	}
	return cmds, nil
}
