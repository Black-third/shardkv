package server

// The replication backlog: a bounded, fixed-size ring holding the most recent bytes
// of the replication stream so a replica that reconnects can be continued from where
// it stopped (PSYNC ... -> +CONTINUE) instead of being handed the whole dataset
// again.
//
// Why bound it in bytes rather than in commands: the cost a master is trying to
// control is memory, and one command can be a 4-byte PING or a 512 MB SET. A byte
// bound also makes the trade-off legible to an operator -- repl-backlog-size is
// exactly "how much write traffic a replica may be absent for and still resync
// partially", which for a known write rate converts directly into a disconnect
// window.
//
// Why a ring rather than an append-and-trim slice: trimming the front of a slice
// either leaks the discarded prefix (re-slicing keeps the whole array alive) or costs
// an O(size) copy per append. A ring pays neither, and the memory is allocated once
// at the configured size instead of growing with traffic.

import (
	"strconv"

	"github.com/Black-third/shardkv/internal/resp"
)

// replBacklogSize is the default number of stream bytes retained, matching Redis's
// repl-backlog-size default of 1 MB.
const replBacklogSize = 1 << 20

// replBacklog retains the tail of the replication stream. start and end are absolute
// stream offsets: end is the offset just past the newest byte fed, and start the
// offset of the oldest byte still retained, so [start, end) is exactly the range a
// partial resync can serve.
//
// The zero value is a disabled backlog (size 0) that retains nothing and never
// permits a continuation, which is the right behaviour for a server configured with
// repl-backlog-size 0: it always answers PSYNC with a full resync.
type replBacklog struct {
	buf   []byte
	start int64
	end   int64
	w     int // write cursor into buf
}

// newReplBacklog returns a backlog retaining the last size bytes of the stream. A
// size of zero or less disables it.
func newReplBacklog(size int) *replBacklog {
	if size <= 0 {
		return &replBacklog{}
	}
	return &replBacklog{buf: make([]byte, size)}
}

// feed records bytes appended to the replication stream, discarding whatever no
// longer fits. Callers hold the lock that also advances the stream offset, so the
// backlog's end offset and the master's offset can never disagree.
func (b *replBacklog) feed(p []byte) {
	b.end += int64(len(p)) // the stream advances by every byte, retained or not
	if len(b.buf) == 0 {
		b.start = b.end
		return
	}
	// A payload larger than the whole ring: only its tail could ever be retained, so
	// keep just that and forget the rest.
	if len(p) >= len(b.buf) {
		p = p[len(p)-len(b.buf):]
		b.w = 0
	}
	n := copy(b.buf[b.w:], p)
	if n < len(p) {
		copy(b.buf, p[n:]) // wrapped
	}
	b.w = (b.w + len(p)) % len(b.buf)
	if b.end-b.start > int64(len(b.buf)) {
		b.start = b.end - int64(len(b.buf))
	}
}

// read returns a copy of the stream from the absolute offset from up to the newest
// byte recorded, and whether that whole range is still retained. A request for
// exactly the current end offset is served as an empty, successful read: the replica
// is fully caught up and needs only the live stream from here on.
//
// The result is copied rather than referenced, because the ring keeps being
// overwritten by concurrent writes as soon as the caller releases the lock.
func (b *replBacklog) read(from int64) ([]byte, bool) {
	if from > b.end || from < b.start {
		return nil, false
	}
	n := int(b.end - from)
	if n == 0 {
		return nil, true
	}
	out := make([]byte, n)
	// The offset of the byte at index i is end-(len-i); walk back n bytes from the
	// write cursor to find where the requested range starts.
	begin := ((b.w-n)%len(b.buf) + len(b.buf)) % len(b.buf)
	copied := copy(out, b.buf[begin:])
	if copied < n {
		copy(out[copied:], b.buf)
	}
	return out, true
}

// histLen reports how many stream bytes are currently retained, which INFO exposes as
// repl_backlog_histlen so an operator can see how much disconnect window is left.
func (b *replBacklog) histLen() int64 { return b.end - b.start }

// encodeCommand renders a command in the same RESP form the replica feeds and the AOF
// write, so the bytes counted into the replication offset and stored in the backlog
// are byte-for-byte what a replica receives. Anything else would make a continuation
// resume at the wrong place.
func encodeCommand(args [][]byte) []byte {
	buf := make([]byte, 0, resp.CommandSize(args))
	buf = append(buf, '*')
	buf = strconv.AppendInt(buf, int64(len(args)), 10)
	buf = append(buf, '\r', '\n')
	for _, a := range args {
		buf = append(buf, '$')
		buf = strconv.AppendInt(buf, int64(len(a)), 10)
		buf = append(buf, '\r', '\n')
		buf = append(buf, a...)
		buf = append(buf, '\r', '\n')
	}
	return buf
}
