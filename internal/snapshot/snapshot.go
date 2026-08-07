// Package snapshot writes and reads shardkv's point-in-time snapshot file: the whole
// dataset as one framed, length-prefixed, checksummed stream of replayable commands.
//
// # This is not an RDB file
//
// The format is shardkv's own. It is not Redis's RDB and deliberately not compatible with
// it, in either direction: Redis cannot load a shardkv snapshot and shardkv cannot load a
// dump.rdb. The header says so in ASCII on the first line of every file, so an operator who
// finds one on disk and an operator who points redis-check-rdb at it both learn the same
// thing from the file itself.
//
// The alternative was attempted-RDB, and it was rejected. RDB is versioned (RDB_VERSION,
// with a per-type opcode table), and each type has several encodings chosen by size
// thresholds -- listpack, quicklist, intset, ziplist, stream listpacks with their own
// internal framing. A half-implemented writer produces a file that either fails to load,
// which is merely useless, or loads *wrongly*, which is a silent data corruption in the
// operator's backup: the one artefact whose whole purpose is to be trustworthy when
// everything else has already gone wrong. An honest native format that refuses to be
// mistaken for RDB is worth more than a plausible imitation of one.
//
// What that costs, stated because it is real:
//
//   - redis-check-rdb, rdb-tools and every other RDB inspector cannot read a snapshot.
//     The mitigation is that the body is a plain RESP command stream, so `redis-cli --pipe`
//     could replay a snapshot's body into any Redis server, and any RESP reader can dump it.
//   - a real Redis's dump.rdb cannot be loaded here. Migrating from Redis goes through the
//     wire (a replica sync, or DUMP/RESTORE per key), not through the file.
//   - there is no cross-version binary compatibility promise beyond the version in the
//     magic line. A future v2 will be a different magic line and this reader will refuse
//     it by name rather than misread it.
//
// # Layout
//
//	<magic line>            ASCII, ends with '\n'; see headerLine. Names the format and
//	                        version, and says in the file that it is not an RDB.
//	uint64 big-endian       the instant the snapshot was taken, Unix milliseconds
//	uint64 big-endian       how many commands the body holds
//	uint64 big-endian       how many bytes the body holds
//	<body>                  exactly that many bytes: RESP arrays of bulk strings, the same
//	                        encoding the AOF and the replica stream use
//	uint64 big-endian       CRC-64 (ECMA) of the body bytes
//
// Three of those fields exist for one reason: a snapshot has to be able to say it is
// incomplete. A bare command stream cannot -- a file truncated after 900 of 1000 commands
// parses cleanly as a smaller dataset, and a reader has no way to know that the other 100
// keys ever existed. The byte length catches truncation, the command count catches a body
// that parses to fewer commands than were written, and the checksum catches the rest. Any
// of the three failing is reported as an error, never as a partial load: unlike an AOF,
// whose torn tail is the expected shape of a crash, a snapshot is written to a temporary
// file and renamed, so it is either whole or absent. Anything else is corruption.
package snapshot

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
)

// magic identifies the format and its version. The reader compares this prefix exactly, so
// a v2 file is refused by name rather than misread as a v1.
const magic = "SHARDKV-SNAPSHOT v1"

// headerLine is the first line of every snapshot. It is ASCII and self-describing because
// the first thing anyone does with an unfamiliar file is look at the front of it.
const headerLine = magic + " -- shardkv native format. NOT an RDB file: Redis cannot read this.\n"

// maxHeaderLine bounds how far the reader will look for the header's newline, so a file
// that is not a snapshot at all cannot make the reader buffer it.
const maxHeaderLine = 256

// fixedFields is the size of the three big-endian counters between the header line and the
// body, and trailerLen the size of the checksum after it.
const (
	fixedFields = 3 * 8
	trailerLen  = 8
)

// crcTable is the CRC-64/ECMA polynomial, from the standard library: a snapshot needs to
// detect corruption, not to resist an adversary, and this is the strongest thing available
// without a dependency.
var crcTable = crc64.MakeTable(crc64.ECMA)

// ErrNotSnapshot is returned for a file whose header is not this format's. It is
// deliberately distinguishable from a corrupt snapshot: "this is a dump.rdb" and "this
// snapshot is damaged" call for different actions.
var ErrNotSnapshot = errors.New("snapshot: not a shardkv snapshot file")

// ErrCorrupt is returned when the header parses but the body does not match what the header
// promised -- wrong length, wrong command count, or a failed checksum.
var ErrCorrupt = errors.New("snapshot: file is corrupt or truncated")

// Save writes cmds to path as a snapshot taken at savedAt, atomically: the bytes go to a
// temporary file in the same directory, are fsynced, and are then renamed over path.
//
// The atomicity is the point of the function. A save that wrote in place would, if the
// process died halfway, leave a truncated file *where a good one used to be* -- so the
// crash that made the backup necessary is the crash that destroyed it. Rename is atomic
// within a directory on every filesystem this runs on, so a reader sees either the previous
// snapshot or the new one and never a mixture. The directory is then fsynced so the rename
// itself survives a power loss; that call is best effort, because some filesystems and
// platforms refuse a directory fsync, and failing a save that has already reached the disk
// would be the worse answer. This is the same sequence, and the same caveat, as the AOF
// rewrite (see aof.Log.Rewrite).
//
// The temporary file is removed on every failure path, so a failing save does not
// accumulate half-written files next to a good snapshot.
func Save(path string, cmds [][][]byte, savedAt time.Time) (err error) {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
		}
	}()

	if err = writeTo(f, cmds, savedAt); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	syncDir(path)
	return nil
}

// Encode returns the same bytes Save would write. It exists for DEBUG RELOAD on a server
// with no snapshot path, which still wants the round trip through the serialized form, and
// for the tests that check the format without needing a filesystem.
func Encode(cmds [][][]byte, savedAt time.Time) ([]byte, error) {
	var buf bytes.Buffer
	if err := writeTo(&buf, cmds, savedAt); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// writeTo is the one encoder. Save and Encode share it so a file and an in-memory snapshot
// cannot drift into two formats.
func writeTo(dst io.Writer, cmds [][][]byte, savedAt time.Time) error {
	// The declared body length is computed before anything is written, from the same
	// accounting the AOF uses for its own size. A test pins it against the bytes actually
	// produced: a length that disagreed with the body would make every future load fail.
	var bodyLen uint64
	for _, c := range cmds {
		bodyLen += uint64(resp.CommandSize(c))
	}

	header := make([]byte, 0, len(headerLine)+fixedFields)
	header = append(header, headerLine...)
	header = binary.BigEndian.AppendUint64(header, uint64(savedAt.UnixMilli()))
	header = binary.BigEndian.AppendUint64(header, uint64(len(cmds)))
	header = binary.BigEndian.AppendUint64(header, bodyLen)
	if _, err := dst.Write(header); err != nil {
		return err
	}

	// The checksum is computed from the bytes on their way out rather than from a second
	// copy of the body: the command stream is already the size of the dataset, and buffering
	// its encoded form as well would double the cost of a save at exactly the moment memory
	// is most likely to be the problem.
	h := crc64.New(crcTable)
	w := resp.NewWriter(io.MultiWriter(dst, h))
	for _, c := range cmds {
		if err := w.WriteCommand(c); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	_, err := dst.Write(binary.BigEndian.AppendUint64(nil, h.Sum64()))
	return err
}

// Load reads the snapshot at path and returns its commands and the instant it was taken.
//
// A missing file is not an error: it is a server that has never saved, which is the same
// answer aof.Load gives for a missing log, so a caller's startup path treats both the same
// way.
//
// Every other failure *is* an error, and the caller is expected to refuse to start rather
// than serve whatever parsed. See the package comment: the atomic rename means a snapshot
// cannot be half-written, so a file that does not check out has been damaged after the
// fact, and loading part of it would silently present a subset of the dataset as the whole
// of it.
func Load(path string) (cmds [][][]byte, savedAt time.Time, err error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, time.Time{}, nil
		}
		return nil, time.Time{}, err
	}
	defer func() { _ = f.Close() }()

	// How much of the file could possibly be body, so a corrupt declared length is refused
	// before it becomes an allocation. -1 means the size is not knowable (a pipe), in which
	// case the short-read check below is what catches it -- correct, just later.
	limit := int64(-1)
	if fi, statErr := f.Stat(); statErr == nil && fi.Mode().IsRegular() {
		limit = fi.Size()
	}
	return readFrom(f, limit)
}

// Decode reads the bytes Encode produced. It is the in-memory half of Load and shares its
// verification, so a snapshot held in memory is checked exactly as strictly as one on disk.
func Decode(b []byte) (cmds [][][]byte, savedAt time.Time, err error) {
	return readFrom(bytes.NewReader(b), int64(len(b)))
}

// readFrom is the one decoder. limit is the total number of bytes available, or -1 when that
// is unknown.
func readFrom(src io.Reader, limit int64) (cmds [][][]byte, savedAt time.Time, err error) {
	f := src
	line, err := readHeaderLine(f)
	if err != nil {
		return nil, time.Time{}, err
	}
	if !strings.HasPrefix(line, magic) {
		// Named explicitly, because the file most likely to be handed to this reader by
		// mistake is a real Redis dump.rdb, whose first five bytes are "REDIS".
		if strings.HasPrefix(line, "REDIS") {
			return nil, time.Time{}, fmt.Errorf("%w: this looks like a Redis RDB file, which shardkv cannot read", ErrNotSnapshot)
		}
		return nil, time.Time{}, fmt.Errorf("%w: header is %q", ErrNotSnapshot, truncate(line, 40))
	}

	var fields [fixedFields]byte
	if _, err := io.ReadFull(f, fields[:]); err != nil {
		return nil, time.Time{}, fmt.Errorf("%w: header is short: %w", ErrCorrupt, err)
	}
	savedMs := binary.BigEndian.Uint64(fields[0:8])
	wantCmds := binary.BigEndian.Uint64(fields[8:16])
	bodyLen := binary.BigEndian.Uint64(fields[16:24])
	savedAt = time.UnixMilli(int64(savedMs))

	// The declared length is checked against how much of the file there actually is *before*
	// it is used as an allocation. It is a number read from a file this process does not
	// control, and `make([]byte, n)` with a corrupt n is an out-of-memory kill of the whole
	// server -- the same class of hazard as invariant 15's negative capacity, reached here
	// through a damaged backup rather than through a client operand.
	if limit >= 0 {
		consumed := int64(len(line)) + 1 + fixedFields // +1 for the header line's newline
		avail := max(limit-consumed-trailerLen, 0)
		if bodyLen > uint64(avail) {
			return nil, savedAt, fmt.Errorf("%w: header declares a %d-byte body but only %d bytes follow it",
				ErrCorrupt, bodyLen, avail)
		}
	}

	// The body is read as exactly the promised number of bytes, so a short file fails here
	// rather than parsing as a smaller dataset, and a long one cannot smuggle extra commands
	// past the count.
	body := make([]byte, bodyLen)
	if n, err := io.ReadFull(f, body); err != nil {
		return nil, savedAt, fmt.Errorf("%w: body stops %d bytes short of the %d the header declares: %w",
			ErrCorrupt, bodyLen-uint64(n), bodyLen, err)
	}
	var trailer [trailerLen]byte
	if _, err := io.ReadFull(f, trailer[:]); err != nil {
		return nil, savedAt, fmt.Errorf("%w: checksum is missing: %w", ErrCorrupt, err)
	}
	if got, want := crc64.Checksum(body, crcTable), binary.BigEndian.Uint64(trailer[:]); got != want {
		return nil, savedAt, fmt.Errorf("%w: checksum is %016x, the file says %016x", ErrCorrupt, got, want)
	}

	cmds, err = parseBody(body, wantCmds)
	if err != nil {
		return nil, savedAt, err
	}
	return cmds, savedAt, nil
}

// parseBody decodes the RESP command stream and checks that it holds exactly want
// commands. Reading through the same resp.Reader an AOF replay and a replica feed use is
// deliberate: a snapshot the writer produced but that reader would reject -- a command
// past resp.MaxMultiBulk, say, which is what invariant 5's chunking exists to prevent --
// fails here, in a test, rather than on an operator's restart.
func parseBody(body []byte, want uint64) ([][][]byte, error) {
	r := resp.NewReader(bytes.NewReader(body))
	cmds := make([][][]byte, 0, want)
	for {
		args, err := r.ReadCommand()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("%w: command %d does not parse: %w", ErrCorrupt, len(cmds)+1, err)
		}
		cmds = append(cmds, args)
	}
	if uint64(len(cmds)) != want {
		return nil, fmt.Errorf("%w: body holds %d commands, the header declares %d",
			ErrCorrupt, len(cmds), want)
	}
	return cmds, nil
}

// readHeaderLine reads up to and including the first newline, refusing anything longer than
// maxHeaderLine. It reads one byte at a time because the reader must not consume past the
// newline: the bytes after it are the binary header, and a buffered reader would swallow
// them.
func readHeaderLine(f io.Reader) (string, error) {
	var b [1]byte
	var line []byte
	for len(line) < maxHeaderLine {
		if _, err := io.ReadFull(f, b[:]); err != nil {
			return "", fmt.Errorf("%w: no header line: %w", ErrNotSnapshot, err)
		}
		if b[0] == '\n' {
			return string(line), nil
		}
		line = append(line, b[0])
	}
	return "", fmt.Errorf("%w: no newline in the first %d bytes", ErrNotSnapshot, maxHeaderLine)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// syncDir fsyncs the directory containing path so a rename survives a crash. Best effort:
// errors (e.g. on platforms that disallow a directory fsync) are ignored, exactly as in
// aof.Log.Rewrite -- the data is already on disk, and failing the save over the durability
// of its directory entry would be the wrong trade.
func syncDir(path string) {
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return
	}
	_ = d.Sync()
	_ = d.Close()
}
