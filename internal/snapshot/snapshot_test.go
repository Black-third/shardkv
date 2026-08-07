package snapshot

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
)

func cmd(parts ...string) [][]byte {
	out := make([][]byte, 0, len(parts))
	for _, p := range parts {
		out = append(out, []byte(p))
	}
	return out
}

var sample = [][][]byte{
	cmd("SET", "greeting", "hello"),
	cmd("RPUSH", "list", "a", "b", "c"),
	cmd("HSET", "hash", "f", "v", "g", "w"),
	cmd("PEXPIREAT", "greeting", "1700000000000"),
	cmd("SELECT", "3"),
	cmd("SADD", "tags", "x"),
	// A value with an embedded NUL, CR and LF: the body is length-prefixed RESP, so none
	// of them may be treated as a delimiter anywhere in the format.
	cmd("SET", "binary\x00key", "a\r\nb\x00c"),
	cmd("SET", "empty", ""),
}

func TestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.skv")
	at := time.UnixMilli(1_700_000_123_456)
	if err := Save(path, sample, at); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, savedAt, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !savedAt.Equal(at) {
		t.Errorf("savedAt = %v, want %v", savedAt, at)
	}
	requireSameCommands(t, got, sample)
}

func TestEncodeDecodeMatchesSaveLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.skv")
	at := time.UnixMilli(1_700_000_123_456)
	if err := Save(path, sample, at); err != nil {
		t.Fatalf("Save: %v", err)
	}
	onDisk, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	inMemory, err := Encode(sample, at)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	// One encoder, so the file and the in-memory form must be identical byte for byte. Two
	// encoders would be free to drift, and DEBUG RELOAD on a server with no snapshot path
	// would then be checking a format nothing else writes.
	if !bytes.Equal(onDisk, inMemory) {
		t.Fatalf("Encode and Save produced different bytes (%d vs %d)", len(inMemory), len(onDisk))
	}
	got, savedAt, err := Decode(inMemory)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !savedAt.Equal(at) {
		t.Errorf("savedAt = %v, want %v", savedAt, at)
	}
	requireSameCommands(t, got, sample)
}

func TestEmptySnapshotRoundTrips(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.skv")
	if err := Save(path, nil, time.UnixMilli(1)); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, _, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %d commands from an empty snapshot", len(got))
	}
}

// TestDeclaredBodyLengthMatchesTheBody pins the one arithmetic the format depends on: the
// body length is computed from resp.CommandSize before anything is written, and every load
// reads exactly that many bytes. If the two ever disagree, every snapshot this build writes
// is unloadable -- and it would be unloadable *silently*, because a short read is reported
// as corruption rather than as a bug in the writer.
func TestDeclaredBodyLengthMatchesTheBody(t *testing.T) {
	blob, err := Encode(sample, time.UnixMilli(1))
	if err != nil {
		t.Fatal(err)
	}
	line := blob[:bytes.IndexByte(blob, '\n')+1]
	fields := blob[len(line) : len(line)+fixedFields]
	declared := binary.BigEndian.Uint64(fields[16:24])
	actual := uint64(len(blob)) - uint64(len(line)) - fixedFields - trailerLen
	if declared != actual {
		t.Fatalf("header declares a %d-byte body, the file holds %d", declared, actual)
	}
	if got := binary.BigEndian.Uint64(fields[8:16]); got != uint64(len(sample)) {
		t.Fatalf("header declares %d commands, %d were written", got, len(sample))
	}
}

// TestHeaderSaysItIsNotAnRDB is the one test on the file's text. The header is the only place
// an operator who found the file, or a tool that sniffed it, learns what it is -- and the
// claim it has to carry is the negative one.
func TestHeaderSaysItIsNotAnRDB(t *testing.T) {
	blob, err := Encode(nil, time.UnixMilli(1))
	if err != nil {
		t.Fatal(err)
	}
	line := string(blob[:bytes.IndexByte(blob, '\n')])
	if !strings.HasPrefix(line, "SHARDKV-SNAPSHOT v1") {
		t.Errorf("header line does not start with the magic: %q", line)
	}
	if !strings.Contains(line, "NOT an RDB") {
		t.Errorf("header line does not say it is not an RDB: %q", line)
	}
	// And it must not start with what an RDB starts with, or `file`, redis-check-rdb and
	// every sniffing tool would guess wrong in the one direction that matters.
	if strings.HasPrefix(line, "REDIS") {
		t.Errorf("header line begins with REDIS: %q", line)
	}
}

func TestMissingFileIsNotAnError(t *testing.T) {
	cmds, _, err := Load(filepath.Join(t.TempDir(), "absent.skv"))
	if err != nil {
		t.Fatalf("a missing snapshot must read as a fresh server, got %v", err)
	}
	if cmds != nil {
		t.Fatalf("got %d commands from a missing file", len(cmds))
	}
}

// TestCorruptionIsRefusedNotPartiallyLoaded is the property the whole framing exists for. A
// bare command stream cannot tell a truncated file from a smaller dataset; each of these
// mutilations has to be an error rather than a shorter load.
func TestCorruptionIsRefusedNotPartiallyLoaded(t *testing.T) {
	good, err := Encode(sample, time.UnixMilli(1))
	if err != nil {
		t.Fatal(err)
	}
	lineLen := bytes.IndexByte(good, '\n') + 1

	cases := []struct {
		name string
		want error
		make func() []byte
	}{
		{"truncated mid-body", ErrCorrupt, func() []byte {
			return append([]byte(nil), good[:len(good)-20]...)
		}},
		{"body cut to nothing", ErrCorrupt, func() []byte {
			return append([]byte(nil), good[:lineLen+fixedFields]...)
		}},
		{"a flipped byte in the body", ErrCorrupt, func() []byte {
			b := append([]byte(nil), good...)
			b[lineLen+fixedFields+9] ^= 0xff
			return b
		}},
		{"a flipped byte in the checksum", ErrCorrupt, func() []byte {
			b := append([]byte(nil), good...)
			b[len(b)-1] ^= 0xff
			return b
		}},
		{"command count overstated", ErrCorrupt, func() []byte {
			b := append([]byte(nil), good...)
			binary.BigEndian.PutUint64(b[lineLen+8:lineLen+16], uint64(len(sample)+1))
			return b
		}},
		{"body length overstated", ErrCorrupt, func() []byte {
			b := append([]byte(nil), good...)
			binary.BigEndian.PutUint64(b[lineLen+16:lineLen+24], 1<<40)
			return b
		}},
		{"not a snapshot at all", ErrNotSnapshot, func() []byte {
			return []byte("REDIS0011\xfa\x09redis-ver\x057.2.0")
		}},
		{"no newline anywhere", ErrNotSnapshot, func() []byte {
			return bytes.Repeat([]byte("x"), maxHeaderLine+10)
		}},
		{"a v2 header this reader must refuse by name", ErrNotSnapshot, func() []byte {
			b := append([]byte(nil), good...)
			return append([]byte("SHARDKV-SNAPSHOT v2 -- from the future\n"), b[lineLen:]...)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			blob := tc.make()
			cmds, _, err := Decode(blob)
			if err == nil {
				t.Fatalf("loaded %d commands from a %s snapshot; corruption must be reported, "+
					"never served as a smaller dataset", len(cmds), tc.name)
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("error is %v, want it to wrap %v", err, tc.want)
			}
			if cmds != nil {
				t.Fatalf("a refused snapshot must yield no commands, got %d", len(cmds))
			}
			// And on disk, through the same path a restart takes.
			path := filepath.Join(t.TempDir(), "dump.skv")
			if err := os.WriteFile(path, blob, 0o644); err != nil {
				t.Fatal(err)
			}
			if _, _, err := Load(path); err == nil {
				t.Fatalf("Load accepted the %s snapshot that Decode refused", tc.name)
			}
		})
	}
}

// TestAnRDBIsNamedAsSuch: the file most likely to be handed to this reader by mistake is a
// real dump.rdb, and "not a shardkv snapshot" is a much less useful thing to print than
// "this is an RDB and shardkv cannot read one".
func TestAnRDBIsNamedAsSuch(t *testing.T) {
	_, _, err := Decode([]byte("REDIS0011\xfa\x09redis-ver\x057.2.0\n"))
	if err == nil {
		t.Fatal("an RDB file must be refused")
	}
	if !strings.Contains(err.Error(), "RDB") {
		t.Errorf("the error should name RDB, got %q", err)
	}
}

// TestSaveIsAtomic checks the property a backup depends on: a good snapshot is never replaced
// by a broken one. The write goes to a temporary file and is renamed, so a failure part-way
// through cannot have touched the live file.
func TestSaveIsAtomic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dump.skv")
	if err := Save(path, sample, time.UnixMilli(1)); err != nil {
		t.Fatal(err)
	}
	first, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// A save that cannot even open its temporary file: the directory is replaced by one the
	// process may read but not write.
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Skipf("cannot make the directory read-only here: %v", err)
	}
	saveErr := Save(path, append(sample, cmd("SET", "later", "value")), time.UnixMilli(2))
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if saveErr == nil {
		t.Skip("the filesystem allowed the write anyway (running as root?)")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("the previous snapshot is gone after a failed save: %v", err)
	}
	if !bytes.Equal(first, after) {
		t.Fatal("a failed save changed the existing snapshot")
	}
	if _, _, err := Load(path); err != nil {
		t.Fatalf("the existing snapshot no longer loads after a failed save: %v", err)
	}
}

// TestFailedSaveLeavesNoTemporaryFile: a save that fails must not leave debris beside a good
// snapshot, or a directory accumulates one .tmp per failure and an operator cannot tell which
// file is the backup.
func TestFailedSaveLeavesNoTemporaryFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dump.skv")
	// A command whose bulk length exceeds what the reader accepts is not what fails here --
	// the writer has no such limit. The failure is arranged by making the destination a
	// directory, so the rename cannot succeed.
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := Save(path, sample, time.UnixMilli(1)); err == nil {
		t.Skip("this filesystem renamed a file over a directory")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Fatalf("a failed save left %s behind", e.Name())
		}
	}
}

// TestBodyIsAPlainRESPStream backs the mitigation the package comment claims for not being
// RDB: the body can be replayed into any Redis server with `redis-cli --pipe`, because it is
// nothing but the commands.
func TestBodyIsAPlainRESPStream(t *testing.T) {
	blob, err := Encode(sample, time.UnixMilli(1))
	if err != nil {
		t.Fatal(err)
	}
	lineLen := bytes.IndexByte(blob, '\n') + 1
	body := blob[lineLen+fixedFields : len(blob)-trailerLen]
	r := resp.NewReader(bytes.NewReader(body))
	for i := range sample {
		args, err := r.ReadCommand()
		if err != nil {
			t.Fatalf("command %d: %v", i, err)
		}
		if strconv.Quote(string(bytes.Join(args, []byte(" ")))) !=
			strconv.Quote(string(bytes.Join(sample[i], []byte(" ")))) {
			t.Fatalf("command %d differs", i)
		}
	}
}

func requireSameCommands(t *testing.T, got, want [][][]byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d commands, want %d", len(got), len(want))
	}
	for i := range want {
		if len(got[i]) != len(want[i]) {
			t.Fatalf("command %d: got %d args, want %d", i, len(got[i]), len(want[i]))
		}
		for j := range want[i] {
			if !bytes.Equal(got[i][j], want[i][j]) {
				t.Fatalf("command %d arg %d: got %q, want %q", i, j, got[i][j], want[i][j])
			}
		}
	}
}
