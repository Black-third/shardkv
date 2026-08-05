package store

import (
	"strconv"
	"testing"
	"time"
)

func TestDumpPreservesTTL(t *testing.T) {
	s := New(4)
	cur := time.Unix(1000, 0)
	s.clock = func() time.Time { return cur }

	s.Set("vol", []byte("v"), 30*time.Second) // deadline = unix 1030 => 1030000 ms
	s.Set("perm", []byte("p"), 0)

	cmds := s.Dump()

	var gotPexpireat string
	permHasExpiry := false
	for _, c := range cmds {
		if string(c[0]) == "PEXPIREAT" {
			switch string(c[1]) {
			case "vol":
				gotPexpireat = string(c[2])
			case "perm":
				permHasExpiry = true
			}
		}
	}
	if gotPexpireat != "1030000" {
		t.Fatalf("Dump PEXPIREAT for vol = %q; want 1030000", gotPexpireat)
	}
	if permHasExpiry {
		t.Fatal("Dump emitted a PEXPIREAT for a persistent key")
	}
}

// TestDumpChunksLargeCollections checks that a collection is emitted as several
// bounded commands instead of one command per key. A single command carrying every
// element of a big collection produces a RESP array past the protocol's multibulk
// limit, and the reader on the far end rejects the whole stream.
func TestDumpChunksLargeCollections(t *testing.T) {
	const n = 700 // > 2 chunks at dumpChunkElems, and not a multiple of it

	s := New(4)
	for i := 0; i < n; i++ {
		v := []byte(strconv.Itoa(i))
		if _, err := s.RPush("list", v); err != nil {
			t.Fatal(err)
		}
		if _, err := s.SAdd("set", strconv.Itoa(i)); err != nil {
			t.Fatal(err)
		}
		if _, err := s.HSet("hash", [2][]byte{v, v}); err != nil {
			t.Fatal(err)
		}
		if _, _, err := s.ZAdd("zset", strconv.Itoa(i), float64(i)); err != nil {
			t.Fatal(err)
		}
	}

	counts := map[string]int{}
	for _, c := range s.Dump() {
		name := string(c[0])
		counts[name]++
		if got := len(c) - 2; got > dumpChunkElems {
			t.Fatalf("%s carries %d elements; want at most %d", name, got, dumpChunkElems)
		}
	}

	// stride-1 commands fit dumpChunkElems elements per chunk, stride-2 ones half
	// that many items, so each collection needs more than one command.
	cases := []struct {
		name string
		want int
	}{
		{"RPUSH", (n + dumpChunkElems - 1) / dumpChunkElems},
		{"SADD", (n + dumpChunkElems - 1) / dumpChunkElems},
		{"HSET", (2*n + dumpChunkElems - 1) / dumpChunkElems},
		{"ZADD", (2*n + dumpChunkElems - 1) / dumpChunkElems},
	}
	for _, c := range cases {
		if c.want < 2 {
			t.Fatalf("test does not force chunking for %s", c.name)
		}
		if counts[c.name] != c.want {
			t.Errorf("Dump emitted %d %s commands; want %d", counts[c.name], c.name, c.want)
		}
	}
}

// TestDumpPutsExpiryAfterEveryChunk pins the ordering a chunked replay depends on:
// a key's PEXPIREAT must follow the last chunk that builds it, or the deadline
// would be set on a half-built key (and, for a past deadline, on a key whose
// remaining chunks then resurrect it).
func TestDumpPutsExpiryAfterEveryChunk(t *testing.T) {
	s := New(1)
	cur := time.Unix(1000, 0)
	s.clock = func() time.Time { return cur }

	for i := 0; i < 600; i++ {
		s.SAdd("big", strconv.Itoa(i))
	}
	if !s.Expire("big", 30*time.Second) {
		t.Fatal("Expire on the collection failed")
	}

	cmds := s.Dump()
	lastSAdd, expiryAt := -1, -1
	for i, c := range cmds {
		switch string(c[0]) {
		case "SADD":
			lastSAdd = i
		case "PEXPIREAT":
			expiryAt = i
		}
	}
	if expiryAt < 0 || lastSAdd < 0 {
		t.Fatalf("Dump did not emit both SADD chunks and a PEXPIREAT: %d cmds", len(cmds))
	}
	if expiryAt < lastSAdd {
		t.Errorf("PEXPIREAT at index %d precedes the last SADD chunk at %d", expiryAt, lastSAdd)
	}
}
