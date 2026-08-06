package server

import (
	"bytes"
	"errors"
	"io"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

// fixedClock is the instant both stores in the round-trip run on, so a TTL carried
// across the snapshot can be compared exactly rather than approximately.
var fixedClock = time.Unix(1_600_000_000, 0)

// TestSnapshotRoundTripsLargeCollections is the end-to-end check on the chunked
// snapshot: encode Dump through the same writer a replica feed and an AOF rewrite
// use, read it back with the same reader they are read by, replay it into a fresh
// store, and require the result to be identical.
//
// The collections are large enough that each spans several commands, so this
// exercises the property chunked replay depends on: the first command for a key
// creates it, the rest append, and the key's PEXPIREAT arrives after all of them.
func TestSnapshotRoundTripsLargeCollections(t *testing.T) {
	const n = 1500 // several chunks per collection

	src := store.New(8)
	src.SetClock(func() time.Time { return fixedClock })

	for i := 0; i < n; i++ {
		v := []byte(strconv.Itoa(i))
		if _, err := src.RPush("list", v); err != nil {
			t.Fatal(err)
		}
		if _, err := src.SAdd("set", strconv.Itoa(i)); err != nil {
			t.Fatal(err)
		}
		if _, err := src.HSet("hash", [2][]byte{v, []byte("val" + strconv.Itoa(i))}); err != nil {
			t.Fatal(err)
		}
		if _, _, err := src.ZAdd("zset", strconv.Itoa(i), float64(i)*1.5); err != nil {
			t.Fatal(err)
		}
	}
	src.Set("str", []byte("hello"), 0)
	src.Set("volatile", []byte("v"), 90*time.Second)
	// A chunked collection that also carries a TTL: its PEXPIREAT has to survive
	// being emitted after the last chunk.
	for i := 0; i < 700; i++ {
		src.SAdd("volset", strconv.Itoa(i))
	}
	if !src.Expire("volset", 120*time.Second) {
		t.Fatal("Expire on volset failed")
	}

	snapshot := src.Dump()

	// Every emitted command must fit the protocol's multibulk limit. One command per
	// key does not: a big enough collection produced an array past this bound and the
	// reader rejected the entire stream, losing the dataset on reload or resync.
	for _, cmd := range snapshot {
		if len(cmd) > resp.MaxMultiBulk {
			t.Fatalf("%s carries %d arguments; the reader rejects anything over %d",
				cmd[0], len(cmd), resp.MaxMultiBulk)
		}
	}

	// Encode, then decode with the reader the AOF loader and replica use.
	var buf bytes.Buffer
	w := resp.NewWriter(&buf)
	for _, cmd := range snapshot {
		if err := w.WriteCommand(cmd); err != nil {
			t.Fatalf("encoding the snapshot: %v", err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("flushing the snapshot: %v", err)
	}

	r := resp.NewReader(&buf)
	var decoded [][][]byte
	for {
		args, err := r.ReadCommand()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("the snapshot stream was rejected after %d commands: %v", len(decoded), err)
		}
		decoded = append(decoded, args)
	}
	if len(decoded) != len(snapshot) {
		t.Fatalf("decoded %d commands; want %d", len(decoded), len(snapshot))
	}

	// Replay into a fresh store, exactly as a replica seed or an AOF replay does.
	dstStore := store.New(8)
	dstStore.SetClock(func() time.Time { return fixedClock })
	New(dstStore).ReplayCommands(decoded)

	assertSameContents(t, src, dstStore, n)
}

func assertSameContents(t *testing.T, src, dst *store.Store, n int) {
	t.Helper()

	if got := dst.Len(); got != src.Len() {
		t.Errorf("replayed store holds %d keys; want %d", got, src.Len())
	}

	// String.
	if v, ok := dst.Get("str"); !ok || string(v) != "hello" {
		t.Errorf("replayed str = %q,%v; want \"hello\",true", v, ok)
	}

	// List: order matters, and it must not have been duplicated or reordered by the
	// chunk boundaries.
	srcList, _ := src.LRange("list", 0, -1)
	dstList, _ := dst.LRange("list", 0, -1)
	if len(dstList) != n {
		t.Errorf("replayed list has %d elements; want %d", len(dstList), n)
	}
	for i := range srcList {
		if i >= len(dstList) || !bytes.Equal(srcList[i], dstList[i]) {
			t.Fatalf("replayed list differs at index %d", i)
		}
	}

	// Set.
	srcSet, _ := src.SMembers("set")
	dstSet, _ := dst.SMembers("set")
	sort.Strings(srcSet)
	sort.Strings(dstSet)
	if len(dstSet) != n {
		t.Errorf("replayed set has %d members; want %d", len(dstSet), n)
	}
	for i := range srcSet {
		if i >= len(dstSet) || srcSet[i] != dstSet[i] {
			t.Fatalf("replayed set differs at index %d", i)
		}
	}

	// Hash: compare field by field.
	if got, _ := dst.HLen("hash"); got != n {
		t.Errorf("replayed hash has %d fields; want %d", got, n)
	}
	for i := 0; i < n; i++ {
		f := strconv.Itoa(i)
		want, _, _ := src.HGet("hash", f)
		got, ok, _ := dst.HGet("hash", f)
		if !ok || !bytes.Equal(got, want) {
			t.Fatalf("replayed hash field %q = %q,%v; want %q", f, got, ok, want)
		}
	}

	// Sorted set: members, scores, and rank order.
	srcZ, _ := src.ZRange("zset", 0, -1)
	dstZ, _ := dst.ZRange("zset", 0, -1)
	if len(dstZ) != n {
		t.Errorf("replayed zset has %d members; want %d", len(dstZ), n)
	}
	for i := range srcZ {
		if i >= len(dstZ) || srcZ[i] != dstZ[i] {
			t.Fatalf("replayed zset differs at rank %d: %+v vs %+v", i, srcZ[i], dstZ[i])
		}
	}

	// TTLs: a chunked collection's deadline and a plain key's deadline both survive.
	for _, key := range []string{"volatile", "volset"} {
		wantD, wantHas, wantOK := src.TTL(key)
		gotD, gotHas, gotOK := dst.TTL(key)
		if gotOK != wantOK || gotHas != wantHas || gotD != wantD {
			t.Errorf("replayed TTL(%s) = %v,%v,%v; want %v,%v,%v",
				key, gotD, gotHas, gotOK, wantD, wantHas, wantOK)
		}
	}
	if got, _ := dst.SCard("volset"); got != 700 {
		t.Errorf("replayed volset has %d members; want 700", got)
	}
	// A persistent key must not have picked up a deadline.
	if _, hasTTL, _ := dst.TTL("str"); hasTTL {
		t.Error("replayed str gained a TTL")
	}
}
