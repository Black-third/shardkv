package server

import (
	"bytes"
	"strings"
	"testing"

	"github.com/Black-third/shardkv/internal/resp"
)

// TestReplBacklogRetainsTail is the ring's contract, table-driven over the cases a
// partial resync depends on: a range still retained is served exactly, one that has
// scrolled off is refused, and the offsets bound both.
func TestReplBacklogRetainsTail(t *testing.T) {
	b := newReplBacklog(10)
	b.feed([]byte("abcdefgh")) // 8 of 10 bytes

	cases := []struct {
		name string
		from int64
		want string
		ok   bool
	}{
		{"from the beginning", 0, "abcdefgh", true},
		{"from the middle", 3, "defgh", true},
		{"fully caught up", 8, "", true},
		{"past the end", 9, "", false},
		{"before the start", -1, "", false},
	}
	for _, tc := range cases {
		got, ok := b.read(tc.from)
		if ok != tc.ok {
			t.Errorf("%s: read(%d) ok = %v; want %v", tc.name, tc.from, ok, tc.ok)
			continue
		}
		if ok && string(got) != tc.want {
			t.Errorf("%s: read(%d) = %q; want %q", tc.name, tc.from, got, tc.want)
		}
	}

	// Wrapping: the oldest bytes are discarded and only the tail stays readable.
	b.feed([]byte("ijklm")) // 13 bytes total, 10 retained: "defghijklm"
	if got := b.histLen(); got != 10 {
		t.Errorf("histLen = %d; want the full ring of 10", got)
	}
	if got, ok := b.read(3); !ok || string(got) != "defghijklm" {
		t.Errorf("read(3) after wrapping = %q, %v; want the whole retained tail", got, ok)
	}
	if _, ok := b.read(2); ok {
		t.Error("read(2) succeeded although those bytes have scrolled off")
	}
	if got, ok := b.read(11); !ok || string(got) != "lm" {
		t.Errorf("read(11) = %q, %v; want the last two bytes", got, ok)
	}
}

// TestReplBacklogHandlesOversizedWrites covers a single command larger than the whole
// ring: only its tail can be retained, and the stream offset still has to advance by
// every byte -- an offset that counted only what was retained would place every later
// command at the wrong position.
func TestReplBacklogHandlesOversizedWrites(t *testing.T) {
	b := newReplBacklog(8)
	b.feed([]byte("0123456789ABCDEF")) // 16 bytes into an 8-byte ring
	if b.end != 16 {
		t.Errorf("end = %d after feeding 16 bytes; want 16", b.end)
	}
	if b.start != 8 {
		t.Errorf("start = %d; want 8, the oldest byte still retained", b.start)
	}
	if got, ok := b.read(8); !ok || string(got) != "89ABCDEF" {
		t.Errorf("read(8) = %q, %v; want the retained tail", got, ok)
	}
	if _, ok := b.read(7); ok {
		t.Error("read(7) succeeded although that byte was discarded")
	}
	// And the ring keeps working afterwards.
	b.feed([]byte("xy"))
	if got, ok := b.read(16); !ok || string(got) != "xy" {
		t.Errorf("read(16) after an oversized write = %q, %v", got, ok)
	}
}

// TestDisabledReplBacklogNeverContinues covers repl-backlog-size 0: the offset still
// tracks the stream, but no range is ever retained, so every PSYNC gets a full resync.
func TestDisabledReplBacklogNeverContinues(t *testing.T) {
	b := newReplBacklog(0)
	b.feed([]byte("hello"))
	if b.end != 5 {
		t.Errorf("end = %d; a disabled backlog must still track the stream position", b.end)
	}
	if b.histLen() != 0 {
		t.Errorf("histLen = %d; a disabled backlog retains nothing", b.histLen())
	}
	if _, ok := b.read(0); ok {
		t.Error("a disabled backlog served a continuation")
	}
	// Being exactly caught up is still answerable: there is nothing to send.
	if _, ok := b.read(5); !ok {
		t.Error("a disabled backlog refused a caught-up replica, which needs no bytes")
	}
}

// TestEncodeCommandMatchesTheWriter is the invariant the offsets rest on: the bytes
// counted into the offset and stored in the backlog have to be exactly the bytes a
// replica receives. If the two encoders ever disagreed, a continuation would resume
// mid-command and the replica would reject the stream.
func TestEncodeCommandMatchesTheWriter(t *testing.T) {
	cases := [][]string{
		{"PING"},
		{"SET", "k", "v"},
		{"SET", "key", strings.Repeat("x", 300)},
		{"MSET", "a", "1", "b", "2"},
		{"SET", "empty", ""},
	}
	for _, parts := range cases {
		args := cmdArgs(parts...)
		var buf bytes.Buffer
		w := resp.NewWriter(&buf)
		if err := w.WriteCommand(args); err != nil {
			t.Fatalf("WriteCommand: %v", err)
		}
		w.Flush()
		if got := encodeCommand(args); !bytes.Equal(got, buf.Bytes()) {
			t.Errorf("encodeCommand(%v) = %q; the writer produced %q", parts, got, buf.Bytes())
		}
		if got, want := len(buf.Bytes()), resp.CommandSize(args); got != want {
			t.Errorf("CommandSize(%v) = %d; the writer wrote %d bytes", parts, want, got)
		}
	}
}
