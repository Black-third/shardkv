package aof

import (
	"os"
	"path/filepath"
	"testing"
)

func flatten(cmds [][][]byte) string {
	out := ""
	for _, c := range cmds {
		for _, a := range c {
			out += string(a) + " "
		}
		out += "| "
	}
	return out
}

func TestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.aof")
	l, err := Open(path, SyncAlways)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	want := [][][]byte{
		{[]byte("SET"), []byte("k"), []byte("v")},
		{[]byte("RPUSH"), []byte("l"), []byte("a"), []byte("b")},
		{[]byte("INCR"), []byte("n")},
	}
	for _, c := range want {
		if err := l.Append(c); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if flatten(got) != flatten(want) {
		t.Fatalf("Load = %q; want %q", flatten(got), flatten(want))
	}
}

func TestLoadMissingFile(t *testing.T) {
	cmds, err := Load(filepath.Join(t.TempDir(), "does-not-exist.aof"))
	if err != nil {
		t.Fatalf("Load missing = %v; want nil", err)
	}
	if cmds != nil {
		t.Fatalf("Load missing returned %d commands; want 0", len(cmds))
	}
}

func TestLoadStopsAtTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.aof")
	l, _ := Open(path, SyncAlways)
	l.Append([][]byte{[]byte("SET"), []byte("k"), []byte("v")})
	l.Close()

	// Simulate a crash that left a half-written final record.
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	f.WriteString("*3\r\n$3\r\nSET\r\n$1\r\nx\r\n") // truncated: missing 3rd arg
	f.Close()

	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// The intact first record survives; the torn tail is dropped.
	want := [][][]byte{{[]byte("SET"), []byte("k"), []byte("v")}}
	if flatten(got) != flatten(want) {
		t.Fatalf("Load = %q; want %q", flatten(got), flatten(want))
	}
}

func TestRewriteStaysUsableAfterReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.aof")
	l, _ := Open(path, SyncAlways)
	defer l.Close()
	l.SetLogger(func(string, ...any) {}) // silence expected warnings

	l.Append([][]byte{[]byte("SET"), []byte("a"), []byte("1")})
	if err := l.Rewrite([][][]byte{{[]byte("SET"), []byte("a"), []byte("2")}}); err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	// The log must still accept appends and persist them after the swap.
	if err := l.Append([][]byte{[]byte("SET"), []byte("b"), []byte("3")}); err != nil {
		t.Fatalf("Append after rewrite: %v", err)
	}
	got, _ := Load(path)
	want := [][][]byte{
		{[]byte("SET"), []byte("a"), []byte("2")},
		{[]byte("SET"), []byte("b"), []byte("3")},
	}
	if flatten(got) != flatten(want) {
		t.Fatalf("Load = %q; want %q", flatten(got), flatten(want))
	}
}

func TestRewriteCompacts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.aof")
	l, err := Open(path, SyncAlways)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	// Simulate a long history.
	for i := 0; i < 100; i++ {
		l.Append([][]byte{[]byte("INCR"), []byte("n")})
	}
	// Compact to the equivalent single command.
	snapshot := [][][]byte{{[]byte("SET"), []byte("n"), []byte("100")}}
	if err := l.Rewrite(snapshot); err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	// New appends still work after the swap.
	l.Append([][]byte{[]byte("INCR"), []byte("n")})

	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := [][][]byte{
		{[]byte("SET"), []byte("n"), []byte("100")},
		{[]byte("INCR"), []byte("n")},
	}
	if flatten(got) != flatten(want) {
		t.Fatalf("after rewrite Load = %q; want %q", flatten(got), flatten(want))
	}
}
