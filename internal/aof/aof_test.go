package aof

import (
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
