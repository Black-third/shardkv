package store

import (
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
