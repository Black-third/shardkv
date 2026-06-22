package server

import (
	"strconv"
	"testing"
)

func TestGlobMatch(t *testing.T) {
	cases := []struct {
		pattern, s string
		want       bool
	}{
		{"*", "anything", true},
		{"", "", true},
		{"", "x", false},
		{"user:*", "user:42", true},
		{"user:*", "admin:42", false},
		{"k?", "k1", true},
		{"k?", "k12", false},
		{"abc", "abc", true},
		{"a*c", "abbbc", true},
		{"a*c", "abbbd", false},
		{"*suffix", "has-suffix", true},
		{"a*b*c", "axxbyyc", true},
	}
	for _, c := range cases {
		if got := globMatch(c.pattern, c.s); got != c.want {
			t.Errorf("globMatch(%q, %q) = %v; want %v", c.pattern, c.s, got, c.want)
		}
	}
}

func TestScanCommand(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	for i := 0; i < 20; i++ {
		c.cmd("SET k" + strconv.Itoa(i) + " v")
	}
	c.cmd("SET other 1")

	// A single SCAN with a large COUNT returns everything and cursor 0.
	full := c.cmd("SCAN 0 COUNT 1000")
	if !contains(full, "[0 [") {
		t.Fatalf("SCAN reply not [cursor [keys]] with cursor 0: %s", full)
	}
	for _, want := range []string{"k0", "k19", "other"} {
		if !contains(full, want) {
			t.Errorf("SCAN result missing %q: %s", want, full)
		}
	}

	// MATCH filters out non-matching keys.
	matched := c.cmd("SCAN 0 MATCH k* COUNT 1000")
	if !contains(matched, "k5") {
		t.Errorf("SCAN MATCH k* missing k5: %s", matched)
	}
	if contains(matched, "other") {
		t.Errorf("SCAN MATCH k* should exclude 'other': %s", matched)
	}
}
