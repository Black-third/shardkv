package server

import (
	"context"
	"io"
	"testing"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

// startNotifyServer starts a server with the given notify-keyspace-events flags.
func startNotifyServer(t *testing.T, flags string) (*Server, string, func()) {
	t.Helper()
	s := New(store.New(8))
	if !s.SetNotifyKeyspaceEvents(flags) {
		t.Fatalf("SetNotifyKeyspaceEvents(%q) rejected the flags", flags)
	}
	if err := s.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Serve(ctx); close(done) }()
	return s, s.Addr().String(), func() {
		cancel()
		<-done
	}
}

// TestNotifyFlagParsing is the table for the flag characters, including the ones that
// have to be refused and the K/E rule that decides whether anything is delivered at
// all.
func TestNotifyFlagParsing(t *testing.T) {
	cases := []struct {
		spec  string
		valid bool
		want  string // what CONFIG GET must report back
	}{
		{"", true, ""},
		{"KEA", true, "AKE"},
		{"AKE", true, "AKE"},
		{"KEx", true, "xKE"},
		// Spelling out every event class collapses back to "A". The stream class ('t')
		// is one of them, so a specification that omits it is *not* the full set and
		// must be reported literally rather than as "A".
		{"Kg$lshzxet", true, "AK"},
		{"Kg$lshzxe", true, "g$lshzxeK"},
		{"gxE", true, "gxE"},
		{"KEt", true, "tKE"},
		{"Elshz", true, "lshzE"},
		// Event classes with no delivery selector deliver nothing, so they read back
		// as disabled rather than as a setting that quietly does nothing.
		{"g", true, ""},
		{"A", true, ""},
		// Unknown characters are rejected, not ignored.
		{"KEq", false, ""},
		{"KE!", false, ""},
	}
	for _, tc := range cases {
		s := New(store.New(4))
		if got := s.SetNotifyKeyspaceEvents(tc.spec); got != tc.valid {
			t.Errorf("SetNotifyKeyspaceEvents(%q) = %v; want %v", tc.spec, got, tc.valid)
			continue
		}
		if !tc.valid {
			continue
		}
		if got := s.NotifyKeyspaceEvents(); got != tc.want {
			t.Errorf("after setting %q, NotifyKeyspaceEvents() = %q; want %q", tc.spec, got, tc.want)
		}
	}

	// A rejected specification must leave the previous setting alone.
	s := New(store.New(4))
	s.SetNotifyKeyspaceEvents("KEA")
	s.SetNotifyKeyspaceEvents("bogus")
	if got := s.NotifyKeyspaceEvents(); got != "AKE" {
		t.Errorf("a rejected specification changed the setting to %q", got)
	}
}

// TestKeyspaceNotificationsForWrites covers both channel families and the per-class
// filtering, over one command from each data type.
func TestKeyspaceNotificationsForWrites(t *testing.T) {
	_, addr, stop := startNotifyServer(t, "KEA")
	defer stop()

	sub := dialTx(t, addr)
	defer sub.close()
	subscribeCmd(t, sub, "PSUBSCRIBE __key*@0__:*", 1)
	c := dialTx(t, addr)
	defer c.close()
	waitFor(t, "the notification subscription to register", func() bool {
		return c.cmd("PUBSUB NUMPAT") == ":1"
	})

	cases := []struct {
		cmd   string
		key   string
		event string
	}{
		{"SET foo bar", "foo", "set"},
		{"APPEND foo !", "foo", "append"},
		{"INCRBY n 5", "n", "incrby"},
		{"EXPIRE foo 100", "foo", "expire"},
		{"PERSIST foo", "foo", "persist"},
		{"DEL foo", "foo", "del"},
		{"RPUSH l a", "l", "rpush"},
		{"LPOP l", "l", "lpop"},
		{"HSET h f v", "h", "hset"},
		{"HDEL h f", "h", "hdel"},
		{"SADD s m", "s", "sadd"},
		{"SREM s m", "s", "srem"},
		{"ZADD z 1 m", "z", "zadd"},
		{"ZREM z m", "z", "zrem"},
	}
	for _, tc := range cases {
		if reply := c.cmd(tc.cmd); len(reply) > 0 && reply[0] == '-' {
			t.Fatalf("%q: %s", tc.cmd, reply)
		}
		// Both channels fire, and they carry the same fact transposed.
		wantKeyspace := "[pmessage __key*@0__:* __keyspace@0__:" + tc.key + " " + tc.event + "]"
		wantKeyevent := "[pmessage __key*@0__:* __keyevent@0__:" + tc.event + " " + tc.key + "]"
		got := []string{nextMessage(t, sub), nextMessage(t, sub)}
		if got[0] != wantKeyspace || got[1] != wantKeyevent {
			t.Errorf("%q notified %v; want %q then %q", tc.cmd, got, wantKeyspace, wantKeyevent)
		}
	}
}

// TestKeyspaceNotificationClassFiltering checks a class that was not enabled stays
// silent, which is the whole point of the flag characters.
func TestKeyspaceNotificationClassFiltering(t *testing.T) {
	_, addr, stop := startNotifyServer(t, "KE$") // strings only
	defer stop()

	sub := dialTx(t, addr)
	defer sub.close()
	subscribeCmd(t, sub, "PSUBSCRIBE __keyevent@0__:*", 1)
	c := dialTx(t, addr)
	defer c.close()
	waitFor(t, "the notification subscription to register", func() bool {
		return c.cmd("PUBSUB NUMPAT") == ":1"
	})

	c.cmd("RPUSH l a") // list class: not enabled
	c.cmd("SADD s m")  // set class: not enabled
	c.cmd("DEL l")     // generic class: not enabled
	c.cmd("SET str v") // string class: enabled
	if got := nextMessage(t, sub); got != "[pmessage __keyevent@0__:* __keyevent@0__:set str]" {
		t.Errorf("first notification = %q; want only the string event", got)
	}
}

// TestKeyspaceNotificationsForTwoKeyCommands covers the commands whose two keys are
// described differently -- the case a single event name per command would get wrong.
func TestKeyspaceNotificationsForTwoKeyCommands(t *testing.T) {
	_, addr, stop := startNotifyServer(t, "KEA")
	defer stop()

	sub := dialTx(t, addr)
	defer sub.close()
	subscribeCmd(t, sub, "PSUBSCRIBE __keyevent@0__:*", 1)
	c := dialTx(t, addr)
	defer c.close()
	waitFor(t, "the notification subscription to register", func() bool {
		return c.cmd("PUBSUB NUMPAT") == ":1"
	})

	c.cmd("SET src v")
	if got := nextMessage(t, sub); !contains(got, "__keyevent@0__:set src") {
		t.Fatalf("setup notification = %q", got)
	}
	c.cmd("RENAME src dst")
	for _, want := range []string{"__keyevent@0__:rename_from src", "__keyevent@0__:rename_to dst"} {
		if got := nextMessage(t, sub); !contains(got, want) {
			t.Errorf("RENAME notified %q; want %q", got, want)
		}
	}
	c.cmd("RPUSH la a")
	nextMessage(t, sub)
	c.cmd("LMOVE la lb LEFT RIGHT")
	for _, want := range []string{"__keyevent@0__:lpop la", "__keyevent@0__:lpush lb"} {
		if got := nextMessage(t, sub); !contains(got, want) {
			t.Errorf("LMOVE notified %q; want %q", got, want)
		}
	}
}

// TestExpiredAndEvictedNotifications covers the two events that come from the store's
// removal hook rather than from a command. They are the reason the hook exists: nothing
// else learns that a key nobody ever read again is gone.
func TestExpiredAndEvictedNotifications(t *testing.T) {
	// Expiration, driven by the janitor through the removal hook.
	s, addr, stop := startNotifyServer(t, "KExe")
	defer stop()

	sub := dialTx(t, addr)
	defer sub.close()
	subscribeCmd(t, sub, "PSUBSCRIBE __keyevent@0__:*", 1)
	c := dialTx(t, addr)
	defer c.close()
	waitFor(t, "the notification subscription to register", func() bool {
		return c.cmd("PUBSUB NUMPAT") == ":1"
	})

	// Only x and e are enabled here, so the SETs below are silent and the only events
	// on this subscription are the two the removal hook produces.
	c.cmd("SET vanishing v PX 20")
	waitFor(t, "the key to expire", func() bool { return c.cmd("GET vanishing") == "(nil)" })
	if got := nextMessage(t, sub); !contains(got, "__keyevent@0__:expired vanishing") {
		t.Errorf("expiry notified %q; want the expired event", got)
	}

	// Eviction, driven by the eviction policy through the same hook.
	s.store.SetMaxKeys(1)
	c.cmd("SET a 1")
	c.cmd("SET b 2")
	s.store.EvictToLimit()
	if got := nextMessage(t, sub); !contains(got, "__keyevent@0__:evicted") {
		t.Errorf("eviction notified %q; want the evicted event", got)
	}
}

// TestKeyspaceNotificationsCostNothingWhenDisabled is the hot-path check. The
// notification hook runs on every dirty write, so with the feature off it must do no
// work: no allocation, and no string built from the command it was handed.
func TestKeyspaceNotificationsCostNothingWhenDisabled(t *testing.T) {
	s := New(store.New(4))
	if got := s.notifyFlags.Load(); got != 0 {
		t.Fatalf("notifications are enabled by default (flags = %d)", got)
	}
	args := cmdArgs("SET", "some-key", "some-value")
	if allocs := testing.AllocsPerRun(200, func() { s.notifyWrite(args) }); allocs != 0 {
		t.Errorf("notifyWrite allocates %v times per call when disabled; want 0", allocs)
	}
	// The removal hook is on the same footing: it fires for every expired key.
	if allocs := testing.AllocsPerRun(200, func() { s.notifyRemoved("some-key", false) }); allocs != 0 {
		t.Errorf("notifyRemoved allocates %v times per call when disabled; want 0", allocs)
	}

	// And nothing is delivered: a session subscribed to everything hears nothing.
	sess := s.newSession(nil)
	sub := sess.subscriberOf()
	s.addSubscription(s.patterns, "*", sess, sub.patterns)
	w := resp.NewWriter(io.Discard)
	s.dispatch(w, cmdArgs("SET", "k", "v"))
	s.dispatch(w, cmdArgs("DEL", "k"))
	if got := len(sub.ch); got != 0 {
		t.Errorf("%d notifications were delivered with the feature disabled", got)
	}

	// Enabling it makes the same write deliver, which is what proves the check above
	// was measuring a disabled path rather than a broken one.
	s.SetNotifyKeyspaceEvents("KEA")
	s.dispatch(w, cmdArgs("SET", "k", "v"))
	if got := len(sub.ch); got == 0 {
		t.Error("no notification was delivered after enabling the feature")
	}
}
