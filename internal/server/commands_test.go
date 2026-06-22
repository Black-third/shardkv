package server

import (
	"bufio"
	"net"
	"testing"
)

func TestDataTypeCommands(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)

	send := func(cmd string) string {
		if _, err := conn.Write([]byte(cmd + "\r\n")); err != nil {
			t.Fatalf("write: %v", err)
		}
		return readReply(t, br)
	}

	cases := []struct{ cmd, want string }{
		// lists
		{"RPUSH mylist a b c", ":3"},
		{"LPUSH mylist z", ":4"},
		{"LRANGE mylist 0 -1", "[z a b c]"},
		{"LLEN mylist", ":4"},
		{"LPOP mylist", "z"},
		{"RPOP mylist", "c"},
		{"LRANGE mylist 0 -1", "[a b]"},
		// hashes
		{"HSET h f1 v1 f2 v2", ":2"},
		{"HSET h f1 v1b", ":0"},
		{"HGET h f1", "v1b"},
		{"HLEN h", ":2"},
		{"HDEL h f2 nope", ":1"},
		// sets
		{"SADD s a b a", ":2"},
		{"SCARD s", ":2"},
		{"SISMEMBER s b", ":1"},
		{"SISMEMBER s zzz", ":0"},
		{"SREM s a", ":1"},
		// sorted sets
		{"ZADD board 100 alice 200 bob 150 carol", ":3"},
		{"ZCARD board", ":3"},
		{"ZRANGE board 0 -1", "[alice carol bob]"},
		{"ZRANGE board 0 -1 WITHSCORES", "[alice 100 carol 150 bob 200]"},
		{"ZRANK board bob", ":2"},
		{"ZSCORE board carol", "150"},
		{"ZADD board 50 bob", ":0"}, // update existing
		{"ZRANK board bob", ":0"},   // now lowest
		{"ZREM board alice", ":1"},
		{"ZCARD board", ":2"},
		// type + wrongtype
		{"TYPE mylist", "+list"},
		{"TYPE board", "+zset"},
		{"SET str hello", "+OK"},
		{"LPUSH str x", "-WRONGTYPE Operation against a key holding the wrong kind of value"},
		{"INCR str", "-ERR value is not an integer or out of range"},
		// multi
		{"MSET k1 v1 k2 v2", "+OK"},
		{"MGET k1 k2 missing", "[v1 v2 (nil)]"},
	}
	for _, c := range cases {
		if got := send(c.cmd); got != c.want {
			t.Errorf("%q -> %q; want %q", c.cmd, got, c.want)
		}
	}
}

func TestInfoCommand(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	conn, _ := net.Dial("tcp", addr)
	defer conn.Close()
	br := bufio.NewReader(conn)

	conn.Write([]byte("INFO\r\n"))
	reply := readReply(t, br)
	for _, want := range []string{"role:master", "shardkv_version", "connected_slaves:0"} {
		if !contains(reply, want) {
			t.Errorf("INFO missing %q; got:\n%s", want, reply)
		}
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
