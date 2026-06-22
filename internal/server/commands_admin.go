package server

import (
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
)

// version is reported by INFO.
const version = "0.2.0"

func init() {
	register("PING", -1, false, cmdPing)
	register("INFO", -1, false, cmdInfo)
	register("DBSIZE", 1, false, cmdDBSize)
	register("FLUSHALL", 1, true, cmdFlushAll)
	register("COMMAND", -1, false, cmdCommand)
	register("REPLICAOF", 3, false, cmdReplicaOf)
	register("SLAVEOF", 3, false, cmdReplicaOf) // legacy alias
}

func cmdPing(s *Server, w *resp.Writer, args [][]byte) bool {
	if len(args) > 1 {
		w.WriteBulk(args[1])
	} else {
		w.WriteSimple("PONG")
	}
	return false
}

func cmdInfo(s *Server, w *resp.Writer, args [][]byte) bool {
	s.mu.Lock()
	role := s.role
	master := s.masterAddr
	nReplicas := len(s.replicas)
	s.mu.Unlock()

	var b strings.Builder
	fmt.Fprintf(&b, "# Server\r\n")
	fmt.Fprintf(&b, "shardkv_version:%s\r\n", version)
	fmt.Fprintf(&b, "go_version:%s\r\n", runtime.Version())
	fmt.Fprintf(&b, "uptime_in_seconds:%d\r\n", int(time.Since(s.startTime).Seconds()))
	fmt.Fprintf(&b, "\r\n# Clients\r\n")
	fmt.Fprintf(&b, "total_connections_received:%d\r\n", s.totalConns.Load())
	fmt.Fprintf(&b, "\r\n# Stats\r\n")
	fmt.Fprintf(&b, "total_commands_processed:%d\r\n", s.totalCmds.Load())
	fmt.Fprintf(&b, "\r\n# Keyspace\r\n")
	fmt.Fprintf(&b, "db_keys:%d\r\n", s.store.Len())
	fmt.Fprintf(&b, "\r\n# Replication\r\n")
	fmt.Fprintf(&b, "role:%s\r\n", role)
	if role == "replica" {
		fmt.Fprintf(&b, "master_host:%s\r\n", master)
	}
	fmt.Fprintf(&b, "connected_slaves:%d\r\n", nReplicas)
	fmt.Fprintf(&b, "aof_enabled:%d\r\n", boolToInt(s.aof != nil))

	w.WriteBulk([]byte(b.String()))
	return false
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func cmdDBSize(s *Server, w *resp.Writer, args [][]byte) bool {
	w.WriteInt(int64(s.store.Len()))
	return false
}

func cmdFlushAll(s *Server, w *resp.Writer, args [][]byte) bool {
	s.store.FlushAll()
	w.WriteSimple("OK")
	return true
}

func cmdCommand(s *Server, w *resp.Writer, args [][]byte) bool {
	// redis-cli issues COMMAND DOCS on connect; an empty array is enough.
	w.WriteArrayHeader(0)
	return false
}

func cmdReplicaOf(s *Server, w *resp.Writer, args [][]byte) bool {
	host := string(args[1])
	port := string(args[2])
	if strings.EqualFold(host, "NO") && strings.EqualFold(port, "ONE") {
		s.ReplicaOf(s.baseCtx, "") // promote to master
	} else {
		s.ReplicaOf(s.baseCtx, host+":"+port)
	}
	w.WriteSimple("OK")
	return false
}
