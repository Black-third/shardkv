package server

import (
	"strconv"
	"strings"
	"time"
)

// propagationForm rewrites a write command into the form that should be
// persisted to the AOF and shipped to replicas. Relative-expiry commands are
// converted to absolute deadlines so a replica or an AOF replay reconstructs the
// same expiry instant regardless of when it applies the command (real Redis does
// the same: EXPIRE -> PEXPIREAT, SET EX -> SET PXAT). All other commands
// propagate verbatim.
func propagationForm(args [][]byte, now time.Time) [][]byte {
	nowMs := now.UnixMilli()
	switch strings.ToUpper(string(args[0])) {
	case "EXPIRE":
		if sec, ok := parseInt(args[2]); ok {
			return pexpireat(args[1], nowMs+int64(sec)*1000)
		}
	case "PEXPIRE":
		if ms, ok := parseInt(args[2]); ok {
			return pexpireat(args[1], nowMs+int64(ms))
		}
	case "EXPIREAT":
		if sec, ok := parseInt(args[2]); ok {
			return pexpireat(args[1], int64(sec)*1000)
		}
	case "SET":
		// SET key val EX sec | PX ms | PXAT ms-timestamp
		if len(args) == 5 {
			v, ok := parseInt(args[4])
			if !ok {
				return args
			}
			var atMs int64
			switch strings.ToUpper(string(args[3])) {
			case "EX":
				atMs = nowMs + int64(v)*1000
			case "PX":
				atMs = nowMs + int64(v)
			case "PXAT":
				atMs = int64(v)
			default:
				return args
			}
			return [][]byte{[]byte("SET"), args[1], args[2], []byte("PXAT"), []byte(strconv.FormatInt(atMs, 10))}
		}
	}
	return args
}

func pexpireat(key []byte, atMs int64) [][]byte {
	return [][]byte{[]byte("PEXPIREAT"), key, []byte(strconv.FormatInt(atMs, 10))}
}
