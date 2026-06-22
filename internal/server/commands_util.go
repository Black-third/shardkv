package server

import (
	"strconv"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

// writeStoreErr translates a store error into the matching RESP error reply.
func writeStoreErr(w *resp.Writer, err error) {
	switch err {
	case store.ErrWrongType:
		w.WriteError(err.Error())
	case store.ErrNotInteger:
		w.WriteError("ERR value is not an integer or out of range")
	case store.ErrOverflow:
		w.WriteError("ERR increment or decrement would overflow")
	default:
		w.WriteError("ERR " + err.Error())
	}
}

func parseInt(b []byte) (int, bool) {
	n, err := strconv.Atoi(string(b))
	return n, err == nil
}

func parseFloat(b []byte) (float64, bool) {
	f, err := strconv.ParseFloat(string(b), 64)
	return f, err == nil
}

// formatFloat renders a score the way Redis does: shortest round-trippable form.
func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'g', -1, 64)
}
