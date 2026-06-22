package store

import "strconv"

// Dump returns a sequence of write commands that, when replayed against an empty
// store, reconstruct the current dataset. It is used to seed a replica and to
// compact the append-only file.
//
// TTLs are not currently captured in the snapshot; replayed keys are persistent.
// (Live expirations still propagate as DEL via the command stream.)
func (s *Store) Dump() [][][]byte {
	now := s.clock()
	var cmds [][][]byte

	for _, sh := range s.shards {
		sh.mu.RLock()
		for k, e := range sh.data {
			if e.expired(now) {
				continue
			}
			key := []byte(k)
			switch e.kind {
			case kindString:
				cmds = append(cmds, [][]byte{[]byte("SET"), key, copyBytes(e.str)})

			case kindList:
				cmd := [][]byte{[]byte("RPUSH"), key}
				for el := e.list.l.Front(); el != nil; el = el.Next() {
					cmd = append(cmd, copyBytes(el.Value.([]byte)))
				}
				cmds = append(cmds, cmd)

			case kindHash:
				cmd := [][]byte{[]byte("HSET"), key}
				for f, v := range e.dict {
					cmd = append(cmd, []byte(f), copyBytes(v))
				}
				cmds = append(cmds, cmd)

			case kindSet:
				cmd := [][]byte{[]byte("SADD"), key}
				for m := range e.set {
					cmd = append(cmd, []byte(m))
				}
				cmds = append(cmds, cmd)

			case kindZSet:
				cmd := [][]byte{[]byte("ZADD"), key}
				node := e.zset.sl.head.next[0].forward
				for node != nil {
					score := strconv.FormatFloat(node.score, 'g', -1, 64)
					cmd = append(cmd, []byte(score), []byte(node.member))
					node = node.next[0].forward
				}
				cmds = append(cmds, cmd)
			}
		}
		sh.mu.RUnlock()
	}
	return cmds
}
