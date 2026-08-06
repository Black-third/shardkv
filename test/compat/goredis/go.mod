// A module of its own, deliberately: it keeps go-redis out of shardkv's own
// go.mod, which must stay dependency-free. A directory containing its own go.mod
// is excluded from the parent module's ./... patterns, so `go build ./...` and
// `go vet ./...` at the repository root never see this file.
//
// No `require` line and no checked-in go.sum: the image build resolves go-redis
// at whatever version is current (`go get ...@latest`) and the binary prints the
// version it linked against, so the matrix records what was actually tested
// rather than what someone pinned a year ago.
module shardkv-compat-goredis

go 1.26
