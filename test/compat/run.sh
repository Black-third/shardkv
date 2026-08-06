#!/usr/bin/env bash
#
# Drive shardkv with the client libraries real applications use.
#
# The point of this suite is that it is *not* redis-cli. redis-cli is forgiving:
# it prints whatever comes back. A client library parses the handshake, validates
# reply shapes against the protocol version it negotiated, matches error text to
# decide which exception to raise, and -- in cluster mode -- builds a routing table
# out of CLUSTER SLOTS and follows MOVED/ASK on its own. Those are the parts of
# wire compatibility that a hand-driven session never touches.
#
# Every suite runs twice: once against shardkv and once against a real
# redis:7-alpine, under the same code. A failure that also fails against real
# Redis is a test bug or a library quirk; a failure that passes against real Redis
# is an incompatibility in shardkv. The matrix prints both columns so the
# difference is the finding, not the raw pass count.
#
#   ./test/compat/run.sh                 # everything
#   ./test/compat/run.sh python node     # only those suites
#   KEEP=1 ./test/compat/run.sh python   # leave the containers up afterwards
#
# Requires only docker. The server is cross-compiled on the host and bind-mounted
# into an alpine container, so no image build is needed for shardkv itself.

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../.." && pwd)"
out="$here/.bin"
results="$here/.results"

NET=${NET:-shardkv-compat}
PREFIX=${PREFIX:-skcompat}
KEEP=${KEEP:-0}
SHARDKV_IMAGE=${SHARDKV_IMAGE:-alpine:3.21}
REDIS_IMAGE=${REDIS_IMAGE:-redis:7-alpine}

suites=("$@")
if [ ${#suites[@]} -eq 0 ]; then
	suites=(python ioredis noderedis goredis)
fi

# Targets: the name a suite connects to, and the label it is reported under. Both
# servers listen on 6380 inside the network so the suites need no per-target
# configuration beyond the host name.
targets=(shardkv redis)
if [ -n "${ONLY_TARGET:-}" ]; then
	targets=("$ONLY_TARGET")
fi

# Only the client-library suites have a cluster client to exercise; bringing up
# six more containers (and waiting on real Redis's gossip to settle) for a run
# that will not touch them is pure cost.
needs_cluster=0
for suite in "${suites[@]}"; do
	case "$suite" in
	tcl) ;;
	*) needs_cluster=1 ;;
	esac
done
if [ "$needs_cluster" = 0 ]; then
	SKIP_CLUSTER=1
fi

log() { printf '\033[36m==>\033[0m %s\n' "$*" >&2; }
warn() { printf '\033[33m!!\033[0m %s\n' "$*" >&2; }

# ---------------------------------------------------------------------------
# Teardown. Only containers this script created (they all carry $PREFIX-) are
# touched: the host may well be running other work.
# ---------------------------------------------------------------------------
cleanup() {
	if [ "$KEEP" = "1" ]; then
		warn "KEEP=1: leaving containers and network up"
		return
	fi
	local ids
	ids=$(docker ps -aq --filter "name=^${PREFIX}-" 2>/dev/null || true)
	if [ -n "$ids" ]; then
		docker rm -f $ids >/dev/null 2>&1 || true
	fi
	docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Refuse to start on top of another run. Two runs sharing $PREFIX destroy each
# other silently: the first one's teardown removes the second one's servers, and
# what the second one then reports is not a server that failed but a server that
# is no longer there -- "couldn't open socket: Name does not resolve" recorded as
# a compatibility result. The check is cheap and the failure it prevents is one
# that looks exactly like a real finding.
if [ -n "$(docker ps -aq --filter "name=^${PREFIX}-" 2>/dev/null)" ]; then
	warn "containers named ${PREFIX}-* already exist: another run is in flight, or"
	warn "a previous one was killed before its teardown. Remove them, or set PREFIX"
	warn "to something else to run alongside it:"
	docker ps -a --filter "name=^${PREFIX}-" --format '  {{.Names}}\t{{.Status}}' >&2
	# Do not tear them down here: the other run is using them.
	trap - EXIT
	exit 1
fi

# ---------------------------------------------------------------------------
# Build the server for the container architecture. The suites run linux
# containers whatever the host is, so the arch comes from docker rather than from
# the host toolchain.
# ---------------------------------------------------------------------------
docker_arch() {
	case "$(docker info --format '{{.Architecture}}' 2>/dev/null)" in
	aarch64 | arm64) echo arm64 ;;
	x86_64 | amd64) echo amd64 ;;
	*) go env GOARCH ;;
	esac
}

mkdir -p "$out" "$results"
# Clear only the suites this run will rewrite. Clearing the whole directory would
# destroy a concurrent run's results -- and a concurrent run is explicitly
# supported, since $PREFIX exists to allow one -- leaving the other run's matrix
# silently short of the files it had already finished.
for suite in "${suites[@]}"; do
	rm -f "$results/$suite".*.tsv
done
if [ "${NOBUILD:-0}" = "1" ] && [ -x "$out/shardkv" ]; then
	warn "NOBUILD=1: reusing the binary already in $out"
else
	log "building shardkv for linux/$(docker_arch)"
	(cd "$root" && CGO_ENABLED=0 GOOS=linux GOARCH="$(docker_arch)" \
		go build -trimpath -o "$out/shardkv" ./cmd/shardkv)
fi

docker network create "$NET" >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------
# Servers.
# ---------------------------------------------------------------------------

# start_shardkv <name> [extra server flags...]
start_shardkv() {
	local name="$1"
	shift
	docker rm -f "$name" >/dev/null 2>&1 || true
	docker run -d --name "$name" --network "$NET" \
		-v "$out:/w:ro" \
		"$SHARDKV_IMAGE" /w/shardkv -addr :6380 "$@" >/dev/null
}

start_redis() {
	local name="$1"
	shift
	docker rm -f "$name" >/dev/null 2>&1 || true
	# enable-debug-command is off by default in Redis 7 and Redis's own test suite
	# uses DEBUG throughout (DEBUG OBJECT, DEBUG JMAP, DEBUG SET-ACTIVE-EXPIRE).
	# Without it the reference column would record "real Redis fails too" for
	# every file that touches DEBUG, which would be an artefact of the reference's
	# configuration rather than a fact about either server.
	docker run -d --name "$name" --network "$NET" "$REDIS_IMAGE" \
		redis-server --port 6380 --notify-keyspace-events KEA \
		--enable-debug-command yes "$@" >/dev/null
}

# cli <host> <args...> -- a throwaway redis-cli on the compat network.
cli() {
	local host="$1"
	shift
	docker run --rm --network "$NET" "$REDIS_IMAGE" \
		redis-cli -h "$host" -p 6380 "$@"
}

container_ip() {
	docker inspect -f \
		'{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$1" 2>/dev/null
}

wait_up() {
	local host="$1" i
	for i in $(seq 1 60); do
		if [ "$(cli "$host" PING 2>/dev/null | tr -d '\r')" = "PONG" ]; then
			return 0
		fi
		sleep 0.5
	done
	warn "$host never answered PING"
	docker logs "$host" 2>&1 | tail -20 >&2 || true
	return 1
}

# Bring up a three-node cluster. There is no cluster bus in shardkv, so slots are
# assigned by hand and MEET is run in both directions for every pair -- see the
# README's Cluster section. Real Redis gossips, but accepts exactly the same
# commands, so the same code brings both up and the clients cannot tell which
# they are talking to from the setup alone.
start_cluster() {
	local kind="$1" n1="$2" n2="$3" n3="$4"
	local nodes=("$n1" "$n2" "$n3")
	local node
	for node in "${nodes[@]}"; do
		docker rm -f "$node" >/dev/null 2>&1 || true
		if [ "$kind" = shardkv ]; then
			docker run -d --name "$node" --network "$NET" -v "$out:/w:ro" \
				"$SHARDKV_IMAGE" /w/shardkv -addr :6380 \
				-cluster-enabled -cluster-config-file "/tmp/$node.conf" \
				-cluster-announce-ip "$node" >/dev/null
		else
			# No --cluster-announce-ip for real Redis: it wants an address there,
			# not a name, and its own container IP is routable on this network
			# anyway. shardkv needs the flag because it otherwise announces
			# 127.0.0.1, which is the client's own loopback, not the node.
			docker run -d --name "$node" --network "$NET" "$REDIS_IMAGE" \
				redis-server --port 6380 --cluster-enabled yes \
				--cluster-config-file "/tmp/$node.conf" >/dev/null
		fi
	done
	for node in "${nodes[@]}"; do wait_up "$node"; done

	cli "$n1" CLUSTER ADDSLOTSRANGE 0 5460 >/dev/null
	cli "$n2" CLUSTER ADDSLOTSRANGE 5461 10922 >/dev/null
	cli "$n3" CLUSTER ADDSLOTSRANGE 10923 16383 >/dev/null

	# MEET is dialled by the *node*, so it gets an address rather than a service
	# name: real Redis wants an IP there. shardkv records whatever the peer
	# announces regardless of how it was reached, so meeting by IP still leaves
	# the announce name in the slot map that clients read.
	local a b
	for a in "${nodes[@]}"; do
		for b in "${nodes[@]}"; do
			[ "$a" = "$b" ] && continue
			cli "$a" CLUSTER MEET "$(container_ip "$b")" 6380 >/dev/null || true
		done
	done

	# Wait for every node to agree the slot space is covered. Real Redis needs the
	# gossip round to settle; shardkv is told directly and so is ready at once.
	local i ok
	for i in $(seq 1 60); do
		ok=1
		for node in "${nodes[@]}"; do
			cli "$node" CLUSTER INFO 2>/dev/null | grep -q 'cluster_state:ok' || ok=0
		done
		[ "$ok" = 1 ] && return 0
		sleep 1
	done
	warn "$kind cluster never reached cluster_state:ok"
	cli "$n1" CLUSTER NODES >&2 || true
	return 1
}

# ---------------------------------------------------------------------------
# Suites.
# ---------------------------------------------------------------------------

# A build that fails must be loud. A suite whose image never built prints no
# result lines, and "no result lines" is indistinguishable from "everything
# passed" unless something says so -- which is why matrix.py treats an empty
# suite as a failure and why this records the reason.
build_suite() {
	local suite="$1"
	log "building the $suite client image"
	if ! docker build -q -t "$PREFIX-$suite" "$here/$suite" >"$results/$suite.build.log" 2>&1; then
		warn "the $suite image did not build; last 30 lines:"
		tail -30 "$results/$suite.build.log" >&2
		return 1
	fi
}

# run_suite <suite> <target-label> <standalone-host> <cluster-seed-host>
run_suite() {
	local suite="$1" target="$2" host="$3" seed="$4"
	local file="$results/$suite.$target.tsv"
	log "running $suite against $target"
	if docker run --rm --network "$NET" \
		-e "SHARDKV_HOST=$host" -e SHARDKV_PORT=6380 \
		-e "CLUSTER_HOST=$seed" -e CLUSTER_PORT=6380 \
		-e "TARGET=$target" \
		-e "TCL_FILES=${TCL_FILES:-}" \
		"$PREFIX-$suite" >"$results/$suite.$target.log" 2>&1; then
		:
	fi
	grep -a '^::RESULT::' "$results/$suite.$target.log" |
		sed 's/^::RESULT:://' |
		awk -F'::' -v OFS='\t' '{print $1, $2, $3}' >"$file" || true
	if [ ! -s "$file" ]; then
		warn "$suite/$target produced no results; last 40 lines:"
		tail -40 "$results/$suite.$target.log" >&2
	fi
}

# ---------------------------------------------------------------------------
# Go.
# ---------------------------------------------------------------------------

log "starting servers"
for target in "${targets[@]}"; do
	if [ "$target" = shardkv ]; then
		start_shardkv "$PREFIX-shardkv" -notify-keyspace-events KEA
	else
		start_redis "$PREFIX-redis"
	fi
	wait_up "$PREFIX-$target"
done

if [ "${SKIP_CLUSTER:-0}" != "1" ]; then
	for target in "${targets[@]}"; do
		log "starting the $target cluster"
		start_cluster "$target" \
			"$PREFIX-$target-c1" "$PREFIX-$target-c2" "$PREFIX-$target-c3" ||
			warn "$target cluster unavailable; its cluster checks will fail"
	done
fi

for suite in "${suites[@]}"; do
	if ! build_suite "$suite"; then
		printf 'image_build\tFAIL\tthe %s client image did not build\n' "$suite" \
			>"$results/$suite.shardkv.tsv"
		continue
	fi
	for target in "${targets[@]}"; do
		run_suite "$suite" "$target" "$PREFIX-$target" "$PREFIX-$target-c1"
	done
done

# ---------------------------------------------------------------------------
# The matrix.
# ---------------------------------------------------------------------------
log "matrix"
python3 "$here/matrix.py" "$results" "${suites[@]}"
status=$?
exit $status
