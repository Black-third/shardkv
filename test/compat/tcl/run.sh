#!/bin/bash
#
# Run a selection of Redis's own unit test files against an external server.
#
# One file per invocation, deliberately. The suite aborts the whole run on an
# exception, and a single unsupported command early in one file would otherwise
# cost the results of every file after it. Per-file runs turn that into one lost
# file and a recorded reason.
#
# Each file becomes one matrix cell whose detail carries the counts, and the same
# selection runs against a real Redis, so the comparison is per file: a file that
# fails on both is measuring something outside shardkv's claimed surface, and one
# that fails only here is a finding.

set -uo pipefail

HOST=${SHARDKV_HOST:-127.0.0.1}
PORT=${SHARDKV_PORT:-6380}
TARGET=${TARGET:-?}

# The files that exercise the surface shardkv implements. Deliberately not the
# whole suite: unit/scripting, unit/functions, unit/acl, unit/maxmemory,
# unit/tracking, unit/aofrw, unit/cluster and the integration directory test
# features this server does not claim (Lua, ACLs, byte-bounded eviction, client
# side caching, RDB, the cluster bus), so running them would report absence as
# failure. unit/sort is included precisely *because* SORT is missing: the gap
# should be visible in the numbers rather than hidden by the selection.
FILES=${TCL_FILES:-"
unit/type/string
unit/type/incr
unit/type/hash
unit/type/list
unit/type/set
unit/type/zset
unit/type/stream
unit/type/stream-cgroups
unit/expire
unit/keyspace
unit/scan
unit/bitops
unit/bitfield
unit/hyperloglog
unit/geo
unit/dump
unit/multi
unit/pubsub
unit/protocol
unit/quit
unit/slowlog
unit/latency-monitor
unit/sort
"}

result() {
	local feature="$1" status="$2" detail="$3"
	detail=$(printf '%s' "$detail" | tr -s '[:space:]' ' ' | cut -c1-220)
	printf '::RESULT::%s::%s::%s\n' "$feature" "$status" "$detail"
}

echo "# redis ${REDIS_VERSION:-?} test suite -> $TARGET at $HOST:$PORT"
result "suite_version" "PASS" "redis ${REDIS_VERSION:-?} tcl suite"

cd /opt/redis || {
	result "suite_available" "FAIL" "no /opt/redis"
	exit 0
}

for file in $FILES; do
	name="tcl:${file#unit/}"
	log=/tmp/$(echo "$file" | tr / _).log

	# --host/--port put the suite in external mode, which makes it skip the tests
	# tagged external:skip (anything needing to restart or reconfigure the server
	# it is testing). --singledb keeps it in database 0; --dont-clean leaves the
	# log where it can be read. Tags are denied with a leading "-" -- there is no
	# --skiptags in 7.4 -- and the four denied here need a second server, an RDB,
	# a byte-bounded maxmemory, or a restart, none of which external mode can do.
	timeout 600 tclsh tests/test_helper.tcl \
		--host "$HOST" --port "$PORT" \
		--singledb --dont-clean \
		--tags "-needs:repl -needs:save -needs:reset -needs:config-maxmemory" \
		--single "$file" >"$log" 2>&1
	status=$?

	ok=$(grep -c '\[ok\]' "$log")
	err=$(grep -c '\[err\]' "$log")
	exc=$(grep -c '\[exception\]' "$log")
	skipped=$(grep -c '\[skip\]' "$log")
	detail="ok=$ok err=$err exceptions=$exc skipped=$skipped"

	if [ "$status" -eq 124 ]; then
		result "$name" "FAIL" "timed out after 600s ($detail)"
	elif [ "$exc" -gt 0 ]; then
		# An exception is usually one unsupported command aborting the file, so
		# the first one is the useful part of the report.
		first=$(grep -A3 '\[exception\]' "$log" | head -6 | tr '\n' ' ')
		result "$name" "FAIL" "$detail :: $first"
	elif [ "$err" -gt 0 ]; then
		first=$(grep -A2 '\[err\]' "$log" | head -6 | tr '\n' ' ')
		result "$name" "FAIL" "$detail :: $first"
	elif [ "$ok" -eq 0 ]; then
		result "$name" "FAIL" "the file ran no assertions at all ($detail)"
	else
		result "$name" "PASS" "$detail"
	fi
done

echo "# done"
