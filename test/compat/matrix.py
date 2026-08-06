#!/usr/bin/env python3
"""Render the library x feature x pass/fail matrix the compat run produces.

Each suite writes one TSV per target (shardkv, redis) with three columns:
feature, status, detail. The interesting cell is not either column on its own but
the pair: a check that fails against shardkv *and* against a real Redis is a bug
in the check or a quirk of the library, while one that fails only against shardkv
is an incompatibility. Only the second kind sets the exit status, so a library
that cannot do something anywhere does not turn the suite red forever.
"""

from __future__ import annotations

import os
import sys

# ANSI, but only when stdout is a terminal: the CI log is read as plain text.
if sys.stdout.isatty():
    RED, GREEN, YELLOW, DIM, OFF = "\033[31m", "\033[32m", "\033[33m", "\033[2m", "\033[0m"
else:
    RED = GREEN = YELLOW = DIM = OFF = ""


def load(path: str) -> dict[str, tuple[str, str]]:
    rows: dict[str, tuple[str, str]] = {}
    if not os.path.exists(path):
        return rows
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            parts = line.rstrip("\n").split("\t")
            if len(parts) >= 2:
                rows[parts[0]] = (parts[1], parts[2] if len(parts) > 2 else "")
    return rows


def main() -> int:
    results_dir, suites = sys.argv[1], sys.argv[2:]
    incompatible: list[tuple[str, str, str]] = []
    both_fail: list[tuple[str, str, str]] = []
    totals = {"pass": 0, "fail": 0, "skip": 0}

    empty: list[str] = []
    for suite in suites:
        mine = load(os.path.join(results_dir, f"{suite}.shardkv.tsv"))
        theirs = load(os.path.join(results_dir, f"{suite}.redis.tsv"))
        if not mine:
            # A suite that produced nothing is a failure, not a pass. A client
            # image that did not build, or a library that could not connect at
            # all, prints no result lines -- and silence must never be mistaken
            # for success.
            print(f"\n{RED}{suite}: NO RESULTS (the suite did not run){OFF}")
            empty.append(suite)
            continue

        print(f"\n{suite}")
        print(f"  {'feature':<34} {'shardkv':<8} {'redis 7':<8} verdict")
        print(f"  {'-' * 34} {'-' * 8} {'-' * 8} {'-' * 30}")
        for feature in sorted(set(mine) | set(theirs)):
            ours, detail = mine.get(feature, ("MISSING", ""))
            ref, ref_detail = theirs.get(feature, ("MISSING", ""))
            totals[ours.lower()] = totals.get(ours.lower(), 0) + 1

            if ours == "PASS":
                verdict, colour = "ok", GREEN
            elif ours == "SKIP":
                verdict, colour = "skipped", DIM
            elif ref == "PASS":
                verdict, colour = "INCOMPATIBLE", RED
                incompatible.append((suite, feature, detail))
            elif ref in ("FAIL", "SKIP"):
                verdict, colour = "real Redis too", YELLOW
                both_fail.append((suite, feature, f"{detail} | redis: {ref_detail}"))
            else:
                verdict, colour = "no reference", YELLOW
                both_fail.append((suite, feature, detail))
            print(f"  {colour}{feature:<34} {ours:<8} {ref:<8} {verdict}{OFF}")

    print()
    if empty:
        print(f"{RED}suites that produced no results at all: {', '.join(empty)}{OFF}")
    if incompatible:
        print(f"{RED}incompatibilities (pass on real Redis, fail here){OFF}")
        for suite, feature, detail in incompatible:
            print(f"  {suite}/{feature}: {detail}")
    if both_fail:
        print(f"{YELLOW}checks that also fail on real Redis (not shardkv's fault){OFF}")
        for suite, feature, detail in both_fail:
            print(f"  {suite}/{feature}: {detail}")
    print(
        f"\n{sum(totals.values())} checks against shardkv: "
        f"{totals.get('pass', 0)} pass, {totals.get('fail', 0)} fail, "
        f"{totals.get('skip', 0)} skipped, {len(incompatible)} incompatible"
    )
    return 1 if (incompatible or empty) else 0


if __name__ == "__main__":
    sys.exit(main())
