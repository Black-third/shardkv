# Security policy

## Supported versions

shardkv is pre-1.0, so there is exactly one supported version: **the latest release**.
Fixes land on `main` and go out in the next tag; there are no maintenance branches and no
backports to older tags.

| Version | Supported |
| --- | --- |
| latest release (0.3.x) | yes |
| any earlier tag | no — upgrade |
| `main` | yes, and it is where a fix appears first |

## Reporting a vulnerability

**Please do not open a public issue for a security problem.**

Preferred: **GitHub private security advisories**, at
<https://github.com/Black-third/shardkv/security/advisories/new> (also reachable from the
repository's *Security* tab → *Report a vulnerability*). It keeps the report, the
discussion and the eventual advisory and CVE in one place, and it lets a fix be prepared
before anything is public.

If that is not available to you, email **housamzaid2006@gmail.com** with `shardkv
security` in the subject.

What helps most, roughly in order:

- the exact bytes or command sequence that triggers it — a raw RESP payload,
  `printf ... | nc`, a short Go or Python reproducer;
- the version (`redis-cli -p 6380 INFO server`, or the tag/commit) and the flags the
  server was started with;
- what an attacker gets out of it: a panic, a hang, unbounded memory, data another client
  should not see, a write that outlives what the protocol said, a master and replica that
  end up disagreeing;
- whether it needs `AUTH` to have succeeded first, and whether it needs cluster mode,
  replication or an AOF to be in use.

### What to expect

This is a single-maintainer project, not a vendor with an on-call rota. The commitments
are therefore modest and meant to be kept:

- **Acknowledgement within 3 business days.** If you have heard nothing in a week, assume
  the mail was lost and send it again.
- **An assessment within 7 days** of acknowledgement: whether it is in scope (see below),
  how severe it looks, and what the fix is likely to be.
- **A fix in the next release** for anything in scope and confirmed, with a private
  advisory published when it ships.
- Credit in the advisory and the release notes unless you would rather not be named.
- No bug bounty. Nothing to offer beyond the credit and the thanks.

Please give a fix a reasonable window before publishing — 90 days is the usual courtesy,
and less is fine by agreement if a fix is already out.

## Scope

Everything below is a real security boundary in this codebase, and a finding in any of
them is in scope.

**The RESP parser** (`internal/resp`) is the only code that touches bytes from an
unauthenticated peer, which makes it the front line. In scope: a panic or an
out-of-bounds on malformed input; a length prefix that causes an allocation
disproportionate to what was actually sent; an inline command or a nested aggregate that
makes the reader loop or recurse without bound; any way past the `MaxMultiBulk` /
bulk-length limits. There is a fuzz target (`FuzzReadCommand`) — a crash it finds is
exactly a report worth sending, and the reproducing corpus entry is the ideal
attachment.

**Authentication and TLS** (`internal/server/auth.go`, the `-requirepass`, `-masterauth`
and `-tls-*` flags). In scope: any command that executes before `AUTH` succeeds that is
not meant to; `HELLO ... AUTH` or `RESET` leaving a pooled connection in the wrong state;
`PSYNC` or a `CLUSTER` subcommand served to an unauthenticated peer; certificate
verification that does not actually verify; a password reaching a place it should not.
That last one includes `MONITOR`, which echoes what clients send: `redactedArgs` is the
single place that decides what a monitor may see, and it must keep covering `AUTH`,
`HELLO ... AUTH` and `CONFIG SET requirepass|masterauth`. A credential visible in a
`MONITOR` stream, a slow-log entry or a log line is a vulnerability, not a cosmetic bug.

**AOF and replication divergence.** The failure this project fears most is not a crash
but a master, its replicas and its AOF quietly disagreeing. In scope: any input sequence
after which a replica's dataset differs from its master's, or a replay of the AOF
produces a different dataset than the process that wrote it, or an acknowledged write is
absent from both. The thirteen invariants in [CLAUDE.md](CLAUDE.md#the-invariants-that-matter)
are the specification here; a demonstrated violation of one of them is a report even if
you cannot name an attacker who benefits, because the same defect under an adversary is
data loss on demand.

`DUMP`/`RESTORE` is part of this surface and is deliberately narrow: a payload's body is
a **whitelist** of the nine commands `DumpKey` emits, each retargeted at the key
`RESTORE` named, rebuilt in a private scratch store and published only when complete. A
payload that executes anything outside that whitelist, escapes the target key, or leaves
a partial value behind is in scope. (A valid CRC-64 proves the bytes are intact, never
that they are benign — that is what the whitelist is for.)

**Cluster redirect correctness** (`internal/server/cluster_redirect.go`,
`cluster_slots.go`). A node that answers for a slot it does not own does not produce an
error, it produces a second copy of the data on a node no client will consult again. In
scope: a key served by a node that should have answered `MOVED`; an `ASKING` flag that
outlives the single command it is granted for; a hash-tag or `CROSSSLOT` case where the
slot computed for routing differs from the slot the key actually belongs to; a
`MULTI`/`EXEC` batch that runs partially across a redirect boundary.

Also in scope, generally: remote crashes and unbounded resource use reachable from a
client connection (a single command that allocates without limit, a blocking command that
can be made to hold a lock, a `MULTI` that never releases one), and anything that lets one
connection observe or corrupt another connection's state.

## Not in scope

These are documented design decisions, not oversights, and reporting them will get you a
link back to this section:

- **No authentication by default.** As with Redis, `-requirepass` is opt-in and the
  server binds where it is told to. Exposing an unauthenticated instance to a hostile
  network is a deployment choice; the [README's authentication
  section](README.md#authentication-and-tls) says so.
- **An authenticated client is fully trusted.** There are no ACLs and no per-command
  permissions. Anyone who can `AUTH` can `FLUSHALL`, `CONFIG SET`, `DEBUG SLEEP`,
  `SHUTDOWN` and `MONITOR`. "An authenticated client deleted everything" is the design.
- **No cluster bus, therefore no failure detection and no automatic failover.** Two nodes
  that are told different things about a slot will disagree — *visibly*, one redirecting
  to the other — until an operator fixes it. The boundary is stated in full in the
  [README's cluster section](README.md#cluster). A report that a killed master stays dead
  is not a vulnerability.
- **`DUMP` payloads are not interchangeable with Redis's RDB object encoding**, by
  design; that they are mutually rejected is the feature.
- **Denial of service by legitimate volume** — a client that stores more than fits, opens
  as many connections as the OS allows, or runs `KEYS *` on a huge keyspace. Use
  `-maxkeys`, and do not hand the port to strangers.
- Findings from a scanner with no demonstrated impact on this codebase, and anything
  about the GitHub repository configuration rather than the software.
