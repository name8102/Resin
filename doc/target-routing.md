# Target-aware connection failover

A node can pass the Google latency probe while refusing another destination.
Global probe health therefore remains separate from passive target failures.

- The first failed active probe or attributable initial upstream failure
  blacklists the **whole node**, across origins, for one hour. Repeat admissions
  increase quarantine to six hours, then 24 hours. Concurrent failures during
  the same quarantine do not continuously extend it. The old configurable
  consecutive-failure threshold is removed rather than bypassing this policy.
- Blacklist expiry alone does not make a node eligible. Only a successful active
  probe after expiry clears its marker; successes from probes before expiry or
  from old user connections cannot clear it. Periodic/queued probes skip nodes
  while quarantined and schedule verification on expiry. No routing fallback
  bypasses the blacklist, including sticky leases or stale platform views.
- Blacklist deadlines and strike counts are persisted with node dynamic state
  and exposed as `blacklist_until` / `blacklist_strikes` in node summaries. Cache
  migration quarantines existing nodes with nonzero recorded failures for one
  hour. Healthy-node aggregates exclude blacklist markers even after expiry.
- A TLS ServerHello alone is not positive route evidence. A subsequent client
  flight followed by valid upstream TLS-record payload is required; framing is
  observed without decryption, payload retention, or certificate verification.
  The initial TLS exchange has a five-second read deadline, cleared immediately
  after this bidirectional progress. EOF after ServerHello and stalled handshakes
  now feed node-wide blacklisting instead of merely generating a fast latency.
- New routes rank exact **platform + node + host:port** response evidence first:
  followed by platform-wide real responses, domain latency, authority probes,
  unknown nodes, and unrecovered failure history. Successful sample count does
  not create a hard rank: candidates use independently decaying response
  reliability, first-response latency and active-download rate. Within an actual-response tier,
  the three best policy scores share new routes randomly. Existing account leases
  remain sticky while routable; sibling origins do not inherit exact success.
- Initial upstream failures, including TLS requests ending without a response,
  cool down that exact origin for 5, 10, 20, then at most 30 minutes. After the
  cooldown, old low-latency samples do not immediately restore priority: failure
  history remains lower-ranked for another 30 minutes until a new valid response.
  Neither Google probes nor older connections can clear a newer target failure.
- Response/failure history shares one in-memory cache capped at 8,192 entries.
  Passive-breaker-disabled platforms ignore penalties; request exclusions remain.
- Probe latency includes proxy setup through TLS completion (first response byte
  for plain HTTP). Passive tunnel timing includes dial plus first-write-to-response
  time, even when Read starts before Write. For TLS-looking requests, positive samples wait for
  the bidirectional TLS progress described above; alerts, wrong protocols, and
  incomplete handshakes do not create positive evidence.
- Health success is recorded at the first qualifying response, not when an old
  long-lived tunnel closes. Initial failure feedback is synchronous and distinguishes upstream IO
  from client cancellation; idle/server-push-only sessions and local hijack errors
  do not reset or increment node-health counters. Established connection shutdowns
  do not masquerade as initial TLS failures.
- Passive traffic updates activity and latency independently of active-probe
  attempt timestamps. Nodes active within the latency window are re-probed at
  half that window (minimum one minute, bounded by the configured probe interval).
  Cold nodes keep their configured probe cadence and all probes use existing
  worker limits; failed user traffic cannot defer active verification indefinitely.
- Anonymous CONNECT/SOCKS5 connections hedge at most eight distinct nodes,
  starting backups 200 ms apart within a shared 4.3-second dial budget. The
  first successful connection wins; canceled or late candidates are closed.
- Account-bound tunnels retain sequential failover and same-egress-IP priority,
  with 1.4 seconds per attempt. Both paths fit Mihomo's five-second SOCKS dial
  budget, and a caller's earlier deadline wins.
- Plain HTTP can also fail over before obtaining an upstream connection, but
  body-bearing requests are never replayed. Once a connection is acquired, no
  cross-node replay occurs, even for requests without a body.
- HTTP upstream CONNECT handshakes honor the dial context through their final
  response, not just the initial TCP connection; successful tunnels have their
  temporary deadline and cancellation hook removed.
- Client cancellation and local CONNECT response/hijack errors do not create
  target cooldowns. Established tunnels are never replayed or cut off by a dial
  timeout.
- Per-request node exclusions still prevent repeated attempts when passive
  circuit breaking is disabled. Existing global health feedback continues for
  each failed attempt; the request log identifies the final selected node.

## Scoring and exploration

- Successful responses and attributable initial upstream failures retain separate,
  exponentially decaying evidence. Recovery does not erase historical failures;
  with no failures, collecting more successes gives no reliability bonus.
- Exact-origin failure only changes that origin's evidence. Node-wide quarantine
  remains independently enforced by the existing probe/health pipeline.
- First-response timing comes from HTTP transport traces or qualified tunnel TLS
  progress. Target download rates use real windows of at least 64 KiB / 100 ms,
  discard gaps over two seconds, and never treat total connection lifetime as
  download time. A missing metric remains unknown. HTTP bodies have per-request
  samples; shared HTTP connections retain their separate node-wide estimates.
- Every 32 initial route requests earn one exploration allocation, with at most
  two saved allocations per platform and exact origin. Retries neither earn nor
  spend credit. Exploration only selects ranked healthy candidates passing all
  filters. Account leases remain sticky; no live connection is migrated or replayed.
- These observations share the bounded in-memory target cache. Request-log restore
  seeds response reliability, not latency or throughput. Scoring needs no extra
  active speed tests or tuning parameters.

## Domain-specific exit regions

Platforms accept `domain_region_rules` on creation and PATCH:

```json
{
  "domain_region_rules": [
    { "domain": "chatgpt.com", "regions": ["us", "jp"] },
    { "domain": "api.chatgpt.com", "regions": ["us"] }
  ]
}
```

A bare domain includes itself and all subdomains, with DNS label boundaries.
Patterns support `*` anywhere in ASCII labels; a star matches zero or more
characters, including dots. `*.example.com` matches nested subdomains but not
`example.com`. The rule with the most non-star characters wins; ties prefer
fewer stars, then configured order. Bare domains retain their subdomain behavior
when competing with a glob. Literal labels normalize to lowercase ASCII/IDNA;
a final dot is ignored. URLs, ports, IP literals, `?`, empty region lists and
duplicate normalized patterns are rejected. Regions use lowercase two-letter
codes: `jp` includes Japan, `!jp,!kr` excludes Japan and South Korea. Any nonempty
region filter also excludes unknown exit regions.

Rules intersect the platform's existing routable view. Exit probe metadata takes
precedence over GeoIP of the exit IP; unknown regions do not satisfy a rule.
Anonymous routes, account-lease reuse, same-IP rotation and request retries all
respect the selected rule. With no matching node, the request fails instead of
falling back to another region. An account retains one lease per platform: a
request requiring a different region replaces an incompatible lease.

In platform creation/editing, enter one rule per line:

```text
chatgpt.com = us,jp
api.chatgpt.com = us
*.example.com = !jp,!kr
```

PATCH `{"domain_region_rules": []}` clears all rules. Rules persist in state.db
and are restored on restart. Existing platforms start with no domain rules;
subscription selection and account-identity rules remain unchanged.

`RESIN_DEFAULT_PLATFORM_DOMAIN_REGION_RULES` accepts the same rules as a JSON
array. Together with `RESIN_DEFAULT_PLATFORM_REGION_FILTERS`, it seeds a new
built-in Default platform and supplies create/reset defaults. Existing persisted
platforms are not overwritten on startup; apply a partial PATCH to update them.
For example, a platform-wide `["!cn"]` excludes China and unknown regions, while
these domain rules further restrict the requested sites (including subdomains):

```json
[
  {"domain":"dlsite.com","regions":["jp"]},
  {"domain":"e-hentai.org","regions":["!jp","!kr"]},
  {"domain":"exhentai.org","regions":["!jp","!kr"]},
  {"domain":"esjzone.cc","regions":["!jp","!kr"]}
]
```
