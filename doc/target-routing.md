# Target-aware connection failover

A node can pass the Google latency probe while refusing another destination.
Global probe health therefore remains separate from passive target failures.

- New routes rank exact **platform + node + host:port** response evidence first:
  two recent responses outrank one, followed by domain latency, authority probes,
  unknown nodes, and unrecovered failure history. Within an actual-response tier,
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
  time, even when Read starts before Write. For TLS-looking requests, a bounded
  six-byte prefix must identify a ServerHello before a positive sample is written;
  alerts, wrong protocols, and truncated replies do not create positive evidence.
  This observes TLS framing, not certificate validity or full handshake completion.
- Health success is recorded at the first qualifying response, not when an old
  long-lived tunnel closes. Initial failure feedback distinguishes upstream IO
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

Subscriptions, platform filters, and account identity rules are unchanged.
