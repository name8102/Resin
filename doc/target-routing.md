# Target-aware connection failover

A node can pass the Google latency probe while refusing another destination.
Global probe health therefore remains separate from passive target failures.

- New routes prefer recent target-domain observations, then recent authority
  probes, then unmeasured nodes. Within the strongest evidence tier, the existing
  platform latency/load policy determines the winner; equal scores are randomized.
  This replaces random two-node sampling, which frequently missed working routes
  in large pools. Existing account leases are retained while routable.
- Probe latency includes proxy connection setup through target TLS completion
  (first response byte for plain HTTP). Passive tunnel samples include dial time
  plus first-write-to-first-response time, including when Read starts before Write.
- A failed upstream dial cools down that **platform + node + host:port** route
  for five minutes. Subdomains and ports remain independent; a Google probe or
  an older successful tunnel does not clear the cooldown.
- Cooldowns live only in memory, are capped at 8,192 entries, and expire lazily.
  They are ignored when a platform disables passive circuit breaking.
- Anonymous CONNECT/SOCKS5 connections hedge at most three distinct nodes,
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
