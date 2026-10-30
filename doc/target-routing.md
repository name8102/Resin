# Target-aware connection failover

A node can pass the Google latency probe while refusing another destination.
Global probe health therefore remains separate from passive target failures.

- A failed upstream dial cools down that **platform + node + host:port** route
  for five minutes. Subdomains and ports remain independent; a Google probe or
  an older successful tunnel does not clear the cooldown.
- Cooldowns live only in memory, are capped at 8,192 entries, and expire lazily.
  They are ignored when a platform disables passive circuit breaking.
- CONNECT and SOCKS5 try at most three distinct nodes before acknowledging the
  tunnel. Each dial has a 1.4-second timeout, keeping all three attempts below
  Mihomo's five-second SOCKS dial budget; a caller's earlier deadline wins.
  Sticky routes still prefer another node with the same egress IP.
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
