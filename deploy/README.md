# Cluster Host Deployment

The production cluster keeps public authentication and TLS on the Scientist
control plane. This host exposes only the private browser-pool data plane.

Expected layout:

```text
/opt/scientist-literature/current       reviewed release
/etc/scientist-literature/cluster.env   resource and bind settings
/etc/scientist-literature/firewall.env control-plane source and private port
/etc/scientist-literature/profile-patrol.json
/etc/scientist-literature/rsc-keep-warm.json
/etc/scientist-literature/profile-fleet-a.json
/etc/scientist-literature/profile-fleet-b.json
/data/scientist/literature/profiles     persistent profile-001..profile-024
/data/scientist/literature/profiles-b   persistent profile-025..profile-048
/data/scientist/literature/private      root-managed internal admin token
```

Install the units in `deploy/systemd/`, then enable the firewall, cluster, and
backend repair timer. Do not enable the profile fleet or legacy patrol lanes in
normal production. They are bounded diagnostic tools for explicit qualification
or recovery runs; foreground reads plus small scheduled article/SI canaries are
the normal health signal. The firewall uses the Docker
`DOCKER-USER` chain and permits the private pool port only from the Scientist
control-plane host. Public MCP keys remain on the control plane and are never
copied to the cluster host.

After an operator completes an RSC challenge in explicitly selected persistent
profiles, install `config/rsc-keep-warm.example.json` as
`/etc/scientist-literature/rsc-keep-warm.json` and enable lanes `1` and `2` of
`scientist-literature-rsc-keep-warm@.service`. The service uses the shared pool
lease and therefore yields to foreground requests. Keep its browser allowlist
limited to the profiles actually verified by the operator. Disable both lanes
before changing that allowlist or running profile replacement.

Two browser-engine shards each own 24 profile workers. Each shard assigns four
profiles to one of six private FlareSolverr services, for twelve solver services
overall. The shared pool interleaves shard A and B at ingress while retaining
one private endpoint. A failed profile worker or solver queue is isolated from
the other shard.

When the optional fleet is enabled for a reviewed diagnostic run, it and foreground
requests share the browser pool and foreground work is admitted first. The example
configuration limits checking to four slots per shard. Automatic profile
replacement is disabled until evidence identifies a profile-local failure; a
publisher or queue failure is not replacement evidence. A profile is exclusively in
one of `available`, `serving_external`, `checking`, `draining`, `replacing`,
`warming`, `retired`, or `quarantined`. Replacement IDs and candidate
generations are immutable one-to-one records in SQLite WAL registries. A
candidate must finish the full probe matrix and improve on its target before the
old profile is removed; otherwise the old generation is restored.

An optional publisher-aware router may run on the Scientist control-plane host
when one publisher is deliberately delegated to a workstation edge. Install
`scientist-literature-browser-router.service` with its environment template and
point all control-plane literature callers at the router. The router keeps the
private cluster as its primary backend, selects an edge by publisher affinity or
hostname, and never retries a routed request through the primary backend. Bind
it only to the Docker bridge gateway or another private control-plane address.
