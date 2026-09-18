## Someguy metrics

Someguy exposes a Prometheus endpoint at `http://127.0.0.1:8190/debug/metrics/prometheus` by default.

The endpoint includes the default [Prometheus Go client metrics](https://prometheus.io/docs/guides/go-application/) plus the Someguy-specific metrics listed below.

### Delegated HTTP Routing (`/routing/v1`) server

`boxo/routing/http/server` (the `/routing/v1` handler) exports metrics with the `delegated_routing_server_` prefix:

- `delegated_routing_server_http_request_duration_seconds_[bucket|sum|count]{code,handler,method}`: histogram of HTTP request latency
- `delegated_routing_server_http_response_size_bytes_[bucket|sum|count]{code,handler,method}`: histogram of HTTP response size

### Delegated HTTP Routing (`/routing/v1`) client

When Someguy aggregates other `/routing/v1` endpoints, `boxo/routing/http/client` exports metrics with the `someguy_` prefix:

- `someguy_routing_http_client_latency_[bucket|sum|count]{code,error,host,operation}`: histogram of operation latency
- `someguy_routing_http_client_length_[bucket|sum|count]{host,operation}`: histogram of response collection size

### Someguy caches

- `someguy_cached_addr_book_probe_duration_seconds_[bucket|sum|count]`: histogram of peer-probing duration in seconds
- `someguy_cached_addr_book_probed_peers{result}`: counter of probed peers, labeled `online` or `offline`
- `someguy_cached_addr_book_peer_state_size`: gauge of peers currently tracked in peer state
- `someguy_cached_addr_book_snapshot_duration_seconds`: gauge of the duration of the last address book snapshot save attempt in seconds
- `someguy_cached_addr_book_snapshot_peers`: gauge of peers in the last successful address book snapshot
- `someguy_cached_addr_book_snapshot_last_success_timestamp_seconds`: gauge of the Unix timestamp of the last successful address book snapshot save
- `someguy_cached_addr_book_snapshot_restored_peers`: gauge of peers restored from the address book snapshot at startup
- `someguy_cached_addr_book_snapshot_restored_addrs`: gauge of addresses offered to the address book when restoring the snapshot at startup
- `someguy_cached_addr_book_snapshot_errors{op}`: counter of failed address book snapshot operations, labeled `save` or `load`
- `someguy_cached_router_peer_addr_lookups{cache,origin}`: counter of peer address-info lookups per origin and cache state. `cache="negative"` counts `/routing/v1/peers` requests answered as not-found from a recorded earlier failure without reaching the DHT. The series is registered at `0` on startup, so it is present even while `SOMEGUY_CACHED_ADDR_BOOK_NEGATIVE_TTL` is `0` and stays flat rather than absent
- `someguy_cached_router_find_peer_lookups_joined`: counter of `/routing/v1/peers` requests that attached to a DHT lookup already in flight for the same peer ID instead of starting their own. Always on; against the peers request rate it is the share of peers traffic that is concurrent requests for the same peer

The five snapshot gauges read `0` while the address book snapshot is disabled (`SOMEGUY_CACHED_ADDR_BOOK_SNAPSHOT_INTERVAL` at `0`). The errors counter has no `_total` suffix (matching `probed_peers`), and each of its `op` series only appears after the first error of that kind, so alerts must handle the absent series.

### Accelerated DHT client

- `someguy_dht_accelerated_ready`: gauge, `1` when the accelerated client's routing table is fresh enough to serve lookups, `0` while requests fall back to the standard client. Sampled every 10 seconds, so it lags reality by up to that long

This is the only metric that says the accelerated client is actually answering,
and it is the one to gate a rollout on. In particular
`someguy_dht_crawl_snapshot_restored_peers` below is not: it counts what a
snapshot replay reported, before the routing table filter decides what to keep,
so it can read non-zero while the table is empty.

Two things it cannot tell you, both of which matter to anything gating on it:

- It reads `0` forever unless `SOMEGUY_DHT` is `accelerated`, because no other
  mode has an accelerated client to be ready. A rollout gate has to check the
  DHT mode too, or it waits forever on a `standard` instance for a `1` that can
  never arrive.
- It reads `1` on a replayed table as readily as on a crawled one, and a replay
  dials nothing, so the table behind that `1` may be as old as
  `SOMEGUY_DHT_CRAWL_SNAPSHOT_MAX_AGE` with its dead entries not yet pruned.
  That is the point of the feature - serving from a stale table beats falling
  back to the standard client for minutes - but a rollout that wants a
  crawl-confirmed table should also wait for
  `someguy_dht_crawl_snapshot_last_success_timestamp_seconds` to advance past
  process start, which happens when the crawl triggered after the replay
  finishes and saves.

### Accelerated DHT crawl

The accelerated client builds its routing table by crawling, and with
`SOMEGUY_DHT_CRAWL_SNAPSHOT_MAX_AGE` set it saves that table after every crawl
and replays it at startup. Only the snapshot populates them - nothing else
reports the crawl, whose one other trace is a log line from
go-libp2p-kad-dht - so with the snapshot disabled every gauge below is still
exported and reads `0`.

- `someguy_dht_crawl_duration_seconds`: gauge of the duration of the last completed crawl in seconds. A crawl cancelled by shutdown does not update it
- `someguy_dht_crawl_peers`: gauge of the peers found by the last completed crawl. A settled table is 10,000 to 25,000; a much smaller number is a broken crawl
- `someguy_dht_crawl_snapshot_peers`: gauge of the peers in the last successfully saved snapshot
- `someguy_dht_crawl_snapshot_last_success_timestamp_seconds`: gauge of the Unix timestamp of the last successful snapshot save
- `someguy_dht_crawl_snapshot_restored_peers`: gauge of the peers replayed from the snapshot at startup, `0` when nothing was replayed
- `someguy_dht_crawl_snapshot_age_seconds_at_restore`: gauge of the age of the snapshot that was replayed at startup
- `someguy_dht_crawl_snapshot_errors{op}`: counter of failed snapshot operations, labeled `save` or `load`

The six gauges read `0` while the crawl snapshot is disabled
(`SOMEGUY_DHT_CRAWL_SNAPSHOT_MAX_AGE` at `0`), exactly as the address book
gauges do. `0` is the Unix epoch for
`snapshot_last_success_timestamp_seconds`, so a staleness alert of the shape
`time() - someguy_dht_crawl_snapshot_last_success_timestamp_seconds > 2h` fires
immediately and permanently on every instance with the snapshot off; guard it
with `> 0` or scope it to the instances that enable the feature.

A snapshot that is absent, older than the configured max age, or too small to be
worth replaying is an expected state rather than an error, so it is not counted
in `snapshot_errors`; an unreadable or malformed file is. Unlike the gauges, it
is a counter vector: like the address book counter it has no `_total` suffix,
and each `op` series only appears after the first error of that kind, so alerts
must handle the absent series.

### Parallel router

When more than one router serves an operation, Someguy fans the request out to
all of them and merges their records. Only the merge point knows what each
router contributed and when, so these are measured there. `op` is `providers`,
`peers` or `closest`; `router` is `dht` or `delegated:<host>`. A request served
by a single router does not fan out, so it is not measured here.

- `someguy_router_first_result_seconds_[bucket|sum|count]{op,router}`: histogram of the time from the start of the request to a router's first record. Only observed for routers that produced something, so its `count` is "requests this router answered", not "requests it saw".
- `someguy_router_done_seconds_[bucket|sum|count]{op,router,reason}`: histogram of the time from the start of the request to a router finishing. `reason` is `exhausted` when its iterator ran out, `cancelled` when the request ended under it - the records limit was reached, or the client went away, and `cut` when the [DHT tail budget](environment-variables.md#someguy_dht_tail_budget) cancelled the DHT after every other router had finished. A JSON response cannot be written until every router is done or the records limit is hit, so the slowest of these is the response time.
- `someguy_router_records{op,router}`: counter of records forwarded per router.
- `someguy_router_exclusive_records{op,router}`: counter of records whose peer ID no other router produced in the same request. Records with no peer ID, and schemas Someguy does not know, are not counted either way.
- `someguy_router_tail_seconds_[bucket|sum|count]{router}`: histogram of the gap between the second-to-last router finishing and the last one finishing, observed once per multi-router request, under the router that was last.
- `someguy_router_last_finisher{router}`: counter of requests in which this router was the last to finish. This is `tail_seconds`'s denominator.
- `someguy_router_tail_held`: counter of times the DHT tail cut fired with fewer delivered results than [`SOMEGUY_DHT_TAIL_MIN_RESULTS`](environment-variables.md#someguy_dht_tail_min_results) and held the DHT open for another budget instead of cutting it. Observed once per below-floor fire, not once per request: a request that fires twice holds twice. It reads `0` (and its series is absent) while the floor is at its default of `0`, because then every fire cuts.
- `someguy_router_tail_held_seconds_[bucket|sum|count]`: histogram of how long the DHT tail cut held the DHT open for on a below-floor fire, from the last non-DHT router finishing to that fire. Together with `tail_held` it is the cost the floor buys: more DHT time spent on requests that had not found enough, visible rather than inferred from `done_seconds`.

Like the snapshot error counters, each series only appears after its first
non-zero observation, so a router that has never been last, or has never found
a record no one else had, has no series at all rather than a series reading
`0`. Alerts and dashboards have to handle the absent series.

`tail_seconds` and `exclusive_records` are the two halves of "is this router
worth waiting for". `tail_seconds{router="dht"}` against
`last_finisher{router="dht"}` is how much response time the DHT costs when it
is the one holding the request open - the time a cut-off would save.
`exclusive_records{router="dht"}` against `records{router="dht"}` is what that
cut-off would throw away: records the DHT alone found. A router with a long
tail and no exclusive records is pure latency; a long tail and many exclusive
records is a real trade.

### Background peer lookups

When a provider record arrives without addresses and the cache has none, Someguy
dispatches a `FindPeer` in the background. These outlive the request that
triggered them, so they are capped per instance by
[`SOMEGUY_CACHED_ADDR_BOOK_MAX_CONCURRENT_FIND_PEERS`](environment-variables.md#someguy_cached_addr_book_max_concurrent_find_peers)
and tracked here. See [peer-address-caching.md](peer-address-caching.md).

- `someguy_cached_router_find_peer_lookups_in_flight`: gauge of background lookups currently running. Compare against the cap: steady state well below it means normal traffic never reaches the limit.
- `someguy_cached_router_find_peer_lookups_rejected`: counter of lookups skipped because the cap was reached. A sustained increase means providers are being dropped that Someguy would otherwise have resolved, so either raise the cap or look at where the traffic is coming from.
- `someguy_cached_router_find_peer_lookup_duration_seconds_[bucket|sum|count]`: histogram of how long each background lookup runs. Multiply by the dispatch rate to get expected concurrency, which is how the cap is sized.


### DNSADDR resolution

Someguy replaces `/dnsaddr` provider addresses with the addresses they name
before applying `filter-addrs`. See
[dnsaddr-resolution.md](dnsaddr-resolution.md).

- `someguy_routers_dnsaddr_resolutions{result}`: counter of resolution outcomes, labeled `cache-hit`, `resolved`, `empty`, `failed`, or `throttled` (out of per-request lookups). `cache-hit` and `throttled` count once per `/dnsaddr` seen; `resolved`, `empty`, and `failed` count once per DNS query, and concurrent requests waiting on the same name share one query. A rising `throttled` means requests are hitting the per-request lookup cap. A low `cache-hit` share means the hostnames being asked for keep changing, which is what an abusive client looks like.
- `someguy_routers_dnsaddr_resolution_duration_seconds_[bucket|sum|count]`: histogram of DNS query durations, one sample per query. Queries run detached from the request, but a record whose name is being queried waits for the answer, so the tail here is added latency for responses that hit uncached names.
