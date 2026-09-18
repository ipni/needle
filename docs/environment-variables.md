# Needle Environment Variables

Renamed from SOMEGUY_ at v1.0.0; there are no aliases.

The environment variables below override `needle`'s built-in defaults.

- [Configuration](#configuration)
  - [`NEEDLE_LISTEN_ADDRESS`](#needle_listen_address)
  - [`NEEDLE_DHT`](#needle_dht)
  - [`NEEDLE_DHT_FIND_PEER_GRACE`](#needle_dht_find_peer_grace)
  - [`NEEDLE_DHT_FIND_PEER_DIAL_TIMEOUT`](#needle_dht_find_peer_dial_timeout)
  - [`NEEDLE_CACHED_ADDR_BOOK`](#needle_cached_addr_book)
  - [`NEEDLE_CACHED_ADDR_BOOK_RECENT_TTL`](#needle_cached_addr_book_recent_ttl)
  - [`NEEDLE_CACHED_ADDR_BOOK_ACTIVE_PROBING`](#needle_cached_addr_book_active_probing)
  - [`NEEDLE_CACHED_ADDR_BOOK_MAX_CONCURRENT_FIND_PEERS`](#needle_cached_addr_book_max_concurrent_find_peers)
  - [`NEEDLE_CACHED_ADDR_BOOK_SNAPSHOT_INTERVAL`](#needle_cached_addr_book_snapshot_interval)
  - [`NEEDLE_CACHED_ADDR_BOOK_NEGATIVE_TTL`](#needle_cached_addr_book_negative_ttl)
  - [`NEEDLE_DHT_CRAWL_SNAPSHOT_MAX_AGE`](#needle_dht_crawl_snapshot_max_age)
  - [`NEEDLE_DNSADDR_RESOLUTION`](#needle_dnsaddr_resolution)
  - [`NEEDLE_ROUTING_TIMEOUT`](#needle_routing_timeout)
  - [`NEEDLE_DHT_TAIL_BUDGET`](#needle_dht_tail_budget)
  - [`NEEDLE_DHT_TAIL_MIN_RESULTS`](#needle_dht_tail_min_results)
  - [`NEEDLE_RECORDS_LIMIT`](#needle_records_limit)
  - [`NEEDLE_STREAMING_RECORDS_LIMIT`](#needle_streaming_records_limit)
  - [`NEEDLE_PROVIDER_ENDPOINTS`](#needle_provider_endpoints)
  - [`NEEDLE_PEER_ENDPOINTS`](#needle_peer_endpoints)
  - [`NEEDLE_IPNS_ENDPOINTS`](#needle_ipns_endpoints)
  - [`NEEDLE_AUTOCONF`](#needle_autoconf)
  - [`NEEDLE_AUTOCONF_URL`](#needle_autoconf_url)
  - [`NEEDLE_AUTOCONF_REFRESH`](#needle_autoconf_refresh)
  - [`NEEDLE_HTTP_BLOCK_PROVIDER_ENDPOINTS`](#needle_http_block_provider_endpoints)
  - [`NEEDLE_HTTP_BLOCK_PROVIDER_PEERIDS`](#needle_http_block_provider_peerids)
  - [`NEEDLE_LIBP2P_LISTEN_ADDRS`](#needle_libp2p_listen_addrs)
  - [`NEEDLE_LIBP2P_CONNMGR_LOW`](#needle_libp2p_connmgr_low)
  - [`NEEDLE_LIBP2P_CONNMGR_HIGH`](#needle_libp2p_connmgr_high)
  - [`NEEDLE_LIBP2P_CONNMGR_GRACE_PERIOD`](#needle_libp2p_connmgr_grace_period)
  - [`NEEDLE_LIBP2P_MAX_MEMORY`](#needle_libp2p_max_memory)
  - [`NEEDLE_LIBP2P_MAX_FD`](#needle_libp2p_max_fd)
- [Logging](#logging)
  - [`GOLOG_LOG_LEVEL`](#golog_log_level)
  - [`GOLOG_LOG_FMT`](#golog_log_fmt)
  - [`GOLOG_FILE`](#golog_file)
  - [`GOLOG_TRACING_FILE`](#golog_tracing_file)
- [Tracing](#tracing)
  - [`NEEDLE_TRACING_AUTH`](#needle_tracing_auth)
  - [`NEEDLE_SAMPLING_FRACTION`](#needle_sampling_fraction)
- [Profiling](#profiling)
  - [`NEEDLE_PPROF`](#needle_pprof)
  - [`NEEDLE_ROUTER_TRACE`](#needle_router_trace)

## Configuration

### `NEEDLE_LISTEN_ADDRESS`

The address to listen on.

Default: `127.0.0.1:8190`

### `NEEDLE_DHT`

Controls DHT client mode: `standard`, `accelerated`, `disabled`

Default: `accelerated`

### `NEEDLE_DHT_FIND_PEER_GRACE`

How long the accelerated DHT client keeps querying after the first peer reports a `FindPeer` target, before cancelling the query and answering. Peers close to the target answer at nearly the same time, so a short grace collects the duplicates and alternate addresses they report and then cuts off the peers that are still slow, instead of waiting out `NEEDLE_ROUTING_TIMEOUT` on them.

The trade-off: an address known only to a peer that answers later than the grace is dropped. In practice those late reports rarely differ from the first. Set `0` to disable the early exit and wait for every queried peer, which restores the pre-fork timing.

Applies only when `NEEDLE_DHT` is `accelerated`. This is the library default, so leaving it unset already gets the behaviour.

Default: `500ms`

### `NEEDLE_DHT_FIND_PEER_DIAL_TIMEOUT`

Budget for the background dial the accelerated DHT client starts once a `FindPeer` query has reported the target's addresses. The dial does not gate the answer: Needle returns the reported addresses immediately and dials afterwards, purely so identify can refine those addresses in the peerstore for later callers.

Needle maintains its own cached address book, refreshed by identify on its own connections and by active probing, so that refinement is largely redundant here. Set `0` to disable the dial entirely and avoid one dial attempt per `FindPeer` for an unreachable target; the reported addresses are still returned and recorded.

Applies only when `NEEDLE_DHT` is `accelerated`. The client bounds these dials to 64 in flight regardless of this setting, skipping rather than queueing beyond that.

Default: `5s`

### `NEEDLE_CACHED_ADDR_BOOK`

Enables the cached address book. When disabled, Needle omits cached addresses from `FindProviders` results for peers lacking multiaddrs.

Default: `true`

### `NEEDLE_CACHED_ADDR_BOOK_RECENT_TTL`

TTL for recently connected peers' multiaddrs in the cached address book. Applies only when `NEEDLE_CACHED_ADDR_BOOK` is enabled.

Default: `48h`

### `NEEDLE_CACHED_ADDR_BOOK_ACTIVE_PROBING`

Enables active probing of cached peers to keep their multiaddrs up to date. Applies only when `NEEDLE_CACHED_ADDR_BOOK` is enabled.

Default: `true`

### `NEEDLE_CACHED_ADDR_BOOK_MAX_CONCURRENT_FIND_PEERS`

Maximum background `FindPeer` lookups that run at once. Needle starts one of these when a provider record arrives without addresses and the cache has none. At the limit Needle skips the lookup and omits the record. Applies only when `NEEDLE_CACHED_ADDR_BOOK` is enabled.

Raise this only if `needle_cached_router_find_peer_lookups_rejected` keeps increasing. See [metrics.md](metrics.md) and [peer-address-caching.md](peer-address-caching.md).

Default: `512`

### `NEEDLE_CACHED_ADDR_BOOK_SNAPSHOT_INTERVAL`

How often to write the cached address book to `<datadir>/cached-addr-book.ndjson`. Needle restores the snapshot at startup, so a restart serves cached addresses immediately instead of the cache refilling over about an hour.

The restore is synchronous and finishes before the HTTP listener is up: about 1.76 s per 200,000 peers on local SSD, so a cache at the 1,000,000-peer cap adds roughly 9 s to startup, which health-check and readiness timeouts must cover. The file is about 260 bytes per peer, around 250 MiB at that cap, so `NEEDLE_DATADIR` needs headroom for it.

The snapshot holds the addresses with their TTLs, reconstructed from the time each peer's addresses were last written, and the probe backoff state, so a restart does not re-dial peers Needle has already given up on. It does not hold signed peer records (re-learned on the next identify), the accelerated DHT routing table (that has a snapshot of its own, `NEEDLE_DHT_CRAWL_SNAPSHOT_MAX_AGE`), or the host peerstore.

The periodic write is the guarantee: a clean shutdown writes one more time, but an OOM kill skips shutdown, so the interval bounds how stale a restart can be.

Requires `NEEDLE_DATADIR`, because the snapshot is written to `<datadir>/cached-addr-book.ndjson`. Needle refuses to start if this is set while `NEEDLE_CACHED_ADDR_BOOK` is disabled or `NEEDLE_DHT` is `disabled`, since the snapshot could never be written. Watch the saves and the restore with the `needle_cached_addr_book_snapshot_*` metrics in [metrics.md](metrics.md) and [peer-address-caching.md](peer-address-caching.md).

Default: `0` (disabled)

### `NEEDLE_CACHED_ADDR_BOOK_NEGATIVE_TTL`

How long a failed peer lookup suppresses further DHT lookups for that peer. Within the TTL, `/routing/v1/peers/{peer-id}` is answered as not-found straight from the failure recorded by the previous lookup, without a DHT query.

A DHT lookup for a peer nobody reports costs the full query timeout, and the same absent peers are requested repeatedly, so those requests dominate the slow tail of the peers endpoint while telling us nothing new.

The trade-off: a peer that comes online is invisible on that instance for at most the TTL, which is one client retry cycle. `1m` is a reasonable starting point - long enough to absorb the repeated lookups for a genuinely absent peer, short enough that a peer coming back is picked up on the client's next retry.

The failure it reads is the same one that drives probe backoff, recorded by `RecordFailedConnection`, so a peer that needle successfully connects to has the record cleared and is not suppressed. Suppressed requests are counted as `needle_cached_router_peer_addr_lookups{cache="negative"}` in [metrics.md](metrics.md); compare that against the `miss` series to see how much of the peers traffic it is absorbing.

Independently of this setting, concurrent `/routing/v1/peers` requests for the same peer ID are collapsed into a single DHT lookup, so a popular missing peer does not start one full-timeout walk per in-flight request.

Applies only when `NEEDLE_CACHED_ADDR_BOOK` is enabled.

Default: `0` (disabled)

### `NEEDLE_DHT_CRAWL_SNAPSHOT_MAX_AGE`

How old the accelerated DHT client's routing-table snapshot may be and still be replayed at startup. The table is written to `<datadir>/dht-crawl.ndjson` after every completed crawl, and replayed on the next start when the file is younger than this, so a restart is ready in seconds instead of after a full crawl.

Needle only answers from the accelerated client once it reports itself ready, which takes a completed crawl of the network - minutes, during which every request falls back to the standard DHT client. The snapshot removes that gap: the replay feeds the saved peers and their addresses straight into the accelerated client without dialling anything, and a real crawl is triggered the moment the replayed table is in place. A replayed table is therefore only as stale as the last completed crawl plus the downtime, and only until that crawl finishes.

The file is written after every completed crawl and at no other time: not on a timer, and not at shutdown. The table only changes when a crawl completes, so there is nothing else to write. A crawl cut short by shutdown is not saved, and neither is an implausibly small one (under 1,000 peers, where a settled table is 10,000 to 25,000), because a broken crawl is worth neither saving nor replaying. A snapshot that is missing, too old, too small, or unreadable is logged and ignored, and Needle crawls as it would have.

Requires `NEEDLE_DATADIR`, because the snapshot is written to `<datadir>/dht-crawl.ndjson`, and `NEEDLE_DHT=accelerated`, because no other client has a crawled routing table; Needle refuses to start if either is missing. The file holds one line per peer with its public addresses, roughly 100 to 200 bytes per peer depending on how many each advertises - a peer reachable over TCP and QUIC on both IPv4 and IPv6 costs twice one with a single address - so a settled table is a few megabytes. Watch the crawls and the replay with the `needle_dht_crawl_*` metrics in [metrics.md](metrics.md).

A value of `2h` is a reasonable start: it covers a restart or a deploy, while a table older than two crawl intervals has drifted enough that crawling from cold is the safer beginning.

Default: `0` (disabled)

### `NEEDLE_DNSADDR_RESOLUTION`

Controls when Needle replaces a `/dnsaddr` address with the addresses it names.

A `/dnsaddr` carries no transport component, so [`filter-addrs`](https://specs.ipfs.tech/routing/http-routing-v1/) can neither match it nor exclude it. A provider reachable only through a `/dnsaddr` is dropped from a filtered response, and a provider the client asked to exclude survives one. Resolving before the filter runs fixes both.

- `append` (default): resolve on every request. A request that sends `filter-addrs` gets the `/dnsaddr` replaced, because a filter cannot match one and it would otherwise survive a filter meant to exclude it. A request without a filter gets the resolved addresses added and keeps the `/dnsaddr`, so it can dial now and re-resolve later.
- `replace`: like `append`, but a request without a filter also gets the `/dnsaddr` replaced. Responses are smaller and no client has to resolve a `/dnsaddr` itself, at the cost of the indirection: a client cannot re-resolve the hostname later from the response alone.
- `filtered`: resolve only when the request sends `filter-addrs`, and replace the `/dnsaddr` when it does. Requests without a filter are left alone and cost no DNS lookup.
- `never`: never resolve.

In every resolving mode, a filter whose positive entries name `dnsaddr` itself keeps the `/dnsaddr`: that is the one filter which can match it, and the client sending it is asking for the indirections. See [dnsaddr-resolution.md](dnsaddr-resolution.md) for the details.

Resolved sets are cached, and one request cannot trigger an unbounded number of DNS lookups. See [dnsaddr-resolution.md](dnsaddr-resolution.md).

Default: `append`

### `NEEDLE_ROUTING_TIMEOUT`

Maximum time one `/routing/v1` request spends in the routers.

Keep this below the timeout the client applies to the whole request. A client that gives up first loses every record Needle found, because the response is still open when the client disconnects. Helia's delegated routing client allows 30 seconds and starts its timer before Needle starts this one.

Default: `25s`

### `NEEDLE_DHT_TAIL_BUDGET`

How long the DHT may keep a request open after every non-DHT router in it has finished, before Needle stops waiting for it and returns what it has. The delegated and block-provider routers answer first; the DHT is the one that trails. When the last non-DHT router finishes, this budget starts, and when it runs out the DHT's lookup is cancelled and the response closes with whatever records have arrived.

It trades a little recall for a lot of tail latency: a request that would otherwise wait out the whole DHT walk now ends one budget after the fast routers are done. The cost is the DHT-exclusive records that would have arrived in that window - small, because by then the other endpoints have already answered.

The cut only fires when there is something to wait on: a request served by the DHT alone, or one where every router is the DHT, is never cut, because there is no faster answer to return early for. A budget of `0` (the default) disables it entirely and Needle behaves as before.

```console
$ NEEDLE_DHT_TAIL_BUDGET=500ms needle start
```

Default: `0` (disabled)

### `NEEDLE_DHT_TAIL_MIN_RESULTS`

A floor of distinct providers below which the DHT tail cut holds the DHT open for another budget instead of cutting it. The cut's timer fires on the same schedule as before - one `NEEDLE_DHT_TAIL_BUDGET` after every non-DHT router has finished - but when it does, Needle now checks how many different providers the request has already been handed: at or above the floor it cuts, exactly as now; below it, it lets the DHT keep running for another budget and checks again. A provider returned by more than one router counts once, not once per router, so a single peer found by both IPNI and the DHT does not satisfy a floor of `2`.

The motivation is that the first provider is the difference between retrievable and not, while the twentieth is not. An unconditional cut prices them the same. A request that has found nothing when the timer fires is the one most worth keeping open; one that already has a handful of providers is not. The floor is checked when the timer fires, not when it arms, so providers that arrive during the budget window count: a request that crosses the floor mid-window is cut on the next fire.

The hold is bounded: after 8 below-floor fires the cut fires anyway, so a request that never reaches the floor still ends at most `9 × NEEDLE_DHT_TAIL_BUDGET` after the last non-DHT router finishes, rather than living indefinitely. A request served by the DHT alone is still never cut, at any floor.

A value of `0` (the default) disables the floor entirely: the timer cuts on its first fire, byte-identical to behaviour before this flag existed. It exists so the cost of the floor can be measured in production - via `needle_router_tail_held` and `needle_router_tail_held_seconds` - rather than assumed.

```console
$ NEEDLE_DHT_TAIL_BUDGET=500ms NEEDLE_DHT_TAIL_MIN_RESULTS=1 needle start
```

Default: `0` (the floor is disabled; the cut is unconditional)

### `NEEDLE_RECORDS_LIMIT`

Maximum providers or peers returned per `Accept: application/json` request. [HTTP Routing v1, section 4.1.5](https://specs.ipfs.tech/routing/http-routing-v1/) recommends `100`. Set to `0` to disable the cap.

Default: `100`

### `NEEDLE_STREAMING_RECORDS_LIMIT`

Maximum providers or peers returned per `Accept: application/x-ndjson` request. Sits above `NEEDLE_RECORDS_LIMIT` so streaming returns more results. Set to `0` to disable the cap.

Default: `1000`

### `NEEDLE_PROVIDER_ENDPOINTS`

Comma-separated list of [Delegated Routing V1](https://specs.ipfs.tech/routing/http-routing-v1/) endpoints for provider lookups.

Supports two URL formats:
- Base URL without path: `https://example.com`
- Full URL with path: `https://example.com/routing/v1/providers`

The `auto` placeholder (default) resolves to endpoints from the network configuration at [`NEEDLE_AUTOCONF_URL`](#needle_autoconf_url).

Default: `auto`

### `NEEDLE_PEER_ENDPOINTS`

Comma-separated list of Delegated Routing V1 endpoints for peer routing.

URL formats: same as [`NEEDLE_PROVIDER_ENDPOINTS`](#needle_provider_endpoints) (use `/routing/v1/peers` path).

Default: `auto`

### `NEEDLE_IPNS_ENDPOINTS`

Comma-separated list of Delegated Routing V1 endpoints for IPNS records.

URL formats: same as [`NEEDLE_PROVIDER_ENDPOINTS`](#needle_provider_endpoints) (use `/routing/v1/ipns` path).

Default: `auto`

### `NEEDLE_AUTOCONF`

Enables automatic configuration (autoconf) of delegated routing endpoints and bootstrap peers.

When enabled, Needle replaces the `auto` placeholder in endpoint configuration with network-recommended values fetched from the autoconf URL.

Default: `true`

### `NEEDLE_AUTOCONF_URL`

URL to fetch autoconf data from. Defaults to the service that provides configuration for [IPFS Mainnet](https://docs.ipfs.tech/concepts/glossary/#mainnet).

Default: `https://conf.ipfs-mainnet.org/autoconf.json`

### `NEEDLE_AUTOCONF_REFRESH`

How often to refresh the autoconf data. The configuration is cached and updated at this interval.

Default: `24h`

### `NEEDLE_HTTP_BLOCK_PROVIDER_ENDPOINTS`

Comma-separated list of [HTTP trustless gateways](https://specs.ipfs.tech/http-gateways/trustless-gateway/) that Needle probes to synthesize provider records.

When a configured gateway responds with HTTP 200 to a `HEAD /ipfs/{cid}?format=raw` request, `FindProviders` returns a provider record that contains the matching PeerID from `NEEDLE_HTTP_BLOCK_PROVIDER_PEERIDS` and the gateway endpoint as a multiaddr with the `/tls/http` suffix.

> [!IMPORTANT]
> When creating a synthetic `/routing/v1` for your gateway, and not a general-purpose routing endpoint, set `NEEDLE_DHT=disabled` and `NEEDLE_PROVIDER_ENDPOINTS=""` to disable default DHT and HTTP routers and exclusively use the explicitly defined `NEEDLE_HTTP_BLOCK_PROVIDER_ENDPOINTS`.

Default: none

### `NEEDLE_HTTP_BLOCK_PROVIDER_PEERIDS`

Comma-separated list of [multibase-encoded peerIDs](https://github.com/libp2p/specs/blob/master/peer-ids/peer-ids.md#string-representation) that Needle embeds in synthetic provider records for the HTTP endpoints in `NEEDLE_HTTP_BLOCK_PROVIDER_ENDPOINTS`. The order of PeerIDs must match the order of endpoints.

If you configure provider endpoints without PeerIDs, Needle generates synthetic PeerIDs automatically, deterministically derived from SHA-256 hashes of the endpoint URLs. These PeerIDs exist only for routing-system compatibility; HTTP trustless gateways never use them for cryptographic operations or peer authentication.

Default: none

### `NEEDLE_LIBP2P_LISTEN_ADDRS`

Comma-separated libp2p listen multiaddresses. Needle binds port `4004` on IPv4 and IPv6 across libp2p's default transports. To see the exact defaults built into this release, run `needle start --help`.

### `NEEDLE_LIBP2P_CONNMGR_LOW`

Minimum number of libp2p connections to keep.

Default: 100

### `NEEDLE_LIBP2P_CONNMGR_HIGH`

Maximum number of libp2p connections to keep.

Default: 3000

### `NEEDLE_LIBP2P_CONNMGR_GRACE_PERIOD`

Minimum libp2p connection TTL.

Default: 1m

### `NEEDLE_LIBP2P_MAX_MEMORY`

Maximum memory to use for libp2p.

Default: 0 (85% of the system's available RAM)

### `NEEDLE_LIBP2P_MAX_FD`

Maximum number of file descriptors used by libp2p node.

Default: 0 (50% of the process' limit)

## Logging

### `GOLOG_LOG_LEVEL`

Sets the log level globally or per subsystem. Levels:

* `debug`
* `info`
* `warn`
* `error`
* `dpanic`
* `panic`
* `fatal`

Specify per-subsystem levels as `subsystem=level`. Combine a global level with any number of per-subsystem levels by separating them with commas.

Default: `error`

Example:

```console
GOLOG_LOG_LEVEL="error,needle=debug" needle
```

### `GOLOG_LOG_FMT`

Sets the log message format. Supported values:

- `color`: human-readable, colorized (ANSI) output
- `nocolor`: human-readable, plain-text output
- `json`: structured JSON

For example, to log structured JSON (for easier parsing):

```bash
export GOLOG_LOG_FMT="json"
```
The logging format defaults to `color` when the output is a terminal, and
`nocolor` otherwise.

### `GOLOG_FILE`

Writes logs to the given file. Defaults to stderr.

### `GOLOG_TRACING_FILE`

Writes tracing events to the given file. Tracing is disabled by default.

Warning: tracing affects performance.

## Tracing

See [tracing.md](tracing.md).

### `NEEDLE_TRACING_AUTH`

Setting a non-empty value enables on-demand per-request tracing.

To honor a `Traceparent` or `Tracestate` header, Needle requires the request to carry an `Authorization` header whose value matches `NEEDLE_TRACING_AUTH`.

### `NEEDLE_SAMPLING_FRACTION`

Fraction of routing requests to sample (0 to 1). Applied independently of `Traceparent`-based sampling.

Default: `0`

## Profiling

### `NEEDLE_PPROF`

Exposes the Go [pprof](https://pkg.go.dev/net/http/pprof) endpoints at `/debug/pprof/` on the API address (`NEEDLE_LISTEN_ADDRESS`), and turns on the sampling that the mutex and block profiles need: `runtime.SetMutexProfileFraction(100)` and `runtime.SetBlockProfileRate(10000)`. Both are off in the Go runtime by default, so without this flag those two profiles come back empty.

That sampling is not free - it instruments every contended lock acquisition and every blocking operation - so leave it off unless you are profiling.

The endpoints serve the process's command line, its goroutine stacks and its heap, so treat them as you would `/debug/metrics/prometheus`: on a loopback API address they are no more exposed than the metrics, but on an API address reachable from the network they are.

```console
$ curl -o cpu.pprof 'http://127.0.0.1:8190/debug/pprof/profile?seconds=30'
```

Default: `false`

### `NEEDLE_ROUTER_TRACE`

Logs one line per parallel routing request, at `info` on the `needle` logger, with each router's first-result time, finish time, record count and exclusive-record count. It is the per-request form of the [parallel router metrics](metrics.md#parallel-router): the histograms say how the fleet behaves, this says what happened to one request, which is what you need to ask "what would cutting this router off after X ms have cost".

This is one log line for every `/routing/v1/providers` and `/routing/v1/peers` request the server answers - at a few hundred requests per second that is a few hundred lines per second, indefinitely. It is meant for a measurement run, not for leaving on.

```console
$ NEEDLE_ROUTER_TRACE=true needle start
{"level":"info","msg":"parallel routing request","op":"providers","total_ms":612,"reason":"exhausted","delegated:cid.contact_first_ms":-1,"delegated:cid.contact_done_ms":38,"delegated:cid.contact_records":0,"delegated:cid.contact_exclusive":0,"dht_first_ms":244,"dht_done_ms":612,"dht_records":4,"dht_exclusive":4}
```

A `first_ms` of `-1` means that router never produced a record; `0` would read as "instantly". The metrics above are always on - only this line is gated.

Default: `false`
