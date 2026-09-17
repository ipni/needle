# Someguy Environment Variables

The environment variables below override `someguy`'s built-in defaults.

- [Configuration](#configuration)
  - [`SOMEGUY_LISTEN_ADDRESS`](#someguy_listen_address)
  - [`SOMEGUY_DHT`](#someguy_dht)
  - [`SOMEGUY_CACHED_ADDR_BOOK`](#someguy_cached_addr_book)
  - [`SOMEGUY_CACHED_ADDR_BOOK_RECENT_TTL`](#someguy_cached_addr_book_recent_ttl)
  - [`SOMEGUY_CACHED_ADDR_BOOK_ACTIVE_PROBING`](#someguy_cached_addr_book_active_probing)
  - [`SOMEGUY_CACHED_ADDR_BOOK_MAX_CONCURRENT_FIND_PEERS`](#someguy_cached_addr_book_max_concurrent_find_peers)
  - [`SOMEGUY_CACHED_ADDR_BOOK_SNAPSHOT_INTERVAL`](#someguy_cached_addr_book_snapshot_interval)
  - [`SOMEGUY_CACHED_ADDR_BOOK_NEGATIVE_TTL`](#someguy_cached_addr_book_negative_ttl)
  - [`SOMEGUY_DNSADDR_RESOLUTION`](#someguy_dnsaddr_resolution)
  - [`SOMEGUY_ROUTING_TIMEOUT`](#someguy_routing_timeout)
  - [`SOMEGUY_RECORDS_LIMIT`](#someguy_records_limit)
  - [`SOMEGUY_STREAMING_RECORDS_LIMIT`](#someguy_streaming_records_limit)
  - [`SOMEGUY_PROVIDER_ENDPOINTS`](#someguy_provider_endpoints)
  - [`SOMEGUY_PEER_ENDPOINTS`](#someguy_peer_endpoints)
  - [`SOMEGUY_IPNS_ENDPOINTS`](#someguy_ipns_endpoints)
  - [`SOMEGUY_AUTOCONF`](#someguy_autoconf)
  - [`SOMEGUY_AUTOCONF_URL`](#someguy_autoconf_url)
  - [`SOMEGUY_AUTOCONF_REFRESH`](#someguy_autoconf_refresh)
  - [`SOMEGUY_HTTP_BLOCK_PROVIDER_ENDPOINTS`](#someguy_http_block_provider_endpoints)
  - [`SOMEGUY_HTTP_BLOCK_PROVIDER_PEERIDS`](#someguy_http_block_provider_peerids)
  - [`SOMEGUY_LIBP2P_LISTEN_ADDRS`](#someguy_libp2p_listen_addrs)
  - [`SOMEGUY_LIBP2P_CONNMGR_LOW`](#someguy_libp2p_connmgr_low)
  - [`SOMEGUY_LIBP2P_CONNMGR_HIGH`](#someguy_libp2p_connmgr_high)
  - [`SOMEGUY_LIBP2P_CONNMGR_GRACE_PERIOD`](#someguy_libp2p_connmgr_grace_period)
  - [`SOMEGUY_LIBP2P_MAX_MEMORY`](#someguy_libp2p_max_memory)
  - [`SOMEGUY_LIBP2P_MAX_FD`](#someguy_libp2p_max_fd)
- [Logging](#logging)
  - [`GOLOG_LOG_LEVEL`](#golog_log_level)
  - [`GOLOG_LOG_FMT`](#golog_log_fmt)
  - [`GOLOG_FILE`](#golog_file)
  - [`GOLOG_TRACING_FILE`](#golog_tracing_file)
- [Tracing](#tracing)
  - [`SOMEGUY_TRACING_AUTH`](#someguy_tracing_auth)
  - [`SOMEGUY_SAMPLING_FRACTION`](#someguy_sampling_fraction)
- [Profiling](#profiling)
  - [`SOMEGUY_PPROF`](#someguy_pprof)
  - [`SOMEGUY_ROUTER_TRACE`](#someguy_router_trace)

## Configuration

### `SOMEGUY_LISTEN_ADDRESS`

The address to listen on.

Default: `127.0.0.1:8190`

### `SOMEGUY_DHT`

Controls DHT client mode: `standard`, `accelerated`, `disabled`

Default: `accelerated`

### `SOMEGUY_CACHED_ADDR_BOOK`

Enables the cached address book. When disabled, Someguy omits cached addresses from `FindProviders` results for peers lacking multiaddrs.

Default: `true`

### `SOMEGUY_CACHED_ADDR_BOOK_RECENT_TTL`

TTL for recently connected peers' multiaddrs in the cached address book. Applies only when `SOMEGUY_CACHED_ADDR_BOOK` is enabled.

Default: `48h`

### `SOMEGUY_CACHED_ADDR_BOOK_ACTIVE_PROBING`

Enables active probing of cached peers to keep their multiaddrs up to date. Applies only when `SOMEGUY_CACHED_ADDR_BOOK` is enabled.

Default: `true`

### `SOMEGUY_CACHED_ADDR_BOOK_MAX_CONCURRENT_FIND_PEERS`

Maximum background `FindPeer` lookups that run at once. Someguy starts one of these when a provider record arrives without addresses and the cache has none. At the limit Someguy skips the lookup and omits the record. Applies only when `SOMEGUY_CACHED_ADDR_BOOK` is enabled.

Raise this only if `someguy_cached_router_find_peer_lookups_rejected` keeps increasing. See [metrics.md](metrics.md) and [peer-address-caching.md](peer-address-caching.md).

Default: `512`

### `SOMEGUY_CACHED_ADDR_BOOK_SNAPSHOT_INTERVAL`

How often to write the cached address book to `<datadir>/cached-addr-book.ndjson`. Someguy restores the snapshot at startup, so a restart serves cached addresses immediately instead of the cache refilling over about an hour.

The restore is synchronous and finishes before the HTTP listener is up: about 1.76 s per 200,000 peers on local SSD, so a cache at the 1,000,000-peer cap adds roughly 9 s to startup, which health-check and readiness timeouts must cover. The file is about 260 bytes per peer, around 250 MiB at that cap, so `SOMEGUY_DATADIR` needs headroom for it.

The snapshot holds the addresses with their TTLs, reconstructed from the time each peer's addresses were last written, and the probe backoff state, so a restart does not re-dial peers Someguy has already given up on. It does not hold signed peer records (re-learned on the next identify), the accelerated DHT routing table (crawled again on start), or the host peerstore.

The periodic write is the guarantee: a clean shutdown writes one more time, but an OOM kill skips shutdown, so the interval bounds how stale a restart can be.

Requires `SOMEGUY_DATADIR`, because the snapshot is written to `<datadir>/cached-addr-book.ndjson`. Someguy refuses to start if this is set while `SOMEGUY_CACHED_ADDR_BOOK` is disabled or `SOMEGUY_DHT` is `disabled`, since the snapshot could never be written. Watch the saves and the restore with the `someguy_cached_addr_book_snapshot_*` metrics in [metrics.md](metrics.md) and [peer-address-caching.md](peer-address-caching.md).

Default: `0` (disabled)

### `SOMEGUY_CACHED_ADDR_BOOK_NEGATIVE_TTL`

How long a failed peer lookup suppresses further DHT lookups for that peer. Within the TTL, `/routing/v1/peers/{peer-id}` is answered as not-found straight from the failure recorded by the previous lookup, without a DHT query.

A DHT lookup for a peer nobody reports costs the full query timeout, and the same absent peers are requested repeatedly, so those requests dominate the slow tail of the peers endpoint while telling us nothing new.

The trade-off: a peer that comes online is invisible on that instance for at most the TTL, which is one client retry cycle. `1m` is a reasonable starting point - long enough to absorb the repeated lookups for a genuinely absent peer, short enough that a peer coming back is picked up on the client's next retry.

The failure it reads is the same one that drives probe backoff, recorded by `RecordFailedConnection`, so a peer that someguy successfully connects to has the record cleared and is not suppressed. Suppressed requests are counted as `someguy_cached_router_peer_addr_lookups{cache="negative"}` in [metrics.md](metrics.md); compare that against the `miss` series to see how much of the peers traffic it is absorbing.

Independently of this setting, concurrent `/routing/v1/peers` requests for the same peer ID are collapsed into a single DHT lookup, so a popular missing peer does not start one full-timeout walk per in-flight request.

Applies only when `SOMEGUY_CACHED_ADDR_BOOK` is enabled.

Default: `0` (disabled)

### `SOMEGUY_DNSADDR_RESOLUTION`

Controls when Someguy replaces a `/dnsaddr` address with the addresses it names.

A `/dnsaddr` carries no transport component, so [`filter-addrs`](https://specs.ipfs.tech/routing/http-routing-v1/) can neither match it nor exclude it. A provider reachable only through a `/dnsaddr` is dropped from a filtered response, and a provider the client asked to exclude survives one. Resolving before the filter runs fixes both.

- `append` (default): resolve on every request. A request that sends `filter-addrs` gets the `/dnsaddr` replaced, because a filter cannot match one and it would otherwise survive a filter meant to exclude it. A request without a filter gets the resolved addresses added and keeps the `/dnsaddr`, so it can dial now and re-resolve later.
- `replace`: like `append`, but a request without a filter also gets the `/dnsaddr` replaced. Responses are smaller and no client has to resolve a `/dnsaddr` itself, at the cost of the indirection: a client cannot re-resolve the hostname later from the response alone.
- `filtered`: resolve only when the request sends `filter-addrs`, and replace the `/dnsaddr` when it does. Requests without a filter are left alone and cost no DNS lookup.
- `never`: never resolve.

In every resolving mode, a filter whose positive entries name `dnsaddr` itself keeps the `/dnsaddr`: that is the one filter which can match it, and the client sending it is asking for the indirections. See [dnsaddr-resolution.md](dnsaddr-resolution.md) for the details.

Resolved sets are cached, and one request cannot trigger an unbounded number of DNS lookups. See [dnsaddr-resolution.md](dnsaddr-resolution.md).

Default: `append`

### `SOMEGUY_ROUTING_TIMEOUT`

Maximum time one `/routing/v1` request spends in the routers.

Keep this below the timeout the client applies to the whole request. A client that gives up first loses every record Someguy found, because the response is still open when the client disconnects. Helia's delegated routing client allows 30 seconds and starts its timer before Someguy starts this one.

Default: `25s`

### `SOMEGUY_RECORDS_LIMIT`

Maximum providers or peers returned per `Accept: application/json` request. [HTTP Routing v1, section 4.1.5](https://specs.ipfs.tech/routing/http-routing-v1/) recommends `100`. Set to `0` to disable the cap.

Default: `100`

### `SOMEGUY_STREAMING_RECORDS_LIMIT`

Maximum providers or peers returned per `Accept: application/x-ndjson` request. Sits above `SOMEGUY_RECORDS_LIMIT` so streaming returns more results. Set to `0` to disable the cap.

Default: `1000`

### `SOMEGUY_PROVIDER_ENDPOINTS`

Comma-separated list of [Delegated Routing V1](https://specs.ipfs.tech/routing/http-routing-v1/) endpoints for provider lookups.

Supports two URL formats:
- Base URL without path: `https://example.com`
- Full URL with path: `https://example.com/routing/v1/providers`

The `auto` placeholder (default) resolves to endpoints from the network configuration at [`SOMEGUY_AUTOCONF_URL`](#someguy_autoconf_url).

Default: `auto`

### `SOMEGUY_PEER_ENDPOINTS`

Comma-separated list of Delegated Routing V1 endpoints for peer routing.

URL formats: same as [`SOMEGUY_PROVIDER_ENDPOINTS`](#someguy_provider_endpoints) (use `/routing/v1/peers` path).

Default: `auto`

### `SOMEGUY_IPNS_ENDPOINTS`

Comma-separated list of Delegated Routing V1 endpoints for IPNS records.

URL formats: same as [`SOMEGUY_PROVIDER_ENDPOINTS`](#someguy_provider_endpoints) (use `/routing/v1/ipns` path).

Default: `auto`

### `SOMEGUY_AUTOCONF`

Enables automatic configuration (autoconf) of delegated routing endpoints and bootstrap peers.

When enabled, Someguy replaces the `auto` placeholder in endpoint configuration with network-recommended values fetched from the autoconf URL.

Default: `true`

### `SOMEGUY_AUTOCONF_URL`

URL to fetch autoconf data from. Defaults to the service that provides configuration for [IPFS Mainnet](https://docs.ipfs.tech/concepts/glossary/#mainnet).

Default: `https://conf.ipfs-mainnet.org/autoconf.json`

### `SOMEGUY_AUTOCONF_REFRESH`

How often to refresh the autoconf data. The configuration is cached and updated at this interval.

Default: `24h`

### `SOMEGUY_HTTP_BLOCK_PROVIDER_ENDPOINTS`

Comma-separated list of [HTTP trustless gateways](https://specs.ipfs.tech/http-gateways/trustless-gateway/) that Someguy probes to synthesize provider records.

When a configured gateway responds with HTTP 200 to a `HEAD /ipfs/{cid}?format=raw` request, `FindProviders` returns a provider record that contains the matching PeerID from `SOMEGUY_HTTP_BLOCK_PROVIDER_PEERIDS` and the gateway endpoint as a multiaddr with the `/tls/http` suffix.

> [!IMPORTANT]
> When creating a synthetic `/routing/v1` for your gateway, and not a general-purpose routing endpoint, set `SOMEGUY_DHT=disabled` and `SOMEGUY_PROVIDER_ENDPOINTS=""` to disable default DHT and HTTP routers and exclusively use the explicitly defined `SOMEGUY_HTTP_BLOCK_PROVIDER_ENDPOINTS`.

Default: none

### `SOMEGUY_HTTP_BLOCK_PROVIDER_PEERIDS`

Comma-separated list of [multibase-encoded peerIDs](https://github.com/libp2p/specs/blob/master/peer-ids/peer-ids.md#string-representation) that Someguy embeds in synthetic provider records for the HTTP endpoints in `SOMEGUY_HTTP_BLOCK_PROVIDER_ENDPOINTS`. The order of PeerIDs must match the order of endpoints.

If you configure provider endpoints without PeerIDs, Someguy generates synthetic PeerIDs automatically, deterministically derived from SHA-256 hashes of the endpoint URLs. These PeerIDs exist only for routing-system compatibility; HTTP trustless gateways never use them for cryptographic operations or peer authentication.

Default: none

### `SOMEGUY_LIBP2P_LISTEN_ADDRS`

Comma-separated libp2p listen multiaddresses. Someguy binds port `4004` on IPv4 and IPv6 across libp2p's default transports. To see the exact defaults built into this release, run `someguy start --help`.

### `SOMEGUY_LIBP2P_CONNMGR_LOW`

Minimum number of libp2p connections to keep.

Default: 100

### `SOMEGUY_LIBP2P_CONNMGR_HIGH`

Maximum number of libp2p connections to keep.

Default: 3000

### `SOMEGUY_LIBP2P_CONNMGR_GRACE_PERIOD`

Minimum libp2p connection TTL.

Default: 1m

### `SOMEGUY_LIBP2P_MAX_MEMORY`

Maximum memory to use for libp2p.

Default: 0 (85% of the system's available RAM)

### `SOMEGUY_LIBP2P_MAX_FD`

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
GOLOG_LOG_LEVEL="error,someguy=debug" someguy
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

### `SOMEGUY_TRACING_AUTH`

Setting a non-empty value enables on-demand per-request tracing.

To honor a `Traceparent` or `Tracestate` header, Someguy requires the request to carry an `Authorization` header whose value matches `SOMEGUY_TRACING_AUTH`.

### `SOMEGUY_SAMPLING_FRACTION`

Fraction of routing requests to sample (0 to 1). Applied independently of `Traceparent`-based sampling.

Default: `0`

## Profiling

### `SOMEGUY_PPROF`

Exposes the Go [pprof](https://pkg.go.dev/net/http/pprof) endpoints at `/debug/pprof/` on the API address (`SOMEGUY_LISTEN_ADDRESS`), and turns on the sampling that the mutex and block profiles need: `runtime.SetMutexProfileFraction(100)` and `runtime.SetBlockProfileRate(10000)`. Both are off in the Go runtime by default, so without this flag those two profiles come back empty.

That sampling is not free - it instruments every contended lock acquisition and every blocking operation - so leave it off unless you are profiling.

The endpoints serve the process's command line, its goroutine stacks and its heap, so treat them as you would `/debug/metrics/prometheus`: on a loopback API address they are no more exposed than the metrics, but on an API address reachable from the network they are.

```console
$ curl -o cpu.pprof 'http://127.0.0.1:8190/debug/pprof/profile?seconds=30'
```

Default: `false`

### `SOMEGUY_ROUTER_TRACE`

Logs one line per parallel routing request, at `info` on the `someguy` logger, with each router's first-result time, finish time, record count and exclusive-record count. It is the per-request form of the [parallel router metrics](metrics.md#parallel-router): the histograms say how the fleet behaves, this says what happened to one request, which is what you need to ask "what would cutting this router off after X ms have cost".

This is one log line for every `/routing/v1/providers` and `/routing/v1/peers` request the server answers - at a few hundred requests per second that is a few hundred lines per second, indefinitely. It is meant for a measurement run, not for leaving on.

```console
$ SOMEGUY_ROUTER_TRACE=true someguy start
{"level":"info","msg":"parallel routing request","op":"providers","total_ms":612,"reason":"exhausted","delegated:cid.contact_first_ms":-1,"delegated:cid.contact_done_ms":38,"delegated:cid.contact_records":0,"delegated:cid.contact_exclusive":0,"dht_first_ms":244,"dht_done_ms":612,"dht_records":4,"dht_exclusive":4}
```

A `first_ms` of `-1` means that router never produced a record; `0` would read as "instantly". The metrics above are always on - only this line is gated.

Default: `false`
