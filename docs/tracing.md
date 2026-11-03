## Tracing

Needle follows the [Open Telemetry] specifications across the stack. It reads the environment variables defined in the [OpenTelemetry Environment Variable Specification], builds on [Boxo Tracing], and lists the variables it uses in [environment-variables.md](environment-variables.md#tracing).

> [!NOTE]
> Unlike the [general tracing in boxo][Boxo Tracing], Needle traces only inbound HTTP routing requests, not background processes. A span that starts with no parent and is not a request (a DHT crawl, a background peer lookup for the cached address book) is never sampled.

- [Turning it on](#turning-it-on)
- [Sampling](#sampling)
  - [Behind a proxy](#behind-a-proxy)
  - [Per-request](#per-request)
- [What a trace shows](#what-a-trace-shows)
  - [The request span](#the-request-span)
  - [Router spans](#router-spans)
  - [/dnsaddr lookups](#dnsaddr-lookups)

### Turning it on

Point needle at an OTLP receiver and choose how much to sample:

```console
$ OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 NEEDLE_SAMPLING_FRACTION=0.01 needle start
...
Tracing: exporting spans, NEEDLE_SAMPLING_FRACTION = 0.01
```

With no exporter configured nothing is sampled, so tracing costs nothing.

### Sampling

needle uses the standard parent-based sampler. Each request is one of:

- **It continues a trusted trace.** Its `Traceparent` header decides: sampled when the flag is set, dropped when it is not. [`NEEDLE_SAMPLING_FRACTION`](environment-variables.md#needle_sampling_fraction) plays no part.
- **Anything else.** needle starts a new trace and samples [`NEEDLE_SAMPLING_FRACTION`](environment-variables.md#needle_sampling_fraction) of them, by trace ID. A `Traceparent` the request carried is kept as a span link, so the caller's trace can still be found, but it cannot force sampling.

Every span below a request follows the request's decision.

A `Traceparent` is trusted when [`NEEDLE_TRACING_TRUST_TRACEPARENT`](environment-variables.md#needle_tracing_trust_traceparent) is set, or when the request carries the [`NEEDLE_TRACING_AUTH`](environment-variables.md#needle_tracing_auth) value in its `Authorization` header.

#### Behind a proxy

When needle is only reachable through a proxy that makes the sampling decision itself (it removes the client's `Traceparent` and `Tracestate`, starts its own span, and passes its own `Traceparent` on), set `NEEDLE_TRACING_TRUST_TRACEPARENT=true`. needle then joins the proxy's trace and samples exactly the requests the proxy sampled. The proxy sends a `Traceparent` on every request, so `NEEDLE_SAMPLING_FRACTION` only matters for requests that reach needle some other way.

Do not set it when clients can reach needle directly, or through a proxy that passes their `Traceparent` through.

#### Per-request

Per-request tracing activates when [`NEEDLE_TRACING_AUTH`](environment-variables.md#needle_tracing_auth) is set and the request carries both an `Authorization` header matching that value and a sampled `Traceparent` header.

```console
$ export NEEDLE_TRACING_AUTH=CHANGEME-tracing-auth-secret # use value from Needle config
$ export CID=bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi
$ curl -H "Authorization: $NEEDLE_TRACING_AUTH" -H "Traceparent: 00-$(openssl rand -hex 16)-00$(openssl rand -hex 7)-01" http://127.0.0.1:8190/routing/v1/providers/$CID -v -o /dev/null
...
> Authorization: CHANGEME-tracing-auth-secret
> Traceparent: 00-b617dc6b6e302ccbabe0115eac80320b-00033792c7de8fc6-01
...
```

Search for `trace_id = b617dc6b6e302ccbabe0115eac80320b` to locate the trace.

A proxy that replaces `Traceparent` replaces this one too, so per-request tracing works only against needle directly.

### What a trace shows

A trace answers which router supplied the answer, how long each one took, and what cutting a router off earlier would have cost. Span names are low cardinality, never a CID, peer ID or IPNS name, so span metrics built from them stay small.

#### The request span

Named after the endpoint, as `needle <method> <route>`:

| Span name | Endpoint |
|-----------|----------|
| `needle GET /routing/v1/providers/{cid}` | provider lookup |
| `needle GET /routing/v1/peers/{peer-id}` | peer lookup |
| `needle GET /routing/v1/ipns/{cid}`, `needle PUT /routing/v1/ipns/{cid}` | IPNS |
| `needle GET /routing/v1/dht/closest/peers/{key}` | closest peers |

It carries the standard HTTP server attributes plus `http.route`. Its status is ERROR on a 5xx response, which includes a request every router failed. A request that found no records is not an error.

When the routers finish before the response does, it also carries:

| Attribute | Meaning |
|-----------|---------|
| `needle.routing.outcome` | `exhausted` (every router ran out), `cut` (the [DHT tail budget](environment-variables.md#needle_dht_tail_budget) ended it), `closed` (the records limit was reached) |
| `needle.routing.routers` | how many routers were asked |
| `needle.routing.last_router`, `needle.routing.tail_ms` | the router that held the request open last, and for how long after the one before it finished |
| `needle.dnsaddr.cache_hits`, `needle.dnsaddr.lookups`, `needle.dnsaddr.throttled` | how `/dnsaddr` resolution answered, see [dnsaddr-resolution.md](dnsaddr-resolution.md) |

On a routing timeout or a client that went away, the response ends without waiting for the routers, so these may be missing; the router spans below still record what happened.

#### Router spans

One child span per router a request asks, named `needle router <op> <router>`, for example `needle router providers delegated:cid.contact` or `needle router providers dht`. `<op>` is `providers`, `peers`, `closest`, `ipns_get` or `ipns_put`; `<router>` is the same label the [router metrics](metrics.md#parallel-router) use. The span starts when needle calls the router and ends when that router finished, so its duration is the router's own. These are the same measurements [`NEEDLE_ROUTER_TRACE`](environment-variables.md#needle_router_trace) logs, without turning that log on.

| Attribute | Meaning |
|-----------|---------|
| `needle.router.name`, `needle.router.op` | as in the span name |
| `needle.router.records` | records the router produced |
| `needle.router.exclusive_records` | records no other router produced in the same request: what the request would have lost without this router |
| `needle.router.first_record_ms` | time to the router's first record, also marked by a `first record` event; absent when it produced none |
| `needle.router.errors` | error results the router returned |
| `needle.router.outcome` | `exhausted` (ran out), `cut` (the DHT tail budget cancelled it), `timeout` (the [routing timeout](environment-variables.md#needle_routing_timeout) ended the request), `client_cancelled` (the client went away), `closed` (needle stopped reading: the records limit was reached, or another router already answered an IPNS lookup), `error` (the call failed) |
| `needle.router.found` | IPNS lookups only: whether this router returned a valid record |

The span's status is ERROR when the call failed or the router returned error results. A router that found nothing, or answered not found, is not an error, and neither is one that was cut off.

The DHT's own spans (it starts one for every peer a lookup queries, a few hundred per provider lookup) are left out of request traces; the DHT router span measures the lookup as a whole.

The DHT's router span also counts how the [cached address book](peer-address-caching.md) filled in addresses: `needle.addr_book.hits`, `needle.addr_book.misses`, `needle.addr_book.unused` (the record already had addresses), `needle.addr_book.lookups_dispatched` (background peer lookups started for records with none), and for `/peers` `needle.addr_book.negative_hits` and `needle.addr_book.lookups_joined`.

#### /dnsaddr lookups

Each DNS query a request waits on gets a `needle dnsaddr lookup` span, with `dns.question.name`, `needle.dnsaddr.result` (`resolved`, `empty`, `failed`, or how the request ended if it gave up waiting), `needle.dnsaddr.addrs`, and `needle.dnsaddr.shared` (the query was already in flight for another request). Cache hits get no span; the request span counts them. A request makes at most 16 such queries.

[Boxo Tracing]: https://github.com/ipfs/boxo/blob/main/docs/tracing.md
[Open Telemetry]: https://opentelemetry.io/
[OpenTelemetry Environment Variable Specification]: https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/configuration/sdk-environment-variables.md
