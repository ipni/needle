# 🧭 Needle

[![Official Part of IPFS Project](https://img.shields.io/badge/project-IPFS-blue.svg?style=flat-square)](https://ipfs.tech)
[![Discourse Forum](https://img.shields.io/discourse/posts?server=https%3A%2F%2Fdiscuss.ipfs.tech)](https://discuss.ipfs.tech)
[![Matrix](https://img.shields.io/matrix/ipfs-space%3Aipfs.io?server_fqdn=matrix.org)](https://matrix.to/#/#ipfs-space:ipfs.io)
[![CI](https://img.shields.io/github/actions/workflow/status/ipni/needle/go-test.yml?branch=main)](https://github.com/ipni/needle/actions)
[![GitHub Release](https://img.shields.io/github/v/release/ipni/needle?filter=!*rc*)](https://github.com/ipni/needle/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/ipni/needle.svg)](https://pkg.go.dev/github.com/ipni/needle)

Needle is IPNI's hard fork of [ipfs/someguy](https://github.com/ipfs/someguy), an [HTTP Delegated Routing V1](https://specs.ipfs.tech/routing/http-routing-v1/) server that proxies requests to the [Amino DHT](https://docs.ipfs.tech/concepts/glossary/#amino) and other [delegated routing servers](https://specs.ipfs.tech/routing/http-routing-v1/). IPNI runs it publicly at `https://delegated-ipfs.dev/routing/v1`. At v1.0.0 the config, metrics and image names all changed: the `SOMEGUY_` env prefix is now `NEEDLE_`, metric series moved from `someguy_` to `needle_`, and the image is `ghcr.io/ipni/needle`.

## Build

```bash
go build -o needle
```

## Install

```bash
go install github.com/ipni/needle@latest
```

### Docker

Automated Docker container releases are available from the [Github container registry](https://github.com/ipni/needle/pkgs/container/needle):

- 🟢 Releases
  - `latest` always points at the latest stable release
  - `vN.N.N` point at a specific [release tag](https://github.com/ipni/needle/releases)
- 🟠 Unreleased developer builds
  - `main-latest` always points at the `HEAD` of the `main` branch
  - `main-YYYY-DD-MM-GITSHA` points at a specific commit from the `main` branch
- ⚠️ Experimental, unstable builds
  - `staging-latest` always points at the `HEAD` of the `staging` branch
  - `staging-YYYY-DD-MM-GITSHA` points at a specific commit from the `staging` branch
  - This tag is used by developers for internal testing, not intended for end users

When using Docker, pass configuration via `-e`:
```console
$ docker pull ghcr.io/ipni/needle:main-latest
$ docker run --rm -it --net=host ghcr.io/ipni/needle:main-latest
```

See [`/docs/environment-variables.md`](./docs/environment-variables.md).

## Usage

Run `needle` as a client or as a server.

### Server

Start the server with `needle start`. By default it proxies requests to the [IPFS Amino DHT](https://blog.ipfs.tech/2023-09-amino-refactoring/) and other [Delegated Routing V1](https://specs.ipfs.tech/routing/http-routing-v1/) servers.

For more details, run `needle start --help`.

### Client

To query an existing server without running one yourself, use `needle ask <subcommand>` to look up a provider, peer, or IPNS record.

For more details, run `needle ask --help`.

### AutoConf

Automatic configuration of bootstrap peers and delegated routing endpoints. When enabled (default), Needle replaces the `auto` placeholder with network-recommended values fetched from a remote URL.

Configuration:
- `--autoconf` / [`NEEDLE_AUTOCONF`](docs/environment-variables.md#needle_autoconf)
- `--autoconf-url` / [`NEEDLE_AUTOCONF_URL`](docs/environment-variables.md#needle_autoconf_url)
- `--autoconf-refresh` / [`NEEDLE_AUTOCONF_REFRESH`](docs/environment-variables.md#needle_autoconf_refresh)

Endpoint flags (default to `auto`):
- `--provider-endpoints` / [`NEEDLE_PROVIDER_ENDPOINTS`](docs/environment-variables.md#needle_provider_endpoints)
- `--peer-endpoints` / [`NEEDLE_PEER_ENDPOINTS`](docs/environment-variables.md#needle_peer_endpoints)
- `--ipns-endpoints` / [`NEEDLE_IPNS_ENDPOINTS`](docs/environment-variables.md#needle_ipns_endpoints)

To use custom endpoints instead of `auto`:
```bash
needle start --ipns-endpoints https://example.com
```

See [environment-variables.md](docs/environment-variables.md) for URL formats and configuration details.

## Documentation

- [environment-variables.md](docs/environment-variables.md): all config flags and environment variables
- [peer-address-caching.md](docs/peer-address-caching.md): how `/providers` and `/peers` cache and refresh peer addresses, and the trade-offs behind what needle deliberately does not do
- [response-streaming.md](docs/response-streaming.md): NDJSON streaming contract, the timeout budget, and traps that break streaming
- [dnsaddr-resolution.md](docs/dnsaddr-resolution.md): why `/dnsaddr` is resolved before `filter-addrs`, and how it is bounded
- [metrics.md](docs/metrics.md): Prometheus metrics
- [tracing.md](docs/tracing.md): OpenTelemetry tracing

## Deployment

For self-hosting, run the [prebuilt Docker image](#docker).

## Release

1. Create a PR from branch `release-vX.Y.Z` against `main` that:
   1. Updates [`CHANGELOG.md`](CHANGELOG.md) with entries for the current release
   2. Updates the [`version.json`](./version.json) file
2. Once the release checker creates a draft release, copy-paste the changelog into the draft
3. Merge the PR; the release workflow tags and publishes automatically

## License

Dual-licensed under [MIT + Apache 2.0](LICENSE.md)
