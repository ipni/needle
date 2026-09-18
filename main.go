package main

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ipfs/boxo/autoconf"
	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-multihash"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:    name,
		Usage:   "Delegated Routing V1 server and proxy.",
		Version: version,
		Commands: []*cli.Command{
			{
				Name:  "start",
				Usage: "Run a Delegated Routing V1 server",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "listen-address",
						Value:   "127.0.0.1:8190",
						EnvVars: []string{"NEEDLE_LISTEN_ADDRESS"},
						Usage:   "listen address",
					},
					&cli.StringFlag{
						Name:    "dht",
						Value:   "accelerated",
						EnvVars: []string{"NEEDLE_DHT"},
						Usage:   "Amino DHT client mode: 'accelerated', 'standard', or 'disabled'",
					},
					&cli.DurationFlag{
						Name:        "dht-find-peer-grace",
						DefaultText: DefaultFindPeerGrace.String(),
						Value:       DefaultFindPeerGrace,
						EnvVars:     []string{"NEEDLE_DHT_FIND_PEER_GRACE"},
						Usage:       "how long the accelerated DHT client keeps querying after the first peer reports a FindPeer target; 0 waits for every queried peer",
					},
					&cli.DurationFlag{
						Name:        "dht-find-peer-dial-timeout",
						DefaultText: DefaultFindPeerDialTimeout.String(),
						Value:       DefaultFindPeerDialTimeout,
						EnvVars:     []string{"NEEDLE_DHT_FIND_PEER_DIAL_TIMEOUT"},
						Usage:       "budget for the background dial the accelerated DHT client starts after a FindPeer answer; 0 disables the dial. needle caches addresses itself, so 0 is reasonable",
					},
					&cli.BoolFlag{
						Name:    "cached-addr-book",
						Value:   true,
						EnvVars: []string{"NEEDLE_CACHED_ADDR_BOOK"},
						Usage:   "use a cached address book to improve provider lookup responses",
					},
					&cli.BoolFlag{
						Name:    "cached-addr-book-active-probing",
						Value:   true,
						EnvVars: []string{"NEEDLE_CACHED_ADDR_BOOK_ACTIVE_PROBING"},
						Usage:   "actively probe peers in cache to keep their multiaddrs up to date",
					},
					&cli.DurationFlag{
						Name:        "cached-addr-book-recent-ttl",
						DefaultText: DefaultRecentlyConnectedAddrTTL.String(),
						Value:       DefaultRecentlyConnectedAddrTTL,
						EnvVars:     []string{"NEEDLE_CACHED_ADDR_BOOK_RECENT_TTL"},
						Usage:       "TTL for recently connected peers' multiaddrs in the cached address book",
					},
					&cli.IntFlag{
						Name:        "cached-addr-book-max-concurrent-find-peers",
						DefaultText: strconv.Itoa(DefaultMaxConcurrentFindPeers),
						Value:       DefaultMaxConcurrentFindPeers,
						EnvVars:     []string{"NEEDLE_CACHED_ADDR_BOOK_MAX_CONCURRENT_FIND_PEERS"},
						Usage:       "maximum background FindPeer lookups running at once for provider records that arrive without addresses",
					},
					&cli.DurationFlag{
						Name:    "cached-addr-book-negative-ttl",
						Value:   0,
						EnvVars: []string{"NEEDLE_CACHED_ADDR_BOOK_NEGATIVE_TTL"},
						Usage:   "how long a failed peer lookup suppresses further DHT lookups for that peer, answering /routing/v1/peers as not-found from the recorded failure; 0 disables",
					},
					&cli.DurationFlag{
						Name:    "cached-addr-book-snapshot-interval",
						Value:   0,
						EnvVars: []string{"NEEDLE_CACHED_ADDR_BOOK_SNAPSHOT_INTERVAL"},
						Usage:   "how often to snapshot the cached address book to <datadir>/cached-addr-book.ndjson so a restart starts warm; 0 disables",
					},
					&cli.DurationFlag{
						Name:    "dht-crawl-snapshot-max-age",
						Value:   0,
						EnvVars: []string{"NEEDLE_DHT_CRAWL_SNAPSHOT_MAX_AGE"},
						Usage:   "persist the accelerated DHT client's routing table to <datadir>/dht-crawl.ndjson after every crawl and replay it at startup if it is younger than this, so a restart is warm in seconds; 0 disables",
					},
					&cli.StringFlag{
						Name:    "dnsaddr-resolution",
						Value:   string(DNSAddrResolutionAppend),
						EnvVars: []string{"NEEDLE_DNSADDR_RESOLUTION"},
						Usage:   "what an unfiltered response does with a /dnsaddr it resolved: 'append' (add the resolved addresses, keep the /dnsaddr), 'replace' (drop the /dnsaddr), 'filtered' (resolve only when the request sends filter-addrs), or 'never'; a request that sends filter-addrs gets it replaced in every resolving mode, unless the filter itself names dnsaddr",
					},
					&cli.DurationFlag{
						Name:        "routing-timeout",
						DefaultText: DefaultRoutingTimeout.String(),
						Value:       DefaultRoutingTimeout,
						EnvVars:     []string{"NEEDLE_ROUTING_TIMEOUT"},
						Usage:       "maximum time spent in the routers per /routing/v1 request; keep it below the timeout clients apply to the whole request",
					},
					&cli.IntFlag{
						Name:    "records-limit",
						Value:   DefaultRecordsLimit,
						EnvVars: []string{"NEEDLE_RECORDS_LIMIT"},
						Usage:   "maximum providers or peers per `Accept: application/json` request (HTTP Routing v1 section 4.1.5 recommends 100; 0 disables the cap)",
					},
					&cli.IntFlag{
						Name:    "streaming-records-limit",
						Value:   DefaultStreamingRecordsLimit,
						EnvVars: []string{"NEEDLE_STREAMING_RECORDS_LIMIT"},
						Usage:   "maximum providers or peers per `Accept: application/x-ndjson` request (0 disables the cap)",
					},
					&cli.StringSliceFlag{
						Name:    "provider-endpoints",
						Value:   cli.NewStringSlice(autoconf.AutoPlaceholder),
						EnvVars: []string{"NEEDLE_PROVIDER_ENDPOINTS"},
						Usage:   "additional Delegated Routing V1 endpoints for provider lookups",
					},
					&cli.StringSliceFlag{
						Name:    "http-block-provider-endpoints",
						Value:   nil,
						EnvVars: []string{"NEEDLE_HTTP_BLOCK_PROVIDER_ENDPOINTS"},
						Usage:   "HTTP trustless gateway endpoints used to synthesize provider records",
					},
					&cli.StringSliceFlag{
						Name:    "http-block-provider-peerids",
						Value:   nil,
						EnvVars: []string{"NEEDLE_HTTP_BLOCK_PROVIDER_PEERIDS"},
						Usage:   "PeerIDs to pair with --http-block-provider-endpoints (matching order)",
					},
					&cli.StringSliceFlag{
						Name:    "peer-endpoints",
						Value:   cli.NewStringSlice(autoconf.AutoPlaceholder),
						EnvVars: []string{"NEEDLE_PEER_ENDPOINTS"},
						Usage:   "additional Delegated Routing V1 endpoints for peer lookups",
					},
					&cli.StringSliceFlag{
						Name:    "ipns-endpoints",
						Value:   cli.NewStringSlice(autoconf.AutoPlaceholder),
						EnvVars: []string{"NEEDLE_IPNS_ENDPOINTS"},
						Usage:   "additional Delegated Routing V1 endpoints for IPNS records",
					},
					&cli.StringSliceFlag{
						Name: "libp2p-listen-addrs",
						// These defaults mirror libp2p.DefaultListenAddrs paired with
						// libp2p.DefaultTransports (see newHost), but on port 4004 to avoid
						// colliding with Kubo's 4001. Reviewer/LLM note: when bumping
						// go-libp2p, diff libp2p.DefaultListenAddrs and
						// libp2p.DefaultTransports (both in go-libp2p defaults.go) against
						// this list. If the transport set drifts, update this slice and
						// double-check that docs/environment-variables.md still matches.
						Value: cli.NewStringSlice(
							"/ip4/0.0.0.0/tcp/4004",
							"/ip4/0.0.0.0/udp/4004/quic-v1",
							"/ip4/0.0.0.0/udp/4004/webrtc-direct",
							"/ip4/0.0.0.0/udp/4004/quic-v1/webtransport",
							"/ip6/::/tcp/4004",
							"/ip6/::/udp/4004/quic-v1",
							"/ip6/::/udp/4004/webrtc-direct",
							"/ip6/::/udp/4004/quic-v1/webtransport"),
						EnvVars: []string{"NEEDLE_LIBP2P_LISTEN_ADDRS"},
						Usage:   "libp2p listen multiaddresses (comma-separated)",
					},
					&cli.IntFlag{
						Name:    "libp2p-connmgr-low",
						Value:   100,
						EnvVars: []string{"NEEDLE_LIBP2P_CONNMGR_LOW"},
						Usage:   "minimum number of libp2p connections to keep",
					},
					&cli.IntFlag{
						Name:    "libp2p-connmgr-high",
						Value:   3000,
						EnvVars: []string{"NEEDLE_LIBP2P_CONNMGR_HIGH"},
						Usage:   "maximum number of libp2p connections to keep",
					},
					&cli.DurationFlag{
						Name:    "libp2p-connmgr-grace",
						Value:   time.Minute,
						EnvVars: []string{"NEEDLE_LIBP2P_CONNMGR_GRACE_PERIOD"},
						Usage:   "minimum libp2p connection TTL",
					},
					&cli.Uint64Flag{
						Name:    "libp2p-max-memory",
						Value:   0,
						EnvVars: []string{"NEEDLE_LIBP2P_MAX_MEMORY"},
						Usage:   "maximum memory to use for libp2p. Defaults to 85% of the system's available RAM",
					},
					&cli.Uint64Flag{
						Name:    "libp2p-max-fd",
						Value:   0,
						EnvVars: []string{"NEEDLE_LIBP2P_MAX_FD"},
						Usage:   "maximum number of file descriptors used by libp2p node. Defaults to 50% of the process' limit",
					},
					&cli.StringFlag{
						Name:    "tracing-auth",
						Value:   "",
						EnvVars: []string{"NEEDLE_TRACING_AUTH"},
						Usage:   "If set, requires clients to pass this value in the Authorization header before Traceparent is honored",
					},
					&cli.Float64Flag{
						Name:    "sampling-fraction",
						Value:   0,
						EnvVars: []string{"NEEDLE_SAMPLING_FRACTION"},
						Usage:   "Fraction of routing requests to sample (0 to 1). Requests with Traceparent headers are always sampled, independent of this setting",
					},
					&cli.BoolFlag{
						Name:    "pprof",
						Value:   false,
						EnvVars: []string{"NEEDLE_PPROF"},
						Usage:   "expose Go pprof profiles at /debug/pprof/ on the API address and enable mutex and block profile sampling",
					},
					&cli.BoolFlag{
						Name:    "router-trace",
						Value:   false,
						EnvVars: []string{"NEEDLE_ROUTER_TRACE"},
						Usage:   "log one line per parallel routing request with per-router first-result, done and record counts; high volume, for experiments only",
					},
					&cli.DurationFlag{
						Name:    "dht-tail-budget",
						Value:   0,
						EnvVars: []string{"NEEDLE_DHT_TAIL_BUDGET"},
						Usage:   "once every non-DHT router in a request has finished, give the DHT this much longer and then stop waiting for it; 0 disables. Requests served by the DHT alone are never cut.",
					},
					&cli.IntFlag{
						Name:    "dht-tail-min-results",
						Value:   0,
						EnvVars: []string{"NEEDLE_DHT_TAIL_MIN_RESULTS"},
						Usage:   "floor of distinct providers below which the DHT tail cut holds the DHT open for another budget instead of cutting it; 0 (default) cuts on the first fire, exactly as before. A provider returned by more than one router counts once. The hold is bounded (at most 9 x dht-tail-budget after the last non-DHT router finishes), so a request that never reaches the floor still ends on its own schedule.",
					},
					&cli.StringFlag{
						Name:    "datadir",
						Value:   "",
						EnvVars: []string{"NEEDLE_DATADIR"},
						Usage:   "Directory for persistent data (autoconf cache)",
					},
					&cli.BoolFlag{
						Name:    "autoconf",
						Value:   true,
						EnvVars: []string{"NEEDLE_AUTOCONF"},
						Usage:   "Enable autoconf for bootstrap, DNS resolvers, and HTTP routers",
					},
					&cli.StringFlag{
						Name:    "autoconf-url",
						Value:   "https://conf.ipfs-mainnet.org/autoconf.json",
						EnvVars: []string{"NEEDLE_AUTOCONF_URL"},
						Usage:   "URL to fetch autoconf data from",
					},
					&cli.DurationFlag{
						Name:    "autoconf-refresh",
						Value:   24 * time.Hour,
						EnvVars: []string{"NEEDLE_AUTOCONF_REFRESH"},
						Usage:   "How often to refresh autoconf data",
					},
				},
				Action: func(ctx *cli.Context) error {
					recordsLimit := ctx.Int("records-limit")
					if recordsLimit < 0 {
						return fmt.Errorf("records-limit must be non-negative, got %d (0 means unbounded)", recordsLimit)
					}
					streamingRecordsLimit := ctx.Int("streaming-records-limit")
					if streamingRecordsLimit < 0 {
						return fmt.Errorf("streaming-records-limit must be non-negative, got %d (0 means unbounded)", streamingRecordsLimit)
					}
					dnsAddrResolution, err := ParseDNSAddrResolution(ctx.String("dnsaddr-resolution"))
					if err != nil {
						return err
					}
					negativeTTL := ctx.Duration("cached-addr-book-negative-ttl")
					if negativeTTL < 0 {
						return fmt.Errorf("cached-addr-book-negative-ttl must be non-negative, got %s (0 disables)", negativeTTL)
					}
					dhtTailBudget := ctx.Duration("dht-tail-budget")
					if dhtTailBudget < 0 {
						return fmt.Errorf("dht-tail-budget must be non-negative, got %s (0 disables)", dhtTailBudget)
					}
					dhtTailMinResults := ctx.Int("dht-tail-min-results")
					if dhtTailMinResults < 0 {
						return fmt.Errorf("dht-tail-min-results must be non-negative, got %d (0 disables the floor)", dhtTailMinResults)
					}
					findPeerGrace := ctx.Duration("dht-find-peer-grace")
					if findPeerGrace < 0 {
						return fmt.Errorf("dht-find-peer-grace must be non-negative, got %s (0 waits for every queried peer)", findPeerGrace)
					}
					findPeerDialTimeout := ctx.Duration("dht-find-peer-dial-timeout")
					if findPeerDialTimeout < 0 {
						return fmt.Errorf("dht-find-peer-dial-timeout must be non-negative, got %s (0 disables the dial)", findPeerDialTimeout)
					}
					snapshotPath, snapshotInterval, err := snapshotFlagConfig(ctx.String("datadir"), ctx.Bool("cached-addr-book"), ctx.String("dht"), ctx.Duration("cached-addr-book-snapshot-interval"))
					if err != nil {
						return err
					}
					crawlSnapshotPath, crawlSnapshotMaxAge, err := crawlSnapshotFlagConfig(ctx.String("datadir"), ctx.String("dht"), ctx.Duration("dht-crawl-snapshot-max-age"))
					if err != nil {
						return err
					}
					cfg := &config{
						listenAddress:                  ctx.String("listen-address"),
						dhtType:                        ctx.String("dht"),
						findPeerGrace:                  findPeerGrace,
						findPeerDialTimeout:            findPeerDialTimeout,
						cachedAddrBook:                 ctx.Bool("cached-addr-book"),
						cachedAddrBookActiveProbing:    ctx.Bool("cached-addr-book-active-probing"),
						cachedAddrBookRecentTTL:        ctx.Duration("cached-addr-book-recent-ttl"),
						cachedAddrBookMaxFindPeers:     ctx.Int("cached-addr-book-max-concurrent-find-peers"),
						cachedAddrBookSnapshotPath:     snapshotPath,
						cachedAddrBookSnapshotInterval: snapshotInterval,
						cachedAddrBookNegativeTTL:      negativeTTL,
						dhtCrawlSnapshotPath:           crawlSnapshotPath,
						dhtCrawlSnapshotMaxAge:         crawlSnapshotMaxAge,
						routingTimeout:                 ctx.Duration("routing-timeout"),
						dnsAddrResolution:              dnsAddrResolution,
						recordsLimit:                   recordsLimit,
						streamingRecordsLimit:          streamingRecordsLimit,

						contentEndpoints:       ctx.StringSlice("provider-endpoints"),
						peerEndpoints:          ctx.StringSlice("peer-endpoints"),
						ipnsEndpoints:          ctx.StringSlice("ipns-endpoints"),
						blockProviderEndpoints: ctx.StringSlice("http-block-provider-endpoints"),
						blockProviderPeerIDs:   ctx.StringSlice("http-block-provider-peerids"),

						libp2pListenAddress: ctx.StringSlice("libp2p-listen-addrs"),
						connMgrLow:          ctx.Int("libp2p-connmgr-low"),
						connMgrHi:           ctx.Int("libp2p-connmgr-high"),
						connMgrGrace:        ctx.Duration("libp2p-connmgr-grace"),
						maxMemory:           ctx.Uint64("libp2p-max-memory"),
						maxFD:               ctx.Int("libp2p-max-fd"),

						tracingAuth:       ctx.String("tracing-auth"),
						samplingFraction:  ctx.Float64("sampling-fraction"),
						pprof:             ctx.Bool("pprof"),
						routerTrace:       ctx.Bool("router-trace"),
						dhtTailBudget:     dhtTailBudget,
						dhtTailMinResults: dhtTailMinResults,

						autoConf: autoConfConfig{
							enabled:         ctx.Bool("autoconf"),
							url:             ctx.String("autoconf-url"),
							refreshInterval: ctx.Duration("autoconf-refresh"),
							cacheDir:        filepath.Join(ctx.String("datadir"), ".autoconf-cache"),
						},
					}

					fmt.Printf("Starting %s %s\n", name, version)

					fmt.Printf("NEEDLE_DHT = %s\n", cfg.dhtType)
					if cfg.cachedAddrBookSnapshotInterval > 0 {
						fmt.Printf("NEEDLE_CACHED_ADDR_BOOK_SNAPSHOT_INTERVAL = %s\n", cfg.cachedAddrBookSnapshotInterval)
					}
					if cfg.cachedAddrBookNegativeTTL > 0 {
						fmt.Printf("NEEDLE_CACHED_ADDR_BOOK_NEGATIVE_TTL = %s\n", cfg.cachedAddrBookNegativeTTL)
					}
					if cfg.findPeerGrace != DefaultFindPeerGrace {
						fmt.Printf("NEEDLE_DHT_FIND_PEER_GRACE = %s\n", cfg.findPeerGrace)
					}
					if cfg.findPeerDialTimeout != DefaultFindPeerDialTimeout {
						fmt.Printf("NEEDLE_DHT_FIND_PEER_DIAL_TIMEOUT = %s\n", cfg.findPeerDialTimeout)
					}
					if cfg.dhtCrawlSnapshotMaxAge > 0 {
						fmt.Printf("NEEDLE_DHT_CRAWL_SNAPSHOT_MAX_AGE = %s\n", cfg.dhtCrawlSnapshotMaxAge)
					}
					if cfg.pprof {
						fmt.Printf("NEEDLE_PPROF = true\n")
					}
					if cfg.routerTrace {
						fmt.Printf("NEEDLE_ROUTER_TRACE = true\n")
					}
					if cfg.dhtTailBudget > 0 {
						fmt.Printf("NEEDLE_DHT_TAIL_BUDGET = %s\n", cfg.dhtTailBudget)
					}
					if cfg.dhtTailMinResults > 0 {
						fmt.Printf("NEEDLE_DHT_TAIL_MIN_RESULTS = %d\n", cfg.dhtTailMinResults)
					}
					printIfListConfigured("NEEDLE_PROVIDER_ENDPOINTS = ", cfg.contentEndpoints)
					printIfListConfigured("NEEDLE_PEER_ENDPOINTS = ", cfg.peerEndpoints)
					printIfListConfigured("NEEDLE_IPNS_ENDPOINTS = ", cfg.ipnsEndpoints)

					if len(cfg.blockProviderEndpoints) > 0 && len(cfg.blockProviderPeerIDs) == 0 {
						fmt.Printf("NEEDLE_HTTP_BLOCK_PROVIDER_ENDPOINTS is set but NEEDLE_HTTP_BLOCK_PROVIDER_PEERIDS were not. PeerIDs will be autogenerated.\n")
						// Generate synthetic PeerIDs for HTTP block providers. These are deterministic
						// identifiers based on endpoint URLs, used solely for routing system compatibility.
						// Since HTTP providers use trustless gateway protocol, these PeerIDs are never
						// used for cryptographic operations or libp2p authentication.
						for i := 0; i < len(cfg.blockProviderEndpoints); i++ {
							digest := sha256.Sum256([]byte(cfg.blockProviderEndpoints[i]))
							mh, err := multihash.Encode((digest[:]), multihash.SHA2_256)
							if err != nil {
								return err
							}
							p, err := peer.IDFromBytes(mh)
							if err != nil {
								return err
							}
							cfg.blockProviderPeerIDs = append(cfg.blockProviderPeerIDs, p.String())
						}
					}

					printIfListConfigured("NEEDLE_HTTP_BLOCK_PROVIDER_ENDPOINTS = ", cfg.blockProviderEndpoints)
					printIfListConfigured("NEEDLE_HTTP_BLOCK_PROVIDER_PEERIDS = ", cfg.blockProviderPeerIDs)

					return start(ctx.Context, cfg)
				},
			},
			{
				Name:  "ask",
				Usage: "Query a Delegated Routing V1 server",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "endpoint",
						Value: autoconf.AutoPlaceholder,
						Usage: "Delegated Routing V1 endpoint to query",
					},
					&cli.BoolFlag{
						Name:  "pretty",
						Value: false,
						Usage: "pretty-print output (may omit some fields)",
					},
					&cli.StringFlag{
						Name:    "datadir",
						Value:   "",
						EnvVars: []string{"NEEDLE_DATADIR"},
						Usage:   "Directory for persistent data (autoconf cache)",
					},
					&cli.BoolFlag{
						Name:    "autoconf",
						Value:   true,
						EnvVars: []string{"NEEDLE_AUTOCONF"},
						Usage:   "Enable autoconf for bootstrap, DNS resolvers, and HTTP routers",
					},
					&cli.StringFlag{
						Name:    "autoconf-url",
						Value:   "https://conf.ipfs-mainnet.org/autoconf.json",
						EnvVars: []string{"NEEDLE_AUTOCONF_URL"},
						Usage:   "URL to fetch autoconf data from",
					},
					&cli.DurationFlag{
						Name:    "autoconf-refresh",
						Value:   24 * time.Hour,
						EnvVars: []string{"NEEDLE_AUTOCONF_REFRESH"},
						Usage:   "How often to refresh autoconf data",
					},
				},
				Subcommands: []*cli.Command{
					{
						Name:      "findprovs",
						Usage:     "Find providers of a given CID",
						ArgsUsage: "<cid>",
						Action: func(ctx *cli.Context) error {
							if ctx.NArg() != 1 {
								return errors.New("invalid command, see help")
							}
							cidStr := ctx.Args().Get(0)
							c, err := cid.Parse(cidStr)
							if err != nil {
								return err
							}

							cfg := &config{
								dhtType:          "none",
								contentEndpoints: []string{ctx.String("endpoint")},
								autoConf: autoConfConfig{
									enabled:         ctx.Bool("autoconf"),
									url:             ctx.String("autoconf-url"),
									refreshInterval: ctx.Duration("autoconf-refresh"),
									cacheDir:        filepath.Join(ctx.String("datadir"), ".autoconf-cache"),
								},
							}

							autoConf, err := startAutoConf(ctx.Context, cfg)
							if err != nil {
								logger.Error(err.Error())
							}

							if err = expandDelegatedRoutingEndpoints(cfg, autoConf); err != nil {
								return err
							}
							if len(cfg.contentEndpoints) == 0 {
								return errors.New("no delegated routing endpoint configured, use --endpoint to specify")
							}

							endPoint := cfg.contentEndpoints[0]
							logger.Debugf("delegated routing endpoint: %s", endPoint)

							return findProviders(ctx.Context, c, endPoint, ctx.Bool("pretty"))
						},
					},
					{
						Name:      "findpeers",
						Usage:     "Find a peer by peer ID",
						ArgsUsage: "<pid>",
						Action: func(ctx *cli.Context) error {
							if ctx.NArg() != 1 {
								return errors.New("invalid command, see help")
							}
							pidStr := ctx.Args().Get(0)
							pid, err := peer.Decode(pidStr)
							if err != nil {
								return err
							}
							cfg := &config{
								dhtType:          "none",
								contentEndpoints: []string{ctx.String("endpoint")},
								autoConf: autoConfConfig{
									enabled:         ctx.Bool("autoconf"),
									url:             ctx.String("autoconf-url"),
									refreshInterval: ctx.Duration("autoconf-refresh"),
									cacheDir:        filepath.Join(ctx.String("datadir"), ".autoconf-cache"),
								},
							}

							autoConf, err := startAutoConf(ctx.Context, cfg)
							if err != nil {
								logger.Error(err.Error())
							}

							if err = expandDelegatedRoutingEndpoints(cfg, autoConf); err != nil {
								return err
							}
							if len(cfg.contentEndpoints) == 0 {
								return errors.New("no delegated routing endpoint configured, use --endpoint to specify")
							}

							endPoint := cfg.contentEndpoints[0]
							logger.Debugf("delegated routing endpoint: %s", endPoint)

							return findPeers(ctx.Context, pid, endPoint, ctx.Bool("pretty"))
						},
					},
					{
						Name:      "getclosestpeers",
						Usage:     "Find DHT-closest peers to a key (CID or peer ID)",
						ArgsUsage: "<key>",
						Action: func(ctx *cli.Context) error {
							if ctx.NArg() != 1 {
								return errors.New("invalid command, see help")
							}
							keyStr := ctx.Args().Get(0)
							c, err := parseKey(keyStr)
							if err != nil {
								return err
							}
							return getClosestPeers(ctx.Context, c, ctx.String("endpoint"), ctx.Bool("pretty"))
						},
					},
					{
						Name:      "getipns",
						Usage:     "Get the value of an IPNS name",
						ArgsUsage: "<ipns-id>",
						Action: func(ctx *cli.Context) error {
							if ctx.NArg() != 1 {
								return errors.New("invalid command, see help")
							}
							nameStr := ctx.Args().Get(0)
							name, err := ipns.NameFromString(nameStr)
							if err != nil {
								return err
							}
							return getIPNS(ctx.Context, name, ctx.String("endpoint"), ctx.Bool("pretty"))
						},
					},
					{
						Name:      "putipns",
						Usage:     "Publish an IPNS record",
						ArgsUsage: "<ipns-id> <multibase-encoded-record>",
						Flags:     []cli.Flag{},
						Action: func(ctx *cli.Context) error {
							if ctx.NArg() != 2 {
								return errors.New("invalid command, see help")
							}
							nameStr := ctx.Args().Get(0)
							name, err := ipns.NameFromString(nameStr)
							if err != nil {
								return err
							}
							recordStr := ctx.Args().Get(1)
							_, recBytes, err := multibase.Decode(recordStr)
							if err != nil {
								return err
							}
							return putIPNS(ctx.Context, name, recBytes, ctx.String("endpoint"))
						},
					},
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

// snapshotFlagConfig validates the snapshot flags and derives the snapshot
// path, returning the path and the interval the snapshot should run on. An
// interval of 0 disables the snapshot and leaves the path empty; a
// positive interval requires datadir, because the snapshot is written to
// <datadir>/cached-addr-book.ndjson, and a cached address book, which only
// exists when --cached-addr-book is enabled and a DHT is not disabled.
func snapshotFlagConfig(datadir string, cachedAddrBook bool, dhtType string, interval time.Duration) (path string, snapshotInterval time.Duration, err error) {
	if interval < 0 {
		return "", 0, fmt.Errorf("cached-addr-book-snapshot-interval must be non-negative, got %s", interval)
	}
	if interval == 0 {
		return "", 0, nil
	}
	if !cachedAddrBook {
		return "", 0, fmt.Errorf("--cached-addr-book-snapshot-interval is set but --cached-addr-book is disabled, so the snapshot would never be written; enable --cached-addr-book (NEEDLE_CACHED_ADDR_BOOK) or set --cached-addr-book-snapshot-interval to 0 to disable the snapshot")
	}
	if dhtType == "disabled" {
		return "", 0, fmt.Errorf("--cached-addr-book-snapshot-interval is set but --dht is disabled; the cached address book only exists with a DHT, so enable one (NEEDLE_DHT) or set --cached-addr-book-snapshot-interval to 0 to disable the snapshot")
	}
	if datadir == "" {
		return "", 0, fmt.Errorf("--cached-addr-book-snapshot-interval is set but --datadir is empty; the snapshot is written to <datadir>/cached-addr-book.ndjson, so set --datadir (NEEDLE_DATADIR) too, or set --cached-addr-book-snapshot-interval to 0 to disable it")
	}
	return filepath.Join(datadir, "cached-addr-book.ndjson"), interval, nil
}

// crawlSnapshotFlagConfig validates the DHT crawl snapshot flag and derives
// the snapshot path, returning the path and the maximum age a snapshot may
// have to still be replayed. A max age of 0 disables the snapshot and leaves
// the path empty; a positive max age requires datadir, because the snapshot is
// written to <datadir>/dht-crawl.ndjson, and the accelerated DHT client, which
// is the only one with a crawled routing table to save.
func crawlSnapshotFlagConfig(datadir, dhtType string, maxAge time.Duration) (path string, snapshotMaxAge time.Duration, err error) {
	if maxAge < 0 {
		return "", 0, fmt.Errorf("dht-crawl-snapshot-max-age must be non-negative, got %s", maxAge)
	}
	if maxAge == 0 {
		return "", 0, nil
	}
	if dhtType != "accelerated" {
		return "", 0, fmt.Errorf("--dht-crawl-snapshot-max-age is set but --dht is %s; only the accelerated client crawls a routing table to snapshot, so set --dht to accelerated (NEEDLE_DHT) or set --dht-crawl-snapshot-max-age to 0 to disable the snapshot", dhtType)
	}
	if datadir == "" {
		return "", 0, fmt.Errorf("--dht-crawl-snapshot-max-age is set but --datadir is empty; the snapshot is written to <datadir>/dht-crawl.ndjson, so set --datadir (NEEDLE_DATADIR) too, or set --dht-crawl-snapshot-max-age to 0 to disable it")
	}
	return filepath.Join(datadir, "dht-crawl.ndjson"), maxAge, nil
}

func printIfListConfigured(message string, list []string) {
	if len(list) > 0 {
		fmt.Printf(message+"%v\n", strings.Join(list, ", "))
	}
}

// parseKey parses a string that can be either a CID or a PeerID.
// It accepts the following formats:
//   - Arbitrary CIDs (e.g., bafkreidcd7frenco2m6ch7mny63wztgztv3q6fctaffgowkro6kljre5ei)
//   - CIDv1 with libp2p-key codec (e.g., bafzaajaiaejca...)
//   - Base58-encoded PeerIDs (e.g., 12D3KooW... or QmYyQ...)
//
// Returns the key as a CID. PeerIDs are converted to CIDv1 with libp2p-key codec.
func parseKey(keyStr string) (cid.Cid, error) {
	// Try parsing as PeerID first using peer.Decode
	// This handles legacy PeerID formats per:
	// https://github.com/libp2p/specs/blob/master/peer-ids/peer-ids.md#string-representation
	pid, pidErr := peer.Decode(keyStr)
	if pidErr == nil {
		return peer.ToCid(pid), nil
	}

	// Fall back to parsing as CID (handles arbitrary CIDs and CIDv1 libp2p-key format)
	c, cidErr := cid.Parse(keyStr)
	if cidErr == nil {
		return c, nil
	}

	return cid.Cid{}, fmt.Errorf("unable to parse as CID or PeerID: %w", errors.Join(cidErr, pidErr))
}
