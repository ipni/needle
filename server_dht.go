package main

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/go-cid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/crawler"
	"github.com/libp2p/go-libp2p-kad-dht/fullrt"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	manet "github.com/multiformats/go-multiaddr/net"
)

type bundledDHT struct {
	standard *dht.IpfsDHT
	fullRT   *fullrt.FullRT

	// crawlSnapshot is nil unless the crawl snapshot is enabled. cancel and wg
	// belong to the goroutine that triggers the refresh after a replay.
	crawlSnapshot *snapshotCrawler
	cancel        context.CancelFunc
	wg            sync.WaitGroup
}

const (
	// DefaultFindPeerGrace mirrors the accelerated client's own default for how
	// long it keeps querying after the first peer reports a FindPeer target.
	// Peers close to the target answer at nearly the same time, so a short grace
	// collects their reports and then cuts the slow ones.
	DefaultFindPeerGrace = 500 * time.Millisecond
	// DefaultFindPeerDialTimeout mirrors the accelerated client's own default
	// budget for the background dial it starts after answering a FindPeer. The
	// dial only lets identify refine the addresses for later callers; it does
	// not gate the answer.
	DefaultFindPeerDialTimeout = 5 * time.Second
)

// newDefaultCrawler builds the crawler fullrt would have built for itself.
// Supplying fullrt.WithCrawler replaces that default, so the snapshot wrapper
// has to rebuild it with the same parallelism: see crawler.NewDefaultCrawler(h,
// crawler.WithParallelism(200)) in go-libp2p-kad-dht fullrt/dht.go (v0.42.1
// lines 184-189).
//
// It is a variable only so tests can inject a crawler that does not dial;
// nothing in production reassigns it.
var newDefaultCrawler = func(h host.Host) (crawler.Crawler, error) {
	return crawler.NewDefaultCrawler(h, crawler.WithParallelism(200))
}

// replayAwareRouteTableFilter widens fullrt's default route table filter for
// the peers a snapshot replay reports, and only for those.
//
// fullrt's default, dht.PublicRoutingTableFilter, keeps a peer only while the
// host has an open connection to it. A crawl satisfies that because the crawler
// has just dialled every peer it reports. A replay dials nothing - that is the
// whole point of it - so without this every replayed peer is dropped and the
// routing table stays empty.
//
// A replayed peer is therefore judged on the rest of what the default asks: a
// public, non-relay address in the peerstore. The snapshot only ever holds
// public addresses and the replay writes them back before reporting the peer,
// so this accepts exactly the peers the file vouches for. Outside a replay the
// filter is the upstream default, unchanged, so real crawls build the table
// they always did.
func replayAwareRouteTableFilter(h host.Host, sc *snapshotCrawler) dht.RouteTableFilterFunc {
	return func(d any, p peer.ID) bool {
		if dht.PublicRoutingTableFilter(d, p) {
			return true
		}
		if !sc.isReplaying() {
			return false
		}
		for _, a := range h.Peerstore().Addrs(p) {
			if manet.IsPublicAddr(a) && !isRelayAddr(a) {
				return true
			}
		}
		return false
	}
}

// newBundledDHT builds the accelerated client: a standard DHT client that
// answers until the accelerated one has crawled, and the accelerated one
// itself. A positive crawlSnapshotMaxAge turns on the crawl snapshot described
// in dht_crawl_snapshot.go: the routing table is written to crawlSnapshotPath
// after every completed crawl and replayed at startup when the file is younger
// than that. At 0 the wrapper is not built at all and fullrt runs its own
// default crawler, exactly as it did before the snapshot existed.
func newBundledDHT(h host.Host, bootstrapAddrInfos []peer.AddrInfo, findPeerGrace, findPeerDialTimeout time.Duration, crawlSnapshotPath string, crawlSnapshotMaxAge time.Duration) (*bundledDHT, error) {
	standardDHT, err := dht.New(h, dht.Mode(dht.ModeClient), dht.BootstrapPeers(bootstrapAddrInfos...))
	if err != nil {
		return nil, err
	}

	fullRTOpts := []fullrt.Option{
		fullrt.DHTOption(
			dht.BucketSize(20),
			dht.Validator(record.NamespacedValidator{
				"pk":   record.PublicKeyValidator{},
				"ipns": ipns.Validator{},
			}),
			dht.BootstrapPeers(bootstrapAddrInfos...),
			dht.Mode(dht.ModeClient),
		),
		fullrt.WithFindPeerGrace(findPeerGrace),
		fullrt.WithFindPeerDialTimeout(findPeerDialTimeout),
	}

	var crawlSnapshot *snapshotCrawler
	if crawlSnapshotMaxAge > 0 {
		inner, err := newDefaultCrawler(h)
		if err != nil {
			standardDHT.Close()
			return nil, err
		}
		crawlSnapshot, err = newSnapshotCrawler(inner, h.Peerstore(), crawlSnapshotPath, crawlSnapshotMaxAge)
		if err != nil {
			standardDHT.Close()
			return nil, err
		}
		fullRTOpts = append(fullRTOpts,
			fullrt.WithCrawler(crawlSnapshot),
			fullrt.WithRouteTableFilter(replayAwareRouteTableFilter(h, crawlSnapshot)),
		)
	}

	fullRT, err := fullrt.NewFullRT(h, "/ipfs", fullRTOpts...)
	if err != nil {
		standardDHT.Close()
		return nil, err
	}

	b := &bundledDHT{
		standard:      standardDHT,
		fullRT:        fullRT,
		crawlSnapshot: crawlSnapshot,
	}

	if crawlSnapshot != nil {
		// A replayed table is as stale as the last completed crawl plus the
		// downtime, so a real crawl has to follow it immediately. The trigger
		// is sent on an unbuffered channel that fullrt reads only between
		// crawls, so it blocks until the replayed table is installed and then
		// starts a crawl seeded with the replayed peers: Ready first, then
		// refresh.
		ctx, cancel := context.WithCancel(context.Background())
		b.cancel = cancel
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			select {
			case <-crawlSnapshot.FirstRunDone():
			case <-ctx.Done():
				return
			}
			if !crawlSnapshot.Replayed() {
				return // a real crawl just ran; there is nothing to refresh
			}
			if err := fullRT.TriggerRefresh(ctx); err != nil {
				logger.Warnf("triggering a dht crawl after replaying the crawl snapshot: %v", err)
				return
			}
			logger.Infof("triggered a dht crawl to refresh the replayed routing table")
		}()
	}

	return b, nil
}

// Close stops both DHT clients. Since go-libp2p-kad-dht v0.42.0 the
// constructors no longer take a context, so cancelling the context that built
// them no longer shuts them down and Close is the only way to stop their
// long-lived goroutines.
func (b *bundledDHT) Close() error {
	if b.cancel != nil {
		// Stop the post-replay refresh first: it can be blocked sending a
		// trigger fullrt will never read once it is closed.
		b.cancel()
		b.wg.Wait()
	}
	return errors.Join(b.fullRT.Close(), b.standard.Close())
}

func (b *bundledDHT) getDHT() routing.Routing {
	if b.fullRT.Ready() {
		return b.fullRT
	}
	return b.standard
}

func (b *bundledDHT) Provide(ctx context.Context, c cid.Cid, brdcst bool) error {
	return b.getDHT().Provide(ctx, c, brdcst)
}

func (b *bundledDHT) FindProvidersAsync(ctx context.Context, c cid.Cid, i int) <-chan peer.AddrInfo {
	return b.getDHT().FindProvidersAsync(ctx, c, i)
}

func (b *bundledDHT) FindPeer(ctx context.Context, id peer.ID) (peer.AddrInfo, error) {
	return b.getDHT().FindPeer(ctx, id)
}

func (b *bundledDHT) PutValue(ctx context.Context, k string, v []byte, option ...routing.Option) error {
	return b.getDHT().PutValue(ctx, k, v, option...)
}

func (b *bundledDHT) GetValue(ctx context.Context, s string, option ...routing.Option) ([]byte, error) {
	return b.getDHT().GetValue(ctx, s, option...)
}

func (b *bundledDHT) SearchValue(ctx context.Context, s string, option ...routing.Option) (<-chan []byte, error) {
	return b.getDHT().SearchValue(ctx, s, option...)
}

func (b *bundledDHT) Bootstrap(ctx context.Context) error {
	return b.standard.Bootstrap(ctx)
}
