package main

import (
	"context"
	"errors"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/go-cid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/fullrt"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
)

type bundledDHT struct {
	standard *dht.IpfsDHT
	fullRT   *fullrt.FullRT
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

func newBundledDHT(h host.Host, bootstrapAddrInfos []peer.AddrInfo, findPeerGrace, findPeerDialTimeout time.Duration) (routing.Routing, error) {
	standardDHT, err := dht.New(h, dht.Mode(dht.ModeClient), dht.BootstrapPeers(bootstrapAddrInfos...))
	if err != nil {
		return nil, err
	}

	fullRT, err := fullrt.NewFullRT(h, "/ipfs",
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
		fullrt.WithFindPeerDialTimeout(findPeerDialTimeout))
	if err != nil {
		return nil, err
	}

	return &bundledDHT{
		standard: standardDHT,
		fullRT:   fullRT,
	}, nil
}

// Close stops both DHT clients. Since go-libp2p-kad-dht v0.42.0 the
// constructors no longer take a context, so cancelling the context that built
// them no longer shuts them down and Close is the only way to stop their
// long-lived goroutines.
func (b *bundledDHT) Close() error {
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
