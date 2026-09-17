package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/routing/http/server"
	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-kad-dht/crawler"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multiaddr"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

const (
	mediaTypeJSON   = "application/json"
	mediaTypeNDJSON = "application/x-ndjson"

	cacheControlShortTTL = "public, max-age=15, stale-while-revalidate=60, stale-if-error=3600"
	cacheControlLongTTL  = "public, max-age=300, stale-while-revalidate=600, stale-if-error=172800"
)

func makeEd25519PeerID(t *testing.T) (crypto.PrivKey, peer.ID) {
	sk, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)

	pid, err := peer.IDFromPrivateKey(sk)
	require.NoError(t, err)

	return sk, pid
}

func requireCloseToNow(t *testing.T, lastModified string) {
	lastModifiedTime, err := time.Parse(http.TimeFormat, lastModified)
	require.NoError(t, err)
	require.WithinDuration(t, time.Now(), lastModifiedTime, 1*time.Minute)
}

func makePeerRecords(t *testing.T, count int) ([]iter.Result[*types.PeerRecord], []peer.ID) {
	var peerRecords []iter.Result[*types.PeerRecord]
	var peerIDs []peer.ID

	for i := range count {
		_, p := makeEd25519PeerID(t)
		peerIDs = append(peerIDs, p)

		addr := fmt.Sprintf("/ip4/127.0.0.%d/tcp/4001", i+1)
		ma, err := multiaddr.NewMultiaddr(addr)
		require.NoError(t, err)

		peerRecords = append(peerRecords, iter.Result[*types.PeerRecord]{
			Val: &types.PeerRecord{
				Schema: types.SchemaPeer,
				ID:     &p,
				Addrs:  []types.Multiaddr{{Multiaddr: ma}},
			},
		})
	}

	return peerRecords, peerIDs
}

func TestGetClosestPeersEndpoint(t *testing.T) {
	t.Parallel()

	makeRequest := func(t *testing.T, router router, contentType, key string) *http.Response {
		handler := server.Handler(&composableRouter{dht: router})
		srv := httptest.NewServer(handler)
		t.Cleanup(srv.Close)

		urlStr := fmt.Sprintf("http://%s/routing/v1/dht/closest/peers/%s", srv.Listener.Addr().String(), key)

		req, err := http.NewRequest(http.MethodGet, urlStr, nil)
		require.NoError(t, err)
		if contentType != "" {
			req.Header.Set("Accept", contentType)
		}
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		return resp
	}

	t.Run("GET /routing/v1/dht/closest/peers/{cid} returns 200 with 20 peers (JSON)", func(t *testing.T) {
		t.Parallel()

		_, pid := makeEd25519PeerID(t)
		key := peer.ToCid(pid)

		peerRecords, peerIDs := makePeerRecords(t, 20)
		results := iter.FromSlice(peerRecords)

		mockRouter := &mockDHTRouter{
			getClosestPeersFunc: func(ctx context.Context, k cid.Cid) (iter.ResultIter[*types.PeerRecord], error) {
				if k.Equals(key) {
					return results, nil
				}
				return nil, routing.ErrNotFound
			},
		}

		resp := makeRequest(t, mockRouter, mediaTypeJSON, key.String())
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, mediaTypeJSON, resp.Header.Get("Content-Type"))
		require.Equal(t, "Accept", resp.Header.Get("Vary"))
		require.Equal(t, cacheControlLongTTL, resp.Header.Get("Cache-Control"))

		requireCloseToNow(t, resp.Header.Get("Last-Modified"))

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		bodyStr := string(body)
		require.Contains(t, bodyStr, `"Peers":[`)
		// Verify all 20 peers and their addresses are present
		for i, p := range peerIDs {
			require.Contains(t, bodyStr, p.String())
			expectedAddr := fmt.Sprintf("/ip4/127.0.0.%d/tcp/4001", i+1)
			require.Contains(t, bodyStr, expectedAddr)
		}
	})

	t.Run("GET /routing/v1/dht/closest/peers/{cid} returns 200 with 20 peers (NDJSON)", func(t *testing.T) {
		t.Parallel()

		_, pid := makeEd25519PeerID(t)
		key := peer.ToCid(pid)

		peerRecords, peerIDs := makePeerRecords(t, 20)
		results := iter.FromSlice(peerRecords)

		mockRouter := &mockDHTRouter{
			getClosestPeersFunc: func(ctx context.Context, k cid.Cid) (iter.ResultIter[*types.PeerRecord], error) {
				if k.Equals(key) {
					return results, nil
				}
				return nil, routing.ErrNotFound
			},
		}

		resp := makeRequest(t, mockRouter, mediaTypeNDJSON, key.String())
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, mediaTypeNDJSON, resp.Header.Get("Content-Type"))
		require.Equal(t, "Accept", resp.Header.Get("Vary"))
		require.Equal(t, cacheControlLongTTL, resp.Header.Get("Cache-Control"))

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		bodyStr := string(body)
		// Verify all 20 peers and their addresses are present
		for i, p := range peerIDs {
			require.Contains(t, bodyStr, p.String())
			expectedAddr := fmt.Sprintf("/ip4/127.0.0.%d/tcp/4001", i+1)
			require.Contains(t, bodyStr, expectedAddr)
		}
	})

	t.Run("GET /routing/v1/dht/closest/peers/{cid} returns 200 with empty results (JSON)", func(t *testing.T) {
		t.Parallel()

		_, pid := makeEd25519PeerID(t)
		key := peer.ToCid(pid)

		results := iter.FromSlice([]iter.Result[*types.PeerRecord]{})

		mockRouter := &mockDHTRouter{
			getClosestPeersFunc: func(ctx context.Context, k cid.Cid) (iter.ResultIter[*types.PeerRecord], error) {
				if k.Equals(key) {
					return results, nil
				}
				return nil, routing.ErrNotFound
			},
		}

		resp := makeRequest(t, mockRouter, mediaTypeJSON, key.String())
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, mediaTypeJSON, resp.Header.Get("Content-Type"))
		require.Equal(t, cacheControlShortTTL, resp.Header.Get("Cache-Control"))

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, `{"Peers":null}`, string(body))
	})

	t.Run("GET /routing/v1/dht/closest/peers/{cid} returns 200 with empty results (NDJSON)", func(t *testing.T) {
		t.Parallel()

		_, pid := makeEd25519PeerID(t)
		key := peer.ToCid(pid)

		results := iter.FromSlice([]iter.Result[*types.PeerRecord]{})

		mockRouter := &mockDHTRouter{
			getClosestPeersFunc: func(ctx context.Context, k cid.Cid) (iter.ResultIter[*types.PeerRecord], error) {
				if k.Equals(key) {
					return results, nil
				}
				return nil, routing.ErrNotFound
			},
		}

		resp := makeRequest(t, mockRouter, mediaTypeNDJSON, key.String())
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, mediaTypeNDJSON, resp.Header.Get("Content-Type"))

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, "", string(body))
	})

	t.Run("GET /routing/v1/dht/closest/peers/{cid} returns 200 when router returns ErrNotFound", func(t *testing.T) {
		t.Parallel()

		_, pid := makeEd25519PeerID(t)
		key := peer.ToCid(pid)

		mockRouter := &mockDHTRouter{
			getClosestPeersFunc: func(ctx context.Context, k cid.Cid) (iter.ResultIter[*types.PeerRecord], error) {
				return nil, routing.ErrNotFound
			},
		}

		resp := makeRequest(t, mockRouter, mediaTypeJSON, key.String())
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, mediaTypeJSON, resp.Header.Get("Content-Type"))

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, `{"Peers":null}`, string(body))
	})

	t.Run("GET /routing/v1/dht/closest/peers/{invalid-key} returns 400", func(t *testing.T) {
		t.Parallel()

		mockRouter := &mockDHTRouter{}

		resp := makeRequest(t, mockRouter, mediaTypeJSON, "not-a-valid-cid")
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})

	t.Run("GET /routing/v1/dht/closest/peers/{arbitrary-cid} returns 200", func(t *testing.T) {
		t.Parallel()

		// arbitrary CID (not a PeerID)
		cidStr := "bafkreidcd7frenco2m6ch7mny63wztgztv3q6fctaffgowkro6kljre5ei"
		key, err := cid.Decode(cidStr)
		require.NoError(t, err)

		_, pid := makeEd25519PeerID(t)

		results := iter.FromSlice([]iter.Result[*types.PeerRecord]{
			{Val: &types.PeerRecord{
				Schema: types.SchemaPeer,
				ID:     &pid,
				Addrs:  []types.Multiaddr{},
			}},
		})

		mockRouter := &mockDHTRouter{
			getClosestPeersFunc: func(ctx context.Context, k cid.Cid) (iter.ResultIter[*types.PeerRecord], error) {
				if k.Equals(key) {
					return results, nil
				}
				return nil, routing.ErrNotFound
			},
		}

		resp := makeRequest(t, mockRouter, mediaTypeJSON, cidStr)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Contains(t, string(body), pid.String())
	})

	t.Run("GET /routing/v1/dht/closest/peers/{peerid-as-cid} returns 200", func(t *testing.T) {
		t.Parallel()

		_, pid := makeEd25519PeerID(t)
		key := peer.ToCid(pid)

		results := iter.FromSlice([]iter.Result[*types.PeerRecord]{
			{Val: &types.PeerRecord{
				Schema: types.SchemaPeer,
				ID:     &pid,
				Addrs:  []types.Multiaddr{},
			}},
		})

		mockRouter := &mockDHTRouter{
			getClosestPeersFunc: func(ctx context.Context, k cid.Cid) (iter.ResultIter[*types.PeerRecord], error) {
				if k.Equals(key) {
					return results, nil
				}
				return nil, routing.ErrNotFound
			},
		}

		resp := makeRequest(t, mockRouter, mediaTypeJSON, key.String())
		require.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Contains(t, string(body), pid.String())
	})

	t.Run("GET /routing/v1/dht/closest/peers with default Accept header returns JSON", func(t *testing.T) {
		t.Parallel()

		_, pid := makeEd25519PeerID(t)
		key := peer.ToCid(pid)

		results := iter.FromSlice([]iter.Result[*types.PeerRecord]{})

		mockRouter := &mockDHTRouter{
			getClosestPeersFunc: func(ctx context.Context, k cid.Cid) (iter.ResultIter[*types.PeerRecord], error) {
				return results, nil
			},
		}

		resp := makeRequest(t, mockRouter, "", key.String())
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, mediaTypeJSON, resp.Header.Get("Content-Type"))
	})

	t.Run("GET /routing/v1/dht/closest/peers with wildcard Accept header returns JSON", func(t *testing.T) {
		t.Parallel()

		_, pid := makeEd25519PeerID(t)
		key := peer.ToCid(pid)

		results := iter.FromSlice([]iter.Result[*types.PeerRecord]{})

		mockRouter := &mockDHTRouter{
			getClosestPeersFunc: func(ctx context.Context, k cid.Cid) (iter.ResultIter[*types.PeerRecord], error) {
				return results, nil
			},
		}

		resp := makeRequest(t, mockRouter, "text/html,*/*", key.String())
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, mediaTypeJSON, resp.Header.Get("Content-Type"))
	})

	t.Run("GET /routing/v1/dht/closest/peers/{cid} returns 500 when DHT is disabled", func(t *testing.T) {
		t.Parallel()

		_, pid := makeEd25519PeerID(t)
		key := peer.ToCid(pid)

		// Pass nil router to simulate DHT disabled via --dht=disabled
		handler := server.Handler(&composableRouter{dht: nil})
		srv := httptest.NewServer(handler)
		t.Cleanup(srv.Close)

		urlStr := fmt.Sprintf("http://%s/routing/v1/dht/closest/peers/%s", srv.Listener.Addr().String(), key.String())
		req, err := http.NewRequest(http.MethodGet, urlStr, nil)
		require.NoError(t, err)
		req.Header.Set("Accept", mediaTypeJSON)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)

		// Returns 500 Internal Server Error instead of misleading 200 with empty results
		require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Contains(t, string(body), "not supported")
	})
}

// mockDHTRouter implements the router interface for testing
type mockDHTRouter struct {
	getClosestPeersFunc func(ctx context.Context, key cid.Cid) (iter.ResultIter[*types.PeerRecord], error)
}

func (m *mockDHTRouter) FindProviders(ctx context.Context, key cid.Cid, limit int) (iter.ResultIter[types.Record], error) {
	return nil, routing.ErrNotSupported
}

func (m *mockDHTRouter) FindPeers(ctx context.Context, pid peer.ID, limit int) (iter.ResultIter[*types.PeerRecord], error) {
	return nil, routing.ErrNotSupported
}

func (m *mockDHTRouter) GetClosestPeers(ctx context.Context, key cid.Cid) (iter.ResultIter[*types.PeerRecord], error) {
	if m.getClosestPeersFunc != nil {
		return m.getClosestPeersFunc(ctx, key)
	}
	return nil, routing.ErrNotSupported
}

func (m *mockDHTRouter) GetIPNS(ctx context.Context, name ipns.Name) (*ipns.Record, error) {
	return nil, routing.ErrNotSupported
}

func (m *mockDHTRouter) PutIPNS(ctx context.Context, name ipns.Name, record *ipns.Record) error {
	return routing.ErrNotSupported
}

// fakeInnerCrawler stands in for crawler.DefaultCrawler inside newBundledDHT,
// so a test can exercise the crawl snapshot without dialling anything.
type fakeInnerCrawler struct {
	mu       sync.Mutex
	runs     int
	starting [][]*peer.AddrInfo
	// report is called with the crawl's handleSuccess, so a test can decide
	// what the crawl "finds".
	report func(handleSuccess crawler.HandleQueryResult)
}

func (f *fakeInnerCrawler) Run(_ context.Context, startingPeers []*peer.AddrInfo, handleSuccess crawler.HandleQueryResult, _ crawler.HandleQueryFail) {
	f.mu.Lock()
	f.runs++
	f.starting = append(f.starting, startingPeers)
	report := f.report
	f.mu.Unlock()

	if report != nil {
		report(handleSuccess)
	}
}

func (f *fakeInnerCrawler) runCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.runs
}

func (f *fakeInnerCrawler) lastStarting() []*peer.AddrInfo {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.starting) == 0 {
		return nil
	}
	return f.starting[len(f.starting)-1]
}

// useFakeInnerCrawler swaps the crawler newBundledDHT builds for a fake, for
// the duration of the test.
func useFakeInnerCrawler(t *testing.T, fake *fakeInnerCrawler) {
	t.Helper()
	prev := newDefaultCrawler
	newDefaultCrawler = func(host.Host) (crawler.Crawler, error) { return fake, nil }
	t.Cleanup(func() { newDefaultCrawler = prev })
}

// TestCrawlSnapshotReplayIsFilteredOutByFullRT records why replaying a
// snapshot does not, today, make the accelerated client ready.
//
// The replay does everything it is supposed to: the addresses land in the host
// peerstore and every saved peer is reported through handleSuccess. But fullrt
// runs each reported peer through kaddht.PublicRoutingTableFilter before it
// keeps it (fullrt/dht.go:370-373 in go-libp2p-kad-dht v0.42.1, unchanged in
// the ipni fork), and that filter starts with
//
//	conns := d.Host().Network().ConnsToPeer(p)
//	if len(conns) == 0 { return false }
//
// (dht_filters.go:87-105). A real crawl passes it because the crawler has just
// dialled the peer and the connection is still open; a replay never dials, so
// every replayed peer is dropped and the routing table stays empty.
//
// Nothing on fullrt's public option surface overrides that filter, so the
// wrapper cannot fix this from outside kad-dht. If a future kad-dht lets the
// route-table filter be supplied, this test fails and should be turned back
// into the Ready() assertion it was meant to be.
func TestCrawlSnapshotReplayIsFilteredOutByFullRT(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dht-crawl.ndjson")
	saved := genCrawlPeers(t, 1500)
	writeCrawlSnapshot(t, path, crawlSnapshotFormatVersion, time.Now().Add(-time.Minute), saved)

	fake := &fakeInnerCrawler{}
	useFakeInnerCrawler(t, fake)

	h, err := libp2p.New(libp2p.NoListenAddrs)
	require.NoError(t, err)
	t.Cleanup(func() { h.Close() })

	b, err := newBundledDHT(h, nil, DefaultFindPeerGrace, DefaultFindPeerDialTimeout, path, time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { b.Close() })
	require.NotNil(t, b.crawlSnapshot)

	// The replay itself ran, and left the addresses where the crawl that
	// follows will look for them.
	<-b.crawlSnapshot.FirstRunDone()
	require.True(t, b.crawlSnapshot.Replayed())
	require.Equal(t, float64(len(saved)), testutil.ToFloat64(crawlSnapshotRestoredPeers))
	require.Equal(t, saved[0].addrs[0].String(), h.Peerstore().Addrs(saved[0].id)[0].String())

	// The refresh the replay triggers does fire, and it is a real crawl.
	require.Eventually(t, func() bool { return fake.runCount() == 1 }, 10*time.Second, 50*time.Millisecond,
		"a replay must be followed by a real crawl")

	// It is seeded with nothing, though: fullrt seeds a refresh from the peers
	// the previous crawl kept, and it kept none of the replayed ones. In
	// production that leaves the bootstrap peers, which is a cold crawl.
	require.Empty(t, fake.lastStarting(), "the refresh is seeded from fullrt's table, which the filter left empty")

	// fullrt kept none of the replayed peers, so it is not ready.
	require.Zero(t, len(b.fullRT.Stat()), "fullrt drops every peer it has no open connection to")
	require.False(t, b.fullRT.Ready())
	require.Zero(t, len(h.Network().ConnsToPeer(saved[0].id)), "the replay dials nothing, which is the point of it")
}

func TestCrawlSnapshotDisabledUsesDefaultCrawler(t *testing.T) {
	fake := &fakeInnerCrawler{}
	useFakeInnerCrawler(t, fake)

	h, err := libp2p.New(libp2p.NoListenAddrs)
	require.NoError(t, err)
	t.Cleanup(func() { h.Close() })

	b, err := newBundledDHT(h, nil, DefaultFindPeerGrace, DefaultFindPeerDialTimeout, "", 0)
	require.NoError(t, err)
	t.Cleanup(func() { b.Close() })

	require.Nil(t, b.crawlSnapshot, "no wrapper when the snapshot is disabled")
	require.Nil(t, b.cancel, "no refresh goroutine when the snapshot is disabled")
	require.Zero(t, fake.runCount(), "fullrt builds its own crawler when the snapshot is disabled")
}
