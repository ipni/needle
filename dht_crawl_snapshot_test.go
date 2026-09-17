package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-kad-dht/crawler"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/stretchr/testify/require"
)

// fakeCrawler stands in for crawler.DefaultCrawler. It reports a configured
// set of peers through handleSuccess, writing each peer's addresses into book
// first, the way a real crawl's dials leave them in the host peerstore. It
// reports from several goroutines at once, because the real crawler calls
// handleSuccess from its worker pool and the wrapper has to be safe there.
type fakeCrawler struct {
	book   peerstore.AddrBook
	report []crawlSnapshotPeer
	fail   []peer.ID
	// block holds Run open until the context is cancelled, after everything
	// has been reported.
	block bool

	mu       sync.Mutex
	runs     int
	starting [][]*peer.AddrInfo
}

const fakeCrawlerWorkers = 4

func (f *fakeCrawler) Run(ctx context.Context, startingPeers []*peer.AddrInfo, handleSuccess crawler.HandleQueryResult, handleFail crawler.HandleQueryFail) {
	f.mu.Lock()
	f.runs++
	f.starting = append(f.starting, startingPeers)
	f.mu.Unlock()

	var wg sync.WaitGroup
	wg.Add(fakeCrawlerWorkers)
	for w := range fakeCrawlerWorkers {
		go func() {
			defer wg.Done()
			for i := w; i < len(f.report); i += fakeCrawlerWorkers {
				p := f.report[i]
				f.book.AddAddrs(p.id, p.addrs, time.Hour)
				handleSuccess(p.id, nil)
			}
		}()
	}
	wg.Wait()

	for _, p := range f.fail {
		handleFail(p, errors.New("dial failed"))
	}

	if f.block {
		<-ctx.Done()
	}
}

func (f *fakeCrawler) runCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.runs
}

func (f *fakeCrawler) startingPeers(run int) []*peer.AddrInfo {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.starting[run]
}

// genCrawlPeers returns n peers with one public address each.
func genCrawlPeers(t *testing.T, n int) []crawlSnapshotPeer {
	t.Helper()
	peers := make([]crawlSnapshotPeer, 0, n)
	for i := range n {
		peers = append(peers, crawlSnapshotPeer{
			id:    genPeerID(t),
			addrs: []ma.Multiaddr{ma.StringCast(fmt.Sprintf("/ip4/1.2.%d.%d/tcp/4001", i/255, i%255+1))},
		})
	}
	return peers
}

// writeCrawlSnapshot writes a snapshot by hand, so tests can control its
// header time, version, and contents.
func writeCrawlSnapshot(t *testing.T, path string, version int, at time.Time, peers []crawlSnapshotPeer) {
	t.Helper()

	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()

	enc := json.NewEncoder(f)
	require.NoError(t, enc.Encode(crawlSnapshotHeader{Version: version, Time: at, Peers: len(peers)}))
	for _, p := range peers {
		strs := make([]string, 0, len(p.addrs))
		for _, a := range p.addrs {
			strs = append(strs, a.String())
		}
		require.NoError(t, enc.Encode(crawlSnapshotEntry{ID: p.id, Addrs: strs}))
	}
}

// readCrawlSnapshot decodes a snapshot file: a header line, then one entry per
// peer, in file order.
func readCrawlSnapshot(t *testing.T, path string) (crawlSnapshotHeader, []crawlSnapshotEntry) {
	t.Helper()

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	require.True(t, scanner.Scan(), "snapshot must have a header line")
	var header crawlSnapshotHeader
	require.NoError(t, json.Unmarshal(scanner.Bytes(), &header))

	var entries []crawlSnapshotEntry
	for scanner.Scan() {
		var e crawlSnapshotEntry
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &e))
		entries = append(entries, e)
	}
	require.NoError(t, scanner.Err())
	return header, entries
}

func entryIDs(entries []crawlSnapshotEntry) map[peer.ID]struct{} {
	ids := make(map[peer.ID]struct{}, len(entries))
	for _, e := range entries {
		ids[e.ID] = struct{}{}
	}
	return ids
}

// newTestSnapshotCrawler wires a fake crawler and an in-memory address book to
// a snapshot at path.
func newTestSnapshotCrawler(t *testing.T, path string, maxAge time.Duration, report []crawlSnapshotPeer) (*snapshotCrawler, *fakeCrawler, peerstore.AddrBook) {
	t.Helper()
	book := pstoremem.NewAddrBook()
	t.Cleanup(func() { book.Close() })
	fake := &fakeCrawler{book: book, report: report}
	sc, err := newSnapshotCrawler(fake, book, path, maxAge)
	require.NoError(t, err)
	return sc, fake, book
}

// countingSuccess counts handleSuccess calls and records the peers reported.
func countingSuccess(mu *sync.Mutex, got *[]peer.ID) crawler.HandleQueryResult {
	return func(p peer.ID, _ []*peer.AddrInfo) {
		mu.Lock()
		defer mu.Unlock()
		*got = append(*got, p)
	}
}

// TestCrawlSnapshotRelayAddrsAreNotSaved holds the predicate the save, the load
// and the replay's route table filter share. A circuit-relay address carries a
// public IP prefix, so manet.IsPublicAddr alone passes it: filtering on that
// would write addresses to the file that the replay then rejects, inflating the
// peer count that the minimum and the restored-peers gauge are read from.
func TestCrawlSnapshotRelayAddrsAreNotSaved(t *testing.T) {
	relay := ma.StringCast("/ip4/1.2.3.4/tcp/4001/p2p/12D3KooWCZ67sU8oCvKd82Y6c9NgpqgoZYuZEUcg4upHCjK3n1aj/p2p-circuit")
	require.True(t, manet.IsPublicAddr(relay), "a relay addr looks public, which is the trap")
	require.False(t, publicDialableAddr(relay), "but it is not one the replay would accept")
	require.True(t, publicDialableAddr(ma.StringCast("/ip4/1.2.3.4/tcp/4001")))
	require.False(t, publicDialableAddr(ma.StringCast("/ip4/10.0.0.1/tcp/4001")))

	dir := t.TempDir()
	path := filepath.Join(dir, "dht-crawl.ndjson")

	public := genCrawlPeers(t, crawlSnapshotMinPeers)
	relayOnly := crawlSnapshotPeer{id: genPeerID(t), addrs: []ma.Multiaddr{relay}}
	sc, _, _ := newTestSnapshotCrawler(t, path, time.Hour, append(append([]crawlSnapshotPeer{}, public...), relayOnly))

	sc.Run(context.Background(), nil, func(peer.ID, []*peer.AddrInfo) {}, func(peer.ID, error) {})

	header, entries := readCrawlSnapshot(t, path)
	require.Equal(t, len(public), header.Peers)
	require.NotContains(t, entryIDs(entries), relayOnly.id, "a peer with only a relay address is not saved")

	// And the same peer in a hand-written file is dropped on load rather than
	// replayed into the peerstore for the filter to reject later.
	loadPath := filepath.Join(t.TempDir(), "dht-crawl.ndjson")
	writeCrawlSnapshot(t, loadPath, crawlSnapshotFormatVersion, time.Now(), append(append([]crawlSnapshotPeer{}, public...), relayOnly))
	sc2, _, book := newTestSnapshotCrawler(t, loadPath, time.Hour, nil)
	var replayed []peer.ID
	var mu sync.Mutex
	sc2.Run(context.Background(), nil, countingSuccess(&mu, &replayed), func(peer.ID, error) {})
	require.Len(t, replayed, len(public))
	require.Empty(t, book.Addrs(relayOnly.id), "a relay-only peer never reaches the peerstore")
}

func TestCrawlSnapshotColdStartCrawlsAndSaves(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dht-crawl.ndjson")

	public := genCrawlPeers(t, crawlSnapshotMinPeers)
	private := []crawlSnapshotPeer{
		{id: genPeerID(t), addrs: []ma.Multiaddr{ma.StringCast("/ip4/10.0.0.1/tcp/4001")}},
		{id: genPeerID(t), addrs: []ma.Multiaddr{ma.StringCast("/ip4/192.168.1.7/tcp/4001")}},
	}
	sc, fake, _ := newTestSnapshotCrawler(t, path, time.Hour, append(append([]crawlSnapshotPeer{}, public...), private...))

	var mu sync.Mutex
	var got []peer.ID
	sc.Run(context.Background(), nil, countingSuccess(&mu, &got), func(peer.ID, error) {})

	require.Equal(t, 1, fake.runCount(), "a cold start must run the real crawler")
	require.False(t, sc.Replayed())
	require.Len(t, got, len(public)+len(private), "every crawled peer reaches the real handleSuccess")
	select {
	case <-sc.FirstRunDone():
	default:
		t.Fatal("FirstRunDone must be closed once the first Run returns")
	}

	header, entries := readCrawlSnapshot(t, path)
	require.Equal(t, crawlSnapshotFormatVersion, header.Version)
	require.Equal(t, len(public), header.Peers, "only public peers are saved")
	require.Len(t, entries, len(public))

	saved := entryIDs(entries)
	for _, p := range public {
		require.Contains(t, saved, p.id)
	}
	for _, p := range private {
		require.NotContains(t, saved, p.id, "a peer with only private addresses must not be saved")
	}

	peers, dur := sc.lastCrawl()
	require.Equal(t, len(public), peers)
	require.Positive(t, dur)
}

func TestCrawlSnapshotSaveIsAtomic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dht-crawl.ndjson")

	sc, _, _ := newTestSnapshotCrawler(t, path, time.Hour, genCrawlPeers(t, crawlSnapshotMinPeers))
	sc.Run(context.Background(), nil, func(peer.ID, []*peer.AddrInfo) {}, func(peer.ID, error) {})

	// The temp file a save writes is renamed into place, so nothing is left
	// beside the snapshot.
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Equal(t, filepath.Base(path), files[0].Name())
}

func TestCrawlSnapshotBelowMinimumIsNotSaved(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dht-crawl.ndjson")

	sc, fake, _ := newTestSnapshotCrawler(t, path, time.Hour, genCrawlPeers(t, 3))
	sc.Run(context.Background(), nil, func(peer.ID, []*peer.AddrInfo) {}, func(peer.ID, error) {})

	require.Equal(t, 1, fake.runCount())
	_, err := os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist, "a crawl this small is broken, not a small network")
}

func TestCrawlSnapshotBelowMinimumIsNotReplayed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dht-crawl.ndjson")
	writeCrawlSnapshot(t, path, crawlSnapshotFormatVersion, time.Now(), genCrawlPeers(t, 3))

	sc, fake, _ := newTestSnapshotCrawler(t, path, time.Hour, genCrawlPeers(t, 5))
	sc.Run(context.Background(), nil, func(peer.ID, []*peer.AddrInfo) {}, func(peer.ID, error) {})

	require.Equal(t, 1, fake.runCount(), "a file this small must be ignored and a real crawl run")
	require.False(t, sc.Replayed())
}

func TestCrawlSnapshotReplaysFreshFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dht-crawl.ndjson")

	saved := genCrawlPeers(t, 1500)
	writeCrawlSnapshot(t, path, crawlSnapshotFormatVersion, time.Now().Add(-time.Minute), saved)

	sc, fake, book := newTestSnapshotCrawler(t, path, time.Hour, genCrawlPeers(t, crawlSnapshotMinPeers))

	starting := []*peer.AddrInfo{{ID: genPeerID(t)}}
	var mu sync.Mutex
	var got []peer.ID
	sc.Run(context.Background(), starting, countingSuccess(&mu, &got), func(peer.ID, error) {})

	require.Zero(t, fake.runCount(), "a replay must not touch the network")
	require.True(t, sc.Replayed())
	require.Len(t, got, len(saved))

	reported := make(map[peer.ID]struct{}, len(got))
	for _, p := range got {
		reported[p] = struct{}{}
	}
	for _, p := range saved {
		require.Contains(t, reported, p.id)
		addrs := book.Addrs(p.id)
		require.Len(t, addrs, 1, "a replayed peer's addresses must be in the peerstore for the crawl that follows")
		require.Equal(t, p.addrs[0].String(), addrs[0].String())
	}
	require.NotContains(t, reported, starting[0].ID, "startingPeers are the crawler's input, not the replay's")

	peers, dur := sc.lastCrawl()
	require.Zero(t, peers, "a replay is not a crawl")
	require.Zero(t, dur)
}

func TestCrawlSnapshotCrawlAfterReplayRewritesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dht-crawl.ndjson")

	saved := genCrawlPeers(t, 1500)
	writeCrawlSnapshot(t, path, crawlSnapshotFormatVersion, time.Now().Add(-time.Minute), saved)
	oldHeader, _ := readCrawlSnapshot(t, path)

	crawled := genCrawlPeers(t, crawlSnapshotMinPeers)
	sc, fake, _ := newTestSnapshotCrawler(t, path, time.Hour, crawled)

	sc.Run(context.Background(), nil, func(peer.ID, []*peer.AddrInfo) {}, func(peer.ID, error) {})
	require.True(t, sc.Replayed())
	require.Zero(t, fake.runCount())

	// The refresh the caller triggers right after a replay.
	starting := []*peer.AddrInfo{{ID: genPeerID(t)}}
	sc.Run(context.Background(), starting, func(peer.ID, []*peer.AddrInfo) {}, func(peer.ID, error) {})
	require.Equal(t, 1, fake.runCount())
	require.Equal(t, starting, fake.startingPeers(0), "a delegated run passes startingPeers through untouched")

	header, entries := readCrawlSnapshot(t, path)
	require.True(t, header.Time.After(oldHeader.Time), "the crawl's result replaces the replayed file")
	require.Len(t, entries, len(crawled))

	ids := entryIDs(entries)
	for _, p := range crawled {
		require.Contains(t, ids, p.id)
	}
	require.NotContains(t, ids, saved[0].id, "the file holds the new crawl, not the replayed one")
}

func TestCrawlSnapshotStaleFileIsReplaced(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dht-crawl.ndjson")

	stale := genCrawlPeers(t, 1500)
	writeCrawlSnapshot(t, path, crawlSnapshotFormatVersion, time.Now().Add(-25*time.Hour), stale)

	crawled := genCrawlPeers(t, crawlSnapshotMinPeers)
	sc, fake, _ := newTestSnapshotCrawler(t, path, 24*time.Hour, crawled)

	sc.Run(context.Background(), nil, func(peer.ID, []*peer.AddrInfo) {}, func(peer.ID, error) {})

	require.Equal(t, 1, fake.runCount(), "a snapshot older than maxAge must not be replayed")
	require.False(t, sc.Replayed())

	_, entries := readCrawlSnapshot(t, path)
	require.Len(t, entries, len(crawled))
	require.NotContains(t, entryIDs(entries), stale[0].id)
}

func TestCrawlSnapshotUnusableFilesFallBackToCrawling(t *testing.T) {
	fresh := genCrawlPeers(t, 1500)

	for _, tc := range []struct {
		name  string
		write func(t *testing.T, path string)
	}{
		{
			name: "corrupt header",
			write: func(t *testing.T, path string) {
				require.NoError(t, os.WriteFile(path, []byte("not json at all\n"), 0o644))
			},
		},
		{
			name: "empty file",
			write: func(t *testing.T, path string) {
				require.NoError(t, os.WriteFile(path, nil, 0o644))
			},
		},
		{
			name: "truncated entry lines",
			write: func(t *testing.T, path string) {
				writeCrawlSnapshot(t, path, crawlSnapshotFormatVersion, time.Now(), fresh)
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(path, data[:len(data)/2], 0o644))
			},
		},
		{
			name: "wrong version",
			write: func(t *testing.T, path string) {
				writeCrawlSnapshot(t, path, crawlSnapshotFormatVersion+1, time.Now(), fresh)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "dht-crawl.ndjson")
			tc.write(t, path)

			sc, fake, _ := newTestSnapshotCrawler(t, path, time.Hour, genCrawlPeers(t, 5))
			require.NotPanics(t, func() {
				sc.Run(context.Background(), nil, func(peer.ID, []*peer.AddrInfo) {}, func(peer.ID, error) {})
			})

			require.Equal(t, 1, fake.runCount())
			require.False(t, sc.Replayed())
			<-sc.FirstRunDone()
		})
	}
}

func TestCrawlSnapshotCancelledCrawlKeepsPreviousFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dht-crawl.ndjson")

	good := genCrawlPeers(t, 1500)
	writeCrawlSnapshot(t, path, crawlSnapshotFormatVersion, time.Now(), good)
	before, err := os.ReadFile(path)
	require.NoError(t, err)

	book := pstoremem.NewAddrBook()
	t.Cleanup(func() { book.Close() })
	crawled := genCrawlPeers(t, crawlSnapshotMinPeers)
	fake := &fakeCrawler{book: book, report: crawled, block: true}
	sc, err := newSnapshotCrawler(fake, book, path, time.Hour)
	require.NoError(t, err)

	// Replay first, so the cancelled run below is a delegated crawl.
	sc.Run(context.Background(), nil, func(peer.ID, []*peer.AddrInfo) {}, func(peer.ID, error) {})
	require.True(t, sc.Replayed())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var reported atomic.Int64
	sc.Run(ctx, nil, func(peer.ID, []*peer.AddrInfo) {
		if reported.Add(1) == int64(len(crawled)) {
			cancel() // shutdown, with the crawl still in flight
		}
	}, func(peer.ID, error) {})

	require.Equal(t, 1, fake.runCount())
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after, "a crawl cut short must not overwrite a good snapshot")

	peers, dur := sc.lastCrawl()
	require.Zero(t, peers, "a cancelled crawl is not a crawl result")
	require.Zero(t, dur)
}

func TestCrawlSnapshotCancelledReplayStopsEarly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dht-crawl.ndjson")

	saved := genCrawlPeers(t, 1500)
	writeCrawlSnapshot(t, path, crawlSnapshotFormatVersion, time.Now(), saved)

	sc, fake, _ := newTestSnapshotCrawler(t, path, time.Hour, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const stopAfter = 10
	var replayed int
	sc.Run(ctx, nil, func(peer.ID, []*peer.AddrInfo) {
		replayed++
		if replayed == stopAfter {
			cancel()
		}
	}, func(peer.ID, error) {})

	require.Equal(t, stopAfter, replayed, "a cancelled replay stops where it is")
	require.Zero(t, fake.runCount(), "a cancelled replay does not fall back to crawling")
	select {
	case <-sc.FirstRunDone():
	default:
		t.Fatal("FirstRunDone must close even when the replay is cancelled")
	}
}

func TestCrawlSnapshotCrawlerValidation(t *testing.T) {
	book := pstoremem.NewAddrBook()
	t.Cleanup(func() { book.Close() })
	fake := &fakeCrawler{book: book}
	path := filepath.Join(t.TempDir(), "dht-crawl.ndjson")

	_, err := newSnapshotCrawler(nil, book, path, time.Hour)
	require.Error(t, err)

	_, err = newSnapshotCrawler(fake, nil, path, time.Hour)
	require.Error(t, err)

	_, err = newSnapshotCrawler(fake, book, "", time.Hour)
	require.Error(t, err)

	_, err = newSnapshotCrawler(fake, book, path, 0)
	require.Error(t, err)

	_, err = newSnapshotCrawler(fake, book, path, -time.Second)
	require.Error(t, err)

	sc, err := newSnapshotCrawler(fake, book, path, time.Hour)
	require.NoError(t, err)
	require.NotNil(t, sc)
}
