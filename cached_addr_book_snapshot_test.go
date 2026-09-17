package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ipfs/boxo/routing/http/types"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/host/eventbus"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// genPeerID returns a real, key-derived peer ID. Only such IDs round-trip
// through peer.ID's base58 JSON encoding; a raw string ID would not.
func genPeerID(t *testing.T) peer.ID {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(nil)
	require.NoError(t, err)
	id, err := peer.IDFromPrivateKey(priv)
	require.NoError(t, err)
	return id
}

// readSnapshot decodes a snapshot file by hand: a header line, then one
// snapshotEntry per peer, in file order.
func readSnapshot(t *testing.T, path string) (snapshotHeader, []snapshotEntry) {
	t.Helper()

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	scanner := bufio.NewScanner(f)
	require.True(t, scanner.Scan(), "snapshot must have a header line")
	var header snapshotHeader
	require.NoError(t, json.Unmarshal(scanner.Bytes(), &header))

	var entries []snapshotEntry
	for scanner.Scan() {
		var e snapshotEntry
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &e))
		entries = append(entries, e)
	}
	require.NoError(t, scanner.Err())
	return header, entries
}

func TestSaveSnapshotDisabledWritesNothing(t *testing.T) {
	cab, err := newCachedAddrBook(WithAllowPrivateIPs())
	require.NoError(t, err)
	require.Empty(t, cab.snapshotPath)

	p := genPeerID(t)
	cab.CacheAddrs(p, []types.Multiaddr{{Multiaddr: ma.StringCast("/ip4/1.2.3.4/tcp/4001")}})

	require.NoError(t, cab.saveSnapshot(func(peer.ID) bool { return false }))

	// With the snapshot disabled, noteAddrWrite must not have touched peerCache.
	require.Zero(t, cab.peerCache.Len())
}

func TestSaveSnapshotRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "addrbook.snap")

	cab, err := newCachedAddrBook(WithAllowPrivateIPs(), WithSnapshot(path, time.Minute))
	require.NoError(t, err)

	directPeer := genPeerID(t)
	relayPeer := genPeerID(t)
	failedPeer := genPeerID(t)

	directAddr := ma.StringCast("/ip4/1.2.3.4/tcp/4001")
	relayAddr := ma.StringCast("/ip4/5.6.7.8/tcp/4001/p2p/12D3KooWCZ67sU8oCvKd82Y6c9NgpqgoZYuZEUcg4upHCjK3n1aj/p2p-circuit")

	cab.CacheAddrs(directPeer, []types.Multiaddr{{Multiaddr: directAddr}})
	cab.CacheAddrs(relayPeer, []types.Multiaddr{{Multiaddr: relayAddr}})
	cab.RecordFailedConnection(failedPeer)

	require.NoError(t, cab.saveSnapshot(func(peer.ID) bool { return false }))

	header, entries := readSnapshot(t, path)
	require.Equal(t, snapshotFormatVersion, header.Version)
	require.False(t, header.Time.IsZero())
	require.Len(t, entries, 3)

	byID := make(map[peer.ID]snapshotEntry, len(entries))
	for _, e := range entries {
		byID[e.ID] = e
	}

	d, ok := byID[directPeer]
	require.True(t, ok)
	require.Equal(t, []string{directAddr.String()}, d.Addrs)
	require.False(t, d.Written.IsZero())
	require.False(t, d.Connected)

	r, ok := byID[relayPeer]
	require.True(t, ok)
	require.Equal(t, []string{relayAddr.String()}, r.Addrs)
	require.False(t, r.Written.IsZero())
	require.False(t, r.Connected)

	// The failed peer has no addrs, a failure count, and no write time.
	f, ok := byID[failedPeer]
	require.True(t, ok)
	require.Empty(t, f.Addrs)
	require.Equal(t, uint(1), f.Failures)
	require.True(t, f.Written.IsZero())
}

func TestSaveSnapshotConnectedPeerUsesSnapshotTime(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "addrbook.snap")

	cab, err := newCachedAddrBook(WithAllowPrivateIPs(), WithSnapshot(path, time.Minute))
	require.NoError(t, err)

	p := genPeerID(t)
	cab.CacheAddrs(p, []types.Multiaddr{{Multiaddr: ma.StringCast("/ip4/1.2.3.4/tcp/4001")}})

	// Report p as connected at snapshot time.
	require.NoError(t, cab.saveSnapshot(func(other peer.ID) bool { return other == p }))

	header, entries := readSnapshot(t, path)
	require.Len(t, entries, 1)
	e := entries[0]
	require.Equal(t, p, e.ID)
	require.True(t, e.Connected)
	require.Equal(t, []string{"/ip4/1.2.3.4/tcp/4001"}, e.Addrs)
	require.True(t, e.Written.Equal(header.Time), "connected peer's Written must equal the header Time")
}

func TestSaveSnapshotSuppressesAddrsWithoutWriteTime(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "addrbook.snap")

	cab, err := newCachedAddrBook(WithAllowPrivateIPs(), WithSnapshot(path, time.Minute))
	require.NoError(t, err)

	// Seed addrs directly so the peer has addrs in addrBook but no recorded
	// write time (it was never written through a tracked path).
	p := genPeerID(t)
	cab.addrBook.AddAddrs(p, []ma.Multiaddr{ma.StringCast("/ip4/1.2.3.4/tcp/4001")}, time.Hour)

	// Disconnected with no write time: the addrs' TTLs cannot be reconstructed,
	// so only the state is saved.
	require.NoError(t, cab.saveSnapshot(func(peer.ID) bool { return false }))
	_, entries := readSnapshot(t, path)
	require.Len(t, entries, 1)
	require.Empty(t, entries[0].Addrs)

	// The same peer reported connected: its addrs are live, so they are saved.
	require.NoError(t, cab.saveSnapshot(func(other peer.ID) bool { return other == p }))
	_, entries = readSnapshot(t, path)
	require.Len(t, entries, 1)
	require.True(t, entries[0].Connected)
	require.Equal(t, []string{"/ip4/1.2.3.4/tcp/4001"}, entries[0].Addrs)
}

func TestSaveSnapshotAtomicSingleFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "addrbook.snap")

	cab, err := newCachedAddrBook(WithAllowPrivateIPs(), WithSnapshot(path, time.Minute))
	require.NoError(t, err)

	cab.CacheAddrs(genPeerID(t), []types.Multiaddr{{Multiaddr: ma.StringCast("/ip4/1.2.3.4/tcp/4001")}})

	require.NoError(t, cab.saveSnapshot(func(peer.ID) bool { return false }))

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "addrbook.snap", entries[0].Name())
}

func TestBackgroundSnapshotSavesOnInterval(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cached-addr-book.ndjson")

	eventBus := eventbus.NewBus()
	mockHost := &mockHost{eventBus: eventBus}

	cab, err := newCachedAddrBook(WithAllowPrivateIPs(), WithSnapshot(path, 50*time.Millisecond))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go cab.background(ctx, mockHost, func(peer.ID) bool { return false })

	require.Eventually(t, func() bool {
		_, statErr := os.Stat(path)
		return statErr == nil
	}, time.Second*3, time.Millisecond*50, "snapshot file was not written within the interval")
}

func TestSaveSnapshotConcurrentCallsBothComplete(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cached-addr-book.ndjson")

	cab, err := newCachedAddrBook(WithAllowPrivateIPs(), WithSnapshot(path, time.Minute))
	require.NoError(t, err)
	cab.CacheAddrs(genPeerID(t), []types.Multiaddr{{Multiaddr: ma.StringCast("/ip4/1.2.3.4/tcp/4001")}})

	// Both saves start together, so the second is guaranteed to contend for
	// the lock: the lock serializes, it does not drop.
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- cab.saveSnapshot(func(peer.ID) bool { return false })
		}()
	}
	close(start)
	wg.Wait()

	require.NoError(t, <-results)
	require.NoError(t, <-results)

	// Contending saves leave exactly the snapshot in place, no temp behind.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	require.Equal(t, []string{"cached-addr-book.ndjson"}, names)
}

func TestLoadSnapshotSweepsOrphanedTempFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cached-addr-book.ndjson")

	orphan := filepath.Join(dir, ".cached-addr-book-12345.tmp")
	keep := filepath.Join(dir, "unrelated.txt")
	require.NoError(t, os.WriteFile(orphan, []byte("partial"), 0o600))
	require.NoError(t, os.WriteFile(keep, []byte("keep"), 0o600))

	cab, err := newCachedAddrBook(WithAllowPrivateIPs(), WithSnapshot(path, time.Minute))
	require.NoError(t, err)

	peers, addrs, err := cab.loadSnapshot()
	require.NoError(t, err)
	require.Zero(t, peers)
	require.Zero(t, addrs)

	_, statErr := os.Stat(orphan)
	require.True(t, os.IsNotExist(statErr))
	_, statErr = os.Stat(keep)
	require.NoError(t, statErr)
}

// writeSnapshotFile writes a snapshot by hand: a header line, then one
// snapshotEntry line per entry.
func writeSnapshotFile(t *testing.T, path string, header snapshotHeader, entries ...snapshotEntry) {
	t.Helper()

	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()

	enc := json.NewEncoder(f)
	require.NoError(t, enc.Encode(header))
	for _, e := range entries {
		require.NoError(t, enc.Encode(e))
	}
}

func maStrings(addrs []ma.Multiaddr) []string {
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, a.String())
	}
	return out
}

func TestLoadSnapshotRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "addrbook.snap")

	opts := []AddrBookOption{WithAllowPrivateIPs(), WithSnapshot(path, time.Minute)}

	cab1, err := newCachedAddrBook(opts...)
	require.NoError(t, err)

	directPeer := genPeerID(t)
	relayPeer := genPeerID(t)
	failedPeer := genPeerID(t)

	directAddr := ma.StringCast("/ip4/1.2.3.4/tcp/4001")
	relayAddr := ma.StringCast("/ip4/5.6.7.8/tcp/4001/p2p/12D3KooWCZ67sU8oCvKd82Y6c9NgpqgoZYuZEUcg4upHCjK3n1aj/p2p-circuit")

	cab1.CacheAddrs(directPeer, []types.Multiaddr{{Multiaddr: directAddr}})
	cab1.CacheAddrs(relayPeer, []types.Multiaddr{{Multiaddr: relayAddr}})
	cab1.RecordFailedConnection(failedPeer)
	cab1.RecordFailedConnection(failedPeer)

	require.NoError(t, cab1.saveSnapshot(func(peer.ID) bool { return false }))

	// Constructing cab2 with the same options loads the snapshot.
	cab2, err := newCachedAddrBook(opts...)
	require.NoError(t, err)

	// The constructor discards the load counts, so call loadSnapshot directly
	// and assert them here (the reload is idempotent).
	peers, addrs, err := cab2.loadSnapshot()
	require.NoError(t, err)
	require.Equal(t, 3, peers)
	require.Equal(t, 2, addrs)

	require.Equal(t, []string{directAddr.String()}, maStrings(cab2.addrBook.Addrs(directPeer)))
	require.Equal(t, []string{relayAddr.String()}, maStrings(cab2.addrBook.Addrs(relayPeer)))
	require.Empty(t, cab2.addrBook.Addrs(failedPeer))

	// The restored failure backoff must suppress re-dialing the dead peer.
	require.False(t, cab2.ShouldProbePeer(failedPeer))

	for _, p := range []peer.ID{directPeer, relayPeer, failedPeer} {
		s1, ok1 := cab1.peerCache.Peek(p)
		s2, ok2 := cab2.peerCache.Peek(p)
		require.True(t, ok1)
		require.True(t, ok2)
		// Compare instants, not representations: require.Equal would also
		// compare the monotonic clock reading and the location pointer.
		require.True(t, s1.lastConnTime.Equal(s2.lastConnTime))
		require.True(t, s1.lastFailedConnTime.Equal(s2.lastFailedConnTime))
		require.Equal(t, s1.connectFailures, s2.connectFailures)
		require.True(t, s1.lastAddrWrite.Equal(s2.lastAddrWrite))
	}
}

func TestLoadSnapshotExpiresStaleAddrs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "addrbook.snap")

	p := genPeerID(t)
	written := time.Now().Add(-49 * time.Hour) // older than the 48h recentlyConnectedTTL
	writeSnapshotFile(t, path,
		snapshotHeader{Version: snapshotFormatVersion, Time: written},
		snapshotEntry{ID: p, Addrs: []string{"/ip4/1.2.3.4/tcp/4001"}, Written: written},
	)

	cab, err := newCachedAddrBook(WithAllowPrivateIPs(), WithSnapshot(path, time.Minute))
	require.NoError(t, err)

	require.Empty(t, cab.addrBook.Addrs(p))
	state, ok := cab.peerCache.Peek(p)
	require.True(t, ok)
	require.True(t, state.lastAddrWrite.Equal(written))
}

func TestLoadSnapshotDropsExpiredRelayAddrs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "addrbook.snap")

	p := genPeerID(t)
	written := time.Now().Add(-3 * time.Hour) // older than the 2h relayAddrTTL, younger than the 48h direct TTL
	directAddr := ma.StringCast("/ip4/1.2.3.4/tcp/4001")
	relayAddr := ma.StringCast("/ip4/5.6.7.8/tcp/4001/p2p/12D3KooWCZ67sU8oCvKd82Y6c9NgpqgoZYuZEUcg4upHCjK3n1aj/p2p-circuit")
	writeSnapshotFile(t, path,
		snapshotHeader{Version: snapshotFormatVersion, Time: written},
		snapshotEntry{ID: p, Addrs: []string{directAddr.String(), relayAddr.String()}, Written: written},
	)

	cab, err := newCachedAddrBook(WithAllowPrivateIPs(), WithSnapshot(path, time.Minute))
	require.NoError(t, err)

	require.Equal(t, []string{directAddr.String()}, maStrings(cab.addrBook.Addrs(p)))
}

func TestLoadSnapshotMissingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "addrbook.snap")

	cab, err := newCachedAddrBook(WithAllowPrivateIPs(), WithSnapshot(path, time.Minute))
	require.NoError(t, err)

	require.Zero(t, cab.peerCache.Len())
	require.Empty(t, cab.addrBook.PeersWithAddrs())

	peers, addrs, err := cab.loadSnapshot()
	require.NoError(t, err)
	require.Zero(t, peers)
	require.Zero(t, addrs)
}

func TestLoadSnapshotSkipsMalformedLines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "addrbook.snap")

	p := genPeerID(t)
	header, err := json.Marshal(snapshotHeader{Version: snapshotFormatVersion, Time: time.Now()})
	require.NoError(t, err)
	entry, err := json.Marshal(snapshotEntry{
		ID:      p,
		Addrs:   []string{"/ip4/1.2.3.4/tcp/4001"},
		Written: time.Now(),
	})
	require.NoError(t, err)

	// A garbage line and an entry missing its id, around one valid entry.
	content := fmt.Sprintf("%s\nnot json\n{\"a\":[\"/ip4/9.9.9.9/tcp/4001\"]}\n%s\n", header, entry)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	cab, err := newCachedAddrBook(WithAllowPrivateIPs(), WithSnapshot(path, time.Minute))
	require.NoError(t, err)

	// A malformed line is skipped, not fatal: the valid entry still loads,
	// and the id-less entry claimed no LRU slot.
	require.Equal(t, []string{"/ip4/1.2.3.4/tcp/4001"}, maStrings(cab.addrBook.Addrs(p)))
	require.Equal(t, 1, cab.peerCache.Len())

	peers, addrs, err := cab.loadSnapshot()
	require.NoError(t, err)
	require.Equal(t, 1, peers)
	require.Equal(t, 1, addrs)
	require.Equal(t, float64(peers), testutil.ToFloat64(snapshotRestoredPeers))
	require.Equal(t, float64(addrs), testutil.ToFloat64(snapshotRestoredAddrs))
}

func TestLoadSnapshotCorrupt(t *testing.T) {
	t.Run("unsupported version", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "addrbook.snap")
		p := genPeerID(t)
		writeSnapshotFile(t, path,
			snapshotHeader{Version: 99, Time: time.Now()},
			snapshotEntry{ID: p, Addrs: []string{"/ip4/1.2.3.4/tcp/4001"}},
		)

		cab, err := newCachedAddrBook(WithAllowPrivateIPs(), WithSnapshot(path, time.Minute))
		require.NoError(t, err) // the constructor swallows load errors and continues cold

		_, _, err = cab.loadSnapshot()
		require.Error(t, err)
		require.Zero(t, cab.peerCache.Len())
		require.Empty(t, cab.addrBook.PeersWithAddrs())
	})

	t.Run("garbage header", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "addrbook.snap")
		require.NoError(t, os.WriteFile(path, []byte("not json at all\n"), 0o600))

		cab, err := newCachedAddrBook(WithAllowPrivateIPs(), WithSnapshot(path, time.Minute))
		require.NoError(t, err)

		_, _, err = cab.loadSnapshot()
		require.Error(t, err)
		require.Zero(t, cab.peerCache.Len())
		require.Empty(t, cab.addrBook.PeersWithAddrs())
	})
}

func TestLoadSnapshotConnectedEntry(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "addrbook.snap")

	p := genPeerID(t)
	written := time.Now().Add(-time.Hour)
	writeSnapshotFile(t, path,
		snapshotHeader{Version: snapshotFormatVersion, Time: written},
		snapshotEntry{ID: p, Addrs: []string{"/ip4/1.2.3.4/tcp/4001"}, Written: written, Connected: true},
	)

	cab, err := newCachedAddrBook(WithAllowPrivateIPs(), WithSnapshot(path, time.Minute))
	require.NoError(t, err)

	require.Equal(t, []string{"/ip4/1.2.3.4/tcp/4001"}, maStrings(cab.addrBook.Addrs(p)))
}

func TestWithSnapshotValidation(t *testing.T) {
	_, err := newCachedAddrBook(WithSnapshot("", time.Minute))
	require.Error(t, err)

	_, err = newCachedAddrBook(WithSnapshot("/some/path", 0))
	require.Error(t, err)

	_, err = newCachedAddrBook(WithSnapshot("/some/path", -time.Minute))
	require.Error(t, err)

	// A valid option succeeds and records its settings.
	dir := t.TempDir()
	want := filepath.Join(dir, "snap")
	cab, err := newCachedAddrBook(WithSnapshot(want, time.Minute))
	require.NoError(t, err)
	require.Equal(t, want, cab.snapshotPath)
	require.Equal(t, time.Minute, cab.snapshotInterval)
}

// A scanner error is the partial-apply path: the entries read before it are
// already in the book, so the restored gauges have to count them rather than
// reporting the zero of a failed load.
func TestLoadSnapshotPartialApplySetsGauges(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "addrbook.snap")

	p := genPeerID(t)
	header, err := json.Marshal(snapshotHeader{Version: snapshotFormatVersion, Time: time.Now()})
	require.NoError(t, err)
	entry, err := json.Marshal(snapshotEntry{
		ID:      p,
		Addrs:   []string{"/ip4/1.2.3.4/tcp/4001"},
		Written: time.Now(),
	})
	require.NoError(t, err)

	// A line past the scanner's 1 MiB token limit, after one good entry.
	huge := strings.Repeat("x", 2*1024*1024)
	content := fmt.Sprintf("%s\n%s\n%s\n", header, entry, huge)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	cab, err := newCachedAddrBook(WithAllowPrivateIPs(), WithSnapshot(path, time.Minute))
	require.NoError(t, err)

	peers, addrs, err := cab.loadSnapshot()
	require.Error(t, err) // the oversized line fails the scan
	require.Equal(t, 1, peers)
	require.Equal(t, 1, addrs)

	// The book holds what the counts say, and so do the gauges.
	require.Equal(t, []string{"/ip4/1.2.3.4/tcp/4001"}, maStrings(cab.addrBook.Addrs(p)))
	require.Equal(t, float64(peers), testutil.ToFloat64(snapshotRestoredPeers))
	require.Equal(t, float64(addrs), testutil.ToFloat64(snapshotRestoredAddrs))
}

// A Written in the future must not buy an entry a TTL beyond the configured
// maximum. The TTL is not readable through pstoremem, so it is asserted
// indirectly: present right after the load, gone once the configured TTL has
// elapsed.
func TestLoadSnapshotClampsFutureWriteTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "addrbook.snap")

		const ttl = time.Minute
		p := genPeerID(t)
		writeSnapshotFile(t, path,
			snapshotHeader{Version: snapshotFormatVersion, Time: time.Now()},
			snapshotEntry{
				ID:      p,
				Addrs:   []string{"/ip4/1.2.3.4/tcp/4001"},
				Written: time.Now().Add(time.Hour), // clock skew, or a foreign snapshot
			},
		)

		cab, err := newCachedAddrBook(
			WithAllowPrivateIPs(),
			WithRecentlyConnectedTTL(ttl),
			WithSnapshot(path, time.Minute),
		)
		require.NoError(t, err)
		defer cab.addrBook.(io.Closer).Close()

		require.Equal(t, []string{"/ip4/1.2.3.4/tcp/4001"}, maStrings(cab.addrBook.Addrs(p)))

		// Without the clamp the addr would have ttl+1h to live.
		time.Sleep(ttl + time.Second)
		require.Empty(t, cab.addrBook.Addrs(p), "a future Written must not extend the TTL")
	})
}

// The shutdown save waits for a periodic save rather than skipping it, so the
// snapshot left on disk when the process exits is the newer of the two.
func TestSaveSnapshotWaitsForInFlightSave(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cached-addr-book.ndjson")

	cab, err := newCachedAddrBook(WithAllowPrivateIPs(), WithSnapshot(path, time.Minute))
	require.NoError(t, err)
	cab.CacheAddrs(genPeerID(t), []types.Multiaddr{{Multiaddr: ma.StringCast("/ip4/1.2.3.4/tcp/4001")}})

	// The first save holds snapshotMu for as long as its connected callback
	// blocks, which is what a periodic save landing on shutdown looks like.
	release := make(chan struct{})
	entered := make(chan struct{})
	first := make(chan error, 1)
	var firstDone atomic.Bool
	go func() {
		var once sync.Once
		err := cab.saveSnapshot(func(peer.ID) bool {
			once.Do(func() { close(entered) })
			<-release
			return false
		})
		firstDone.Store(true)
		first <- err
	}()
	<-entered

	// The second save must not return while the first still holds the lock.
	second := make(chan error, 1)
	go func() { second <- cab.saveSnapshot(func(peer.ID) bool { return false }) }()
	select {
	case <-second:
		t.Fatal("the second save returned while the first still held the lock")
	case <-time.After(50 * time.Millisecond):
	}

	beforeRelease := time.Now()
	close(release)
	require.NoError(t, <-first)
	require.NoError(t, <-second)
	require.True(t, firstDone.Load(), "the second save returned before the first finished")

	// The file on disk is the second save's: its header is stamped when it
	// took the lock, which is after the first one let go.
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	var header snapshotHeader
	require.NoError(t, json.NewDecoder(f).Decode(&header))
	require.True(t, header.Time.After(beforeRelease),
		"the snapshot on disk is the earlier save's, so the later one was skipped")

	// And nothing was left behind mid-write.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		orphan, _ := filepath.Match(snapshotTempPattern, e.Name())
		require.False(t, orphan, "temp file %s left behind", e.Name())
	}
}
