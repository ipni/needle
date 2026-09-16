package main

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ipfs/boxo/routing/http/types"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
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
