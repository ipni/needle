package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// snapshotFormatVersion is the version written in every snapshot's header
// line. Bump it when the on-disk format changes in a way restore must handle
// differently; restore rejects versions it does not understand.
//
// The snapshot itself is an optional, off-by-default on-disk copy of the
// cached address book, so a restart does not start cold.
//
// We snapshot instead of backing the address book with go-libp2p's
// datastore-backed peerstore (pstoreds): it is deprecated, it writes a record
// to the datastore on every AddAddrs, and its PeersWithAddrs does a full
// datastore scan, which the probe loop triggers every ProbeInterval. A
// snapshot writes one file on a timer and on shutdown, and reads nothing in
// the hot path.
//
// pstoremem exposes no per-address expiry: Addrs returns bare multiaddrs and
// the expiring entry is unexported. The snapshot therefore cannot record each
// address's remaining TTL, so restore reconstructs TTLs from
// peerState.lastAddrWrite, the time the peer's addrs were last written.
//
// Reconstructing from a single per-peer write time over-extends the TTL of
// addrs written earlier than that: CacheAddrs union-adds addrs (AddAddrs only
// ever extends a TTL), so a peer's set can hold addrs from several write
// times, and all of them are re-anchored to the last one. The over-extension
// is bounded by the TTL itself, because any addr older than its TTL is already
// gone from Addrs at snapshot time, and the probe loop re-confirms or evicts
// each addr regardless, so the skew ages out quickly.
const snapshotFormatVersion = 1

const (
	snapshotOp     = "op"
	snapshotOpSave = "save"
	snapshotOpLoad = "load"
)

// snapshotTempPattern names the temp file a save writes before renaming it
// into place. A hard exit mid-save orphans it, so loadSnapshot sweeps
// matches from the snapshot directory.
const snapshotTempPattern = ".cached-addr-book-*.tmp"

var (
	snapshotDurationSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Name:      "snapshot_duration_seconds",
		Namespace: name,
		Subsystem: Subsystem,
		Help:      "Duration of the last address book snapshot save in seconds",
	})

	snapshotPeers = promauto.NewGauge(prometheus.GaugeOpts{
		Name:      "snapshot_peers",
		Namespace: name,
		Subsystem: Subsystem,
		Help:      "Number of peers in the last successful address book snapshot",
	})

	snapshotLastSuccessTimestampSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Name:      "snapshot_last_success_timestamp_seconds",
		Namespace: name,
		Subsystem: Subsystem,
		Help:      "Unix timestamp of the last successful address book snapshot save",
	})

	snapshotRestoredPeers = promauto.NewGauge(prometheus.GaugeOpts{
		Name:      "snapshot_restored_peers",
		Namespace: name,
		Subsystem: Subsystem,
		Help:      "Number of peers restored from the address book snapshot at startup",
	})

	snapshotRestoredAddrs = promauto.NewGauge(prometheus.GaugeOpts{
		Name:      "snapshot_restored_addrs",
		Namespace: name,
		Subsystem: Subsystem,
		Help:      "Number of addresses offered to the address book when restoring the snapshot at startup",
	})

	snapshotErrorsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "snapshot_errors",
		Namespace: name,
		Subsystem: Subsystem,
		Help:      "Number of failed address book snapshot operations",
	}, []string{snapshotOp})
)

// snapshotHeader is the first line of a snapshot.
type snapshotHeader struct {
	Version int       `json:"v"`
	Time    time.Time `json:"t"` // snapshot time; also the Written of connected peers
}

// snapshotEntry is one line per peer in a snapshot.
//
// On-disk format is NDJSON: one snapshotHeader line, then one snapshotEntry
// line per peer.
//
//	id  peer ID (base58). Always present.
//	a   addrs as multiaddr strings. Omitted when empty.
//	w   last write time. Omitted when zero.
//	c   connected at snapshot time. Omitted when false.
//	lc  last successful connection. Omitted when zero.
//	lf  last failed connection. Omitted when zero.
//	f   consecutive connection failures. Omitted when zero.
//
// w, lc, and lf use omitzero (not omitempty): encoding/json's omitempty has no
// effect on struct types like time.Time and would write every zero time as
// "0001-01-01T00:00:00Z". An omitted field unmarshals to a zero time, so this
// only changes what is written, not what restore reads.
type snapshotEntry struct {
	ID        peer.ID   `json:"id"`
	Addrs     []string  `json:"a,omitempty"`
	Written   time.Time `json:"w,omitzero"`
	Connected bool      `json:"c,omitempty"`
	LastConn  time.Time `json:"lc,omitzero"`
	LastFail  time.Time `json:"lf,omitzero"`
	Failures  uint      `json:"f,omitempty"`
}

// noteAddrWrite records that p's addrs were just written to addrBook, so the
// snapshot can reconstruct their TTLs from the write time. It is a no-op when
// the snapshot is disabled, so write paths may call it unconditionally
// without touching peerCache in the default configuration.
//
// Like the identify handler, it does a non-atomic Peek → mutate → Add, so a
// concurrent update to the same peer (e.g. RecordFailedConnection) can drop
// either this write time or the other's failure increment. The consequence is
// bounded: a lost increment, or a slightly stale write time that only
// under-extends a TTL on restore. This is the same race the package already
// accepts, so no lock is added.
func (cab *cachedAddrBook) noteAddrWrite(p peer.ID) {
	if cab.snapshotPath == "" {
		return
	}
	pState, exists := cab.peerCache.Peek(p)
	if !exists {
		pState = peerState{}
	}
	pState.lastAddrWrite = time.Now()
	cab.peerCache.Add(p, pState)
	peerStateSize.Set(float64(cab.peerCache.Len())) // update metric
}

// saveSnapshot writes the address book to snapshotPath as NDJSON: a header
// line, then one snapshotEntry per peer. It writes to a temp file in the same
// directory and renames it into place, so a reader never observes a partial
// snapshot. connected reports which peers are connected at snapshot time;
// their addrs are stamped with the snapshot time. It returns nil without
// writing anything when the snapshot is disabled.
func (cab *cachedAddrBook) saveSnapshot(connected func(peer.ID) bool) error {
	if cab.snapshotPath == "" {
		return nil
	}

	cab.snapshotMu.Lock()
	defer cab.snapshotMu.Unlock()
	return cab.saveSnapshotLocked(connected)
}

// saveSnapshotLocked runs the snapshot save and updates the snapshot metrics.
// Callers must hold snapshotMu. It does not check that the snapshot is
// enabled: it relies on the invariant that WithSnapshot is the sole setter
// of snapshotPath and snapshotInterval and rejects an empty path, so
// snapshotInterval > 0 implies snapshotPath != "".
func (cab *cachedAddrBook) saveSnapshotLocked(connected func(peer.ID) bool) (err error) {
	start := time.Now()
	defer func() {
		snapshotDurationSeconds.Set(time.Since(start).Seconds())
		if err != nil {
			snapshotErrorsCounter.WithLabelValues(snapshotOpSave).Inc()
		}
	}()

	// The union of peers with addrs and peers with tracked state: a peer can
	// be in peerCache without addrs (e.g. only failure history) and vice
	// versa.
	peers := make(map[peer.ID]struct{})
	for _, p := range cab.addrBook.PeersWithAddrs() {
		peers[p] = struct{}{}
	}
	for _, p := range cab.peerCache.Keys() {
		peers[p] = struct{}{}
	}

	dir := filepath.Dir(cab.snapshotPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create snapshot directory %s: %w", dir, err)
	}

	tmp, err := os.CreateTemp(dir, snapshotTempPattern)
	if err != nil {
		return fmt.Errorf("create temp snapshot file: %w", err)
	}
	defer os.Remove(tmp.Name()) // removes the temp on any error; no-op after the rename below

	w := bufio.NewWriter(tmp)
	enc := json.NewEncoder(w)

	header := snapshotHeader{Version: snapshotFormatVersion, Time: start}
	if err := enc.Encode(header); err != nil {
		tmp.Close()
		return fmt.Errorf("encode snapshot header: %w", err)
	}

	count := 0
	for p := range peers {
		state, _ := cab.peerCache.Peek(p)
		isConnected := connected(p)
		written := state.lastAddrWrite
		if isConnected {
			// A connected peer's addrs are live now, so they are stamped with
			// the snapshot time rather than their last write.
			written = start
		}

		entry := snapshotEntry{
			ID:        p,
			Written:   written,
			Connected: isConnected,
			LastConn:  state.lastConnTime,
			LastFail:  state.lastFailedConnTime,
			Failures:  state.connectFailures,
		}

		// Emit addrs only when their TTLs can be reconstructed. A peer with a
		// recorded write time can have its TTLs rebuilt from it, and a
		// connected peer always has one because it was just stamped above. A
		// disconnected peer with no write time (e.g. evicted from peerCache)
		// would restore addrs with no TTL, so keep only its state.
		if !written.IsZero() {
			addrs := cab.addrBook.Addrs(p)
			strs := make([]string, 0, len(addrs))
			for _, a := range addrs {
				strs = append(strs, a.String())
			}
			entry.Addrs = strs
		}

		if err := enc.Encode(entry); err != nil {
			tmp.Close()
			return fmt.Errorf("encode snapshot entry for %s: %w", p, err)
		}
		count++
	}

	if err := w.Flush(); err != nil {
		tmp.Close()
		return fmt.Errorf("flush snapshot: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync snapshot: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close snapshot: %w", err)
	}
	if err := os.Rename(tmp.Name(), cab.snapshotPath); err != nil {
		return fmt.Errorf("rename snapshot into place: %w", err)
	}

	snapshotPeers.Set(float64(count))
	snapshotLastSuccessTimestampSeconds.Set(float64(time.Now().Unix()))
	logger.Infof("saved address book snapshot of %d peers to %s in %s", count, cab.snapshotPath, time.Since(start).Round(time.Millisecond))
	return nil
}

// loadSnapshot loads a snapshot written by saveSnapshot into addrBook and
// peerCache, so a restart resumes from the cached state instead of starting
// cold. It first removes orphaned temp files matching snapshotTempPattern,
// which a hard exit mid-save leaves behind; those removals log their
// failures but never fail the load. It returns 0, 0, nil when the snapshot
// is disabled or the file does not exist yet. An unsupported version or a
// malformed header is an error having added nothing; malformed entry lines,
// including ones with no id, are skipped and counted. A line over the
// scanner's token limit stops the
// scan: that error returns the counts loaded so far with the entries read
// so far already applied to addrBook and peerCache.
//
// Restored addrs are re-anchored to the peer's recorded write time: direct
// addresses get recentlyConnectedTTL minus the age, relay addresses get
// relayAddrTTL minus the age, and an addr whose TTL has already run out is
// dropped. Restored addrs are unsigned: the next identify's ConsumePeerRecord
// replaces them, and nothing in someguy reads certifications, so signed
// envelopes are deliberately not persisted.
//
// pstoremem.AddAddrs drops unconnected addrs silently once the book holds
// 1,000,000 of them, so a large restore may be truncated; addrs is the count
// offered, not the count kept.
func (cab *cachedAddrBook) loadSnapshot() (peers, addrs int, err error) {
	if cab.snapshotPath == "" {
		return 0, 0, nil
	}

	sweepOrphanedSnapshotTemps(cab.snapshotPath)

	defer func() {
		// The gauges describe what the book actually holds, so they are set
		// on the error path too: a scanner error (e.g. a line over the token
		// limit) leaves every entry read before it applied, and reporting
		// zero for those would understate the book.
		snapshotRestoredPeers.Set(float64(peers))
		snapshotRestoredAddrs.Set(float64(addrs))
		if err != nil {
			snapshotErrorsCounter.WithLabelValues(snapshotOpLoad).Inc()
		}
	}()

	f, err := os.Open(cab.snapshotPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, 0, nil
		}
		return 0, 0, fmt.Errorf("open snapshot %s: %w", cab.snapshotPath, err)
	}
	defer f.Close()

	start := time.Now()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024) // 1 MiB max token

	if !scanner.Scan() {
		if scanner.Err() == nil {
			return 0, 0, fmt.Errorf("snapshot %s has no header line", cab.snapshotPath)
		}
		return 0, 0, fmt.Errorf("read snapshot header: %w", scanner.Err())
	}
	var header snapshotHeader
	if err := json.Unmarshal(scanner.Bytes(), &header); err != nil {
		return 0, 0, fmt.Errorf("decode snapshot header: %w", err)
	}
	if header.Version != snapshotFormatVersion {
		return 0, 0, fmt.Errorf("unsupported snapshot version %d, want %d", header.Version, snapshotFormatVersion)
	}

	now := start
	var malformed int
	for scanner.Scan() {
		var entry snapshotEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			malformed++
			continue
		}
		// An absent id decodes to the zero peer.ID rather than a JSON error,
		// so reject it here: it would claim an LRU slot and skew the counts.
		if entry.ID == "" {
			malformed++
			continue
		}
		peers++

		// Restore the state for every entry, including ones with no addrs:
		// the failure backoff is what stops a restart from re-dialing every
		// dead peer it has already given up on. lastAddrWrite is the file's
		// Written, not now, so a later snapshot does not refresh entries that
		// were already stale.
		cab.peerCache.Add(entry.ID, peerState{
			lastConnTime:       entry.LastConn,
			lastFailedConnTime: entry.LastFail,
			connectFailures:    entry.Failures,
			lastAddrWrite:      entry.Written,
		})

		if entry.Written.IsZero() {
			continue // no write time, so the addrs' TTLs cannot be reconstructed
		}
		age := now.Sub(entry.Written)
		// A Written in the future - clock skew, or a snapshot copied from
		// another box - would otherwise extend TTLs past their configured
		// maximum.
		if age < 0 {
			age = 0
		}

		parsed := make([]ma.Multiaddr, 0, len(entry.Addrs))
		for _, s := range entry.Addrs {
			a, err := ma.NewMultiaddr(s)
			if err != nil {
				continue
			}
			parsed = append(parsed, a)
		}
		direct, relayAddrs := splitRelayAddrs(parsed)
		if ttl := cab.recentlyConnectedTTL - age; ttl > 0 && len(direct) > 0 {
			cab.addrBook.AddAddrs(entry.ID, direct, ttl)
			addrs += len(direct)
		}
		if ttl := cab.relayAddrTTL - age; ttl > 0 && len(relayAddrs) > 0 {
			cab.addrBook.AddAddrs(entry.ID, relayAddrs, ttl)
			addrs += len(relayAddrs)
		}
	}
	// Update the gauge before the scanner error return: a scanner error
	// (e.g. a line over the token limit) leaves the entries read so far in
	// peerCache, so the gauge must reflect them even on that path.
	peerStateSize.Set(float64(cab.peerCache.Len())) // update metric
	if malformed > 0 {
		logger.Warnf("skipped %d malformed line(s) in snapshot %s", malformed, cab.snapshotPath)
	}
	if err := scanner.Err(); err != nil {
		return peers, addrs, fmt.Errorf("read snapshot %s: %w", cab.snapshotPath, err)
	}

	logger.Infof("restored address book snapshot: %d peers, %d addrs, snapshot age %s, load duration %s", peers, addrs, now.Sub(header.Time).Round(time.Millisecond), time.Since(start).Round(time.Millisecond))
	return peers, addrs, nil
}

// sweepOrphanedSnapshotTemps removes temp files matching snapshotTempPattern
// from the directory holding snapshotPath. A save writes its snapshot to a
// temp file and renames it into place, so any matching file present at
// startup was orphaned by a hard exit mid-save; each is a full-size
// snapshot, and unclean restarts would accumulate them. Failures are logged,
// not returned: a stale temp file must not block startup.
func sweepOrphanedSnapshotTemps(snapshotPath string) {
	dir := filepath.Dir(snapshotPath)
	entries, err := os.ReadDir(dir)
	if err != nil {
		logger.Warnf("sweeping orphaned snapshot temp files from %s: %v", dir, err)
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		orphan, _ := filepath.Match(snapshotTempPattern, e.Name())
		if !orphan {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
			logger.Warnf("removing orphaned snapshot temp file %s: %v", e.Name(), err)
		}
	}
}
