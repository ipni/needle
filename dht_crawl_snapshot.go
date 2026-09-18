// snapshotCrawler persists the accelerated DHT client's routing table to disk
// and replays it on the next start, so a restart is warm in seconds instead of
// after a full crawl.
//
// It works because fullrt builds its entire routing table out of what the
// crawler reports: for every handleSuccess(p, rtPeers) call, fullrt records p
// with whatever go-libp2p's peerstore holds for p at that moment, and ignores
// rtPeers. Once the crawl returns, that set becomes the routing table and
// Ready() is true as long as the crawl is within the crawl interval and the
// table is bigger than the bootstrap list. So a crawler whose first Run puts a
// saved snapshot's addresses into the peerstore and calls handleSuccess for
// each of its peers hands fullrt a full table in seconds, with no network
// activity.
//
// One thing stands in the way of that, and it is why needle pins the ipni
// fork of go-libp2p-kad-dht. fullrt runs every peer the crawler reports through
// a route table filter, whose default keeps a peer only while the host has an
// open connection to it - true of a peer the crawler just dialled, never true
// of a replayed one. The fork adds fullrt.WithRouteTableFilter so the caller
// can supply that filter; server_dht.go supplies one that judges a replayed
// peer on its addresses instead, and leaves real crawls on the default. Without
// it the replay runs and every peer it reports is silently dropped.
//
// Everything else here is the public option surface (fullrt.WithCrawler, the
// crawler.Crawler interface, FullRT.TriggerRefresh): a crawler wrapper, not a
// rewrite of kad-dht.
//
// A replayed table is exactly as stale as the last completed crawl plus the
// downtime, and nothing in the replay re-checks whether those peers are still
// reachable. The caller is therefore expected to trigger a real crawl
// immediately after a replay; the replayed addresses are written with a short
// TTL sized for that follow-up crawl to read them as its starting peers.
//
// The file is rewritten after every completed crawl rather than on a timer,
// because fullrt's table only changes when a crawl completes: between crawls
// there is nothing new to save.
//
// Four fullrt behaviours hold this up, none of them part of its documented
// contract, all of them worth re-checking the next time go-libp2p-kad-dht is
// bumped (line numbers are v0.42.2-ipni.2):
//
//  1. fullrt runs the route table filter synchronously inside handleSuccess
//     (fullrt/dht.go:408), which is what makes isReplaying() true for exactly
//     the peers a replay reports and no others.
//  2. It ignores rtPeers and records h.Peerstore().Addrs(p) at that moment
//     (fullrt/dht.go:415), which is why the replay must AddAddrs before it
//     calls handleSuccess, in that order.
//  3. It sets lastCrawlTime after every crawl, a replayed one included
//     (fullrt/dht.go:454), which is what lets Ready() flip without a dial.
//  4. crawler.Run extends each starting peer with h.Peerstore().Addrs(ai.ID)
//     and skips peers with none (crawler/crawler.go:222-235), which is the
//     whole reason replayed addresses carry crawlReplayAddrTTL rather than
//     being handed over and forgotten.
//
// If a bump breaks 1, replayed peers would be judged by the widened filter that
// only a replay should get. If it breaks 2, the table would come up with no
// addresses. If it breaks 3, Ready() would stay false and the replay would buy
// nothing. If it breaks 4, the crawl after a replay would start from the
// bootstrap peers alone, i.e. cold.

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-kad-dht/crawler"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// crawlSnapshotFormatVersion is the version written in every crawl snapshot's
// header line. Bump it when the on-disk format changes in a way a replay must
// handle differently; a replay ignores versions it does not understand.
const crawlSnapshotFormatVersion = 1

// crawlSnapshotMinPeers is the smallest crawl result worth keeping. A settled
// Amino DHT table is 10k-25k peers, so a crawl that finds far fewer is a
// broken crawl, not a small network: saving it would persist the breakage, and
// replaying it would make Ready() true for a table too thin to answer the
// lookups that follow. Below this, a crawl is not saved and a file is not
// replayed.
const crawlSnapshotMinPeers = 1000

// crawlReplayAddrTTL is how long replayed addresses live in the host
// peerstore. It has to outlast the real crawl the caller triggers right after
// a replay, because that crawl reads its starting peers' addresses from the
// peerstore and skips peers that have none; it is kept short so addresses that
// nothing re-confirms do not linger.
const crawlReplayAddrTTL = 10 * time.Minute

// crawlSnapshotTempPattern names the temp file a save writes before renaming
// it into place. A hard exit mid-save orphans it, so a load sweeps matches
// from the snapshot directory.
const crawlSnapshotTempPattern = ".dht-crawl-*.tmp"

// crawlSubsystem is the metric subsystem for everything in this file:
// needle_dht_crawl_*.
const crawlSubsystem = "dht_crawl"

const (
	crawlSnapshotOp     = "op"
	crawlSnapshotOpSave = "save"
	crawlSnapshotOpLoad = "load"
)

var (
	// crawlDurationSeconds and crawlPeers describe the crawl itself rather than
	// the snapshot, and are the only numbers anything reports about it: the
	// alternative is the fullrt logger's "crawl took" line, which production
	// silences. Like every gauge here they are registered at init and so are
	// always exported; only the wrapper sets them, so they read 0 until the
	// snapshot is enabled and a crawl has completed.
	crawlDurationSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Name:      "duration_seconds",
		Namespace: name,
		Subsystem: crawlSubsystem,
		Help:      "Duration of the last completed DHT crawl in seconds",
	})

	crawlPeers = promauto.NewGauge(prometheus.GaugeOpts{
		Name:      "peers",
		Namespace: name,
		Subsystem: crawlSubsystem,
		Help:      "Number of peers found by the last completed DHT crawl",
	})

	crawlSnapshotPeers = promauto.NewGauge(prometheus.GaugeOpts{
		Name:      "snapshot_peers",
		Namespace: name,
		Subsystem: crawlSubsystem,
		Help:      "Number of peers in the last successfully saved DHT crawl snapshot",
	})

	crawlSnapshotLastSuccessTimestampSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Name:      "snapshot_last_success_timestamp_seconds",
		Namespace: name,
		Subsystem: crawlSubsystem,
		Help:      "Unix timestamp of the last successful DHT crawl snapshot save",
	})

	crawlSnapshotRestoredPeers = promauto.NewGauge(prometheus.GaugeOpts{
		Name:      "snapshot_restored_peers",
		Namespace: name,
		Subsystem: crawlSubsystem,
		Help:      "Number of peers replayed from the DHT crawl snapshot at startup, 0 when nothing was replayed",
	})

	crawlSnapshotAgeSecondsAtRestore = promauto.NewGauge(prometheus.GaugeOpts{
		Name:      "snapshot_age_seconds_at_restore",
		Namespace: name,
		Subsystem: crawlSubsystem,
		Help:      "Age in seconds of the DHT crawl snapshot that was replayed at startup",
	})

	crawlSnapshotErrorsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "snapshot_errors",
		Namespace: name,
		Subsystem: crawlSubsystem,
		Help:      "Number of failed DHT crawl snapshot operations",
	}, []string{crawlSnapshotOp})
)

// publicDialableAddr reports whether an address is worth putting in a snapshot:
// public, and not a circuit-relay address. It is the one predicate the save,
// the load and the replay's route table filter all use, so the file holds
// exactly what the replay will accept back - a relay address passes
// manet.IsPublicAddr on the public IP it is prefixed with, so filtering on that
// alone would persist addresses the filter then rejects.
//
// It approximates rather than reproduces the address test in kad-dht's
// PublicRoutingTableFilter, which needle cannot call because kad-dht does not
// export it. That one starts with manet.ToIP and rejects anything without an
// IP, so it drops /dns4 and /dnsaddr addresses that this keeps, and its IPv6
// rules differ slightly. The effect is a replayed table that can hold a few
// peers a crawled table would not, which the crawl that follows a replay drops
// again.
func publicDialableAddr(a ma.Multiaddr) bool {
	return manet.IsPublicAddr(a) && !isRelayAddr(a)
}

// crawlSnapshotHeader is the first line of a crawl snapshot.
type crawlSnapshotHeader struct {
	Version int       `json:"v"`
	Time    time.Time `json:"t"`     // when the crawl that produced this file finished
	Peers   int       `json:"peers"` // number of entry lines that follow
}

// crawlSnapshotEntry is one line per peer in a crawl snapshot: the peer ID
// (base58, peer.ID's native JSON encoding) and its public addresses as
// multiaddr strings.
type crawlSnapshotEntry struct {
	ID    peer.ID  `json:"id"`
	Addrs []string `json:"a,omitempty"`
}

// crawlSnapshotPeer is a decoded crawlSnapshotEntry.
type crawlSnapshotPeer struct {
	id    peer.ID
	addrs []ma.Multiaddr
}

// snapshotCrawler wraps a crawler.Crawler with the snapshot described at the
// top of this file. Its zero value is not usable; construct it with
// newSnapshotCrawler.
type snapshotCrawler struct {
	inner  crawler.Crawler
	addrs  peerstore.AddrBook // the host peerstore's address book
	path   string
	maxAge time.Duration

	mu        sync.Mutex
	runs      int
	replayed  bool
	replaying bool
	lastPeers int
	lastDur   time.Duration

	// firstRunDone is closed when the first Run returns, whether or not it
	// replayed a snapshot.
	firstRunDone     chan struct{}
	firstRunDoneOnce sync.Once
}

var _ crawler.Crawler = (*snapshotCrawler)(nil)

// newSnapshotCrawler wraps inner so that the first crawl can be served from
// the snapshot at path, and every completed crawl is written back to it. addrs
// is the address book of the host the DHT runs on: a replay writes there, and
// a real crawl reads from there. A snapshot whose header time is older than
// maxAge is not replayed.
func newSnapshotCrawler(inner crawler.Crawler, addrs peerstore.AddrBook, path string, maxAge time.Duration) (*snapshotCrawler, error) {
	if inner == nil {
		return nil, errors.New("crawl snapshot: inner crawler must not be nil")
	}
	if addrs == nil {
		return nil, errors.New("crawl snapshot: address book must not be nil")
	}
	if path == "" {
		return nil, errors.New("crawl snapshot: path must not be empty")
	}
	if maxAge <= 0 {
		return nil, fmt.Errorf("crawl snapshot: max age must be positive, got %s", maxAge)
	}
	return &snapshotCrawler{
		inner:        inner,
		addrs:        addrs,
		path:         path,
		maxAge:       maxAge,
		firstRunDone: make(chan struct{}),
	}, nil
}

// Run implements crawler.Crawler. The first call replays a usable snapshot if
// there is one, reporting its peers through handleSuccess without touching the
// network and without running the inner crawler at all. Every other call, and
// the first when there is nothing usable to replay, runs the inner crawler and
// writes what it found back to the snapshot.
func (c *snapshotCrawler) Run(ctx context.Context, startingPeers []*peer.AddrInfo, handleSuccess crawler.HandleQueryResult, handleFail crawler.HandleQueryFail) {
	c.mu.Lock()
	c.runs++
	first := c.runs == 1
	c.mu.Unlock()

	if first {
		// Deferred so a panic in the inner crawler does not leave a caller
		// waiting on FirstRunDone forever.
		defer c.firstRunDoneOnce.Do(func() { close(c.firstRunDone) })
	}

	if first && c.replay(ctx, handleSuccess) {
		return
	}
	c.crawl(ctx, startingPeers, handleSuccess, handleFail)
}

// FirstRunDone is closed once the first Run has returned, whether it replayed
// a snapshot or ran a real crawl. A caller that wants to trigger a real crawl
// right after a replay waits on this first, so its refresh does not race the
// crawl fullrt starts at construction.
func (c *snapshotCrawler) FirstRunDone() <-chan struct{} { return c.firstRunDone }

// Replayed reports whether the first Run served the routing table from a
// snapshot instead of crawling.
func (c *snapshotCrawler) Replayed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.replayed
}

// isReplaying reports whether a replay is running right now. The caller's
// route table filter needs it: a replayed peer has to be judged on its
// addresses, because there is no connection to it, while a peer a real crawl
// reports must still face fullrt's unmodified default. handleSuccess, and so
// the filter, is called from the replay's own goroutine, so this is true for
// exactly the peers the replay reports.
func (c *snapshotCrawler) isReplaying() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.replaying
}

// lastCrawl returns the peer count and duration of the last real crawl that
// ran to completion. It is zero until one has, and a crawl cut short by a
// cancelled context does not update it, so a shutdown does not leave a
// truncated count behind.
func (c *snapshotCrawler) lastCrawl() (peers int, dur time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastPeers, c.lastDur
}

// replay reports a snapshot's peers through handleSuccess, having first put
// their addresses into the address book so fullrt records them with the
// addresses it reads back. It returns false when there is nothing usable to
// replay, in which case the caller must fall through to a real crawl.
func (c *snapshotCrawler) replay(ctx context.Context, handleSuccess crawler.HandleQueryResult) bool {
	start := time.Now()
	peers, age, ok := c.load()
	if !ok {
		return false
	}

	c.mu.Lock()
	c.replayed = true
	c.replaying = true
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.replaying = false
		c.mu.Unlock()
	}()
	crawlSnapshotAgeSecondsAtRestore.Set(age.Seconds())

	replayed := 0
	for _, p := range peers {
		select {
		case <-ctx.Done():
			// Shutdown, or a crawl the caller gave up on. Stop where we are:
			// the partial table is fullrt's problem to refresh, and the file
			// on disk is untouched either way.
			crawlSnapshotRestoredPeers.Set(float64(replayed))
			logger.Infof("dht crawl snapshot replay cancelled after %d of %d peers", replayed, len(peers))
			return true
		default:
		}
		c.addrs.AddAddrs(p.id, p.addrs, crawlReplayAddrTTL)
		handleSuccess(p.id, nil)
		replayed++
	}

	crawlSnapshotRestoredPeers.Set(float64(replayed))
	logger.Infof("replayed dht crawl snapshot: %d peers, snapshot age %s, replay duration %s",
		replayed, age.Round(time.Second), time.Since(start).Round(time.Millisecond))
	return true
}

// crawl runs the inner crawler and saves what it reported. handleSuccess is
// called before anything is recorded, so the snapshot never delays the routing
// table it is a copy of.
func (c *snapshotCrawler) crawl(ctx context.Context, startingPeers []*peer.AddrInfo, handleSuccess crawler.HandleQueryResult, handleFail crawler.HandleQueryFail) {
	start := time.Now()
	found := make(map[peer.ID][]ma.Multiaddr)

	c.inner.Run(ctx, startingPeers, func(p peer.ID, rtPeers []*peer.AddrInfo) {
		handleSuccess(p, rtPeers)

		// The crawler has just dialled p, so its addresses are in the
		// peerstore now. Only the ones a replay would accept are worth
		// persisting: fullrt filters the rest out of the routing table anyway,
		// and keeping them out of the file keeps them out of the peerstore on
		// the next replay.
		addrs := ma.FilterAddrs(c.addrs.Addrs(p), publicDialableAddr)
		if len(addrs) == 0 {
			return
		}
		c.mu.Lock()
		found[p] = addrs
		c.mu.Unlock()
	}, handleFail)

	// Taking the map under the lock orders this read after the last
	// handleSuccess; the crawler's own workers are done by the time Run
	// returns, so nothing writes to it after this point.
	c.mu.Lock()
	crawled := found
	c.mu.Unlock()

	dur := time.Since(start)
	if ctx.Err() != nil {
		// A crawl cut short by shutdown found only part of the network. Saving
		// it would replace a good file with a thin one.
		logger.Infof("dht crawl cancelled after %s with %d peers, not saving a snapshot", dur.Round(time.Millisecond), len(crawled))
		return
	}

	c.mu.Lock()
	c.lastPeers = len(crawled)
	c.lastDur = dur
	c.mu.Unlock()
	crawlDurationSeconds.Set(dur.Seconds())
	crawlPeers.Set(float64(len(crawled)))

	if len(crawled) < crawlSnapshotMinPeers {
		logger.Warnf("dht crawl found only %d peers, fewer than the %d needed for a snapshot; not saving", len(crawled), crawlSnapshotMinPeers)
		return
	}
	if err := c.save(crawled); err != nil {
		crawlSnapshotErrorsCounter.WithLabelValues(crawlSnapshotOpSave).Inc()
		logger.Warnf("saving dht crawl snapshot to %s: %v", c.path, err)
		return
	}
	crawlSnapshotPeers.Set(float64(len(crawled)))
	crawlSnapshotLastSuccessTimestampSeconds.Set(float64(time.Now().Unix()))
	logger.Infof("saved dht crawl snapshot of %d peers to %s, crawl duration %s", len(crawled), c.path, dur.Round(time.Millisecond))
}

// save writes peers to path as NDJSON: a header line, then one
// crawlSnapshotEntry per peer. It writes a temp file in the same directory and
// renames it into place, so a reader never sees a partial snapshot and a crash
// mid-save leaves the previous snapshot intact.
func (c *snapshotCrawler) save(peers map[peer.ID][]ma.Multiaddr) error {
	dir := filepath.Dir(c.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create snapshot directory %s: %w", dir, err)
	}

	tmp, err := os.CreateTemp(dir, crawlSnapshotTempPattern)
	if err != nil {
		return fmt.Errorf("create temp snapshot file: %w", err)
	}
	defer os.Remove(tmp.Name()) // removes the temp on any error; no-op after the rename below

	w := bufio.NewWriter(tmp)
	enc := json.NewEncoder(w)

	if err := enc.Encode(crawlSnapshotHeader{
		Version: crawlSnapshotFormatVersion,
		Time:    time.Now(),
		Peers:   len(peers),
	}); err != nil {
		tmp.Close()
		return fmt.Errorf("encode snapshot header: %w", err)
	}

	for p, addrs := range peers {
		strs := make([]string, 0, len(addrs))
		for _, a := range addrs {
			strs = append(strs, a.String())
		}
		if err := enc.Encode(crawlSnapshotEntry{ID: p, Addrs: strs}); err != nil {
			tmp.Close()
			return fmt.Errorf("encode snapshot entry for %s: %w", p, err)
		}
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
	if err := os.Rename(tmp.Name(), c.path); err != nil {
		return fmt.Errorf("rename snapshot into place: %w", err)
	}
	return nil
}

// load reads the snapshot at path. It reports ok=false, having logged why,
// when there is nothing usable to replay: no file yet, a file older than
// maxAge, one written in a format this build does not understand, one that
// cannot be parsed, or one holding fewer than crawlSnapshotMinPeers usable
// entries. A snapshot is never worth crashing over, so every failure here is a
// reason to crawl instead.
//
// It first removes orphaned temp files, which a hard exit mid-save leaves
// behind; those removals log their failures but never fail the load.
func (c *snapshotCrawler) load() (peers []crawlSnapshotPeer, age time.Duration, ok bool) {
	sweepOrphanedCrawlSnapshotTemps(c.path)

	f, err := os.Open(c.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			logger.Infof("no dht crawl snapshot at %s yet, crawling", c.path)
		} else {
			crawlSnapshotErrorsCounter.WithLabelValues(crawlSnapshotOpLoad).Inc()
			logger.Warnf("opening dht crawl snapshot %s: %v", c.path, err)
		}
		return nil, 0, false
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024) // 1 MiB max token

	if !scanner.Scan() {
		crawlSnapshotErrorsCounter.WithLabelValues(crawlSnapshotOpLoad).Inc()
		if err := scanner.Err(); err != nil {
			logger.Warnf("reading dht crawl snapshot header from %s: %v", c.path, err)
		} else {
			logger.Warnf("dht crawl snapshot %s is empty, no header line; crawling", c.path)
		}
		return nil, 0, false
	}
	var header crawlSnapshotHeader
	if err := json.Unmarshal(scanner.Bytes(), &header); err != nil {
		crawlSnapshotErrorsCounter.WithLabelValues(crawlSnapshotOpLoad).Inc()
		logger.Warnf("decoding dht crawl snapshot header from %s: %v", c.path, err)
		return nil, 0, false
	}
	if header.Version != crawlSnapshotFormatVersion {
		crawlSnapshotErrorsCounter.WithLabelValues(crawlSnapshotOpLoad).Inc()
		logger.Warnf("dht crawl snapshot %s has unsupported version %d, want %d; crawling", c.path, header.Version, crawlSnapshotFormatVersion)
		return nil, 0, false
	}
	age = time.Since(header.Time)
	if age > c.maxAge {
		logger.Infof("dht crawl snapshot %s is %s old, older than the %s limit; crawling", c.path, age.Round(time.Second), c.maxAge)
		return nil, 0, false
	}

	var malformed, noAddrs int
	for scanner.Scan() {
		var entry crawlSnapshotEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil || entry.ID == "" {
			// An absent id decodes to the zero peer.ID rather than a JSON
			// error, so it is rejected here rather than replayed as a peer
			// nothing can dial.
			malformed++
			continue
		}
		addrs := make([]ma.Multiaddr, 0, len(entry.Addrs))
		for _, s := range entry.Addrs {
			a, err := ma.NewMultiaddr(s)
			if err != nil {
				continue
			}
			addrs = append(addrs, a)
		}
		// A peer with no address to replay is worse than no entry at all: it
		// would enter the routing table with nothing to dial, and the crawl
		// that follows would skip it for lack of addresses. Filtered on load as
		// well as on save, so a file written by an older build - or by hand -
		// cannot smuggle in an address the replay would reject.
		addrs = ma.FilterAddrs(addrs, publicDialableAddr)
		if len(addrs) == 0 {
			noAddrs++
			continue
		}
		peers = append(peers, crawlSnapshotPeer{id: entry.ID, addrs: addrs})
	}
	if err := scanner.Err(); err != nil {
		crawlSnapshotErrorsCounter.WithLabelValues(crawlSnapshotOpLoad).Inc()
		logger.Warnf("reading dht crawl snapshot %s: %v", c.path, err)
		return nil, 0, false
	}
	if malformed > 0 || noAddrs > 0 {
		logger.Warnf("dht crawl snapshot %s: skipped %d malformed line(s) and %d peer(s) with no public address", c.path, malformed, noAddrs)
	}
	if len(peers) < crawlSnapshotMinPeers {
		logger.Warnf("dht crawl snapshot %s holds only %d usable peers, fewer than the %d needed to replay; crawling", c.path, len(peers), crawlSnapshotMinPeers)
		return nil, 0, false
	}
	return peers, age, true
}

// sweepOrphanedCrawlSnapshotTemps removes temp files matching
// crawlSnapshotTempPattern from the directory holding path. A save writes its
// snapshot to a temp file and renames it into place, so any matching file
// present at startup was orphaned by a hard exit mid-save; each is a full-size
// snapshot, and unclean restarts would accumulate them. Failures are logged,
// not returned: a stale temp file must not stop a replay.
func sweepOrphanedCrawlSnapshotTemps(path string) {
	dir := filepath.Dir(path)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			logger.Warnf("sweeping orphaned dht crawl snapshot temp files from %s: %v", dir, err)
		}
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		orphan, _ := filepath.Match(crawlSnapshotTempPattern, e.Name())
		if !orphan {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
			logger.Warnf("removing orphaned dht crawl snapshot temp file %s: %v", e.Name(), err)
		}
	}
}
