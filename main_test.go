package main

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSnapshotFlagConfig(t *testing.T) {
	t.Run("zero interval disables without a datadir", func(t *testing.T) {
		path, interval, err := snapshotFlagConfig("", true, "accelerated", 0)
		require.NoError(t, err)
		require.Empty(t, path)
		require.Zero(t, interval)
	})

	t.Run("positive interval with the cached addr book disabled is rejected", func(t *testing.T) {
		_, _, err := snapshotFlagConfig("/var/lib/someguy", false, "accelerated", time.Minute)
		require.Error(t, err)
		require.Contains(t, err.Error(), "--cached-addr-book")
	})

	t.Run("positive interval with the DHT disabled is rejected", func(t *testing.T) {
		_, _, err := snapshotFlagConfig("/var/lib/someguy", true, "disabled", time.Minute)
		require.Error(t, err)
		require.Contains(t, err.Error(), "--dht")
	})

	t.Run("positive interval requires a datadir", func(t *testing.T) {
		_, _, err := snapshotFlagConfig("", true, "accelerated", time.Minute)
		require.Error(t, err)
		require.Contains(t, err.Error(), "--cached-addr-book-snapshot-interval")
		require.Contains(t, err.Error(), "--datadir")
	})

	t.Run("positive interval with a datadir derives the path", func(t *testing.T) {
		path, interval, err := snapshotFlagConfig("/var/lib/someguy", true, "accelerated", time.Minute)
		require.NoError(t, err)
		require.Equal(t, filepath.Join("/var/lib/someguy", "cached-addr-book.ndjson"), path)
		require.Equal(t, time.Minute, interval)
	})

	t.Run("negative interval is rejected", func(t *testing.T) {
		_, _, err := snapshotFlagConfig("/var/lib/someguy", true, "accelerated", -time.Minute)
		require.Error(t, err)
	})
}

func TestCrawlSnapshotFlagConfig(t *testing.T) {
	t.Run("zero max age disables without a datadir", func(t *testing.T) {
		path, maxAge, err := crawlSnapshotFlagConfig("", "accelerated", 0)
		require.NoError(t, err)
		require.Empty(t, path)
		require.Zero(t, maxAge)
	})

	t.Run("negative max age is rejected", func(t *testing.T) {
		_, _, err := crawlSnapshotFlagConfig("/var/lib/someguy", "accelerated", -time.Minute)
		require.Error(t, err)
	})

	t.Run("positive max age with the standard client is rejected", func(t *testing.T) {
		_, _, err := crawlSnapshotFlagConfig("/var/lib/someguy", "standard", time.Hour)
		require.Error(t, err)
		require.Contains(t, err.Error(), "--dht")
	})

	t.Run("positive max age with the DHT disabled is rejected", func(t *testing.T) {
		_, _, err := crawlSnapshotFlagConfig("/var/lib/someguy", "disabled", time.Hour)
		require.Error(t, err)
		require.Contains(t, err.Error(), "--dht")
	})

	t.Run("positive max age requires a datadir", func(t *testing.T) {
		_, _, err := crawlSnapshotFlagConfig("", "accelerated", time.Hour)
		require.Error(t, err)
		require.Contains(t, err.Error(), "--dht-crawl-snapshot-max-age")
		require.Contains(t, err.Error(), "--datadir")
	})

	t.Run("positive max age with a datadir derives the path", func(t *testing.T) {
		path, maxAge, err := crawlSnapshotFlagConfig("/var/lib/someguy", "accelerated", 2*time.Hour)
		require.NoError(t, err)
		require.Equal(t, filepath.Join("/var/lib/someguy", "dht-crawl.ndjson"), path)
		require.Equal(t, 2*time.Hour, maxAge)
	})
}
