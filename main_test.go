package main

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSnapshotFlagConfig(t *testing.T) {
	t.Run("zero interval disables without a datadir", func(t *testing.T) {
		path, interval, err := snapshotFlagConfig("", 0)
		require.NoError(t, err)
		require.Empty(t, path)
		require.Zero(t, interval)
	})

	t.Run("positive interval requires a datadir", func(t *testing.T) {
		_, _, err := snapshotFlagConfig("", time.Minute)
		require.Error(t, err)
		require.Contains(t, err.Error(), "--cached-addr-book-snapshot-interval")
		require.Contains(t, err.Error(), "--datadir")
	})

	t.Run("positive interval with a datadir derives the path", func(t *testing.T) {
		path, interval, err := snapshotFlagConfig("/var/lib/someguy", time.Minute)
		require.NoError(t, err)
		require.Equal(t, filepath.Join("/var/lib/someguy", "cached-addr-book.ndjson"), path)
		require.Equal(t, time.Minute, interval)
	})

	t.Run("negative interval is rejected", func(t *testing.T) {
		_, _, err := snapshotFlagConfig("/var/lib/someguy", -time.Minute)
		require.Error(t, err)
	})
}
