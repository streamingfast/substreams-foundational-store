package badger

import (
	"os"
	"testing"

	storelib "github.com/streamingfast/substreams-foundational-store/store"
	"github.com/stretchr/testify/require"
)

func TestReadyFlag(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "badger-ready-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	dsn, err := storelib.ParseDSN("badger://" + tempDir)
	require.NoError(t, err)

	open := func() *Store {
		s, err := NewStore(dsn, "type.googleapis.com/test.TestAccountOwner", 10, nil)
		require.NoError(t, err)
		return s
	}

	store := open()

	// Default: a store that was never marked ready is not ready.
	ready, err := store.IsReady()
	require.NoError(t, err)
	require.False(t, ready)

	// Mark ready and read it back.
	require.NoError(t, store.SetReady(true))
	ready, err = store.IsReady()
	require.NoError(t, err)
	require.True(t, ready)

	// The flag is persisted: reopen the database and it survives.
	require.NoError(t, store.Close())
	store = open()
	ready, err = store.IsReady()
	require.NoError(t, err)
	require.True(t, ready)

	// It can be turned back off.
	require.NoError(t, store.SetReady(false))
	ready, err = store.IsReady()
	require.NoError(t, err)
	require.False(t, ready)

	require.NoError(t, store.Close())
}
