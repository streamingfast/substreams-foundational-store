package badger

import (
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v3"
)

// readyKey is a reserved key used to persist the store readiness flag for the
// "remote-feed" ingest mode. It is prefixed with NUL bytes to avoid collisions
// with application keys (which are arbitrary user-provided bytes).
var readyKey = []byte("\x00\x00\x00\x00__remote_feed_ready__")

// SetReady persists the readiness state of the store and updates the in-memory
// cache. While the store is not ready, the gRPC read service reports
// block_reached = false.
func (s *Store) SetReady(ready bool) error {
	value := []byte{0}
	if ready {
		value[0] = 1
	}

	err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(readyKey, value)
	})
	if err != nil {
		return fmt.Errorf("failed to persist readiness flag: %w", err)
	}

	// Keep the in-memory cache in sync only after the write succeeds.
	s.ready.Store(ready)
	return nil
}

// IsReady reports whether the store has been marked ready, using the in-memory
// cache loaded at startup. The error return is kept for interface compatibility
// and is always nil.
func (s *Store) IsReady() (bool, error) {
	return s.ready.Load(), nil
}

// readReadyFromDB reads the persisted readiness flag directly from badger. A
// store that was never marked ready (no persisted flag) is considered not ready.
// Used once at startup to seed the in-memory cache.
func (s *Store) readReadyFromDB() (bool, error) {
	ready := false
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(readyKey)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			return err
		}
		return item.Value(func(val []byte) error {
			ready = len(val) > 0 && val[0] == 1
			return nil
		})
	})
	if err != nil {
		return false, fmt.Errorf("failed to read readiness flag: %w", err)
	}

	return ready, nil
}
