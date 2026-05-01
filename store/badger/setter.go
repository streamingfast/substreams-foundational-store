package badger

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v3"
	pbmodel "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/model/v2"
)

// Set stores a single entry in Badger
func (s *Store) Set(entry *pbmodel.Entry, IfNotExist bool, blockNumber uint64) error {
	if IfNotExist {
		// Check if key already exists
		err := s.db.View(func(txn *badger.Txn) error {
			_, err := txn.Get(entry.Key.Bytes)
			return err
		})
		if err == nil {
			// Key exists, skip this entry
			return nil
		}
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return fmt.Errorf("failed to check existence of key: %w", err)
		}
		// Key doesn't exist, proceed with insertion
	}

	// Prepend block_number and block_hash as bytes to the value
	blockNumBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(blockNumBytes, blockNumber)

	// Combine block number, block hash, and value
	valueWithBlockInfo := blockNumBytes
	valueWithBlockInfo = append(valueWithBlockInfo, entry.Value.Value...)

	err := s.db.Update(func(txn *badger.Txn) error {
		// Use the entry.Key value with the combined value
		err := txn.Set(entry.Key.Bytes, valueWithBlockInfo)
		if err != nil {
			return fmt.Errorf("failed to set value in Badger: %w", err)
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to update Badger: %w", err)
	}

	return nil
}

// SetAll stores multiple entries in Badger
func (s *Store) SetAll(entries []*pbmodel.Entry, deletePrefixes []string, IfNotExist bool, blockNumber uint64) error {
	// Handle delete-prefix operations first, before writing new entries.
	if len(deletePrefixes) > 0 {
		if err := s.applyDeletePrefixes(deletePrefixes); err != nil {
			return fmt.Errorf("failed to apply delete prefixes: %w", err)
		}
	}

	if len(entries) == 0 {
		return nil
	}

	// Use a batch writer for better performance with multiple entries
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	for _, entry := range entries {
		if IfNotExist {
			// Check if key already exists
			err := s.db.View(func(txn *badger.Txn) error {
				_, err := txn.Get(entry.Key.Bytes)
				return err
			})
			if err == nil {
				// Key exists, skip this entry
				continue
			}
			if !errors.Is(err, badger.ErrKeyNotFound) {
				return fmt.Errorf("failed to check existence of key: %w", err)
			}
			// Key doesn't exist, proceed with insertion
		}

		// Prepend block_number and block_hash as bytes to the value
		blockNumBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(blockNumBytes, blockNumber)

		// Combine block number, block hash, and value
		valueWithBlockInfo := blockNumBytes
		valueWithBlockInfo = append(valueWithBlockInfo, entry.Value.Value...)

		err := wb.Set(entry.Key.Bytes, valueWithBlockInfo)
		if err != nil {
			return fmt.Errorf("failed to add entry to batch: %w", err)
		}
	}

	err := wb.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush batch to Badger: %w", err)
	}

	return nil
}

// applyDeletePrefixes removes all keys from Badger that start with any of the given prefixes.
func (s *Store) applyDeletePrefixes(prefixes []string) error {
	for _, prefix := range prefixes {
		prefixBytes := []byte(prefix)
		err := s.db.Update(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = false
			it := txn.NewIterator(opts)
			defer it.Close()

			var keysToDelete [][]byte
			for it.Seek(prefixBytes); it.ValidForPrefix(prefixBytes); it.Next() {
				keysToDelete = append(keysToDelete, it.Item().KeyCopy(nil))
			}
			it.Close()

			for _, key := range keysToDelete {
				if err := txn.Delete(key); err != nil {
					return fmt.Errorf("failed to delete key %q: %w", key, err)
				}
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to delete prefix %q: %w", prefix, err)
		}
	}
	return nil
}
