package badger

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v3"
	pbmodel "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/model/v2"
	pbssinternal "github.com/streamingfast/substreams/pb/sf/substreams/intern/v2"
)

// Set stores a single entry in Badger
func (s *Store) Set(entry *pbmodel.Entry, IfNotExist bool, blockNumber uint64) error {
	if s.timeTraversalEnabled {
		return s.setWithTimeTraversal(entry, IfNotExist, blockNumber)
	}
	return s.setDirect(entry, IfNotExist, blockNumber)
}

// setDirect stores a single entry using direct key storage (original behavior)
func (s *Store) setDirect(entry *pbmodel.Entry, IfNotExist bool, blockNumber uint64) error {
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

// setWithTimeTraversal stores a single entry using time traversal key encoding
func (s *Store) setWithTimeTraversal(entry *pbmodel.Entry, IfNotExist bool, blockNumber uint64) error {
	if IfNotExist {
		// For time traversal, check if any version of this key exists
		err := s.db.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = 10
			it := txn.NewIterator(opts)
			defer it.Close()

			prefix := entry.Key.Bytes
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				// If we find any key with this prefix, it means the key exists
				return nil
			}
			return badger.ErrKeyNotFound
		})
		if err == nil {
			// Key exists, skip this entry
			return nil
		}
		if err != badger.ErrKeyNotFound {
			return fmt.Errorf("failed to check existence of key: %w", err)
		}
		// Key doesn't exist, proceed with insertion
	}

	// Create composite key with block number
	compositeKey := makeTimeTraversalKey(entry.Key.Bytes, blockNumber)

	// Store the actual value (without prepending block info like original implementation)
	// The block info is now encoded in the key itself
	value := entry.Value.Value

	err := s.db.Update(func(txn *badger.Txn) error {
		err := txn.Set(compositeKey, value)
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

// getRawValue retrieves the raw value for a key, skipping the block number prefix
func (s *Store) getRawValue(key []byte) ([]byte, error) {
	var value []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		return nil, err
	}
	if len(value) < 8 {
		return nil, fmt.Errorf("invalid stored value length for key %s", string(key))
	}
	return value[8:], nil
}

// setRawValue sets the raw value for a key, prepending the block number
func (s *Store) setRawValue(key []byte, value []byte, blockNumber uint64) error {
	blockNumBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(blockNumBytes, blockNumber)
	valueWithBlockInfo := make([]byte, 8+len(value))
	copy(valueWithBlockInfo, blockNumBytes)
	copy(valueWithBlockInfo[8:], value)
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, valueWithBlockInfo)
	})
}

// ApplyOperations applies a list of operations to the store
func (s *Store) ApplyOperations(operations []*pbssinternal.Operation, blockNumber uint64) error {
	if s.timeTraversalEnabled {
		return s.applyOperationsWithTimeTraversal(operations, blockNumber)
	}
	return s.applyOperationsDirect(operations, blockNumber)
}

// applyOperationsDirect applies operations using direct key storage (original behavior)
func (s *Store) applyOperationsDirect(operations []*pbssinternal.Operation, blockNumber uint64) error {
	for _, op := range operations {
		if err := s.applyOperationDirect(op, blockNumber); err != nil {
			return err
		}
	}
	return nil
}

// applyOperationsWithTimeTraversal applies operations using time traversal key encoding
func (s *Store) applyOperationsWithTimeTraversal(operations []*pbssinternal.Operation, blockNumber uint64) error {
	for _, op := range operations {
		if err := s.applyOperationWithTimeTraversal(op, blockNumber); err != nil {
			return err
		}
	}
	return nil
}

// getRawValueTimeTraversal retrieves the raw value for a key at a specific block using time traversal
func (s *Store) getRawValueTimeTraversal(key []byte, blockNumber uint64) ([]byte, error) {
	timeTraversalKey := makeTimeTraversalKey(key, blockNumber)
	var value []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(timeTraversalKey)
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

// setRawValueTimeTraversal sets the raw value for a key at a specific block using time traversal
func (s *Store) setRawValueTimeTraversal(key []byte, value []byte, blockNumber uint64) error {
	timeTraversalKey := makeTimeTraversalKey(key, blockNumber)
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(timeTraversalKey, value)
	})
}

// SetAll stores multiple entries in Badger
func (s *Store) SetAll(entries []*pbmodel.Entry, IfNotExist bool, blockNumber uint64) error {
	if s.timeTraversalEnabled {
		return s.setAllWithTimeTraversal(entries, IfNotExist, blockNumber)
	}
	return s.setAllDirect(entries, IfNotExist, blockNumber)
}

// setAllDirect stores multiple entries using direct key storage (original behavior)
func (s *Store) setAllDirect(entries []*pbmodel.Entry, IfNotExist bool, blockNumber uint64) error {
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

// setAllWithTimeTraversal stores multiple entries using time traversal key encoding
func (s *Store) setAllWithTimeTraversal(entries []*pbmodel.Entry, IfNotExist bool, blockNumber uint64) error {
	if len(entries) == 0 {
		return nil
	}

	// Use a batch writer for better performance with multiple entries
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	for _, entry := range entries {
		if IfNotExist {
			// For time traversal, check if any version of this key exists
			err := s.db.View(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.PrefetchSize = 10
				it := txn.NewIterator(opts)
				defer it.Close()

				prefix := entry.Key.Bytes
				for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
					// If we find any key with this prefix, it means the key exists
					return nil
				}
				return badger.ErrKeyNotFound
			})
			if err == nil {
				// Key exists, skip this entry
				continue
			}
			if err != badger.ErrKeyNotFound {
				return fmt.Errorf("failed to check existence of key: %w", err)
			}
			// Key doesn't exist, proceed with insertion
		}

		// Create composite key with block number
		compositeKey := makeTimeTraversalKey(entry.Key.Bytes, blockNumber)

		// Store the actual value (without prepending block info)
		value := entry.Value.Value

		err := wb.Set(compositeKey, value)
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
