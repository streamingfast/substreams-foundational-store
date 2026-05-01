package badger_time_traversal

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/dgraph-io/badger/v3"
	pbmodel "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/model/v2"
	"github.com/streamingfast/substreams-foundational-store/store/arithmetic"
)

// makeTimeTraversalKey creates a composite key by appending the reversed block number to the original key
// Format: original_key + (math.MaxUint64 - block_number) (8 bytes, big-endian)
// This reverses the ordering so newer blocks come first in lexicographic order
func makeTimeTraversalKey(originalKey []byte, blockNumber uint64) []byte {
	// Create a new key with original key + 8 bytes for block number
	compositeKey := make([]byte, len(originalKey)+8)

	// Copy original key
	copy(compositeKey, originalKey)

	// Append reversed block number as 8 bytes (big-endian)
	// This makes newer blocks (higher block numbers) sort first
	reversedBlockNumber := math.MaxUint64 - blockNumber
	binary.BigEndian.PutUint64(compositeKey[len(originalKey):], reversedBlockNumber)

	return compositeKey
}

// Set stores a single entry in Badger with time traversal support
func (s *Store) Set(entry *pbmodel.Entry, IfNotExist bool, blockNumber uint64) error {
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

// SetAll stores multiple entries in Badger with time traversal support
// Now supports policy-aware writes for ADD, MIN, MAX, APPEND, and SET_SUM operations
func (s *Store) SetAll(entries []*pbmodel.Entry, deletePrefixes []string, IfNotExist bool, blockNumber uint64) error {
	// Handle delete-prefix operations first, before writing new entries.
	if len(deletePrefixes) > 0 {
		for _, prefix := range deletePrefixes {
			if err := s.DeletePrefix(prefix); err != nil {
				return fmt.Errorf("failed to delete prefix %q: %w", prefix, err)
			}
		}
	}

	if len(entries) == 0 {
		return nil
	}

	// Process entries in a transaction for consistency
	err := s.db.Update(func(txn *badger.Txn) error {
		for _, entry := range entries {
			// Determine effective update policy
			policy := entry.UpdatePolicy
			if policy == 0 {
				policy = pbmodel.UpdatePolicy_UPDATE_POLICY_SET
			}
			
			// Override with IfNotExist if specified at batch level
			if IfNotExist {
				policy = pbmodel.UpdatePolicy_UPDATE_POLICY_SET_IF_NOT_EXISTS
			}

			valueType := entry.ValueType
			if valueType == "" {
				valueType = "bytes" // default
			}

			// Create composite key with block number
			compositeKey := makeTimeTraversalKey(entry.Key.Bytes, blockNumber)

			// For policies that need read-modify-write, read existing value first
			var existingValue []byte
			needsRead := policy != pbmodel.UpdatePolicy_UPDATE_POLICY_SET

			if needsRead {
				// Try to get existing value at this block or earlier
				existingValue, _ = s.getValueAtBlock(txn, entry.Key.Bytes, blockNumber)
			}

			// Apply update policy to determine final value
			newValue := entry.Value.Value
			finalValue, err := arithmetic.ApplyUpdatePolicy(existingValue, newValue, policy, valueType)
			if err != nil {
				return fmt.Errorf("failed to apply update policy for key %s: %w", string(entry.Key.Bytes), err)
			}

			// Only write if the policy allows it
			if policy == pbmodel.UpdatePolicy_UPDATE_POLICY_SET_IF_NOT_EXISTS && len(existingValue) > 0 {
				// Key exists, skip
				continue
			}

			// Store the final value
			err = txn.Set(compositeKey, finalValue)
			if err != nil {
				return fmt.Errorf("failed to set value in Badger for key %s: %w", string(entry.Key.Bytes), err)
			}
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to update Badger: %w", err)
	}

	return nil
}

// getValueAtBlock retrieves the value for a key at or before the specified block
// This is used for read-modify-write operations in policy-aware updates
func (s *Store) getValueAtBlock(txn *badger.Txn, key []byte, blockNumber uint64) ([]byte, error) {
	// Seek to the key at the requested block
	seekKey := makeTimeTraversalKey(key, blockNumber)

	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 1
	it := txn.NewIterator(opts)
	defer it.Close()

	// Seek to the requested block or earlier
	it.Seek(seekKey)

	// Check if we found a valid entry for this key
	if !it.ValidForPrefix(key) {
		return nil, badger.ErrKeyNotFound
	}

	item := it.Item()
	value, err := item.ValueCopy(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to copy value: %w", err)
	}

	return value, nil
}

// DeletePrefix removes all keys with the specified prefix
// This scans all time-traversal versions of keys matching the prefix and deletes them
func (s *Store) DeletePrefix(prefix string) error {
	prefixBytes := []byte(prefix)

	err := s.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // We only need keys, not values
		it := txn.NewIterator(opts)
		defer it.Close()

		// Collect keys to delete
		var keysToDelete [][]byte
		for it.Seek(prefixBytes); it.ValidForPrefix(prefixBytes); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			keysToDelete = append(keysToDelete, key)
		}

		// Delete all matching keys
		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				return fmt.Errorf("failed to delete key %s: %w", string(key), err)
			}
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to delete prefix %s: %w", prefix, err)
	}

	return nil
}
