package postgres_time_traversal

import (
	"fmt"
	"time"

	pbmodel "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/model/v2"
)

func (s *Store) Set(entry *pbmodel.Entry, IfNotExist bool, blockNumber uint64) error {
	if entry == nil {
		return fmt.Errorf("entry cannot be nil")
	}

	if IfNotExist {
		// Check if key already exists
		var count int
		err := s.db.Get(&count, fmt.Sprintf("SELECT COUNT(*) FROM %s.entries WHERE key = $1", s.schemaName), entry.Key)
		if err != nil {
			return fmt.Errorf("failed to check existence of key: %w", err)
		}
		if count > 0 {
			// Key already exists, skip insertion
			return nil
		}
	}

	// Use the prepared insert statement to insert the entry
	// The statement expects: block_number, key, value, create_time
	_, err := s.insertStatement.Exec(blockNumber, entry.Key, entry.Value.Value, time.Now())
	if err != nil {
		return fmt.Errorf("failed to insert entry: %w", err)
	}

	return nil
}

func (s *Store) SetAll(entries []*pbmodel.Entry, deletePrefixes []string, IfNotExist bool, blockNumber uint64) error {
	if len(entries) == 0 {
		return nil
	}

	// Begin a transaction for better performance and consistency
	tx, err := s.db.Beginx()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Use the existing prepared statement from the store
	insertStmt := s.insertStatement
	if insertStmt == nil {
		_ = tx.Rollback()
		return fmt.Errorf("insert statement is nil")
	}

	// Insert each entry with the same block number
	for _, entry := range entries {
		if entry == nil {
			_ = tx.Rollback()
			return fmt.Errorf("entry cannot be nil")
		}

		if IfNotExist {
			// Check if key already exists
			var count int
			err := tx.Get(&count, fmt.Sprintf("SELECT COUNT(*) FROM %s.entries WHERE key = $1", s.schemaName), entry.Key)
			if err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("failed to check existence of key: %w", err)
			}
			if count > 0 {
				// Key already exists, skip insertion
				continue
			}
		}

		// Use the block_number from the parameter
		_, err := insertStmt.Exec(blockNumber, entry.Key, entry.Value.Value, time.Now())
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("failed to insert entry: %w", err)
		}
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
