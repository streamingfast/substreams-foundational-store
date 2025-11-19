package badger

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
	"github.com/streamingfast/substreams-foundational-store/store"
	"go.uber.org/zap"
)

// Store implements the foundational-store.Store interface for Badger DB
type Store struct {
	db                   *badger.DB
	typeUrl              string
	numWorkers           int
	logger               *zap.Logger
	timeTraversalEnabled bool
}

// NewStore creates a new Badger foundational-store
func NewStore(dsn *store.DSN, typeUrl string, numWorkers int, logger *zap.Logger, enableTimeTraversal bool) (*Store, error) {
	// Provide default logger if nil
	if logger == nil {
		logger = zap.NewNop()
	}
	// Extract Badger-specific parameters
	// For Badger, we'll use the Database field to hold the path to the Badger DB directory
	dbPath := dsn.Database

	// Ensure the directory exists
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory for Badger DB: %w", err)
	}

	// Configure Badger options based on time traversal setting
	badgerOpts := badger.DefaultOptions(dbPath)
	badgerOpts.Logger = nil

	if enableTimeTraversal {
		// Apply performance optimizations for time traversal
		badgerOpts = badgerOpts.
			WithBlockCacheSize(512 << 20). // 512MB
			WithIndexCacheSize(0).
			WithBloomFalsePositive(0.001).
			WithValueThreshold(128 << 10). // 128KB
			WithCompression(options.None).
			WithNumMemtables(5).
			WithValueLogFileSize(256 << 20).
			WithMemTableSize(512 << 20).
			WithNumGoroutines(32)
	}

	db, err := badger.Open(badgerOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open Badger DB: %w", err)
	}

	// Create foundational-store with provided values
	store := &Store{
		db:                   db,
		typeUrl:              typeUrl,
		numWorkers:           numWorkers,
		logger:               logger,
		timeTraversalEnabled: enableTimeTraversal,
	}

	store.logger.Info("badger foundational-store initialized",
		zap.String("path", dbPath),
		zap.Int("workers", store.numWorkers),
		zap.Bool("timeTraversal", enableTimeTraversal))

	return store, nil
}

// Close closes the Badger database
func (s *Store) Close() error {
	return s.db.Close()
}

// GetDB returns the underlying Badger database
func (s *Store) GetDB() *badger.DB {
	return s.db
}

// GetTypeURL returns the type URL for the stored values
func (s *Store) GetTypeURL() string {
	return s.typeUrl
}

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
	reversedBlockNumber := ^uint64(0) - blockNumber // math.MaxUint64 - blockNumber
	binary.BigEndian.PutUint64(compositeKey[len(originalKey):], reversedBlockNumber)

	return compositeKey
}
