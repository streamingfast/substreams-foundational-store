package badger

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	"github.com/streamingfast/substreams-foundational-store/sink"
	"google.golang.org/protobuf/types/known/anypb"
)

// Get retrieves a single entry from Badger
func (s *Store) Get(request *pbstore.GetRequest) (*pbstore.GetResponse, error) {
	// Track total execution time
	executionStart := time.Now()
	defer sink.DatabaseExecutionDuration.ObserveDuration(time.Since(executionStart))

	// Track key requests - single key per Get call
	sink.DatabaseKeysRequestedTotal.Inc()
	sink.DatabaseCallCount.Inc()

	defer func() {
		lsm, vlog := s.db.Size()
		totalSize := uint64(lsm + vlog)
		if totalSize > 0 {
			sink.BadgerStoreSize.SetUint64(totalSize)
			sink.BadgerLSMSize.SetUint64(uint64(lsm))
			sink.BadgerVLogSize.SetUint64(uint64(vlog))
		}
		sink.DatabaseKeysProcessed.Inc()
	}()

	var storedValue []byte
	var found bool

	// Track Badger-specific operation time
	badgerStart := time.Now()
	err := s.db.View(func(txn *badger.Txn) error {
		// Directly look up the key
		getStart := time.Now()
		item, err := txn.Get(request.Key)
		sink.BadgerGetOperationDuration.ObserveDuration(time.Since(getStart))
		sink.BadgerGetOperationCount.Inc()

		if err != nil {
			if err == badger.ErrKeyNotFound {
				// Key not found, return nil error to indicate not found
				return nil
			}
			return err
		}

		// Key found
		found = true

		// Get the value
		err = item.Value(func(val []byte) error {
			// Make a copy of the value as it's only valid within this transaction
			storedValue = append([]byte{}, val...)
			return nil
		})
		if err != nil {
			return err
		}

		return nil
	})
	sink.BadgerTransactionDuration.ObserveDuration(time.Since(badgerStart))
	sink.BadgerTransactionCount.Inc()

	if err != nil {
		sink.DatabaseGetErrors.Inc()
		return nil, fmt.Errorf("failed to get value from Badger: %w", err)
	}

	if !found {
		// Track no keys found for this call
		sink.DatabaseGetMisses.Inc()
		return &pbstore.GetResponse{
			Code: pbstore.ResponseCode_RESPONSE_CODE_NOT_FOUND,
		}, nil
	}

	// Track found keys - single key found
	sink.DatabaseKeysFoundTotal.Inc()
	sink.DatabaseGetHits.Inc()

	// Extract the block number and the actual value
	// Ensure we have at least 8 bytes for the block number
	if len(storedValue) < 8 {
		return nil, fmt.Errorf("invalid stored value: expected at least 8 bytes for block number")
	}

	// Extract the block number from the first 8 bytes
	blockNumber := binary.BigEndian.Uint64(storedValue[:8])

	// The actual value is everything after the block number
	actualValue := storedValue[8:]

	if request.BlockNumber < blockNumber {
		return &pbstore.GetResponse{
			Code: pbstore.ResponseCode_RESPONSE_CODE_NOT_FOUND,
		}, nil
	}

	// Note: Block hash validation is not supported in this implementation
	// as the setter does not store block hash information

	return &pbstore.GetResponse{
		Code: pbstore.ResponseCode_RESPONSE_CODE_FOUND,
		Value: &anypb.Any{
			TypeUrl: s.typeUrl,
			Value:   actualValue,
		},
	}, nil
}

// GetAll retrieves multiple entries from Badger using goroutines for parallelism
func (s *Store) GetAll(request *pbstore.GetAllRequest) (*pbstore.GetAllResponse, error) {
	// Track total execution time
	executionStart := time.Now()
	defer sink.DatabaseExecutionDuration.ObserveDuration(time.Since(executionStart))

	// Track key requests - number of keys in this GetAll call
	sink.DatabaseKeysRequestedTotal.AddInt(len(request.Keys))
	sink.DatabaseCallCount.Inc()

	defer sink.DatabaseKeysProcessed.AddInt(len(request.Keys))

	// Create a slice to foundational-store the entries
	var entries []*pbstore.ResponseEntry
	var keysFoundCount int

	// Create a transaction
	badgerStart := time.Now()
	err := s.db.View(func(txn *badger.Txn) error {
		// Use a channel to distribute keys to workers
		keyChan := make(chan []byte, len(request.Keys))
		seenKeys := make(map[string]bool)
		for _, key := range request.Keys {
			if seenKeys[string(key)] {
				continue
			}
			sKey := hex.EncodeToString(key)
			keyChan <- key
			seenKeys[sKey] = true
		}
		close(keyChan)

		// Use a channel to collect errors
		errChan := make(chan error, len(request.Keys))

		// Use a mutex to protect the entries slice and key count
		var mutex sync.Mutex

		// Use the configured number of workers
		numWorkers := s.numWorkers
		if len(request.Keys) < numWorkers {
			numWorkers = len(request.Keys)
		}

		// Use a wait group to wait for all workers to finish
		var wg sync.WaitGroup
		wg.Add(numWorkers)

		// Start workers
		for i := 0; i < numWorkers; i++ {
			go func() {
				defer wg.Done()

				for key := range keyChan {
					var value []byte

					// Directly look up the key
					getStart := time.Now()
					item, err := txn.Get(key)
					sink.BadgerGetOperationDuration.ObserveDuration(time.Since(getStart))
					sink.BadgerGetOperationCount.Inc()
					if err != nil {
						if err == badger.ErrKeyNotFound {
							// Key not found, add a NOT_FOUND response
							mutex.Lock()
							entries = append(entries,
								&pbstore.ResponseEntry{
									Key: key,
									Response: &pbstore.GetResponse{
										Code: pbstore.ResponseCode_RESPONSE_CODE_NOT_FOUND,
									},
								})
							mutex.Unlock()
							continue
						}
						errChan <- err
						return
					}

					// Get the value
					err = item.Value(func(val []byte) error {
						// Make a copy of the value as it's only valid within this transaction
						value = append([]byte{}, val...)
						return nil
					})
					if err != nil {
						errChan <- err
						return
					}

					// Extract the block number and the actual value
					// Ensure we have at least 8 bytes for the block number
					if len(value) < 8 {
						errChan <- fmt.Errorf("invalid stored value: expected at least 8 bytes for block number")
						return
					}

					// Extract the block number from the first 8 bytes
					blockNumber := binary.BigEndian.Uint64(value[:8])

					// The actual value is everything after the block number
					actualValue := value[8:]

					if request.BlockNumber < blockNumber {
						mutex.Lock()
						entries = append(entries, &pbstore.ResponseEntry{
							Key: key,
							Response: &pbstore.GetResponse{
								Code: pbstore.ResponseCode_RESPONSE_CODE_NOT_FOUND,
							},
						})
						mutex.Unlock()
						continue
					}

					// Note: Block hash validation is not supported in this implementation
					// as the setter does not store block hash information

					mutex.Lock()
					keysFoundCount++
					entries = append(entries,
						&pbstore.ResponseEntry{
							Key: key,
							Response: &pbstore.GetResponse{
								Code: pbstore.ResponseCode_RESPONSE_CODE_FOUND,
								Value: &anypb.Any{
									TypeUrl: s.typeUrl,
									Value:   actualValue,
								},
							},
						})
					mutex.Unlock()
				}
			}()
		}

		// Wait for all workers to finish
		wg.Wait()

		// Check for errors
		select {
		case err := <-errChan:
			return err
		default:
			return nil
		}
	})
	sink.BadgerTransactionDuration.ObserveDuration(time.Since(badgerStart))
	sink.BadgerTransactionCount.Inc()

	if err != nil {
		return nil, fmt.Errorf("failed to get values from Badger: %w", err)
	}
	sink.DatabaseKeysFoundTotal.AddInt(keysFoundCount)

	return &pbstore.GetAllResponse{
		Entries: entries,
	}, nil
}
