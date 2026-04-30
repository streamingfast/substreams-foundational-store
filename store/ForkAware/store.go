package ForkAware

import (
	"fmt"
	"sync"

	pbmodel "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/model/v2"
	pbservice "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/service/v2"
	"github.com/streamingfast/substreams-foundational-store/store"
	"github.com/streamingfast/substreams-foundational-store/store/arithmetic"
	"google.golang.org/protobuf/types/known/anypb"
)

type cachedEntry struct {
	entry       *pbmodel.Entry
	blockNumber uint64
}

// Store implements the foundational-store.Store interface by wrapping another foundational-store
// and caching entries in memory until flushUpToBlock is changed.
type Store struct {
	wrapped        store.Store
	cache          map[string]cachedEntry // key -> Entry
	flushUpToBlock uint64
	mu             sync.RWMutex
}

// NewStore creates a new ForkAware foundational-store that wraps the provided foundational-store.
func NewStore(wrapped store.Store) *Store {
	return &Store{
		wrapped:        wrapped,
		cache:          make(map[string]cachedEntry),
		flushUpToBlock: 0,
	}
}

// SetAll stores multiple entries in the ForkAware.
func (s *Store) SetAll(entries []*pbmodel.Entry, IfNotExist bool, blockNumber uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var toSet []*pbmodel.Entry
	for _, entry := range entries {
		key := string(entry.Key.Bytes)
		if IfNotExist {
			if _, exists := s.cache[key]; exists {
				continue
			}
			toSet = append(toSet, entry)
		} else {
			toSet = append(toSet, entry)
		}
	}

	// If IfNotExist, filter out entries that exist in wrapped store
	if IfNotExist && len(toSet) > 0 {
		var keysToCheck []*pbmodel.Key
		for _, entry := range toSet {
			keysToCheck = append(keysToCheck, entry.Key)
		}
		req := &pbservice.GetRequest{
			Keys:        keysToCheck,
			BlockNumber: blockNumber,
		}
		resp, err := s.wrapped.Get(req)
		if err != nil {
			return fmt.Errorf("failed to check existence in wrapped store: %w", err)
		}
		var filtered []*pbmodel.Entry
		for i, queried := range resp.Entries.Entries {
			if queried.Code != pbmodel.ResponseCode_RESPONSE_CODE_FOUND {
				filtered = append(filtered, toSet[i])
			}
		}
		toSet = filtered
	}

	// Set in cache with policy-aware merging
	for _, entry := range toSet {
		key := string(entry.Key.Bytes)

		policy := entry.UpdatePolicy
		if policy == 0 {
			policy = pbmodel.UpdatePolicy_UPDATE_POLICY_SET
		}

		valueType := entry.ValueType
		if valueType == "" {
			valueType = "bytes"
		}

		// For accumulating policies, merge with the existing cached value regardless of block number.
		// This ensures cross-block accumulation is handled entirely in the ForkAware layer;
		// when we eventually flush to Badger we send a SET with the fully-resolved value.
		if existing, exists := s.cache[key]; exists {
			shouldAccumulate := policy == pbmodel.UpdatePolicy_UPDATE_POLICY_ADD ||
				policy == pbmodel.UpdatePolicy_UPDATE_POLICY_SET_SUM ||
				policy == pbmodel.UpdatePolicy_UPDATE_POLICY_APPEND

			if shouldAccumulate {
				mergedValue, err := arithmetic.ApplyUpdatePolicy(
					existing.entry.Value.Value,
					entry.Value.Value,
					policy,
					valueType,
				)
				if err != nil {
					return fmt.Errorf("failed to merge cached entry for key %s: %w", key, err)
				}

				// Clone the entry before storing to avoid mutating the caller's proto.
				// The flushed-to-Badger value will be the fully-accumulated result,
				// sent as UPDATE_POLICY_SET so Badger does not re-apply the policy.
				entry = &pbmodel.Entry{
					Key:          entry.Key,
					Value:        &anypb.Any{TypeUrl: entry.Value.TypeUrl, Value: mergedValue},
					UpdatePolicy: pbmodel.UpdatePolicy_UPDATE_POLICY_SET,
					ValueType:    entry.ValueType,
				}
			}
		}

		s.cache[key] = cachedEntry{
			entry:       entry,
			blockNumber: blockNumber,
		}
	}

	var actuallySet []*pbmodel.Entry = toSet

	// Collect entries that need to be flushed to the wrapped foundational-store
	var toFlush []*pbmodel.Entry
	for _, entry := range actuallySet {
		if blockNumber <= s.flushUpToBlock {
			toFlush = append(toFlush, entry)
		}
	}

	// Flush collected entries to the wrapped foundational-store
	if len(toFlush) > 0 {
		if err := s.wrapped.SetAll(toFlush, IfNotExist, blockNumber); err != nil {
			return fmt.Errorf("failed to set entries in wrapped foundational-store: %w", err)
		}
	}

	return nil
}

// Get retrieves entries for the provided keys by delegating to the wrapped store and overlaying cache when possible.
func (s *Store) Get(request *pbservice.GetRequest) (*pbservice.GetResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	resp, err := s.wrapped.Get(request)
	if err != nil {
		return nil, err
	}
	// Overlay cache results when cached blockNumber <= requested block
	for i, key := range request.Keys {
		if cached, ok := s.cache[string(key.Bytes)]; ok {
			if cached.blockNumber <= request.BlockNumber {
				resp.Entries.Entries[i] = &pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_FOUND, Entry: cached.entry}
			}
		}
	}
	return resp, nil
}

// GetFirst delegates to the wrapped store and overlays the cache similarly for exact key matches
func (s *Store) GetFirst(request *pbservice.GetRequest) (*pbservice.GetResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	resp, err := s.wrapped.GetFirst(request)
	if err != nil {
		return nil, err
	}
	for i, key := range request.Keys {
		if cached, ok := s.cache[string(key.Bytes)]; ok {
			if cached.blockNumber <= request.BlockNumber {
				resp.Entries.Entries[i] = &pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_FOUND, Entry: cached.entry}
			}
		}
	}
	return resp, nil
}

// FlushUpToBlock flushes all entries with block numbers <= blockNum to the wrapped foundational-store.
func (s *Store) FlushUpToBlock(blockNum uint64, IfNotExist bool) error {

	s.mu.Lock()
	defer s.mu.Unlock()

	// Update flushUpToBlock
	s.flushUpToBlock = blockNum

	// Collect entries to flush
	var toFlush []*pbmodel.Entry
	for _, cached := range s.cache {
		if cached.blockNumber <= blockNum {
			toFlush = append(toFlush, cached.entry)
		}
	}

	// Flush collected entries to the wrapped foundational-store
	if len(toFlush) > 0 {
		if err := s.wrapped.SetAll(toFlush, IfNotExist, blockNum); err != nil {
			return fmt.Errorf("failed to flush entries to wrapped foundational-store: %w", err)
		}

		// Remove flushed entries from ForkAware
		for _, entry := range toFlush {
			delete(s.cache, string(entry.Key.Bytes))
		}
	}

	return nil
}

// EvictUpToBlock removes all keys from the ForkAware where the block number is >= to upToBlockNumber.
func (s *Store) EvictUpToBlock(upToBlockNumber uint64) error {

	s.mu.Lock()
	defer s.mu.Unlock()

	// Collect keys to evict
	var keysToEvict []string
	for key, cached := range s.cache {
		if cached.blockNumber >= upToBlockNumber {
			keysToEvict = append(keysToEvict, key)
		}
	}

	// Remove evicted entries from ForkAware
	for _, key := range keysToEvict {
		delete(s.cache, key)
	}

	return nil
}
