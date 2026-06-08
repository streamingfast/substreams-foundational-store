package ForkAware

import (
	"fmt"
	"sync"

	pbmodel "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/model/v2"
	pbservice "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/service/v2"
	"github.com/streamingfast/substreams-foundational-store/store"
)

type cachedEntry struct {
	entry       *pbmodel.Entry
	blockNumber uint64
	// insertOnly records the write mode this entry must be flushed with: when true the
	// wrapped store skips the key if it already exists (first-write-wins) instead of
	// overwriting it. Mirrors the IfNotExist flag from the entry's originating output.
	insertOnly bool
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

	// Set in cache
	for _, entry := range toSet {
		key := string(entry.Key.Bytes)
		s.cache[key] = cachedEntry{
			entry:       entry,
			blockNumber: blockNumber,
			insertOnly:  IfNotExist,
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
// Each entry is flushed with the IfNotExist flag it was set with, so the flush stays consistent
// even when it runs on a block that produced no output of its own.
func (s *Store) FlushUpToBlock(blockNum uint64) error {

	s.mu.Lock()
	defer s.mu.Unlock()

	// Update flushUpToBlock
	s.flushUpToBlock = blockNum

	// Collect entries to flush, grouped by their write mode.
	var toFlush, toFlushInsertOnly []*pbmodel.Entry
	for _, cached := range s.cache {
		if cached.blockNumber <= blockNum {
			if cached.insertOnly {
				toFlushInsertOnly = append(toFlushInsertOnly, cached.entry)
			} else {
				toFlush = append(toFlush, cached.entry)
			}
		}
	}

	// Flush collected entries to the wrapped foundational-store
	for _, group := range []struct {
		entries    []*pbmodel.Entry
		ifNotExist bool
	}{
		{toFlush, false},
		{toFlushInsertOnly, true},
	} {
		if len(group.entries) == 0 {
			continue
		}
		if err := s.wrapped.SetAll(group.entries, group.ifNotExist, blockNum); err != nil {
			return fmt.Errorf("failed to flush entries to wrapped foundational-store: %w", err)
		}
		// Remove flushed entries from ForkAware
		for _, entry := range group.entries {
			delete(s.cache, string(entry.Key.Bytes))
		}
	}

	return nil
}

// EvictAfterBlock removes all keys from the ForkAware whose block number is strictly greater
// than blockNumber, i.e. everything written after the last valid block of an undo signal.
func (s *Store) EvictAfterBlock(blockNumber uint64) error {

	s.mu.Lock()
	defer s.mu.Unlock()

	// Collect keys to evict
	var keysToEvict []string
	for key, cached := range s.cache {
		if cached.blockNumber > blockNumber {
			keysToEvict = append(keysToEvict, key)
		}
	}

	// Remove evicted entries from ForkAware
	for _, key := range keysToEvict {
		delete(s.cache, key)
	}

	return nil
}
