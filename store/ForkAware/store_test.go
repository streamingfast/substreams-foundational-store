package ForkAware

import (
	"testing"

	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	"google.golang.org/protobuf/types/known/anypb"
)

type mockStoreCachedEntry struct {
	entry       *pbstore.Entry
	blockNumber uint64
	blockHash   []byte
}

// mockStore is a simple in-memory implementation of the foundational-store.Store interface for testing
type mockStore struct {
	entries map[string]mockStoreCachedEntry
}

func newMockStore() *mockStore {
	return &mockStore{
		entries: make(map[string]mockStoreCachedEntry),
	}
}

func (m *mockStore) Set(entry *pbstore.Entry, blockNumber uint64) error {
	m.entries[string(entry.Key)] = mockStoreCachedEntry{
		entry:       entry,
		blockNumber: blockNumber,
		blockHash:   nil, // Keep for compatibility but not used
	}
	return nil
}

func (m *mockStore) SetAll(entries []*pbstore.Entry, blockNumber uint64) error {
	for _, entry := range entries {
		m.entries[string(entry.Key)] = mockStoreCachedEntry{
			entry:       entry,
			blockNumber: blockNumber,
			blockHash:   nil, // Keep for compatibility but not used
		}
	}
	return nil
}

func (m *mockStore) Get(request *pbstore.GetRequest) (*pbstore.GetResponse, error) {
	cached, ok := m.entries[string(request.Key)]
	if !ok || cached.blockNumber > request.BlockNumber {
		return &pbstore.GetResponse{
			Code: pbstore.ResponseCode_RESPONSE_CODE_NOT_FOUND,
		}, nil
	}
	return &pbstore.GetResponse{
		Code:  pbstore.ResponseCode_RESPONSE_CODE_FOUND,
		Value: cached.entry.Value,
	}, nil
}

func (m *mockStore) GetAll(request *pbstore.GetAllRequest) (*pbstore.GetAllResponse, error) {
	response := &pbstore.GetAllResponse{
		Entries: make([]*pbstore.ResponseEntry, 0, len(request.Keys)),
	}
	for _, key := range request.Keys {
		cached, ok := m.entries[string(key)]
		if !ok || cached.blockNumber > request.BlockNumber {
			response.Entries = append(response.Entries, &pbstore.ResponseEntry{
				Key: key,
				Response: &pbstore.GetResponse{
					Code: pbstore.ResponseCode_RESPONSE_CODE_NOT_FOUND,
				},
			})
		} else {
			response.Entries = append(response.Entries, &pbstore.ResponseEntry{
				Key: key,
				Response: &pbstore.GetResponse{
					Code:  pbstore.ResponseCode_RESPONSE_CODE_FOUND,
					Value: cached.entry.Value,
				},
			})
		}
	}
	return response, nil
}

func TestCacheStore(t *testing.T) {
	// Create a mock foundational-store
	mockStore := newMockStore()

	// Create a ForkAware foundational-store that wraps the mock foundational-store
	cacheStore := NewStore(mockStore)

	// Create some test entries
	entry1 := &pbstore.Entry{
		Key:   []byte("key1"),
		Value: &anypb.Any{TypeUrl: "test", Value: []byte("value1")},
	}
	entry2 := &pbstore.Entry{
		Key:   []byte("key2"),
		Value: &anypb.Any{TypeUrl: "test", Value: []byte("value2")},
	}
	entry3 := &pbstore.Entry{
		Key:   []byte("key3"),
		Value: &anypb.Any{TypeUrl: "test", Value: []byte("value3")},
	}

	// Set entries in the ForkAware foundational-store
	if err := cacheStore.Set(entry1, 100); err != nil {
		t.Fatalf("Failed to set entry1: %v", err)
	}
	if err := cacheStore.Set(entry2, 200); err != nil {
		t.Fatalf("Failed to set entry2: %v", err)
	}
	if err := cacheStore.Set(entry3, 300); err != nil {
		t.Fatalf("Failed to set entry3: %v", err)
	}

	// Verify that entries are in the ForkAware but not in the mock foundational-store
	// (since flushUpToBlock is 0 by default)
	if len(mockStore.entries) != 0 {
		t.Errorf("Expected 0 entries in mock foundational-store, got %d", len(mockStore.entries))
	}

	// Get entry1 from the ForkAware foundational-store
	resp1, err := cacheStore.Get(&pbstore.GetRequest{
		BlockNumber: 150,
		Key:         []byte("key1"),
	})
	if err != nil {
		t.Fatalf("Failed to get entry1: %v", err)
	}
	if resp1.Code != pbstore.ResponseCode_RESPONSE_CODE_FOUND {
		t.Errorf("Expected FOUND response for entry1, got %v", resp1.Code)
	}

	// Flush entries with block numbers <= 200
	if err := cacheStore.FlushUpToBlock(200); err != nil {
		t.Fatalf("Failed to flush entries: %v", err)
	}

	// Verify that entry1 and entry2 are now in the mock foundational-store
	if len(mockStore.entries) != 2 {
		t.Errorf("Expected 2 entries in mock foundational-store, got %d", len(mockStore.entries))
	}

	// Verify that entry1 and entry2 are no longer in the ForkAware
	// by checking if the mock foundational-store is used for retrieval
	mockStore.entries["key1"] = mockStoreCachedEntry{
		blockNumber: 100,
		entry: &pbstore.Entry{
			Key:   []byte("key1"),
			Value: &anypb.Any{TypeUrl: "test", Value: []byte("modified1")},
		},
	}

	resp1, err = cacheStore.Get(&pbstore.GetRequest{
		BlockNumber: 150,
		Key:         []byte("key1"),
	})
	if err != nil {
		t.Fatalf("Failed to get entry1: %v", err)
	}
	if string(resp1.Value.Value) != "modified1" {
		t.Errorf("Expected modified value for entry1, got %s", string(resp1.Value.Value))
	}

	// Verify that entry3 is still in the ForkAware
	resp3, err := cacheStore.Get(&pbstore.GetRequest{
		BlockNumber: 350,
		Key:         []byte("key3"),
	})
	if err != nil {
		t.Fatalf("Failed to get entry3: %v", err)
	}
	if resp3.Code != pbstore.ResponseCode_RESPONSE_CODE_FOUND {
		t.Errorf("Expected FOUND response for entry3, got %v", resp3.Code)
	}
	if string(resp3.Value.Value) != "value3" {
		t.Errorf("Expected original value for entry3, got %s", string(resp3.Value.Value))
	}
}
