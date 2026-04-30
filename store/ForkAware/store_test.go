package ForkAware

import (
	"fmt"
	"strconv"
	"testing"

	pbmodel "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/model/v2"
	pbservice "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/service/v2"
	"google.golang.org/protobuf/types/known/anypb"
)

type mockStoreCachedEntry struct {
	entry       *pbmodel.Entry
	blockNumber uint64
}

// mockStore is a simple in-memory implementation of the store.Store interface for testing
// It mimics basic fork-aware semantics based on block numbers only.
type mockStore struct {
	entries map[string]mockStoreCachedEntry
}

func newMockStore() *mockStore {
	return &mockStore{entries: make(map[string]mockStoreCachedEntry)}
}

func (m *mockStore) Set(entry *pbmodel.Entry, IfNotExist bool, blockNumber uint64) error {
	key := string(entry.Key.Bytes)
	if IfNotExist {
		if _, exists := m.entries[key]; exists {
			return nil
		}
	}
	m.entries[key] = mockStoreCachedEntry{entry: entry, blockNumber: blockNumber}
	return nil
}

func TestForkAwareIfNotExist(t *testing.T) {
	ms := newMockStore()
	fa := NewStore(ms)

	entry1 := &pbmodel.Entry{Key: &pbmodel.Key{Bytes: []byte("key1")}, Value: &anypb.Any{TypeUrl: "test", Value: []byte("value1")}}
	entry1Dup := &pbmodel.Entry{Key: &pbmodel.Key{Bytes: []byte("key1")}, Value: &anypb.Any{TypeUrl: "test", Value: []byte("value1-dup")}}

	// Set entry1 normally
	if err := fa.SetAll([]*pbmodel.Entry{entry1}, false, 100); err != nil {
		t.Fatalf("Failed to set entry1: %v", err)
	}

	// Try to set duplicate with IfNotExist=true, should skip
	if err := fa.SetAll([]*pbmodel.Entry{entry1Dup}, true, 200); err != nil {
		t.Fatalf("Failed to set duplicate: %v", err)
	}

	// Check that cache still has original value
	resp, err := fa.Get(&pbservice.GetRequest{BlockNumber: 300, Keys: []*pbmodel.Key{{Bytes: []byte("key1")}}})
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}
	if resp.Entries.Entries[0].Code != pbmodel.ResponseCode_RESPONSE_CODE_FOUND {
		t.Errorf("Expected FOUND, got %v", resp.Entries.Entries[0].Code)
	}
	if got := string(resp.Entries.Entries[0].Entry.Value.Value); got != "value1" {
		t.Errorf("Expected original value 'value1', got %s", got)
	}

	// Test SetAll with IfNotExist
	entry2 := &pbmodel.Entry{Key: &pbmodel.Key{Bytes: []byte("key2")}, Value: &anypb.Any{TypeUrl: "test", Value: []byte("value2")}}
	entry2Dup := &pbmodel.Entry{Key: &pbmodel.Key{Bytes: []byte("key2")}, Value: &anypb.Any{TypeUrl: "test", Value: []byte("value2-dup")}}
	entry3 := &pbmodel.Entry{Key: &pbmodel.Key{Bytes: []byte("key3")}, Value: &anypb.Any{TypeUrl: "test", Value: []byte("value3")}}

	// Set entry2 and entry3
	if err := fa.SetAll([]*pbmodel.Entry{entry2, entry3}, false, 100); err != nil {
		t.Fatalf("Failed to set entries: %v", err)
	}

	// Try to set duplicates with IfNotExist=true
	if err := fa.SetAll([]*pbmodel.Entry{entry2Dup, entry3}, true, 200); err != nil {
		t.Fatalf("Failed to set duplicates: %v", err)
	}

	// Check entry2 still has original value, entry3 should be updated since it's the same
	resp, err = fa.Get(&pbservice.GetRequest{BlockNumber: 300, Keys: []*pbmodel.Key{{Bytes: []byte("key2")}, {Bytes: []byte("key3")}}})
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}
	if got := string(resp.Entries.Entries[0].Entry.Value.Value); got != "value2" {
		t.Errorf("Expected 'value2', got %s", got)
	}
	if got := string(resp.Entries.Entries[1].Entry.Value.Value); got != "value3" {
		t.Errorf("Expected 'value3', got %s", got)
	}

	// Now test IfNotExist when key exists in wrapped store but not in cache
	// Flush entry2 to wrapped store
	if err := fa.FlushUpToBlock(100, false); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}
	// Now cache should not have key2, but wrapped does
	entry2Dup2 := &pbmodel.Entry{Key: &pbmodel.Key{Bytes: []byte("key2")}, Value: &anypb.Any{TypeUrl: "test", Value: []byte("value2-dup2")}}
	// Try to set with IfNotExist=true, should skip because exists in wrapped
	if err := fa.SetAll([]*pbmodel.Entry{entry2Dup2}, true, 400); err != nil {
		t.Fatalf("Failed to set: %v", err)
	}
	// Check value is still original
	resp, err = fa.Get(&pbservice.GetRequest{BlockNumber: 500, Keys: []*pbmodel.Key{{Bytes: []byte("key2")}}})
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}
	if got := string(resp.Entries.Entries[0].Entry.Value.Value); got != "value2" {
		t.Errorf("Expected 'value2' after IfNotExist set, got %s", got)
	}
}

func (m *mockStore) SetAll(entries []*pbmodel.Entry, IfNotExist bool, blockNumber uint64) error {
	for _, entry := range entries {
		key := string(entry.Key.Bytes)
		if IfNotExist {
			if _, exists := m.entries[key]; exists {
				continue
			}
		}
		m.entries[key] = mockStoreCachedEntry{entry: entry, blockNumber: blockNumber}
	}
	return nil
}

func (m *mockStore) Get(request *pbservice.GetRequest) (*pbservice.GetResponse, error) {
	entries := make([]*pbmodel.QueriedEntry, len(request.Keys))
	for i, k := range request.Keys {
		cached, ok := m.entries[string(k.Bytes)]
		if !ok || cached.blockNumber > request.BlockNumber {
			entries[i] = &pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND}
		} else {
			entries[i] = &pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_FOUND, Entry: cached.entry}
		}
	}
	return &pbservice.GetResponse{Entries: &pbmodel.QueriedEntries{Entries: entries}}, nil
}

func (m *mockStore) GetFirst(request *pbservice.GetRequest) (*pbservice.GetResponse, error) {
	entries := make([]*pbmodel.QueriedEntry, len(request.Keys))
	for i, k := range request.Keys {
		if cached, ok := m.entries[string(k.Bytes)]; ok {
			entries[i] = &pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_FOUND, Entry: cached.entry}
			continue
		}
		var bestKey string
		for key := range m.entries {
			if bestKey == "" {
				if key >= string(k.Bytes) {
					bestKey = key
				}
				continue
			}
			if key >= string(k.Bytes) && key < bestKey {
				bestKey = key
			}
		}
		if bestKey != "" {
			ce := m.entries[bestKey]
			entries[i] = &pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_FOUND, Entry: ce.entry}
		} else {
			entries[i] = &pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND}
		}
	}
	return &pbservice.GetResponse{Entries: &pbmodel.QueriedEntries{Entries: entries}}, nil
}

func TestCacheStore(t *testing.T) {
	// Create a mock store
	mockStore := newMockStore()

	// Create a ForkAware store that wraps the mock store
	cacheStore := NewStore(mockStore)

	// Create some test entries
	entry1 := &pbmodel.Entry{Key: &pbmodel.Key{Bytes: []byte("key1")}, Value: &anypb.Any{TypeUrl: "test", Value: []byte("value1")}}
	entry2 := &pbmodel.Entry{Key: &pbmodel.Key{Bytes: []byte("key2")}, Value: &anypb.Any{TypeUrl: "test", Value: []byte("value2")}}
	entry3 := &pbmodel.Entry{Key: &pbmodel.Key{Bytes: []byte("key3")}, Value: &anypb.Any{TypeUrl: "test", Value: []byte("value3")}}

	// Set entries in the ForkAware store
	if err := cacheStore.SetAll([]*pbmodel.Entry{entry1}, false, 100); err != nil {
		t.Fatalf("Failed to set entry1: %v", err)
	}
	if err := cacheStore.SetAll([]*pbmodel.Entry{entry2}, false, 200); err != nil {
		t.Fatalf("Failed to set entry2: %v", err)
	}
	if err := cacheStore.SetAll([]*pbmodel.Entry{entry3}, false, 300); err != nil {
		t.Fatalf("Failed to set entry3: %v", err)
	}

	// Verify that entries are in the ForkAware but not in the mock store (flushUpToBlock defaults to 0)
	if len(mockStore.entries) != 0 {
		t.Errorf("Expected 0 entries in mock store, got %d", len(mockStore.entries))
	}

	// Get entry1 from the ForkAware store
	resp1, err := cacheStore.Get(&pbservice.GetRequest{BlockNumber: 150, Keys: []*pbmodel.Key{{Bytes: []byte("key1")}}})
	if err != nil {
		t.Fatalf("Failed to get entry1: %v", err)
	}
	if resp1.Entries.Entries[0].Code != pbmodel.ResponseCode_RESPONSE_CODE_FOUND {
		t.Errorf("Expected FOUND response for entry1, got %v", resp1.Entries.Entries[0].Code)
	}

	// Flush entries with block numbers <= 200
	if err := cacheStore.FlushUpToBlock(200, false); err != nil {
		t.Fatalf("Failed to flush entries: %v", err)
	}

	// Verify that entry1 and entry2 are now in the mock store
	if len(mockStore.entries) != 2 {
		t.Errorf("Expected 2 entries in mock store, got %d", len(mockStore.entries))
	}

	// Verify that entry1 and entry2 are no longer in the ForkAware by checking that wrapped store's value is returned
	mockStore.entries["key1"] = mockStoreCachedEntry{blockNumber: 100, entry: &pbmodel.Entry{Key: &pbmodel.Key{Bytes: []byte("key1")}, Value: &anypb.Any{TypeUrl: "test", Value: []byte("modified1")}}}

	resp1, err = cacheStore.Get(&pbservice.GetRequest{BlockNumber: 150, Keys: []*pbmodel.Key{{Bytes: []byte("key1")}}})
	if err != nil {
		t.Fatalf("Failed to get entry1: %v", err)
	}
	if got := string(resp1.Entries.Entries[0].Entry.Value.Value); got != "modified1" {
		t.Errorf("Expected modified value for entry1, got %s", got)
	}

	// Verify that entry3 is still in the ForkAware
	resp3, err := cacheStore.Get(&pbservice.GetRequest{BlockNumber: 350, Keys: []*pbmodel.Key{{Bytes: []byte("key3")}}})
	if err != nil {
		t.Fatalf("Failed to get entry3: %v", err)
	}
	if resp3.Entries.Entries[0].Code != pbmodel.ResponseCode_RESPONSE_CODE_FOUND {
		t.Errorf("Expected FOUND response for entry3, got %v", resp3.Entries.Entries[0].Code)
	}
	if got := string(resp3.Entries.Entries[0].Entry.Value.Value); got != "value3" {
		t.Errorf("Expected original value for entry3, got %s", got)
	}
}

func TestForkAwareGetFirst_PrefersWrappedOverCache(t *testing.T) {
	ms := newMockStore()
	fa := NewStore(ms)

	// Put a value in cache for key "a"
	cacheEntry := &pbmodel.Entry{Key: &pbmodel.Key{Bytes: []byte("a")}, Value: &anypb.Any{TypeUrl: "t", Value: []byte("cache")}}
	if err := fa.SetAll([]*pbmodel.Entry{cacheEntry}, false, 200); err != nil {
		t.Fatalf("set cache: %v", err)
	}

	// Put an older value in wrapped for the same key
	wrappedEntry := &pbmodel.Entry{Key: &pbmodel.Key{Bytes: []byte("a")}, Value: &anypb.Any{TypeUrl: "t", Value: []byte("wrapped-old")}}
	_ = ms.Set(wrappedEntry, false, 100)

	resp, err := fa.GetFirst(&pbservice.GetRequest{Keys: []*pbmodel.Key{{Bytes: []byte("a")}}})
	if err != nil {
		t.Fatalf("GetFirst: %v", err)
	}
	if resp.Entries.Entries[0].Code != pbmodel.ResponseCode_RESPONSE_CODE_FOUND {
		t.Fatalf("expected FOUND, got %v", resp.Entries.Entries[0].Code)
	}
	if got := string(resp.Entries.Entries[0].Entry.Value.Value); got != "wrapped-old" {
		t.Fatalf("expected wrapped value, got %q", got)
	}
}

func TestForkAwareGetFirst_WrappedNotFound(t *testing.T) {
	ms := newMockStore()
	fa := NewStore(ms)

	// Only cache contains candidate >= key
	cacheEntry := &pbmodel.Entry{Key: &pbmodel.Key{Bytes: []byte("b")}, Value: &anypb.Any{TypeUrl: "t", Value: []byte("cache-b")}}
	if err := fa.SetAll([]*pbmodel.Entry{cacheEntry}, false, 123); err != nil {
		t.Fatalf("set cache: %v", err)
	}

	resp, err := fa.GetFirst(&pbservice.GetRequest{Keys: []*pbmodel.Key{{Bytes: []byte("a")}}})
	if err != nil {
		t.Fatalf("GetFirst: %v", err)
	}
	// Current ForkAware implementation delegates to wrapped store, so expect NOT_FOUND when wrapped has no match
	if resp.Entries.Entries[0].Code != pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND {
		t.Fatalf("expected NOT_FOUND, got %v", resp.Entries.Entries[0].Code)
	}
}

// encodeInt64 encodes an int64 as a decimal string (matching arithmetic.go wire format)
func encodeInt64(v int64) []byte {
	return []byte(fmt.Sprintf("%d", v))
}

func decodeInt64(b []byte) int64 {
	v, _ := strconv.ParseInt(string(b), 10, 64)
	return v
}

// TestForkAwareCrossBlockADD verifies that ADD accumulation works across different blocks.
// Block 2: ADD 5 → cache holds 5
// Block 3: ADD 10 → cache should hold 15 (not 10)
func TestForkAwareCrossBlockADD(t *testing.T) {
	ms := newMockStore()
	fa := NewStore(ms)

	key := &pbmodel.Key{Bytes: []byte("counter")}

	entry1 := &pbmodel.Entry{
		Key:          key,
		Value:        &anypb.Any{Value: encodeInt64(5)},
		UpdatePolicy: pbmodel.UpdatePolicy_UPDATE_POLICY_ADD,
		ValueType:    "int64",
	}
	entry2 := &pbmodel.Entry{
		Key:          key,
		Value:        &anypb.Any{Value: encodeInt64(10)},
		UpdatePolicy: pbmodel.UpdatePolicy_UPDATE_POLICY_ADD,
		ValueType:    "int64",
	}

	if err := fa.SetAll([]*pbmodel.Entry{entry1}, false, 2); err != nil {
		t.Fatalf("block 2 set: %v", err)
	}
	if err := fa.SetAll([]*pbmodel.Entry{entry2}, false, 3); err != nil {
		t.Fatalf("block 3 set: %v", err)
	}

	resp, err := fa.Get(&pbservice.GetRequest{BlockNumber: 3, Keys: []*pbmodel.Key{key}})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if resp.Entries.Entries[0].Code != pbmodel.ResponseCode_RESPONSE_CODE_FOUND {
		t.Fatalf("expected FOUND, got %v", resp.Entries.Entries[0].Code)
	}
	got := decodeInt64(resp.Entries.Entries[0].Entry.Value.Value)
	if got != 15 {
		t.Errorf("expected accumulated value 15, got %d", got)
	}
}

// TestForkAwareSameBlockADD verifies same-block ADD accumulation and that the caller's
// original entry proto is not mutated (BUG-4).
func TestForkAwareSameBlockADD(t *testing.T) {
	ms := newMockStore()
	fa := NewStore(ms)

	key := &pbmodel.Key{Bytes: []byte("counter")}

	entry1 := &pbmodel.Entry{
		Key:          key,
		Value:        &anypb.Any{Value: encodeInt64(3)},
		UpdatePolicy: pbmodel.UpdatePolicy_UPDATE_POLICY_ADD,
		ValueType:    "int64",
	}
	entry2 := &pbmodel.Entry{
		Key:          key,
		Value:        &anypb.Any{Value: encodeInt64(7)},
		UpdatePolicy: pbmodel.UpdatePolicy_UPDATE_POLICY_ADD,
		ValueType:    "int64",
	}

	// Both at block 5
	if err := fa.SetAll([]*pbmodel.Entry{entry1}, false, 5); err != nil {
		t.Fatalf("first set: %v", err)
	}
	if err := fa.SetAll([]*pbmodel.Entry{entry2}, false, 5); err != nil {
		t.Fatalf("second set: %v", err)
	}

	// entry2's original value should NOT be mutated
	if got := decodeInt64(entry2.Value.Value); got != 7 {
		t.Errorf("caller entry2 was mutated: expected 7, got %d", got)
	}

	resp, err := fa.Get(&pbservice.GetRequest{BlockNumber: 5, Keys: []*pbmodel.Key{key}})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	got := decodeInt64(resp.Entries.Entries[0].Entry.Value.Value)
	if got != 10 {
		t.Errorf("expected 10, got %d", got)
	}
}

// TestForkAwareFlushSendsSetPolicy verifies that after cross-block ADD accumulation,
// the value flushed to the wrapped store uses UPDATE_POLICY_SET (not ADD),
// since the ForkAware layer has already resolved the accumulated value.
func TestForkAwareFlushSendsSetPolicy(t *testing.T) {
	ms := newMockStore()
	fa := NewStore(ms)

	key := &pbmodel.Key{Bytes: []byte("counter")}

	entry1 := &pbmodel.Entry{
		Key:          key,
		Value:        &anypb.Any{Value: encodeInt64(5)},
		UpdatePolicy: pbmodel.UpdatePolicy_UPDATE_POLICY_ADD,
		ValueType:    "int64",
	}
	entry2 := &pbmodel.Entry{
		Key:          key,
		Value:        &anypb.Any{Value: encodeInt64(10)},
		UpdatePolicy: pbmodel.UpdatePolicy_UPDATE_POLICY_ADD,
		ValueType:    "int64",
	}

	if err := fa.SetAll([]*pbmodel.Entry{entry1}, false, 2); err != nil {
		t.Fatalf("block 2: %v", err)
	}
	if err := fa.SetAll([]*pbmodel.Entry{entry2}, false, 3); err != nil {
		t.Fatalf("block 3: %v", err)
	}

	if err := fa.FlushUpToBlock(3, false); err != nil {
		t.Fatalf("flush: %v", err)
	}

	cached, ok := ms.entries["counter"]
	if !ok {
		t.Fatal("expected entry in wrapped store after flush")
	}
	// Value should be 15 (accumulated)
	if got := decodeInt64(cached.entry.Value.Value); got != 15 {
		t.Errorf("expected flushed value 15, got %d", got)
	}
	// Policy should be SET (not ADD) so Badger doesn't double-apply
	if cached.entry.UpdatePolicy != pbmodel.UpdatePolicy_UPDATE_POLICY_SET {
		t.Errorf("expected UPDATE_POLICY_SET in flushed entry, got %v", cached.entry.UpdatePolicy)
	}
}
