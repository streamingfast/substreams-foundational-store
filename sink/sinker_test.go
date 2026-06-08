package sink

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/streamingfast/bstream"
	pbmodel "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/model/v2"
	pbservice "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/service/v2"
	"github.com/streamingfast/substreams-foundational-store/store"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	sink "github.com/streamingfast/substreams/sink"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/types/known/anypb"
)

// Simplified test with only cursor tests and basic functionality

var _ store.ForkawareStore = (*SimpleMockStore)(nil)

type SimpleMockStore struct {
	mu          sync.Mutex
	setAllCalls []SimpleSetAllCall
	flushCalls  []uint64
	evictCalls  []uint64
}

type SimpleSetAllCall struct {
	Entries     []*pbmodel.Entry
	IfNotExist  bool
	BlockNumber uint64
}

func NewSimpleMockStore() *SimpleMockStore {
	return &SimpleMockStore{
		setAllCalls: make([]SimpleSetAllCall, 0),
		flushCalls:  make([]uint64, 0),
		evictCalls:  make([]uint64, 0),
	}
}

func (m *SimpleMockStore) Set(entry *pbmodel.Entry, IfNotExist bool, blockNumber uint64) error {
	return m.SetAll([]*pbmodel.Entry{entry}, IfNotExist, blockNumber)
}

func (m *SimpleMockStore) SetAll(entries []*pbmodel.Entry, IfNotExist bool, blockNumber uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entriesCopy := make([]*pbmodel.Entry, len(entries))
	copy(entriesCopy, entries)

	m.setAllCalls = append(m.setAllCalls, SimpleSetAllCall{
		Entries:     entriesCopy,
		IfNotExist:  IfNotExist,
		BlockNumber: blockNumber,
	})
	return nil
}

func (m *SimpleMockStore) Get(request *pbservice.GetRequest) (*pbservice.GetResponse, error) {
	// Always return NOT_FOUND for simplicity
	entries := make([]*pbmodel.QueriedEntry, len(request.Keys))
	for i := range entries {
		entries[i] = &pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND}
	}
	return &pbservice.GetResponse{BlockReached: true, Entries: &pbmodel.QueriedEntries{Entries: entries}}, nil
}

func (m *SimpleMockStore) GetFirst(request *pbservice.GetRequest) (*pbservice.GetResponse, error) {
	// Always return NOT_FOUND for simplicity
	entries := make([]*pbmodel.QueriedEntry, len(request.Keys))
	for i := range entries {
		entries[i] = &pbmodel.QueriedEntry{Code: pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND}
	}
	return &pbservice.GetResponse{BlockReached: true, Entries: &pbmodel.QueriedEntries{Entries: entries}}, nil
}

func (m *SimpleMockStore) FlushUpToBlock(blockNum uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushCalls = append(m.flushCalls, blockNum)
	return nil
}

func (m *SimpleMockStore) EvictAfterBlock(blockNumber uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.evictCalls = append(m.evictCalls, blockNumber)
	return nil
}

func (m *SimpleMockStore) GetSetAllCalls() []SimpleSetAllCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	calls := make([]SimpleSetAllCall, len(m.setAllCalls))
	copy(calls, m.setAllCalls)
	return calls
}

func (m *SimpleMockStore) GetFlushCalls() []uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	calls := make([]uint64, len(m.flushCalls))
	copy(calls, m.flushCalls)
	return calls
}

func TestLastBlockSaveAndLoad(t *testing.T) {
	logger := zaptest.NewLogger(t)

	tempDir := t.TempDir()
	blockFilePath := filepath.Join(tempDir, "state.block")

	if err := SaveLastBlockToFile(25088807, blockFilePath, logger); err != nil {
		t.Fatalf("Failed to save last block: %v", err)
	}

	if _, err := os.Stat(blockFilePath); os.IsNotExist(err) {
		t.Fatalf("Block file was not created")
	}

	loaded, ok := LoadLastBlockFromFile(logger, blockFilePath)
	if !ok {
		t.Fatalf("Failed to load last block from file")
	}
	if loaded != 25088807 {
		t.Errorf("Block mismatch: expected 25088807, got %d", loaded)
	}
}

func TestLoadLastBlockMissingFile(t *testing.T) {
	logger := zaptest.NewLogger(t)
	blockFilePath := filepath.Join(t.TempDir(), "does-not-exist.block")

	if _, ok := LoadLastBlockFromFile(logger, blockFilePath); ok {
		t.Errorf("Expected missing file to report not-found")
	}
}

func TestLoadLastBlockMigratesLegacyCursor(t *testing.T) {
	logger := zaptest.NewLogger(t)
	blockFilePath := filepath.Join(t.TempDir(), "state.cursor")

	legacyCursorStr := "XWQh1iJoYAKTDtvllL7yraWwLpc_DFhvVQvlKhhCjYGDiHqspvzCXTgfFUum8f32iBSqMQXahNirXjQmq6AKuJSypu8Sm3NpAXkk8YPs-7TvePP7OgIRBMNqNpHvBoWCMUGBFGuvfOQBoa-4TKneAQh4P55GdmL211oH1PMGIeQTsRE="
	legacyCursor, err := sink.NewCursor(legacyCursorStr)
	if err != nil {
		t.Fatalf("Failed to create legacy cursor: %v", err)
	}
	if err := os.WriteFile(blockFilePath, []byte(legacyCursorStr), 0644); err != nil {
		t.Fatalf("Failed to write legacy cursor file: %v", err)
	}

	loaded, ok := LoadLastBlockFromFile(logger, blockFilePath)
	if !ok {
		t.Fatalf("Failed to migrate legacy cursor file")
	}
	if loaded != legacyCursor.LIB.Num() {
		t.Errorf("Migrated block mismatch: expected %d, got %d", legacyCursor.LIB.Num(), loaded)
	}
}

func TestHandleBlockScopedData(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockStore := NewSimpleMockStore()

	tempDir := t.TempDir()
	blockFilePath := filepath.Join(tempDir, "state.block")

	// Create test cursor
	testCursorStr := "XWQh1iJoYAKTDtvllL7yraWwLpc_DFhvVQvlKhhCjYGDiHqspvzCXTgfFUum8f32iBSqMQXahNirXjQmq6AKuJSypu8Sm3NpAXkk8YPs-7TvePP7OgIRBMNqNpHvBoWCMUGBFGuvfOQBoa-4TKneAQh4P55GdmL211oH1PMGIeQTsRE="
	testCursor, err := sink.NewCursor(testCursorStr)
	if err != nil {
		t.Fatalf("Failed to create cursor: %v", err)
	}

	handler := NewSinker(mockStore, logger, blockFilePath, 0)
	defer func() {
		handler.Shutdown(nil)
		<-handler.Terminated()
	}()

	// Create test entries
	entries := []*pbmodel.Entry{
		{
			Key: &pbmodel.Key{Bytes: []byte("test_key_1")},
			Value: &anypb.Any{
				TypeUrl: "test.Entry",
				Value:   []byte("test_value_1"),
			},
		},
		{
			Key: &pbmodel.Key{Bytes: []byte("test_key_2")},
			Value: &anypb.Any{
				TypeUrl: "test.Entry",
				Value:   []byte("test_value_2"),
			},
		},
	}

	// Create entries wrapper
	entriesWrapper := &pbmodel.SinkEntries{
		Entries: entries,
	}

	anyValue, err := anypb.New(entriesWrapper)
	if err != nil {
		t.Fatalf("Failed to create Any value: %v", err)
	}

	// Create test data
	data := &pbsubstreamsrpc.BlockScopedData{
		Output: &pbsubstreamsrpc.MapModuleOutput{
			MapOutput: anyValue,
		},
		Clock: &pbsubstreams.Clock{
			Number: 1000,
			Id:     "test_block_id",
		},
	}

	// Handle the block scoped data
	err = handler.HandleBlockScopedData(context.Background(), data, nil, testCursor)
	if err != nil {
		t.Fatalf("HandleBlockScopedData failed: %v", err)
	}

	// Verify that SetAll was called
	setAllCalls := mockStore.GetSetAllCalls()
	if len(setAllCalls) != 1 {
		t.Errorf("Expected 1 SetAll call, got %d", len(setAllCalls))
	}

	if len(setAllCalls) > 0 && len(setAllCalls[0].Entries) != 2 {
		t.Errorf("Expected 2 entries in SetAll call, got %d", len(setAllCalls[0].Entries))
	}

	// Verify that FlushUpToBlock was called
	flushCalls := mockStore.GetFlushCalls()
	if len(flushCalls) != 1 {
		t.Errorf("Expected 1 FlushUpToBlock call, got %d", len(flushCalls))
	}

	// Verify the last (irreversible) block was persisted at the LIB
	savedBlock, ok := LoadLastBlockFromFile(logger, blockFilePath)
	if !ok {
		t.Fatal("Block file should have been saved")
	}
	if savedBlock != testCursor.LIB.Num() {
		t.Errorf("Expected saved block %d, got %d", testCursor.LIB.Num(), savedBlock)
	}
}

func TestHandleBlockUndoSignal(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockStore := NewSimpleMockStore()

	tempDir := t.TempDir()
	blockFilePath := filepath.Join(tempDir, "undo_test.block")

	// Create test cursor
	testCursorStr := "XWQh1iJoYAKTDtvllL7yraWwLpc_DFhvVQvlKhhCjYGDiHqspvzCXTgfFUum8f32iBSqMQXahNirXjQmq6AKuJSypu8Sm3NpAXkk8YPs-7TvePP7OgIRBMNqNpHvBoWCMUGBFGuvfOQBoa-4TKneAQh4P55GdmL211oH1PMGIeQTsRE="
	testCursor, err := sink.NewCursor(testCursorStr)
	if err != nil {
		t.Fatalf("Failed to create cursor: %v", err)
	}

	handler := NewSinker(mockStore, logger, blockFilePath, 0)
	defer func() {
		handler.Shutdown(nil)
		<-handler.Terminated()
	}()

	// Create undo signal
	undoSignal := &pbsubstreamsrpc.BlockUndoSignal{
		LastValidBlock: &pbsubstreams.BlockRef{
			Number: 999,
		},
	}

	// Handle the undo signal
	err = handler.HandleBlockUndoSignal(context.Background(), undoSignal, testCursor)
	if err != nil {
		t.Fatalf("HandleBlockUndoSignal failed: %v", err)
	}

	// Verify that evict was called
	if len(mockStore.evictCalls) != 1 {
		t.Errorf("Expected 1 evict call, got %d", len(mockStore.evictCalls))
	}
	if len(mockStore.evictCalls) > 0 && mockStore.evictCalls[0] != 999 {
		t.Errorf("Expected evict call with block 999, got %d", mockStore.evictCalls[0])
	}

	// An undo only affects the reversible segment, so no resume point should be written.
	if _, err := os.Stat(blockFilePath); !os.IsNotExist(err) {
		t.Error("Block file should not be written on undo signal")
	}
}

// TestHandleBlockScopedData_NoOutput verifies that a block carrying no module output still
// flushes finalized data and persists the LIB as the resume point, that the resume point is
// only rewritten when the LIB advances, and that no SetAll happens without output.
func TestHandleBlockScopedData_NoOutput(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockStore := NewSimpleMockStore()
	blockFilePath := filepath.Join(t.TempDir(), "state.block")

	handler := NewSinker(mockStore, logger, blockFilePath, 0)
	defer func() {
		handler.Shutdown(nil)
		<-handler.Terminated()
	}()

	cursorAt := func(blockNum, libNum uint64) *sink.Cursor {
		return &sink.Cursor{Cursor: &bstream.Cursor{
			Block:     bstream.NewBlockRef("blk", blockNum),
			LIB:       bstream.NewBlockRef("lib", libNum),
			HeadBlock: bstream.NewBlockRef("blk", blockNum),
		}}
	}
	noOutputAt := func(blockNum uint64) *pbsubstreamsrpc.BlockScopedData {
		return &pbsubstreamsrpc.BlockScopedData{Clock: &pbsubstreams.Clock{Number: blockNum, Id: "blk"}}
	}

	// No-output block with an advancing LIB: must flush and persist the LIB.
	if err := handler.HandleBlockScopedData(context.Background(), noOutputAt(1000), nil, cursorAt(1000, 900)); err != nil {
		t.Fatalf("HandleBlockScopedData failed: %v", err)
	}
	if got := mockStore.GetFlushCalls(); len(got) != 1 || got[0] != 900 {
		t.Fatalf("Expected a single flush at lib 900, got %v", got)
	}
	if got := mockStore.GetSetAllCalls(); len(got) != 0 {
		t.Errorf("Expected no SetAll on a no-output block, got %d", len(got))
	}
	if saved, ok := LoadLastBlockFromFile(logger, blockFilePath); !ok || saved != 900 {
		t.Fatalf("Expected saved block 900, got %d (ok=%v)", saved, ok)
	}

	// Remove the file so we can detect whether the next call rewrites it.
	if err := os.Remove(blockFilePath); err != nil {
		t.Fatalf("remove block file: %v", err)
	}

	// LIB does not advance: still flushes, but must NOT rewrite the resume point.
	if err := handler.HandleBlockScopedData(context.Background(), noOutputAt(1001), nil, cursorAt(1001, 900)); err != nil {
		t.Fatalf("HandleBlockScopedData failed: %v", err)
	}
	if got := mockStore.GetFlushCalls(); len(got) != 2 {
		t.Fatalf("Expected flush to run again on the non-advancing block, got %v", got)
	}
	if _, err := os.Stat(blockFilePath); !os.IsNotExist(err) {
		t.Error("resume point should not be rewritten when the LIB does not advance")
	}

	// LIB advances again: resume point is persisted at the new LIB.
	if err := handler.HandleBlockScopedData(context.Background(), noOutputAt(1002), nil, cursorAt(1002, 950)); err != nil {
		t.Fatalf("HandleBlockScopedData failed: %v", err)
	}
	if saved, ok := LoadLastBlockFromFile(logger, blockFilePath); !ok || saved != 950 {
		t.Fatalf("Expected saved block 950 after LIB advanced, got %d (ok=%v)", saved, ok)
	}
}
