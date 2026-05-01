package sink

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

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
	return m.SetAll([]*pbmodel.Entry{entry}, nil, IfNotExist, blockNumber)
}

func (m *SimpleMockStore) SetAll(entries []*pbmodel.Entry, deletePrefixes []string, IfNotExist bool, blockNumber uint64) error {
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

func (m *SimpleMockStore) FlushUpToBlock(blockNum uint64, IfNotExist bool) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushCalls = append(m.flushCalls, blockNum)
	return 0, nil
}

func (m *SimpleMockStore) EvictUpToBlock(upToBlockNumber uint64) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.evictCalls = append(m.evictCalls, upToBlockNumber)
	return 0, nil
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

func TestCursorSaveAndLoad(t *testing.T) {
	logger := zaptest.NewLogger(t)

	tempDir := t.TempDir()
	cursorFilePath := filepath.Join(tempDir, "test.cursor")

	testCursorStr := "XWQh1iJoYAKTDtvllL7yraWwLpc_DFhvVQvlKhhCjYGDiHqspvzCXTgfFUum8f32iBSqMQXahNirXjQmq6AKuJSypu8Sm3NpAXkk8YPs-7TvePP7OgIRBMNqNpHvBoWCMUGBFGuvfOQBoa-4TKneAQh4P55GdmL211oH1PMGIeQTsRE="
	originalCursor, err := sink.NewCursor(testCursorStr)
	if err != nil {
		t.Fatalf("Failed to create cursor from test string: %v", err)
	}

	mockStore := NewSimpleMockStore()
	handler := NewSinker(mockStore, logger, cursorFilePath, originalCursor)
	defer func() {
		handler.Shutdown(nil)
		<-handler.Terminated()
	}()

	err = SaveCursorToFile(originalCursor, cursorFilePath, logger)
	if err != nil {
		t.Fatalf("Failed to save cursor: %v", err)
	}

	if _, err := os.Stat(cursorFilePath); os.IsNotExist(err) {
		t.Fatalf("Cursor file was not created")
	}

	loadedCursor := LoadCursorFromFile(logger, cursorFilePath)
	if loadedCursor == nil {
		t.Fatalf("Failed to load cursor from file")
	}

	originalStr := originalCursor.String()
	loadedStr := loadedCursor.String()

	if originalStr != loadedStr {
		t.Errorf("Cursor mismatch:\nOriginal: %s\nLoaded:   %s", originalStr, loadedStr)
	}
}

func TestHandleBlockScopedData(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockStore := NewSimpleMockStore()

	tempDir := t.TempDir()
	cursorFilePath := filepath.Join(tempDir, "test.cursor")

	// Create test cursor
	testCursorStr := "XWQh1iJoYAKTDtvllL7yraWwLpc_DFhvVQvlKhhCjYGDiHqspvzCXTgfFUum8f32iBSqMQXahNirXjQmq6AKuJSypu8Sm3NpAXkk8YPs-7TvePP7OgIRBMNqNpHvBoWCMUGBFGuvfOQBoa-4TKneAQh4P55GdmL211oH1PMGIeQTsRE="
	testCursor, err := sink.NewCursor(testCursorStr)
	if err != nil {
		t.Fatalf("Failed to create cursor: %v", err)
	}

	handler := NewSinker(mockStore, logger, cursorFilePath, testCursor)
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

	// Verify cursor was saved
	if _, err := os.Stat(cursorFilePath); os.IsNotExist(err) {
		t.Error("Cursor file should have been saved")
	}
}

func TestHandleBlockUndoSignal(t *testing.T) {
	logger := zaptest.NewLogger(t)
	mockStore := NewSimpleMockStore()

	tempDir := t.TempDir()
	cursorFilePath := filepath.Join(tempDir, "undo_test.cursor")

	// Create test cursor
	testCursorStr := "XWQh1iJoYAKTDtvllL7yraWwLpc_DFhvVQvlKhhCjYGDiHqspvzCXTgfFUum8f32iBSqMQXahNirXjQmq6AKuJSypu8Sm3NpAXkk8YPs-7TvePP7OgIRBMNqNpHvBoWCMUGBFGuvfOQBoa-4TKneAQh4P55GdmL211oH1PMGIeQTsRE="
	testCursor, err := sink.NewCursor(testCursorStr)
	if err != nil {
		t.Fatalf("Failed to create cursor: %v", err)
	}

	handler := NewSinker(mockStore, logger, cursorFilePath, testCursor)
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

	// Verify cursor was saved
	if _, err := os.Stat(cursorFilePath); os.IsNotExist(err) {
		t.Error("Cursor file should have been saved after undo signal")
	}
}
