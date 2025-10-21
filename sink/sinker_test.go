package sink

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
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
	Entries     []*pbstore.Entry
	BlockNumber uint64
}

func NewSimpleMockStore() *SimpleMockStore {
	return &SimpleMockStore{
		setAllCalls: make([]SimpleSetAllCall, 0),
		flushCalls:  make([]uint64, 0),
		evictCalls:  make([]uint64, 0),
	}
}

func (m *SimpleMockStore) Set(entry *pbstore.Entry, blockNumber uint64) error {
	return m.SetAll([]*pbstore.Entry{entry}, blockNumber)
}

func (m *SimpleMockStore) SetAll(entries []*pbstore.Entry, blockNumber uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entriesCopy := make([]*pbstore.Entry, len(entries))
	copy(entriesCopy, entries)

	m.setAllCalls = append(m.setAllCalls, SimpleSetAllCall{
		Entries:     entriesCopy,
		BlockNumber: blockNumber,
	})
	return nil
}

func (m *SimpleMockStore) Get(request *pbstore.GetRequest) (*pbstore.GetResponse, error) {
	return &pbstore.GetResponse{Code: pbstore.ResponseCode_RESPONSE_CODE_NOT_FOUND}, nil
}

func (m *SimpleMockStore) GetAll(request *pbstore.GetAllRequest) (*pbstore.GetAllResponse, error) {
	return &pbstore.GetAllResponse{Entries: []*pbstore.ResponseEntry{}}, nil
}

func (m *SimpleMockStore) FlushUpToBlock(blockNum uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushCalls = append(m.flushCalls, blockNum)
	return nil
}

func (m *SimpleMockStore) EvictUpToBlock(upToBlockNumber uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.evictCalls = append(m.evictCalls, upToBlockNumber)
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

func TestCursorSaveAndLoad(t *testing.T) {
	logger := zaptest.NewLogger(t)

	tempDir := t.TempDir()
	cursorFilePath := filepath.Join(tempDir, "test.cursor")

	mockStore := NewSimpleMockStore()
	handler := NewSinker(mockStore, logger, cursorFilePath)
	defer func() {
		handler.Shutdown(nil)
		<-handler.Terminated()
	}()

	testCursorStr := "XWQh1iJoYAKTDtvllL7yraWwLpc_DFhvVQvlKhhCjYGDiHqspvzCXTgfFUum8f32iBSqMQXahNirXjQmq6AKuJSypu8Sm3NpAXkk8YPs-7TvePP7OgIRBMNqNpHvBoWCMUGBFGuvfOQBoa-4TKneAQh4P55GdmL211oH1PMGIeQTsRE="

	originalCursor, err := sink.NewCursor(testCursorStr)
	if err != nil {
		t.Fatalf("Failed to create cursor from test string: %v", err)
	}

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

	handler := NewSinker(mockStore, logger, cursorFilePath)
	defer func() {
		handler.Shutdown(nil)
		<-handler.Terminated()
	}()

	// Create test entries
	entries := []*pbstore.Entry{
		{
			Key: []byte("test_key_1"),
			Value: &anypb.Any{
				TypeUrl: "test.Entry",
				Value:   []byte("test_value_1"),
			},
		},
		{
			Key: []byte("test_key_2"),
			Value: &anypb.Any{
				TypeUrl: "test.Entry",
				Value:   []byte("test_value_2"),
			},
		},
	}

	// Create entries wrapper
	entriesWrapper := &pbstore.Entries{
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

	// Create test cursor
	testCursorStr := "XWQh1iJoYAKTDtvllL7yraWwLpc_DFhvVQvlKhhCjYGDiHqspvzCXTgfFUum8f32iBSqMQXahNirXjQmq6AKuJSypu8Sm3NpAXkk8YPs-7TvePP7OgIRBMNqNpHvBoWCMUGBFGuvfOQBoa-4TKneAQh4P55GdmL211oH1PMGIeQTsRE="
	testCursor, err := sink.NewCursor(testCursorStr)
	if err != nil {
		t.Fatalf("Failed to create cursor: %v", err)
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

	handler := NewSinker(mockStore, logger, cursorFilePath)
	defer func() {
		handler.Shutdown(nil)
		<-handler.Terminated()
	}()

	// Create test cursor
	testCursorStr := "XWQh1iJoYAKTDtvllL7yraWwLpc_DFhvVQvlKhhCjYGDiHqspvzCXTgfFUum8f32iBSqMQXahNirXjQmq6AKuJSypu8Sm3NpAXkk8YPs-7TvePP7OgIRBMNqNpHvBoWCMUGBFGuvfOQBoa-4TKneAQh4P55GdmL211oH1PMGIeQTsRE="
	testCursor, err := sink.NewCursor(testCursorStr)
	if err != nil {
		t.Fatalf("Failed to create cursor: %v", err)
	}

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
