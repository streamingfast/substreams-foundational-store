package sink

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/streamingfast/shutter"
	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	"github.com/streamingfast/substreams-foundational-store/store"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	sink "github.com/streamingfast/substreams/sink"
	"go.uber.org/zap"
)

const (
	DefaultFlushQueueSize = 3
)

type Sinker struct {
	store          store.ForkawareStore
	typeUrl        string
	logger         *zap.Logger
	cursorFilePath string

	// Batching fields
	batchBuffer    []*pbstore.Entry
	batchSize      int
	batchSizeBytes int
	maxBatchTime   time.Duration
	maxBatchBytes  int
	batchStartTime time.Time

	batchMutex sync.Mutex

	// Flusher for async ops
	flusher *Flusher

	// Shutdown coordination
	*shutter.Shutter
}

func NewSinker(typeUrl string, store store.ForkawareStore, logger *zap.Logger, cursorFilePath string, batchSize int, maxBatchTime time.Duration, flushQueueSize int) *Sinker {
	logger = logger.Named("foundational-store-sinker")

	if batchSize <= 0 {
		batchSize = 1000
	}
	if maxBatchTime <= 0 {
		maxBatchTime = 30 * time.Second
	}
	if flushQueueSize <= 0 {
		flushQueueSize = DefaultFlushQueueSize
	}

	shutter := shutter.New()

	// Create the flusher
	flusher := NewFlusher(store, logger, shutter, batchSize, 8*1024*1024, maxBatchTime, flushQueueSize)

	sinker := &Sinker{
		store:          store,
		typeUrl:        typeUrl,
		logger:         logger,
		cursorFilePath: cursorFilePath,
		batchBuffer:    make([]*pbstore.Entry, 0, batchSize),
		batchSize:      batchSize,
		maxBatchTime:   maxBatchTime,

		// this represents ~80% badger size
		maxBatchBytes:  8 * 1024 * 1024,
		batchSizeBytes: 0,
		flusher:        flusher,
		Shutter:        shutter,
	}
	return sinker
}

func (s *Sinker) HandleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {
	// Check for async flush errors first - if flush failed, stop processing
	select {
	case err := <-s.flusher.ErrorChan():
		return fmt.Errorf("error during last flush: %w", err)
	default:
	}

	var entriesCount int

	// Process data if present
	if data.Output != nil && data.Output.MapOutput != nil && data.Output.MapOutput.Value != nil {

		entries := &pbstore.Entries{}
		if err := data.Output.MapOutput.UnmarshalTo(entries); err != nil {
			return fmt.Errorf("unmarshalling map output to Entry: %w", err)
		}

		entriesCount = len(entries.Entries)

		// Add entries to batch buffer instead of immediate insert
		if err := s.addToBatch(entries.Entries); err != nil {
			return fmt.Errorf("adding entries to batch: %w", err)
		}
	}

	lib := cursor.LIB.Num()

	if !s.shouldFlush() {
		return nil
	}

	s.FlushPendingBatch(ctx, lib, data.Clock.Number, cursor)

	blockNum := data.GetClock().Number
	RecordBlockProcessing(entriesCount, blockNum)

	return nil
}

// shouldFlush determines if the current batch should be flushed based on size, bytes, or time
func (s *Sinker) shouldFlush() bool {
	// Check if we're shutting down, if so, flush immediately
	if s.IsTerminating() {
		return true
	}
	return len(s.batchBuffer) >= s.batchSize ||
		s.batchSizeBytes >= s.maxBatchBytes ||
		(s.batchStartTime != (time.Time{}) && time.Since(s.batchStartTime) > s.maxBatchTime)
}

// FlushPendingBatch flushes any pending batch entries to the flusher
func (s *Sinker) FlushPendingBatch(ctx context.Context, blockNumber uint64, clockNum uint64, cursor *sink.Cursor) error {
	// Get the batch data to flush
	batch, batchBytes := s.GetPendingBatchAndReset(clockNum)

	if len(batch) == 0 {
		return nil // Nothing to flush
	}

	// Submit batch to flusher
	req := &BatchRequest{
		blockNumber:    blockNumber,
		cursor:         cursor,
		cursorFilePath: s.cursorFilePath,
		batch:          batch,
		batchBytes:     batchBytes,
	}

	// Submit batch to flusher (possibly blocking)
	submitted := s.flusher.SubmitBatch(req)
	select {
	case err := <-s.flusher.ErrorChan():
		return fmt.Errorf("error during last flush: %w", err)
	case <-ctx.Done():
		return ctx.Err()
	case <-submitted:
	}

	return nil
}

func (s *Sinker) HandleBlockUndoSignal(ctx context.Context, undoSignal *pbsubstreamsrpc.BlockUndoSignal, cursor *sink.Cursor) error {

	blockNum := undoSignal.LastValidBlock.Number
	lib := cursor.LIB.Num()

	if err := s.FlushPendingBatch(ctx, lib, blockNum, cursor); err != nil {
		s.logger.Warn("failed to flush pending batch before undo", zap.Error(err))
	}

	evictStart := time.Now()
	if err := s.store.EvictUpToBlock(blockNum); err != nil {
		return fmt.Errorf("failed to evict data up to block %d: %w", blockNum, err)
	}
	StoreEvictDuration.ObserveDuration(time.Since(evictStart))

	// Save the cursor to a file after handling the undo signal
	if err := SaveCursorToFile(cursor, s.cursorFilePath, s.logger); err != nil {
		CursorSaveErrors.Inc()
		s.logger.Warn("failed to save cursor to file after undo signal", zap.Error(err))
		// Don't return an error here, as we don't want to fail the processing
	}

	s.logger.Debug("evicted data due to undo signal",
		zap.Uint64("block_number", blockNum))

	return nil
}
