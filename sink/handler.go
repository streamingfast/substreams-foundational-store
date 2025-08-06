package sink

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	"github.com/streamingfast/substreams-foundational-store/store"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	sink "github.com/streamingfast/substreams/sink"
	"go.uber.org/zap"
)

const (
	DefaultFlushQueueSize = 100
)

// LoadCursorFromFile attempts to load a cursor from the specified file.
// Returns nil if the file doesn't exist or if there's an error reading it.
func LoadCursorFromFile(logger *zap.Logger, cursorFilePath string) *sink.Cursor {
	// Use the provided cursor file path or default to "state.cursor" in the current directory
	var cursorPath string
	if cursorFilePath != "" {
		cursorPath = cursorFilePath
	} else {
		// Use the current working directory for simplicity
		dir, err := os.Getwd()
		if err != nil {
			dir = "."
		}
		cursorPath = filepath.Join(dir, "state.cursor")
	}

	data, err := os.ReadFile(cursorPath)
	if err != nil {
		if !os.IsNotExist(err) {
			logger.Warn("Failed to read cursor file", zap.Error(err))
		}
		return nil
	}

	cursorStr := string(data)
	cursor, err := sink.NewCursor(cursorStr)
	if err != nil {
		logger.Warn("Failed to create cursor from string", zap.Error(err))
		return nil
	}

	logger.Info("Loaded cursor from file", zap.String("path", cursorPath))
	return cursor
}

// flushRequest represents a pending flush op
type flushRequest struct {
	blockNumber uint64
	cursor      *sink.Cursor
}

type Handler struct {
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
	batchTimer     *time.Timer
	mu             sync.Mutex

	// Async flush fields
	flushQueue      chan *flushRequest
	flushWorkerDone chan struct{}
	flushError      chan error // for errors coming from the database, when something gets through, the world stops
	shutdown        chan struct{}
}

func NewSinker(typeUrl string, store store.ForkawareStore, logger *zap.Logger, cursorFilePath string, batchSize int, maxBatchTime time.Duration, flushQueueSize int) *Handler {
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

	handler := &Handler{
		store:          store,
		typeUrl:        typeUrl,
		logger:         logger,
		cursorFilePath: cursorFilePath,
		batchBuffer:    make([]*pbstore.Entry, 0, batchSize),
		batchSize:      batchSize,
		maxBatchTime:   maxBatchTime,
		// this represents ~80% badger size
		maxBatchBytes:   8 * 1024 * 1024,
		batchSizeBytes:  0,
		flushQueue:      make(chan *flushRequest, flushQueueSize),
		flushWorkerDone: make(chan struct{}),
		shutdown:        make(chan struct{}),
	}

	// Start the flush worker
	go handler.flushWorker()

	return handler
}

func (h *Handler) HandleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {
	select {
	case err := <-h.flushError:
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
		if err := h.addToBatch(entries.Entries, data.GetClock().Number); err != nil {
			return fmt.Errorf("adding entries to batch: %w", err)
		}
	}

	lib := cursor.LIB.Num()

	// Submit flush request to background worker
	req := &flushRequest{
		blockNumber: lib,
		cursor:      cursor,
	}

	if !h.shouldFlush() {
		return nil
	}

	// here we do the flushing
	//
	batch, size := h.GetPendingBatchAndReset(data.Clock.Number)

	// send the batch to the queue
	_ = batch
	_ = size // for logging or metrics ?

	select {
	case h.flushQueue <- req:
		// Request queued successfully
		FlushQueueDepth.SetUint64(uint64(len(h.flushQueue)))
	case err := <-h.flushError:
		return fmt.Errorf("error during last flush: %w", err)
	case <-ctx.Done():
		return ctx.Err()
	default:
		// Queue is full - increment metric and apply backpressure
		FlushQueueFull.Inc()
		h.logger.Warn("Flush queue is full, applying backpressure",
			zap.Uint64("block", lib),
			zap.Int("queue_capacity", cap(h.flushQueue)))

		// Block until we can queue the request or context is cancelled
		select {
		case err := <-h.flushError:
			return fmt.Errorf("error during last flush: %w", err)
		case h.flushQueue <- req:
			FlushQueueDepth.SetUint64(uint64(len(h.flushQueue)))
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	//	// Wait for flush to complete
	//	select {
	//	case err := <-req.resultChan:
	//		if err != nil {
	//			return fmt.Errorf("flushing data up to block %d: %w", lib, err)
	//		}
	//	case <-ctx.Done():
	//		return ctx.Err()
	//	}

	blockNum := data.GetClock().Number
	RecordBlockProcessing(entriesCount, blockNum)

	return nil
}

// saveCursorToFile saves the cursor to a file
func (h *Handler) saveCursorToFile(cursor *sink.Cursor) error {
	if err := sink.WriteCursor(h.cursorFilePath, cursor); err != nil {
		return fmt.Errorf("writing cursor to file: %w", err)
	}
	return nil
}

func (h *Handler) addToBatch(entries []*pbstore.Entry, blockNumber uint64) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	var newBytes int
	for _, entry := range entries {
		newBytes += len(entry.Key) + len(entry.Value.Value)
	}

	// Add entries to batch buffer
	h.batchBuffer = append(h.batchBuffer, entries...)
	h.batchSizeBytes += newBytes
}

func (h *Handler) shouldFlush() bool {

	//// Start timer on first entry if not already started
	//if len(h.batchBuffer) == len(entries) {
	//	h.batchStartTime = time.Now()
	//	if h.batchTimer != nil {
	//		h.batchTimer.Stop()
	//	}
	//	h.batchTimer = time.AfterFunc(h.maxBatchTime, func() {
	//		h.mu.Lock()
	//		defer h.mu.Unlock()
	//		if len(h.batchBuffer) > 0 {
	//			h.logger.Debug("Flushing batch due to timeout",
	//				zap.Duration("elapsed", time.Since(h.batchStartTime)),
	//				zap.Int("entries", len(h.batchBuffer)))
	//			_ = h.flushBatchLocked(blockNumber)
	//		}
	//	})
	//}

	return h.batchSizeBytes >= h.maxBatchBytes //  || time.Since(h.batchStartTime) > h.maxBatchTime
	//if len(h.batchBuffer) >= h.batchSize ||
	//	h.batchSizeBytes >= h.maxBatchBytes {
	//	return h.flushBatchLocked(blockNumber)
	//}

	//return nil
}

func (h *Handler) FlushBatch(blockNumber uint64, batch []*pbstore.Entry) error {
	if len(batch) == 0 {
		return nil
	}

	setAllStart := time.Now()

	// Perform the actual store operation
	if err := h.store.SetAll(batch, blockNumber); err != nil {
		return fmt.Errorf("setting foundational-store batch: %w", err)
	}

	StoreSetAllDuration.ObserveDuration(time.Since(setAllStart))
	h.logger.Debug("Flushed batch to store",
		zap.Int("batch_size", len(batch)),
		zap.Uint64("block_number", blockNumber))

	return nil
}

// GetPendingBatchAndReset returns the pending batch and its size in bytes, then resets the h.batchBuffer and h.batchSize on the Handler
func (h *Handler) GetPendingBatchAndReset(blockNumber uint64) ([]*pbstore.Entry, int) {
	batchBuffer := h.batchBuffer
	batchSize := h.batchSizeBytes
	h.batchBuffer = make([]*pbstore.Entry, 0)
	h.batchSize = 0
	if h.batchTimer != nil {
		h.batchTimer.Stop()
	}
	h.batchTimer = nil

	return batchBuffer, batchSize
}

func (h *Handler) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.batchTimer != nil {
		h.batchTimer.Stop()
		h.batchTimer = nil
	}

	// Flush any pending entries
	if err := h.flushBatchLocked(0); err != nil {
		return err
	}

	// Signal shutdown to flush worker and wait for it to finish
	close(h.shutdown)
	<-h.flushWorkerDone

	return nil
}

func (h *Handler) HandleBlockUndoSignal(ctx context.Context, undoSignal *pbsubstreamsrpc.BlockUndoSignal, cursor *sink.Cursor) error {

	blockNum := undoSignal.LastValidBlock.Number

	if err := h.FlushPendingBatch(blockNum); err != nil {
		h.logger.Warn("Failed to flush pending batch before undo", zap.Error(err))
	}

	evictStart := time.Now()
	if err := h.store.EvictUpToBlock(blockNum); err != nil {
		return fmt.Errorf("failed to evict data up to block %d: %w", blockNum, err)
	}
	StoreEvictDuration.ObserveDuration(time.Since(evictStart))

	// Save the cursor to a file after handling the undo signal
	if err := h.saveCursorToFile(cursor); err != nil {
		CursorSaveErrors.Inc()
		h.logger.Warn("Failed to save cursor to file after undo signal", zap.Error(err))
		// Don't return an error here, as we don't want to fail the processing
	}

	h.logger.Debug("Evicted data due to undo signal",
		zap.Uint64("block_number", blockNum))

	return nil
}

// flushWorker runs in the background and processes flush requests sequentially
func (h *Handler) flushWorker() {
	defer close(h.flushWorkerDone)

	for {
		select {
		case req := <-h.flushQueue:

			// Update queue depth after dequeuing
			FlushQueueDepth.SetUint64(uint64(len(h.flushQueue)))

			asyncFlushStart := time.Now()
			flushStart := time.Now()
			err := h.store.FlushUpToBlock(req.blockNumber)
			StoreFlushDuration.ObserveDuration(time.Since(flushStart))

			if err != nil {
				AsyncFlushDuration.ObserveDuration(time.Since(asyncFlushStart))
				h.logger.Error("Failed to flush to database",
					zap.Uint64("block", req.blockNumber),
					zap.Error(err))
				h.flushError <- err
				return
			}

			// After successful flush, save the cursor
			if err := h.saveCursorToFile(req.cursor); err != nil {
				CursorSaveErrors.Inc()
				AsyncFlushDuration.ObserveDuration(time.Since(asyncFlushStart))
				h.logger.Error("Failed to save cursor after flush",
					zap.Uint64("block", req.blockNumber),
					zap.Error(err))
				h.flushError <- err
				return
			}

			AsyncFlushDuration.ObserveDuration(time.Since(asyncFlushStart))

		case <-h.shutdown:
			h.logger.Info("Flush worker shutting down")
			return
		}
	}
}
