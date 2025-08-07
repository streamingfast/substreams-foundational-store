package sink

import (
	"time"

	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	sink "github.com/streamingfast/substreams/sink"
	"go.uber.org/zap"
)

// flushRequest represents a pending flush operation
type flushRequest struct {
	blockNumber uint64
	cursor      *sink.Cursor
	batch       []*pbstore.Entry
	batchBytes  int
}


// FlushPendingBatch forces a flush of any pending batch entries to the async queue
func (h *Handler) FlushPendingBatch(blockNumber uint64) error {
	// Force flush any pending batch by getting it and sending to async queue
	batch, batchBytes := h.GetPendingBatchAndReset(blockNumber)
	if len(batch) == 0 {
		return nil
	}

	// Create a flush request but don't wait for completion (truly async)
	req := &flushRequest{
		blockNumber: blockNumber,
		cursor:      nil, // No cursor save needed for manual flush
		batch:       batch,
		batchBytes:  batchBytes,
	}

	// Try to send to queue, but don't block if it's full
	select {
	case h.flushQueue <- req:
		FlushQueueDepth.SetUint64(uint64(len(h.flushQueue)))
	default:
		h.logger.Warn("Could not queue manual flush - queue full")
	}
	return nil
}

// flushWorker runs in the background and processes flush requests sequentially
// This is where the truly async magic happens - I/O operations run without blocking the main processing
func (h *Handler) flushWorker() {
	defer close(h.flushWorkerDone)

	for {
		select {
		case req := <-h.flushQueue:

			// Update queue depth after dequeuing
			FlushQueueDepth.SetUint64(uint64(len(h.flushQueue)))

			asyncFlushStart := time.Now()

			// First process the batch data if we have any
			if len(req.batch) > 0 {
				setAllStart := time.Now()
				err := h.store.SetAll(req.batch, req.blockNumber)
				StoreSetAllDuration.ObserveDuration(time.Since(setAllStart))

				if err != nil {
					AsyncFlushDuration.ObserveDuration(time.Since(asyncFlushStart))
					h.logger.Error("Failed to store batch to database",
						zap.Uint64("block", req.blockNumber),
						zap.Int("batch_size", len(req.batch)),
						zap.Int("batch_bytes", req.batchBytes),
						zap.Error(err))
					h.flushError <- err
					return
				}

				h.logger.Debug("Successfully stored batch",
					zap.Uint64("block", req.blockNumber),
					zap.Int("batch_size", len(req.batch)),
					zap.Int("batch_bytes", req.batchBytes))
			}

			// Then flush to disk
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

			// After successful flush, save the cursor (if provided)
			if req.cursor != nil {
				if err := h.saveCursorToFile(req.cursor); err != nil {
					CursorSaveErrors.Inc()
					AsyncFlushDuration.ObserveDuration(time.Since(asyncFlushStart))
					h.logger.Error("Failed to save cursor after flush",
						zap.Uint64("block", req.blockNumber),
						zap.Error(err))
					h.flushError <- err
					return
				}
			}

			AsyncFlushDuration.ObserveDuration(time.Since(asyncFlushStart))

		case <-h.shutdown:
			h.logger.Info("Flush worker shutting down")
			return
		}
	}
}
