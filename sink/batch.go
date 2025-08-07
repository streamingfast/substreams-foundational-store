package sink

import (
	"time"

	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
)

// batchWorker runs as single writer for all batch operations (lock-free)
func (h *Handler) batchWorker() {
	defer close(h.batchWorkerDone)

	for {
		select {
		case op := <-h.batchOp:
			result := h.processBatchOperation(op)
			op.resultChan <- result

		case <-h.shutdown:
			return
		}
	}
}

// processBatchOperation handles batch operations in single-writer context (no locks needed)
func (h *Handler) processBatchOperation(op *batchOperation) *batchResult {
	result := &batchResult{}

	if op.entries != nil {
		// Add entries to batch
		var newBytes int64
		for _, entry := range op.entries {
			newBytes += int64(len(entry.Key) + len(entry.Value.Value))
		}

		h.batchBuffer = append(h.batchBuffer, op.entries...)
		h.batchSizeBytes.Add(newBytes)
		h.batchCount.Add(int32(len(op.entries)))

		// Start timer on first entry if not already started
		if len(h.batchBuffer) == len(op.entries) {
			h.batchStartTime.Store(time.Now())
		}
	}

	// Check if should flush single reader of own state
	result.shouldFlush = h.shouldFlushInternal() || op.forceFlush

	if result.shouldFlush && len(h.batchBuffer) > 0 {
		// Get pending batch and reset
		result.batch = h.batchBuffer
		result.batchBytes = h.batchSizeBytes.Load()

		// Reset
		h.batchBuffer = make([]*pbstore.Entry, 0, h.batchSize)
		h.batchSizeBytes.Store(0)
		h.batchCount.Store(0)
		h.batchStartTime.Store(time.Time{})
	}

	return result
}

// shouldFlushInternal checks flush conditions (called only from single writer)
func (h *Handler) shouldFlushInternal() bool {
	currentBytes := h.batchSizeBytes.Load()
	currentCount := h.batchCount.Load()
	startTimeValue := h.batchStartTime.Load()

	var startTime time.Time
	if startTimeValue != nil {
		startTime = startTimeValue.(time.Time)
	}

	return int(currentCount) >= h.batchSize ||
		currentBytes >= int64(h.maxBatchBytes) ||
		(!startTime.IsZero() && time.Since(startTime) > h.maxBatchTime)
}

// addToBatch using single writer pattern
func (h *Handler) addToBatch(entries []*pbstore.Entry) (*batchResult, error) {
	op := &batchOperation{
		entries:    entries,
		forceFlush: false,
		resultChan: make(chan *batchResult, 1),
	}

	// Send to single writer
	h.batchOp <- op

	// Wait for result
	result := <-op.resultChan

	return result, nil
}

// shouldFlush using atomic reads
func (h *Handler) shouldFlush() bool {
	// Fast atomic reads (no blocking)
	currentBytes := h.batchSizeBytes.Load()
	currentCount := h.batchCount.Load()
	startTimeValue := h.batchStartTime.Load()

	var startTime time.Time
	if startTimeValue != nil {
		startTime = startTimeValue.(time.Time)
	}

	return int(currentCount) >= h.batchSize ||
		currentBytes >= int64(h.maxBatchBytes) ||
		(!startTime.IsZero() && time.Since(startTime) > h.maxBatchTime)
}

// Lock-free GetPendingBatchAndReset using single writer pattern
func (h *Handler) GetPendingBatchAndReset(blockNumber uint64) ([]*pbstore.Entry, int) {
	op := &batchOperation{
		entries:    nil,
		forceFlush: true, // Force flush for manual calls
		resultChan: make(chan *batchResult, 1),
	}

	// Send to single writer
	h.batchOp <- op

	// Wait for result
	result := <-op.resultChan

	return result.batch, int(result.batchBytes)
}
