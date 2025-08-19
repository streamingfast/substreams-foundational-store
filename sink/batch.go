package sink

import (
	"time"

	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
)

// addToBatch adds entries to the batch buffer with proper synchronization
func (s *Sinker) addToBatch(entries []*pbstore.Entry) error {

	s.batchMutex.Lock()
	defer s.batchMutex.Unlock()

	if len(s.batchBuffer) == 0 {
		s.batchStartTime = time.Now()
	}

	var newBytes int
	for _, entry := range entries {
		newBytes += len(entry.Key) + len(entry.Value.Value)
	}

	// Add entries to batch buffer
	s.batchBuffer = append(s.batchBuffer, entries...)
	s.batchSizeBytes += newBytes

	return nil
}

// GetPendingBatchAndReset returns the pending batch and its size in bytes, then resets the batch state
func (s *Sinker) GetPendingBatchAndReset(blockNumber uint64) ([]*pbstore.Entry, int) {

	s.batchMutex.Lock()
	defer s.batchMutex.Unlock()

	if len(s.batchBuffer) == 0 {
		return nil, 0
	}

	// take ownership of current batch buffer
	batchBuffer := s.batchBuffer
	batchBytes := s.batchSizeBytes

	// Reset batch state with fresh slice (pre-allocate capacity)
	s.batchBuffer = make([]*pbstore.Entry, 0, s.batchSize)
	s.batchSizeBytes = 0
	s.batchStartTime = time.Time{}

	return batchBuffer, batchBytes
}
