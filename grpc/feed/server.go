package feed

import (
	"context"
	"fmt"
	"time"

	pbfeed "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/feed/v2"
	pbmodel "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/model/v2"
	"go.uber.org/zap"
)

// Store is the subset of the store backend required by the feed ingest service.
type Store interface {
	SetAll(entries []*pbmodel.Entry, IfNotExist bool, blockNumber uint64) error
	SetReady(ready bool) error
}

// Server implements the Feed ingest gRPC service. External services call into
// this service to populate the store directly, instead of running a Substreams
// sink. Pushed data is treated as final and written with latest-value semantics.
type Server struct {
	pbfeed.UnimplementedFeedServer
	store  Store
	logger *zap.Logger
}

// NewServer creates a new feed ingest server backed by the given store.
func NewServer(store Store, logger *zap.Logger) *Server {
	return &Server{
		store:  store,
		logger: logger.Named("feed"),
	}
}

// Set writes a batch of entries into the store. Data is treated as final, so it
// is written at block number 0 (latest-value semantics, no fork-awareness).
func (s *Server) Set(ctx context.Context, req *pbfeed.SetRequest) (*pbfeed.SetResponse, error) {
	entries := req.GetEntries()
	if entries == nil || len(entries.Entries) == 0 {
		return &pbfeed.SetResponse{}, nil
	}

	executionStart := time.Now()
	if err := s.store.SetAll(entries.Entries, entries.IfNotExist, 0); err != nil {
		return nil, fmt.Errorf("setting entries: %w", err)
	}

	s.logger.Info("feed set",
		zap.Int("entries", len(entries.Entries)),
		zap.Bool("if_not_exist", entries.IfNotExist),
		zap.Duration("execution_time", time.Since(executionStart)),
	)
	return &pbfeed.SetResponse{}, nil
}

// SetReady persists the readiness state. While not ready, the read service
// reports block_reached = false.
func (s *Server) SetReady(ctx context.Context, req *pbfeed.SetReadyRequest) (*pbfeed.SetReadyResponse, error) {
	if err := s.store.SetReady(req.GetReady()); err != nil {
		return nil, fmt.Errorf("setting readiness: %w", err)
	}

	s.logger.Info("feed set ready", zap.Bool("ready", req.GetReady()))
	return &pbfeed.SetReadyResponse{}, nil
}
