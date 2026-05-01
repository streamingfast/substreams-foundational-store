package grpc

import (
	"context"
	"fmt"
	"time"

	dgrpcServer "github.com/streamingfast/dgrpc/server"
	"github.com/streamingfast/dgrpc/server/factory"
	"github.com/streamingfast/shutter"
	legacy "github.com/streamingfast/substreams-foundational-store/grpc/legacy"
	pbmodel "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/model/v2"
	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/service/v1"
	pbservice "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/service/v2"
	"github.com/streamingfast/substreams-foundational-store/store"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// StoreServer implements the StoreKV gRPC service
type GrpcServer struct {
	*shutter.Shutter
	pbservice.UnimplementedStoreServer
	store            store.Store
	dgrpcServer      dgrpcServer.Server
	headBlockFetcher FetchHeadBlock
	logger           *zap.Logger
}

type FetchHeadBlock func() uint64

// NewStoreServer creates a new StoreServer with the given foundational-store
func NewStoreServer(store store.Store, headBlockFetcher FetchHeadBlock, logger *zap.Logger) *GrpcServer {
	return &GrpcServer{
		Shutter:          shutter.New(),
		store:            store,
		dgrpcServer:      nil,
		headBlockFetcher: headBlockFetcher,
		logger:           logger,
	}
}

// Get implements the unified Get method of the Store service (multi-keys)
func (s *GrpcServer) Get(ctx context.Context, req *pbservice.GetRequest) (*pbservice.GetResponse, error) {
	headBlock := s.headBlockFetcher()
	if headBlock < req.BlockNumber {
		return &pbservice.GetResponse{BlockReached: false}, nil
	}

	executionStart := time.Now()
	r, err := s.store.Get(req)
	if err != nil {
		return nil, fmt.Errorf("getting keys from store: %w", err)
	}

	r.BlockReached = true
	foundKeyCount := 0
	for _, entry := range r.Entries.Entries {
		if entry.Code == pbmodel.ResponseCode_RESPONSE_CODE_FOUND {
			foundKeyCount++
		}
	}
	s.logger.Info("request stats",
		zap.Uint64("block_number", req.BlockNumber),
		zap.Uint64("head_block", headBlock),
		zap.Int("requested_keys", len(req.Keys)),
		zap.Int("found_keys", foundKeyCount),
		zap.Duration("execution_time", time.Since(executionStart)),
		zap.Bool("keep", false),
	)
	return r, nil
}

// GetFirst implements the unified GetFirst method of the Store service (multi-keys)
func (s *GrpcServer) GetFirst(ctx context.Context, req *pbservice.GetRequest) (*pbservice.GetResponse, error) {
	headBlock := s.headBlockFetcher()
	if headBlock < req.BlockNumber {
		return &pbservice.GetResponse{BlockReached: false}, nil
	}

	executionStart := time.Now()
	r, err := s.store.GetFirst(req)
	if err != nil {
		return nil, fmt.Errorf("getting first keys from store: %w", err)
	}

	r.BlockReached = true

	s.logger.Info("request stats",
		zap.Uint64("block_number", req.BlockNumber),
		zap.Uint64("head_block", headBlock),
		zap.Int("requested_keys", len(req.Keys)),
		zap.Int("found_keys", len(r.Entries.Entries)),
		zap.Duration("execution_time", time.Since(executionStart)),
		zap.Bool("keep", false),
	)
	return r, nil
}

// SetAll implements the write endpoint for BadgerBackedStore to persist operations
func (s *GrpcServer) SetAll(ctx context.Context, req *pbservice.SetRequest) (*pbservice.SetResponse, error) {
	executionStart := time.Now()

	if req.SinkEntries == nil {
		return &pbservice.SetResponse{EntriesWritten: 0}, nil
	}

	err := s.store.SetAll(req.SinkEntries.Entries, req.SinkEntries.DeletePrefixes, req.SinkEntries.IfNotExist, req.BlockNumber)
	if err != nil {
		return nil, fmt.Errorf("setting entries in store: %w", err)
	}

	s.logger.Info("setall stats",
		zap.Uint64("block_number", req.BlockNumber),
		zap.Int("entries_count", len(req.SinkEntries.Entries)),
		zap.Int("delete_prefixes_count", len(req.SinkEntries.DeletePrefixes)),
		zap.Bool("if_not_exist", req.SinkEntries.IfNotExist),
		zap.Duration("execution_time", time.Since(executionStart)),
	)

	return &pbservice.SetResponse{
		EntriesWritten: uint64(len(req.SinkEntries.Entries)),
	}, nil
}

// FlushUpToBlock implements the flush endpoint to persist entries up to LIB
func (s *GrpcServer) FlushUpToBlock(ctx context.Context, req *pbservice.FlushRequest) (*pbservice.FlushResponse, error) {
	executionStart := time.Now()

	// Check if store supports flushing
	forkAwareStore, ok := s.store.(store.ForkawareStore)
	if !ok {
		return nil, fmt.Errorf("store does not support FlushUpToBlock operation")
	}

	flushed, err := forkAwareStore.FlushUpToBlock(req.BlockNumber, req.IfNotExist)
	if err != nil {
		return nil, fmt.Errorf("flushing store up to block %d: %w", req.BlockNumber, err)
	}

	s.logger.Info("flush stats",
		zap.Uint64("block_number", req.BlockNumber),
		zap.Uint64("entries_flushed", flushed),
		zap.Bool("if_not_exist", req.IfNotExist),
		zap.Duration("execution_time", time.Since(executionStart)),
	)

	return &pbservice.FlushResponse{
		EntriesFlushed: flushed,
	}, nil
}

// EvictUpToBlock implements the eviction endpoint for fork handling
func (s *GrpcServer) EvictUpToBlock(ctx context.Context, req *pbservice.EvictRequest) (*pbservice.EvictResponse, error) {
	executionStart := time.Now()

	// Check if store supports eviction
	forkAwareStore, ok := s.store.(store.ForkawareStore)
	if !ok {
		return nil, fmt.Errorf("store does not support EvictUpToBlock operation")
	}

	evicted, err := forkAwareStore.EvictUpToBlock(req.BlockNumber)
	if err != nil {
		return nil, fmt.Errorf("evicting entries from block %d: %w", req.BlockNumber, err)
	}

	s.logger.Info("evict stats",
		zap.Uint64("block_number", req.BlockNumber),
		zap.Uint64("entries_evicted", evicted),
		zap.Duration("execution_time", time.Since(executionStart)),
	)

	return &pbservice.EvictResponse{
		EntriesEvicted: evicted,
	}, nil
}

func (s *GrpcServer) Run(addr string, opts ...grpc.ServerOption) {
	// Create the dgrpc server with reduced per-call logging
	grpcLogger := s.logger.Named("grpc").WithOptions(zap.IncreaseLevel(zap.WarnLevel))
	s.dgrpcServer = factory.ServerFromOptions(
		dgrpcServer.WithLogger(grpcLogger),
		dgrpcServer.WithPlainTextServer(),
		dgrpcServer.WithGRPCServerOptions(opts...),
		dgrpcServer.WithRegisterService(func(gs *grpc.Server) {
			pbservice.RegisterStoreServer(gs, s)
			pbstore.RegisterStoreServer(gs, legacy.NewServer(s.store, s.headBlockFetcher, s.logger))
		}),
		dgrpcServer.WithHealthCheck(dgrpcServer.HealthCheckOverGRPC|dgrpcServer.HealthCheckOverHTTP, healthCheck),
	)

	s.dgrpcServer.OnTerminated(func(err error) {
		s.Shutter.Shutdown(err)
	})

	s.dgrpcServer.Launch(addr)
}

func healthCheck(ctx context.Context) (isReady bool, out interface{}, err error) {
	// In your own code, you should tied the `isReady` value to the lifecycle of your application.
	// If your application is ready to accept requests, return `true`, otherwise return `false`.
	//
	// The `out` value that can anything is used by the HTTP health check (if configured in the `WithHealthCheck`
	// option by using for example `dgrpc.HealthCheckOverGRPC | dgrpc.HealthCheckOverHTTP`) will be serialized
	// in the body as JSON. It is **not** used by the GRPC health check because there is no such notion of
	// return payload.
	//
	// An error should be returned only in really rare cases, most of the time if there is an error it means
	// your application is not ready to accept requests.
	return true, nil, nil
}

func (s *GrpcServer) Shutdown(err error) {
	if server := s.dgrpcServer; server != nil {
		server.Shutdown(15 * time.Second)
	}

	s.Shutter.Shutdown(err)
}
