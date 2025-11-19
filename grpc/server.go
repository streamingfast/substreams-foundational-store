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
	pbssinternal "github.com/streamingfast/substreams/pb/sf/substreams/intern/v2"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
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

// Flush implements the Flush method of the Store service
func (s *GrpcServer) Flush(ctx context.Context, req *pbservice.FlushRequest) (*pbservice.FlushResponse, error) {
	// Convert operations to entries
	var entries []*pbmodel.Entry
	for _, op := range req.Operations.Operations {
		if op.Type == pbssinternal.Operation_SET {
			// op.Value is the marshaled Any
			value := &anypb.Any{}
			if err := proto.Unmarshal(op.Value, value); err != nil {
				return nil, fmt.Errorf("unmarshaling operation value: %w", err)
			}
			entry := &pbmodel.Entry{
				Key:   &pbmodel.Key{Bytes: []byte(op.Key)},
				Value: value,
			}
			entries = append(entries, entry)
		}
		// Handle other types if needed
	}

	// Use ord as block number, assume all same
	blockNumber := uint64(0)
	if len(req.Operations.Operations) > 0 {
		blockNumber = req.Operations.Operations[0].Ord
	}

	if err := s.store.SetAll(entries, false, blockNumber); err != nil {
		return nil, fmt.Errorf("setting entries from flush: %w", err)
	}

	return &pbservice.FlushResponse{}, nil
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
