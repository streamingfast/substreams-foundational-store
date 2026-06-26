package grpc

import (
	"context"
	"fmt"
	"math"
	"time"

	dgrpcServer "github.com/streamingfast/dgrpc/server"
	"github.com/streamingfast/dgrpc/server/factory"
	"github.com/streamingfast/shutter"
	feed "github.com/streamingfast/substreams-foundational-store/grpc/feed"
	legacy "github.com/streamingfast/substreams-foundational-store/grpc/legacy"
	pbfeed "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/feed/v2"
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
	readyFunc        CheckReady
	feedServer       pbfeed.FeedServer
	logger           *zap.Logger
}

type FetchHeadBlock func() uint64

// CheckReady reports whether the store is ready to serve reads. When it returns
// false, read requests respond with block_reached = false.
type CheckReady func() (bool, error)

// RemoteFeedStore is the store backend used in "remote-feed" ingest mode. It
// must support reads, batch writes, and a persisted readiness flag.
type RemoteFeedStore interface {
	store.Store
	SetReady(ready bool) error
	IsReady() (bool, error)
}

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

// NewRemoteFeedServer creates a server for the "remote-feed" ingest mode. It
// serves the read service (Get/GetFirst) gated by the persisted readiness flag,
// alongside the Feed ingest service (Set/SetReady), on the same address.
func NewRemoteFeedServer(store RemoteFeedStore, logger *zap.Logger) *GrpcServer {
	return &GrpcServer{
		Shutter:          shutter.New(),
		store:            store,
		dgrpcServer:      nil,
		headBlockFetcher: func() uint64 { return math.MaxUint64 },
		readyFunc:        store.IsReady,
		feedServer:       feed.NewServer(store, logger),
		logger:           logger,
	}
}

// Get implements the unified Get method of the Store service (multi-keys)
func (s *GrpcServer) Get(ctx context.Context, req *pbservice.GetRequest) (*pbservice.GetResponse, error) {
	if ready, err := s.checkReady(); err != nil {
		return nil, err
	} else if !ready {
		return &pbservice.GetResponse{BlockReached: false}, nil
	}

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
	if ready, err := s.checkReady(); err != nil {
		return nil, err
	} else if !ready {
		return &pbservice.GetResponse{BlockReached: false}, nil
	}

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

// checkReady reports the store readiness. When no readiness function is
// configured (normal serving mode), the store is always considered ready.
func (s *GrpcServer) checkReady() (bool, error) {
	if s.readyFunc == nil {
		return true, nil
	}
	ready, err := s.readyFunc()
	if err != nil {
		return false, fmt.Errorf("checking store readiness: %w", err)
	}
	return ready, nil
}

func (s *GrpcServer) Run(addr string, auth AuthConfig, opts ...grpc.ServerOption) {
	// Create the dgrpc server with reduced per-call logging
	grpcLogger := s.logger.Named("grpc").WithOptions(zap.IncreaseLevel(zap.WarnLevel))
	serverOptions := []dgrpcServer.Option{
		dgrpcServer.WithLogger(grpcLogger),
		dgrpcServer.WithPlainTextServer(),
		dgrpcServer.WithGRPCServerOptions(opts...),
	}
	for _, interceptor := range authUnaryInterceptors(auth, s.logger) {
		serverOptions = append(serverOptions, dgrpcServer.WithPostUnaryInterceptor(interceptor))
	}
	for _, interceptor := range authStreamInterceptors(auth, s.logger) {
		serverOptions = append(serverOptions, dgrpcServer.WithPostStreamInterceptor(interceptor))
	}
	s.dgrpcServer = factory.ServerFromOptions(
		append(serverOptions,
			dgrpcServer.WithRegisterService(func(gs *grpc.Server) {
				pbservice.RegisterStoreServer(gs, s)
				pbstore.RegisterStoreServer(gs, legacy.NewServer(s.store, s.headBlockFetcher, s.logger))
				if s.feedServer != nil {
					pbfeed.RegisterFeedServer(gs, s.feedServer)
				}
			}),
			dgrpcServer.WithHealthCheck(dgrpcServer.HealthCheckOverGRPC|dgrpcServer.HealthCheckOverHTTP, healthCheck),
		)...,
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
