package grpc

import (
	"context"
	"strings"
	"sync"

	"github.com/streamingfast/dauth"
	dauthgrpc "github.com/streamingfast/dauth/grpc"
	dauthgrpcmw "github.com/streamingfast/dauth/middleware/grpc"
	dauthnull "github.com/streamingfast/dauth/null"
	dauthtrust "github.com/streamingfast/dauth/trust"
	paymentGatewayAuth "github.com/streamingfast/payment-gateway/auth"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AuthConfig configures optional JWT authentication and organization scoping
// for the foundational-store gRPC server. When disabled the server accepts
// unauthenticated requests (local dev / P1 without auth flags).
type AuthConfig struct {
	Authenticator  dauth.Authenticator
	OrganizationID string
}

// Enabled reports whether auth interceptors should be installed.
func (c AuthConfig) Enabled() bool {
	return c.Authenticator != nil
}

var registerAuthPluginsOnce sync.Once

// RegisterAuthPlugins registers dauth plugins used by foundational-store. It is
// safe to call multiple times (public and internal listeners both invoke it).
//
// The "trust" plugin is used by the internal listener: it trusts identity
// headers (e.g. x-organization-id, x-api-key-id) forwarded by internal callers
// such as Substreams tier1, which authenticate the end user upstream and only
// propagate the resulting trusted headers (no end-user JWT/api-key reaches the
// internal hop).
func RegisterAuthPlugins() {
	registerAuthPluginsOnce.Do(func() {
		dauthgrpc.Register()
		dauthnull.Register()
		dauthtrust.Register()
		paymentGatewayAuth.Register()
	})
}

func authUnaryInterceptors(cfg AuthConfig, logger *zap.Logger) []grpc.UnaryServerInterceptor {
	if !cfg.Enabled() {
		return nil
	}
	authLogger := logger.Named("auth")
	chain := []grpc.UnaryServerInterceptor{
		skipHealthUnary(dauthgrpcmw.UnaryAuthChecker(cfg.Authenticator, authLogger)),
	}
	if cfg.OrganizationID != "" {
		chain = append(chain, organizationUnaryChecker(cfg.OrganizationID, authLogger))
	}
	return chain
}

func authStreamInterceptors(cfg AuthConfig, logger *zap.Logger) []grpc.StreamServerInterceptor {
	if !cfg.Enabled() {
		return nil
	}
	authLogger := logger.Named("auth")
	chain := []grpc.StreamServerInterceptor{
		skipHealthStream(dauthgrpcmw.StreamAuthChecker(cfg.Authenticator, authLogger)),
	}
	if cfg.OrganizationID != "" {
		chain = append(chain, organizationStreamChecker(cfg.OrganizationID, authLogger))
	}
	return chain
}

func skipHealthUnary(next grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if isHealthMethod(info.FullMethod) {
			return handler(ctx, req)
		}
		return next(ctx, req, info, handler)
	}
}

func skipHealthStream(next grpc.StreamServerInterceptor) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if isHealthMethod(info.FullMethod) {
			return handler(srv, ss)
		}
		return next(srv, ss, info, handler)
	}
}

func isHealthMethod(fullMethod string) bool {
	return strings.HasPrefix(fullMethod, "/grpc.health.v1.Health/")
}

func organizationUnaryChecker(organizationID string, logger *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if isHealthMethod(info.FullMethod) {
			return handler(ctx, req)
		}
		if err := checkOrganization(ctx, organizationID, logger); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func organizationStreamChecker(organizationID string, logger *zap.Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if isHealthMethod(info.FullMethod) {
			return handler(srv, ss)
		}
		if err := checkOrganization(ss.Context(), organizationID, logger); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

func checkOrganization(ctx context.Context, organizationID string, logger *zap.Logger) error {
	trusted := dauth.FromContext(ctx)
	if trusted == nil {
		return status.Error(codes.Unauthenticated, "missing authenticated context")
	}
	callerOrg := trusted.OrganizationID()
	if callerOrg == "" {
		return status.Error(codes.Unauthenticated, "missing organization id in token")
	}
	if callerOrg != organizationID {
		logger.Warn("organization id mismatch",
			zap.String("expected", organizationID),
			zap.String("actual", callerOrg),
		)
		return status.Error(codes.PermissionDenied, "organization id mismatch")
	}
	return nil
}
