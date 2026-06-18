package grpc

import (
	"context"
	"testing"

	"github.com/streamingfast/dauth"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCheckOrganization_Match(t *testing.T) {
	ctx := dauth.WithTrustedHeaders(context.Background(), dauth.TrustedHeaders{
		dauth.HeaderNewOrganizationID: "org-1",
	})
	require.NoError(t, checkOrganization(ctx, "org-1", zap.NewNop()))
}

func TestCheckOrganization_Mismatch(t *testing.T) {
	ctx := dauth.WithTrustedHeaders(context.Background(), dauth.TrustedHeaders{
		dauth.HeaderNewOrganizationID: "org-other",
	})
	err := checkOrganization(ctx, "org-1", zap.NewNop())
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.PermissionDenied, st.Code())
}

func TestOrganizationUnaryChecker_SkipsHealth(t *testing.T) {
	called := false
	interceptor := organizationUnaryChecker("org-1", zap.NewNop())
	_, err := interceptor(
		context.Background(),
		nil,
		&grpc.UnaryServerInfo{FullMethod: "/grpc.health.v1.Health/Check"},
		func(ctx context.Context, req interface{}) (interface{}, error) {
			called = true
			return nil, nil
		},
	)
	require.NoError(t, err)
	require.True(t, called)
}
