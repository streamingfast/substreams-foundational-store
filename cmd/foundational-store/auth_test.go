package main

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestLoadAuthConfig_RequiresBothFlags(t *testing.T) {
	cmd := &cobra.Command{}
	addAuthFlags(cmd)

	_, err := cmd.Flags().GetString("common-auth-plugin")
	require.NoError(t, err)

	require.NoError(t, cmd.Flags().Set("organization-id", "org-1"))
	_, err = loadAuthConfig(cmd, zap.NewNop())
	require.Error(t, err)
	require.Contains(t, err.Error(), "common-auth-plugin")

	require.NoError(t, cmd.Flags().Set("organization-id", ""))
	require.NoError(t, cmd.Flags().Set("common-auth-plugin", "grpc://auth:9000"))
	_, err = loadAuthConfig(cmd, zap.NewNop())
	require.Error(t, err)
	require.Contains(t, err.Error(), "organization-id")
}

func TestLoadAuthConfig_DisabledWhenUnset(t *testing.T) {
	cmd := &cobra.Command{}
	addAuthFlags(cmd)

	cfg, err := loadAuthConfig(cmd, zap.NewNop())
	require.NoError(t, err)
	require.False(t, cfg.Enabled())
}
