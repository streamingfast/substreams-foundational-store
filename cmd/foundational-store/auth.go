package main

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/streamingfast/dauth"
	"github.com/streamingfast/substreams-foundational-store/grpc"
	"go.uber.org/zap"
)

func addAuthFlags(cmd *cobra.Command) {
	cmd.Flags().String("common-auth-plugin", "", "Auth plugin URI (e.g. tgm://auth.staging.thegraph.market); unset disables JWT auth")
	cmd.Flags().String("organization-id", "", "Reject calls when the JWT organization id does not match this value")
}

func loadAuthConfig(cmd *cobra.Command, logger *zap.Logger) (grpc.AuthConfig, error) {
	plugin, err := cmd.Flags().GetString("common-auth-plugin")
	if err != nil {
		return grpc.AuthConfig{}, err
	}
	orgID, err := cmd.Flags().GetString("organization-id")
	if err != nil {
		return grpc.AuthConfig{}, err
	}

	plugin = strings.TrimSpace(plugin)
	orgID = strings.TrimSpace(orgID)

	if plugin == "" {
		if orgID != "" {
			return grpc.AuthConfig{}, fmt.Errorf("organization-id requires common-auth-plugin")
		}
		return grpc.AuthConfig{}, nil
	}

	if orgID == "" {
		return grpc.AuthConfig{}, fmt.Errorf("common-auth-plugin requires organization-id")
	}

	grpc.RegisterAuthPlugins()
	authenticator, err := dauth.New(plugin, logger)
	if err != nil {
		return grpc.AuthConfig{}, fmt.Errorf("initialize auth plugin: %w", err)
	}

	return grpc.AuthConfig{
		Authenticator:  authenticator,
		OrganizationID: orgID,
	}, nil
}
