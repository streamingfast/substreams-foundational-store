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
	cmd.Flags().String("organization-id", "", "Reject calls when the organization id does not match this value")
	cmd.Flags().String("internal-addr", "", "When set, serve an additional internal listener on this address using --internal-auth-plugin (for internal callers such as Substreams tier1 that forward trusted identity headers instead of an end-user JWT)")
	cmd.Flags().String("internal-auth-plugin", "trust://?allowed=x-organization-id,x-user-id,x-api-key-id", "Auth plugin URI for the internal listener; the default trust:// plugin trusts the listed forwarded identity headers")
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

// loadInternalAuthConfig builds the auth config for the optional internal
// listener. It returns an empty address when --internal-addr is unset, in which
// case no internal listener should be started.
//
// The internal listener typically uses a trust:// plugin so that forwarded
// identity headers (x-organization-id, x-api-key-id) from internal callers are
// trusted without an end-user JWT. Organization scoping still applies: when
// --organization-id is set, the trusted x-organization-id must match it.
func loadInternalAuthConfig(cmd *cobra.Command, logger *zap.Logger) (cfg grpc.AuthConfig, addr string, err error) {
	addr, err = cmd.Flags().GetString("internal-addr")
	if err != nil {
		return grpc.AuthConfig{}, "", err
	}
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return grpc.AuthConfig{}, "", nil
	}

	plugin, err := cmd.Flags().GetString("internal-auth-plugin")
	if err != nil {
		return grpc.AuthConfig{}, "", err
	}
	plugin = strings.TrimSpace(plugin)

	orgID, err := cmd.Flags().GetString("organization-id")
	if err != nil {
		return grpc.AuthConfig{}, "", err
	}
	orgID = strings.TrimSpace(orgID)

	if plugin == "" {
		// Internal listener explicitly running without any auth (local dev).
		return grpc.AuthConfig{}, addr, nil
	}

	grpc.RegisterAuthPlugins()
	authenticator, err := dauth.New(plugin, logger)
	if err != nil {
		return grpc.AuthConfig{}, "", fmt.Errorf("initialize internal auth plugin: %w", err)
	}

	return grpc.AuthConfig{
		Authenticator:  authenticator,
		OrganizationID: orgID,
	}, addr, nil
}
