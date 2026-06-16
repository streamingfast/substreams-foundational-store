package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/streamingfast/cli"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/substreams-foundational-store/grpc"
	"github.com/streamingfast/substreams-foundational-store/store"
	"github.com/streamingfast/substreams-foundational-store/store/badger"
	"go.uber.org/zap"
)

// RemoteFeedCmd starts the server in "remote-feed" ingest mode: external
// services push data into the badger store over the Feed gRPC service instead
// of the store running a Substreams sink.
var RemoteFeedCmd = &cobra.Command{
	Use:   "remote-feed",
	Short: "Start the gRPC server in remote-feed ingest mode",
	Long: `Start the gRPC server in remote-feed mode.

Instead of running a Substreams sink, the store exposes a Feed ingest service
(Set / SetReady) that external services call to populate the badger database.
Data pushed through Set is treated as final and written with latest-value
semantics (no time-traversal, no fork-awareness). The read service (Get /
GetFirst) is served on the same address and returns block_reached = false until
SetReady(true) has been called; the readiness flag is persisted in badger.`,
	RunE: remoteFeedCmdE,
}

func remoteFeedCmdE(cmd *cobra.Command, args []string) error {
	zlog, _ := logging.ApplicationLogger("remote-feed", "info")

	serverDSN, _ := cmd.Flags().GetString("dsn")
	serverTypeUrl, _ := cmd.Flags().GetString("type-url")
	serverAddr, _ := cmd.Flags().GetString("addr")
	serverWorkers, _ := cmd.Flags().GetInt("workers")
	startupDelay, _ := cmd.Flags().GetDuration("startup-delay")

	if startupDelay > 0 {
		zlog.Info("waiting before starting server", zap.Duration("startup_delay", startupDelay))
		time.Sleep(startupDelay)
	}

	if serverDSN == "" {
		return fmt.Errorf("dsn is required")
	}

	if serverTypeUrl == "" {
		return fmt.Errorf("type URL is required")
	}

	if !strings.HasPrefix(serverTypeUrl, "type.googleapis.com/") {
		serverTypeUrl = "type.googleapis.com/" + serverTypeUrl
	}

	dsn, err := store.ParseDSN(serverDSN)
	if err != nil {
		return fmt.Errorf("failed to parse DSN: %w", err)
	}

	if dsn.Driver() != "badger" {
		return fmt.Errorf("remote-feed mode only supports the badger driver, got %q", dsn.Driver())
	}

	// remote-feed uses the latest-value badger store directly: data is final, so
	// there is no time-traversal and no ForkAware caching.
	badgerStore, err := badger.NewStore(dsn, serverTypeUrl, serverWorkers, zlog)
	if err != nil {
		return fmt.Errorf("failed to create Badger foundational-store: %w", err)
	}
	defer badgerStore.Close()

	app := cli.NewApplication(cmd.Context())

	server := grpc.NewRemoteFeedServer(badgerStore, zlog)
	app.SuperviseAndStartUsing(server, func() {
		server.Run(serverAddr)
	})

	appErr := app.WaitForTermination(zlog, 5*time.Second, 15*time.Second)
	if appErr != nil {
		zlog.Error("application error", zap.Error(appErr))
		zlog.Core().Sync()
		os.Exit(1)
	}

	return nil
}

func init() {
	RemoteFeedCmd.Flags().String("addr", ":50051", "Address to listen on")
	RemoteFeedCmd.Flags().String("dsn", "", "DSN for the badger foundational-store (e.g. badger:///path/to/db)")
	RemoteFeedCmd.Flags().String("type-url", "", "any.Any type URL are stripped at storage, this needs to be the domain specific type URL like 'sf.substreams.spl-initialized-account.v2.AccountOwner', used by the server to reconstruct the correct any.Any value at retrieval time")
	RemoteFeedCmd.Flags().Int("workers", 10, "Number of workers for parallel operations")
	RemoteFeedCmd.Flags().Duration("startup-delay", 0, "Delay before starting the server (e.g. 5s, 1m)")

	RemoteFeedCmd.MarkFlagRequired("dsn")
	RemoteFeedCmd.MarkFlagRequired("type-url")

	viper.BindPFlag("remote_feed.addr", RemoteFeedCmd.Flags().Lookup("addr"))
	viper.BindPFlag("remote_feed.dsn", RemoteFeedCmd.Flags().Lookup("dsn"))
	viper.BindPFlag("remote_feed.type_url", RemoteFeedCmd.Flags().Lookup("type-url"))
	viper.BindPFlag("remote_feed.workers", RemoteFeedCmd.Flags().Lookup("workers"))
}
