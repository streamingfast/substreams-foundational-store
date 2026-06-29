package main

import (
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/streamingfast/cli"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/substreams-foundational-store/grpc"
	pbrouter "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store-management/router/v2"
	"github.com/streamingfast/substreams-foundational-store/sink"
	"github.com/streamingfast/substreams-foundational-store/store"
	"github.com/streamingfast/substreams-foundational-store/store/ForkAware"
	"github.com/streamingfast/substreams-foundational-store/store/badger"
	"github.com/streamingfast/substreams-foundational-store/store/badger_time_traversal"
	"github.com/streamingfast/substreams-foundational-store/store/postgres"
	"github.com/streamingfast/substreams-foundational-store/store/postgres_time_traversal"
	"go.uber.org/zap"
	grpcclient "google.golang.org/grpc"

	subsink "github.com/streamingfast/substreams/sink"
)

// ServerCmd represents the server command
var ServerCmd = &cobra.Command{
	Use:   "server",
	Short: "Start the gRPC server",
	Long: `Start the gRPC server that provides access to the foundational-store.
The server supports various foundational-store implementations (PostgreSQL, Badger) with different configurations.`,
	RunE: serverCmdE,
}

func serverCmdE(cmd *cobra.Command, args []string) error {
	// Initialize logger
	zlog, tracer := logging.ApplicationLogger("server", "info")

	// Initialize metrics
	// sink.RegisterMetrics()

	// Get flag values
	serverDSN, _ := cmd.Flags().GetString("dsn")
	serverTypeUrl, _ := cmd.Flags().GetString("type-url")
	serverAddr, _ := cmd.Flags().GetString("addr")
	serverWorkers, _ := cmd.Flags().GetInt("workers")
	manifestPath, _ := cmd.Flags().GetString("manifest-path")
	outputModuleName, _ := cmd.Flags().GetString("output-module-name")
	cursorFilePath, _ := cmd.Flags().GetString("cursor-file-path")
	noTimeTraversal, _ := cmd.Flags().GetBool("no-time-traversal")
	storeManagerAddr, _ := cmd.Flags().GetString("store-manager-address")
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

	// Parse the DSN
	dsn, err := store.ParseDSN(serverDSN)
	if err != nil {
		return fmt.Errorf("failed to parse DSN: %w", err)
	}

	// Create the foundational-store based on the DSN driver
	var baseStore store.Store

	switch dsn.Driver() {
	case "badger":
		if noTimeTraversal {
			// Use original badger store implementation
			badgerStore, err := badger.NewStore(dsn, serverTypeUrl, serverWorkers, zlog)
			if err != nil {
				return fmt.Errorf("failed to create Badger foundational-store: %w", err)
			}
			defer badgerStore.Close()
			baseStore = badgerStore
		} else {
			// Use time traversal badger store implementation (default)
			badgerTimeTraversalStore, err := badger_time_traversal.NewStore(dsn, serverTypeUrl, serverWorkers, zlog)
			if err != nil {
				return fmt.Errorf("failed to create Badger time traversal foundational-store: %w", err)
			}
			defer badgerTimeTraversalStore.Close()
			baseStore = badgerTimeTraversalStore
		}
	case "postgres":
		if noTimeTraversal {
			// Use original postgres store implementation
			pgStore, err := postgres.NewStore(dsn, serverTypeUrl)
			if err != nil {
				return fmt.Errorf("failed to create Postgres foundational-store: %w", err)
			}
			baseStore = pgStore
		} else {
			// Use time traversal postgres store implementation (default)
			pgTimeTraversalStore, err := postgres_time_traversal.NewStore(dsn, serverTypeUrl)
			if err != nil {
				return fmt.Errorf("failed to create Postgres time traversal foundational-store: %w", err)
			}
			baseStore = pgTimeTraversalStore
		}
	default:
		return fmt.Errorf("unsupported foundational-store driver: %s", dsn.Driver())
	}

	// Wrap the foundational-store with a ForkAware foundational-store
	storeImpl := ForkAware.NewStore(baseStore)

	// Load cursor from file if it exists and set it in the command flags so subsink.NewFromViper can use it
	cursor := sink.LoadCursorFromFile(zlog, cursorFilePath)
	if cursor != nil {
		zlog.Info("loaded cursor from file, will resume from saved position")
	} else {
		zlog.Info("no cursor file found, will start from the beginning")
	}

	app := cli.NewApplication(cmd.Context())
	ping := func() {}
	headBlock := func() uint64 { return math.MaxUint64 }

	if manifestPath != "" {
		zlog.Info("using manifest file", zap.String("path", manifestPath))
		// Create a substreams sink using Viper configuration
		substreamsClient, err := subsink.NewFromViper(
			cmd,
			"",
			manifestPath,
			outputModuleName,
			"substreams-foundational-store",
			zlog,
			tracer, // tracer is nil
		)
		if err != nil {
			return fmt.Errorf("failed to create substreams sink: %w", err)
		}

		sinker := sink.NewSinker(storeImpl, zlog, cursorFilePath, cursor)
		headBlock = sinker.HeadBlock

		conn, err := grpcclient.Dial(storeManagerAddr, grpcclient.WithInsecure())
		if err != nil {
			return fmt.Errorf("dialing store manager: %w", err)
		}
		defer conn.Close()

		client := pbrouter.NewStoreManagerClient(conn)

		ping = func() {
			zlog.Info("pinging store manager", zap.String("module_output_hash", substreamsClient.OutputModuleHash()), zap.String("network", substreamsClient.Pkg.Network))
			resp, err := client.Ping(cmd.Context(), &pbrouter.PingRequest{
				ModuleOutputHash: substreamsClient.OutputModuleHash(),
				Network:          substreamsClient.Pkg.Network,
			})
			if err != nil {
				zlog.Error("ping failed", zap.Error(err))
			} else if resp == nil {
				zlog.Error("ping failed: nil response")
			} else if resp.Code == pbrouter.PingResponse_pong {
				zlog.Info("ping successful")
			} else {
				zlog.Error("ping failed", zap.String("code", resp.Code.String()), zap.String("reason", resp.GetFailureReason()))
			}
		}

		app.SuperviseAndStartUsing(sinker.Shutter, func() {
			substreamsClient.Run(cmd.Context(), cursor, sinker)
			sinker.Shutdown(substreamsClient.Err())
		})
	}

	authConfig, err := loadAuthConfig(cmd, zlog)
	if err != nil {
		return err
	}

	internalAuthConfig, internalAddr, err := loadInternalAuthConfig(cmd, zlog)
	if err != nil {
		return err
	}

	server := grpc.NewStoreServer(storeImpl, headBlock, zlog)

	app.SuperviseAndStartUsing(server, func() {
		if internalAddr != "" {
			zlog.Info("serving public and internal listeners", zap.String("public_addr", serverAddr), zap.String("internal_addr", internalAddr))
			server.RunWithInternal(serverAddr, authConfig, internalAddr, internalAuthConfig)
			return
		}
		server.Run(serverAddr, authConfig)
	})

	if storeManagerAddr != "" {
		go func() {

			ctx := cmd.Context()
			ping()

			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					ping()
				}
			}
		}()
	}

	appErr := app.WaitForTermination(zlog, 5*time.Second, 15*time.Second)
	if appErr != nil {
		zlog.Error("application error", zap.Error(appErr))
		zlog.Core().Sync()
		os.Exit(1)
	}

	return nil
}

func init() {
	subsink.AddFlagsToSet(ServerCmd.Flags())

	ServerCmd.Flags().String("addr", ":50051", "Address to listen on")
	ServerCmd.Flags().String("dsn", "", "DSN for the foundational-store (e.g. badger:///path/to/db or postgres://user:pass@host:port/dbname)")
	ServerCmd.Flags().String("type-url", "", "any.Any type URL are stripped at storage, this needs to be the domain specific type URL like 'sf.substreams.spl-initialized-account.v2.AccountOwner', used by the server to reconstruct the correct any.Any value at retrieval time")
	ServerCmd.Flags().Int("workers", 10, "Number of workers for parallel operations")
	ServerCmd.Flags().String("manifest-path", "", "Path to the manifest file")
	ServerCmd.Flags().String("output-module-name", "", "Name of the output module")
	ServerCmd.Flags().String("cursor-file-path", "state.cursor", "Path to the cursor file")
	ServerCmd.Flags().Int("batch-size", 1, "Number of entries to batch for insertion")
	ServerCmd.Flags().Duration("max-batch-time", 30*time.Second, "Maximum time to wait before flushing a partial batch")
	ServerCmd.Flags().Int("flush-queue-size", 3, "Size of the async flush queue buffer")
	ServerCmd.Flags().Bool("no-time-traversal", false, "Disable time traversal mode and use original badger store implementation")
	ServerCmd.Flags().String("store-manager-address", "", "Address of the store manager to ping every 30 seconds")
	ServerCmd.Flags().Duration("startup-delay", 0, "Delay before starting the server (e.g. 5s, 1m)")

	ServerCmd.MarkFlagRequired("dsn")
	ServerCmd.MarkFlagRequired("type-url")
	addAuthFlags(ServerCmd)

	viper.BindPFlag("server.addr", ServerCmd.Flags().Lookup("addr"))
	viper.BindPFlag("server.dsn", ServerCmd.Flags().Lookup("dsn"))
	viper.BindPFlag("server.type_url", ServerCmd.Flags().Lookup("type-url"))
	viper.BindPFlag("server.workers", ServerCmd.Flags().Lookup("workers"))
	viper.BindPFlag("substreams.manifest_path", ServerCmd.Flags().Lookup("manifest-path"))
	viper.BindPFlag("substreams.output_module_name", ServerCmd.Flags().Lookup("output-module-name"))
	viper.BindPFlag("server.cursor_file_path", ServerCmd.Flags().Lookup("cursor-file-path"))

	viper.BindPFlag("endpoint", ServerCmd.Flags().Lookup("endpoint"))
	viper.BindPFlag("start-block", ServerCmd.Flags().Lookup("start-block"))
	viper.BindPFlag("stop-block", ServerCmd.Flags().Lookup("stop-block"))
	viper.BindPFlag("development-mode", ServerCmd.Flags().Lookup("development-mode"))
	viper.BindPFlag("plaintext", ServerCmd.Flags().Lookup("plaintext"))
}
