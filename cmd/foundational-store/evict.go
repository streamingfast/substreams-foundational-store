package main

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	pbservice "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/service/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// EvictCmd represents the evict command
var EvictCmd = &cobra.Command{
	Use:   "evict",
	Short: "Evict entries from foundational-store cache from a block number onwards",
	Long: `Evict entries from the ForkAware cache at or above the specified block number.
This is used for handling blockchain reorganizations (forks) by removing speculative data.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Get flag values
		evictServer, _ := cmd.Flags().GetString("server")
		evictBlockNumber, _ := cmd.Flags().GetUint64("block-number")

		if evictServer == "" {
			return fmt.Errorf("server address is required")
		}

		if evictBlockNumber == 0 {
			return fmt.Errorf("block-number must be greater than 0")
		}

		// Connect to the gRPC server
		fmt.Printf("Connecting to gRPC server at %s\n", evictServer)
		conn, err := grpc.Dial(evictServer, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("failed to connect to server: %w", err)
		}
		defer conn.Close()

		// Create a client for the Store service
		client := pbservice.NewStoreClient(conn)

		// Create the EvictRequest
		request := &pbservice.EvictRequest{
			BlockNumber: evictBlockNumber,
		}

		// Make the EvictUpToBlock request
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		fmt.Printf("Evicting entries from block %d onwards (reorg handling)\n", evictBlockNumber)
		start := time.Now()

		resp, err := client.EvictUpToBlock(ctx, request)
		if err != nil {
			return fmt.Errorf("failed to evict: %w", err)
		}

		fmt.Printf("Evict time: %s\n", time.Since(start))
		fmt.Printf("Entries evicted: %d\n", resp.EntriesEvicted)

		return nil
	},
}

func init() {
	EvictCmd.Flags().String("server", "localhost:9020", "gRPC server address")
	EvictCmd.Flags().Uint64("block-number", 0, "Block number to evict from (reorg point)")

	EvictCmd.MarkFlagRequired("block-number")

	viper.BindPFlag("evict.server", EvictCmd.Flags().Lookup("server"))
	viper.BindPFlag("evict.block_number", EvictCmd.Flags().Lookup("block-number"))
}
