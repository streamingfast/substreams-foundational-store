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

// FlushCmd represents the flush command
var FlushCmd = &cobra.Command{
	Use:   "flush",
	Short: "Flush foundational-store cache up to a block number",
	Long: `Flush the ForkAware cache to Badger up to the specified block number (typically LIB).
This persists all entries with block numbers <= the specified block from cache to the backing store.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Get flag values
		flushServer, _ := cmd.Flags().GetString("server")
		flushBlockNumber, _ := cmd.Flags().GetUint64("block-number")
		flushIfNotExist, _ := cmd.Flags().GetBool("if-not-exist")

		if flushServer == "" {
			return fmt.Errorf("server address is required")
		}

		if flushBlockNumber == 0 {
			return fmt.Errorf("block-number must be greater than 0")
		}

		// Connect to the gRPC server
		fmt.Printf("Connecting to gRPC server at %s\n", flushServer)
		conn, err := grpc.Dial(flushServer, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("failed to connect to server: %w", err)
		}
		defer conn.Close()

		// Create a client for the Store service
		client := pbservice.NewStoreClient(conn)

		// Create the FlushRequest
		request := &pbservice.FlushRequest{
			BlockNumber: flushBlockNumber,
			IfNotExist:  flushIfNotExist,
		}

		// Make the FlushUpToBlock request
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		fmt.Printf("Flushing cache up to block %d (if_not_exist: %v)\n", flushBlockNumber, flushIfNotExist)
		start := time.Now()

		resp, err := client.FlushUpToBlock(ctx, request)
		if err != nil {
			return fmt.Errorf("failed to flush: %w", err)
		}

		fmt.Printf("Flush time: %s\n", time.Since(start))
		fmt.Printf("Entries flushed: %d\n", resp.EntriesFlushed)

		return nil
	},
}

func init() {
	FlushCmd.Flags().String("server", "localhost:9020", "gRPC server address")
	FlushCmd.Flags().Uint64("block-number", 0, "Block number to flush up to (LIB)")
	FlushCmd.Flags().Bool("if-not-exist", false, "Use if-not-exist semantics when flushing")

	FlushCmd.MarkFlagRequired("block-number")

	viper.BindPFlag("flush.server", FlushCmd.Flags().Lookup("server"))
	viper.BindPFlag("flush.block_number", FlushCmd.Flags().Lookup("block-number"))
	viper.BindPFlag("flush.if_not_exist", FlushCmd.Flags().Lookup("if-not-exist"))
}
