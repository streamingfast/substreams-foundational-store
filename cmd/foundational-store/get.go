package main

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/mr-tron/base58"
	"github.com/protocolbuffers/protoscope"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	pbStore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// decodeBytes decodes a string using the specified encoding type
func decodeBytes(data, encoding string) ([]byte, error) {
	switch encoding {
	case "base58":
		return base58.Decode(data)
	case "hex":
		// Strip "0x" prefix if present
		hexData := strings.TrimPrefix(data, "0x")
		return hex.DecodeString(hexData)
	case "base64":
		return base64.StdEncoding.DecodeString(data)
	default:
		return nil, fmt.Errorf("unsupported encoding: %s. Supported encodings: base58, hex, base64", encoding)
	}
}

// GetCmd represents the get command
var GetCmd = &cobra.Command{
	Use:   "get",
	Short: "Get a value from the foundational-store using gRPC",
	Long: `Get a value from the foundational-store using gRPC.
This command connects to a gRPC server and retrieves a value for the specified key.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Get flag values
		getKey, _ := cmd.Flags().GetString("key")
		getServer, _ := cmd.Flags().GetString("server")
		getBlockNumber, _ := cmd.Flags().GetUint64("block-number")
		getOmitDeleted, _ := cmd.Flags().GetBool("omit-deleted")
		getEncoding, _ := cmd.Flags().GetString("encoding")

		if getKey == "" {
			return fmt.Errorf("key is required")
		}

		if getServer == "" {
			return fmt.Errorf("server address is required")
		}

		// Decode the key using the specified encoding
		keyBytes, err := decodeBytes(getKey, getEncoding)
		if err != nil {
			return fmt.Errorf("failed to decode key as %s: %w", getEncoding, err)
		}

		// Connect to the gRPC server
		fmt.Printf("Connecting to gRPC server at %s\n", getServer)
		conn, err := grpc.Dial(getServer, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("failed to connect to server: %w", err)
		}
		defer conn.Close()

		// Create a client for the StoreKV service
		client := pbStore.NewStoreClient(conn)

		// Get block hash flag value
		getBlockHash, _ := cmd.Flags().GetString("block-hash")
		var blockHashBytes []byte
		if getBlockHash != "" {
			blockHashBytes, err = decodeBytes(getBlockHash, getEncoding)
			if err != nil {
				return fmt.Errorf("failed to decode block-hash as %s: %w", getEncoding, err)
			}
		}

		// Create the GetRequest
		request := &pbStore.GetRequest{
			BlockNumber: getBlockNumber,
			BlockHash:   blockHashBytes,
			OmitDeleted: getOmitDeleted,
			Key:         keyBytes,
		}

		// Make the Get request
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		fmt.Printf("Sending Get request for key: %s\n", getKey)
		start := time.Now()

		resp, err := client.Get(ctx, request)
		if err != nil {
			return fmt.Errorf("failed to get value: %w", err)
		}

		fmt.Printf("Query time: %s\n", time.Since(start))

		// Display the response
		switch resp.Code {
		case pbStore.ResponseCode_RESPONSE_CODE_FOUND:
			fmt.Printf("Type URL: %s\n", resp.Value.TypeUrl)
			protoscopeOutput := protoscope.Write(resp.Value.Value, protoscope.WriterOptions{})
			fmt.Printf("Value: %s\n", protoscopeOutput)
			fmt.Printf("Value size: %d bytes\n", len(resp.Value.Value))
		case pbStore.ResponseCode_RESPONSE_CODE_NOT_FOUND:
			fmt.Println("Value not found")
		case pbStore.ResponseCode_RESPONSE_CODE_NOT_FOUND_FINALIZE:
			fmt.Println("Value not found (finalized)")
		default:
			fmt.Printf("Unknown response code: %s\n", resp.Code)
		}

		return nil
	},
}

func init() {
	GetCmd.Flags().String("server", "localhost:50051", "gRPC server address")
	GetCmd.Flags().String("key", "", "Key to lookup")
	GetCmd.Flags().Uint64("block-number", 0, "Block number for the query")
	GetCmd.Flags().String("block-hash", "", "Block hash for the query")
	GetCmd.Flags().Bool("omit-deleted", false, "Whether to omit deleted values")
	GetCmd.Flags().String("encoding", "hex", "Encoding type for key and block-hash (base58, hex, base64)")

	GetCmd.MarkFlagRequired("key")
	//GetCmd.MarkFlagRequired("server")

	viper.BindPFlag("get.server", GetCmd.Flags().Lookup("server"))
	viper.BindPFlag("get.key", GetCmd.Flags().Lookup("key"))
	viper.BindPFlag("get.block_number", GetCmd.Flags().Lookup("block-number"))
	viper.BindPFlag("get.block_hash", GetCmd.Flags().Lookup("block-hash"))
	viper.BindPFlag("get.omit_deleted", GetCmd.Flags().Lookup("omit-deleted"))
	viper.BindPFlag("get.encoding", GetCmd.Flags().Lookup("encoding"))
}
