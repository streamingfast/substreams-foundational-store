package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	pbmodel "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/model/v2"
	pbservice "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/service/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/anypb"
)

// SetCmd represents the set command
var SetCmd = &cobra.Command{
	Use:   "set",
	Short: "Set a value in the foundational-store using gRPC",
	Long: `Set a value in the foundational-store using gRPC.
This command connects to a gRPC server and writes a value for the specified key with the given update policy.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Get flag values
		setKey, _ := cmd.Flags().GetString("key")
		setValue, _ := cmd.Flags().GetString("value")
		setServer, _ := cmd.Flags().GetString("server")
		setBlockNumber, _ := cmd.Flags().GetUint64("block-number")
		setEncoding, _ := cmd.Flags().GetString("encoding")
		setValueType, _ := cmd.Flags().GetString("value-type")
		setPolicyStr, _ := cmd.Flags().GetString("update-policy")
		setIfNotExist, _ := cmd.Flags().GetBool("if-not-exist")

		if setKey == "" {
			return fmt.Errorf("key is required")
		}

		if setValue == "" {
			return fmt.Errorf("value is required")
		}

		if setServer == "" {
			return fmt.Errorf("server address is required")
		}

		// Parse update policy
		policy := parseUpdatePolicy(setPolicyStr)

		// Decode the key using the specified encoding
		keyBytes, err := decodeBytes(setKey, setEncoding)
		if err != nil {
			return fmt.Errorf("failed to decode key as %s: %w", setEncoding, err)
		}

		// Encode the value based on value type
		valueBytes, err := encodeValue(setValue, setValueType)
		if err != nil {
			return fmt.Errorf("failed to encode value: %w", err)
		}

		// Connect to the gRPC server
		fmt.Printf("Connecting to gRPC server at %s\n", setServer)
		conn, err := grpc.Dial(setServer, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("failed to connect to server: %w", err)
		}
		defer conn.Close()

		// Create a client for the Store service
		client := pbservice.NewStoreClient(conn)

		// Create the Entry
		entry := &pbmodel.Entry{
			Key: &pbmodel.Key{
				Bytes: keyBytes,
			},
			Value: &anypb.Any{
				Value: valueBytes,
			},
			UpdatePolicy: policy,
			ValueType:    setValueType,
		}

		// Create the SetRequest
		request := &pbservice.SetRequest{
			SinkEntries: &pbmodel.SinkEntries{
				Entries:     []*pbmodel.Entry{entry},
				IfNotExist:  setIfNotExist,
			},
			BlockNumber: setBlockNumber,
		}

		// Make the SetAll request
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		fmt.Printf("Setting key: %s = %s (policy: %s, type: %s, block: %d)\n", 
			setKey, setValue, setPolicyStr, setValueType, setBlockNumber)
		start := time.Now()

		resp, err := client.SetAll(ctx, request)
		if err != nil {
			return fmt.Errorf("failed to set value: %w", err)
		}

		fmt.Printf("Write time: %s\n", time.Since(start))
		fmt.Printf("Entries written: %d\n", resp.EntriesWritten)

		return nil
	},
}

// parseUpdatePolicy converts string to UpdatePolicy enum
func parseUpdatePolicy(policy string) pbmodel.UpdatePolicy {
	switch strings.ToUpper(policy) {
	case "SET":
		return pbmodel.UpdatePolicy_UPDATE_POLICY_SET
	case "SET_IF_NOT_EXISTS":
		return pbmodel.UpdatePolicy_UPDATE_POLICY_SET_IF_NOT_EXISTS
	case "ADD":
		return pbmodel.UpdatePolicy_UPDATE_POLICY_ADD
	case "MIN":
		return pbmodel.UpdatePolicy_UPDATE_POLICY_MIN
	case "MAX":
		return pbmodel.UpdatePolicy_UPDATE_POLICY_MAX
	case "APPEND":
		return pbmodel.UpdatePolicy_UPDATE_POLICY_APPEND
	case "SET_SUM":
		return pbmodel.UpdatePolicy_UPDATE_POLICY_SET_SUM
	default:
		return pbmodel.UpdatePolicy_UPDATE_POLICY_SET
	}
}

// encodeValue converts string value to bytes based on value type
func encodeValue(value, valueType string) ([]byte, error) {
	switch valueType {
	case "int64":
		i, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid int64 value: %w", err)
		}
		return []byte(fmt.Sprintf("%d", i)), nil

	case "float64":
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float64 value: %w", err)
		}
		return []byte(fmt.Sprintf("%f", f)), nil

	case "bigint", "bigdecimal":
		// Just return as string for big numbers
		return []byte(value), nil

	case "bytes":
		// Hex decode if it looks like hex, otherwise use raw string
		if strings.HasPrefix(value, "0x") {
			return hex.DecodeString(value[2:])
		}
		return []byte(value), nil

	default:
		// Default to raw bytes
		return []byte(value), nil
	}
}

func init() {
	SetCmd.Flags().String("server", "localhost:9020", "gRPC server address")
	SetCmd.Flags().String("key", "", "Key to set")
	SetCmd.Flags().String("value", "", "Value to set")
	SetCmd.Flags().Uint64("block-number", 0, "Block number for the write")
	SetCmd.Flags().String("encoding", "hex", "Encoding type for key (base58, hex, base64)")
	SetCmd.Flags().String("value-type", "bytes", "Value type (int64, float64, bigint, bigdecimal, bytes)")
	SetCmd.Flags().String("update-policy", "SET", "Update policy (SET, SET_IF_NOT_EXISTS, ADD, MIN, MAX, APPEND, SET_SUM)")
	SetCmd.Flags().Bool("if-not-exist", false, "Only set if key does not exist")

	SetCmd.MarkFlagRequired("key")
	SetCmd.MarkFlagRequired("value")

	viper.BindPFlag("set.server", SetCmd.Flags().Lookup("server"))
	viper.BindPFlag("set.key", SetCmd.Flags().Lookup("key"))
	viper.BindPFlag("set.value", SetCmd.Flags().Lookup("value"))
	viper.BindPFlag("set.block_number", SetCmd.Flags().Lookup("block-number"))
	viper.BindPFlag("set.encoding", SetCmd.Flags().Lookup("encoding"))
	viper.BindPFlag("set.value_type", SetCmd.Flags().Lookup("value-type"))
	viper.BindPFlag("set.update_policy", SetCmd.Flags().Lookup("update-policy"))
	viper.BindPFlag("set.if_not_exist", SetCmd.Flags().Lookup("if-not-exist"))
}
