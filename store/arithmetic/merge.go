package arithmetic

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"strconv"

	"github.com/shopspring/decimal"
	pbmodel "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/model/v2"
)

// ApplyUpdatePolicy applies the specified update policy to merge existing and new values
// Returns the merged value as bytes
func ApplyUpdatePolicy(existingValue []byte, newValue []byte, policy pbmodel.UpdatePolicy, valueType string) ([]byte, error) {
	switch policy {
	case pbmodel.UpdatePolicy_UPDATE_POLICY_SET:
		return newValue, nil

	case pbmodel.UpdatePolicy_UPDATE_POLICY_SET_IF_NOT_EXISTS:
		if len(existingValue) == 0 {
			return newValue, nil
		}
		return existingValue, nil

	case pbmodel.UpdatePolicy_UPDATE_POLICY_ADD:
		return applyAdd(existingValue, newValue, valueType)

	case pbmodel.UpdatePolicy_UPDATE_POLICY_MIN:
		return applyMin(existingValue, newValue, valueType)

	case pbmodel.UpdatePolicy_UPDATE_POLICY_MAX:
		return applyMax(existingValue, newValue, valueType)

	case pbmodel.UpdatePolicy_UPDATE_POLICY_APPEND:
		return applyAppend(existingValue, newValue)

	case pbmodel.UpdatePolicy_UPDATE_POLICY_SET_SUM:
		return applySetSum(existingValue, newValue, valueType)

	default:
		return nil, fmt.Errorf("unsupported update policy: %v", policy)
	}
}

func applyAdd(existingValue []byte, newValue []byte, valueType string) ([]byte, error) {
	switch valueType {
	case "int64":
		v0 := bytesToInt64(existingValue)
		v1 := bytesToInt64(newValue)
		return []byte(fmt.Sprintf("%d", v0+v1)), nil

	case "float64":
		v0 := bytesToFloat64(existingValue)
		v1 := bytesToFloat64(newValue)
		return float64ToBytes(v0 + v1), nil

	case "bigint":
		v0 := bytesToBigInt(existingValue)
		v1 := bytesToBigInt(newValue)
		result := new(big.Int).Add(v0, v1)
		return []byte(result.String()), nil

	case "bigdecimal", "bigfloat":
		v0 := bytesToBigDecimal(existingValue)
		v1 := bytesToBigDecimal(newValue)
		result := v0.Add(v1)
		return []byte(result.String()), nil

	default:
		return nil, fmt.Errorf("ADD policy not supported for value type %q", valueType)
	}
}

func applyMin(existingValue []byte, newValue []byte, valueType string) ([]byte, error) {
	if len(existingValue) == 0 {
		return newValue, nil
	}

	switch valueType {
	case "int64":
		v0 := bytesToInt64(existingValue)
		v1 := bytesToInt64(newValue)
		if v0 <= v1 {
			return existingValue, nil
		}
		return newValue, nil

	case "float64":
		v0 := bytesToFloat64(existingValue)
		v1 := bytesToFloat64(newValue)
		if v0 < v1 {
			return existingValue, nil
		}
		return newValue, nil

	case "bigint":
		v0 := bytesToBigInt(existingValue)
		v1 := bytesToBigInt(newValue)
		if v0.Cmp(v1) <= 0 {
			return existingValue, nil
		}
		return newValue, nil

	case "bigdecimal", "bigfloat":
		v0 := bytesToBigDecimal(existingValue)
		v1 := bytesToBigDecimal(newValue)
		if v0.Cmp(v1) <= 0 {
			return existingValue, nil
		}
		return newValue, nil

	default:
		return nil, fmt.Errorf("MIN policy not supported for value type %q", valueType)
	}
}

func applyMax(existingValue []byte, newValue []byte, valueType string) ([]byte, error) {
	if len(existingValue) == 0 {
		return newValue, nil
	}

	switch valueType {
	case "int64":
		v0 := bytesToInt64(existingValue)
		v1 := bytesToInt64(newValue)
		if v0 >= v1 {
			return existingValue, nil
		}
		return newValue, nil

	case "float64":
		v0 := bytesToFloat64(existingValue)
		v1 := bytesToFloat64(newValue)
		if v0 > v1 {
			return existingValue, nil
		}
		return newValue, nil

	case "bigint":
		v0 := bytesToBigInt(existingValue)
		v1 := bytesToBigInt(newValue)
		if v0.Cmp(v1) >= 0 {
			return existingValue, nil
		}
		return newValue, nil

	case "bigdecimal", "bigfloat":
		v0 := bytesToBigDecimal(existingValue)
		v1 := bytesToBigDecimal(newValue)
		if v0.Cmp(v1) >= 0 {
			return existingValue, nil
		}
		return newValue, nil

	default:
		return nil, fmt.Errorf("MAX policy not supported for value type %q", valueType)
	}
}

func applyAppend(existingValue []byte, newValue []byte) ([]byte, error) {
	if len(existingValue) == 0 {
		return newValue, nil
	}
	result := make([]byte, len(existingValue)+len(newValue))
	copy(result, existingValue)
	copy(result[len(existingValue):], newValue)
	return result, nil
}

func applySetSum(existingValue []byte, newValue []byte, valueType string) ([]byte, error) {
	switch valueType {
	case "int64":
		// Check if new value has "set:" prefix
		if bytes.HasPrefix(newValue, []byte("set:")) {
			return bytes.Join([][]byte{[]byte("sum:"), newValue[4:]}, nil), nil
		}
		// Sum the values
		v0 := bytesToPrefixedInt64(existingValue)
		v1 := bytesToPrefixedInt64(newValue)
		return []byte(fmt.Sprintf("sum:%d", v0+v1)), nil

	case "float64":
		if bytes.HasPrefix(newValue, []byte("set:")) {
			valueBytes := newValue[4:]
			return float64ToPrefixedBytes("sum:", bytesToFloat64(valueBytes)), nil
		}
		v0 := bytesToPrefixedFloat64(existingValue)
		v1 := bytesToPrefixedFloat64(newValue)
		return float64ToPrefixedBytes("sum:", v0+v1), nil

	case "bigint":
		if bytes.HasPrefix(newValue, []byte("set:")) {
			return bytes.Join([][]byte{[]byte("sum:"), newValue[4:]}, nil), nil
		}
		v0 := bytesToPrefixedBigInt(existingValue)
		v1 := bytesToPrefixedBigInt(newValue)
		result := new(big.Int).Add(v0, v1)
		return []byte(fmt.Sprintf("sum:%s", result.String())), nil

	case "bigdecimal", "bigfloat":
		if bytes.HasPrefix(newValue, []byte("set:")) {
			return []byte(fmt.Sprintf("sum:%s", string(newValue[4:]))), nil
		}
		v0 := bytesToPrefixedBigDecimal(existingValue)
		v1 := bytesToPrefixedBigDecimal(newValue)
		result := v0.Add(v1)
		return []byte(fmt.Sprintf("sum:%s", result.String())), nil

	default:
		return nil, fmt.Errorf("SET_SUM policy not supported for value type %q", valueType)
	}
}

// Helper functions for type conversion

func bytesToInt64(b []byte) int64 {
	if len(b) == 0 {
		return 0
	}
	v, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return 0
	}
	return v
}

func bytesToFloat64(b []byte) float64 {
	if len(b) == 0 {
		return 0.0
	}
	if len(b) == 8 {
		// Try binary encoding first
		bits := binary.BigEndian.Uint64(b)
		return math.Float64frombits(bits)
	}
	// Try string encoding
	v, err := strconv.ParseFloat(string(b), 64)
	if err != nil {
		return 0.0
	}
	return v
}

func float64ToBytes(f float64) []byte {
	bits := math.Float64bits(f)
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, bits)
	return bytes
}

func bytesToBigInt(b []byte) *big.Int {
	if len(b) == 0 {
		return big.NewInt(0)
	}
	v := new(big.Int)
	v.SetString(string(b), 10)
	return v
}

func bytesToBigDecimal(b []byte) decimal.Decimal {
	if len(b) == 0 {
		return decimal.Zero
	}
	v, err := decimal.NewFromString(string(b))
	if err != nil {
		return decimal.Zero
	}
	return v
}

func bytesToPrefixedInt64(b []byte) int64 {
	if len(b) == 0 {
		return 0
	}
	// Strip "set:" or "sum:" prefix if present
	if bytes.HasPrefix(b, []byte("set:")) {
		return bytesToInt64(b[4:])
	}
	if bytes.HasPrefix(b, []byte("sum:")) {
		return bytesToInt64(b[4:])
	}
	return bytesToInt64(b)
}

func bytesToPrefixedFloat64(b []byte) float64 {
	if len(b) == 0 {
		return 0.0
	}
	// Strip "set:" or "sum:" prefix if present
	if bytes.HasPrefix(b, []byte("set:")) {
		return bytesToFloat64(b[4:])
	}
	if bytes.HasPrefix(b, []byte("sum:")) {
		return bytesToFloat64(b[4:])
	}
	return bytesToFloat64(b)
}

func float64ToPrefixedBytes(prefix string, f float64) []byte {
	return []byte(fmt.Sprintf("%s%f", prefix, f))
}

func bytesToPrefixedBigInt(b []byte) *big.Int {
	if len(b) == 0 {
		return big.NewInt(0)
	}
	// Strip "set:" or "sum:" prefix if present
	if bytes.HasPrefix(b, []byte("set:")) {
		return bytesToBigInt(b[4:])
	}
	if bytes.HasPrefix(b, []byte("sum:")) {
		return bytesToBigInt(b[4:])
	}
	return bytesToBigInt(b)
}

func bytesToPrefixedBigDecimal(b []byte) decimal.Decimal {
	if len(b) == 0 {
		return decimal.Zero
	}
	// Strip "set:" or "sum:" prefix if present
	if bytes.HasPrefix(b, []byte("set:")) {
		return bytesToBigDecimal(b[4:])
	}
	if bytes.HasPrefix(b, []byte("sum:")) {
		return bytesToBigDecimal(b[4:])
	}
	return bytesToBigDecimal(b)
}
