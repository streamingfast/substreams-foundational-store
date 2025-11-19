package badger

import (
	"encoding/binary"
	"math"
	"math/big"
	"testing"

	storelib "github.com/streamingfast/substreams-foundational-store/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplySet(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")
	value := []byte("test-value")

	err := applySet(store, key, value, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, value, retrieved)
}

func TestApplySetIfNotExists(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")
	value1 := []byte("value1")
	value2 := []byte("value2")

	// First set should succeed
	err := applySetIfNotExists(store, key, value1, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, value1, retrieved)

	// Second set should be ignored
	err = applySetIfNotExists(store, key, value2, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, value1, retrieved) // Should still be value1
}

func TestApplyAppend(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")
	value1 := []byte("hello")
	value2 := []byte(" world")

	// Set initial value
	err := applySet(store, key, value1, 100)
	require.NoError(t, err)

	// Append to it
	err = applyAppend(store, key, value2, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, []byte("hello world"), retrieved)
}

func TestApplySetMaxInt64(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Test with empty key (should set)
	val1 := int64ToBytes(100)
	err := applySetMaxInt64(store, key, val1, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved)

	// Test with smaller value (should not set)
	val2 := int64ToBytes(50)
	err = applySetMaxInt64(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved) // Should still be 100

	// Test with larger value (should set)
	val3 := int64ToBytes(200)
	err = applySetMaxInt64(store, key, val3, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val3, retrieved) // Should now be 200
}

func TestApplySetMinInt64(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Test with empty key (should set)
	val1 := int64ToBytes(100)
	err := applySetMinInt64(store, key, val1, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved)

	// Test with larger value (should not set)
	val2 := int64ToBytes(150)
	err = applySetMinInt64(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved) // Should still be 100

	// Test with smaller value (should set)
	val3 := int64ToBytes(50)
	err = applySetMinInt64(store, key, val3, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val3, retrieved) // Should now be 50
}

func TestApplySumInt64(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Start with 100
	val1 := int64ToBytes(100)
	err := applySet(store, key, val1, 100)
	require.NoError(t, err)

	// Add 50
	val2 := int64ToBytes(50)
	err = applySumInt64(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValue(key)
	require.NoError(t, err)
	expected := int64ToBytes(150)
	assert.Equal(t, expected, retrieved)
}

func TestApplySetSumInt64(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Test with empty key (should work like sum with 0)
	val1 := int64ToBytes(100)
	err := applySetSumInt64(store, key, val1, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved)

	// Add another 50
	val2 := int64ToBytes(50)
	err = applySetSumInt64(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValue(key)
	require.NoError(t, err)
	expected := int64ToBytes(150)
	assert.Equal(t, expected, retrieved)
}

func TestApplySetMaxFloat64(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Test with empty key (should set)
	val1 := float64ToBytes(100.5)
	err := applySetMaxFloat64(store, key, val1, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved)

	// Test with smaller value (should not set)
	val2 := float64ToBytes(50.5)
	err = applySetMaxFloat64(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved) // Should still be 100.5

	// Test with larger value (should set)
	val3 := float64ToBytes(200.5)
	err = applySetMaxFloat64(store, key, val3, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val3, retrieved) // Should now be 200.5
}

func TestApplySumFloat64(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Start with 100.5
	val1 := float64ToBytes(100.5)
	err := applySet(store, key, val1, 100)
	require.NoError(t, err)

	// Add 50.25
	val2 := float64ToBytes(50.25)
	err = applySumFloat64(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValue(key)
	require.NoError(t, err)
	expected := float64ToBytes(150.75)
	assert.Equal(t, expected, retrieved)
}

func TestApplySetMaxBigInt(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Test with empty key (should set)
	val1 := bigIntToBytes(big.NewInt(100))
	err := applySetMaxBigInt(store, key, val1, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved)

	// Test with smaller value (should not set)
	val2 := bigIntToBytes(big.NewInt(50))
	err = applySetMaxBigInt(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved) // Should still be 100

	// Test with larger value (should set)
	val3 := bigIntToBytes(big.NewInt(200))
	err = applySetMaxBigInt(store, key, val3, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val3, retrieved) // Should now be 200
}

func TestApplySumBigInt(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Start with 100
	val1 := bigIntToBytes(big.NewInt(100))
	err := applySet(store, key, val1, 100)
	require.NoError(t, err)

	// Add 50
	val2 := bigIntToBytes(big.NewInt(50))
	err = applySumBigInt(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValue(key)
	require.NoError(t, err)
	expected := bigIntToBytes(big.NewInt(150))
	assert.Equal(t, expected, retrieved)
}

func TestApplySetMaxBigDecimal(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Test with empty key (should set)
	val1 := bigDecimalToBytes(big.NewInt(100), 2) // 100 with scale 2
	err := applySetMaxBigDecimal(store, key, val1, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved)

	// Test with smaller value (should not set)
	val2 := bigDecimalToBytes(big.NewInt(50), 2) // 50 with scale 2
	err = applySetMaxBigDecimal(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved) // Should still be 100

	// Test with larger value (should set)
	val3 := bigDecimalToBytes(big.NewInt(200), 2) // 200 with scale 2
	err = applySetMaxBigDecimal(store, key, val3, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val3, retrieved) // Should now be 200
}

func TestApplySumBigDecimal(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Start with 100 (scale 2)
	val1 := bigDecimalToBytes(big.NewInt(100), 2)
	err := applySet(store, key, val1, 100)
	require.NoError(t, err)

	// Add 50 (scale 2)
	val2 := bigDecimalToBytes(big.NewInt(50), 2)
	err = applySumBigDecimal(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValue(key)
	require.NoError(t, err)
	expected := bigDecimalToBytes(big.NewInt(150), 2)
	assert.Equal(t, expected, retrieved)
}

// Time traversal versions
func TestApplySetTimeTraversal(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")
	value := []byte("test-value")

	err := applySetTimeTraversal(store, key, value, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	assert.Equal(t, value, retrieved)
}

func TestApplySetMaxInt64TimeTraversal(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Test with empty key (should set)
	val1 := int64ToBytes(100)
	err := applySetMaxInt64TimeTraversal(store, key, val1, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved)

	// Test with smaller value (should not set)
	val2 := int64ToBytes(50)
	err = applySetMaxInt64TimeTraversal(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved) // Should still be 100
}

func TestApplySetIfNotExistsTimeTraversal(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")
	value1 := []byte("value1")
	value2 := []byte("value2")

	// First set should succeed
	err := applySetIfNotExistsTimeTraversal(store, key, value1, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	assert.Equal(t, value1, retrieved)

	// Second set should be ignored
	err = applySetIfNotExistsTimeTraversal(store, key, value2, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	assert.Equal(t, value1, retrieved) // Should still be value1
}

func TestApplyAppendTimeTraversal(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")
	value1 := []byte("hello")
	value2 := []byte(" world")

	// Set initial value
	err := applySetTimeTraversal(store, key, value1, 100)
	require.NoError(t, err)

	// Append to it
	err = applyAppendTimeTraversal(store, key, value2, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	assert.Equal(t, []byte("hello world"), retrieved)
}

func TestApplySetMinInt64TimeTraversal(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Test with empty key (should set)
	val1 := int64ToBytes(100)
	err := applySetMinInt64TimeTraversal(store, key, val1, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved)

	// Test with larger value (should not set)
	val2 := int64ToBytes(150)
	err = applySetMinInt64TimeTraversal(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved) // Should still be 100

	// Test with smaller value (should set)
	val3 := int64ToBytes(50)
	err = applySetMinInt64TimeTraversal(store, key, val3, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	assert.Equal(t, val3, retrieved) // Should now be 50
}

func TestApplySumInt64TimeTraversal(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Start with 100
	val1 := int64ToBytes(100)
	err := applySetTimeTraversal(store, key, val1, 100)
	require.NoError(t, err)

	// Add 50
	val2 := int64ToBytes(50)
	err = applySumInt64TimeTraversal(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	expected := int64ToBytes(150)
	assert.Equal(t, expected, retrieved)
}

func TestApplySetSumInt64TimeTraversal(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Test with empty key (should work like sum with 0)
	val1 := int64ToBytes(100)
	err := applySetSumInt64TimeTraversal(store, key, val1, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved)

	// Add another 50
	val2 := int64ToBytes(50)
	err = applySetSumInt64TimeTraversal(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	expected := int64ToBytes(150)
	assert.Equal(t, expected, retrieved)
}

func TestApplySetMinFloat64(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Test with empty key (should set)
	val1 := float64ToBytes(100.5)
	err := applySetMinFloat64(store, key, val1, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved)

	// Test with larger value (should not set)
	val2 := float64ToBytes(150.5)
	err = applySetMinFloat64(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved) // Should still be 100.5

	// Test with smaller value (should set)
	val3 := float64ToBytes(50.5)
	err = applySetMinFloat64(store, key, val3, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val3, retrieved) // Should now be 50.5
}

func TestApplySetSumFloat64(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Test with empty key (should work like sum with 0)
	val1 := float64ToBytes(100.5)
	err := applySetSumFloat64(store, key, val1, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved)

	// Add another 50.25
	val2 := float64ToBytes(50.25)
	err = applySetSumFloat64(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValue(key)
	require.NoError(t, err)
	expected := float64ToBytes(150.75)
	assert.Equal(t, expected, retrieved)
}

func TestApplySetMinFloat64TimeTraversal(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Test with empty key (should set)
	val1 := float64ToBytes(100.5)
	err := applySetMinFloat64TimeTraversal(store, key, val1, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved)

	// Test with larger value (should not set)
	val2 := float64ToBytes(150.5)
	err = applySetMinFloat64TimeTraversal(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved) // Should still be 100.5
}

func TestApplySetSumFloat64TimeTraversal(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Test with empty key (should work like sum with 0)
	val1 := float64ToBytes(100.5)
	err := applySetSumFloat64TimeTraversal(store, key, val1, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved)

	// Add another 50.25
	val2 := float64ToBytes(50.25)
	err = applySetSumFloat64TimeTraversal(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	expected := float64ToBytes(150.75)
	assert.Equal(t, expected, retrieved)
}

func TestApplySetMinBigInt(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Test with empty key (should set)
	val1 := bigIntToBytes(big.NewInt(100))
	err := applySetMinBigInt(store, key, val1, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved)

	// Test with larger value (should not set)
	val2 := bigIntToBytes(big.NewInt(150))
	err = applySetMinBigInt(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved) // Should still be 100

	// Test with smaller value (should set)
	val3 := bigIntToBytes(big.NewInt(50))
	err = applySetMinBigInt(store, key, val3, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val3, retrieved) // Should now be 50
}

func TestApplySetSumBigInt(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Test with empty key (should work like sum with 0)
	val1 := bigIntToBytes(big.NewInt(100))
	err := applySetSumBigInt(store, key, val1, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved)

	// Add another 50
	val2 := bigIntToBytes(big.NewInt(50))
	err = applySetSumBigInt(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValue(key)
	require.NoError(t, err)
	expected := bigIntToBytes(big.NewInt(150))
	assert.Equal(t, expected, retrieved)
}

func TestApplySetMinBigIntTimeTraversal(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Test with empty key (should set)
	val1 := bigIntToBytes(big.NewInt(100))
	err := applySetMinBigIntTimeTraversal(store, key, val1, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved)

	// Test with larger value (should not set)
	val2 := bigIntToBytes(big.NewInt(150))
	err = applySetMinBigIntTimeTraversal(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved) // Should still be 100
}

func TestApplySetSumBigIntTimeTraversal(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Test with empty key (should work like sum with 0)
	val1 := bigIntToBytes(big.NewInt(100))
	err := applySetSumBigIntTimeTraversal(store, key, val1, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved)

	// Add another 50
	val2 := bigIntToBytes(big.NewInt(50))
	err = applySetSumBigIntTimeTraversal(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	expected := bigIntToBytes(big.NewInt(150))
	assert.Equal(t, expected, retrieved)
}

func TestApplySetMinBigDecimal(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Test with empty key (should set)
	val1 := bigDecimalToBytes(big.NewInt(100), 2) // 100 with scale 2
	err := applySetMinBigDecimal(store, key, val1, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved)

	// Test with larger value (should not set)
	val2 := bigDecimalToBytes(big.NewInt(150), 2) // 150 with scale 2
	err = applySetMinBigDecimal(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved) // Should still be 100

	// Test with smaller value (should set)
	val3 := bigDecimalToBytes(big.NewInt(50), 2) // 50 with scale 2
	err = applySetMinBigDecimal(store, key, val3, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val3, retrieved) // Should now be 50
}

func TestApplySetSumBigDecimal(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Test with empty key (should work like sum with 0)
	val1 := bigDecimalToBytes(big.NewInt(100), 2)
	err := applySetSumBigDecimal(store, key, val1, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved)

	// Add another 50 (scale 2)
	val2 := bigDecimalToBytes(big.NewInt(50), 2)
	err = applySetSumBigDecimal(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValue(key)
	require.NoError(t, err)
	expected := bigDecimalToBytes(big.NewInt(150), 2)
	assert.Equal(t, expected, retrieved)
}

func TestApplySetMinBigDecimalTimeTraversal(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Test with empty key (should set)
	val1 := bigDecimalToBytes(big.NewInt(100), 2) // 100 with scale 2
	err := applySetMinBigDecimalTimeTraversal(store, key, val1, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved)

	// Test with larger value (should not set)
	val2 := bigDecimalToBytes(big.NewInt(150), 2) // 150 with scale 2
	err = applySetMinBigDecimalTimeTraversal(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved) // Should still be 100
}

func TestApplySetSumBigDecimalTimeTraversal(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Test with empty key (should work like sum with 0)
	val1 := bigDecimalToBytes(big.NewInt(100), 2)
	err := applySetSumBigDecimalTimeTraversal(store, key, val1, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved)

	// Add another 50 (scale 2)
	val2 := bigDecimalToBytes(big.NewInt(50), 2)
	err = applySetSumBigDecimalTimeTraversal(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	expected := bigDecimalToBytes(big.NewInt(150), 2)
	assert.Equal(t, expected, retrieved)
}

func TestApplyDeletePrefix(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	// Set up some test keys with a common prefix
	prefix := []byte("test-prefix")
	key1 := []byte("test-prefix-key1")
	key2 := []byte("test-prefix-key2")
	key3 := []byte("other-prefix-key3")

	val := []byte("value")

	// Set values for keys
	err := applySet(store, key1, val, 100)
	require.NoError(t, err)
	err = applySet(store, key2, val, 100)
	require.NoError(t, err)
	err = applySet(store, key3, val, 100)
	require.NoError(t, err)

	// Verify keys exist
	retrieved, err := store.getRawValue(key1)
	require.NoError(t, err)
	assert.Equal(t, val, retrieved)

	retrieved, err = store.getRawValue(key2)
	require.NoError(t, err)
	assert.Equal(t, val, retrieved)

	retrieved, err = store.getRawValue(key3)
	require.NoError(t, err)
	assert.Equal(t, val, retrieved)

	// Delete prefix
	err = applyDeletePrefix(store, prefix)
	require.NoError(t, err)

	// Verify prefixed keys are gone
	_, err = store.getRawValue(key1)
	assert.Error(t, err) // Should not exist

	_, err = store.getRawValue(key2)
	assert.Error(t, err) // Should not exist

	// Verify non-prefixed key still exists
	retrieved, err = store.getRawValue(key3)
	require.NoError(t, err)
	assert.Equal(t, val, retrieved)
}

func TestApplySetMaxFloat64TimeTraversal(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Test with empty key (should set)
	val1 := float64ToBytes(100.5)
	err := applySetMaxFloat64TimeTraversal(store, key, val1, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved)

	// Test with smaller value (should not set)
	val2 := float64ToBytes(50.5)
	err = applySetMaxFloat64TimeTraversal(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved) // Should still be 100.5

	// Test with larger value (should set)
	val3 := float64ToBytes(200.5)
	err = applySetMaxFloat64TimeTraversal(store, key, val3, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	assert.Equal(t, val3, retrieved) // Should now be 200.5
}

func TestApplySetMaxBigIntTimeTraversal(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Test with empty key (should set)
	val1 := bigIntToBytes(big.NewInt(100))
	err := applySetMaxBigIntTimeTraversal(store, key, val1, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved)

	// Test with smaller value (should not set)
	val2 := bigIntToBytes(big.NewInt(50))
	err = applySetMaxBigIntTimeTraversal(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved) // Should still be 100

	// Test with larger value (should set)
	val3 := bigIntToBytes(big.NewInt(200))
	err = applySetMaxBigIntTimeTraversal(store, key, val3, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	assert.Equal(t, val3, retrieved) // Should now be 200
}

func TestApplySetMaxBigDecimalTimeTraversal(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Test with empty key (should set)
	val1 := bigDecimalToBytes(big.NewInt(100), 2) // 100 with scale 2
	err := applySetMaxBigDecimalTimeTraversal(store, key, val1, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved)

	// Test with smaller value (should not set)
	val2 := bigDecimalToBytes(big.NewInt(50), 2) // 50 with scale 2
	err = applySetMaxBigDecimalTimeTraversal(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved) // Should still be 100

	// Test with larger value (should set)
	val3 := bigDecimalToBytes(big.NewInt(200), 2) // 200 with scale 2
	err = applySetMaxBigDecimalTimeTraversal(store, key, val3, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	assert.Equal(t, val3, retrieved) // Should now be 200
}

func TestApplySumFloat64TimeTraversal(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Start with 100.5
	val1 := float64ToBytes(100.5)
	err := applySetTimeTraversal(store, key, val1, 100)
	require.NoError(t, err)

	// Add 50.25
	val2 := float64ToBytes(50.25)
	err = applySumFloat64TimeTraversal(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	expected := float64ToBytes(150.75)
	assert.Equal(t, expected, retrieved)
}

func TestApplySumBigIntTimeTraversal(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Start with 100
	val1 := bigIntToBytes(big.NewInt(100))
	err := applySetTimeTraversal(store, key, val1, 100)
	require.NoError(t, err)

	// Add 50
	val2 := bigIntToBytes(big.NewInt(50))
	err = applySumBigIntTimeTraversal(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	expected := bigIntToBytes(big.NewInt(150))
	assert.Equal(t, expected, retrieved)
}

func TestApplySumBigDecimalTimeTraversal(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Start with 100 (scale 2)
	val1 := bigDecimalToBytes(big.NewInt(100), 2)
	err := applySetTimeTraversal(store, key, val1, 100)
	require.NoError(t, err)

	// Add 50 (scale 2)
	val2 := bigDecimalToBytes(big.NewInt(50), 2)
	err = applySumBigDecimalTimeTraversal(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValueTimeTraversal(key, 100)
	require.NoError(t, err)
	expected := bigDecimalToBytes(big.NewInt(150), 2)
	assert.Equal(t, expected, retrieved)
}

// Test edge cases and error conditions
func TestApplySetIfNotExists_EdgeCases(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")
	value := []byte("test-value")

	// Test with non-existent key (should set)
	err := applySetIfNotExists(store, key, value, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, value, retrieved)

	// Test with existing key (should not set)
	newValue := []byte("new-value")
	err = applySetIfNotExists(store, key, newValue, 100)
	require.NoError(t, err)

	retrieved, err = store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, value, retrieved) // Should still be original value
}

func TestApplyAppend_EmptyValue(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")
	initialValue := []byte("hello")
	emptyAppend := []byte("")

	// Set initial value
	err := applySet(store, key, initialValue, 100)
	require.NoError(t, err)

	// Append empty value
	err = applyAppend(store, key, emptyAppend, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, initialValue, retrieved) // Should remain unchanged
}

func TestApplySetMaxInt64_EqualValues(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Set initial value
	val1 := int64ToBytes(100)
	err := applySet(store, key, val1, 100)
	require.NoError(t, err)

	// Try to set max with equal value (should not change)
	err = applySetMaxInt64(store, key, val1, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved)
}

func TestApplySumInt64_ZeroAddition(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Start with 100
	val1 := int64ToBytes(100)
	err := applySet(store, key, val1, 100)
	require.NoError(t, err)

	// Add 0
	val2 := int64ToBytes(0)
	err = applySumInt64(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val1, retrieved) // Should remain 100
}

func TestApplySetMaxBigDecimal_DifferentScales(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Set initial value with scale 2 (100 with scale 2 = 1.00)
	val1 := bigDecimalToBytes(big.NewInt(100), 2)
	err := applySet(store, key, val1, 100)
	require.NoError(t, err)

	// Try to set max with larger value but different scale (500 with scale 1 = 50.0)
	// When normalized to scale 2: 500 * 10^(2-1) = 5000 (representing 50.00)
	val2 := bigDecimalToBytes(big.NewInt(500), 1)
	err = applySetMaxBigDecimal(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValue(key)
	require.NoError(t, err)
	assert.Equal(t, val2, retrieved) // Should be the new larger value
}

func TestApplySumBigDecimal_DifferentScales(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	key := []byte("test-key")

	// Start with 100 (scale 2) = 1.00
	val1 := bigDecimalToBytes(big.NewInt(100), 2)
	err := applySet(store, key, val1, 100)
	require.NoError(t, err)

	// Add 50 (scale 1) = 50.0
	// When adding: current=100 (scale 2), add=50 (scale 1)
	// Normalize add to scale 2: 50 * 10^(2-1) = 500
	// Result: 100 + 500 = 600 with scale 2
	val2 := bigDecimalToBytes(big.NewInt(50), 1)
	err = applySumBigDecimal(store, key, val2, 100)
	require.NoError(t, err)

	retrieved, err := store.getRawValue(key)
	require.NoError(t, err)
	// Result should be 600 with scale 2
	expected := bigDecimalToBytes(big.NewInt(600), 2)
	assert.Equal(t, expected, retrieved)
}

// Helper functions for creating test data
func createTestStore(t *testing.T) *Store {
	tempDir := t.TempDir()
	dsn, err := storelib.ParseDSN("badger://" + tempDir)
	require.NoError(t, err)

	store, err := NewStore(dsn, "test", 10, nil, false)
	require.NoError(t, err)
	return store
}

func int64ToBytes(val int64) []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(val))
	return bytes
}

func float64ToBytes(val float64) []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, math.Float64bits(val))
	return bytes
}

func bigIntToBytes(val *big.Int) []byte {
	return val.Bytes()
}

func bigDecimalToBytes(val *big.Int, scale int32) []byte {
	bytes := make([]byte, 4+len(val.Bytes()))
	binary.BigEndian.PutUint32(bytes[:4], uint32(scale))
	copy(bytes[4:], val.Bytes())
	return bytes
}
