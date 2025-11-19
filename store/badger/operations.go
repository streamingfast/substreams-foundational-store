package badger

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"

	"github.com/dgraph-io/badger/v3"
	pbssinternal "github.com/streamingfast/substreams/pb/sf/substreams/intern/v2"
)

// Helper functions for operations - package-level for easier testing

func applySet(s *Store, key []byte, value []byte, blockNumber uint64) error {
	return s.setRawValue(key, value, blockNumber)
}

func applySetIfNotExists(s *Store, key []byte, value []byte, blockNumber uint64) error {
	_, err := s.getRawValue(key)
	if err == nil {
		// Exists, skip
		return nil
	}
	if !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to check key %s: %w", string(key), err)
	}
	return s.setRawValue(key, value, blockNumber)
}

func applyAppend(s *Store, key []byte, value []byte, blockNumber uint64) error {
	current, err := s.getRawValue(key)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get key %s: %w", string(key), err)
	}
	newValue := make([]byte, len(current)+len(value))
	copy(newValue, current)
	copy(newValue[len(current):], value)
	return s.setRawValue(key, newValue, blockNumber)
}

func applyDeletePrefix(s *Store, prefix []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.KeyCopy(nil)
			if err := txn.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

func applySetMaxInt64(s *Store, key []byte, value []byte, blockNumber uint64) error {
	currentBytes, err := s.getRawValue(key)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get key %s: %w", string(key), err)
	}
	var current int64
	if len(currentBytes) >= 8 {
		current = int64(binary.BigEndian.Uint64(currentBytes))
	}
	var newVal int64
	if len(value) >= 8 {
		newVal = int64(binary.BigEndian.Uint64(value))
	}
	if newVal > current || len(currentBytes) < 8 {
		return s.setRawValue(key, value, blockNumber)
	}
	return nil
}

func applySetMinInt64(s *Store, key []byte, value []byte, blockNumber uint64) error {
	currentBytes, err := s.getRawValue(key)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get key %s: %w", string(key), err)
	}
	var current int64
	if len(currentBytes) >= 8 {
		current = int64(binary.BigEndian.Uint64(currentBytes))
	}
	var newVal int64
	if len(value) >= 8 {
		newVal = int64(binary.BigEndian.Uint64(value))
	}
	if newVal < current || len(currentBytes) < 8 {
		return s.setRawValue(key, value, blockNumber)
	}
	return nil
}

func applySumInt64(s *Store, key []byte, value []byte, blockNumber uint64) error {
	currentBytes, err := s.getRawValue(key)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get key %s: %w", string(key), err)
	}
	var current int64
	if len(currentBytes) >= 8 {
		current = int64(binary.BigEndian.Uint64(currentBytes))
	}
	var addVal int64
	if len(value) >= 8 {
		addVal = int64(binary.BigEndian.Uint64(value))
	}
	sum := current + addVal
	sumBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(sumBytes, uint64(sum))
	return s.setRawValue(key, sumBytes, blockNumber)
}

func applySetSumInt64(s *Store, key []byte, value []byte, blockNumber uint64) error {
	return applySumInt64(s, key, value, blockNumber)
}

func applySumFloat64(s *Store, key []byte, value []byte, blockNumber uint64) error {
	currentBytes, err := s.getRawValue(key)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get key %s: %w", string(key), err)
	}
	var current float64
	if len(currentBytes) >= 8 {
		current = math.Float64frombits(binary.BigEndian.Uint64(currentBytes))
	}
	var addVal float64
	if len(value) >= 8 {
		addVal = math.Float64frombits(binary.BigEndian.Uint64(value))
	}
	sum := current + addVal
	sumBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(sumBytes, math.Float64bits(sum))
	return s.setRawValue(key, sumBytes, blockNumber)
}

func applySetSumFloat64(s *Store, key []byte, value []byte, blockNumber uint64) error {
	return applySumFloat64(s, key, value, blockNumber)
}

func applySetMaxFloat64(s *Store, key []byte, value []byte, blockNumber uint64) error {
	currentBytes, err := s.getRawValue(key)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get key %s: %w", string(key), err)
	}
	var current float64
	if len(currentBytes) >= 8 {
		current = math.Float64frombits(binary.BigEndian.Uint64(currentBytes))
	}
	var newVal float64
	if len(value) >= 8 {
		newVal = math.Float64frombits(binary.BigEndian.Uint64(value))
	}
	if newVal > current || len(currentBytes) < 8 {
		return s.setRawValue(key, value, blockNumber)
	}
	return nil
}

func applySetMinFloat64(s *Store, key []byte, value []byte, blockNumber uint64) error {
	currentBytes, err := s.getRawValue(key)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get key %s: %w", string(key), err)
	}
	var current float64
	if len(currentBytes) >= 8 {
		current = math.Float64frombits(binary.BigEndian.Uint64(currentBytes))
	}
	var newVal float64
	if len(value) >= 8 {
		newVal = math.Float64frombits(binary.BigEndian.Uint64(value))
	}
	if newVal < current || len(currentBytes) < 8 {
		return s.setRawValue(key, value, blockNumber)
	}
	return nil
}

func applySetMaxBigInt(s *Store, key []byte, value []byte, blockNumber uint64) error {
	currentBytes, err := s.getRawValue(key)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get key %s: %w", string(key), err)
	}
	currentBigInt := &big.Int{}
	if len(currentBytes) > 0 {
		currentBigInt.SetBytes(currentBytes)
	}
	newBigInt := &big.Int{}
	if len(value) > 0 {
		newBigInt.SetBytes(value)
	}
	if newBigInt.Cmp(currentBigInt) > 0 || len(currentBytes) == 0 {
		return s.setRawValue(key, value, blockNumber)
	}
	return nil
}

func applySetMinBigInt(s *Store, key []byte, value []byte, blockNumber uint64) error {
	currentBytes, err := s.getRawValue(key)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get key %s: %w", string(key), err)
	}
	currentBigInt := &big.Int{}
	if len(currentBytes) > 0 {
		currentBigInt.SetBytes(currentBytes)
	}
	newBigInt := &big.Int{}
	if len(value) > 0 {
		newBigInt.SetBytes(value)
	}
	if newBigInt.Cmp(currentBigInt) < 0 || len(currentBytes) == 0 {
		return s.setRawValue(key, value, blockNumber)
	}
	return nil
}

func applySumBigInt(s *Store, key []byte, value []byte, blockNumber uint64) error {
	currentBytes, err := s.getRawValue(key)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get key %s: %w", string(key), err)
	}
	currentBigInt := &big.Int{}
	if len(currentBytes) > 0 {
		currentBigInt.SetBytes(currentBytes)
	}
	addBigInt := &big.Int{}
	if len(value) > 0 {
		addBigInt.SetBytes(value)
	}
	sum := &big.Int{}
	sum.Add(currentBigInt, addBigInt)
	sumBytes := sum.Bytes()
	return s.setRawValue(key, sumBytes, blockNumber)
}

func applySetSumBigInt(s *Store, key []byte, value []byte, blockNumber uint64) error {
	return applySumBigInt(s, key, value, blockNumber)
}

func applySetMaxBigDecimal(s *Store, key []byte, value []byte, blockNumber uint64) error {
	currentBytes, err := s.getRawValue(key)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get key %s: %w", string(key), err)
	}
	var currentScale int32
	var currentValue *big.Int
	if len(currentBytes) >= 4 {
		currentScale = int32(binary.BigEndian.Uint32(currentBytes[:4]))
		currentValue = &big.Int{}
		currentValue.SetBytes(currentBytes[4:])
	} else {
		currentScale = 0
		currentValue = &big.Int{}
	}

	var newScale int32
	var newValue *big.Int
	if len(value) >= 4 {
		newScale = int32(binary.BigEndian.Uint32(value[:4]))
		newValue = &big.Int{}
		newValue.SetBytes(value[4:])
	} else {
		newScale = 0
		newValue = &big.Int{}
	}

	// Normalize scales for comparison
	if newScale > currentScale {
		scaleDiff := newScale - currentScale
		multiplier := &big.Int{}
		multiplier.Exp(big.NewInt(10), big.NewInt(int64(scaleDiff)), nil)
		currentValue.Mul(currentValue, multiplier)
	} else if currentScale > newScale {
		scaleDiff := currentScale - newScale
		multiplier := &big.Int{}
		multiplier.Exp(big.NewInt(10), big.NewInt(int64(scaleDiff)), nil)
		newValue.Mul(newValue, multiplier)
	}

	if newValue.Cmp(currentValue) > 0 || len(currentBytes) < 4 {
		return s.setRawValue(key, value, blockNumber)
	}
	return nil
}

func applySetMinBigDecimal(s *Store, key []byte, value []byte, blockNumber uint64) error {
	currentBytes, err := s.getRawValue(key)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get key %s: %w", string(key), err)
	}
	var currentScale int32
	var currentValue *big.Int
	if len(currentBytes) >= 4 {
		currentScale = int32(binary.BigEndian.Uint32(currentBytes[:4]))
		currentValue = &big.Int{}
		currentValue.SetBytes(currentBytes[4:])
	} else {
		currentScale = 0
		currentValue = &big.Int{}
	}

	var newScale int32
	var newValue *big.Int
	if len(value) >= 4 {
		newScale = int32(binary.BigEndian.Uint32(value[:4]))
		newValue = &big.Int{}
		newValue.SetBytes(value[4:])
	} else {
		newScale = 0
		newValue = &big.Int{}
	}

	// Normalize scales for comparison
	if newScale > currentScale {
		scaleDiff := newScale - currentScale
		multiplier := &big.Int{}
		multiplier.Exp(big.NewInt(10), big.NewInt(int64(scaleDiff)), nil)
		currentValue.Mul(currentValue, multiplier)
	} else if currentScale > newScale {
		scaleDiff := currentScale - newScale
		multiplier := &big.Int{}
		multiplier.Exp(big.NewInt(10), big.NewInt(int64(scaleDiff)), nil)
		newValue.Mul(newValue, multiplier)
	}

	if newValue.Cmp(currentValue) < 0 || len(currentBytes) < 4 {
		return s.setRawValue(key, value, blockNumber)
	}
	return nil
}

func applySumBigDecimal(s *Store, key []byte, value []byte, blockNumber uint64) error {
	currentBytes, err := s.getRawValue(key)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get key %s: %w", string(key), err)
	}
	var currentScale int32
	var currentValue *big.Int
	if len(currentBytes) >= 4 {
		currentScale = int32(binary.BigEndian.Uint32(currentBytes[:4]))
		currentValue = &big.Int{}
		currentValue.SetBytes(currentBytes[4:])
	} else {
		currentScale = 0
		currentValue = &big.Int{}
	}

	var addScale int32
	var addValue *big.Int
	if len(value) >= 4 {
		addScale = int32(binary.BigEndian.Uint32(value[:4]))
		addValue = &big.Int{}
		addValue.SetBytes(value[4:])
	} else {
		addScale = 0
		addValue = &big.Int{}
	}

	// Normalize scales for addition
	resultScale := currentScale
	if addScale > currentScale {
		resultScale = addScale
		scaleDiff := addScale - currentScale
		multiplier := &big.Int{}
		multiplier.Exp(big.NewInt(10), big.NewInt(int64(scaleDiff)), nil)
		currentValue.Mul(currentValue, multiplier)
	} else if currentScale > addScale {
		scaleDiff := currentScale - addScale
		multiplier := &big.Int{}
		multiplier.Exp(big.NewInt(10), big.NewInt(int64(scaleDiff)), nil)
		addValue.Mul(addValue, multiplier)
	}

	sum := &big.Int{}
	sum.Add(currentValue, addValue)

	resultBytes := make([]byte, 4+len(sum.Bytes()))
	binary.BigEndian.PutUint32(resultBytes[:4], uint32(resultScale))
	copy(resultBytes[4:], sum.Bytes())

	return s.setRawValue(key, resultBytes, blockNumber)
}

func applySetSumBigDecimal(s *Store, key []byte, value []byte, blockNumber uint64) error {
	return applySumBigDecimal(s, key, value, blockNumber)
}

// Time traversal versions of the helper functions

func applySetTimeTraversal(s *Store, key []byte, value []byte, blockNumber uint64) error {
	return s.setRawValueTimeTraversal(key, value, blockNumber)
}

func applySetIfNotExistsTimeTraversal(s *Store, key []byte, value []byte, blockNumber uint64) error {
	_, err := s.getRawValueTimeTraversal(key, blockNumber)
	if err == nil {
		// Exists, skip
		return nil
	}
	if !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to check key %s: %w", string(key), err)
	}
	return s.setRawValueTimeTraversal(key, value, blockNumber)
}

func applyAppendTimeTraversal(s *Store, key []byte, value []byte, blockNumber uint64) error {
	current, err := s.getRawValueTimeTraversal(key, blockNumber)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get key %s: %w", string(key), err)
	}
	newValue := make([]byte, len(current)+len(value))
	copy(newValue, current)
	copy(newValue[len(current):], value)
	return s.setRawValueTimeTraversal(key, newValue, blockNumber)
}

func applySetMaxInt64TimeTraversal(s *Store, key []byte, value []byte, blockNumber uint64) error {
	currentBytes, err := s.getRawValueTimeTraversal(key, blockNumber)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get key %s: %w", string(key), err)
	}
	var current int64
	if len(currentBytes) >= 8 {
		current = int64(binary.BigEndian.Uint64(currentBytes))
	}
	var newVal int64
	if len(value) >= 8 {
		newVal = int64(binary.BigEndian.Uint64(value))
	}
	if newVal > current || len(currentBytes) < 8 {
		return s.setRawValueTimeTraversal(key, value, blockNumber)
	}
	return nil
}

func applySetMinInt64TimeTraversal(s *Store, key []byte, value []byte, blockNumber uint64) error {
	currentBytes, err := s.getRawValueTimeTraversal(key, blockNumber)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get key %s: %w", string(key), err)
	}
	var current int64
	if len(currentBytes) >= 8 {
		current = int64(binary.BigEndian.Uint64(currentBytes))
	}
	var newVal int64
	if len(value) >= 8 {
		newVal = int64(binary.BigEndian.Uint64(value))
	}
	if newVal < current || len(currentBytes) < 8 {
		return s.setRawValueTimeTraversal(key, value, blockNumber)
	}
	return nil
}

func applySumInt64TimeTraversal(s *Store, key []byte, value []byte, blockNumber uint64) error {
	currentBytes, err := s.getRawValueTimeTraversal(key, blockNumber)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get key %s: %w", string(key), err)
	}
	var current int64
	if len(currentBytes) >= 8 {
		current = int64(binary.BigEndian.Uint64(currentBytes))
	}
	var addVal int64
	if len(value) >= 8 {
		addVal = int64(binary.BigEndian.Uint64(value))
	}
	sum := current + addVal
	sumBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(sumBytes, uint64(sum))
	return s.setRawValueTimeTraversal(key, sumBytes, blockNumber)
}

func applySetSumInt64TimeTraversal(s *Store, key []byte, value []byte, blockNumber uint64) error {
	return applySumInt64TimeTraversal(s, key, value, blockNumber)
}

func applySumFloat64TimeTraversal(s *Store, key []byte, value []byte, blockNumber uint64) error {
	currentBytes, err := s.getRawValueTimeTraversal(key, blockNumber)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get key %s: %w", string(key), err)
	}
	var current float64
	if len(currentBytes) >= 8 {
		current = math.Float64frombits(binary.BigEndian.Uint64(currentBytes))
	}
	var addVal float64
	if len(value) >= 8 {
		addVal = math.Float64frombits(binary.BigEndian.Uint64(value))
	}
	sum := current + addVal
	sumBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(sumBytes, math.Float64bits(sum))
	return s.setRawValueTimeTraversal(key, sumBytes, blockNumber)
}

func applySetSumFloat64TimeTraversal(s *Store, key []byte, value []byte, blockNumber uint64) error {
	return applySumFloat64TimeTraversal(s, key, value, blockNumber)
}

func applySetMaxFloat64TimeTraversal(s *Store, key []byte, value []byte, blockNumber uint64) error {
	currentBytes, err := s.getRawValueTimeTraversal(key, blockNumber)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get key %s: %w", string(key), err)
	}
	var current float64
	if len(currentBytes) >= 8 {
		current = math.Float64frombits(binary.BigEndian.Uint64(currentBytes))
	}
	var newVal float64
	if len(value) >= 8 {
		newVal = math.Float64frombits(binary.BigEndian.Uint64(value))
	}
	if newVal > current || len(currentBytes) < 8 {
		return s.setRawValueTimeTraversal(key, value, blockNumber)
	}
	return nil
}

func applySetMinFloat64TimeTraversal(s *Store, key []byte, value []byte, blockNumber uint64) error {
	currentBytes, err := s.getRawValueTimeTraversal(key, blockNumber)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get key %s: %w", string(key), err)
	}
	var current float64
	if len(currentBytes) >= 8 {
		current = math.Float64frombits(binary.BigEndian.Uint64(currentBytes))
	}
	var newVal float64
	if len(value) >= 8 {
		newVal = math.Float64frombits(binary.BigEndian.Uint64(value))
	}
	if newVal < current || len(currentBytes) < 8 {
		return s.setRawValueTimeTraversal(key, value, blockNumber)
	}
	return nil
}

func applySetMaxBigIntTimeTraversal(s *Store, key []byte, value []byte, blockNumber uint64) error {
	currentBytes, err := s.getRawValueTimeTraversal(key, blockNumber)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get key %s: %w", string(key), err)
	}
	currentBigInt := &big.Int{}
	if len(currentBytes) > 0 {
		currentBigInt.SetBytes(currentBytes)
	}
	newBigInt := &big.Int{}
	if len(value) > 0 {
		newBigInt.SetBytes(value)
	}
	if newBigInt.Cmp(currentBigInt) > 0 || len(currentBytes) == 0 {
		return s.setRawValueTimeTraversal(key, value, blockNumber)
	}
	return nil
}

func applySetMinBigIntTimeTraversal(s *Store, key []byte, value []byte, blockNumber uint64) error {
	currentBytes, err := s.getRawValueTimeTraversal(key, blockNumber)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get key %s: %w", string(key), err)
	}
	currentBigInt := &big.Int{}
	if len(currentBytes) > 0 {
		currentBigInt.SetBytes(currentBytes)
	}
	newBigInt := &big.Int{}
	if len(value) > 0 {
		newBigInt.SetBytes(value)
	}
	if newBigInt.Cmp(currentBigInt) < 0 || len(currentBytes) == 0 {
		return s.setRawValueTimeTraversal(key, value, blockNumber)
	}
	return nil
}

func applySumBigIntTimeTraversal(s *Store, key []byte, value []byte, blockNumber uint64) error {
	currentBytes, err := s.getRawValueTimeTraversal(key, blockNumber)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get key %s: %w", string(key), err)
	}
	currentBigInt := &big.Int{}
	if len(currentBytes) > 0 {
		currentBigInt.SetBytes(currentBytes)
	}
	addBigInt := &big.Int{}
	if len(value) > 0 {
		addBigInt.SetBytes(value)
	}
	sum := &big.Int{}
	sum.Add(currentBigInt, addBigInt)
	sumBytes := sum.Bytes()
	return s.setRawValueTimeTraversal(key, sumBytes, blockNumber)
}

func applySetSumBigIntTimeTraversal(s *Store, key []byte, value []byte, blockNumber uint64) error {
	return applySumBigIntTimeTraversal(s, key, value, blockNumber)
}

func applySetMaxBigDecimalTimeTraversal(s *Store, key []byte, value []byte, blockNumber uint64) error {
	currentBytes, err := s.getRawValueTimeTraversal(key, blockNumber)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get key %s: %w", string(key), err)
	}
	var currentScale int32
	var currentValue *big.Int
	if len(currentBytes) >= 4 {
		currentScale = int32(binary.BigEndian.Uint32(currentBytes[:4]))
		currentValue = &big.Int{}
		currentValue.SetBytes(currentBytes[4:])
	} else {
		currentScale = 0
		currentValue = &big.Int{}
	}

	var newScale int32
	var newValue *big.Int
	if len(value) >= 4 {
		newScale = int32(binary.BigEndian.Uint32(value[:4]))
		newValue = &big.Int{}
		newValue.SetBytes(value[4:])
	} else {
		newScale = 0
		newValue = &big.Int{}
	}

	// Normalize scales for comparison
	if newScale > currentScale {
		scaleDiff := newScale - currentScale
		multiplier := &big.Int{}
		multiplier.Exp(big.NewInt(10), big.NewInt(int64(scaleDiff)), nil)
		currentValue.Mul(currentValue, multiplier)
	} else if currentScale > newScale {
		scaleDiff := currentScale - newScale
		multiplier := &big.Int{}
		multiplier.Exp(big.NewInt(10), big.NewInt(int64(scaleDiff)), nil)
		newValue.Mul(newValue, multiplier)
	}

	if newValue.Cmp(currentValue) > 0 || len(currentBytes) < 4 {
		return s.setRawValueTimeTraversal(key, value, blockNumber)
	}
	return nil
}

func applySetMinBigDecimalTimeTraversal(s *Store, key []byte, value []byte, blockNumber uint64) error {
	currentBytes, err := s.getRawValueTimeTraversal(key, blockNumber)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get key %s: %w", string(key), err)
	}
	var currentScale int32
	var currentValue *big.Int
	if len(currentBytes) >= 4 {
		currentScale = int32(binary.BigEndian.Uint32(currentBytes[:4]))
		currentValue = &big.Int{}
		currentValue.SetBytes(currentBytes[4:])
	} else {
		currentScale = 0
		currentValue = &big.Int{}
	}

	var newScale int32
	var newValue *big.Int
	if len(value) >= 4 {
		newScale = int32(binary.BigEndian.Uint32(value[:4]))
		newValue = &big.Int{}
		newValue.SetBytes(value[4:])
	} else {
		newScale = 0
		newValue = &big.Int{}
	}

	// Normalize scales for comparison
	if newScale > currentScale {
		scaleDiff := newScale - currentScale
		multiplier := &big.Int{}
		multiplier.Exp(big.NewInt(10), big.NewInt(int64(scaleDiff)), nil)
		currentValue.Mul(currentValue, multiplier)
	} else if currentScale > newScale {
		scaleDiff := currentScale - newScale
		multiplier := &big.Int{}
		multiplier.Exp(big.NewInt(10), big.NewInt(int64(scaleDiff)), nil)
		newValue.Mul(newValue, multiplier)
	}

	if newValue.Cmp(currentValue) < 0 || len(currentBytes) < 4 {
		return s.setRawValueTimeTraversal(key, value, blockNumber)
	}
	return nil
}

func applySumBigDecimalTimeTraversal(s *Store, key []byte, value []byte, blockNumber uint64) error {
	currentBytes, err := s.getRawValueTimeTraversal(key, blockNumber)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get key %s: %w", string(key), err)
	}
	var currentScale int32
	var currentValue *big.Int
	if len(currentBytes) >= 4 {
		currentScale = int32(binary.BigEndian.Uint32(currentBytes[:4]))
		currentValue = &big.Int{}
		currentValue.SetBytes(currentBytes[4:])
	} else {
		currentScale = 0
		currentValue = &big.Int{}
	}

	var addScale int32
	var addValue *big.Int
	if len(value) >= 4 {
		addScale = int32(binary.BigEndian.Uint32(value[:4]))
		addValue = &big.Int{}
		addValue.SetBytes(value[4:])
	} else {
		addScale = 0
		addValue = &big.Int{}
	}

	// Normalize scales for addition
	resultScale := currentScale
	if addScale > currentScale {
		resultScale = addScale
		scaleDiff := addScale - currentScale
		multiplier := &big.Int{}
		multiplier.Exp(big.NewInt(10), big.NewInt(int64(scaleDiff)), nil)
		currentValue.Mul(currentValue, multiplier)
	} else if currentScale > addScale {
		scaleDiff := currentScale - addScale
		multiplier := &big.Int{}
		multiplier.Exp(big.NewInt(10), big.NewInt(int64(scaleDiff)), nil)
		addValue.Mul(addValue, multiplier)
	}

	sum := &big.Int{}
	sum.Add(currentValue, addValue)

	resultBytes := make([]byte, 4+len(sum.Bytes()))
	binary.BigEndian.PutUint32(resultBytes[:4], uint32(resultScale))
	copy(resultBytes[4:], sum.Bytes())

	return s.setRawValueTimeTraversal(key, resultBytes, blockNumber)
}

func applySetSumBigDecimalTimeTraversal(s *Store, key []byte, value []byte, blockNumber uint64) error {
	return applySumBigDecimalTimeTraversal(s, key, value, blockNumber)
}

// applyOperationDirect applies a single operation using direct key storage
func (s *Store) applyOperationDirect(op *pbssinternal.Operation, blockNumber uint64) error {
	key := []byte(op.Key)
	switch op.Type {
	case pbssinternal.Operation_SET, pbssinternal.Operation_SET_BYTES:
		return applySet(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SET_IF_NOT_EXISTS, pbssinternal.Operation_SET_BYTES_IF_NOT_EXISTS:
		return applySetIfNotExists(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_APPEND:
		return applyAppend(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_DELETE_PREFIX:
		return applyDeletePrefix(s, key)
	case pbssinternal.Operation_SET_MAX_INT64:
		return applySetMaxInt64(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SET_MIN_INT64:
		return applySetMinInt64(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SET_MAX_BIG_INT:
		return applySetMaxBigInt(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SET_MAX_FLOAT64:
		return applySetMaxFloat64(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SET_MAX_BIG_DECIMAL:
		return applySetMaxBigDecimal(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SET_MIN_BIG_INT:
		return applySetMinBigInt(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SET_MIN_FLOAT64:
		return applySetMinFloat64(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SET_MIN_BIG_DECIMAL:
		return applySetMinBigDecimal(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SUM_BIG_INT:
		return applySumBigInt(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SUM_INT64:
		return applySumInt64(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SUM_FLOAT64:
		return applySumFloat64(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SUM_BIG_DECIMAL:
		return applySumBigDecimal(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SET_SUM_INT64:
		return applySetSumInt64(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SET_SUM_FLOAT64:
		return applySetSumFloat64(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SET_SUM_BIG_INT:
		return applySetSumBigInt(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SET_SUM_BIG_DECIMAL:
		return applySetSumBigDecimal(s, key, op.Value, blockNumber)
	default:
		return fmt.Errorf("unsupported operation type %v", op.Type)
	}
}

// applyOperationWithTimeTraversal applies a single operation using time traversal key encoding
func (s *Store) applyOperationWithTimeTraversal(op *pbssinternal.Operation, blockNumber uint64) error {
	key := []byte(op.Key)
	switch op.Type {
	case pbssinternal.Operation_SET, pbssinternal.Operation_SET_BYTES:
		return applySetTimeTraversal(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SET_IF_NOT_EXISTS, pbssinternal.Operation_SET_BYTES_IF_NOT_EXISTS:
		return applySetIfNotExistsTimeTraversal(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_APPEND:
		return applyAppendTimeTraversal(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_DELETE_PREFIX:
		return applyDeletePrefix(s, key)
	case pbssinternal.Operation_SET_MAX_INT64:
		return applySetMaxInt64TimeTraversal(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SET_MIN_INT64:
		return applySetMinInt64TimeTraversal(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SET_MAX_BIG_INT:
		return applySetMaxBigIntTimeTraversal(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SET_MAX_FLOAT64:
		return applySetMaxFloat64TimeTraversal(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SET_MAX_BIG_DECIMAL:
		return applySetMaxBigDecimalTimeTraversal(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SET_MIN_BIG_INT:
		return applySetMinBigIntTimeTraversal(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SET_MIN_FLOAT64:
		return applySetMinFloat64TimeTraversal(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SET_MIN_BIG_DECIMAL:
		return applySetMinBigDecimalTimeTraversal(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SUM_BIG_INT:
		return applySumBigIntTimeTraversal(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SUM_INT64:
		return applySumInt64TimeTraversal(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SUM_FLOAT64:
		return applySumFloat64TimeTraversal(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SUM_BIG_DECIMAL:
		return applySumBigDecimalTimeTraversal(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SET_SUM_INT64:
		return applySetSumInt64TimeTraversal(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SET_SUM_FLOAT64:
		return applySetSumFloat64TimeTraversal(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SET_SUM_BIG_INT:
		return applySetSumBigIntTimeTraversal(s, key, op.Value, blockNumber)
	case pbssinternal.Operation_SET_SUM_BIG_DECIMAL:
		return applySetSumBigDecimalTimeTraversal(s, key, op.Value, blockNumber)
	default:
		return fmt.Errorf("unsupported operation type %v", op.Type)
	}
}
