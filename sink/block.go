package sink

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	sink "github.com/streamingfast/substreams/sink"
	"go.uber.org/zap"
)

// LoadLastBlockFromFile reads the last irreversible (flushed) block number persisted to
// blockFilePath. On a cold start the stream resumes from blockNum+1.
//
// It returns (0, false) when the file is missing, empty or cannot be interpreted. For
// backward compatibility with older deployments it also accepts a legacy cursor file and
// migrates it by extracting the cursor's LIB block number (everything up to the LIB was
// already flushed, so it is a safe resume point).
func LoadLastBlockFromFile(logger *zap.Logger, blockFilePath string) (uint64, bool) {
	if blockFilePath == "" {
		return 0, false
	}

	data, err := os.ReadFile(blockFilePath)
	if err != nil {
		if !os.IsNotExist(err) {
			logger.Warn("failed to read block file", zap.String("path", blockFilePath), zap.Error(err))
		}
		return 0, false
	}

	content := strings.TrimSpace(string(data))
	if content == "" {
		return 0, false
	}

	if blockNum, err := strconv.ParseUint(content, 10, 64); err == nil {
		logger.Info("loaded last saved block from file", zap.String("path", blockFilePath), zap.Uint64("block", blockNum))
		return blockNum, true
	}

	// Legacy migration: the file may still contain a cursor string from a previous version.
	// Resume from its LIB, which is the last block guaranteed to have been flushed.
	if cursor, err := sink.NewCursor(content); err == nil && !cursor.IsBlank() {
		lib := cursor.LIB.Num()
		logger.Info("migrated legacy cursor file to last saved block",
			zap.String("path", blockFilePath), zap.Uint64("block", lib))
		return lib, true
	}

	logger.Warn("block file content not understood, starting from configured start block",
		zap.String("path", blockFilePath))
	return 0, false
}

// SaveLastBlockToFile atomically persists the last irreversible block number to blockFilePath.
func SaveLastBlockToFile(blockNum uint64, blockFilePath string, logger *zap.Logger) error {
	if blockFilePath == "" {
		return nil
	}

	tmpPath := blockFilePath + ".tmp"
	if err := os.WriteFile(tmpPath, []byte(strconv.FormatUint(blockNum, 10)), 0644); err != nil {
		logger.Error("failed to write block file", zap.String("path", tmpPath), zap.Error(err))
		return fmt.Errorf("writing block file: %w", err)
	}
	if err := os.Rename(tmpPath, blockFilePath); err != nil {
		logger.Error("failed to rename block file", zap.String("path", blockFilePath), zap.Error(err))
		return fmt.Errorf("renaming block file: %w", err)
	}
	return nil
}
