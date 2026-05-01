package sink

import (
	"context"
	"fmt"

	"github.com/streamingfast/shutter"
	pbmodel "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/model/v2"
	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/service/v1"
	"github.com/streamingfast/substreams-foundational-store/store"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	sink "github.com/streamingfast/substreams/sink"
	"go.uber.org/zap"
)

type Sinker struct {
	store          store.ForkawareStore
	logger         *zap.Logger
	cursorFilePath string

	cursorHistory map[string]*sink.Cursor

	// Shutdown coordination
	*shutter.Shutter

	headBlock uint64
}

func NewSinker(store store.ForkawareStore, logger *zap.Logger, cursorFilePath string, cursor *sink.Cursor) *Sinker {
	logger = logger.Named("foundational-store-sinker")

	shutter := shutter.New()

	headBlock := uint64(0)
	if cursor != nil {
		headBlock = cursor.HeadBlock.Num()
	}

	sinker := &Sinker{
		store:          store,
		logger:         logger,
		cursorFilePath: cursorFilePath,
		cursorHistory:  map[string]*sink.Cursor{},
		Shutter:        shutter,
		headBlock:      headBlock,
	}
	return sinker
}

func (s *Sinker) HandleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {

	s.cursorHistory[data.Clock.Id] = cursor

	lib := cursor.LIB.Num()

	// Process data if present
	if data.Output != nil && data.Output.MapOutput != nil && data.Output.MapOutput.Value != nil {
		entries := &pbmodel.SinkEntries{}
		//fmt.Println(data.Output.MapOutput.TypeUrl)
		if data.Output.MapOutput.TypeUrl == "type.googleapis.com/sf.substreams.foundational_store.v1.Entries" {
			legacyEntries := &pbstore.Entries{}
			if err := data.Output.MapOutput.UnmarshalTo(legacyEntries); err != nil {
				return fmt.Errorf("unmarshaling map output to Entry: %w", err)
			}
			for _, i := range legacyEntries.Entries {
				entries.Entries = append(entries.Entries, &pbmodel.Entry{
					Key:   &pbmodel.Key{Bytes: i.Key},
					Value: i.Value,
				})
			}

		} else {
			if err := data.Output.MapOutput.UnmarshalTo(entries); err != nil {
				return fmt.Errorf("unmarshaling map output to Entry: %w", err)
			}

		}

		if err := s.store.SetAll(entries.Entries, entries.DeletePrefixes, entries.IfNotExist, data.GetClock().Number); err != nil {
			return fmt.Errorf("setting foundational-store entry: %w", err)
		}

		if _, err := s.store.FlushUpToBlock(lib, entries.IfNotExist); err != nil {
			return fmt.Errorf("flushing up to block up to lib %d: %w", lib, err)
		}
	}

	libCursor := s.cursorHistory[cursor.LIB.ID()]
	if libCursor == nil {
		libCursor = cursor
	}

	// Always save the cursor to a file, regardless of whether there was output data
	if err := SaveCursorToFile(libCursor, s.cursorFilePath, s.logger); err != nil {
		return fmt.Errorf("saving cursor to file %w", err)
	}

	for _, historyCursor := range s.cursorHistory {
		if historyCursor.Block().Num() <= lib {
			delete(s.cursorHistory, historyCursor.Block().ID())
		}
	}

	blockNum := data.GetClock().Number

	s.headBlock = blockNum
	return nil
}

func (s *Sinker) HandleBlockUndoSignal(ctx context.Context, undoSignal *pbsubstreamsrpc.BlockUndoSignal, cursor *sink.Cursor) error {
	blockNum := undoSignal.LastValidBlock.Number
	s.headBlock = blockNum

	if _, err := s.store.EvictUpToBlock(blockNum); err != nil {
		return fmt.Errorf("failed to evict data up to block %d: %w", blockNum, err)
	}

	// Save the cursor to a file after handling the undo signal
	if err := SaveCursorToFile(cursor, s.cursorFilePath, s.logger); err != nil {
		s.logger.Warn("failed to save cursor to file after undo signal", zap.Error(err))
		// Don't return an error here, as we don't want to fail the processing
	}

	s.logger.Debug("evicted data due to undo signal",
		zap.Uint64("block_number", blockNum))

	return nil
}

func (s *Sinker) HeadBlock() uint64 {
	return s.headBlock
}
