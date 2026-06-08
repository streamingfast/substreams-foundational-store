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
	store         store.ForkawareStore
	logger        *zap.Logger
	blockFilePath string

	// lastSavedBlock is the highest block number we have flushed and persisted. Everything
	// up to and including it is irreversible and durable, so a cold restart resumes from
	// lastSavedBlock+1. The in-process cursor used for hot reconnection is handled entirely
	// by the substreams sink library; we never persist it.
	lastSavedBlock uint64

	// Shutdown coordination
	*shutter.Shutter

	headBlock uint64
}

func NewSinker(store store.ForkawareStore, logger *zap.Logger, blockFilePath string, lastSavedBlock uint64) *Sinker {
	logger = logger.Named("foundational-store-sinker")

	return &Sinker{
		store:          store,
		logger:         logger,
		blockFilePath:  blockFilePath,
		lastSavedBlock: lastSavedBlock,
		Shutter:        shutter.New(),
		headBlock:      lastSavedBlock,
	}
}

func (s *Sinker) HandleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {
	lib := cursor.LIB.Num()

	// Process data if present
	if data.Output != nil && data.Output.MapOutput != nil && data.Output.MapOutput.Value != nil {
		entries := &pbmodel.SinkEntries{}
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

		if err := s.store.SetAll(entries.Entries, entries.IfNotExist, data.GetClock().Number); err != nil {
			return fmt.Errorf("setting foundational-store entry: %w", err)
		}
	}

	// Flush finalized data (<= lib) on every block, before persisting our resume point, so
	// the saved block is always backed by durable data. Doing this only on output blocks
	// would let the resume point advance past finalized-but-unflushed blocks and lose their
	// writes on restart.
	if err := s.store.FlushUpToBlock(lib); err != nil {
		return fmt.Errorf("flushing up to lib %d: %w", lib, err)
	}

	// Everything up to and including the LIB is now durable and irreversible. Persist it as
	// the cold-restart resume point (we resume from lib+1). The reversible segment
	// (lib..head) lives only in memory and is re-streamed on restart; transient
	// disconnections are recovered by the substreams sink library via its in-memory cursor.
	if lib > s.lastSavedBlock {
		if err := SaveLastBlockToFile(lib, s.blockFilePath, s.logger); err != nil {
			return fmt.Errorf("saving last block to file: %w", err)
		}
		s.lastSavedBlock = lib
	}

	s.headBlock = data.GetClock().Number
	return nil
}

func (s *Sinker) HandleBlockUndoSignal(ctx context.Context, undoSignal *pbsubstreamsrpc.BlockUndoSignal, cursor *sink.Cursor) error {
	blockNum := undoSignal.LastValidBlock.Number
	s.headBlock = blockNum

	if err := s.store.EvictAfterBlock(blockNum); err != nil {
		return fmt.Errorf("failed to evict data up to block %d: %w", blockNum, err)
	}

	// No resume point to persist: an undo only affects the reversible segment (> lib), which
	// we never write to the block file.
	s.logger.Debug("evicted data due to undo signal", zap.Uint64("block_number", blockNum))

	return nil
}

func (s *Sinker) HeadBlock() uint64 {
	return s.headBlock
}
