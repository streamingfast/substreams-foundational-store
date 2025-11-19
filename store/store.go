package store

import (
	pbmodel "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/model/v2"
	pbservice "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/service/v2"
	pbssinternal "github.com/streamingfast/substreams/pb/sf/substreams/intern/v2"
)

type Store interface {
	SetAll(entries []*pbmodel.Entry, IfNotExist bool, blockNumber uint64) error
	Get(request *pbservice.GetRequest) (*pbservice.GetResponse, error)
	GetFirst(request *pbservice.GetRequest) (*pbservice.GetResponse, error)
	ApplyOperations(operations []*pbssinternal.Operation, blockNumber uint64) error
}

type ForkawareStore interface {
	Store
	FlushUpToBlock(blockNum uint64, IfNotExist bool) error
	EvictUpToBlock(upToBlockNumber uint64) error
}
