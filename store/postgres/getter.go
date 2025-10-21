package postgres

import (
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	"github.com/streamingfast/substreams-foundational-store/sink"
	"google.golang.org/protobuf/types/known/anypb"
)

type Entry struct {
	BlockNumber uint64    `db:"block_number"`
	BlockHash   []byte    `db:"block_hash"`
	Key         []byte    `db:"key"`
	Value       []byte    `db:"value"`
	CreateTime  time.Time `db:"create_time"`
}

func (s *Store) Get(request *pbstore.GetRequest) (*pbstore.GetResponse, error) {
	// Track total execution time
	executionStart := time.Now()
	defer func() {
		sink.DatabaseExecutionDuration.ObserveDuration(time.Since(executionStart))
	}()

	// Track key requests - single key per Get call
	sink.DatabaseKeysRequestedTotal.Inc()
	sink.DatabaseCallCount.Inc()
	defer func() {
		sink.DatabaseKeysProcessed.Inc()
	}()

	entry := &Entry{}
	err := s.selectStatement.Get(entry, request.Key, request.BlockNumber, request.BlockHash)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Track no keys found for this call
			sink.DatabaseGetMisses.Inc()
			return &pbstore.GetResponse{
				Code: pbstore.ResponseCode_RESPONSE_CODE_NOT_FOUND,
			}, nil
		}
		sink.DatabaseGetErrors.Inc()
		return nil, err
	}

	// Track found keys - single key found
	sink.DatabaseKeysFoundTotal.Inc()
	sink.DatabaseGetHits.Inc()

	return &pbstore.GetResponse{
		Code: pbstore.ResponseCode_RESPONSE_CODE_FOUND,
		Value: &anypb.Any{
			TypeUrl: s.typeUrl,
			Value:   entry.Value,
		},
	}, nil
}

func (s *Store) GetAll(request *pbstore.GetAllRequest) (*pbstore.GetAllResponse, error) {
	// Track total execution time
	executionStart := time.Now()
	defer func() {
		sink.DatabaseExecutionDuration.ObserveDuration(time.Since(executionStart))
	}()

	// Track key requests - number of keys in this GetAll call
	sink.DatabaseKeysRequestedTotal.AddInt(len(request.Keys))
	sink.DatabaseCallCount.Inc()
	defer func() {
		sink.DatabaseKeysProcessed.AddInt(len(request.Keys))
	}()

	rows, err := s.selectAnyStatement.Queryx(pq.Array(request.Keys), request.BlockNumber, request.BlockHash)
	if err != nil {
		return nil, fmt.Errorf("failed to select entries: %w", err)
	}

	var entriesMap = make(map[string]*Entry)
	for rows.Next() {
		entry := &Entry{}
		err := rows.StructScan(entry)
		if err != nil {
			return nil, fmt.Errorf("failed to scan entry: %w", err)
		}
		entriesMap[hex.EncodeToString(entry.Key)] = entry
	}

	var keysFoundCount int
	out := []*pbstore.ResponseEntry{}
	for _, key := range request.Keys {
		entry, found := entriesMap[hex.EncodeToString(key)]
		if !found {
			out =
				append(out, &pbstore.ResponseEntry{
					Key: key,
					Response: &pbstore.GetResponse{
						Code: pbstore.ResponseCode_RESPONSE_CODE_NOT_FOUND,
						Value: &anypb.Any{
							TypeUrl: s.typeUrl,
							Value:   nil,
						},
					},
				})
			continue
		}
		keysFoundCount++
		out = append(out, &pbstore.ResponseEntry{
			Key: key,
			Response: &pbstore.GetResponse{
				Code: pbstore.ResponseCode_RESPONSE_CODE_FOUND,
				Value: &anypb.Any{
					TypeUrl: s.typeUrl,
					Value:   entry.Value,
				},
			},
		})
	}

	// Track found keys - number of keys found in this GetAll call
	sink.DatabaseKeysFoundTotal.AddInt(keysFoundCount)

	return &pbstore.GetAllResponse{
		Entries: out,
	}, nil
}
