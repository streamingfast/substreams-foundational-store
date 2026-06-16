package grpc_test

import (
	"context"
	"os"
	"testing"

	grpcsrv "github.com/streamingfast/substreams-foundational-store/grpc"
	"github.com/streamingfast/substreams-foundational-store/grpc/feed"
	pbtest "github.com/streamingfast/substreams-foundational-store/internal/pb/test"
	pbfeed "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/feed/v2"
	pbmodel "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/model/v2"
	pbservice "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/service/v2"
	storelib "github.com/streamingfast/substreams-foundational-store/store"
	"github.com/streamingfast/substreams-foundational-store/store/badger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

const testTypeURL = "type.googleapis.com/test.TestAccountOwner"

func setupBadger(t *testing.T) *badger.Store {
	tempDir, err := os.MkdirTemp("", "remote-feed-test")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(tempDir) })

	dsn, err := storelib.ParseDSN("badger://" + tempDir)
	require.NoError(t, err)

	store, err := badger.NewStore(dsn, testTypeURL, 10, nil)
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })

	return store
}

func anyAccountOwner(t *testing.T, owner string) *anypb.Any {
	data, err := proto.Marshal(&pbtest.TestAccountOwner{Mint: []byte("mint"), Owner: []byte(owner)})
	require.NoError(t, err)
	return &anypb.Any{TypeUrl: testTypeURL, Value: data}
}

// TestRemoteFeedSetAndReadGate exercises the full remote-feed flow at the
// component level: entries pushed through the Feed service are written to the
// store, but reads return block_reached = false until SetReady(true).
func TestRemoteFeedSetAndReadGate(t *testing.T) {
	store := setupBadger(t)
	ctx := context.Background()

	feedServer := feed.NewServer(store, zap.NewNop())
	readServer := grpcsrv.NewRemoteFeedServer(store, zap.NewNop())

	key := []byte("account-1")
	_, err := feedServer.Set(ctx, &pbfeed.SetRequest{
		Entries: &pbmodel.SinkEntries{
			Entries: []*pbmodel.Entry{
				{Key: &pbmodel.Key{Bytes: key}, Value: anyAccountOwner(t, "owner-1")},
			},
		},
	})
	require.NoError(t, err)

	getReq := &pbservice.GetRequest{
		BlockNumber: 100,
		Keys:        []*pbmodel.Key{{Bytes: key}},
	}

	// Not ready yet: read is gated regardless of stored data.
	resp, err := readServer.Get(ctx, getReq)
	require.NoError(t, err)
	assert.False(t, resp.BlockReached)

	// Mark ready through the Feed service.
	_, err = feedServer.SetReady(ctx, &pbfeed.SetReadyRequest{Ready: true})
	require.NoError(t, err)

	// Now the read goes through and finds the entry written by Set.
	resp, err = readServer.Get(ctx, getReq)
	require.NoError(t, err)
	require.True(t, resp.BlockReached)
	require.Len(t, resp.Entries.Entries, 1)
	assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_FOUND, resp.Entries.Entries[0].Code)

	got := &pbtest.TestAccountOwner{}
	require.NoError(t, resp.Entries.Entries[0].Entry.Value.UnmarshalTo(got))
	assert.Equal(t, []byte("owner-1"), got.Owner)
}

// TestRemoteFeedIfNotExist verifies the if_not_exist flag is honored by Set.
func TestRemoteFeedIfNotExist(t *testing.T) {
	store := setupBadger(t)
	ctx := context.Background()

	feedServer := feed.NewServer(store, zap.NewNop())
	readServer := grpcsrv.NewRemoteFeedServer(store, zap.NewNop())
	require.NoError(t, store.SetReady(true))

	key := []byte("account-2")
	_, err := feedServer.Set(ctx, &pbfeed.SetRequest{
		Entries: &pbmodel.SinkEntries{
			Entries: []*pbmodel.Entry{{Key: &pbmodel.Key{Bytes: key}, Value: anyAccountOwner(t, "first")}},
		},
	})
	require.NoError(t, err)

	// Second write with if_not_exist must not overwrite.
	_, err = feedServer.Set(ctx, &pbfeed.SetRequest{
		Entries: &pbmodel.SinkEntries{
			IfNotExist: true,
			Entries:    []*pbmodel.Entry{{Key: &pbmodel.Key{Bytes: key}, Value: anyAccountOwner(t, "second")}},
		},
	})
	require.NoError(t, err)

	resp, err := readServer.Get(ctx, &pbservice.GetRequest{BlockNumber: 0, Keys: []*pbmodel.Key{{Bytes: key}}})
	require.NoError(t, err)
	require.True(t, resp.BlockReached)
	got := &pbtest.TestAccountOwner{}
	require.NoError(t, resp.Entries.Entries[0].Entry.Value.UnmarshalTo(got))
	assert.Equal(t, []byte("first"), got.Owner)
}
