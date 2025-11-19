package badger

import (
	"testing"

	pbtest "github.com/streamingfast/substreams-foundational-store/internal/pb/test"
	pbmodel "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/model/v2"
	pbservice "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/service/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetAllFirst_Badger(t *testing.T) {
	testCases := []struct {
		name                 string
		timeTraversalEnabled bool
	}{
		{"without_time_traversal", false},
		{"with_time_traversal", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ts := setupTestStoreWithTimeTraversal(t, tc.timeTraversalEnabled)
			defer ts.cleanup()

			// Prepare two entries
			owner1 := createAccountOwner("owner-1")
			entry1, err := createEntry(100, []byte("k1"), owner1, ts.typeURL)
			require.NoError(t, err)
			require.NoError(t, ts.store.Set(entry1, false, 100))

			owner2 := createAccountOwner("owner-2")
			entry2, err := createEntry(100, []byte("k2"), owner2, ts.typeURL)
			require.NoError(t, err)
			require.NoError(t, ts.store.Set(entry2, false, 100))

			req := &pbservice.GetRequest{
				BlockNumber: 200,
				Keys: []*pbmodel.Key{
					{Bytes: []byte("k1")},
					{Bytes: []byte("missing")},
					{Bytes: []byte("k2")},
				},
			}

			resp, err := ts.store.GetFirst(req)
			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.Equal(t, true, resp.BlockReached)
			require.Equal(t, 3, len(resp.Entries.Entries))

			// k1
			assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_FOUND, resp.Entries.Entries[0].Code)
			got1 := &pbtest.TestAccountOwner{}
			require.NoError(t, resp.Entries.Entries[0].Entry.Value.UnmarshalTo(got1))
			assert.Equal(t, owner1.Owner, got1.Owner)

			// missing
			assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND, resp.Entries.Entries[1].Code)

			// k2
			assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_FOUND, resp.Entries.Entries[2].Code)
			got2 := &pbtest.TestAccountOwner{}
			require.NoError(t, resp.Entries.Entries[2].Entry.Value.UnmarshalTo(got2))
			assert.Equal(t, owner2.Owner, got2.Owner)
		})
	}
}
