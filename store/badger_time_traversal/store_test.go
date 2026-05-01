package badger_time_traversal

import (
	"encoding/binary"
	"math"
	"os"
	"testing"

	pbtest "github.com/streamingfast/substreams-foundational-store/internal/pb/test"
	pbmodel "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/model/v2"
	pbservice "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/service/v2"
	storelib "github.com/streamingfast/substreams-foundational-store/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// testStore represents a test badger time traversal store with cleanup function
type testStore struct {
	store   *Store
	typeURL string
	tempDir string
	cleanup func()
}

// setupTestStore creates a new badger time traversal store for testing
func setupTestStore(t *testing.T) *testStore {
	// Create a temporary directory for the badger DB
	tempDir, err := os.MkdirTemp("", "badger-time-traversal-test")
	require.NoError(t, err)

	// Create a DSN for the badger store
	dsn, err := storelib.ParseDSN("badger://" + tempDir)
	require.NoError(t, err)

	// Create a new badger time traversal store
	typeURL := "type.googleapis.com/test.TestAccountOwner"
	badgerStore, err := NewStore(dsn, typeURL, 10, nil)
	require.NoError(t, err)

	cleanup := func() {
		badgerStore.Close()
		os.RemoveAll(tempDir)
	}

	return &testStore{
		store:   badgerStore,
		typeURL: typeURL,
		tempDir: tempDir,
		cleanup: cleanup,
	}
}

// createAccountOwner creates an AccountOwner with the given owner address
func createAccountOwner(ownerAddress string) *pbtest.TestAccountOwner {
	return &pbtest.TestAccountOwner{
		Mint:  []byte("mint-address"),
		Owner: []byte(ownerAddress),
	}
}

// createEntry creates a store Entry with the given block number, key, and AccountOwner
func createEntry(blockNumber uint64, key []byte, accountOwner *pbtest.TestAccountOwner, typeURL string) (*pbmodel.Entry, error) {
	// Marshal the AccountOwner proto message
	data, err := proto.Marshal(accountOwner)
	if err != nil {
		return nil, err
	}

	// Create an Any proto message to wrap the AccountOwner
	anyValue := &anypb.Any{
		TypeUrl: typeURL,
		Value:   data,
	}

	// Create an Entry to store
	return &pbmodel.Entry{
		Key:   &pbmodel.Key{Bytes: key},
		Value: anyValue,
	}, nil
}

func TestMakeTimeTraversalKey(t *testing.T) {
	testCases := []struct {
		name        string
		originalKey []byte
		blockNumber uint64
		expectedLen int
	}{
		{
			name:        "Simple key",
			originalKey: []byte("test-key"),
			blockNumber: 123,
			expectedLen: len("test-key") + 8,
		},
		{
			name:        "Empty key",
			originalKey: []byte{},
			blockNumber: 456,
			expectedLen: 8,
		},
		{
			name:        "Long key",
			originalKey: []byte("this-is-a-very-long-key-for-testing-purposes"),
			blockNumber: 789,
			expectedLen: len("this-is-a-very-long-key-for-testing-purposes") + 8,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			compositeKey := makeTimeTraversalKey(tc.originalKey, tc.blockNumber)

			// Check length
			assert.Equal(t, tc.expectedLen, len(compositeKey))

			// Check original key portion
			assert.Equal(t, tc.originalKey, compositeKey[:len(tc.originalKey)])

			// Check block number portion (should be reversed)
			reversedBlockNumber := binary.BigEndian.Uint64(compositeKey[len(tc.originalKey):])
			expectedReversedBlockNumber := math.MaxUint64 - tc.blockNumber
			assert.Equal(t, expectedReversedBlockNumber, reversedBlockNumber)
		})
	}
}

func TestStoreAndRetrieveAccountOwner(t *testing.T) {
	testCases := []struct {
		name         string
		blockNumber  uint64
		key          []byte
		ownerValue   string
		requestBlock uint64
		expectFound  bool
	}{
		{
			name:         "Store and retrieve at same block",
			blockNumber:  123,
			key:          []byte("test-account-key"),
			ownerValue:   "owner-address",
			requestBlock: 123,
			expectFound:  true,
		},
		{
			name:         "Store at block 100, retrieve at block 50",
			blockNumber:  100,
			key:          []byte("test-account-key-2"),
			ownerValue:   "owner-address-2",
			requestBlock: 50,
			expectFound:  false,
		},
		{
			name:         "Store at block 100, retrieve at block 150",
			blockNumber:  100,
			key:          []byte("test-account-key-3"),
			ownerValue:   "owner-address-3",
			requestBlock: 150,
			expectFound:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup test store
			ts := setupTestStore(t)
			defer ts.cleanup()

			// Create an AccountOwner object
			accountOwner := createAccountOwner(tc.ownerValue)

			// Create and store the entry
			entry, err := createEntry(tc.blockNumber, tc.key, accountOwner, ts.typeURL)
			require.NoError(t, err)

			err = ts.store.Set(entry, false, tc.blockNumber)
			require.NoError(t, err)

			// Create a GetRequest to retrieve the Entry
			getRequest := &pbservice.GetRequest{
				BlockNumber: tc.requestBlock,
				BlockHash:   []byte("test_block_hash"),
				Keys:        []*pbmodel.Key{{Bytes: tc.key}},
			}

			// Retrieve the Entry
			getResponse, err := ts.store.Get(getRequest)
			require.NoError(t, err)

			if tc.expectFound {
				assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_FOUND, getResponse.Entries.Entries[0].Code)

				// Unmarshal the retrieved value into an AccountOwner
				retrievedAccountOwner := &pbtest.TestAccountOwner{}
				err = getResponse.Entries.Entries[0].Entry.Value.UnmarshalTo(retrievedAccountOwner)
				require.NoError(t, err)

				// Verify the retrieved AccountOwner matches the original
				assert.Equal(t, accountOwner.Mint, retrievedAccountOwner.Mint)
				assert.Equal(t, accountOwner.Owner, retrievedAccountOwner.Owner)
			} else {
				assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND, getResponse.Entries.Entries[0].Code)
			}
		})
	}
}

func TestTimeTraversalWithMultipleVersions(t *testing.T) {
	// Setup test store
	ts := setupTestStore(t)
	defer ts.cleanup()

	key := []byte("versioned-key")

	// Store multiple versions of the same key at different blocks
	versions := []struct {
		blockNumber uint64
		ownerValue  string
	}{
		{100, "owner-v2"},
		{200, "owner-v2"},
		{300, "owner-v3"},
	}

	for _, version := range versions {
		accountOwner := createAccountOwner(version.ownerValue)
		entry, err := createEntry(version.blockNumber, key, accountOwner, ts.typeURL)
		require.NoError(t, err)

		err = ts.store.Set(entry, false, version.blockNumber)
		require.NoError(t, err)
	}

	// Test retrieving at different block numbers
	testCases := []struct {
		name          string
		requestBlock  uint64
		expectFound   bool
		expectedOwner string
	}{
		{
			name:         "Retrieve at block 50 (before all versions)",
			requestBlock: 50,
			expectFound:  false,
		},
		{
			name:          "Retrieve at block 100 (exact match v2)",
			requestBlock:  100,
			expectFound:   true,
			expectedOwner: "owner-v2",
		},
		{
			name:          "Retrieve at block 150 (should get v2)",
			requestBlock:  150,
			expectFound:   true,
			expectedOwner: "owner-v2",
		},
		{
			name:          "Retrieve at block 200 (exact match v2)",
			requestBlock:  200,
			expectFound:   true,
			expectedOwner: "owner-v2",
		},
		{
			name:          "Retrieve at block 250 (should get v2)",
			requestBlock:  250,
			expectFound:   true,
			expectedOwner: "owner-v2",
		},
		{
			name:          "Retrieve at block 300 (exact match v3)",
			requestBlock:  300,
			expectFound:   true,
			expectedOwner: "owner-v3",
		},
		{
			name:          "Retrieve at block 400 (should get v3)",
			requestBlock:  400,
			expectFound:   true,
			expectedOwner: "owner-v3",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			getRequest := &pbservice.GetRequest{
				BlockNumber: tc.requestBlock,
				BlockHash:   []byte("test_block_hash"),
				Keys:        []*pbmodel.Key{{Bytes: key}},
			}

			getResponse, err := ts.store.Get(getRequest)
			require.NoError(t, err)

			if tc.expectFound {
				assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_FOUND, getResponse.Entries.Entries[0].Code)

				retrievedAccountOwner := &pbtest.TestAccountOwner{}
				err = getResponse.Entries.Entries[0].Entry.Value.UnmarshalTo(retrievedAccountOwner)
				require.NoError(t, err)

				assert.Equal(t, []byte(tc.expectedOwner), retrievedAccountOwner.Owner)
			} else {
				assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND, getResponse.Entries.Entries[0].Code)
			}
		})
	}
}

func TestSetAllAndGetAll(t *testing.T) {
	// Setup test store
	ts := setupTestStore(t)
	defer ts.cleanup()

	blockNumber := uint64(150)

	// Create multiple entries
	entries := []*pbmodel.Entry{}
	expectedOwners := []string{}

	for i := 0; i < 3; i++ {
		key := []byte("test-key-" + string(rune('a'+i)))
		ownerValue := "owner-" + string(rune('a'+i))
		expectedOwners = append(expectedOwners, ownerValue)

		accountOwner := createAccountOwner(ownerValue)
		entry, err := createEntry(blockNumber, key, accountOwner, ts.typeURL)
		require.NoError(t, err)

		entries = append(entries, entry)
	}

	// Store all entries
	err := ts.store.SetAll(entries, nil, false, blockNumber)
	require.NoError(t, err)

	// Retrieve all entries
	keys := []*pbmodel.Key{}
	for _, entry := range entries {
		keys = append(keys, entry.Key)
	}

	getRequest := &pbservice.GetRequest{
		BlockNumber: blockNumber,
		BlockHash:   []byte("test_block_hash"),
		Keys:        keys,
	}

	getResponse, err := ts.store.Get(getRequest)
	require.NoError(t, err)

	// Verify all entries were retrieved
	assert.Equal(t, len(entries), len(getResponse.Entries.Entries))

	// Verify each entry
	for i, queriedEntry := range getResponse.Entries.Entries {
		assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_FOUND, queriedEntry.Code)
		assert.Equal(t, entries[i].Key, queriedEntry.Entry.Key)

		retrievedAccountOwner := &pbtest.TestAccountOwner{}
		err = queriedEntry.Entry.Value.UnmarshalTo(retrievedAccountOwner)
		require.NoError(t, err)

		assert.Equal(t, []byte(expectedOwners[i]), retrievedAccountOwner.Owner)
	}
}

func TestSetAllAndGetAllWithDifferentBlocks(t *testing.T) {
	// Setup test store
	ts := setupTestStore(t)
	defer ts.cleanup()

	// Store entries at different blocks
	key1 := []byte("key1")
	key2 := []byte("key2")
	key3 := []byte("key3")

	// Store key1 at block 100
	accountOwner1 := createAccountOwner("owner1")
	entry1, err := createEntry(100, key1, accountOwner1, ts.typeURL)
	require.NoError(t, err)
	err = ts.store.Set(entry1, false, 100)
	require.NoError(t, err)

	// Store key2 at block 200
	accountOwner2 := createAccountOwner("owner2")
	entry2, err := createEntry(200, key2, accountOwner2, ts.typeURL)
	require.NoError(t, err)
	err = ts.store.Set(entry2, false, 200)
	require.NoError(t, err)

	// Store key3 at block 300
	accountOwner3 := createAccountOwner("owner3")
	entry3, err := createEntry(300, key3, accountOwner3, ts.typeURL)
	require.NoError(t, err)
	err = ts.store.Set(entry3, false, 300)
	require.NoError(t, err)

	// Test Get at block 150 (should find key1 only)
	getRequest := &pbservice.GetRequest{
		BlockNumber: 150,
		BlockHash:   []byte("test_block_hash"),
		Keys:        []*pbmodel.Key{&pbmodel.Key{Bytes: key1}, &pbmodel.Key{Bytes: key2}, &pbmodel.Key{Bytes: key3}},
	}

	getResponse, err := ts.store.Get(getRequest)
	require.NoError(t, err)

	// Verify responses
	assert.Equal(t, 3, len(getResponse.Entries.Entries))

	// Find entries by key for verification
	responseMap := make(map[string]*pbmodel.QueriedEntry)
	for _, queriedEntry := range getResponse.Entries.Entries {
		responseMap[string(queriedEntry.Entry.Key.Bytes)] = queriedEntry
	}

	// key1 should be found (stored at 100, requesting at 150)
	assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_FOUND, responseMap[string(key1)].Code)

	// key2 should not be found (stored at 200, requesting at 150)
	assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND, responseMap[string(key2)].Code)

	// key3 should not be found (stored at 300, requesting at 150)
	assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND, responseMap[string(key3)].Code)
}

func TestEmptySetAll(t *testing.T) {
	// Setup test store
	ts := setupTestStore(t)
	defer ts.cleanup()

	// Test SetAll with empty slice
	err := ts.store.SetAll([]*pbmodel.Entry{}, nil, false, 100)
	require.NoError(t, err)
}

func TestGetNonExistentKey(t *testing.T) {
	// Setup test store
	ts := setupTestStore(t)
	defer ts.cleanup()

	// Try to retrieve a key that was never stored
	getRequest := &pbservice.GetRequest{
		BlockNumber: 100,
		BlockHash:   []byte("test_block_hash"),
		Keys:        []*pbmodel.Key{{Bytes: []byte("non-existent-key")}},
	}

	getResponse, err := ts.store.Get(getRequest)
	require.NoError(t, err)

	assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND, getResponse.Entries.Entries[0].Code)
}

func TestGetFirstReturnsOldestVersionForKey(t *testing.T) {
	// Setup test store
	ts := setupTestStore(t)
	defer ts.cleanup()

	key := []byte("k1")
	versions := []struct {
		block uint64
		owner string
	}{
		{100, "v2"},
		{200, "v2"},
		{300, "v3"},
	}

	for _, v := range versions {
		msg := createAccountOwner(v.owner)
		entry, err := createEntry(v.block, key, msg, ts.typeURL)
		require.NoError(t, err)
		require.NoError(t, ts.store.Set(entry, false, v.block))
	}

	// Add another key to ensure iterator ordering across different keys still works
	other := createAccountOwner("other")
	otherEntry, err := createEntry(150, []byte("k2"), other, ts.typeURL)
	require.NoError(t, err)
	require.NoError(t, ts.store.Set(otherEntry, false, 150))

	resp, err := ts.store.GetFirst(&pbservice.GetRequest{Keys: []*pbmodel.Key{{Bytes: []byte("k1")}}, BlockNumber: 200, BlockHash: []byte("test_block_hash")})
	require.NoError(t, err)
	assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_FOUND, resp.Entries.Entries[0].Code)
	got := &pbtest.TestAccountOwner{}
	require.NoError(t, resp.Entries.Entries[0].Entry.Value.UnmarshalTo(got))
	// Oldest version (lowest block) must be returned for the key
	assert.Equal(t, []byte("v2"), got.Owner)
}

func TestIfNotExist(t *testing.T) {
	// Setup test store
	ts := setupTestStore(t)
	defer ts.cleanup()

	key := []byte("test-key")
	value1 := "value1"
	value2 := "value2"

	// Create first entry
	entry1, err := createEntry(100, key, createAccountOwner(value1), ts.typeURL)
	require.NoError(t, err)

	// Set first entry normally
	err = ts.store.Set(entry1, false, 100)
	require.NoError(t, err)

	// Create second entry with same key but different value
	entry2, err := createEntry(200, key, createAccountOwner(value2), ts.typeURL)
	require.NoError(t, err)

	// Try to set second entry with IfNotExist=true, should skip
	err = ts.store.Set(entry2, true, 200)
	require.NoError(t, err)

	// Retrieve and check that value is still the first one
	resp, err := ts.store.Get(&pbservice.GetRequest{Keys: []*pbmodel.Key{{Bytes: key}}, BlockNumber: 300})
	require.NoError(t, err)
	require.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_FOUND, resp.Entries.Entries[0].Code)
	retrievedAccountOwner := &pbtest.TestAccountOwner{}
	err = resp.Entries.Entries[0].Entry.Value.UnmarshalTo(retrievedAccountOwner)
	require.NoError(t, err)
	assert.Equal(t, value1, string(retrievedAccountOwner.Owner))

	// Test SetAll with IfNotExist
	key2 := []byte("test-key2")
	entry3, err := createEntry(100, key2, createAccountOwner("value3"), ts.typeURL)
	require.NoError(t, err)
	entry4, err := createEntry(200, key2, createAccountOwner("value4"), ts.typeURL)
	require.NoError(t, err)

	// Set entry3 normally
	err = ts.store.SetAll([]*pbmodel.Entry{entry3}, nil, false, 100)
	require.NoError(t, err)

	// Try to set entry4 with IfNotExist=true, should skip
	err = ts.store.SetAll([]*pbmodel.Entry{entry4}, nil, true, 200)
	require.NoError(t, err)

	// Check value is still value3
	resp, err = ts.store.Get(&pbservice.GetRequest{Keys: []*pbmodel.Key{{Bytes: key2}}, BlockNumber: 300})
	require.NoError(t, err)
	require.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_FOUND, resp.Entries.Entries[0].Code)
	retrievedAccountOwner = &pbtest.TestAccountOwner{}
	err = resp.Entries.Entries[0].Entry.Value.UnmarshalTo(retrievedAccountOwner)
	require.NoError(t, err)
	assert.Equal(t, "value3", string(retrievedAccountOwner.Owner))
}
