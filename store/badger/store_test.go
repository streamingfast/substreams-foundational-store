package badger

import (
	"os"
	"testing"

	pbtest "github.com/streamingfast/substreams-foundational-store/internal/pb/test"
	pbstore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	storelib "github.com/streamingfast/substreams-foundational-store/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// testStore represents a test badger store with cleanup function
type testStore struct {
	store   *Store
	typeURL string
	tempDir string
	cleanup func()
}

// setupTestStore creates a new badger foundational-store for testing
func setupTestStore(t *testing.T) *testStore {
	// Create a temporary directory for the badger DB
	tempDir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)

	// Create a DSN for the badger foundational-store
	dsn, err := storelib.ParseDSN("badger://" + tempDir)
	require.NoError(t, err)

	// Create a new badger foundational-store
	typeURL := "type.googleapis.com/test.TestAccountOwner"
	badgerStore, err := NewStore(dsn, typeURL)
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

// createEntry creates a foundational-store Entry with the given block number, key, and AccountOwner
func createEntry(blockNumber uint64, key []byte, accountOwner *pbtest.TestAccountOwner, typeURL string) (*pbstore.Entry, error) {
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

	// Create an Entry to foundational-store
	return &pbstore.Entry{
		Key:   key,
		Value: anyValue,
	}, nil
}

func TestStoreAndRetrieveAccountOwner(t *testing.T) {
	// Define test cases
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
			// Setup test foundational-store
			ts := setupTestStore(t)
			defer ts.cleanup()

			// Create an AccountOwner object
			accountOwner := createAccountOwner(tc.ownerValue)

			// Create and foundational-store the entry
			entry, err := createEntry(tc.blockNumber, tc.key, accountOwner, ts.typeURL)
			require.NoError(t, err)

			err = ts.store.Set(entry, tc.blockNumber)
			require.NoError(t, err)

			// Create a GetRequest to retrieve the Entry
			getRequest := &pbstore.GetRequest{
				BlockNumber: tc.requestBlock,
				BlockHash:   []byte("test_block_hash"),
				Key:         tc.key,
			}

			// Retrieve the Entry
			getResponse, err := ts.store.Get(getRequest)
			require.NoError(t, err)

			if tc.expectFound {
				assert.Equal(t, pbstore.ResponseCode_RESPONSE_CODE_FOUND, getResponse.Code)

				// Unmarshal the retrieved value into an AccountOwner
				retrievedAccountOwner := &pbtest.TestAccountOwner{}
				err = getResponse.Value.UnmarshalTo(retrievedAccountOwner)
				require.NoError(t, err)

				// Verify the retrieved AccountOwner matches the original
				assert.Equal(t, accountOwner.Mint, retrievedAccountOwner.Mint)
				assert.Equal(t, accountOwner.Owner, retrievedAccountOwner.Owner)
			} else {
				assert.Equal(t, pbstore.ResponseCode_RESPONSE_CODE_NOT_FOUND, getResponse.Code)
			}
		})
	}
}

func TestGetWithBlockNumber(t *testing.T) {
	// Define test cases
	type testSetup struct {
		blockNumber uint64
		key         []byte
		ownerValue  string
	}

	type testCase struct {
		name          string
		setup         []testSetup
		requestBlock  uint64
		requestKey    []byte
		expectFound   bool
		expectedOwner string
	}

	testCases := []testCase{
		{
			name: "Get with block number less than any stored block",
			setup: []testSetup{
				{
					blockNumber: 100,
					key:         []byte("test-account-key-1"),
					ownerValue:  "owner-address-1",
				},
			},
			requestBlock: 50,
			requestKey:   []byte("test-account-key-1"),
			expectFound:  false,
		},
		{
			name: "Get with exact block number",
			setup: []testSetup{
				{
					blockNumber: 100,
					key:         []byte("test-account-key-1"),
					ownerValue:  "owner-address-1",
				},
			},
			requestBlock:  100,
			requestKey:    []byte("test-account-key-1"),
			expectFound:   true,
			expectedOwner: "owner-address-1",
		},
		{
			name: "Get with block number between stored blocks",
			setup: []testSetup{
				{
					blockNumber: 100,
					key:         []byte("test-account-key-1"),
					ownerValue:  "owner-address-1",
				},
				{
					blockNumber: 200,
					key:         []byte("test-account-key-2"),
					ownerValue:  "owner-address-2",
				},
			},
			requestBlock:  150,
			requestKey:    []byte("test-account-key-1"),
			expectFound:   true,
			expectedOwner: "owner-address-1",
		},
		{
			name: "Get with exact block number for second entry",
			setup: []testSetup{
				{
					blockNumber: 100,
					key:         []byte("test-account-key-1"),
					ownerValue:  "owner-address-1",
				},
				{
					blockNumber: 200,
					key:         []byte("test-account-key-2"),
					ownerValue:  "owner-address-2",
				},
			},
			requestBlock:  200,
			requestKey:    []byte("test-account-key-2"),
			expectFound:   true,
			expectedOwner: "owner-address-2",
		},
		{
			name: "Get with block number greater than any stored block",
			setup: []testSetup{
				{
					blockNumber: 100,
					key:         []byte("test-account-key-1"),
					ownerValue:  "owner-address-1",
				},
				{
					blockNumber: 200,
					key:         []byte("test-account-key-2"),
					ownerValue:  "owner-address-2",
				},
			},
			requestBlock:  300,
			requestKey:    []byte("test-account-key-2"),
			expectFound:   true,
			expectedOwner: "owner-address-2",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup test foundational-store
			ts := setupTestStore(t)
			defer ts.cleanup()

			// Store all entries in the setup
			for _, setup := range tc.setup {
				accountOwner := createAccountOwner(setup.ownerValue)
				entry, err := createEntry(setup.blockNumber, setup.key, accountOwner, ts.typeURL)
				require.NoError(t, err)

				err = ts.store.Set(entry, setup.blockNumber)
				require.NoError(t, err)
			}

			// Create a GetRequest to retrieve the Entry
			getRequest := &pbstore.GetRequest{
				BlockNumber: tc.requestBlock,
				BlockHash:   []byte("test_block_hash"),
				Key:         tc.requestKey,
			}

			// Retrieve the Entry
			getResponse, err := ts.store.Get(getRequest)
			require.NoError(t, err)

			if tc.expectFound {
				assert.Equal(t, pbstore.ResponseCode_RESPONSE_CODE_FOUND, getResponse.Code,
					"Should find entry with block number %d", tc.requestBlock)

				// Unmarshal the retrieved value into an AccountOwner
				retrievedAccountOwner := &pbtest.TestAccountOwner{}
				err = getResponse.Value.UnmarshalTo(retrievedAccountOwner)
				require.NoError(t, err)

				// Verify the retrieved AccountOwner matches the expected one
				expectedOwner := []byte(tc.expectedOwner)
				assert.Equal(t, expectedOwner, retrievedAccountOwner.Owner,
					"Should retrieve %s", tc.expectedOwner)
			} else {
				assert.Equal(t, pbstore.ResponseCode_RESPONSE_CODE_NOT_FOUND, getResponse.Code,
					"Should not find entry with block number %d", tc.requestBlock)
			}
		})
	}
}

func TestSetAllAndGetAll(t *testing.T) {
	// Define test cases
	type accountOwnerData struct {
		key        string
		ownerValue string
	}

	type testCase struct {
		name         string
		blockNumber  uint64
		accounts     []accountOwnerData
		requestBlock uint64
		expectedKeys map[string]bool
	}

	testCases := []testCase{
		{
			name:        "Store and retrieve multiple entries at same block",
			blockNumber: 123,
			accounts: []accountOwnerData{
				{key: "test-account-key-0", ownerValue: "owner-address-1"},
				{key: "test-account-key-1", ownerValue: "owner-address-2"},
				{key: "test-account-key-2", ownerValue: "owner-address-3"},
			},
			requestBlock: 123,
			expectedKeys: map[string]bool{
				"test-account-key-0": true,
				"test-account-key-1": true,
				"test-account-key-2": true,
			},
		},
		{
			name:        "Store at block 100, retrieve at block 150",
			blockNumber: 100,
			accounts: []accountOwnerData{
				{key: "test-account-key-0", ownerValue: "owner-address-1"},
				{key: "test-account-key-1", ownerValue: "owner-address-2"},
			},
			requestBlock: 150,
			expectedKeys: map[string]bool{
				"test-account-key-0": true,
				"test-account-key-1": true,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup test foundational-store
			ts := setupTestStore(t)
			defer ts.cleanup()

			// Create entries to foundational-store
			entries := make([]*pbstore.Entry, len(tc.accounts))
			keys := make([][]byte, len(tc.accounts))
			accountOwners := make([]*pbtest.TestAccountOwner, len(tc.accounts))

			for i, acc := range tc.accounts {
				// Create an AccountOwner object
				accountOwner := createAccountOwner(acc.ownerValue)
				accountOwners[i] = accountOwner

				// Create a key for the AccountOwner
				key := []byte(acc.key)
				keys[i] = key

				// Create an Entry
				entry, err := createEntry(tc.blockNumber, key, accountOwner, ts.typeURL)
				require.NoError(t, err)
				entries[i] = entry
			}

			// Store all entries using SetAll
			err := ts.store.SetAll(entries, tc.blockNumber)
			require.NoError(t, err)

			// Create a GetAllRequest to retrieve all entries
			getAllRequest := &pbstore.GetAllRequest{
				BlockNumber: tc.requestBlock,
				BlockHash:   []byte("test_block_hash"),
				Keys:        keys,
			}

			// Retrieve all entries using GetAll
			getAllResponse, err := ts.store.GetAll(getAllRequest)
			require.NoError(t, err)

			// Verify the number of retrieved entries matches the number of requested keys
			assert.Equal(t, len(keys), len(getAllResponse.Entries), "Should return responses for all keys")

			// Create a map of keys to entries for easier verification
			entryMap := make(map[string]*pbtest.TestAccountOwner)
			for i, entry := range entries {
				entryMap[string(entry.Key)] = accountOwners[i]
			}

			// Create a map to foundational-store the responses by key
			responseMap := make(map[string]*pbstore.ResponseEntry)
			foundCount := 0
			for _, responseEntry := range getAllResponse.Entries {
				responseMap[string(responseEntry.Key)] = responseEntry
				if responseEntry.Response.Code == pbstore.ResponseCode_RESPONSE_CODE_FOUND {
					foundCount++
				}
			}

			// Verify each key
			for _, key := range keys {
				responseEntry := responseMap[string(key)]
				require.NotNil(t, responseEntry, "Response entry not found for key %s", key)

				// Check if the key is expected to be found
				expectedFound := tc.expectedKeys[string(key)]
				if expectedFound {
					assert.Equal(t, pbstore.ResponseCode_RESPONSE_CODE_FOUND, responseEntry.Response.Code,
						"Should find entry with block number %d for key %s", tc.requestBlock, key)

					// Unmarshal the retrieved value into an AccountOwner
					retrievedAccountOwner := &pbtest.TestAccountOwner{}
					err = responseEntry.Response.Value.UnmarshalTo(retrievedAccountOwner)
					require.NoError(t, err)

					// Get the original AccountOwner for this key
					originalAccountOwner := entryMap[string(key)]
					require.NotNil(t, originalAccountOwner, "Original AccountOwner not found for key %s", key)

					// Verify the retrieved AccountOwner matches the original
					assert.Equal(t, originalAccountOwner.Mint, retrievedAccountOwner.Mint)
					assert.Equal(t, originalAccountOwner.Owner, retrievedAccountOwner.Owner)
				} else {
					assert.Equal(t, pbstore.ResponseCode_RESPONSE_CODE_NOT_FOUND, responseEntry.Response.Code,
						"Should not find entry with block number %d for key %s", tc.requestBlock, key)
				}
			}
		})
	}
}

func TestGetAllWithBlockNumber(t *testing.T) {
	// Define test cases
	type entryData struct {
		blockNumber uint64
		key         string
		ownerValue  string
	}

	type testCase struct {
		name         string
		entries      []entryData
		requestBlock uint64
		expectedKeys map[string]bool
		keys         []string // Define keys directly in the test case
	}

	testCases := []testCase{
		{
			name: "GetAll with block number less than any stored block",
			entries: []entryData{
				{
					blockNumber: 100,
					key:         "test-account-key-1",
					ownerValue:  "owner-address-1-block-100",
				},
				{
					blockNumber: 100,
					key:         "test-account-key-2",
					ownerValue:  "owner-address-2-block-100",
				},
				{
					blockNumber: 200,
					key:         "test-account-key-3",
					ownerValue:  "owner-address-1-block-200",
				},
				{
					blockNumber: 200,
					key:         "test-account-key-4",
					ownerValue:  "owner-address-2-block-200",
				},
			},
			requestBlock: 50,
			expectedKeys: map[string]bool{
				"test-account-key-1": false,
				"test-account-key-2": false,
			},
			keys: []string{"test-account-key-1", "test-account-key-2"},
		},
		{
			name: "GetAll with exact block number (100)",
			entries: []entryData{
				{
					blockNumber: 100,
					key:         "test-account-key-1",
					ownerValue:  "owner-address-1-block-100",
				},
				{
					blockNumber: 100,
					key:         "test-account-key-2",
					ownerValue:  "owner-address-2-block-100",
				},
				{
					blockNumber: 200,
					key:         "test-account-key-3",
					ownerValue:  "owner-address-1-block-200",
				},
				{
					blockNumber: 200,
					key:         "test-account-key-4",
					ownerValue:  "owner-address-2-block-200",
				},
			},
			requestBlock: 100,
			expectedKeys: map[string]bool{
				"test-account-key-1": true,
				"test-account-key-2": true,
			},
			keys: []string{"test-account-key-1", "test-account-key-2"},
		},
		{
			name: "GetAll with block number between stored blocks",
			entries: []entryData{
				{
					blockNumber: 100,
					key:         "test-account-key-1",
					ownerValue:  "owner-address-1-block-100",
				},
				{
					blockNumber: 100,
					key:         "test-account-key-2",
					ownerValue:  "owner-address-2-block-100",
				},
				{
					blockNumber: 200,
					key:         "test-account-key-3",
					ownerValue:  "owner-address-1-block-200",
				},
				{
					blockNumber: 200,
					key:         "test-account-key-4",
					ownerValue:  "owner-address-2-block-200",
				},
			},
			requestBlock: 150,
			expectedKeys: map[string]bool{
				"test-account-key-1": true,
				"test-account-key-2": true,
			},
			keys: []string{"test-account-key-1", "test-account-key-2"},
		},
		{
			name: "GetAll with exact block number (200)",
			entries: []entryData{
				{
					blockNumber: 100,
					key:         "test-account-key-1",
					ownerValue:  "owner-address-1-block-100",
				},
				{
					blockNumber: 100,
					key:         "test-account-key-2",
					ownerValue:  "owner-address-2-block-100",
				},
				{
					blockNumber: 200,
					key:         "test-account-key-3",
					ownerValue:  "owner-address-1-block-200",
				},
				{
					blockNumber: 200,
					key:         "test-account-key-4",
					ownerValue:  "owner-address-2-block-200",
				},
			},
			requestBlock: 200,
			expectedKeys: map[string]bool{
				"test-account-key-1": true,
				"test-account-key-2": true,
			},
			keys: []string{"test-account-key-1", "test-account-key-2"},
		},
		{
			name: "GetAll with block number greater than any stored block",
			entries: []entryData{
				{
					blockNumber: 100,
					key:         "test-account-key-1",
					ownerValue:  "owner-address-1-block-100",
				},
				{
					blockNumber: 100,
					key:         "test-account-key-2",
					ownerValue:  "owner-address-2-block-100",
				},
				{
					blockNumber: 200,
					key:         "test-account-key-3",
					ownerValue:  "owner-address-1-block-200",
				},
				{
					blockNumber: 200,
					key:         "test-account-key-4",
					ownerValue:  "owner-address-2-block-200",
				},
			},
			requestBlock: 300,
			expectedKeys: map[string]bool{
				"test-account-key-1": true,
				"test-account-key-2": true,
			},
			keys: []string{"test-account-key-1", "test-account-key-2"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup test foundational-store
			ts := setupTestStore(t)
			defer ts.cleanup()

			// Convert keys to byte slices
			byteKeys := make([][]byte, len(tc.keys))
			for i, key := range tc.keys {
				byteKeys[i] = []byte(key)
			}

			// Group entries by block number
			entriesByBlock := make(map[uint64][]*pbstore.Entry)
			for _, entryData := range tc.entries {
				// Create an AccountOwner object
				accountOwner := createAccountOwner(entryData.ownerValue)

				// Create an Entry
				entry, err := createEntry(entryData.blockNumber, []byte(entryData.key), accountOwner, ts.typeURL)
				require.NoError(t, err)

				entriesByBlock[entryData.blockNumber] = append(entriesByBlock[entryData.blockNumber], entry)
			}

			// Store entries for each block
			for blockNumber, entries := range entriesByBlock {
				err := ts.store.SetAll(entries, blockNumber)
				require.NoError(t, err)
			}

			// Create a GetAllRequest to retrieve all entries
			getAllRequest := &pbstore.GetAllRequest{
				BlockNumber: tc.requestBlock,
				BlockHash:   []byte("test_block_hash"),
				Keys:        byteKeys,
			}

			// Retrieve all entries using GetAll
			getAllResponse, err := ts.store.GetAll(getAllRequest)
			require.NoError(t, err)
			assert.Equal(t, len(byteKeys), len(getAllResponse.Entries), "Should return responses for all keys")

			// Create a map to foundational-store the responses by key
			responseMap := make(map[string]*pbstore.ResponseEntry)
			foundCount := 0
			for _, responseEntry := range getAllResponse.Entries {
				responseMap[string(responseEntry.Key)] = responseEntry
				if responseEntry.Response.Code == pbstore.ResponseCode_RESPONSE_CODE_FOUND {
					foundCount++
				}
			}

			// For the test case "GetAll with exact block number (100)", verify that we have 2 entries
			if tc.name == "GetAll with exact block number (100)" {
				assert.Equal(t, 2, foundCount, "Should find exactly 2 entries for block number 100")
			}

			// Verify each entry
			for _, key := range byteKeys {
				responseEntry := responseMap[string(key)]
				require.NotNil(t, responseEntry, "Response entry not found for key %s", key)

				// Check if the key is expected to be found
				expectedFound := tc.expectedKeys[string(key)]
				if expectedFound {
					assert.Equal(t, pbstore.ResponseCode_RESPONSE_CODE_FOUND, responseEntry.Response.Code,
						"Should find entry with block number %d for key %s", tc.requestBlock, key)

					// Unmarshal the retrieved value into an AccountOwner
					retrievedAccountOwner := &pbtest.TestAccountOwner{}
					err = responseEntry.Response.Value.UnmarshalTo(retrievedAccountOwner)
					require.NoError(t, err)

					// For this test, we don't need to verify the specific owner value
					// as we're just testing if the keys are found or not
				} else {
					assert.Equal(t, pbstore.ResponseCode_RESPONSE_CODE_NOT_FOUND, responseEntry.Response.Code,
						"Should not find entry with block number %d for key %s", tc.requestBlock, key)
				}
			}
		})
	}
}

func TestSetAllAndGetAllWithNonExistentKey(t *testing.T) {
	// Define test cases
	type accountOwnerData struct {
		key        string
		ownerValue string
	}

	type testCase struct {
		name            string
		blockNumber     uint64
		accounts        []accountOwnerData
		requestBlock    uint64
		nonExistentKeys []string
	}

	testCases := []testCase{
		{
			name:        "GetAll with one non-existent key",
			blockNumber: 123,
			accounts: []accountOwnerData{
				{key: "test-account-key-0", ownerValue: "owner-address-1"},
				{key: "test-account-key-1", ownerValue: "owner-address-2"},
				{key: "test-account-key-2", ownerValue: "owner-address-3"},
			},
			requestBlock:    123,
			nonExistentKeys: []string{"non-existent-key"},
		},
		{
			name:        "GetAll with multiple non-existent keys",
			blockNumber: 123,
			accounts: []accountOwnerData{
				{key: "test-account-key-0", ownerValue: "owner-address-1"},
				{key: "test-account-key-1", ownerValue: "owner-address-2"},
			},
			requestBlock:    123,
			nonExistentKeys: []string{"non-existent-key-1", "non-existent-key-2"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup test foundational-store
			ts := setupTestStore(t)
			defer ts.cleanup()

			// Create entries to foundational-store
			entries := make([]*pbstore.Entry, len(tc.accounts))
			existingKeys := make([][]byte, len(tc.accounts))
			accountOwners := make([]*pbtest.TestAccountOwner, len(tc.accounts))

			for i, acc := range tc.accounts {
				// Create an AccountOwner object
				accountOwner := createAccountOwner(acc.ownerValue)
				accountOwners[i] = accountOwner

				// Create a key for the AccountOwner
				key := []byte(acc.key)
				existingKeys[i] = key

				// Create an Entry
				entry, err := createEntry(tc.blockNumber, key, accountOwner, ts.typeURL)
				require.NoError(t, err)
				entries[i] = entry
			}

			// Store all entries using SetAll
			err := ts.store.SetAll(entries, tc.blockNumber)
			require.NoError(t, err)

			// Create non-existent keys
			nonExistentByteKeys := make([][]byte, len(tc.nonExistentKeys))
			for i, key := range tc.nonExistentKeys {
				nonExistentByteKeys[i] = []byte(key)
			}

			// Combine existing and non-existent keys
			allKeys := append(existingKeys, nonExistentByteKeys...)

			// Create a GetAllRequest to retrieve all entries including non-existent keys
			getAllRequest := &pbstore.GetAllRequest{
				BlockNumber: tc.requestBlock,
				BlockHash:   []byte("test_block_hash"),
				Keys:        allKeys,
			}

			// Retrieve all entries using GetAll
			getAllResponse, err := ts.store.GetAll(getAllRequest)
			require.NoError(t, err)

			// Verify the number of retrieved entries matches the number of requested keys
			assert.Equal(t, len(allKeys), len(getAllResponse.Entries))

			// Create a map of keys to response entries for easier verification
			responseMap := make(map[string]*pbstore.ResponseEntry)
			for _, responseEntry := range getAllResponse.Entries {
				responseMap[string(responseEntry.Key)] = responseEntry
			}

			// Verify existing keys are found
			for i, key := range existingKeys {
				responseEntry := responseMap[string(key)]
				require.NotNil(t, responseEntry, "Response entry not found for key %s", key)
				assert.Equal(t, pbstore.ResponseCode_RESPONSE_CODE_FOUND, responseEntry.Response.Code)

				// Unmarshal the retrieved value into an AccountOwner
				retrievedAccountOwner := &pbtest.TestAccountOwner{}
				err = responseEntry.Response.Value.UnmarshalTo(retrievedAccountOwner)
				require.NoError(t, err)

				// Verify the retrieved AccountOwner matches the original
				assert.Equal(t, accountOwners[i].Mint, retrievedAccountOwner.Mint)
				assert.Equal(t, accountOwners[i].Owner, retrievedAccountOwner.Owner)
			}

			// Verify non-existent keys are not found
			for _, key := range nonExistentByteKeys {
				responseEntry := responseMap[string(key)]
				require.NotNil(t, responseEntry, "Response for non-existent key %s not found", key)
				assert.Equal(t, pbstore.ResponseCode_RESPONSE_CODE_NOT_FOUND, responseEntry.Response.Code)
			}
		})
	}
}
