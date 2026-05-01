package postgres_time_traversal

import (
	"fmt"
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

// testStore represents a test postgres time traversal store with cleanup function
type testStore struct {
	store      *Store
	typeURL    string
	schemaName string
	cleanup    func()
}

// setupTestStore creates a new postgres time traversal store for testing
func setupTestStore(t *testing.T) *testStore {
	// Skip test if TEST_POSTGRES environment variable is not set
	if os.Getenv("TEST_POSTGRES") == "" {
		t.Skip("Skipping postgres_time_traversal test: TEST_POSTGRES environment variable not set")
	}

	// Get PostgreSQL connection string from environment or use default
	postgresHost := os.Getenv("POSTGRES_HOST")
	if postgresHost == "" {
		postgresHost = "localhost"
	}

	postgresPort := os.Getenv("POSTGRES_PORT")
	if postgresPort == "" {
		postgresPort = "5432"
	}

	postgresUser := os.Getenv("POSTGRES_USER")
	if postgresUser == "" {
		postgresUser = "postgres"
	}

	postgresPassword := os.Getenv("POSTGRES_PASSWORD")
	if postgresPassword == "" {
		postgresPassword = "password"
	}

	postgresDB := os.Getenv("POSTGRES_DB")
	if postgresDB == "" {
		postgresDB = "postgres"
	}

	// Create unique schema name for this test
	schemaName := fmt.Sprintf("test_schema_%s", t.Name())

	// Create DSN with unique schema
	dsnString := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?schemaName=%s&sslmode=disable",
		postgresUser, postgresPassword, postgresHost, postgresPort, postgresDB, schemaName)

	// Parse the DSN
	dsn, err := storelib.ParseDSN(dsnString)
	if err != nil {
		t.Skipf("Failed to parse PostgreSQL DSN (skipping test): %v", err)
	}

	// Create a new postgres time traversal store
	typeURL := "type.googleapis.com/test.TestAccountOwner"
	pgStore, err := NewStore(dsn, typeURL)
	if err != nil {
		t.Skipf("Failed to create PostgreSQL store (skipping test): %v", err)
	}

	cleanup := func() {
		if pgStore != nil {
			// Drop the test schema to clean up
			_, _ = pgStore.db.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schemaName))
			pgStore.Close()
		}
	}

	return &testStore{
		store:      pgStore,
		typeURL:    typeURL,
		schemaName: schemaName,
		cleanup:    cleanup,
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

func TestStoreAndRetrieveAccountOwner(t *testing.T) {
	ts := setupTestStore(t)
	defer ts.cleanup()

	// Create test data
	key := []byte("account-123")
	accountOwner := createAccountOwner("owner-123")
	blockNumber := uint64(100)

	// Create an entry
	entry, err := createEntry(blockNumber, key, accountOwner, ts.typeURL)
	require.NoError(t, err)

	// Store the entry
	err = ts.store.Set(entry, false, blockNumber)
	require.NoError(t, err)

	// Retrieve the entry
	getRequest := &pbservice.GetRequest{
		Keys:        []*pbmodel.Key{{Bytes: key}},
		BlockNumber: blockNumber,
		BlockHash:   []byte("test_block_hash"),
	}

	response, err := ts.store.Get(getRequest)
	require.NoError(t, err)
	require.NotNil(t, response)

	// Verify the response
	assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_FOUND, response.Entries.Entries[0].Code)
	assert.Equal(t, ts.typeURL, response.Entries.Entries[0].Entry.Value.TypeUrl)

	// Unmarshal and verify the stored data
	var retrievedAccountOwner pbtest.TestAccountOwner
	err = proto.Unmarshal(response.Entries.Entries[0].Entry.Value.Value, &retrievedAccountOwner)
	require.NoError(t, err)

	assert.Equal(t, accountOwner.Mint, retrievedAccountOwner.Mint)
	assert.Equal(t, accountOwner.Owner, retrievedAccountOwner.Owner)
}

func TestTimeTraversalWithMultipleVersions(t *testing.T) {
	ts := setupTestStore(t)
	defer ts.cleanup()

	key := []byte("account-456")

	// Store multiple versions of the same key at different block numbers
	testVersions := []struct {
		blockNumber  uint64
		ownerAddress string
	}{
		{100, "owner-v2"},
		{200, "owner-v2"},
		{300, "owner-v3"},
	}

	// Store all versions
	for _, version := range testVersions {
		accountOwner := createAccountOwner(version.ownerAddress)
		entry, err := createEntry(version.blockNumber, key, accountOwner, ts.typeURL)
		require.NoError(t, err)

		err = ts.store.Set(entry, false, version.blockNumber)
		require.NoError(t, err)
	}

	// Test time traversal: query at different block numbers
	testCases := []struct {
		name          string
		queryBlock    uint64
		expectedOwner string
		shouldFind    bool
	}{
		{"Before first version", 50, "", false},
		{"At first version", 100, "owner-v2", true},
		{"Between v2 and v2", 150, "owner-v2", true},
		{"At second version", 200, "owner-v2", true},
		{"Between v2 and v3", 250, "owner-v2", true},
		{"At third version", 300, "owner-v3", true},
		{"After last version", 400, "owner-v3", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			getRequest := &pbservice.GetRequest{
				Keys:        []*pbmodel.Key{{Bytes: key}},
				BlockNumber: tc.queryBlock,
				BlockHash:   []byte("test_block_hash"),
			}

			response, err := ts.store.Get(getRequest)
			require.NoError(t, err)
			require.NotNil(t, response)

			if !tc.shouldFind {
				assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND, response.Entries.Entries[0].Code)
			} else {
				assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_FOUND, response.Entries.Entries[0].Code)

				var retrievedAccountOwner pbtest.TestAccountOwner
				err = proto.Unmarshal(response.Entries.Entries[0].Entry.Value.Value, &retrievedAccountOwner)
				require.NoError(t, err)

				assert.Equal(t, []byte(tc.expectedOwner), retrievedAccountOwner.Owner)
			}
		})
	}
}

func TestSetAllAndGetAll(t *testing.T) {
	ts := setupTestStore(t)
	defer ts.cleanup()

	blockNumber := uint64(500)

	// Create multiple entries
	entries := []*pbmodel.Entry{}
	expectedOwners := []string{"owner-1", "owner-2", "owner-3"}

	for i, owner := range expectedOwners {
		key := []byte(fmt.Sprintf("account-%d", i+1))
		accountOwner := createAccountOwner(owner)
		entry, err := createEntry(blockNumber, key, accountOwner, ts.typeURL)
		require.NoError(t, err)
		entries = append(entries, entry)
	}

	// Store all entries
	err := ts.store.SetAll(entries, nil, false, blockNumber)
	require.NoError(t, err)

	// Prepare keys for GetAll
	keys := make([]*pbmodel.Key, len(entries))
	for i, entry := range entries {
		keys[i] = entry.Key
	}

	// Retrieve all entries
	getRequest := &pbservice.GetRequest{
		Keys:        keys,
		BlockNumber: blockNumber,
		BlockHash:   []byte("test_block_hash"),
	}

	response, err := ts.store.Get(getRequest)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Equal(t, len(expectedOwners), len(response.Entries.Entries))

	// Verify all entries
	for i, responseEntry := range response.Entries.Entries {
		assert.Equal(t, keys[i], responseEntry.Entry.Key)
		assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_FOUND, responseEntry.Code)

		var retrievedAccountOwner pbtest.TestAccountOwner
		err = proto.Unmarshal(responseEntry.Entry.Value.Value, &retrievedAccountOwner)
		require.NoError(t, err)

		assert.Equal(t, []byte(expectedOwners[i]), retrievedAccountOwner.Owner)
	}
}

func TestSetAllAndGetAllWithDifferentBlocks(t *testing.T) {
	ts := setupTestStore(t)
	defer ts.cleanup()

	// Store the same keys at different block numbers with different values
	keys := []*pbmodel.Key{
		&pbmodel.Key{Bytes: []byte("account-multi-1")},
		&pbmodel.Key{Bytes: []byte("account-multi-2")},
	}

	// Block 100: Initial values
	entries100 := []*pbmodel.Entry{}
	owners100 := []string{"owner-100-1", "owner-100-2"}

	for i, owner := range owners100 {
		accountOwner := createAccountOwner(owner)
		entry, err := createEntry(100, keys[i].Bytes, accountOwner, ts.typeURL)
		require.NoError(t, err)
		entries100 = append(entries100, entry)
	}

	err := ts.store.SetAll(entries100, nil, false, 100)
	require.NoError(t, err)

	// Block 200: Updated values
	entries200 := []*pbmodel.Entry{}
	owners200 := []string{"owner-200-1", "owner-200-2"}

	for i, owner := range owners200 {
		accountOwner := createAccountOwner(owner)
		entry, err := createEntry(200, keys[i].Bytes, accountOwner, ts.typeURL)
		require.NoError(t, err)
		entries200 = append(entries200, entry)
	}

	err = ts.store.SetAll(entries200, nil, false, 200)
	require.NoError(t, err)

	// Test retrieving at different block numbers
	testCases := []struct {
		name           string
		queryBlock     uint64
		expectedOwners []string
	}{
		{"At block 100", 100, owners100},
		{"Between blocks", 150, owners100},
		{"At block 200", 200, owners200},
		{"After block 200", 300, owners200},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			getRequest := &pbservice.GetRequest{
				Keys:        keys,
				BlockNumber: tc.queryBlock,
				BlockHash:   []byte("test_block_hash"),
			}

			response, err := ts.store.Get(getRequest)
			require.NoError(t, err)
			require.NotNil(t, response)
			require.Equal(t, len(keys), len(response.Entries.Entries))

			for i, responseEntry := range response.Entries.Entries {
				assert.Equal(t, keys[i], responseEntry.Entry.Key)
				assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_FOUND, responseEntry.Code)

				var retrievedAccountOwner pbtest.TestAccountOwner
				err = proto.Unmarshal(responseEntry.Entry.Value.Value, &retrievedAccountOwner)
				require.NoError(t, err)

				assert.Equal(t, []byte(tc.expectedOwners[i]), retrievedAccountOwner.Owner)
			}
		})
	}
}

func TestGetAllKeysOnly(t *testing.T) {
	ts := setupTestStore(t)
	defer ts.cleanup()

	blockNumber := uint64(600)
	keys := [][]byte{
		[]byte("key-only-1"),
		[]byte("key-only-2"),
		[]byte("key-only-3"),
	}

	// Store entries
	entries := []*pbmodel.Entry{}
	for i, key := range keys {
		accountOwner := createAccountOwner(fmt.Sprintf("owner-%d", i))
		entry, err := createEntry(blockNumber, key, accountOwner, ts.typeURL)
		require.NoError(t, err)
		entries = append(entries, entry)
	}

	err := ts.store.SetAll(entries, nil, false, blockNumber)
	require.NoError(t, err)

	// Test GetAllKeysOnly (service method)
	keyOnlyEntries, err := ts.store.GetAllKeysOnly(keys, blockNumber)
	require.NoError(t, err)
	require.Equal(t, len(keys), len(keyOnlyEntries))

	// Verify that all keys are returned
	for i, keyOnlyEntry := range keyOnlyEntries {
		assert.Equal(t, keys[i], keyOnlyEntry.Key)
		assert.Equal(t, blockNumber, keyOnlyEntry.BlockNumber)
	}
}

func TestGetKeyOnlyInternal(t *testing.T) {
	ts := setupTestStore(t)
	defer ts.cleanup()

	key := []byte("single-key-only")
	blockNumber := uint64(700)

	// Store entry
	accountOwner := createAccountOwner("owner-single")
	entry, err := createEntry(blockNumber, key, accountOwner, ts.typeURL)
	require.NoError(t, err)

	err = ts.store.Set(entry, false, blockNumber)
	require.NoError(t, err)

	// Test GetKeyOnly (service method)
	keyOnlyEntry, err := ts.store.GetKeyOnly(key, blockNumber)
	require.NoError(t, err)
	require.NotNil(t, keyOnlyEntry)
	assert.Equal(t, key, keyOnlyEntry.Key)
	assert.Equal(t, blockNumber, keyOnlyEntry.BlockNumber)

	// Test with non-existent key
	notFoundEntry, err := ts.store.GetKeyOnly([]byte("non-existent-key"), blockNumber)
	//require.Error(t, err) // Should return sql.ErrNoRows error
	require.Nil(t, notFoundEntry)
}

func TestEmptySetAll(t *testing.T) {
	ts := setupTestStore(t)
	defer ts.cleanup()

	// Test SetAll with empty entries
	err := ts.store.SetAll([]*pbmodel.Entry{}, nil, false, 100)
	require.NoError(t, err)
}

func TestGetNonExistentKey(t *testing.T) {
	ts := setupTestStore(t)
	defer ts.cleanup()

	// Try to get a key that doesn't exist
	getRequest := &pbservice.GetRequest{
		Keys:        []*pbmodel.Key{{Bytes: []byte("non-existent-key")}},
		BlockNumber: 100,
		BlockHash:   []byte("test_block_hash"),
	}

	response, err := ts.store.Get(getRequest)
	require.NoError(t, err)
	require.NotNil(t, response)
	assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND, response.Entries.Entries[0].Code)
}

func TestGetAllWithMixedExistence(t *testing.T) {
	ts := setupTestStore(t)
	defer ts.cleanup()

	blockNumber := uint64(800)

	// Store only some of the requested keys
	existingKey := &pbmodel.Key{Bytes: []byte("existing-key")}
	accountOwner := createAccountOwner("existing-owner")
	entry, err := createEntry(blockNumber, existingKey.Bytes, accountOwner, ts.typeURL)
	require.NoError(t, err)

	err = ts.store.Set(entry, false, blockNumber)
	require.NoError(t, err)

	// Request both existing and non-existing keys
	keys := []*pbmodel.Key{
		existingKey,
		&pbmodel.Key{Bytes: []byte("non-existing-key-1")},
		&pbmodel.Key{Bytes: []byte("non-existing-key-2")},
	}

	getRequest := &pbservice.GetRequest{
		Keys:        keys,
		BlockNumber: blockNumber,
		BlockHash:   []byte("test_block_hash"),
	}

	response, err := ts.store.Get(getRequest)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Equal(t, len(keys), len(response.Entries.Entries))

	// Verify mixed results
	assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_FOUND, response.Entries.Entries[0].Code)
	assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND, response.Entries.Entries[1].Code)
	assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND, response.Entries.Entries[2].Code)
}

// GetFirst tests for postgres_time_traversal
func TestGetFirstReturnsOldestVersionForKey_PostgresTimeTraversal(t *testing.T) {
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
		entry, err := createEntry(v.block, key, createAccountOwner(v.owner), ts.typeURL)
		require.NoError(t, err)
		require.NoError(t, ts.store.Set(entry, false, v.block))
	}
	// Add another key to ensure selection stays on k1
	other, err := createEntry(150, []byte("k2"), createAccountOwner("other"), ts.typeURL)
	require.NoError(t, err)
	require.NoError(t, ts.store.Set(other, false, 150))

	resp, err := ts.store.GetFirst(&pbservice.GetRequest{Keys: []*pbmodel.Key{{Bytes: []byte("k1")}}, BlockNumber: 200, BlockHash: []byte("test_block_hash")})
	require.NoError(t, err)
	assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_FOUND, resp.Entries.Entries[0].Code)
	got := &pbtest.TestAccountOwner{}
	require.NoError(t, resp.Entries.Entries[0].Entry.Value.UnmarshalTo(got))
	assert.Equal(t, []byte("v2"), got.Owner)
}

func TestGetFirstOrderingAndNotFound_PostgresTimeTraversal(t *testing.T) {
	ts := setupTestStore(t)
	defer ts.cleanup()

	pairs := []struct {
		k string
		v string
	}{
		{"a1", "v2"},
		{"a2", "v2"},
		{"b1", "v3"},
	}
	for _, p := range pairs {
		entry, err := createEntry(100, []byte(p.k), createAccountOwner(p.v), ts.typeURL)
		require.NoError(t, err)
		require.NoError(t, ts.store.Set(entry, false, 100))
	}

	// Exact match should return that key's value (oldest since only one)
	resp, err := ts.store.GetFirst(&pbservice.GetRequest{Keys: []*pbmodel.Key{{Bytes: []byte("a2")}}, BlockNumber: 100, BlockHash: []byte("test_block_hash")})
	require.NoError(t, err)
	assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_FOUND, resp.Entries.Entries[0].Code)
	got := &pbtest.TestAccountOwner{}
	require.NoError(t, resp.Entries.Entries[0].Entry.Value.UnmarshalTo(got))
	assert.Equal(t, []byte("v2"), got.Owner)

	// Between a2 and b1 -> expect b1
	resp, err = ts.store.GetFirst(&pbservice.GetRequest{Keys: []*pbmodel.Key{{Bytes: []byte("a3")}}, BlockNumber: 100, BlockHash: []byte("test_block_hash")})
	require.NoError(t, err)
	assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_FOUND, resp.Entries.Entries[0].Code)
	got = &pbtest.TestAccountOwner{}
	require.NoError(t, resp.Entries.Entries[0].Entry.Value.UnmarshalTo(got))
	assert.Equal(t, []byte("v2"), got.Owner)

	// Between a2 and b1 -> return b1
	resp, err = ts.store.GetFirst(&pbservice.GetRequest{Keys: []*pbmodel.Key{{Bytes: []byte("a3")}}, BlockNumber: 100, BlockHash: []byte("test_block_hash")})
	require.NoError(t, err)
	assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_FOUND, resp.Entries.Entries[0].Code)
	got = &pbtest.TestAccountOwner{}
	require.NoError(t, resp.Entries.Entries[0].Entry.Value.UnmarshalTo(got))
	assert.Equal(t, []byte("v3"), got.Owner)

	// After last -> not found
	resp, err = ts.store.GetFirst(&pbservice.GetRequest{Keys: []*pbmodel.Key{{Bytes: []byte("z9")}}, BlockNumber: 100, BlockHash: []byte("test_block_hash")})
	require.NoError(t, err)
	assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND, resp.Entries.Entries[0].Code)
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
