package postgres

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

// testStore represents a test postgres store with cleanup function
type testStore struct {
	store      *Store
	typeURL    string
	schemaName string
	cleanup    func()
}

// setupTestStore creates a new postgres store for testing
func setupTestStore(t *testing.T) *testStore {
	// Skip test if TEST_POSTGRES environment variable is not set
	if os.Getenv("TEST_POSTGRES") == "" {
		t.Skip("Skipping postgres test: TEST_POSTGRES environment variable not set")
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

	// Create a new postgres store
	typeURL := "type.googleapis.com/test.TestAccountOwner"
	pgStore, err := NewStore(dsn, typeURL)
	if err != nil {
		t.Skipf("Failed to create PostgreSQL store (skipping test): %v", err)
	}

	cleanup := func() {
		if pgStore != nil && pgStore.db != nil {
			// Drop the test schema to clean up
			_, _ = pgStore.db.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schemaName))
			pgStore.db.Close()
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
		Keys: []*pbmodel.Key{{Bytes: key}},
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

func TestGetNonExistentKey(t *testing.T) {
	ts := setupTestStore(t)
	defer ts.cleanup()

	// Try to retrieve a non-existent key
	getRequest := &pbservice.GetRequest{
		Keys: []*pbmodel.Key{{Bytes: []byte("non-existent-key")}},
	}

	response, err := ts.store.Get(getRequest)
	require.NoError(t, err)
	require.NotNil(t, response)

	// Verify the response indicates not found
	assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND, response.Entries.Entries[0].Code)
}

func TestSetAllAndGetAll(t *testing.T) {
	ts := setupTestStore(t)
	defer ts.cleanup()

	blockNumber := uint64(200)

	// Create multiple entries
	entries := make([]*pbmodel.Entry, 3)
	keys := []*pbmodel.Key{
		&pbmodel.Key{Bytes: []byte("account-1")},
		&pbmodel.Key{Bytes: []byte("account-2")},
		&pbmodel.Key{Bytes: []byte("account-3")},
	}
	owners := []*pbtest.TestAccountOwner{
		createAccountOwner("owner-1"),
		createAccountOwner("owner-2"),
		createAccountOwner("owner-3"),
	}

	for i := range entries {
		entry, err := createEntry(blockNumber, keys[i].Bytes, owners[i], ts.typeURL)
		require.NoError(t, err)
		entries[i] = entry
	}

	// Store all entries
	err := ts.store.SetAll(entries, nil, false, blockNumber)
	require.NoError(t, err)

	// Retrieve all entries
	getRequest := &pbservice.GetRequest{
		BlockNumber: blockNumber,
		BlockHash:   []byte("test_block_hash"),
		Keys:        keys,
	}

	response, err := ts.store.Get(getRequest)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Len(t, response.Entries.Entries, 3)

	// Verify all entries were found
	for i, queriedEntry := range response.Entries.Entries {
		assert.Equal(t, keys[i], queriedEntry.Entry.Key)
		assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_FOUND, queriedEntry.Code)
		assert.Equal(t, ts.typeURL, queriedEntry.Entry.Value.TypeUrl)

		// Unmarshal and verify the stored data
		var retrievedAccountOwner pbtest.TestAccountOwner
		err = proto.Unmarshal(queriedEntry.Entry.Value.Value, &retrievedAccountOwner)
		require.NoError(t, err)

		assert.Equal(t, owners[i].Mint, retrievedAccountOwner.Mint)
		assert.Equal(t, owners[i].Owner, retrievedAccountOwner.Owner)
	}
}

func TestGetAllWithMixedExistence(t *testing.T) {
	ts := setupTestStore(t)
	defer ts.cleanup()

	blockNumber := uint64(300)

	// Store only one entry
	key1 := []byte("existing-account")
	key2 := []byte("non-existing-account")
	accountOwner := createAccountOwner("existing-owner")

	entry, err := createEntry(blockNumber, key1, accountOwner, ts.typeURL)
	require.NoError(t, err)

	err = ts.store.Set(entry, false, blockNumber)
	require.NoError(t, err)

	// Try to get both existing and non-existing keys
	keys := []*pbmodel.Key{
		{Bytes: key1},
		{Bytes: key2},
	}
	getRequest := &pbservice.GetRequest{
		BlockNumber: blockNumber,
		BlockHash:   []byte("test_block_hash"),
		Keys:        keys,
	}

	response, err := ts.store.Get(getRequest)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Len(t, response.Entries, 2)

	// Verify first entry was found
	assert.Equal(t, key1, response.Entries.Entries[0].Entry.Key)
	assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_FOUND, response.Entries.Entries[0].Code)
	assert.Equal(t, ts.typeURL, response.Entries.Entries[0].Entry.Value.TypeUrl)

	// Verify second entry was not found
	assert.Equal(t, key2, response.Entries.Entries[1].Entry.Key)
	assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_NOT_FOUND, response.Entries.Entries[1].Code)
}

func TestSetWithNilEntry(t *testing.T) {
	ts := setupTestStore(t)
	defer ts.cleanup()

	// Try to set a nil entry
	err := ts.store.Set(nil, false, 100)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "entry cannot be nil")
}

func TestSetAllWithNilEntry(t *testing.T) {
	ts := setupTestStore(t)
	defer ts.cleanup()

	blockNumber := uint64(400)

	// Create entries with one nil entry
	key := []byte("valid-key")
	accountOwner := createAccountOwner("valid-owner")
	validEntry, err := createEntry(blockNumber, key, accountOwner, ts.typeURL)
	require.NoError(t, err)

	entries := []*pbmodel.Entry{validEntry, nil}

	// Try to store entries with nil entry
	err = ts.store.SetAll(entries, nil, false, blockNumber)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "entry cannot be nil")
}

func TestEmptySetAll(t *testing.T) {
	ts := setupTestStore(t)
	defer ts.cleanup()

	// Test SetAll with empty entries slice
	err := ts.store.SetAll([]*pbmodel.Entry{}, nil, false, 100)
	require.NoError(t, err)
}

func TestGetAllWithEmptyKeys(t *testing.T) {
	ts := setupTestStore(t)
	defer ts.cleanup()

	// Test Get with empty keys slice
	response, err := ts.store.Get(&pbservice.GetRequest{
		BlockNumber: 100,
		BlockHash:   []byte("test_block_hash"),
		Keys:        []*pbmodel.Key{},
	})
	require.NoError(t, err)
	require.NotNil(t, response)
	assert.Empty(t, response.Entries)
}

func TestStoreCreation(t *testing.T) {
	// Test store creation with valid DSN
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

	schemaName := fmt.Sprintf("test_schema_%s", t.Name())
	dsnString := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?schemaName=%s&sslmode=disable",
		postgresUser, postgresPassword, postgresHost, postgresPort, postgresDB, schemaName)

	dsn, err := storelib.ParseDSN(dsnString)
	if err != nil {
		t.Skipf("Failed to parse PostgreSQL DSN (skipping test): %v", err)
	}

	typeURL := "type.googleapis.com/test.TestAccountOwner"
	store, err := NewStore(dsn, typeURL)
	if err != nil {
		t.Skipf("Failed to create PostgreSQL store (skipping test): %v", err)
	}

	// Verify store was created successfully
	require.NotNil(t, store)
	assert.Equal(t, typeURL, store.typeUrl)
	assert.Equal(t, schemaName, store.schemaName)
	assert.NotNil(t, store.db)
	assert.NotNil(t, store.insertStatement)
	assert.NotNil(t, store.selectStatement)
	assert.NotNil(t, store.selectAnyStatement)

	// Cleanup
	_, _ = store.db.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schemaName))
	store.db.Close()
}

func TestMultipleOperations(t *testing.T) {
	ts := setupTestStore(t)
	defer ts.cleanup()

	blockNumber := uint64(500)

	// Test multiple Set operations
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("account-%d", i))
		accountOwner := createAccountOwner(fmt.Sprintf("owner-%d", i))

		entry, err := createEntry(blockNumber, key, accountOwner, ts.typeURL)
		require.NoError(t, err)

		err = ts.store.Set(entry, false, blockNumber)
		require.NoError(t, err)
	}

	// Verify all entries can be retrieved
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("account-%d", i))
		getRequest := &pbservice.GetRequest{Keys: []*pbmodel.Key{{Bytes: key}}}

		response, err := ts.store.Get(getRequest)
		require.NoError(t, err)
		assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_FOUND, response.Entries.Entries[0].Code)
	}

	// Test GetAll with all keys
	allKeys := make([]*pbmodel.Key, 5)
	for i := 0; i < 5; i++ {
		allKeys[i] = &pbmodel.Key{Bytes: []byte(fmt.Sprintf("account-%d", i))}
	}

	getRequest := &pbservice.GetRequest{
		BlockNumber: 100,
		BlockHash:   []byte("test_block_hash"),
		Keys:        allKeys,
	}
	response, err := ts.store.Get(getRequest)
	require.NoError(t, err)
	assert.Len(t, response.Entries.Entries, 5)

	// Verify all entries were found
	for _, queriedEntry := range response.Entries.Entries {
		assert.Equal(t, pbmodel.ResponseCode_RESPONSE_CODE_FOUND, queriedEntry.Code)
	}
}

// GetFirst tests for postgres (non-time-traversal)
func TestGetFirstOrderingAndNotFound_Postgres(t *testing.T) {
	ts := setupTestStore(t)
	defer ts.cleanup()

	// Insert several keys
	keys := [][]byte{[]byte("a1"), []byte("a2"), []byte("b1")}
	for i, k := range keys {
		entry, err := createEntry(100, k, createAccountOwner(fmt.Sprintf("v%d", i+1)), ts.typeURL)
		require.NoError(t, err)
		require.NoError(t, ts.store.Set(entry, false, 100))
	}

	// Exact match
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
	assert.Equal(t, []byte("v3"), got.Owner)

	// Beyond last key -> not found
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
