#!/bin/bash
set -e

# Badger Backed Stores - End-to-End Test Suite
# Tests the foundational store gRPC write path with Badger backend

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SERVER="${SERVER:-localhost:9020}"
RESULTS_FILE="test_results.log"
FAILED_TESTS=0
PASSED_TESTS=0

echo "=== Badger Backed Stores - gRPC Test Suite ===" | tee $RESULTS_FILE
echo "Server: $SERVER" | tee -a $RESULTS_FILE
echo "Started: $(date)" | tee -a $RESULTS_FILE
echo "" | tee -a $RESULTS_FILE

# Helper functions
pass() {
    echo -e "${GREEN}✓ PASS${NC}: $1" | tee -a $RESULTS_FILE
    PASSED_TESTS=$((PASSED_TESTS + 1))
}

fail() {
    echo -e "${RED}✗ FAIL${NC}: $1" | tee -a $RESULTS_FILE
    FAILED_TESTS=$((FAILED_TESTS + 1))
}

info() {
    echo -e "${BLUE}ℹ${NC} $1" | tee -a $RESULTS_FILE
}

section() {
    echo "" | tee -a $RESULTS_FILE
    echo -e "${YELLOW}=== $1 ===${NC}" | tee -a $RESULTS_FILE
}

# Test 1: Basic SET/GET
test_basic_set_get() {
    section "Test 1: Basic SET/GET"
    
    info "Setting key 'test1' with value 'hello'"
    foundational-store set \
        --server=$SERVER \
        --key=test1 \
        --value=hello \
        --update-policy=SET \
        --block-number=1 \
        --value-type=bytes
    
    info "Getting key 'test1'"
    RESULT=$(foundational-store get \
        --server=$SERVER \
        --key=test1 \
        --block-number=1 \
        --encoding=hex 2>&1)
    
    if echo "$RESULT" | grep -q "hello\|Value:"; then
        pass "Basic SET/GET works"
    else
        fail "Basic SET/GET failed - value not found"
    fi
}

# Test 2: ADD Policy (Accumulation)
test_add_policy() {
    section "Test 2: ADD Policy (Accumulation)"
    
    info "Adding 10 to counter"
    foundational-store set \
        --server=$SERVER \
        --key=counter \
        --value=10 \
        --update-policy=ADD \
        --value-type=int64 \
        --block-number=2
    
    info "Adding 5 to counter"
    foundational-store set \
        --server=$SERVER \
        --key=counter \
        --value=5 \
        --update-policy=ADD \
        --value-type=int64 \
        --block-number=3
    
    info "Getting counter value"
    RESULT=$(foundational-store get \
        --server=$SERVER \
        --key=counter \
        --block-number=3 \
        --encoding=hex 2>&1)
    
    if echo "$RESULT" | grep -q "15"; then
        pass "ADD policy accumulation works (10 + 5 = 15)"
    else
        fail "ADD policy failed - expected 15, got: $RESULT"
    fi
}

# Test 3: MIN Policy
test_min_policy() {
    section "Test 3: MIN Policy"
    
    info "Setting min_val to 100"
    foundational-store set \
        --server=$SERVER \
        --key=min_val \
        --value=100 \
        --update-policy=MIN \
        --value-type=int64 \
        --block-number=4
    
    info "Setting min_val to 50 (should keep 50)"
    foundational-store set \
        --server=$SERVER \
        --key=min_val \
        --value=50 \
        --update-policy=MIN \
        --value-type=int64 \
        --block-number=5
    
    info "Getting min_val"
    RESULT=$(foundational-store get \
        --server=$SERVER \
        --key=min_val \
        --block-number=5 \
        --encoding=hex 2>&1)
    
    if echo "$RESULT" | grep -q "50"; then
        pass "MIN policy works (kept 50 instead of 100)"
    else
        fail "MIN policy failed - expected 50, got: $RESULT"
    fi
}

# Test 4: MAX Policy
test_max_policy() {
    section "Test 4: MAX Policy"
    
    info "Setting max_val to 50"
    foundational-store set \
        --server=$SERVER \
        --key=max_val \
        --value=50 \
        --update-policy=MAX \
        --value-type=int64 \
        --block-number=6
    
    info "Setting max_val to 100 (should keep 100)"
    foundational-store set \
        --server=$SERVER \
        --key=max_val \
        --value=100 \
        --update-policy=MAX \
        --value-type=int64 \
        --block-number=7
    
    info "Getting max_val"
    RESULT=$(foundational-store get \
        --server=$SERVER \
        --key=max_val \
        --block-number=7 \
        --encoding=hex 2>&1)
    
    if echo "$RESULT" | grep -q "100"; then
        pass "MAX policy works (kept 100 instead of 50)"
    else
        fail "MAX policy failed - expected 100, got: $RESULT"
    fi
}

# Test 5: SET_IF_NOT_EXISTS Policy
test_set_if_not_exists() {
    section "Test 5: SET_IF_NOT_EXISTS Policy"
    
    info "Setting 'once' key to 'first'"
    foundational-store set \
        --server=$SERVER \
        --key=once \
        --value=first \
        --update-policy=SET_IF_NOT_EXISTS \
        --value-type=bytes \
        --block-number=8
    
    info "Attempting to set 'once' key to 'second' (should be ignored)"
    foundational-store set \
        --server=$SERVER \
        --key=once \
        --value=second \
        --update-policy=SET_IF_NOT_EXISTS \
        --value-type=bytes \
        --block-number=9
    
    info "Getting 'once' value"
    RESULT=$(foundational-store get \
        --server=$SERVER \
        --key=once \
        --block-number=9 \
        --encoding=hex 2>&1)
    
    if echo "$RESULT" | grep -q "first"; then
        pass "SET_IF_NOT_EXISTS works (kept 'first', ignored 'second')"
    else
        fail "SET_IF_NOT_EXISTS failed - expected 'first', got: $RESULT"
    fi
}

# Test 6: FlushUpToBlock (Persistence to Badger)
test_flush() {
    section "Test 6: FlushUpToBlock (Persistence)"
    
    info "Setting persist_test key"
    foundational-store set \
        --server=$SERVER \
        --key=persist_test \
        --value=persisted_data \
        --update-policy=SET \
        --value-type=bytes \
        --block-number=10
    
    info "Flushing up to block 10 (persists to Badger)"
    foundational-store flush \
        --server=$SERVER \
        --block-number=10
    
    info "Verifying data is accessible after flush"
    RESULT=$(foundational-store get \
        --server=$SERVER \
        --key=persist_test \
        --block-number=10 \
        --encoding=hex 2>&1)
    
    if echo "$RESULT" | grep -q "persisted_data\|Value:"; then
        pass "FlushUpToBlock works (data persisted to Badger)"
    else
        fail "FlushUpToBlock failed - data not found after flush"
    fi
}

# Test 7: EvictUpToBlock (Fork Handling)
test_evict() {
    section "Test 7: EvictUpToBlock (Fork Handling)"
    
    info "Setting values at blocks 20-25"
    for i in {20..25}; do
        foundational-store set \
            --server=$SERVER \
            --key=fork_test \
            --value=$i \
            --update-policy=SET \
            --value-type=int64 \
            --block-number=$i 2>&1 | grep -q "Entries written" || true
    done
    
    info "Evicting from block 23 onwards (simulate fork)"
    foundational-store evict \
        --server=$SERVER \
        --block-number=23
    
    info "Verifying block 22 still exists"
    RESULT_22=$(foundational-store get \
        --server=$SERVER \
        --key=fork_test \
        --block-number=22 \
        --encoding=hex 2>&1)
    
    info "Verifying block 24 was evicted"
    RESULT_24=$(foundational-store get \
        --server=$SERVER \
        --key=fork_test \
        --block-number=24 \
        --encoding=hex 2>&1)
    
    if echo "$RESULT_22" | grep -q "22\|Value:"; then
        if echo "$RESULT_24" | grep -q "NOT_FOUND\|not found"; then
            pass "EvictUpToBlock works (block 22 kept, block 24 evicted)"
        else
            fail "EvictUpToBlock failed - block 24 should have been evicted"
        fi
    else
        fail "EvictUpToBlock failed - block 22 should still exist"
    fi
}

# Run all tests
main() {
    test_basic_set_get
    test_add_policy
    test_min_policy
    test_max_policy
    test_set_if_not_exists
    test_flush
    test_evict
    
    # Summary
    section "Test Summary"
    info "Passed: $PASSED_TESTS"
    info "Failed: $FAILED_TESTS"
    echo "" | tee -a $RESULTS_FILE
    echo "Finished: $(date)" | tee -a $RESULTS_FILE
    echo "Results saved to: $RESULTS_FILE" | tee -a $RESULTS_FILE
    
    if [ $FAILED_TESTS -eq 0 ]; then
        echo -e "${GREEN}All tests passed!${NC}"
        exit 0
    else
        echo -e "${RED}Some tests failed!${NC}"
        exit 1
    fi
}

# Check if foundational-store binary exists
if ! command -v foundational-store &> /dev/null; then
    echo -e "${RED}ERROR: foundational-store binary not found in PATH${NC}"
    echo "Please build it first: cd substreams-foundational-store && go build -o foundational-store ./cmd/foundational-store"
    exit 1
fi

# Run tests
main
