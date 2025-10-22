#!/bin/bash
# Test runner for PR description validation

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VALIDATOR="$SCRIPT_DIR/../../validate-pr-description.py"
TEST_DIR="$SCRIPT_DIR"

echo "Running PR description validation tests..."
echo

# Track test results
PASSED=0
FAILED=0

# Test helper function
run_test() {
    local test_name="$1"
    local test_file="$2"
    local expected_exit_code="$3"

    echo -n "Testing $test_name... "

    if "$VALIDATOR" "$test_file" > /dev/null 2>&1; then
        actual_exit_code=0
    else
        actual_exit_code=$?
    fi

    if [ "$actual_exit_code" -eq "$expected_exit_code" ]; then
        echo "✓ PASS"
        PASSED=$((PASSED + 1))
    else
        echo "✗ FAIL (expected exit code $expected_exit_code, got $actual_exit_code)"
        FAILED=$((FAILED + 1))
    fi
}

# Run tests
run_test "empty description" "$TEST_DIR/test_empty.txt" 1
run_test "valid description" "$TEST_DIR/test_valid.txt" 0
run_test "sourcery bot only" "$TEST_DIR/test_sourcery.txt" 1
run_test "no template, valid" "$TEST_DIR/test_no_template_valid.txt" 0
run_test "no template, short" "$TEST_DIR/test_no_template_short.txt" 1

echo
echo "================================"
echo "Test Results: $PASSED passed, $FAILED failed"
echo "================================"

if [ "$FAILED" -gt 0 ]; then
    exit 1
fi
