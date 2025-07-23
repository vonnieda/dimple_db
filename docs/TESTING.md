# Testing DimpleDb

This document describes how to run the comprehensive test suite for DimpleDb, including S3 storage integration tests.

## Running Basic Tests

```bash
# Run all unit tests
cargo test

# Run with output
cargo test -- --nocapture

# Run specific test modules
cargo test sync::sync_engine
cargo test storage::s3_storage
```

## S3 Storage Integration Tests

The S3 storage module includes comprehensive integration tests that can run against real S3 services.

### Environment Variables

Set these environment variables to enable S3 testing:

```bash
# Required
export DIMPLE_TEST_S3_ENDPOINT="https://s3.amazonaws.com"  # S3 endpoint URL
export DIMPLE_TEST_S3_BUCKET="your-test-bucket"           # S3 bucket name  
export DIMPLE_TEST_S3_ACCESS_KEY="your-access-key"        # S3 access key
export DIMPLE_TEST_S3_SECRET_KEY="your-secret-key"        # S3 secret key

# Optional
export DIMPLE_TEST_S3_REGION="us-east-1"                  # S3 region (default: us-east-1)
export DIMPLE_TEST_S3_PREFIX="dimple-test"                # Storage prefix (default: dimple-test)
```

### Testing with AWS S3

```bash
# Set AWS credentials
export DIMPLE_TEST_S3_ENDPOINT="https://s3.amazonaws.com"
export DIMPLE_TEST_S3_BUCKET="your-test-bucket"
export DIMPLE_TEST_S3_REGION="us-west-2"
export DIMPLE_TEST_S3_ACCESS_KEY="your-access-key"
export DIMPLE_TEST_S3_SECRET_KEY="your-secret-key"
export DIMPLE_TEST_S3_PREFIX="dimple-ci-test"

# Run tests
cargo test storage::s3_storage -- --nocapture
```

## Test Coverage

### S3 Storage Tests

The S3 storage tests cover:

- **Basic Operations**: Put, get, and list files
- **Binary Content**: Handling of arbitrary binary data
- **Large Files**: 1MB+ content upload/download
- **JSON Content**: Structured data like sync engine payloads
- **Empty Files**: Zero-byte content handling
- **Prefix Isolation**: Ensuring proper namespace separation
- **Error Handling**: Non-existent file retrieval
- **Environment Check**: Validation of test configuration

### Sync Engine Tests

- **Basic Sync**: Two-way synchronization between databases
- **Catch-up Sync**: Handling multiple changes between sync cycles  
- **Conflict Resolution**: Last-write-wins per attribute
- **Foreign Key Ordering**: Proper entity creation order
- **Encrypted Storage**: End-to-end encryption functionality

## Continuous Integration

For CI/CD pipelines, tests will automatically skip S3 tests when credentials are not provided:

```bash
# Without credentials - only unit tests run
cargo test

# With S3 credentials in CI
export DIMPLE_TEST_S3_ENDPOINT=https://s3.amazonaws.com
export DIMPLE_TEST_S3_BUCKET=ci-test-bucket
export DIMPLE_TEST_S3_ACCESS_KEY=$AWS_ACCESS_KEY_ID
export DIMPLE_TEST_S3_SECRET_KEY=$AWS_SECRET_ACCESS_KEY
export DIMPLE_TEST_S3_PREFIX=ci-test-$(date +%s)
cargo test
```

## Debugging Tests

```bash
# Run with debug logging
RUST_LOG=debug cargo test storage::s3_storage -- --nocapture

# Check environment variable configuration
cargo test test_environment_variables_info -- --nocapture

# Run specific test
cargo test test_s3_put_and_get -- --nocapture
```

## Expected Output

When S3 credentials are configured correctly:

```
running 9 tests
S3 put/get test passed
S3 list test passed - found 3 files
S3 binary content test passed
S3 large content test passed - 1048576 bytes
S3 JSON content test passed
S3 prefix isolation test passed
S3 error handling test passed
S3 empty content test passed
All required S3 test credentials are available
test result: ok. 9 passed; 0 failed; 0 ignored
```

When S3 credentials are not configured:

```
running 9 tests
Skipping S3 test - no credentials provided
...
S3 tests will be skipped - missing credentials
   Set environment variables to enable S3 testing
test result: ok. 9 passed; 0 failed; 0 ignored
```