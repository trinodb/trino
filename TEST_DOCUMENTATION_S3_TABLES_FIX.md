# Test Documentation for S3 Tables and Exchange Manager Isolation Fix

This document describes the comprehensive test suite created to validate the fix for issue #26419.

## Test Files Created

### 1. `TestS3ClientIsolation.java`
**Purpose**: Unit and integration tests for S3 client isolation between catalog and exchange operations.

#### Test Cases:

- **`testS3ClientCreationWithRegion()`**
  - Validates S3 client creation with region configuration
  - Ensures proper endpoint resolution based on region
  - Verifies isolation from S3 Tables endpoints

- **`testS3ClientCreationWithExplicitEndpoint()`**
  - Tests explicit endpoint override functionality
  - Validates that custom endpoints are properly applied
  - Ensures independence from AWS SDK default resolution

- **`testS3ClientIsolationFromCatalogEndpoints()`**
  - **KEY TEST** - Simulates S3 Tables catalog interference
  - Verifies exchange manager operations are isolated
  - Validates that catalog-level configurations don't affect exchange

- **`testMultipleBucketIsolation()`**
  - Tests S3 client caching per bucket
  - Ensures each bucket gets its own isolated client instance
  - Prevents cross-contamination between buckets

- **`testRegionOrEndpointRequired()`**
  - Validates configuration requirements
  - Ensures either region or endpoint is always set
  - Tests the `isEndpointOrRegionSet()` validation

- **`testS3AsyncClientEndpointConfiguration()`**
  - Tests S3AsyncClient endpoint configuration logic
  - Validates region-based endpoint resolution
  - Ensures storage operations work with regular S3 buckets

- **`testS3TablesPatternDoesNotAffectExchange()`**
  - Verifies S3 Tables bucket pattern recognition
  - Ensures exchange manager ignores S3 Tables patterns
  - Validates that `--table-s3` suffix doesn't affect exchange

- **`testConcurrentS3ClientCreation()`**
  - Tests concurrent client creation
  - Ensures thread-safety and isolation
  - Validates no interference between concurrent operations

---

### 2. `TestS3TablesExchangeIsolation.java`
**Purpose**: Integration tests simulating real-world S3 Tables and exchange manager scenarios.

#### Test Cases:

- **`testExchangeWithRegularBucket()`**
  - End-to-end test with regular S3 bucket
  - Validates basic write and read operations
  - Ensures proper S3 endpoint usage

- **`testExchangeNotAffectedByS3TablesPattern()`**
  - **CRITICAL TEST** - Simulates S3 Tables naming patterns
  - Verifies exchange manager remains unaffected
  - Validates standard S3 endpoint usage despite S3 Tables patterns

- **`testConcurrentCatalogAndExchangeOperations()`**
  - Simulates simultaneous catalog and exchange operations
  - Tests the exact scenario from issue #26419
  - Validates no interference with 10 concurrent operations
  - Ensures all operations complete successfully

- **`testExchangeWithDifferentRegions()`**
  - Tests multiple AWS regions
  - Validates regional endpoint isolation
  - Ensures each region uses correct standard S3 endpoints

- **`testExchangeMultipartUpload()`**
  - Tests multipart upload functionality
  - Critical for fault tolerance (handles large data)
  - Validates endpoint isolation during complex S3 operations

---

### 3. `TestS3AsyncClientConfiguration.java`
**Purpose**: Low-level unit tests for S3AsyncClient builder configuration.

#### Test Cases:

- **`testClientCreationWithRegionOnly()`**
  - Tests AWS SDK default endpoint resolution
  - Validates region-based configuration
  - Ensures no explicit endpoint override when not configured

- **`testClientCreationWithExplicitEndpoint()`**
  - Tests custom endpoint override
  - Validates both region and endpoint can be set
  - Ensures explicit endpoint takes precedence

- **`testClientCreationWithEndpointOnly()`**
  - Tests MinIO/S3-compatible endpoint configuration
  - Validates endpoint-only setup (no region)
  - Important for local development and testing

- **`testMultipleRegionsUseDifferentEndpoints()`**
  - Tests 4 different AWS regions
  - Validates each region's independent configuration
  - Ensures proper isolation between regional clients

- **`testPathStyleAccessConfiguration()`**
  - Tests path-style access (required for MinIO)
  - Validates S3Configuration builder settings
  - Ensures compatibility with S3-compatible services

- **`testClientConfigurationImmutability()`**
  - Tests that client configs are independent
  - Validates no shared state between clients
  - Ensures concurrent client creation safety

- **`testAsyncClientConcurrencyConfiguration()`**
  - Tests async client performance settings
  - Validates concurrency and connection pooling
  - Important for fault tolerance throughput

- **`testS3ClientCachingPerBucket()`**
  - Tests client caching mechanism
  - Validates efficient resource usage
  - Ensures proper client reuse per bucket

- **`testNoEndpointProviderFallback()`**
  - **THE CORE FIX TEST** - Validates removal of AwsClientEndpointProvider
  - Ensures no dynamic endpoint provider fallback
  - Verifies the exact fix for issue #26419

- **`testRegionBasedEndpointResolution()`**
  - Comprehensive test of region-based resolution
  - Tests multiple AWS regions
  - Validates standard S3 endpoint usage
  - Ensures S3 Tables isolation

---

## Test Coverage Matrix

| Scenario | Test File | Test Method | Status |
|----------|-----------|-------------|--------|
| Basic S3 operations | TestS3TablesExchangeIsolation | testExchangeWithRegularBucket | ✅ |
| S3 Tables pattern isolation | TestS3ClientIsolation | testS3TablesPatternDoesNotAffectExchange | ✅ |
| S3 Tables pattern isolation | TestS3TablesExchangeIsolation | testExchangeNotAffectedByS3TablesPattern | ✅ |
| Concurrent operations | TestS3TablesExchangeIsolation | testConcurrentCatalogAndExchangeOperations | ✅ |
| Region-based endpoints | TestS3AsyncClientConfiguration | testRegionBasedEndpointResolution | ✅ |
| Explicit endpoint override | TestS3AsyncClientConfiguration | testClientCreationWithExplicitEndpoint | ✅ |
| No AwsClientEndpointProvider | TestS3AsyncClientConfiguration | testNoEndpointProviderFallback | ✅ |
| Multiple regions | TestS3TablesExchangeIsolation | testExchangeWithDifferentRegions | ✅ |
| Multipart uploads | TestS3TablesExchangeIsolation | testExchangeMultipartUpload | ✅ |
| Client caching | TestS3AsyncClientConfiguration | testS3ClientCachingPerBucket | ✅ |
| Configuration validation | TestS3ClientIsolation | testRegionOrEndpointRequired | ✅ |
| Thread safety | TestS3ClientIsolation | testConcurrentS3ClientCreation | ✅ |

---

## How to Run the Tests

### Run all S3 isolation tests:
```bash
./mvnw test -pl plugin/trino-exchange-filesystem -Dtest=TestS3*Isolation*
```

### Run S3AsyncClient configuration tests:
```bash
./mvnw test -pl plugin/trino-exchange-filesystem -Dtest=TestS3AsyncClientConfiguration
```

### Run all S3 exchange tests:
```bash
./mvnw test -pl plugin/trino-exchange-filesystem -Dtest=TestS3*
```

---

## What the Tests Validate

### ✅ The Fix Works:
1. **Endpoint Isolation**: Exchange manager uses standard S3 endpoints, never S3 Tables endpoints
2. **Region-Based Resolution**: AWS SDK properly resolves regional S3 endpoints
3. **No Dynamic Fallback**: Removed AwsClientEndpointProvider prevents interference
4. **Concurrent Safety**: Multiple operations work simultaneously without conflicts
5. **Configuration Flexibility**: Both region-based and endpoint-based configs work

### ✅ Issue #26419 is Resolved:
- S3 Tables catalog can run alongside fault-tolerant exchange manager
- No API routing conflicts between catalog and exchange operations
- Standard S3 endpoints used for exchange, S3 Tables endpoints for catalog
- Complete isolation between the two systems

### ✅ Backward Compatibility:
- Existing configurations continue to work
- No breaking changes to API or configuration
- MinIO and S3-compatible services still supported
- Path-style access still works correctly

---

## Key Test Assertions

### Isolation Assertions:
```java
// Verifies standard S3 endpoint (not S3 Tables/Glue)
assertThat(endpointStr).doesNotContain("glue");
assertThat(endpointStr).doesNotContain("s3tables");
```

### Configuration Assertions:
```java
// Verifies region is set when no endpoint specified
assertThat(config.getS3Region()).isPresent();
assertThat(config.getS3Endpoint()).isEmpty();
```

### Concurrency Assertions:
```java
// Verifies all concurrent operations succeed
for (CompletableFuture<Void> future : futures) {
    assertThat(future).isCompleted();
    assertThat(future.isCompletedExceptionally()).isFalse();
}
```

---

## Test Dependencies

- **MinioStorage**: Provides S3-compatible test environment
- **TestExchangeManagerContext**: Mocks exchange manager context
- **FileSystemExchangeManagerFactory**: Creates exchange manager instances
- **AssertJ**: Fluent assertion library
- **JUnit 5**: Test framework

---

## Future Test Enhancements

Potential additional tests to consider:

1. **AWS Integration Tests**: Test against real AWS S3 and S3 Tables (requires AWS credentials)
2. **Performance Tests**: Measure throughput with the fix
3. **Chaos Engineering**: Inject failures to test resilience
4. **Memory Leak Tests**: Ensure client instances are properly garbage collected
5. **Cross-Region Tests**: Test multi-region deployments

---

## Debugging Failed Tests

If tests fail:

1. **Check MinIO startup**: Ensure MinIO container starts successfully
2. **Verify AWS SDK version**: Confirm compatible AWS SDK v2 version
3. **Check network connectivity**: Ensure tests can reach MinIO endpoint
4. **Review logs**: Look for S3 client creation errors
5. **Validate configuration**: Ensure region/endpoint validation passes

---

## References

- **Issue**: #26419 - S3 Tables Catalog Conflicts with Exchange Manager S3 Operations
- **Fix**: `S3FileSystemExchangeStorage.java:488-524`
- **Documentation**: `SOLUTION_S3_TABLES_EXCHANGE_FIX.md`
