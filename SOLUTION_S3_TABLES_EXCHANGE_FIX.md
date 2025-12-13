# Fix for S3 Tables Catalog Conflicts with Exchange Manager S3 Operations (Issue #26419)

## Problem Summary

When using S3 Tables as the Iceberg catalog with Trino's fault tolerance enabled (exchange manager using S3), the exchange manager's S3 operations fail due to endpoint configuration conflicts.

**Affected Scenario:**
- **Catalog**: S3 Tables (for Iceberg table storage)
- **Exchange Manager**: Regular S3 bucket (for fault tolerance temp data)
- **Result**: Fault tolerance execution fails with S3 API errors

## Root Cause

The issue stemmed from the exchange manager's S3 client configuration in `S3FileSystemExchangeStorage.java`. The original implementation used `AwsClientEndpointProvider` to dynamically construct S3 endpoints when no explicit endpoint was configured:

```java
.endpointOverride(endpoint.map(URI::create).orElseGet(() -> AwsClientEndpointProvider.builder()
    .serviceEndpointPrefix("s3")
    .defaultProtocol("http")
    .region(region.orElseThrow(() -> new IllegalArgumentException("region is expected to be set")))
    .build()
    .clientEndpoint()));
```

This approach was problematic because:

1. **Dynamic Endpoint Resolution**: The `AwsClientEndpointProvider` could be influenced by AWS SDK internal state or configuration from other components
2. **S3 Tables Interference**: When Iceberg catalog operations interact with S3 Tables buckets (identified by the `--table-s3` suffix pattern), they may affect AWS SDK's endpoint resolution behavior
3. **Lack of Explicit Isolation**: The exchange manager's S3 client wasn't explicitly isolated from catalog-level S3 configurations

## Solution

The fix modifies `S3FileSystemExchangeStorage.createS3AsyncClient()` to ensure complete isolation between catalog and exchange S3 clients:

### Key Changes

1. **Removed Dynamic Endpoint Provider**: Eliminated the fallback to `AwsClientEndpointProvider`
2. **Explicit Region Configuration**: Region is now set explicitly on the client builder
3. **Optional Endpoint Override**: Endpoint is only overridden when explicitly configured
4. **AWS SDK Default Behavior**: When no endpoint is specified, the AWS SDK uses its built-in regional endpoint resolution based on the configured region

### Code Changes

**File**: `plugin/trino-exchange-filesystem/src/main/java/io/trino/plugin/exchange/filesystem/s3/S3FileSystemExchangeStorage.java`

**Before**:
```java
.endpointOverride(endpoint.map(URI::create).orElseGet(() -> AwsClientEndpointProvider.builder()
    .serviceEndpointPrefix("s3")
    .defaultProtocol("http")
    .region(region.orElseThrow(() -> new IllegalArgumentException("region is expected to be set")))
    .build()
    .clientEndpoint()));

region.ifPresent(clientBuilder::region);
```

**After**:
```java
// Explicitly set region to ensure proper S3 endpoint resolution
// This is critical when using Trino with S3 Tables catalogs to prevent
// endpoint configuration conflicts between catalog operations (which may use
// S3 Tables endpoints) and exchange manager operations (which must use
// standard S3 endpoints)
region.ifPresent(clientBuilder::region);

// Only override endpoint if explicitly configured
// When not set, the SDK will use the standard regional S3 endpoint
// based on the configured region, ensuring isolation from any
// S3 Tables-specific endpoint configurations
endpoint.map(URI::create).ifPresent(clientBuilder::endpointOverride);
```

## How It Works

### Isolation Architecture

```
┌─────────────────────────┐    S3 Tables/Glue APIs    ┌──────────────────┐
│ Iceberg Catalog         │ ────────────────────────► │ S3 Tables        │
│ (S3FileSystemFactory)   │                            │ (catalog data)   │
└─────────────────────────┘                            └──────────────────┘
         ↑
         │ Isolated S3Client instances
         │
┌─────────────────────────┐    Standard S3 APIs       ┌──────────────────┐
│ Exchange Manager        │ ────────────────────────► │ Regular S3       │
│ (S3FileSystemExchange)  │                            │ (temp FT data)   │
└─────────────────────────┘                            └──────────────────┘
```

### Configuration Requirements

The fix maintains the existing validation that either `exchange.s3.region` or `exchange.s3.endpoint` must be configured (enforced by `ExchangeS3Config.isEndpointOrRegionSet()`).

**Configuration Option 1 - Using Region (Recommended)**:
```properties
# Catalog configuration (S3 Tables)
catalog.type=iceberg
iceberg.rest-catalog.uri=https://glue.us-east-1.amazonaws.com/iceberg
iceberg.rest-catalog.warehouse=s3tablescatalog/my-s3tables-bucket
s3.region=us-east-1

# Exchange Manager configuration (Regular S3)
exchange.filesystem.type=s3
exchange.s3.bucket=my-exchange-bucket
exchange.s3.region=us-east-1  # ← AWS SDK will use standard S3 endpoint for this region
```

**Configuration Option 2 - Using Explicit Endpoint**:
```properties
# Exchange Manager with custom endpoint
exchange.s3.endpoint=https://s3.us-east-1.amazonaws.com
```

## Benefits

1. **Complete Isolation**: Exchange manager S3 operations are completely isolated from catalog S3 operations
2. **Standard S3 Endpoints**: Exchange manager always uses standard regional S3 endpoints, never S3 Tables endpoints
3. **Explicit Configuration**: Endpoint resolution is now explicit and predictable
4. **Backward Compatible**: Existing configurations continue to work without changes
5. **Future-Proof**: Reduces dependency on AWS SDK internal endpoint resolution behavior

## Testing

To verify the fix works correctly:

1. **Setup S3 Tables Catalog**:
   ```properties
   catalog.type=iceberg
   iceberg.rest-catalog.uri=https://glue.<region>.amazonaws.com/iceberg
   iceberg.rest-catalog.warehouse=s3tablescatalog/<s3tables-bucket>
   iceberg.rest-catalog.security=sigv4
   s3.region=<region>
   ```

2. **Setup Exchange Manager with Fault Tolerance**:
   ```properties
   exchange.filesystem.type=s3
   exchange.s3.bucket=<regular-s3-bucket>
   exchange.s3.region=<region>
   ```

3. **Run Query with Fault Tolerance**:
   ```sql
   SET SESSION task_max_writer_count = 1;  -- Force exchange
   SELECT * FROM large_table ORDER BY column;
   ```

4. **Verify**:
   - Query completes successfully
   - No S3 API errors in logs
   - Exchange data written to regular S3 bucket
   - Table data stored in S3 Tables bucket

## Related Files

- **Modified**: `plugin/trino-exchange-filesystem/src/main/java/io/trino/plugin/exchange/filesystem/s3/S3FileSystemExchangeStorage.java`
- **Related**: `plugin/trino-exchange-filesystem/src/main/java/io/trino/plugin/exchange/filesystem/s3/ExchangeS3Config.java`
- **Related**: `lib/trino-filesystem-s3/src/main/java/io/trino/filesystem/s3/S3FileSystemFactory.java`
- **Related**: `lib/trino-filesystem/src/main/java/io/trino/filesystem/Locations.java` (S3 Tables detection)

## Implementation Notes

- The fix leverages AWS SDK v2's built-in regional endpoint resolution
- Each S3AsyncClient instance is independently configured
- No global state or shared configuration between catalog and exchange
- S3 Tables bucket detection (via `--table-s3` pattern) is only used by Iceberg, not the exchange manager

## Migration Path

No migration required - this is a bug fix that maintains full backward compatibility with existing configurations.
