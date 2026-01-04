# Trino Iceberg Connector - Copy-on-Write Support

## Overview

The Trino Iceberg connector supports both **Merge-on-Read (MoR)** and **Copy-on-Write (CoW)** strategies for DELETE, UPDATE, and MERGE operations, allowing users to optimize for their specific workload patterns.

## Quick Start

### Enable Copy-on-Write

```sql
-- Enable CoW for specific operations
ALTER TABLE my_table SET PROPERTIES write_delete_mode = 'copy-on-write';
ALTER TABLE my_table SET PROPERTIES write_update_mode = 'copy-on-write';
ALTER TABLE my_table SET PROPERTIES write_merge_mode = 'copy-on-write';

-- Create table with CoW enabled
CREATE TABLE events (
  id BIGINT,
  event_time TIMESTAMP,
  data VARCHAR
)
WITH (
  format_version = 2,  -- Required for row-level operations
  write_delete_mode = 'copy-on-write'
);
```

### When to Use Each Mode

**Copy-on-Write**: Best for read-heavy workloads, batch updates, dimension tables
- Fast reads (no merge overhead)
- Slower writes (file rewriting)

**Merge-on-Read**: Best for write-heavy workloads, streaming updates, sparse changes
- Fast writes (small delete files)
- Slower reads (merge at query time)

## Implementation Architecture

### Core Components

#### 1. Enums and Configuration

**`UpdateMode`** - Defines write strategies:
- `MERGE_ON_READ`: Default, creates delete files
- `COPY_ON_WRITE`: Rewrites data files

**`UpdateKind`** - Tracks operation type:
- `DELETE`, `UPDATE`, `MERGE`

**Table Properties**:
- `write_delete_mode`: Controls DELETE behavior (default: `merge-on-read`)
- `write_update_mode`: Controls UPDATE behavior (default: `merge-on-read`)
- `write_merge_mode`: Controls MERGE behavior (default: `merge-on-read`)

#### 2. Query Planning and Routing

**`IcebergMetadata.applyDelete()`**:
- Routes DELETE operations based on write mode
- MoR: Returns metadata-only execution plan (`executeDelete()`)
- CoW: Returns empty to force full write path (`finishWrite()`)

**`IcebergMetadata.beginMerge()`**:
- Sets `UpdateKind.MERGE` in table handle
- Enables write mode selection for MERGE operations

**`IcebergTableHandle`**:
- Tracks `UpdateKind` throughout query lifecycle
- Preserves operation type across transformations

#### 3. Copy-on-Write Implementation

**`IcebergMetadata.finishWrite()`** - Main CoW orchestration:

```java
// Detection logic
if (updateMode == UpdateMode.COPY_ON_WRITE && 
    (detectedUpdateKind == UpdateKind.DELETE || 
     detectedUpdateKind == UpdateKind.UPDATE || 
     detectedUpdateKind == UpdateKind.MERGE))
{
    // Track position delete files
    // Store new data files for rewriting
    // Delegate to rewriteDataFilesForCowDelete()
}
```

**`IcebergMetadata.rewriteDataFilesForCowDelete()`** - File rewriting logic:

1. **Load manifests**: Scan current snapshot's manifest files to locate DataFile objects
2. **Build file map**: Create path-to-DataFile mapping for efficient lookup
3. **Process each data file**:
   - Read associated delete files
   - Load position deletes into `Roaring64Bitmap`
   - Open original data file via page source
   - Filter out deleted rows page-by-page
   - Write filtered data to new file
   - Preserve partition data and metrics
4. **Atomic commit**: Use `RewriteFiles` API to swap old files for new files

**`IcebergMetadata.readPositionDeletesFromDeleteFile()`** - Delete file reading:
- Opens delete file with `file_path` and `pos` columns
- Filters for target data file using tuple domain
- Populates `Roaring64Bitmap` with deleted positions
- Uses same implementation as query execution for consistency

### Position Delete Handling

**Key Implementation Details**:

1. **Row positions are 0-based** within each data file (Iceberg standard)
2. **Bitmap storage**: Uses `Roaring64Bitmap` for memory-efficient position tracking
3. **Page-level filtering**: Processes data in pages, filtering deleted positions
4. **Position tracking**: Maintains current row position across all pages

```java
// Example from rewriteDataFilesForCowDelete
long currentRowPosition = 0;  // 0-based
while (page = pageSource.getNextPage()) {
    // Build array of non-deleted positions
    for (int i = 0; i < positions; i++) {
        if (!deletedPositions.contains(currentRowPosition + i)) {
            positionArray[outputCount++] = i;
        }
    }
    // Filter page and write to new file
    currentRowPosition += positions;
}
```

### Safety and Error Handling

**Resource Management**:
- Explicit rollback lifecycle for file writers
- Try-catch-finally blocks ensure cleanup
- Suppressed exceptions preserve original error context

**Validation**:
- Snapshot isolation using Iceberg's optimistic concurrency
- Null checks for empty tables
- Missing file validation before commit
- Format version checks (requires v2)

**Error Messages**:
- All `IOException` wrapped in `TrinoException`
- File paths included for debuggability
- Clear error messages for common issues

### Performance Considerations

**Manifest Reading Optimization**:
- Reads manifests directly instead of scanning all data files
- O(manifest_count) vs O(data_file_count)
- Early termination when all target files found

**Known Performance Trade-offs**:
- Linear scan through manifests for file lookup (no indexing)
- Entire files rewritten even for small changes
- Temporary storage equal to affected file size
- Higher write amplification vs MoR

## Testing

Comprehensive test suite with **25 test methods** across 3 classes:

### `TestIcebergCopyOnWriteOperations` (19 tests, 952 lines)
- Basic operations: DELETE, UPDATE, MERGE
- Edge cases: empty tables, format v1
- Large batches: 1,000-5,000 row operations
- Performance benchmarks and comparisons
- Partitioned table operations

### `TestIcebergCopyOnWriteIntegration` (5 tests, 642 lines)
- Resource cleanup on failure
- Concurrent operations
- Snapshot isolation
- Conflict detection and retry
- Logging verification

### `TestIcebergCopyOnWriteDeleteOperations` (1 test, 264 lines)
- Low-level delete operation testing

**Test Coverage Areas**:
- Basic CRUD operations with CoW
- Partitioning and partition pruning
- Format version compatibility
- Scale testing (large batches)
- Concurrency and isolation
- Error handling and cleanup
- Performance comparisons (CoW vs MoR)

## Limitations

### Current Limitations

1. **Equality Deletes**: Only position deletes supported (value-based deletes not yet implemented)
2. **Format Version**: Requires Iceberg format v2 or higher
3. **File-Level Rewrites**: Entire files rewritten, not row groups
4. **Manifest Scanning**: Linear scan for file lookup (no caching/indexing)

### Future Enhancements

- Equality delete support for CoW mode
- Incremental CoW (row group level rewrites)
- Parallel file rewriting for large operations
- Manifest caching for batch operations
- Adaptive mode switching based on workload patterns

## Troubleshooting

### Common Issues

**"Could not find X file(s) to delete in table manifests"**
- Cause: Concurrent modification or file cleanup
- Solution: Retry operation (Iceberg handles conflicts automatically)

**Slow CoW operations**
- Cause: Large files or many small files
- Solution: Run `OPTIMIZE` before CoW operations, increase worker resources

**High write amplification**
- Expected behavior: CoW rewrites entire files
- Solution: Use MoR for write-heavy workloads, batch operations

### Performance Tuning

1. **Compact small files**: `CALL system.rewrite_data_files('schema.table')`
2. **Use partitioning**: Limits rewrite scope to affected partitions
3. **Batch operations**: Group multiple updates together
4. **Monitor file counts**: Check `table$files` system table

## Design Decisions

### User-Specified vs Automatic Mode Selection

This implementation uses **explicit user configuration** rather than automatic heuristics:

**Rationale**:
1. **Predictability**: Operations have consistent, known performance characteristics
2. **Debuggability**: Mode is self-documenting via table properties
3. **Business context**: Users understand workload priorities better than heuristics
4. **Simplicity**: Cleaner implementation, easier to maintain and test
5. **Evolution path**: Can add optional automatic mode in future informed by real usage

**Industry Precedent**: Apache Iceberg, Delta Lake, PostgreSQL, MySQL all use explicit configuration for performance-critical trade-offs.

## References

- **Iceberg Spec**: [Row-level Deletes](https://iceberg.apache.org/spec/#row-level-deletes)
- **Iceberg API**: `RewriteFiles` for atomic file swaps
- **RoaringBitmap**: Memory-efficient bitmap for position tracking

## Related Issues

- Fixes: #26161
- Design considerations: #17272
