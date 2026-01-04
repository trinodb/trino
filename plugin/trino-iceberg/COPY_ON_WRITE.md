# Trino Iceberg Connector - Copy-on-Write Support

## Overview

The Trino Iceberg connector now supports both **Copy-on-Write (CoW)** and **Merge-on-Read (MoR)** strategies for row-level operations (DELETE, UPDATE, and MERGE). This enables users to optimize performance based on their specific workload patterns.

| Mode | Best For | Advantages | Trade-offs |
|------|----------|------------|------------|
| **Copy-on-Write** | Read-heavy workloads, dimension tables | Fast reads (no merge overhead) | Slower writes (file rewriting) |
| **Merge-on-Read** | Write-heavy workloads, streaming data | Fast writes (small delete files) | Slower reads (merge at query time) |

## Quick Start

### Enable Copy-on-Write

```sql
-- Enable CoW for specific operations (table-level)
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

### Usage Examples

```sql
-- Delete with CoW (rewrites affected files)
DELETE FROM events WHERE event_time < TIMESTAMP '2023-01-01';

-- Update with CoW (rewrites affected files with updated rows)
UPDATE events SET data = 'updated' WHERE id = 123;

-- Merge with CoW (rewrites affected files with merged data)
MERGE INTO events t
USING new_events s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET data = s.data
WHEN NOT MATCHED THEN INSERT (id, event_time, data) VALUES (s.id, s.event_time, s.data);
```

## Configuration

### Table Properties

| Property | Description | Default | Values |
|----------|-------------|---------|--------|
| `write_delete_mode` | Controls DELETE behavior | `merge-on-read` | `merge-on-read`, `copy-on-write` |
| `write_update_mode` | Controls UPDATE behavior | `merge-on-read` | `merge-on-read`, `copy-on-write` |
| `write_merge_mode` | Controls MERGE behavior | `merge-on-read` | `merge-on-read`, `copy-on-write` |

**Note:** Requires Iceberg format version 2 or higher.

## Performance Considerations

### When to Choose Copy-on-Write

- Read-heavy analytical workloads
- Dimension tables with infrequent updates
- When read performance is prioritized over write performance
- When storage cost is not a primary concern

### Performance Tuning

1. **Compact small files** before CoW operations:
   ```sql
   CALL system.rewrite_data_files('schema.table')
   ```

2. **Use partitioning** to limit rewrite scope to affected partitions

3. **Batch operations** to minimize file rewrites:
   ```sql
   -- Better: One batch update
   UPDATE events SET data = 'updated' WHERE id IN (1, 2, 3, 4, 5);
   
   -- Worse: Multiple small updates
   UPDATE events SET data = 'updated' WHERE id = 1;
   UPDATE events SET data = 'updated' WHERE id = 2;
   -- etc.
   ```

4. **Monitor file counts** using system tables:
   ```sql
   SELECT * FROM "schema"."table$files";
   ```

## Implementation Architecture

CoW operations rewrite entire data files, excluding deleted rows or including updated rows:

1. **Detection**: Identifies DELETE, UPDATE, or MERGE operations with CoW mode
2. **Manifest Scan**: Locates affected data files efficiently
3. **Position Delete Processing**: Tracks deleted row positions in bitmap
4. **File Rewriting**: Reads original files, filters out deleted rows, writes new files
5. **Atomic Commit**: Uses Iceberg's RewriteFiles API for consistent state

## Current Limitations

1. **Equality Deletes**: Only position deletes supported (value-based deletes not yet implemented)
2. **Format Version**: Requires Iceberg format v2 or higher
3. **File-Level Rewrites**: Entire files rewritten, not row groups
4. **Manifest Scanning**: Linear scan for file lookup (no caching/indexing)

## Troubleshooting

### Common Issues

- **"Could not find X file(s) to delete in table manifests"**  
  Cause: Concurrent modification or file cleanup  
  Solution: Retry operation (Iceberg handles conflicts automatically)

- **Slow CoW operations**  
  Cause: Large files or many small files  
  Solution: Run `OPTIMIZE` before CoW operations, increase worker resources

- **High write amplification**  
  Expected behavior: CoW rewrites entire files  
  Solution: Use MoR for write-heavy workloads, batch operations

## References

- **Iceberg Spec**: [Row-level Deletes](https://iceberg.apache.org/spec/#row-level-deletes)
- **Related Issues**: Fixes: #26161