# Ducklake Connector - REVISED Implementation Plan

## Core Strategy: Leverage Trino Infrastructure

### What We REUSE (~70% of functionality)
- ✅ `ParquetPageSource` - Trino's Parquet reader (vectorization, bloom filters, etc.)
- ✅ `FixedSplitSource` - Standard split scheduling
- ✅ `DeleteManager` - Iceberg's delete file handling (adapt)
- ✅ `TupleDomainParquetPredicate` - Parquet-level pushdown

### What We BUILD (~30% new code)
- 🆕 SQL Catalog Layer (DONE ✓)
- 🆕 SQL-based file pruning via `ducklake_file_column_stats`
- 🆕 Type mapping (DONE ✓)
- 🔧 Delete file SQL integration

## Two-Layer Optimization

1. **SQL Metadata Pruning** (our value-add): Query stats to prune files
2. **Parquet-Level Pushdown** (FREE): Row groups, bloom filters, vectorization

## Implementation Steps

### Phase 2: PageSourceProvider (CURRENT - 2-3 hours)
```java
// Just wire up Trino's ParquetPageSource
ParquetPageSource.createParquetPageSource(
    inputFile,    // from TrinoFileSystem
    columns,      // our column mapping
    predicate,    // Trino handles pushdown
    parquetOptions // all the magic
);
```

### Phase 3: Delete Files (2-3 hours)
- Copy DeleteManager from Iceberg (~500 lines)
- Load delete files from SQL catalog
- Apply merge-on-read

### Phase 4: SQL-based File Pruning (2-3 hours)
- Query `ducklake_file_column_stats` for min/max
- Filter files before creating splits
- Benchmark improvement

## Next Actions
1. Create DucklakePageSourceProvider
2. Add missing dependencies to pom.xml
3. Test basic SELECT queries
