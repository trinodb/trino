# Ducklake Smoke Test

## Fast Path

```bash
cd plugin/trino-ducklake
mvn test
```

Expected: `BUILD SUCCESS`.

## Targeted Runs

```bash
# Catalog and metadata behavior
mvn test -Dtest=TestDucklakeCatalog

# Split pruning behavior
mvn test -Dtest=TestDucklakeSplitManager,TestDucklakePartitionPruning

# Page source and delete handling
mvn test -Dtest=TestDucklakePageSourceProvider,TestDucklakeDeleteFileHandling
```

## What These Tests Cover
- Catalog reads and type reconstruction from Ducklake metadata.
- Split planning and file-level stats pruning.
- Partition pruning.
- Parquet read path for primitive and nested types.
- Merge-on-read delete-file filtering.
- Basic plugin wiring.

## Current Limits of Smoke Coverage
- No query-runner integration tests.
- No non-empty dynamic filter behavior tests.
- No direct assertion tests for `getTableStatistics` output quality.

## Optional Packaging Check (Deferred)

```bash
mvn clean package -DskipTests
```

Note: root reactor wiring for `plugin/trino-ducklake` is still intentionally deferred.
