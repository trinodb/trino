# Suite Audit: SuiteStorageFormatsDetailed

## Suite Summary

- Baseline note: the detailed coverage notes below are retained from earlier audit work and must not be treated as final semantic findings until this suite is marked `complete` in `PROGRESS.md`.

- Purpose: JUnit suite for StorageFormatsDetailed coverage.
- Owning lane: `hive`
- Current suite class:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteStorageFormatsDetailed.java`
- CI bucket: `hive-storage`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `HiveStorageFormatsEnvironment`
- Include tags: `StorageFormatsDetailed`.
- Exclude tags: `HiveCompression`.
- Expected mapped classes covered: `TestHiveCompatibility`, `TestHiveStorageFormats`.
- Expected mapped methods covered: `22` method(s).

### Run 2

- Run name: `default`
- Environment: `HiveStorageFormatsEnvironment`
- Include tags: `HiveCompression`.
- Exclude tags: none.
- Expected mapped classes covered: `TestHiveCompression`.
- Expected mapped methods covered: `4` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `hive-storage`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `Remove legacy SuiteStorageFormatsDetailed`

## Parity Checklist

- Legacy suite or lane source: legacy launcher `SuiteStorageFormatsDetailed`.
- Current suite class: `SuiteStorageFormatsDetailed`
- Explicit runs and environments: verified from current suite source.
- Include tags: verified from current suite source.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `3`
- Expected migrated method count: `26`
- Expected migrated classes covered: `TestHiveCompatibility`, `TestHiveCompression`, `TestHiveStorageFormats`.
- Expected migrated methods covered: `TestHiveCompatibility.testInsertAllSupportedDataTypesWithTrino`,
  `TestHiveCompatibility.testSmallDecimalFieldWrittenByOptimizedParquetWriterCanBeReadByHive`,
  `TestHiveCompatibility.testTimestampFieldWrittenByOptimizedParquetWriterCanBeReadByHive`,
  `TestHiveCompatibility.testTrinoHiveParquetBloomFilterCompatibility`,
  `TestHiveCompression.testReadSequencefileWithLzo`, `TestHiveCompression.testReadTextfileWithLzop`,
  `TestHiveCompression.testSnappyCompressedParquetTableCreatedInHive`,
  `TestHiveCompression.testSnappyCompressedParquetTableCreatedInTrino`,
  `TestHiveStorageFormats.testCreatePartitionedTableAs`, `TestHiveStorageFormats.testCreateTableAs`,
  `TestHiveStorageFormats.testInsertAndSelectWithNullFormat`, `TestHiveStorageFormats.testInsertIntoPartitionedTable`,
  `TestHiveStorageFormats.testInsertIntoTable`, `TestHiveStorageFormats.testLargeOrcInsert`,
  `TestHiveStorageFormats.testLargeParquetInsert`, `TestHiveStorageFormats.testNestedFieldsWrittenByHive`,
  `TestHiveStorageFormats.testNestedFieldsWrittenByTrino`,
  `TestHiveStorageFormats.testOrcStructsWithNonLowercaseFields`, `TestHiveStorageFormats.testOrcTableCreatedInTrino`,
  `TestHiveStorageFormats.testSelectFromZeroByteFile`, `TestHiveStorageFormats.testSelectWithNullFormat`,
  `TestHiveStorageFormats.testStructTimestampsFromHive`, `TestHiveStorageFormats.testStructTimestampsFromTrino`,
  `TestHiveStorageFormats.testTimestampCreatedFromHive`, `TestHiveStorageFormats.testTimestampCreatedFromTrino`,
  `TestHiveStorageFormats.verifyDataProviderCompleteness`.
- Parity status: `verified`
