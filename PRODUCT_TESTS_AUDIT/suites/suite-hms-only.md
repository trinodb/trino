# Suite Audit: SuiteHmsOnly

## Suite Summary

- Baseline note: the detailed coverage notes below are retained from earlier audit work and must not be treated as final semantic findings until this suite is marked `complete` in `PROGRESS.md`.

- Purpose: JUnit suite for HmsOnly coverage.
- Owning lane: `hive`
- Current suite class: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteHmsOnly.java`
- CI bucket: `hive-basic`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `HiveStorageFormatsEnvironment`
- Include tags: `HmsOnly`.
- Exclude tags: none.
- Expected mapped classes covered: `TestCsv`, `TestHiveStorageFormats`.
- Expected mapped methods covered: `28` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `hive-basic`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `Remove legacy SuiteHmsOnly`

## Parity Checklist

- Legacy suite or lane source: legacy launcher `SuiteHmsOnly`.
- Current suite class: `SuiteHmsOnly`
- Explicit runs and environments: verified from current suite source.
- Include tags: verified from current suite source.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `2`
- Expected migrated method count: `28`
- Expected migrated classes covered: `TestCsv`, `TestHiveStorageFormats`.
- Expected migrated methods covered: `TestCsv.testCreateCsvTableAs`, `TestCsv.testCreateCsvTableAsWithCustomProperties`,
  `TestCsv.testCreatePartitionedCsvTableAs`, `TestCsv.testCreatePartitionedCsvTableAsWithCustomParamters`,
  `TestCsv.testInsertIntoCsvTable`, `TestCsv.testInsertIntoCsvTableWithCustomProperties`,
  `TestCsv.testInsertIntoPartitionedCsvTable`, `TestCsv.testInsertIntoPartitionedCsvTableWithCustomProperties`,
  `TestCsv.testReadCsvTableWithMultiCharProperties`, `TestCsv.testWriteCsvTableWithMultiCharProperties`,
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
