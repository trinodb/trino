# Suite Audit: SuiteHiveSpark

## Suite Summary

- Baseline note: the detailed coverage notes below are retained from earlier audit work and must not be treated as final semantic findings until this suite is marked `complete` in `PROGRESS.md`.

- Purpose: JUnit suite for HiveSpark coverage.
- Owning lane: `hive`
- Current suite class: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteHiveSpark.java`
- CI bucket: `hive-storage`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `HiveSparkEnvironment`
- Include tags: `HiveSpark`.
- Exclude tags: none.
- Expected mapped classes covered: `TestHiveSparkCompatibility`, `TestHiveViewCompatibility`,
  `TestParquetFileWithReorderedColumns`.
- Expected mapped methods covered: `22` method(s).

### Run 2

- Run name: `default`
- Environment: `HiveSparkNoStatsFallbackEnvironment`
- Include tags: `HiveSparkNoStatsFallback`.
- Exclude tags: none.
- Expected mapped classes covered: none from the audited product-test classes.
- Expected mapped methods covered: `0` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `hive-storage`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `No dedicated legacy launcher suite identified`

## Parity Checklist

- Legacy suite or lane source: the legacy Hive/Spark compatibility coverage, including the no-stats-fallback sub-run.
- Current suite class: `SuiteHiveSpark`
- Explicit runs and environments: verified from current suite source.
- Include tags: verified from current suite source.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `3`
- Expected migrated method count: `22`
- Expected migrated classes covered: `TestHiveSparkCompatibility`, `TestHiveViewCompatibility`,
  `TestParquetFileWithReorderedColumns`.
- Expected migrated methods covered: `TestHiveSparkCompatibility.testDeleteFailsOnBucketedTableCreatedBySpark`,
  `TestHiveSparkCompatibility.testIgnoringSparkStatisticsWithDisabledFallback`,
  `TestHiveSparkCompatibility.testInsertFailsOnBucketedTableCreatedBySpark`,
  `TestHiveSparkCompatibility.testReadSparkBucketedTable`, `TestHiveSparkCompatibility.testReadSparkCreatedTable`,
  `TestHiveSparkCompatibility.testReadSparkDateAndTimePartitionName`,
  `TestHiveSparkCompatibility.testReadSparkInvalidDatePartitionName`,
  `TestHiveSparkCompatibility.testReadSparkStatisticsPartitionedTable`,
  `TestHiveSparkCompatibility.testReadSparkStatisticsUnpartitionedTable`,
  `TestHiveSparkCompatibility.testReadTrinoCreatedOrcTable`,
  `TestHiveSparkCompatibility.testReadTrinoCreatedParquetTable`,
  `TestHiveSparkCompatibility.testSparkClusteringCaseSensitiveCompatibility`,
  `TestHiveSparkCompatibility.testSparkParquetBloomFilterCompatibility`,
  `TestHiveSparkCompatibility.testSparkParquetTimestampCompatibility`,
  `TestHiveSparkCompatibility.testTextInputFormatWithParquetHiveSerDe`,
  `TestHiveSparkCompatibility.testTrinoSparkParquetBloomFilterCompatibility`,
  `TestHiveSparkCompatibility.testUpdateFailsOnBucketedTableCreatedBySpark`,
  `TestHiveViewCompatibility.testCommentOnViewColumn`, `TestHiveViewCompatibility.testExistingView`,
  `TestHiveViewCompatibility.testSelectOnView`, `TestHiveViewCompatibility.testSelectOnViewFromDifferentSchema`,
  `TestParquetFileWithReorderedColumns.testReadParquetFileWithReorderedColumns`.
- Parity status: `verified`
- Recorded differences:
  - Current suite makes the Spark and no-stats-fallback runs explicit instead of routing both through a broader launcher environment definition.
