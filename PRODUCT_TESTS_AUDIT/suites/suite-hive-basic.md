# Suite Audit: SuiteHiveBasic

## Suite Summary

- Baseline note: the detailed coverage notes below are retained from earlier audit work and must not be treated as final semantic findings until this suite is marked `complete` in `PROGRESS.md`.

- Purpose: JUnit suite for HiveBasic coverage.
- Owning lane: `hive`
- Current suite class: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteHiveBasic.java`
- CI bucket: `hive-basic`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `HiveBasicEnvironment`
- Include tags: `HdfsNoImpersonation`.
- Exclude tags: none.
- Expected mapped classes covered: `TestImpersonationNoImpersonationJunit`, `TestCreateDropSchema`,
  `TestCsvFileHiveTable`, `TestExternalHiveTable`, `TestHdfsSyncPartitionMetadata`, `TestHiveBasicTableStatistics`,
  `TestHiveBucketedTables`, `TestHiveHiddenFiles`, `TestHiveIgnoreAbsentPartitions`, `TestHivePartitionSchemaEvolution`,
  `TestHivePartitionsTable`, `TestHivePropertiesTable`, `TestHiveRequireQueryPartitionsFilter`, `TestHiveSchema`,
  `TestHiveViews`, `TestHiveViewsLegacy`, `TestInsertIntoHiveTable`, `TestTablePartitioningSelect`,
  `TestTablePartitioningWithSpecialChars`, `TestTextFileHiveTable`.
- Expected mapped methods covered: `153` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `hive-basic`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `No dedicated legacy launcher suite identified`

## Parity Checklist

- Legacy suite or lane source: the `HDFS_NO_IMPERSONATION` Hive run embedded in legacy `Suite2`.
- Current suite class: `SuiteHiveBasic`
- Explicit runs and environments: verified from current suite source.
- Include tags: verified from current suite source.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `20`
- Expected migrated method count: `153`
- Expected migrated classes covered: `TestCreateDropSchema`, `TestCsvFileHiveTable`, `TestExternalHiveTable`,
  `TestHdfsSyncPartitionMetadata`, `TestHiveBasicTableStatistics`, `TestHiveBucketedTables`, `TestHiveHiddenFiles`,
  `TestHiveIgnoreAbsentPartitions`, `TestHivePartitionSchemaEvolution`, `TestHivePartitionsTable`,
  `TestHivePropertiesTable`, `TestHiveRequireQueryPartitionsFilter`, `TestHiveSchema`, `TestHiveViews`,
  `TestHiveViewsLegacy`, `TestImpersonationNoImpersonationJunit`, `TestInsertIntoHiveTable`,
  `TestTablePartitioningSelect`, `TestTablePartitioningWithSpecialChars`, `TestTextFileHiveTable`.
- Expected migrated methods covered: `TestCreateDropSchema.testCreateDropSchema`,
  `TestCreateDropSchema.testDropSchemaFiles`, `TestCreateDropSchema.testDropSchemaFilesTransactions`,
  `TestCreateDropSchema.testDropSchemaFilesWithEmptyExternalSubdir`,
  `TestCreateDropSchema.testDropSchemaFilesWithLocation`, `TestCreateDropSchema.testDropTransactionsWithExternalFiles`,
  `TestCreateDropSchema.testDropWithExternalFilesInSubdirectory`,
  `TestCsvFileHiveTable.testCreateCsvFileTableAsSelectSkipHeaderFooter`,
  `TestExternalHiveTable.testAnalyzeExternalTable`,
  `TestExternalHiveTable.testCreateExternalTableWithInaccessibleSchemaLocation`,
  `TestExternalHiveTable.testDeleteFromExternalPartitionedTableTable`,
  `TestExternalHiveTable.testDeleteFromExternalTable`, `TestExternalHiveTable.testInsertIntoExternalTable`,
  `TestExternalHiveTable.testShowStatisticsForExternalTable`,
  `TestHdfsSyncPartitionMetadata.testAddNonConventionalHivePartition`, `TestHdfsSyncPartitionMetadata.testAddPartition`,
  `TestHdfsSyncPartitionMetadata.testAddPartitionContainingCharactersThatNeedUrlEncoding`,
  `TestHdfsSyncPartitionMetadata.testConflictingMixedCasePartitionNames`,
  `TestHdfsSyncPartitionMetadata.testDropPartition`,
  `TestHdfsSyncPartitionMetadata.testDropPartitionContainingCharactersThatNeedUrlEncoding`,
  `TestHdfsSyncPartitionMetadata.testFullSyncPartition`, `TestHdfsSyncPartitionMetadata.testInvalidSyncMode`,
  `TestHdfsSyncPartitionMetadata.testMixedCasePartitionNames`,
  `TestHdfsSyncPartitionMetadata.testSyncPartitionMetadataWithNullArgument`,
  `TestHiveBasicTableStatistics.testAnalyzePartitioned`, `TestHiveBasicTableStatistics.testAnalyzeUnpartitioned`,
  `TestHiveBasicTableStatistics.testCreateExternalUnpartitioned`, `TestHiveBasicTableStatistics.testCreatePartitioned`,
  `TestHiveBasicTableStatistics.testCreateTableWithNoData`, `TestHiveBasicTableStatistics.testCreateUnpartitioned` ....
- Parity status: `verified`
- Recorded differences:
  - Legacy coverage ran inside the broader launcher `EnvMultinode` aggregate run; current suite isolates the same tag-driven Hive-basic coverage on `HiveBasicEnvironment`.
