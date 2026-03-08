# Suite Audit: SuiteDeltaLakeOss

## Suite Summary

- Purpose: JUnit suite for DeltaLakeOss coverage.
- Owning lane: `delta-lake`
- Current suite class: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteDeltaLakeOss.java`
- CI bucket: `delta-lake`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `DeltaLakeMinioEnvironment`
- Include tags: `DeltaLakeMinio`.
- Exclude tags: none.
- Expected mapped classes covered: `TestDeltaLakeActiveFilesCache`, `TestDeltaLakeAlterTableCompatibility`,
  `TestDeltaLakeCaseInsensitiveMapping`, `TestDeltaLakeChangeDataFeedCompatibility`,
  `TestDeltaLakeCheckConstraintCompatibility`, `TestDeltaLakeCheckpointsCompatibility`,
  `TestDeltaLakeCloneTableCompatibility`, `TestDeltaLakeColumnMappingMode`,
  `TestDeltaLakeCreateOrReplaceTableAsSelectCompatibility`, `TestDeltaLakeCreateTableAsSelectCompatibility`,
  `TestDeltaLakeDeleteCompatibility`, `TestDeltaLakeDropTableCompatibility`, `TestDeltaLakeInsertCompatibility`,
  `TestDeltaLakeJmx`, `TestDeltaLakePartitioningCompatibility`, `TestDeltaLakeProceduresCompatibility`,
  `TestDeltaLakeSelectCompatibility`, `TestDeltaLakeSystemTableCompatibility`, `TestDeltaLakeTimeTravelCompatibility`,
  `TestDeltaLakeTransactionLogCache`, `TestDeltaLakeWriteDatabricksCompatibility`,
  `TestDeltaLakeDatabricksMinioReads`, `TestDeltaLakeOssDeltaLakeMinioReads`.
- Expected mapped methods covered: `147` method(s).

### Run 2

- Run name: `default`
- Environment: `DeltaLakeOssEnvironment`
- Include tags: `DeltaLakeHdfs`.
- Exclude tags: none.
- Expected mapped classes covered: `TestDeltaLakeDatabricksHdfsReads`, `TestDeltaLakeOssDeltaLakeHdfsReads`.
- Expected mapped methods covered: `2` method(s).

### Run 3

- Run name: `default`
- Environment: `HiveDeltaLakeMinioEnvironment`
- Include tags: `DeltaLakeOss`.
- Exclude tags: none.
- Expected mapped classes covered: `TestHiveAndDeltaLakeRedirect`.
- Expected mapped methods covered: `30` method(s).

### Run 4

- Run name: `default`
- Environment: `DeltaLakeMinioCachingEnvironment`
- Include tags: `DeltaLakeAlluxioCaching`.
- Exclude tags: none.
- Expected mapped classes covered: `TestDeltaLakeAlluxioCaching`.
- Expected mapped methods covered: `1` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `delta-lake`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `Remove legacy SuiteDeltaLakeOss`
- Later extended in lane(s): `delta-lake`.

## Parity Checklist

- Legacy suite or lane source: `delta-lake` lane and the corresponding legacy launcher coverage.
- Current suite class: `SuiteDeltaLakeOss`
- Explicit runs and environments: verified from current suite source.
- Include tags: verified from current suite source.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `27`
- Expected migrated method count: `180`
- Expected migrated classes covered: `TestDeltaLakeDatabricksHdfsReads`, `TestDeltaLakeDatabricksMinioReads`,
  `TestDeltaLakeOssDeltaLakeHdfsReads`, `TestDeltaLakeOssDeltaLakeMinioReads`,
  `TestDeltaLakeActiveFilesCache`, `TestDeltaLakeAlluxioCaching`, `TestDeltaLakeAlterTableCompatibility`,
  `TestDeltaLakeCaseInsensitiveMapping`, `TestDeltaLakeChangeDataFeedCompatibility`,
  `TestDeltaLakeCheckConstraintCompatibility`, `TestDeltaLakeCheckpointsCompatibility`,
  `TestDeltaLakeCloneTableCompatibility`, `TestDeltaLakeColumnMappingMode`,
  `TestDeltaLakeCreateOrReplaceTableAsSelectCompatibility`, `TestDeltaLakeCreateTableAsSelectCompatibility`,
  `TestDeltaLakeDeleteCompatibility`, `TestDeltaLakeDropTableCompatibility`, `TestDeltaLakeInsertCompatibility`,
  `TestDeltaLakeJmx`, `TestDeltaLakePartitioningCompatibility`, `TestDeltaLakeProceduresCompatibility`,
  `TestDeltaLakeSelectCompatibility`, `TestDeltaLakeSystemTableCompatibility`, `TestDeltaLakeTimeTravelCompatibility`,
  `TestDeltaLakeTransactionLogCache`, `TestDeltaLakeWriteDatabricksCompatibility`, `TestHiveAndDeltaLakeRedirect`.
- Expected migrated methods covered: `TestDeltaLakeDatabricksHdfsReads.testReads`,
  `TestDeltaLakeDatabricksMinioReads.testReadRegionTable`, `TestDeltaLakeOssDeltaLakeHdfsReads.testReads`,
  `TestDeltaLakeOssDeltaLakeMinioReads.testReadRegionTable`,
  `TestDeltaLakeActiveFilesCache.testRefreshTheFilesCacheWhenTableIsRecreated`,
  `TestDeltaLakeAlluxioCaching.testReadFromCache`,
  `TestDeltaLakeAlterTableCompatibility.testAddColumnWithCommentOnTrino`,
  `TestDeltaLakeAlterTableCompatibility.testCommentOnColumn`, `TestDeltaLakeAlterTableCompatibility.testCommentOnTable`,
  `TestDeltaLakeAlterTableCompatibility.testDropNotNullConstraint`,
  `TestDeltaLakeAlterTableCompatibility.testRenameColumn`,
  `TestDeltaLakeAlterTableCompatibility.testRenamePartitionedColumn`,
  `TestDeltaLakeAlterTableCompatibility.testTrinoPreservesReaderAndWriterVersions`,
  `TestDeltaLakeAlterTableCompatibility.testTrinoPreservesTableFeature`,
  `TestDeltaLakeAlterTableCompatibility.testTypeWideningInteger`,
  `TestDeltaLakeCaseInsensitiveMapping.testColumnCommentWithNonLowerCaseColumnName`,
  `TestDeltaLakeCaseInsensitiveMapping.testNonLowercaseColumnNames`,
  `TestDeltaLakeCaseInsensitiveMapping.testNonLowercaseFieldNames`,
  `TestDeltaLakeCaseInsensitiveMapping.testNotNullColumnWithNonLowerCaseColumnName`,
  `TestDeltaLakeChangeDataFeedCompatibility.testDeleteFromNullPartitionWithCdfEnabled`,
  `TestDeltaLakeChangeDataFeedCompatibility.testDeleteFromTableWithCdf`,
  `TestDeltaLakeChangeDataFeedCompatibility.testDeltaCanReadCdfEntriesGeneratedByTrino`,
  `TestDeltaLakeChangeDataFeedCompatibility.testMergeDeleteIntoTableWithCdfEnabled`,
  `TestDeltaLakeChangeDataFeedCompatibility.testMergeMixedDeleteAndUpdateIntoTableWithCdfEnabled`,
  `TestDeltaLakeChangeDataFeedCompatibility.testMergeUpdateIntoTableWithCdfEnabled`,
  `TestDeltaLakeChangeDataFeedCompatibility.testThatCdfDoesntWorkWhenPropertyIsNotSet`,
  `TestDeltaLakeChangeDataFeedCompatibility.testTrinoCanReadCdfEntriesGeneratedByDelta`,
  `TestDeltaLakeChangeDataFeedCompatibility.testTurningOnAndOffCdfFromTrino`,
  `TestDeltaLakeChangeDataFeedCompatibility.testUpdateCdfTableWithNonLowercaseColumn`,
  `TestDeltaLakeChangeDataFeedCompatibility.testUpdatePartitionedTableCdfEnabledAndPartitioningColumnUpdated`,
  `TestDeltaLakeChangeDataFeedCompatibility.testUpdatePartitionedTableWithCdf`,
  `TestDeltaLakeChangeDataFeedCompatibility.testUpdatePartitionedTableWithManyRowsInsertedInTheSameRequestAndCdfEnabled` ....
- Parity status: `verified`
- Reviewer note: the current suite executes the inherited HDFS/Minio read coverage through the concrete
  `TestDeltaLakeDatabricks...` and `TestDeltaLakeOss...` subclasses; the shared implementations remain audited in the
  lane under `BaseTestDeltaLakeMinioReadsJunit` and `BaseTestDeltaLakeHdfsReadsJunit`.
