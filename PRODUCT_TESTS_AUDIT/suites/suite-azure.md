# Suite Audit: SuiteAzure

## Suite Summary

- Manual review note: this suite was compared directly against legacy launcher `SuiteAzure` and the current `SuiteAzure` source after the lane-level method/environment audit was completed.

- Purpose: JUnit suite for Azure coverage.
- Owning lane: `azure`
- Current suite class: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteAzure.java`
- CI bucket: `cloud-object-store`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `AzureEnvironment`
- Include tags: `ConfiguredFeatures`, `Azure`.
- Exclude tags: none.
- Expected mapped classes covered: `TestHiveAzure`, `TestAzureBlobFileSystem`, `TestAbfsSyncPartitionMetadata`.
- Expected mapped methods covered: `13` method(s).

### Run 2

- Run name: `default`
- Environment: `AzureEnvironment`
- Include tags: `ConfiguredFeatures`, `DeltaLakeAzure`.
- Exclude tags: none.
- Expected mapped classes covered: `TestDeltaLakeAzure`.
- Expected mapped methods covered: `3` method(s).

### Run 3

- Run name: `default`
- Environment: `AzureEnvironment`
- Include tags: `ConfiguredFeatures`, `IcebergAzure`.
- Exclude tags: none.
- Expected mapped classes covered: `TestIcebergAzure`.
- Expected mapped methods covered: `4` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `cloud-object-store`
- Special secret/credential gate: `ABFS_CONTAINER`, `ABFS_ACCOUNT`, `ABFS_ACCESS_KEY`
- Legacy launcher suite removed: `Remove legacy SuiteAzure`

## Parity Checklist

- Legacy suite or lane source: `azure` lane and the corresponding legacy launcher coverage.
- Current suite class: `SuiteAzure`
- Explicit runs and environments: verified from current suite source.
- Include tags: verified from current suite source.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `5`
- Expected migrated method count: `20`
- Expected migrated classes covered: `TestAbfsSyncPartitionMetadata`, `TestAzureBlobFileSystem`, `TestDeltaLakeAzure`,
  `TestHiveAzure`, `TestIcebergAzure`.
- Expected migrated methods covered: `TestAbfsSyncPartitionMetadata.testAddPartition`,
  `TestAbfsSyncPartitionMetadata.testAddPartitionContainingCharactersThatNeedUrlEncoding`,
  `TestAbfsSyncPartitionMetadata.testConflictingMixedCasePartitionNames`,
  `TestAbfsSyncPartitionMetadata.testDropPartition`,
  `TestAbfsSyncPartitionMetadata.testDropPartitionContainingCharactersThatNeedUrlEncoding`,
  `TestAbfsSyncPartitionMetadata.testFullSyncPartition`, `TestAbfsSyncPartitionMetadata.testInvalidSyncMode`,
  `TestAbfsSyncPartitionMetadata.testMixedCasePartitionNames`,
  `TestAbfsSyncPartitionMetadata.testSyncPartitionMetadataWithNullArgument`,
  `TestAzureBlobFileSystem.testPathContainsSpecialCharacter`, `TestDeltaLakeAzure.testBasicWriteOperations`,
  `TestDeltaLakeAzure.testCreateAndSelectNationTable`, `TestDeltaLakeAzure.testPathContainsSpecialCharacter`,
  `TestHiveAzure.testInsertTable`, `TestHiveAzure.testPathContainsSpecialCharacter`,
  `TestHiveAzure.testSparkReadingTrinoData`, `TestIcebergAzure.testBasicWriteOperations`,
  `TestIcebergAzure.testCreateAndSelectNationTable`, `TestIcebergAzure.testPathContainsSpecialCharacter`,
  `TestIcebergAzure.testSparkReadingTrinoData`.
- Parity status: `verified`
- Manual comparison result: legacy and current suite shapes match semantically. Both execute three Azure runs on the same effective environment family and the same include-tag slices; the current suite only differs by using direct JUnit `SuiteRunner` calls instead of launcher test runs.
