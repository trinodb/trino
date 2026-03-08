# Suite Audit: SuiteHiveTransactional

## Suite Summary

- Baseline note: the detailed coverage notes below are retained from earlier audit work and must not be treated as final semantic findings until this suite is marked `complete` in `PROGRESS.md`.

- Purpose: JUnit suite for HiveTransactional coverage.
- Owning lane: `hive`
- Current suite class:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteHiveTransactional.java`
- CI bucket: `hive-transactional`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `HiveTransactionalEnvironment`
- Include tags: `HiveTransactional`.
- Exclude tags: none.
- Expected mapped classes covered: `TestHiveMerge`, `TestHivePartitionProcedures`, `TestHiveTransactionalTableInsert`,
  `TestHiveTransactionalTable`, `TestWriteToHiveTransactionalTableInTrino`.
- Expected mapped methods covered: `69` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `hive-transactional`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `Remove legacy SuiteHiveTransactional`

## Parity Checklist

- Legacy suite or lane source: legacy launcher `SuiteHiveTransactional`.
- Current suite class: `SuiteHiveTransactional`
- Explicit runs and environments: verified from current suite source.
- Include tags: verified from current suite source.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `5`
- Expected migrated method count: `69`
- Expected migrated classes covered: `TestHiveMerge`, `TestHivePartitionProcedures`, `TestHiveTransactionalTable`,
  `TestHiveTransactionalTableInsert`, `TestWriteToHiveTransactionalTableInTrino`.
- Expected migrated methods covered: `TestHiveMerge.testMergeAllColumnsUpdated`, `TestHiveMerge.testMergeAllInserts`,
  `TestHiveMerge.testMergeAllMatchesDeleted`, `TestHiveMerge.testMergeCasts`,
  `TestHiveMerge.testMergeFailingPartitioning`, `TestHiveMerge.testMergeFailureWithDifferentPartitioning`,
  `TestHiveMerge.testMergeFalseJoinCondition`, `TestHiveMerge.testMergeMultipleOperationsBucketedUnpartitioned`,
  `TestHiveMerge.testMergeMultipleOperationsUnbucketedPartitioned`,
  `TestHiveMerge.testMergeMultipleOperationsUnbucketedUnpartitioned`, `TestHiveMerge.testMergeMultipleRowsMatchFails`,
  `TestHiveMerge.testMergeOriginalFilesTarget`, `TestHiveMerge.testMergeOverManySplits`,
  `TestHiveMerge.testMergeQueryWithStrangeCapitalization`, `TestHiveMerge.testMergeSimpleQuery`,
  `TestHiveMerge.testMergeSimpleQueryPartitioned`, `TestHiveMerge.testMergeSimpleSelect`,
  `TestHiveMerge.testMergeSimpleSelectPartitioned`, `TestHiveMerge.testMergeSubqueries`,
  `TestHiveMerge.testMergeUnBucketedUnPartitionedFailure`, `TestHiveMerge.testMergeUpdateWithVariousLayouts`,
  `TestHiveMerge.testMergeWithDifferentPartitioning`, `TestHiveMerge.testMergeWithSimplifiedUnpredictablePredicates`,
  `TestHiveMerge.testMergeWithUnpredictablePredicates`, `TestHiveMerge.testMergeWithoutTablesAliases`,
  `TestHivePartitionProcedures.testRegisterPartition`,
  `TestHivePartitionProcedures.testRegisterPartitionCollisionShouldFail`,
  `TestHivePartitionProcedures.testRegisterPartitionFromAnyLocation`,
  `TestHivePartitionProcedures.testRegisterPartitionInvalidLocationShouldFail`,
  `TestHivePartitionProcedures.testRegisterPartitionInvalidPartitionColumnsShouldFail` ....
- Parity status: `verified`
