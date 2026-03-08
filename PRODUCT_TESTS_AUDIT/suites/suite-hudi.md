# Suite Audit: SuiteHudi

## Suite Summary

- Manual review note: this suite was compared directly against legacy launcher `SuiteHudi` and the current `SuiteHudi` source after the lane-level method/environment audit was completed.

- Purpose: JUnit suite for Hudi coverage.
- Owning lane: `hudi`
- Current suite class: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteHudi.java`
- CI bucket: `hive-kerberos`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `HudiEnvironment`
- Include tags: `Hudi`.
- Exclude tags: none.
- Expected mapped classes covered: `TestHudiSparkCompatibility`.
- Expected mapped methods covered: `11` method(s).

### Run 2

- Run name: `default`
- Environment: `HiveHudiRedirectionsEnvironment`
- Include tags: `HiveHudiRedirections`.
- Exclude tags: none.
- Expected mapped classes covered: `TestHiveRedirectionToHudi`.
- Expected mapped methods covered: `17` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `hive-kerberos`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `Remove legacy SuiteHudi`

## Parity Checklist

- Legacy suite or lane source: `hudi` lane and the corresponding legacy launcher coverage.
- Current suite class: `SuiteHudi`
- Explicit runs and environments: verified from current suite source.
- Include tags: verified from current suite source.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `2`
- Expected migrated method count: `28`
- Expected migrated classes covered: `TestHiveRedirectionToHudi`, `TestHudiSparkCompatibility`.
- Expected migrated methods covered: `TestHiveRedirectionToHudi.testDelete`, `TestHiveRedirectionToHudi.testDeny`,
  `TestHiveRedirectionToHudi.testDescribe`, `TestHiveRedirectionToHudi.testDropTable`,
  `TestHiveRedirectionToHudi.testGrant`, `TestHiveRedirectionToHudi.testInsert`, `TestHiveRedirectionToHudi.testMerge`,
  `TestHiveRedirectionToHudi.testRedirect`, `TestHiveRedirectionToHudi.testRedirectPartitionsToPartitioned`,
  `TestHiveRedirectionToHudi.testRedirectPartitionsToUnpartitioned`,
  `TestHiveRedirectionToHudi.testRedirectToNonexistentCatalog`,
  `TestHiveRedirectionToHudi.testRedirectWithDefaultSchemaInSession`,
  `TestHiveRedirectionToHudi.testRedirectWithNonDefaultSchema`, `TestHiveRedirectionToHudi.testRevoke`,
  `TestHiveRedirectionToHudi.testSetTableAuthorization`, `TestHiveRedirectionToHudi.testShowGrants`,
  `TestHiveRedirectionToHudi.testUpdate`, `TestHudiSparkCompatibility.testCopyOnWritePartitionedTableSelect`,
  `TestHudiSparkCompatibility.testCopyOnWriteShowCreateTable`, `TestHudiSparkCompatibility.testCopyOnWriteTableSelect`,
  `TestHudiSparkCompatibility.testCopyOnWriteTableSelectAfterUpdate`,
  `TestHudiSparkCompatibility.testCopyOnWriteTableSelectWithSessionProperties`,
  `TestHudiSparkCompatibility.testMergeOnReadPartitionedTableSelect`,
  `TestHudiSparkCompatibility.testMergeOnReadTableSelect`,
  `TestHudiSparkCompatibility.testMergeOnReadTableSelectAfterUpdate`,
  `TestHudiSparkCompatibility.testReadCopyOnWriteTableWithReplaceCommits`,
  `TestHudiSparkCompatibility.testTimelineTable`, `TestHudiSparkCompatibility.testTimelineTableRedirect`.
- Parity status: `verified`
- Manual comparison result: legacy and current suite shapes match semantically. Both execute one Hudi interoperability run and one Hive-to-Hudi redirection run; the current suite only differs by using direct JUnit `SuiteRunner` execution instead of launcher test runs.
