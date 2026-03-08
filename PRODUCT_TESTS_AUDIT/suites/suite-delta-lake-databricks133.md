# Suite Audit: SuiteDeltaLakeDatabricks133

## Suite Summary

- Purpose: JUnit suite for DeltaLakeDatabricks133 coverage.
- Owning lane: `databricks`
- Current suite class:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteDeltaLakeDatabricks133.java`
- CI bucket: `databricks-133`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `DeltaLakeDatabricks133Environment`
- Include tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`.
- Exclude tags: none.
- Expected mapped classes covered: `TestDatabricksWithGlueMetastoreCleanUp`,
  `TestDeltaLakeCheckpointsDatabricksCompatibility`, `TestDeltaLakeCreateTableAsSelectCompatibilityDatabricks`,
  `TestDeltaLakeDatabricksCleanUpGlueMetastore`, `TestDeltaLakeDatabricksCreateTableCompatibility`,
  `TestDeltaLakeDeleteCompatibilityDatabricks`, `TestDeltaLakeIdentityColumnCompatibility`,
  `TestDeltaLakeInsertCompatibilityDatabricks`, `TestDeltaLakeUpdateCompatibility`,
  `TestDeltaLakeWriteDatabricksCompatibilityDatabricks`.
- Expected mapped methods covered: `44` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `databricks-133`
- Special secret/credential gate: `S3_BUCKET`, `AWS_REGION`, `TRINO_AWS_ACCESS_KEY_ID`, `TRINO_AWS_SECRET_ACCESS_KEY`,
  `DATABRICKS_TOKEN`, `DATABRICKS_133_JDBC_URL`
- Legacy launcher suite removed: `Remove legacy Databricks launcher suites`

## Parity Checklist

- Legacy suite or lane source: `databricks` lane and the corresponding legacy launcher coverage.
- Current suite class: `SuiteDeltaLakeDatabricks133`
- Explicit runs and environments: verified from current suite source.
- Include tags: verified from current suite source.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `10`
- Expected migrated method count: `44`
- Expected migrated classes covered: `TestDatabricksWithGlueMetastoreCleanUp`,
  `TestDeltaLakeCheckpointsDatabricksCompatibility`, `TestDeltaLakeCreateTableAsSelectCompatibilityDatabricks`,
  `TestDeltaLakeDatabricksCleanUpGlueMetastore`, `TestDeltaLakeDatabricksCreateTableCompatibility`,
  `TestDeltaLakeDeleteCompatibilityDatabricks`, `TestDeltaLakeIdentityColumnCompatibility`,
  `TestDeltaLakeInsertCompatibilityDatabricks`, `TestDeltaLakeUpdateCompatibility`,
  `TestDeltaLakeWriteDatabricksCompatibilityDatabricks`.
- Expected migrated methods covered: `44` method(s), spanning the shared Databricks-tagged methods recorded in the
  `delta-lake` and `databricks` lane files.
- Parity status: `verified`
