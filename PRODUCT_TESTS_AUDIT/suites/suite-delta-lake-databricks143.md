# Suite Audit: SuiteDeltaLakeDatabricks143

## Suite Summary

- The current suite source comment claiming there are zero tagged tests is stale.
- Purpose: JUnit suite for DeltaLakeDatabricks143 coverage.
- Owning lane: `databricks`
- Current suite class:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteDeltaLakeDatabricks143.java`
- CI bucket: `databricks-143`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `DeltaLakeDatabricks143Environment`
- Include tags: `ConfiguredFeatures`, `DeltaLakeDatabricks143`.
- Exclude tags: none.
- Expected mapped classes covered: `TestDeltaLakeCheckpointsDatabricksCompatibility`,
  `TestDeltaLakeCreateTableAsSelectCompatibilityDatabricks`, `TestDeltaLakeInsertCompatibilityDatabricks`.
- Expected mapped methods covered: `4` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `databricks-143`
- Special secret/credential gate: `S3_BUCKET`, `AWS_REGION`, `TRINO_AWS_ACCESS_KEY_ID`, `TRINO_AWS_SECRET_ACCESS_KEY`,
  `DATABRICKS_TOKEN`, `DATABRICKS_143_JDBC_URL`
- Legacy launcher suite removed: `Remove legacy Databricks launcher suites`

## Parity Checklist

- Legacy suite or lane source: `databricks` lane and the corresponding legacy launcher coverage.
- Current suite class: `SuiteDeltaLakeDatabricks143`
- Explicit runs and environments: verified from current suite source.
- Include tags: verified from current suite source.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `3`
- Expected migrated method count: `4`
- Expected migrated classes covered: `TestDeltaLakeCheckpointsDatabricksCompatibility`,
  `TestDeltaLakeCreateTableAsSelectCompatibilityDatabricks`, `TestDeltaLakeInsertCompatibilityDatabricks`.
- Expected migrated methods covered: `TestDeltaLakeCheckpointsDatabricksCompatibility.testDatabricksUsesCheckpointInterval`,
  `TestDeltaLakeCheckpointsDatabricksCompatibility.testTrinoCheckpointMinMaxStatisticsForRowType`,
  `TestDeltaLakeCreateTableAsSelectCompatibilityDatabricks.testTrinoTypesWithDatabricks`,
  `TestDeltaLakeInsertCompatibilityDatabricks.testPartitionedInsertCompatibility`.
- Parity status: `verified`
