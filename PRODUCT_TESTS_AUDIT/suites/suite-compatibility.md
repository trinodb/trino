# Suite Audit: SuiteCompatibility

## Suite Summary

- Purpose: JUnit suite for Compatibility coverage.
- Owning lane: `compatibility`
- Current suite class: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteCompatibility.java`
- CI bucket: `delta-lake`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `HiveSparkEnvironment`
- Include tags: `HiveViewCompatibility`.
- Exclude tags: none.
- Expected mapped classes covered: `TestHiveViewCompatibility`.
- Expected mapped methods covered: `4` method(s).

### Run 2

- Run name: `default`
- Environment: `SparkIcebergCompatibilityEnvironment`
- Include tags: `IcebergFormatVersionCompatibility`.
- Exclude tags: none.
- Expected mapped classes covered: `TestIcebergFormatVersionCompatibility`.
- Expected mapped methods covered: `1` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `delta-lake`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `Remove legacy SuiteCompatibility`

## Parity Checklist

- Legacy suite or lane source: `compatibility` lane and the corresponding legacy launcher coverage.
- Current suite class: `SuiteCompatibility`
- Explicit runs and environments:
  - `HiveSparkEnvironment` + `HiveViewCompatibility`
  - `SparkIcebergCompatibilityEnvironment` + `IcebergFormatVersionCompatibility`
- Include tags: `HiveViewCompatibility`, `IcebergFormatVersionCompatibility`
- Exclude tags: none
- Expected migrated class count: `2`
- Expected migrated method count: `5`
- Expected migrated classes covered: `TestHiveViewCompatibility`, `TestIcebergFormatVersionCompatibility`.
- Expected migrated methods covered: `TestHiveViewCompatibility.testCommentOnViewColumn`,
  `TestHiveViewCompatibility.testExistingView`, `TestHiveViewCompatibility.testSelectOnView`,
  `TestHiveViewCompatibility.testSelectOnViewFromDifferentSchema`,
  `TestIcebergFormatVersionCompatibility.testTrinoTimeTravelReadTableCreatedByEarlyVersionTrino`.
- Environment-run parity:
  - Legacy compatibility coverage was split between Hive view compatibility and Iceberg format-version compatibility.
  - Current suite preserves that split as two explicit runs.
- Coverage-intent parity:
  - `TestIcebergFormatVersionCompatibility` remains semantically faithful.
  - `TestHiveViewCompatibility` keeps the same four test methods and again compares current Trino against the
    compatibility Trino executor.
- CI placement parity:
  - Current placement in `delta-lake` is scheduling-only.
- Any observed difference:
  - Current compatibility coverage is wired through explicit environment helpers instead of the old launcher executor
    abstraction.
- Parity status: `verified`
