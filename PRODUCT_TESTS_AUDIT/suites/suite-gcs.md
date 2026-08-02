# Suite Audit: SuiteGcs

## Suite Summary

- Purpose: JUnit suite for Gcs coverage.
- Owning lane: `gcs`
- Current suite class: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteGcs.java`
- CI bucket: `cloud-object-store`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `GcsEnvironment`
- Include tags: `ConfiguredFeatures`, `DeltaLakeGcs`.
- Exclude tags: none.
- Expected mapped classes covered: `TestDeltaLakeGcs`.
- Expected mapped methods covered: `4` method(s).

### Run 2

- Run name: `default`
- Environment: `GcsEnvironment`
- Include tags: `ConfiguredFeatures`, `IcebergGcs`.
- Exclude tags: none.
- Expected mapped classes covered: `TestIcebergGcs`.
- Expected mapped methods covered: `5` method(s).

### Run 3

- Run name: `default`
- Environment: `GcsEnvironment`
- Include tags: `ConfiguredFeatures`, `HiveGcs`.
- Exclude tags: none.
- Expected mapped classes covered: `TestHiveGcs`.
- Expected mapped methods covered: `4` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `cloud-object-store`
- Special secret/credential gate: `GCP_CREDENTIALS_KEY`, `GCP_STORAGE_BUCKET`
- Legacy launcher suite removed: `Remove legacy SuiteGcs`

## Parity Checklist

- Legacy suite or lane source: `gcs` lane and the corresponding legacy launcher coverage.
- Current suite class: `SuiteGcs`
- Explicit runs and environments:
  - one `GcsEnvironment` run for `DeltaLakeGcs`
  - one `GcsEnvironment` run for `IcebergGcs`
  - one `GcsEnvironment` run for `HiveGcs`
- Include tags: `ConfiguredFeatures + DeltaLakeGcs`, `ConfiguredFeatures + IcebergGcs`, `ConfiguredFeatures + HiveGcs`
- Exclude tags: none
- Expected migrated class count: `3`
- Expected migrated method count: `13`
- Expected migrated classes covered: `TestDeltaLakeGcs`, `TestHiveGcs`, `TestIcebergGcs`.
- Expected migrated methods covered: `TestDeltaLakeGcs.testBasicWriteOperations`,
  `TestDeltaLakeGcs.testCreateAndSelectNationTable`, `TestDeltaLakeGcs.testLocationContainsDiscouragedCharacter`,
  `TestDeltaLakeGcs.testPathContainsSpecialCharacter`, `TestHiveGcs.testInsertTable`,
  `TestHiveGcs.testLocationContainsDiscouragedCharacter`, `TestHiveGcs.testPathContainsSpecialCharacter`,
  `TestHiveGcs.testSparkReadingTrinoData`, `TestIcebergGcs.testBasicWriteOperations`,
  `TestIcebergGcs.testCreateAndSelectNationTable`, `TestIcebergGcs.testLocationContainsDiscouragedCharacter`,
  `TestIcebergGcs.testPathContainsSpecialCharacter`, `TestIcebergGcs.testSparkReadingTrinoData`.
- Environment-run parity:
  - Legacy `SuiteGcs` used one `EnvMultinodeGcs` topology for all three runs.
  - Current suite preserves the same three-run structure on `GcsEnvironment`.
- Coverage-intent parity:
  - current suite still covers the same three GCS-backed families and the same 13 mapped methods.
- CI placement parity:
  - current placement in `cloud-object-store` is scheduling-only
- Any observed difference:
  - none beyond the framework replacement and the direct environment extraction
- Parity status: `verified`
