# Suite Audit: SuiteParquet

## Suite Summary

- Purpose: JUnit suite for Parquet coverage.
- Owning lane: `parquet`
- Current suite class: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteParquet.java`
- CI bucket: `hive-storage`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `ParquetEnvironment`
- Include tags: `ConfiguredFeatures`, `Parquet`.
- Exclude tags: none.
- Expected mapped classes covered: `TestParquetJunit`.
- Expected mapped methods covered: `2` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `hive-storage`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `Remove legacy SuiteParquet`

## Parity Checklist

- Legacy suite or lane source: `parquet` lane and the corresponding legacy launcher coverage.
- Current suite class: `SuiteParquet`
- Explicit runs and environments:
  - one run on `ParquetEnvironment`
- Include tags: `ConfiguredFeatures`, `Parquet`
- Exclude tags: none
- Expected migrated class count: `1`
- Expected migrated method count: `2`
- Expected migrated classes covered: `TestParquetJunit`.
- Expected migrated methods covered: `TestParquetJunit.testTpcds`, `TestParquetJunit.testTpch`.
- Environment-run parity:
  - Legacy `SuiteParquet` ran one `EnvMultinodeParquet` job with the same effective tag pair.
  - Current suite preserves that as one explicit `ParquetEnvironment` run.
- Coverage-intent parity:
  - current suite still targets the same SQL-backed TPCDS and TPCH Parquet query corpus
- CI placement parity:
  - current placement in `hive-storage` is scheduling-only
- Any observed difference:
  - none beyond the framework replacement and the code-driven environment extraction
- Parity status: `verified`
