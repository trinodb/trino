# Suite Audit: SuiteHive4

## Suite Summary

- Manual review note: this suite was compared directly against legacy launcher `SuiteHive4` and the current `SuiteHive4` source after the lane-level method/environment audit was completed.

- Purpose: JUnit suite for Hive4 coverage.
- Owning lane: `hive4`
- Current suite class: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteHive4.java`
- CI bucket: `hive-kerberos`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `MultinodeHive4Environment`
- Include tags: `Hive4`.
- Exclude tags: none.
- Expected mapped classes covered: `TestHiveOnOrcLegacyDateCompatibility`.
- Expected mapped methods covered: `2` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `hive-kerberos`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `Remove legacy SuiteHive4`

## Parity Checklist

- Legacy suite or lane source: `hive4` lane and the corresponding legacy launcher coverage.
- Current suite class: `SuiteHive4`
- Explicit runs and environments: verified from current suite source.
- Include tags: verified from current suite source.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `1`
- Expected migrated method count: `2`
- Expected migrated classes covered: `TestHiveOnOrcLegacyDateCompatibility`.
- Expected migrated methods covered: `TestHiveOnOrcLegacyDateCompatibility.testReadLegacyDateFromOrcWrittenByHive`,
  `TestHiveOnOrcLegacyDateCompatibility.testReadLegacyDateFromOrcWrittenByTrino`.
- Parity status: `verified`
- Manual comparison result: legacy and current suite shapes match semantically for migrated coverage. Both execute a single Hive4 run on the multinode Hive4 environment family; the current lane also contains current-only environment verification tests that remain outside suite routing by design.
