# Suite Audit: SuiteFaultTolerant

## Suite Summary

- Baseline note: the detailed coverage notes below are retained from earlier audit work and must not be treated as final semantic findings until this suite is marked `complete` in `PROGRESS.md`.

- Purpose: JUnit suite for FaultTolerant coverage.
- Owning lane: `fault-tolerant`
- Current suite class: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteFaultTolerant.java`
- CI bucket: `hive-transactional`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `FaultTolerantEnvironment`
- Include tags: `FaultTolerant`.
- Exclude tags: none.
- Expected mapped classes covered: `TestTaskRetriesFilesystemSmoke`.
- Expected mapped methods covered: `1` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `hive-transactional`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `No dedicated legacy launcher suite existed`

## Parity Checklist

- Legacy suite or lane source: legacy `FAULT_TOLERANT`-tagged coverage from `TestTaskRetriesFilesystemSmoke`; no dedicated legacy launcher suite existed.
- Current suite class: `SuiteFaultTolerant`
- Explicit runs and environments: verified from current suite source.
- Include tags: verified from current suite source.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `1`
- Expected migrated method count: `1`
- Expected migrated classes covered: `TestTaskRetriesFilesystemSmoke`.
- Expected migrated methods covered: `TestTaskRetriesFilesystemSmoke.testSimpleQuery`.
- Parity status: `verified`
- Recorded differences:
  - Current suite is an explicit dedicated JUnit suite for legacy tag-driven coverage.
  - Current suite uses explicit `FaultTolerantEnvironment` runtime wiring that was implicit in the old execution model.
