# Product Test Audit

This directory contains the working lane-by-lane and suite-by-suite fidelity audit for the JUnit product test migration.

## Purpose

This directory contains the detailed lane-by-lane and suite-by-suite fidelity audit backing the review of this branch.

## Completion Rule

- A lane, environment, or suite is `complete` only if the relevant legacy and current source bodies were manually read
  and compared.
- Structural evidence alone does not count as completion:
    - path existence
    - method-name matching
    - prior audit prose
    - CI success
- Lanes and suites marked `not started` in `PROGRESS.md` may still contain older baseline notes below; those notes are
  retained as raw material and are not final semantic findings until the tracker marks them complete.
- Older baseline notes must not be promoted to `complete` after only a revalidation, tracker cleanup, or suite/env spot
  check. Final completion requires a fresh one-method-at-a-time source-body pass for the relevant lane.

## Audit status model

- `verified`: source/history review found no material unexplained drift.
- `intentional difference`: the reviewed difference is explained by the approved migration-change set or by deliberate
  SQL-file consolidation/new JUnit-side verification coverage.
- `needs follow-up`: the current source/history review found a difference or mapping issue that is not yet clearly
  justified.

## Approved intentional differences

- `HDP to Hive 3.1 migration`
- `Ignite upgrade`
- `JUnit/AssertJ/Testcontainers framework replacement`
- `Kerberos deployment cleanup without intended coverage change`
- `MySQL upgrade`
- `new JUnit-side environment verification coverage`
- `outside-Docker execution`

## Notes

- The final lane-by-lane and suite-by-suite semantic pass is complete.
- Hive is complete in the final pass.
- Iceberg is complete in the final pass.
- Delta Lake is complete in the final pass.
- Databricks is complete in the final pass.
- All previously recorded fidelity gaps have been addressed on this branch.
- Every observed difference is recorded, even if minor and non-actionable.
- Lane files are the primary artifact. Suite files are derived from the lane findings plus current suite/CI source.
- Unless a lane entry says otherwise, assume the legacy test class moved from the documented legacy path to the
  documented current path.
- Legacy source coverage is not limited to Java methods. SQL-file-backed and resource-backed tests are treated as
  first-class legacy sources and should be documented as such in the owning migration audit entry.
- Resource cleanup commits are not treated as proof of porting. If a legacy SQL/resource-backed test was migrated, the
  owning migration commit should account for it explicitly; later cleanup only removes obsolete copies.
- `TestSqlCancel` remains an intentional unscheduled retirement, not a missing port.
- Current retired-method and non-gap summary is tracked in this README and the lane notes.
- A fresh suite-selection audit on `2026-03-09` found no current suite/environment mismatches.
- A follow-up class-inventory pass on `2026-03-09` confirmed that every concrete current `@ProductTest` class is now
  represented in the lane audits, including current-only environment verification classes.
- The same revalidation confirmed two categories of intentionally unscheduled current code:
    - current-only JUnit environment verification classes that are not routed by any suite
    - `TestHiveTableStatistics`, which is unscheduled in both the legacy Hive4 launcher lane and the current JUnit tree
- A mechanical current-target validation pass on `2026-03-09` found no stale `Current target method` path/method
  entries in the lane audits.

## Stop Failure Analysis

- The explicit stop rules were already strict, but the docs still left a loophole: rich older baseline lane prose could
  be mistaken for already-finished final-pass work.
- Words like `revalidated` and `compared the remaining suite and environment source` made it sound acceptable to close a
  lane after partial verification instead of after a fresh full method pass.
- The tracker had no explicit pre-stop checklist proving that each remaining lane had been reread end to end in the
  current pass.
- The false-complete state therefore came from misapplying existing notes, not from any actual instruction that
  checkpoints or partial work were acceptable.

## Remaining Final-Pass Task List

1. None. The Iceberg and Delta Lake method passes were completed in the final source-body reread, and the tracker/lane
   docs now reflect that finished state.

## SQL / Resource Coverage Notes

- Old SQL-file-backed connector cases are explicitly tracked in the lane audits, not inferred from later cleanup.
- Confirmed lane patterns:
    - `mysql-mariadb`: legacy connector SQL files under `src/main/resources/sql-tests/testcases/connectors/mysql/**` are
      mapped one-by-one to inline JUnit methods in `TestMySqlSqlTests`.
    - `postgresql`: legacy connector SQL files under `src/main/resources/sql-tests/testcases/connectors/postgresql/**`
      are mapped one-by-one to inherited JUnit methods in `BasePostgresqlSqlTests`, exercised by
      `TestPostgresqlSqlTests`.
    - `parquet`: legacy query resources removed in `Migrate TestParquet to JUnit` are accounted for in
      `TestParquetJunit`; specific absorbed files are documented in the Parquet lane entry.
- The later cleanup/removal of the old `src/main/resources/sql-tests/**` tree is not treated as a migration event by
  itself.

## Indexes

- [Lane Index](lanes/INDEX.md)
- [Suite Index](suites/INDEX.md)
