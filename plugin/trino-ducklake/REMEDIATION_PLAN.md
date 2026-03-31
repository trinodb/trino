# Ducklake Remediation Plan

Last updated: 2026-03-30

## Objective
Close correctness and parity gaps in the read-only connector while increasing reuse of public Iceberg/Trino components where it is practical and safe.

## Priorities

## P0: Correctness (must complete first)

### P0.1 Schema-evolution-safe reads
Problem:
Current page-source creation throws when a requested top-level column is absent in a Parquet file.

Actions:
- Return null-producing fields/pages for missing top-level columns instead of failing.
- Preserve behavior for nested fields where parent exists but child is missing.
- Add regression tests with mixed-schema files in one table snapshot.

Done when:
- Querying columns added after older files were written returns nulls for old files.
- No false failures for valid schema evolution scenarios.

### P0.2 Temporal partition transform semantics
Problem:
Temporal transform handling in split pruning currently does not match the checked-in Ducklake spec semantics.

Actions:
- Align transform encoding/decoding for `year`, `month`, `day`, `hour` with agreed spec semantics.
- If implementation intentionally follows DuckDB producer behavior that differs from spec text, document and gate it explicitly.
- Add focused tests that cover boundary and wraparound scenarios.

Done when:
- Enforced predicate classification and partition pruning are both semantically correct.
- Test cases prove no false-positive pruning.

## P1: Planner/Stats Quality

### P1.1 Typed aggregated min/max stats
Problem:
Aggregated `min_value`/`max_value` are currently derived with string min/max ordering.

Actions:
- Compute typed min/max safely (query or post-processing strategy).
- Keep null/invalid parsing behavior conservative (never prune incorrectly).
- Add tests for numeric/date columns showing correct aggregate ranges.

Done when:
- `getTableStatistics` ranges are type-correct for numeric/date types.

### P1.2 Dynamic-filter and table-statistics tests
Problem:
Dynamic filters are only exercised with `DynamicFilter.EMPTY`, and table-statistic output has no direct tests.

Actions:
- Add tests with non-empty dynamic filters that prune data at read time.
- Add direct tests for `getTableStatistics` row count, null fraction, and ranges.

Done when:
- Both behaviors are covered by deterministic unit tests.

## P2: Reuse Alignment

### P2.1 Reuse review and extraction
Problem:
Some code currently custom where Iceberg public utilities exist.

Actions:
- Evaluate replacing or wrapping custom delete logic with Iceberg public delete helpers where model alignment is strong.
- Evaluate replacing or sharing parquet field conversion helpers with Iceberg public converter or shared extracted utility.
- If direct reuse is not viable, document why in `REUSE.md` with explicit constraints.

Done when:
- Reuse decision is explicit per subsystem (adopted / intentionally custom + reason).

## P3: Validation Depth

### P3.1 Query-runner integration tests
Actions:
- Add query-runner tests for end-to-end planner + execution behavior.
- Cover: partition pruning, delete filtering, nested reads, and stats-driven planning.

Done when:
- Query-runner suite is runnable in CI/dev and verifies core read-only behavior.

## Non-Goals (for this plan)
- Write operations (INSERT/UPDATE/DELETE/DDL).
- Full `variant` shredding.
- Full geometry semantics.
- Time travel semantics.

## Execution Order
1. P0.1
2. P0.2
3. P1.1 and P1.2
4. P2.1
5. P3.1

## Tracking
Use this file as the single remediation tracker. Keep `STATUS.md` as outcome summary only.
