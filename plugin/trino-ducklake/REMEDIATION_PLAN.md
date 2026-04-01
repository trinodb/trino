# Ducklake Remediation Plan

Last updated: 2026-03-30

## Objective
Close correctness and parity gaps in the read-only connector while increasing reuse of public Iceberg/Trino components where it is practical and safe.

## Priorities

## P0: Correctness (must complete first)

### P0.1 Schema-evolution-safe reads — DONE
Fixed: Missing top-level Parquet columns now return null blocks via `TransformConnectorPageSource`.
Tests: `testSchemaEvolutionMissingColumnReturnsNulls`, `testSchemaEvolutionMixedColumns`.

### P0.2 Temporal partition transform semantics — DEFERRED
Problem:
Temporal transform handling in split pruning currently does not match the checked-in Ducklake spec semantics (spec says epoch-based encoding).

Status: **Deferred until Ducklake 1.0 release.**
Current implementation follows DuckDB ducklake extension behavior, which may differ from spec text.
The spec itself may change before the 1.0 release (expected within weeks).

Action for 1.0:
- Verify DuckDB ducklake 1.0 final transform encoding and reconcile with our implementation.
- If spec aligns with DuckDB behavior post-1.0, close this item.
- If spec diverges, decide whether to follow spec or DuckDB and document.

see reported issue: https://github.com/duckdb/ducklake-web/issues/312

## P1: Planner/Stats Quality

### P1.1 Typed aggregated min/max stats — DONE
Fixed: `getColumnStats()` now fetches per-file stats and aggregates in Java with type-aware comparison.
Supports numeric (int/long/double), date, and falls back to string for other types.
Tests: `testGetColumnStatsReturnsTypedMinMax`, `testGetTableStatisticsRowCountAndRanges`.

### P1.2 Dynamic-filter and table-statistics tests — DONE
Added tests for non-empty dynamic filters at page-source level and direct table-statistics verification.
Tests: `testDynamicFilterExcludesAllReturnsNoRows`, `testDynamicFilterIncludesAllReturnsAllRows`.

## P2: Reuse Alignment

### P2.1 Reuse review and extraction — DONE
Evaluated four areas: parquet field construction, delete handling, type conversion, split/metadata.
All custom code is intentionally separate — Iceberg abstractions don't align with Ducklake's simpler model.
Full rationale documented in [REUSE.md](REUSE.md).

## P3: Validation Depth

### P3.1 Query-runner integration tests
See [TESTING_UPGRADE.md](TESTING_UPGRADE.md) for the full testing upgrade plan.

Actions:
- Build `DucklakeQueryRunner` + `TestDucklakeConnectorTest` (Tier 1).
- Port DuckDB ducklake extension test data for cross-engine reads (Tier 2).
- Cover: partition pruning, delete filtering, nested reads, stats-driven planning, all types.

Done when:
- Query-runner suite is runnable in CI/dev and verifies core read-only behavior.

## Non-Goals (for this plan)
- Write operations (INSERT/UPDATE/DELETE/DDL).
- Full `variant` shredding.
- Full geometry semantics.
- Time travel semantics.

## Execution Order
1. P0.1 — DONE
2. P0.2 — DEFERRED (pending Ducklake 1.0)
3. P1.1 and P1.2 — DONE
4. P2.1 — DONE
5. P3.1

## Tracking
Use this file as the single remediation tracker. Keep `STATUS.md` as outcome summary only.
