# Product Tests Reviewer Guide

This file is the main entry point for reviewing the product-test migration.

## Purpose

This branch ports the product test framework and product test suites from the legacy TestNG/Tempto model to JUnit 5
plus Testcontainers.

The primary goal is fidelity to the original product test coverage and intent. Review should assume that behavior is
meant to stay the same unless a change is called out here or in a lane audit entry.

## Main review material

- Audit overview and indexes:
  - `PRODUCT_TESTS_AUDIT/README.md`
  - `PRODUCT_TESTS_AUDIT/PROGRESS.md`
  - `PRODUCT_TESTS_AUDIT/lanes/INDEX.md`
  - `PRODUCT_TESTS_AUDIT/suites/INDEX.md`
- Product-test framework and developer docs:
  - `testing/trino-product-tests/README.md`
  - `testing/trino-product-tests/RUN_PRODUCT_TESTS.md`
  - `testing/trino-product-tests/DEVELOPER_GUIDE.md`

## Intentional framework changes

These changes are intentional and are the main structural differences from the original code:

- Product tests now run outside Docker instead of being deployed into a test container.
- The new framework uses JUnit 5, AssertJ, and Testcontainers.
- The legacy flaky retry behavior was reimplemented as a JUnit 5 extension.
- There is no special product-test launcher anymore.
- Developers run product tests directly in the IDE.
- CI executes suite classes via their `main` method.
- MySQL was upgraded.
- Ignite was upgraded.
- Hive was migrated from HDP to Apache Hive 3.1. HDP is fully removed.
- Kerberos deployment was cleaned up, but not structurally changed from a test-coverage perspective.

## Intended non-changes

Outside of the approved differences above, the branch is intended to preserve the original product test behavior and
coverage:

- The same product-test lanes should still exist.
- Migrated test classes should preserve the original test intent and method coverage.
- Environments should preserve the legacy environment behavior, except where required by the approved differences listed
  above.
- CI ownership should move from the legacy launcher/TestNG path to the new JUnit suite path without silently dropping
  coverage.

## Commit structure

The history is deliberately structured to be reviewer-friendly and mostly lane-local.

1. Framework commits
   - `Add JUnit 5 Flaky test retry extension`
   - `Add JUnit product test infrastructure`
2. Repeated lane sections
   - `Add <Lane> environment(s)`
   - `Add <Lane> suite(s)`
   - `Migrate <TestClass> to JUnit`
   - `Remove legacy <Suite...>`
3. Terminal cleanup tranche
   - helper/resource moves
   - legacy launcher and Tempto/TestNG removal
   - final mechanical cleanup
4. Terminal docs commit(s)
   - audit and migration-end documentation

## What each commit type means

- `Add <Lane> environment(s)`
  - Introduces the new JUnit/Testcontainers environment classes needed for that lane.
- `Add <Lane> suite(s)`
  - Introduces the JUnit suite class for that lane and wires it into the JUnit CI path.
- `Migrate <TestClass> to JUnit`
  - Ports one legacy product test class at a time.
  - The migrated class lands in the same commit that removes the legacy class.
- `Remove legacy <Suite...>`
  - Deletes the old launcher suite for that lane and unwires it from the legacy CI path.

## Framework invariants reviewers should expect

- Concrete `@ProductTest` classes are required to declare `@RequiresEnvironment(...)` directly or through an abstract
  superclass.
- Abstract base test classes are allowed to defer the concrete environment to subclasses.
- Test methods receive the environment via JUnit parameter injection rather than through Tempto services or in-container
  execution.
- Environment/test class hierarchy compatibility is validated up front. If a test method expects a more specific
  environment type than the class declares, framework validation fails early.
- Product tests are intentionally single-threaded. Parallel execution is not supported.
- Suite `main` methods print a consolidated summary at the end instead of mixing failures into the middle of the logs.
- Environment-owned cleanup runs through framework hooks, so reusable cleanup should live in environments rather than in
  repeated `@BeforeEach`/`@AfterEach` blocks.
- Suite tag selection is explicit and important: multiple included tags are combined with logical `AND`, and excluded
  tags are subtracted from that result.

## Flaky behavior

- The legacy flaky retry behavior is intentionally preserved.
- Retry is only enabled on CI.
- Retry happens only when the thrown stack trace matches the configured pattern.
- Maximum retries are capped at 2, for 3 total attempts.
- The `@Flaky` annotation is intentionally marked deprecated to signal that it is a temporary escape hatch, not desired
  steady state.

## Terminal cleanup tranche

The series ends with a small cleanup tranche that is intentionally mechanical:

1. `Retire unscheduled TestSqlCancel`
2. `Move shared product-test helpers to src/test`
3. `Remove legacy launcher and Tempto product-test framework`
4. `Move remaining resources and utility classes into test package`
5. terminal docs commits

These commits are not intended to introduce new product-test behavior. Their purpose is to finish the transition to the
JUnit-only end state by:

- deleting the legacy launcher module, launcher script, and Tempto/TestNG-era runtime path,
- removing obsolete launcher CI wiring,
- moving the remaining helper classes and test resources into `src/test`,
- keeping only the artifacts still needed by the JUnit/Testcontainers framework,
- documenting the final shape of the migration.

## Review guidance

For a given lane, the main review questions are:

1. Does the new environment match the old lane behavior?
2. Does the new suite select the same intended coverage?
3. Does each migrated JUnit class preserve the original test intent?
4. Is the legacy class removed in the same migration commit?
5. Is the old launcher suite only removed after the new JUnit suite exists?

For framework review, the main questions are:

1. Does the JUnit/Testcontainers framework preserve the old operational model closely enough for the migrated tests?
2. Are tag semantics and suite selection deterministic and explicit?
3. Do environment lifecycle and cleanup rules make sense for large, stateful product-test environments?
4. Are CI-only behaviors, especially flaky retry, clearly constrained rather than silently broadening local behavior?
