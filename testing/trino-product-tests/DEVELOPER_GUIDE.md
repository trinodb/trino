# Product Tests Developer Guide

This guide covers how to add or change product tests in the JUnit/Testcontainers framework.

For CI-failure reproduction commands, see [`RUN_PRODUCT_TESTS.md`](RUN_PRODUCT_TESTS.md).

## Architecture in one page

1. Docker images provide service runtimes (Trino, Hive, Spark, KDC, etc.).
2. Testcontainers wrappers manage container lifecycle and networking.
3. Environment classes compose one or more containers for a lane.
4. Product tests declare required environment and tags.
5. Suites select tags and run tests in explicit environments via `SuiteRunner`.

Execution model:

- Product tests are intentionally single-threaded.
- Parallel execution is not supported.
- This reduces peak memory load and avoids environment-manager conflicts caused by concurrent environment switching.

Environment and test code both use inheritance heavily:

- Environments typically form a class hierarchy where specialized lanes extend stable base environments.
- Tests often share assertions through abstract base classes, with concrete subclasses binding specific environments.
- `@RequiresEnvironment` compatibility is assignability-based, so hierarchy design directly affects reuse.

Tags are equally critical:

- Suites include/exclude tags to define CI coverage.
- Adding or changing a tag automatically changes where a test runs in CI.
- Bad tag choices can under-run (missed coverage) or over-run (too many environments).

## Writing a product test

### Conventions

- Annotate class with `@ProductTest`.
- Annotate class with `@RequiresEnvironment(...)`.
- Add explicit `@TestGroup.*` tags used by suites.

Example pattern from current product tests:

```java
@ProductTest
@RequiresEnvironment(MySqlEnvironment.class)
@TestGroup.Mysql
@TestGroup.ProfileSpecificTests
class TestCreateTableAsSelect
{
    private static final String TABLE_NAME = "nation_ctas_tmp";

    @Test
    void testCreateTableAsSelect(MySqlEnvironment environment)
            throws Exception
    {
        try (Connection conn = environment.createTrinoConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS mysql.test." + TABLE_NAME);
            stmt.executeUpdate("CREATE TABLE mysql.test." + TABLE_NAME + " AS SELECT * FROM tpch.tiny.nation");
        }
    }
}
```

### Lifecycle methods (`@BeforeEach`, `@AfterEach`, etc.)

Prefer environment-owned lifecycle hooks for reusable setup/cleanup behavior. This keeps cleanup logic in one place
and avoids repeating setup/teardown in every test class.

Use test-level lifecycle methods only when the setup is specific to that test class (for example, test-local fixture
creation that is not shared with other classes). Keep these methods minimal and avoid storing environment objects in
fields; pass the environment directly to test methods and helpers instead.

### Typical workflow

1. Copy a nearby test in the same lane.
2. Keep data setup/cleanup local to the test where possible.
3. Reuse base test classes only when behavior is intentionally shared.
4. Run class/method locally in IntelliJ.
5. If server/runtime code changed, rebuild Trino image before rerun.

## Environments: reuse vs new

Default rule: reuse an existing environment if at all possible. Environments are costly to create and stabilize.

Use an existing environment when:

- required containers and auth path already match,
- only test data/setup differs.

Add a new environment when:

- container topology changes,
- auth model changes,
- startup contract differs significantly.

Environment compatibility in suites relies on class hierarchy/assignability. Keep environment class design explicit and
predictable.

## Suites and CI wiring

- Suite classes live in `io.trino.tests.product.suite`.
- Suites use `includeTag(...)` and `excludeTag(...)` plus explicit environment runs.
- CI executes suites through the `pt` matrix by calling suite `main` methods directly.
- Suite buckets are runtime-balanced; materially increasing runtime needs maintainer discussion.

Runtime expectations:

- If a test significantly slows an existing suite, it can cause cancellations/timeouts and slows all development loops.
- Target runtime is about 30 minutes per suite bucket; sustained increases burn substantial CI resources.

When adding new coverage:

1. Add or update suite selection.
2. Keep lane ownership clear in commit history.
3. Coordinate CI bucket wiring changes with maintainers when suite composition changes materially.

## Flaky tests policy

Use flaky annotation only when:

- failure is non-deterministic,
- there is a linked issue tracking remediation.

Expectations:

- keep flaky scope as narrow as possible,
- avoid broad match patterns,
- remove flaky annotation after fix lands.

## Intel-only and credential-gated lanes

Design and maintenance guidance:

- Avoid adding new Intel-only environment, and instead try to use multi-arch images.
- Intel-only lane is currently Exasol. On Apple Silicon it runs via Rosetta and often appears hung due to very slow
  startup.
- Credential-gated lanes: keep required credential contract documented and deterministic.
- Do not hide prerequisite failures as assertion failures; fail with clear setup messages.

## Author checklist

Before finalizing changes:

1. Test class has `@ProductTest`, `@RequiresEnvironment`, and correct tags.
2. Suite selection includes the intended tags.
3. Coordinate CI suite bucket changes with maintainers when needed.
4. Local reproduction command succeeds for impacted suite/class.
