# Test writing guidelines

The Trino project aims to provide high quality artifacts and features to all
users. A suite of tests used for development, continuous integration builds,
and releases is critical for achieving this goal.

Trino acts as an integration layer for many different data sources with the help
of different connector plugin, and includes support of other plugins as well.
This results in the need testing many different use cases with a number of
external systems. The Trino test suite limits the executed tests for any changes
in pull requests as much as possible, while any merges to the master branch
process all tests.

The following requirements and guidelines help contributors to create tests with
the following goals:

* Fast execution.
* Low requirements in terms of hardware, software, and compute power, and
  therefore low cost.
* Reliable execution and test results across multiple invocations.
* Complex enough to have enough coverage of the execution paths, while avoiding
  unnecessary complexity.
* Enable quick, iterative development on developer workstation.

## Conventions and recommendations

The following section details conventions and recommendations to follow when
creating new tests or refactoring existing test code. The preferred approaches
The existing codebase is a mixture of newer test code that adheres to these
guidelines and older legacy code. The legacy test code should not be used as
example for new tests, rather follow the guidelines in this document.

Also note that the guidelines are subject to change in a process of further
refinement and improvements from practical experience.

A number of requirements apply to all new tests, and any refactoring work of
existing tests:

* All tests must use JUnit 5.
* All tests must use statically imported AssertJ assertions, typically from
  `org.assertj.core.api.Assertions`.
* Test class names must start with `Test`, for example `TestExample`
* Test classes should be defined as package-private and final.
* Test method must start with `test`, for example `testExplain()`
* Test methods should be defined as package-private.
* Tests must be written as unit tests, including tests that abstract production
  infrastructure with TestContainers, when possible. Product or other
  integration tests should be avoided. These tests typically rely on external
  infrastructure, use a full Trino runtime, and therefore are often slower and
  suffer from reliability issues.
* Tests must not be duplicated across unit and product tests, or different
  plugins and other integrations.

## Guidelines

In addition to the requirements, a number of specific guidelines help with
authoring new tests as well as refactoring existing tests.

### Focus on high value tests

Testing in Trino is extremely expensive, and slows down all development as they
take hours of compute time in a limited environment. For large expensive tests,
consider the value the test brings to Trino, and ensure the value is justified
by the cost. We effectively have a limited budget for testing, and CI tests
queue on most days, often for many hours, which reduces the overall project
velocity.

### Avoid combinatorial tests

Prefer tests of items in isolation and test a few common combinations to verify
integrations are functional. Do not implement tests for all possible
combinations.

### Avoid product tests

If you can create a unit test for a feature, use a unit test and avoid writing a
product test. Over time the aim is to remove the majority of product tests, and
avoiding new product tests helps to prevent the migration costs from growing.

Only use product tests in the following cases:

* Minimal, high level integration testing that uses a full server. For example,
  this can verify that a plugin works correctly with the plugin classloader and
  classpath.
* When the test code needs to run in a specialized environment, such as a
  container with Kerberos configured. Only run the minimum set of tests
  necessary to verify this integration.

### Avoid creating testing abstractions

The following approaches should be avoided because the existing build tools and
frameworks provide sufficient capabilities:

* Creating custom dispatch frameworks for parallelizing test execution
* Creating test-specific assertion frameworks
* Creating custom parameterized test frameworks

### Avoid data providers and parametric tests

Data providers and parametric tests add unnecessary complexity. Consider
focusing on high value tests and avoiding combinatorial tests, and the
following details:

* Most data providers are either trivially small, or generate massive
  combinatorial, indiscriminate, data sets for testing.
* Prefer to write explicit test cases for trivial cases like a boolean
  parameter.
* For small datasets, use a “for-each item in an inline list”.
* For larger datasets, consider using a type safe enum class.
* For large test datasets, discuss your use case with Trino maintainers to work
  on a solution or other guidance.
* Avoid multiple independent data providers in a test, including multiple nested
  for loops or multiple data provider parameters.

### Avoid writing stateful test classes

Stateful tests can lead to issues from on one test leaking into other tests,
especially when test runs are parallelized. As a result debugging and
troubleshooting test failures and maintenance of the tests is more difficult. If
possible these stateful test classes should be avoided.

### Do not try to manage memory

JUnit and the JVM take care of test life cycle and memory management. Avoid
manual steps such as nulling out fields in `@After` methods to “free memory”. It
is safe to assign memory intensive objects to final fields, as the class is
automatically dereferenced after the test run.

### Use simple resource initialization

Prefer resource initialization in constructors and tear them down in `@After`
methods if necessary. This approach, combined with not nulling fields, allows
the fields to be final and behave like any `Closeable` class in normal Java code
Consider using the Guava `Closer` class to simplify cleanup.

### Keep test setup and teardown simple

Avoid the `@Before`/`@After` each test method style of setup and teardown.

* Prefer try-with-resources if natural
* If necessary, use a shared initialization or cleanup method that is explicitly
  called.
* If you have a test that benefits from @Before/After methods, discuss the
  approach with the maintainers to develop a solution and improve guidance.

### Ensure testability of new plugin and connector features

New plugin/connector features should be testable using one of the testing
plugins (e.g., memory or null). There are existing features only tested in
plugins in Hive, and over time we expect coverage using the testing plugins

### Keep focus on plugin and connector tests

For plugins and specifically connector plugins, focus on the code unique to the
plugin. Do not add tests for core engine features. Plugins should be focused on
the correctness of the SPI implementation, and compatibility with external
systems.

### Avoid flaky tests

Flaky tests are test that are not reliable. Multiple runs of the same test
result in inconsistent results. Typically the tests are successful, and then
rarely fail. Reasons for flakiness include reliance on external, unstable
systems, connections, and other hard to troubleshoot setups.

Existing flaky tests using the legacy TestNG library can be marked with the
`@Flaky` annotation temporarily to improve CI reliability until a fix is
implemented:

* Ideally the fix is to make the test reliable.
* Rewrite the test to not rely on flakey infrastructure, including the practice
  to avoid HDFS.
* If necessary, add explicit retries, but be cognizant of resource usage.

After a certain time period, if the test hasn’t been fixed, it should be
removed.

New tests with the `@Flaky` annotation can not be introduced, since new tests
must use JUnit. Rewrite the test to be stable or avoid the test altogether.

### Avoid disabling tests

Prefer to remove a test instead of disabling it. Test code is maintained and
updated as the codebase changes, and inactive tests just waste time and effort.

Disabled tests can be removed at any time.

### Avoid using `Assumptions.abort()`

The approach to use `Assumptions.abort()` to skip a test, especially deep in the
call stack, makes it difficult to debug tests failures. The `abort()` works by
throwing an exception, which can be caught by intervening code inadvertently,
leading to misleading stack traces and test failures.

### Avoid test inheritance

Inheritance of tests creates unnecessary complexity. Keep tests simple and use
composition if necessary.

## Avoid helper assertions

The required usage of AssertJ provides a rich set of assertions, that typically
makes custom helper assertions unnecessary. Custom assertions often make tests
harder to follow and debug.

If you decide a helper assertion is needed, consider the following details:

* Start the name with `assert`, for example `assertSomeLogicWorks`
* Prefer private and static

## Examples

The following examples showcase recommended and discourage practices.

### Concurrency for tests

Use `PER_CLASS` for instances because `QueryAssertions` is too expensive to
create per-method, and a allow parallel execution of tests with `CONCURRENT`:

```java
@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestJoin
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testXXX()
    {
        assertThat(assertions.query("""
            ...
            """))
            .matches("...");
    }
}
```

### Avoid manual lifecycle management

Avoid managing the lifecycle of a Closeable like a connection with
`@BeforeEach`/`@AfterEach` to reduce overhead:

```java
@TestInstance(PER_METHOD)
class Test
{
    private Connection connection;

    @BeforeEach
    public void setup()
    {
        // WRONG: create this in the test method using try-with-resources
        connection = newConnection();
    }

    @AfterEach
    public void teardown()
    {
        connection.close();
    }

    @Test
    public void test()
    {
        ...
    }
}
```

Using a try with resources approach allows clean parallelization of tests and
includes automatic memory management:

```java
class Test
{

    @Test
    public void testSomething()
    {
        try (Connection connection = newConnection();) {
            ...
        }
    }

    @Test
    public void testSomethingElse()
    {
        try (Connection connection = newConnection();) {
            ...
        }
    }
}
```

## Avoid fake abstractions

Avoid using fake abstraction for tests.

```java
@DataProvider(name = "data")
public void test(boolean flag)
{
    // WRONG: use separate test methods
    assertEqual(
        flag ? ... : ...,
        flag ? ... : ...);
}
```

Replace with simplified separate assertions:

```java
public void test()
{
    assertThat(...).isEqualTo(...); // case corresponding to flag == true
    assertThat(...).isEqualTo(...); // case corresponding to flag == false
}
```

## Avoid custom parallelization

Do not develop a custom parallel test execution framework:

```java
@Test(dataProvider = "parallelTests")
public void testParallel(Runnable runnable)
{
   try {
       parallelTestsSemaphore.acquire();
   }
   catch (InterruptedException e) {
       Thread.currentThread().interrupt();
       throw new RuntimeException(e);
   }
   try {
       runnable.run();
   }
   finally {
       parallelTestsSemaphore.release();
   }
}

@DataProvider(name = "parallelTests", parallel = true)
public Object[][] parallelTests()
{
   return new Object[][] {
        parallelTest("testCreateTable", this::testCreateTable),
        parallelTest("testInsert", this::testInsert),
        parallelTest("testDelete", this::testDelete),
        parallelTest("testDeleteWithSubquery", this::testDeleteWithSubquery),
        parallelTest("testUpdate", this::testUpdate),
        parallelTest("testUpdateWithSubquery", this::testUpdateWithSubquery),
        parallelTest("testMerge", this::testMerge),
        parallelTest("testAnalyzeTable", this::testAnalyzeTable),
        parallelTest("testExplainAnalyze", this::testExplainAnalyze),
        parallelTest("testRequestTimeouts", this::testRequestTimeouts)
   };
}
```

Leave parallelization to JUnit instead, and implement separate test methods
instead.

## Avoid parameterized tests

Do not create a custom parameterized test framework:

```java
@Test
public void testTinyint()
{
    SqlDataTypeTest.create()
        .addRoundTrip(...)
        .addRoundTrip(...)
        .addRoundTrip(...)
        .execute(getQueryRunner(), trinoCreateAsSelect("test_tinyint"))
        .execute(getQueryRunner(), trinoCreateAndInsert("test_tinyint"))
        .addRoundTrip(...)
        .execute(getQueryRunner(), clickhouseQuery("tpch.test_tinyint"));
}
```
