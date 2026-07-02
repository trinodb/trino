# Trino product tests

Trino product tests validate end-to-end behavior (connectors, auth, CLI, storage, and interoperability)
using JUnit 5 and Testcontainers.

This guide is optimized for the common workflow: a CI failure that you need to reproduce locally.

For authoring and maintenance guidance, see [`DEVELOPER_GUIDE.md`](DEVELOPER_GUIDE.md).

## Prerequisites

- Java 25
- Container runtime compatible with Docker API
- At least 8 GB RAM recommended

Linux:

- Docker Engine

macOS:

- [OrbStack](https://orbstack.dev/) (recommended)
- [Docker Desktop](https://www.docker.com/products/docker-desktop) (alternative)

## Reproduce a CI failure locally

Product tests are designed to run from an IDE first. In most cases, start by running the single failing test method.

### Running tests in IntelliJ

The tests just work in IntelliJ, so just run as you would any other JUnit test. The framework will start the required
docker environment for the test. If you have environment-related issues, check the "Common failure patterns" section
below.

### Running tests with Maven

Use Surefire to run one test class or one test method:

```bash
./mvnw -pl testing/trino-product-tests \
  -Dair.check.skip-all=true \
  -Dtest=TestMySqlSqlTests \
  test
```

```bash
./mvnw -pl testing/trino-product-tests \
  -Dair.check.skip-all=true \
  -Dtest=TestMySqlSqlTests#testInsert \
  test
```

For single-test debugging, prefer IntelliJ run actions because they are faster to iterate on than suite runs.

If a fresh checkout fails test compilation with `cannot find symbol ... Flaky`, run once:

```bash
./mvnw -pl testing/trino-testing-services -DskipTests -Dair.check.skip-all=true install
```

### Rebuild artifacts and Trino server image when needed

If your branch changes Trino server/runtime code, rebuild first:

```bash
./mvnw -DskipTests -Dair.check.skip-all=true install
(cd core/docker && ./build.sh)
```

This builds the Docker image for the Trino server from your local code. Product tests execute against that packaged
server image inside the test Docker environment, so the server must be packaged before runtime changes can be tested.
Because the server runs in-container, direct server debugging is usually harder than normal unit/integration tests.

### Running Suites (CI/maintainer workflow)

Suites are usually too slow for local debugging. Prefer IntelliJ method/class runs or Surefire class/method runs.
If you need to run a suite locally (for CI parity), use:

```bash
./mvnw -pl testing/trino-product-tests \
  -Dair.check.skip-all=true \
  -DskipTests \
  test-compile exec:java \
  -Dexec.classpathScope=test \
  -Dexec.mainClass=io.trino.tests.product.suite.SuiteHiveBasic
```

With explicit image override:

```bash
./mvnw -pl testing/trino-product-tests \
  -Dair.check.skip-all=true \
  -DskipTests \
  test-compile exec:java \
  -Dexec.classpathScope=test \
  -Dexec.mainClass=io.trino.tests.product.suite.SuiteHiveBasic \
  -Dtrino.product-tests.image=trino:dev
```

## Intel-only and credential-gated environments

Some environments require a special setup to run locally:

- Intel-only lane: currently `SuiteExasol`, requiring an amd64 host.
- Credential-gated lanes: require cloud/SaaS credentials (for example Azure, GCS, Databricks, Snowflake).

On Apple Silicon, Intel-only images typically run via Rosetta and are often too slow to start reliably. For these
lanes, local debugging is limited; use a remote amd64 host for validation.

## Common failure patterns

- Stale Trino image: tests run an older server image than your code changes.
  Fix: run `./mvnw -DskipTests -Dair.check.skip-all=true install` and `(cd core/docker && ./build.sh)`.
- Missing credentials: cloud/SaaS env vars or secret files not present.
  Fix: check the lane credential requirements and ask maintainers in project channels for current setup details.
- Wrong architecture: running Intel-only suite on ARM.
  Fix: run on a remote amd64 host such as AWS EC2.
- Environment startup timeout: dependency container not healthy or startup ordering issue.
  Fix: check container logs; for Intel-only lanes, consider shorter startup timeouts during debug loops.

## Useful paths

- JUnit tests and environments: `testing/trino-product-tests/src/test/java/io/trino/tests/product/`
- Tags (`@TestGroup`): `testing/trino-product-tests/src/test/java/io/trino/tests/product/TestGroup.java`
- Suites: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/`
- Dependency images (Hive, Spark, KDC, etc.): [`trinodb/docker-images`](https://github.com/trinodb/docker-images)
- Container framework: `testing/trino-testing-containers/src/main/java/io/trino/testing/containers/`

Environment classes are intentionally hierarchical and reused via inheritance. Tag choice is critical because tags drive
suite selection and therefore CI coverage.

List suites quickly:

```bash
ls testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/Suite*.java
```
