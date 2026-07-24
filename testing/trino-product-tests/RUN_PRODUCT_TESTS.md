# Trino product tests

Trino product tests validate end-to-end behavior (connectors, auth, CLI, storage, and interoperability)
using JUnit 5 and Testcontainers.

This guide is optimized for the common workflow: a CI failure that you need to reproduce locally.

For authoring and maintenance guidance, see [`DEVELOPER_GUIDE.md`](DEVELOPER_GUIDE.md).

## Prerequisites

In addition to the standard Trino development setup, product tests require:

- Container runtime compatible with Docker API
- At least 8 GB RAM recommended

Linux:

- Docker Engine

macOS:

- [OrbStack](https://orbstack.dev/) (recommended)
- [Docker Desktop](https://www.docker.com/products/docker-desktop) (alternative)

## Select the Trino server image

Product tests start their dependencies with Testcontainers. Environments that query Trino, such as
`MySqlEnvironment`, also start a Trino server container. Running Maven or an IDE test does not build that server image.

The framework selects the Trino image from the `trino.product-tests.image` system property. If the property is absent,
it uses `trinodb/trino:latest`. Outside CI, this normally resolves to a published image that does not contain changes
from the current checkout. Building a local image does not select it automatically; always pass the property when
testing local server or plugin changes.

To package the current checkout and select its image:

```bash
case "$(docker info --format '{{.Architecture}}')" in
  amd64|x86_64) architecture=amd64 ;;
  arm64|aarch64) architecture=arm64 ;;
  *) echo "Unsupported Docker architecture" >&2; exit 1 ;;
esac
version="$(./mvnw -f pom.xml --quiet help:evaluate \
  -Dexpression=project.version -DforceStdout --raw-streams)"

./mvnw -DskipTests -Dair.check.skip-all install
core/docker/build.sh -a "${architecture}"

export TRINO_PRODUCT_TESTS_IMAGE="trino:${version}-${architecture}"
docker image inspect "${TRINO_PRODUCT_TESTS_IMAGE}" >/dev/null
```

Keep the same shell open for the commands below. Alternatively, replace `${TRINO_PRODUCT_TESTS_IMAGE}` with the
printed image tag. When the current checkout does not affect Trino server or plugin behavior, the full build can be
skipped with `export TRINO_PRODUCT_TESTS_IMAGE=trinodb/trino:latest`.

## Reproduce a CI failure locally

Product tests are designed to run from an IDE first. In most cases, start by running the single failing test method.

### Running tests in IntelliJ

Run a product test as a normal JUnit test. The framework starts the required Docker environment. To use an image built
from the current checkout, add this VM option to the run configuration:

```text
-Dtrino.product-tests.image=trino:<project-version>-<architecture>
```

Use the value of `TRINO_PRODUCT_TESTS_IMAGE` from the build commands above. If you have environment-related issues,
check the "Common failure patterns" section below.

### Running tests with Maven

Use Surefire to run one test class or one test method:

```bash
./mvnw -pl testing/trino-product-tests \
  -Dair.check.skip-all \
  -Dtrino.product-tests.image="${TRINO_PRODUCT_TESTS_IMAGE}" \
  -Dtest=TestMySqlSqlTests \
  test
```

```bash
./mvnw -pl testing/trino-product-tests \
  -Dair.check.skip-all \
  -Dtrino.product-tests.image="${TRINO_PRODUCT_TESTS_IMAGE}" \
  -Dtest=TestMySqlSqlTests#testSelect \
  test
```

For single-test debugging, prefer IntelliJ run actions because they are faster to iterate on than suite runs.

If a fresh checkout fails test compilation with `cannot find symbol ... Flaky`, run once:

```bash
./mvnw -pl testing/trino-testing-services -DskipTests -Dair.check.skip-all install
```

### Running Suites (CI/maintainer workflow)

Suites are usually too slow for local debugging. Prefer IntelliJ method/class runs or Surefire class/method runs.
If you need to run a suite locally (for CI parity), use:

```bash
./mvnw -pl testing/trino-product-tests \
  -Dair.check.skip-all \
  -DskipTests \
  test-compile exec:java \
  -Dexec.classpathScope=test \
  -Dtrino.product-tests.image="${TRINO_PRODUCT_TESTS_IMAGE}" \
  -Dexec.mainClass=io.trino.tests.product.suite.SuiteHiveBasic
```

CI follows the same model: it builds the Maven artifacts, packages a Trino image from them, transfers that image to
the product-test jobs, and passes it with `trino.product-tests.image`. Thus CI suites always test the checked-out
revision rather than the published `trinodb/trino:latest` image.

## Intel-only and credential-gated environments

Some environments require a special setup to run locally:

- Intel-only lane: currently `SuiteExasol`, requiring an amd64 host.
- Credential-gated lanes: require cloud/SaaS credentials (for example Azure, GCS, Databricks, Snowflake).

On Apple Silicon, Intel-only images typically run via Rosetta and are often too slow to start reliably. For these
lanes, local debugging is limited; use a remote amd64 host for validation.

## Common failure patterns

- Stale Trino image: tests run an older server image than your code changes.
  Fix: rebuild the Maven artifacts and image, then pass the resulting tag with
  `-Dtrino.product-tests.image="${TRINO_PRODUCT_TESTS_IMAGE}"`.
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
