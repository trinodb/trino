# Trino product tests

The product tests make use of user visible interfaces (e.g., the CLI)
to test Trino for correctness. The product tests complement the unit tests
because unlike the unit tests, they exercise the Trino codebase end-to-end.

To keep the execution of the product tests as lightweight as possible we
decided to use [Docker](http://www.docker.com/). The general execution
setup is as follows: a single Docker container runs Hadoop in pseudo-distributed
mode and Trino runs either in Docker container(s) (both pseudo-distributed
and distributed setups are possible) or manually from IntelliJ (for
debugging Trino). The tests run in a separate JVM and they can be started
using the launcher found in `testing/trino-product-tests-launcher/bin/run-launcher`. The product
tests are run using the [Tempto](https://github.com/trinodb/tempto) harness.

**There is a helper script at `testing/bin/ptl` which calls
`testing/trino-product-tests-launcher/bin/run-launcher` and helps you avoid
typing the full path to the launcher everytime. Rest of this document uses
`testing/bin/ptl` to start the launcher but you can use the full path too.**

Developers should consider writing product tests in addition to any unit tests
when making changes to user visible features. The product tests should also
be run after all code changes to ensure no existing functionality has been
broken.

## Requirements

*Running the Trino product tests requires at least 4GB of free memory*

### GNU/Linux
* [`docker >= 1.10`](https://docs.docker.com/installation/#installation)

    ```
    wget -qO- https://get.docker.com/ | sh
    ```

### OS X using Docker for Mac

* Install [Docker for Mac](https://docs.docker.com/docker-for-mac/)

* Add entries in `/etc/hosts` for all services running in docker containers:
`hadoop-master`, `postgres`, `cassandra`, `presto-master`.
They should point to your external IP address (shown by `ifconfig` on your Mac, not inside Docker).

* The default memory setting of 2GB might not be sufficient for some profiles like `singlenode-ldap`.
You may need 4-8 GB or even more to run certain tests. You can increase Docker memory by going to
*Docker Preferences -> Advanced -> Memory*.

## Running the product tests

The Trino product tests must be run explicitly because they do not run
as part of the Maven build like the unit tests do. Note that the product
tests cannot be run in parallel. This means that only one instance of a
test can be run at once in a given environment. To run all product
tests and exclude the `quarantine`, `large_query` and `profile_specific_tests`
groups run the following command:

```
./mvnw install -DskipTests
testing/bin/ptl test run --environment <environment> \
[--config <environment config>] \
-- <tempto arguments>
```

#### Environments
- **multinode** - pseudo-distributed Hadoop installation running on a
 single Docker container and a distributed Trino installation running on
 multiple Docker containers. For multinode the default configuration is
 1 coordinator and 1 worker.
- **multinode-tls** - pseudo-distributed Hadoop installation running on a
 single Docker container and a distributed Trino installation running on
 multiple Docker containers. Trino is configured to only accept connections
 on the HTTPS port (7878), and both coordinator and worker traffic is encrypted.
 For multinode-tls, the default configuration is 1 coordinator and 2 workers.
- **multinode-tls-kerberos** - pseudo-distributed Hadoop installation running on a
  single Docker container and a distributed installation of kerberized Trino
  running on multiple Docker containers. Trino is configured to only accept
  connections on the HTTPS port (7778), and both coordinator and worker traffic
  is encrypted and kerberized. For multinode-tls-kerberos, the default configuration
  is 1 coordinator and 2 workers.
- **singlenode-hdfs-impersonation** - HDFS impersonation enabled on top of the
 environment in singlenode profile. Trino impersonates the user
 who is running the query when accessing HDFS.
- **singlenode-kerberos-hdfs-impersonation** - pseudo-distributed kerberized
 Hadoop installation running on a single Docker container and a single node
 installation of kerberized Trino also running on a single Docker container.
 This profile has Kerberos impersonation. Trino impersonates the user who
 is running the query when accessing HDFS.
- **singlenode-ldap** - Three single node Docker containers, one running an
 OpenLDAP server, one running with SSL/TLS certificates installed on top of a
 single node Trino installation, and one with a pseudo-distributed Hadoop
 installation.
- **two-kerberos-hives** - two pseudo-distributed Hadoop installations running on
 a single Docker containers. Both Hadoop (Hive) installations are kerberized.
 A single node installation of kerberized Trino also
 running on a single Docker container.
 
You can obtain list of available environments using command:
 
```
./mvnw install -DskipTests
testing/bin/ptl env list
```

#### Environment config

Most of the Hadoop-based environments can be run in multiple configurations that use different Hadoop distribution:

- **config-default** - executes tests against vanilla Hadoop distribution
- **config-hdp3** - executes tests against HDP3 distribution of Hadoop

You can obtain list of available environment configurations using command:

```
testing/bin/ptl env list
```

All of `test run`, `env up` and `suite run` commands accept `--config <environment config>` setting.

### Running a single test

The `run-launcher` script can also run individual product tests. Trino
product tests are either [Java based](https://github.com/trinodb/tempto#java-based-tests)
or [convention based](https://github.com/trinodb/tempto#convention-based-sql-query-tests)
and each type can be run individually with the following commands:

```
# Run single Java based test
testing/bin/ptl test run \
            --environment <environment> \
            [--config <environment config>] \
            -- -t io.trino.tests.functions.operators.Comparison.testLessThanOrEqualOperatorExists

# Run single convention based test
testing/bin/ptl test run \
            --environment <environment> \
            [--config <environment config>] \
            -- -t sql_tests.testcases.system.selectInformationSchemaTables
```

### Running groups of tests

Tests belong to a single or possibly multiple groups. Java based tests are
tagged with groups in the `@Test` annotation and convention based tests have
group information in the first line of their test file. Instead of running
single tests, or all tests, users can also run one or more test groups.
This enables users to test features in a cross functional way. To run a
particular group, use the `-g` argument as shown:

```
# Run all tests in the string_functions and create_table groups
testing/bin/ptl test run \
            --environment <environment> \
            [--config <environment config>] \
            -- -g string_functions,create_tables
```

Some groups of tests can only be run with certain profiles. Incorrect use of profile
for such test groups will result in test failures. We call these tests that
require a specific profile to run as *profile specific tests*. In addition to their
respective group, all such tests also belong to a parent group called
`profile_specific_tests`. To exclude such tests from a run, make sure to add the
`profile_specific_tests` group to the list of excluded groups.

Following table describes the profile specific test categories, the corresponding
test group names and the profile(s) which must be used to run tests in those test
groups.

| Tests                 | Test Group                | Profiles                                                                      |
| ----------------------|---------------------------|-------------------------------------------------------------------------------|
| Authorization         | ``authorization``         | ``singlenode-kerberos-hdfs-impersonation``                                    |
| HDFS impersonation    | ``hdfs_impersonation``    | ``singlenode-hdfs-impersonation``, ``singlenode-kerberos-hdfs-impersonation`` |
| No HDFS impersonation | ``hdfs_no_impersonation`` | ``multinode``, ``singlenode-kerberos-hdfs-no_impersonation``                  |
| LDAP                  | ``ldap``                  | ``singlenode-ldap``                                                           |

Below is a list of commands that explain how to run these profile specific tests
and also the entire test suite:

Note: SQL Server product-tests use `mcr.microsoft.com/mssql/server` docker container.
By running SQL Server product tests you accept the license [ACCEPT_EULA](https://go.microsoft.com/fwlink/?LinkId=746388)

### Running test suites

Tests are further organized into suites which contain execution of multiple test groups in multiple different environments.

You can obtain list of available test suites using command:

```
testing/bin/ptl suite list
```

Command `testing/bin/ptl suite describe --suite <suite name>` shows list of tests that will be executed and environments 
that will be used when `suite run` is invoked.

You can execute single suite using command:

```
testing/bin/ptl suite run --suite <suite name> \
    [--config <environment config>]
```

Test runs are executed sequentially, one by one.

### Interrupting a test run

To interrupt a product test run, send a single `Ctrl-C` signal. The scripts
running the tests will gracefully shutdown all containers. Any follow up
`Ctrl-C` signals will interrupt the shutdown procedure and possibly leave
containers in an inconsistent state.

## Debugging Product Tests

The `run-launcher` script also accepts an argument `--debug` which can be used
to instruct the various components (Trino co-ordinator, workers, JVM that runs
the tests etc.) within the product test environment to expose JVM debug ports.

For example to debug the `TestHiveViews.testFromUtcTimestampCornerCases` on the
`multinode` environment you can run:

```
testing/bin/ptl test run \
    --environment multinode \
    --debug \
    -- -t TestHiveViews.testFromUtcTimestampCornerCases
```

The port numbers being used are logged in the `run-launcher` output as
`Listening for transport dt_socket at address: <port_number>`.

Once you have the port numbers you can create a *Run/Debug Configuration* in
IntelliJ of type **Remote JVM Debug** with the following configuration:

- **Debugger mode:** `Attach to remote JVM`
- **Host:** `localhost`
- **Port:** the debug `<port_number>` for the component you're trying to debug
- **Command line arguments for remote JVM:** Leave this as it is
- **Use module classpath:** `trino-product-tests`

You can now start the debug configuration that you just created and IntelliJ
will attach to the remote JVM and you can use the debugger from within
IntelliJ.

## Skipping unrelated tests

Test environments track which Trino features they enable, like connectors or password authenticators.
This can be used to skip running product tests on environments that don't use specific features, by passing
`--impacted-features=<file>` to the product test launcher. The file should contain a list of features to test,
one per line, as `feature-kind:feature-name`, e.g., `connector:hive`.

Such a file can be generated from a list of Maven modules that are Trino plugins
by running the `testing/trino-plugin-reader` utility:
```bash
testing/trino-plugin-reader/target/trino-plugin-reader-*-executable.jar \
    -i modules.txt \
    -p core/trino-server/target/trino-server-*-hardlinks/plugin
```

A list of modules modified on a particular Git branch can be obtained by enabling the `gib` profile
when building Trino. It's saved as `gib-impacted.log`.

> Note: all product tests should be run when there are changes in any common files, including:
> any modules from `core` or `trino-server`, product tests or product tests launcher itself,
> or CI workflows in `.github`.

Skipping unrelated product tests is enabled by default in the CI workflows run for pull requests.
On the master branch (after merging pull requests), the CI workflow always runs all product tests.
To force running all tests in a pull request, label it with `tests:all` or `tests:all-product`.

## Known issues

### Port 1180 already allocated

If you see the error
```
ERROR: for hadoop-master  Cannot start service hadoop-master: driver failed programming external connectivity on endpoint common_hadoop-master_1 Error starting userland proxy: Bind for 0.0.0.0:1180 failed: port is already allocated
```
You most likely have some application listening on port 1180 on either docker-machine or on your local machine if you are running docker natively.
You can override the default socks proxy port (1180) used by dockerized Hive deployment in product tests using the
`HIVE_PROXY_PORT` environment variable, e.g. `export HIVE_PROXY_PORT=1180`. This will run all of the dockerized tests using the custom port for the socks proxy.
When you change the default socks proxy port (1180) and want to use Hive provided by product tests from outside docker (e.g. access it from Trino running in your IDE),
you have to modify the property `hive.metastore.thrift.client.socks-proxy` and `hive.hdfs.socks-proxy` in your `hive.properties` file accordingly.
Trino inside docker (used while starting tests using `run-launcher`) will still use default port (1180) though.

### Malformed reply from SOCKS server

If you see an error similar to
```
Failed on local exception: java.net.SocketException: Malformed reply from SOCKS server; Host Details : local host is [...]
```
Make sure your `/etc/hosts` points to proper IP address (see [Debugging Java based tests](#debugging-java-based-tests), step 3).
Also it's worth confirming that your Hive properties file accounts for the socks proxy used in Hive container (steps 4-5 of [Debugging Java based tests](#debugging-java-based-tests)).

If `/etc/hosts` entries have changed since the time when Docker containers were provisioned it's worth removing them and re-provisioning.
To do so, use `docker rm` on each container used in product tests.
