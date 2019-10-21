# Presto product tests

The product tests make use of user visible interfaces (e.g. `presto-cli`)
to test Presto for correctness. The product tests complement the unit tests
because unlike the unit tests, they exercise the Presto codebase end-to-end.

To keep the execution of the product tests as lightweight as possible we
decided to use [Docker](http://www.docker.com/). The general execution
setup is as follows: a single Docker container runs Hadoop in pseudo-distributed
mode and Presto runs either in Docker container(s) (both pseudo-distributed
and distributed setups are possible) or manually from IntelliJ (for
debugging Presto). The tests run in a separate JVM and they can be started
using the scripts found in `presto-product-tests/bin`. The product
tests are run using the [Tempto](https://github.com/prestosql/tempto) harness.

Developers should consider writing product tests in addition to any unit tests
when making changes to user visible features. The product tests should also
be run after all code changes to ensure no existing functionality has been
broken.

## Requirements

*Running the Presto product tests requires at least 4GB of free memory*

### GNU/Linux
* [`docker >= 1.10`](https://docs.docker.com/installation/#installation)

    ```
    wget -qO- https://get.docker.com/ | sh
    ```

* [`docker-compose >= 1.8.1`](https://docs.docker.com/compose/install/)

    ```
    pip install docker-compose
    ```

### OS X using Docker for Mac

* Install [Docker for Mac](https://docs.docker.com/docker-for-mac/)

* Add entries in `/etc/hosts` for all services running in docker containers:
`hadoop-master`, `mysql`, `postgres`, `cassandra`, `presto-master`.
They should point to your external IP address (shown by `ifconfig` on your Mac, not inside Docker).

* The default memory setting of 2GB might not be sufficient for some profiles like `singlenode-ldap`.
You may need 4-8 GB or even more to run certain tests. You can increase Docker memory by going to
*Docker Preferences -> Advanced -> Memory*.

## Use the `docker-compose` wrappers

We're using [multiple compose files](https://docs.docker.com/compose/extends/#multiple-compose-files)
because of the number of overrides needed for different environments,
and deficiencies of `extends:` syntax (see the note
[here](https://docs.docker.com/compose/extends/#extending-services)).


To ease the pain of passing multiple `-f` arguments to `docker-compose`,
each environment has a `compose.sh` wrapper script. Thanks to it, instead of e.g.

`docker-compose -f ./docker-compose.yml -f ../common/standard.yml [compose commands]`

one can simply write

`compose.sh [compose commands]`

## Running the product tests

The Presto product tests must be run explicitly because they do not run
as part of the Maven build like the unit tests do. Note that the product
tests cannot be run in parallel. This means that only one instance of a
test can be run at once in a given environment. To run all product
tests and exclude the `quarantine`, `big_query` and `profile_specific_tests`
groups run the following command:

```
./mvnw install -DskipTests
presto-product-tests/bin/run_on_docker.sh <profile> -x quarantine,big_query,profile_specific_tests
```

where profile is one of either:
#### Profiles
- **multinode** - pseudo-distributed Hadoop installation running on a
 single Docker container and a distributed Presto installation running on
 multiple Docker containers. For multinode the default configuration is
 1 coordinator and 1 worker.
- **multinode-tls** - pseudo-distributed Hadoop installation running on a
 single Docker container and a distributed Presto installation running on
 multiple Docker containers. Presto is configured to only accept connections
 on the HTTPS port (7878), and both coordinator and worker traffic is encrypted.
 For multinode-tls, the default configuration is 1 coordinator and 2 workers.
- **multinode-tls-kerberos** - pseudo-distributed Hadoop installation running on a
  single Docker container and a distributed installation of kerberized Presto
  running on multiple Docker containers. Presto is configured to only accept
  connections on the HTTPS port (7778), and both coordinator and worker traffic
  is encrypted and kerberized. For multinode-tls-kerberos, the default configuration
  is 1 coordinator and 2 workers.
- **singlenode** - pseudo-distributed Hadoop installation running on a
 single Docker container and a single node installation of Presto also running
 on a single Docker container.
- **singlenode-hdfs-impersonation** - HDFS impersonation enabled on top of the
 environment in singlenode profile. Presto impersonates the user
 who is running the query when accessing HDFS.
- **singlenode-kerberos-hdfs-impersonation** - pseudo-distributed kerberized
 Hadoop installation running on a single Docker container and a single node
 installation of kerberized Presto also running on a single Docker container.
 This profile has Kerberos impersonation. Presto impersonates the user who
 is running the query when accessing HDFS.
- **singlenode-kerberos-hdfs-no-impersonation** - pseudo-distributed Hadoop
 installation running on a single Docker container and a single node
 installation of kerberized Presto also running on a single Docker container.
 This profile runs Kerberos without impersonation.
- **singlenode-ldap** - Three single node Docker containers, one running an
 OpenLDAP server, one running with SSL/TLS certificates installed on top of a
 single node Presto installation, and one with a pseudo-distributed Hadoop
 installation.
- **singlenode-sqlserver** - pseudo-distributed Hadoop installation running on
 a single Docker container, a single node installation of Presto
 also running on a single Docker container and one running SQL Server server.
 While running tests on ``singlenode-sqlserver`` make sure to exclude
 `mysql` and `postgresql` tests i.e.
 `-x mysql, postgresql`.
- **two-mixed-hives** - two pseudo-distributed Hadoop installations running on
 a single Docker containers. One Hadoop (Hive) installations is kerberized.
 A single node installation of kerberized Presto also
 running on a single Docker container.
- **two-kerberos-hives** - two pseudo-distributed Hadoop installations running on
 a single Docker containers. Both Hadoop (Hive) installations are kerberized.
 A single node installation of kerberized Presto also
 running on a single Docker container.

### Hadoop docker image used for testing
The default Hadoop/Hive docker image used for testing is defined in `conf/common/compose-commons.sh` and can be controlled
via the `HADOOP_BASE_IMAGE` and `DOCKER_IMAGES_VERSION` env variables.
- `HADOOP_BASE_IMAGE` defines the Hadoop distribution family (as found in [Hadoop images in Presto Development docker
repo](https://cloud.docker.com/swarm/prestodev/repository/list?name=hive&namespace=prestodev)). The name should be without
the `-kerberized` suffix, eg. `cdh5.13-hive`. Only images that have their kerberized counterparts can be used with test profiles
implying a kerberized environment eg. `singlenode-kerberos-hdfs-impersonation`, you should still use the base name for this
env variable, the `-kerberized` suffix will be added automatically.
- `DOCKER_IMAGES_VERSION` determines the version of the images used, both Hadoop images and base Centos images to host Presto,
and serve as various run environments throughout the tests. Versions can be found on the
[Presto Development docker repo](https://cloud.docker.com/swarm/prestodev/repository/list) as well. You may use any version, either
release or snapshot. Note that all images will be required to have this version, because this version is used globally.
This is to ease maintenance and simplify debugging.

For more information on the various ways in which Presto can be configured to
interact with Kerberized Hive and Hadoop, please refer to the [Hive connector documentation](https://prestosql.io/docs/current/connector/hive.html).

### Running a single test

The `run_on_docker.sh` script can also run individual product tests. Presto
product tests are either [Java based](https://github.com/prestosql/tempto#java-based-tests)
or [convention based](https://github.com/prestosql/tempto#convention-based-sql-query-tests)
and each type can be run individually with the following commands:

```
# Run single Java based test
presto-product-tests/bin/run_on_docker.sh <profile> -t io.prestosql.tests.functions.operators.Comparison.testLessThanOrEqualOperatorExists
# Run single convention based test
presto-product-tests/bin/run_on_docker.sh <profile> -t sql_tests.testcases.system.selectInformationSchemaTables
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
presto-product-tests/bin/run_on_docker.sh <profile> -g string_functions,create_tables
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

| Tests                 | Test Group                | Profiles                                                                         |
| ----------------------|---------------------------| -------------------------------------------------------------------------------- |
| Authorization         | ``authorization``         | ``singlenode-kerberos-hdfs-impersonation``                                       |
| HDFS impersonation    | ``hdfs_impersonation``    | ``singlenode-hdfs-impersonation``, ``singlenode-kerberos-hdfs-impersonation``    |
| No HDFS impersonation | ``hdfs_no_impersonation`` | ``singlenode``, ``singlenode-kerberos-hdfs-no_impersonation``                    |
| LDAP                  | ``ldap``                  | ``singlenode-ldap``                                                              |
| SQL Server            | ``sqlserver``             | ``singlenode-sqlserver``                                                         |

Below is a list of commands that explain how to run these profile specific tests
and also the entire test suite:

* Run **Authorization** tests:

    ```
    presto-product-tests/bin/run_on_docker.sh singlenode-kerberos-hdfs-impersonation -g authorization
    ```
* Run **HDFS impersonation** tests, where &lt;profile> is one of either
``singlenode-hdfs-impersonation`` or ``singlenode-kerberos-hdfs-impersonation``:

    ```
    presto-product-tests/bin/run_on_docker.sh <profile> -g hdfs_impersonation
    ```
* Run **no HDFS impersonation** tests, where &lt;profile> is one of either
``singlenode`` or ``singlenode-kerberos-hdfs-no-impersonation``:

    ```
    presto-product-tests/bin/run_on_docker.sh <profile> -g hdfs_no_impersonation
    ```
* Run **LDAP** tests:

    ```
    presto-product-tests/bin/run_on_docker.sh singlenode-ldap -g ldap
    ```
* Run **SQL Server** tests:

    ```
    presto-product-tests/bin/run_on_docker.sh singlenode-sqlserver -g sqlserver
    ```

* Run the **entire test suite** excluding all profile specific tests, where &lt;profile> can
be any one of the available profiles:

    ```
    presto-product-tests/bin/run_on_docker.sh <profile> -x quarantine,big_query,profile_specific_tests
    ```

Note: SQL Server product-tests use `microsoft/mssql-server-linux` docker container.
By running SQL Server product tests you accept the license [ACCEPT_EULA](https://go.microsoft.com/fwlink/?LinkId=746388)

### Running with custom / downloaded artifacts

To run with custom versions of presto / presto-cli / product tests, just set the appropriate
environment variables:

```
export PRESTO_SERVER_DIR=/tmp/presto-server-dir      #unpacked presto-server.tar.gz
export PRESTO_CLI_JAR=/tmp/artifacts/presto-cli-executable.jar
export PRODUCT_TESTS_JAR=/tmp/artifacts/presto-product-tests-executable.jar
export PRESTO_JDBC_DRIVER_JAR=libs/PrestoJDBC42.jar
export PRESTO_JDBC_DRIVER_CLASS=com.teradata.presto.jdbc42.Driver
presto-product-tests/bin/run_on_docker.sh multinode -x quarantine,big_query,profile_specific_tests
```

To override tempto configuration put a new file into `presto-product-tests/conf/EXTRA_TEMPTO_CONFIG.yml` 
and then set environment variable like below. Your configuration file will be loaded as last and so 
it is able to override any configuration entry.

```
export TEMPTO_EXTRA_CONFIG_FILE=/docker/volumes/conf/EXTRA_TEMPTO_CONFIG.yml
```

All of the variables are optional and fall back to local sources / build artifacts if unspecified.

### Interrupting a test run

To interrupt a product test run, send a single `Ctrl-C` signal. The scripts
running the tests will gracefully shutdown all containers. Any follow up
`Ctrl-C` signals will interrupt the shutdown procedure and possibly leave
containers in an inconsistent state.

## Troubleshooting

Use the `docker-compose` (probably using a [wrapper](#use-the-docker-compose-wrappers))
and `docker` utilities to control and troubleshoot containers.
In the following examples ``<profile>`` is [profiles](#profiles).

1. Use the following command to view output from running containers:

    ```
    presto-product-tests/conf/docker/<profile>/compose.sh logs
    ```

2. To connect to a running container in an interactive Bash shell to
examine state and debug as necessary, use the following commands:

    ```
    # Find the ID of the desired container
    docker ps
    # Connect to running container
    docker exec -ti <container-id> /bin/bash
    ```

3. To reset the Hadoop container when debugging tests, first teardown the
container and then pull from Docker Hub to ensure the latest version has
been downloaded:

    ```
    # Stop Hadoop container (the down command stops and removes containers,
    # network, images, and volumes). This effectively resets the container.
    presto-product-tests/conf/docker/<profile>/compose.sh down
    # Pull from Docker Hub to ensure the latest version of the image is
    # downloaded.
    presto-product-tests/conf/docker/<profile>/compose.sh pull
    ```

## Known issues

### Port 1180 already allocated

If you see the error
```
ERROR: for hadoop-master  Cannot start service hadoop-master: driver failed programming external connectivity on endpoint common_hadoop-master_1 Error starting userland proxy: Bind for 0.0.0.0:1180 failed: port is already allocated
```
You most likely have some application listening on port 1180 on either docker-machine or on your local machine if you are running docker natively.
You can override the default socks proxy port (1180) used by dockerized Hive deployment in product tests using the
`HIVE_PROXY_PORT` environment variable, e.g. `export HIVE_PROXY_PORT=1180`. This will run all of the dockerized tests using the custom port for the socks proxy.
When you change the default socks proxy port (1180) and want to use Hive provided by product tests from outside docker (e.g. access it from Presto running in your IDE),
you have to modify the property `hive.metastore.thrift.client.socks-proxy` and `hive.hdfs.socks-proxy` in your `hive.properties` file accordingly.
Presto inside docker (used while starting tests using `run_on_docker.sh`) will still use default port (1180) though.

### Malformed reply from SOCKS server

If you see an error similar to
```
Failed on local exception: java.net.SocketException: Malformed reply from SOCKS server; Host Details : local host is [...]
```
Make sure your `/etc/hosts` points to proper IP address (see [Debugging Java based tests](#debugging-java-based-tests), step 3).
Also it's worth confirming that your Hive properties file accounts for the socks proxy used in Hive container (steps 4-5 of [Debugging Java based tests](#debugging-java-based-tests)).

If `/etc/hosts` entries have changed since the time when Docker containers were provisioned it's worth removing them and re-provisioning.
To do so, use `docker rm` on each container used in product tests.
