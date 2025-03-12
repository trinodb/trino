<p align="center">
    <a href="https://trino.io/"><img alt="Trino Logo" src=".github/homepage.png" /></a>
</p>
<p align="center">
    <b>Trino is a fast distributed SQL query engine for big data analytics.</b>
</p>
<p align="center">
    See the <a href="https://trino.io/docs/current/">User Manual</a> for deployment instructions and end user documentation.
</p>
<p align="center">
  <a href="https://trino.io/download.html" style="text-decoration: none">
    <img src="https://img.shields.io/maven-central/v/io.trino/trino-server.svg?label=Trino" alt="Trino download" />
  </a>
  <a href="https://github.com/jvm-repo-rebuild/reproducible-central/blob/master/content/io/trino/README.md" style="text-decoration: none">
    <img src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/jvm-repo-rebuild/reproducible-central/master/content/io/trino/badge.json" alt="Reproducible builds supported" />
  </a>
  <a href="https://trino.io/slack.html"  style="text-decoration: none">
    <img src="https://img.shields.io/static/v1?logo=slack&logoColor=959DA5&label=Slack&labelColor=333a41&message=join%20conversation&color=3AC358" alt="Trino Slack" />
  </a>
  <a href="https://trino.io/trino-the-definitive-guide.html"  style="text-decoration: none">
    <img src="https://img.shields.io/badge/Trino%3A%20The%20Definitive%20Guide-download-brightgreen" alt="Trino: The Definitive Guide book download" />
  </a>
</p>

## Development

Learn about development for all Trino organization projects:

* [Vision](https://trino.io/development/vision)
* [Contribution process](https://trino.io/development/process#contribution-process)
* [Pull request and commit guidelines](https://trino.io/development/process#pull-request-and-commit-guidelines-)
* [Release note guidelines](https://trino.io/development/process#release-note-guidelines-)

Further information in the [development section of the
website](https://trino.io/development) includes different roles, like
contributors, reviewers, and maintainers, related processes, and other aspects.

See [the Trino developer guide](https://trino.io/docs/current/develop.html) for
information about the SPI, implementing connectors and other plugins plugins,
the client protocol, writing tests and other lower level details.

See [DEVELOPMENT](.github/DEVELOPMENT.md) for information about code style,
development process, and guidelines.

See [CONTRIBUTING](.github/CONTRIBUTING.md) for contribution requirements.

## Security

See the project [security policy](.github/SECURITY.md) for
information about reporting vulnerabilities.

Trino supports [reproducible builds](https://reproducible-builds.org) as of version 449.

## Build requirements

* Mac OS X or Linux
  * Note that some npm packages used to build the web UI are only available
    for x86 architectures, so if you're building on Apple Silicon, you need 
    to have Rosetta 2 installed
* Java 23.0.0+, 64-bit
* Docker
  * Turn SELinux or other systems disabling write access to the local checkout
    off, to allow containers to mount parts of the Trino source tree

## Building Trino

Trino is a standard Maven project. Simply run the following command from the
project root directory:

    ./mvnw clean install -DskipTests

On the first build, Maven downloads all the dependencies from the internet
and caches them in the local repository (`~/.m2/repository`), which can take a
while, depending on your connection speed. Subsequent builds are faster.

Trino has a comprehensive set of tests that take a considerable amount of time
to run, and are thus disabled by the above command. These tests are run by the
CI system when you submit a pull request. We recommend only running tests
locally for the areas of code that you change.

## Running Trino in your IDE

### Overview

After building Trino for the first time, you can load the project into your IDE
and run the server.  We recommend using
[IntelliJ IDEA](http://www.jetbrains.com/idea/). Because Trino is a standard
Maven project, you easily can import it into your IDE.  In IntelliJ, choose
*Open Project* from the *Quick Start* box or choose *Open*
from the *File* menu and select the root `pom.xml` file.

After opening the project in IntelliJ, double check that the Java SDK is
properly configured for the project:

* Open the File menu and select Project Structure
* In the SDKs section, ensure that JDK 23 is selected (create one if none exist)
* In the Project section, ensure the Project language level is set to 23

### Running a testing server

The simplest way to run Trino for development is to run the `TpchQueryRunner`
class. It will start a development version of the server that is configured with
the TPCH connector. You can then use the CLI to execute queries against this
server. Many other connectors have their own `*QueryRunner` class that you can
use when working on a specific connector.

### Running the full server

Trino comes with sample configuration that should work out-of-the-box for
development. Use the following options to create a run configuration:

* Main Class: `io.trino.server.DevelopmentServer`
* VM Options: `-ea -Dconfig=etc/config.properties -Dlog.levels-file=etc/log.properties -Djdk.attach.allowAttachSelf=true`
* Working directory: `$MODULE_DIR$`
* Use classpath of module: `trino-server-dev`

The working directory should be the `trino-server-dev` subdirectory. In
IntelliJ, using `$MODULE_DIR$` accomplishes this automatically.

If `VM options` doesn't exist in the dialog, you need to select `Modify options`
and enable `Add VM options`.

To adjust which plugins are enabled for the development server, adjust the value of
`plugin.bundles` in `config.properties`. Each entry in this list must represent a plugin
specified by one of the following options:
* A path to a `pom.xml` or `*.pom` file describing a Maven project that produces a plugin.
* Maven coordinates, in the form `<groupId>:<artifactId>[:<extension>[:<classifier>]]:<version>`. The plugin will be loaded via Maven and therefore must be available in your local repository or a remote repository.
* A path to a plugin directory containing JAR files. See [Deploying a custom plugin](https://trino.io/docs/current/develop/spi-overview.html#deploying-a-custom-plugin) for more details.

If you want to use a plugin in a catalog, you must add a corresponding
`<catalog_name>.properties` file to `testing/trino-server-dev/etc/catalog`.

### Running the CLI

Start the CLI to connect to the server and run SQL queries:

    client/trino-cli/target/trino-cli-*-executable.jar

Run a query to see the nodes in the cluster:

    SELECT * FROM system.runtime.nodes;

Run a query against the TPCH connector:

    SELECT * FROM tpch.tiny.region;
