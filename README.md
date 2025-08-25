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
  <a href="https://trino.io/download.html" style="text-decoration: none"><img
    src="https://img.shields.io/maven-central/v/io.trino/trino-server.svg?label=Trino"
    alt="Trino download"
  /></a>
  <a href="https://github.com/jvm-repo-rebuild/reproducible-central/blob/master/content/io/trino/README.md" style="text-decoration: none"><img
    src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/jvm-repo-rebuild/reproducible-central/master/content/io/trino/badge.json"
    alt="Reproducible builds supported"
  /></a>
  <a href="https://trino.io/slack.html" style="text-decoration: none"><img
    src="https://img.shields.io/static/v1?logo=slack&logoColor=959DA5&label=Slack&labelColor=333a41&message=join%20conversation&color=3AC358"
    alt="Trino Slack"
  /></a>
  <a href="https://trino.io/trino-the-definitive-guide.html" style="text-decoration: none"><img
    src="https://img.shields.io/badge/Trino%3A%20The%20Definitive%20Guide-download-brightgreen"
    alt="Trino: The Definitive Guide book download"
  /></a>
</p>

## Arrow Spooling Support

This fork includes Arrow spooling support built on top of the work from [PR #25015](https://github.com/trinodb/trino/pull/25015/) authored by [@dysn](https://github.com/dysn) and [@wendigo](https://github.com/trinodb/trino/commits?author=wendigo).

This is for now not meant to be merged but only for the Trino dev community to be reviewed and discussed.

### Motivation

The primary motivation for implementing columnar storage (Arrow) vs traditional JSON encoding when using the Spooling protocol is addressing the tremendous serialization/deserialization costs that make currently Trino inconvenient to use with large query outputs.

**Current Pain Points with JSON:**
- Massive serialization/deserialization overhead for large result sets
- Forces users to implement workaround ad hoc "unload" solutions
  - creating temporary Hive/Iceberg tables in Parquet format before final consumption
  - Users must manually manage temporary table creation and cleanup
  - Added operational complexity

**Benefits of Arrow Spooling:**
- Dramatically reduced serialization/deserialization costs through columnar format.
- Fast consumption of large query result through Trino's native Spooling Protocol.
- Eliminates need for temporary table or other ad-hoc workarounds.
- Enables new use cases that were previously impractical due to performance constraints. 
- Make the deserialization cost very small for the client, and make the query result ready be to used with other Arrow compatible libraries.


### Configuration

To enable Arrow spooling, you need to:

1. **Add JVM options** to your Java environment:
   ```
   --enable-native-access=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED
   ```

2. **Add configuration properties** to your Trino configuration:
   ```properties
   protocol.spooling.encoding.arrow.enabled=true
   protocol.spooling.encoding.arrow+zstd.enabled=true
   ```

3. **Optional: Configure Arrow performance and memory settings**:
   ```properties
   # Control Arrow serialization parallelism (default: 5)
   # Limits concurrent Arrow segment encoding operations per JVM
   protocol.spooling.arrow.max-concurrent-serialization=5
   
   # Control Arrow memory allocation (default: 200MB)
   # Sets the maximum memory Arrow buffer allocator can use
   protocol.spooling.arrow.max-allocation=200MB
   ```

#### Arrow Spooling additional Configuration

- **`protocol.spooling.arrow.max-concurrent-serialization`**: Controls the maximum number of Arrow segments that can be encoded concurrently in a single JVM. This helps prevent resource exhaustion during heavy concurrent query processing. Higher values allow more parallelism but consume more CPU and memory.

- **`protocol.spooling.arrow.max-allocation`**: Sets the total memory budget for Arrow buffer allocation across all operations. This prevents Arrow operations from consuming excessive memory and provides a safety bound for memory usage. The allocator will reject new allocations once this limit is reached.

**Important Note on Arrow Memory Management**: Arrow uses **off-heap memory** (native memory outside the JVM heap) for its buffer allocations. This means Arrow memory usage is not managed by the JVM's garbage collector and does not count toward your `-Xmx` heap limit. Because this memory is not subject to Java's automatic memory management, we provide these explicit controls to prevent unbounded off-heap memory consumption that could lead to system memory exhaustion.

**Technical Detail**: Arrow achieves its high performance by using Java's `Unsafe` API for direct memory access, which allows it to allocate and manipulate memory directly without JVM overhead. While this provides significant performance benefits for columnar data processing, it bypasses Java's memory management safeguards, making explicit memory controls essential.

### Python Client Support

For asynchronous retrieval of Arrow spooled segments using PyArrow, see this fork of aiotrino: [jonasbrami/aiotrino](https://github.com/jonasbrami/aiotrino).

### Timestamp and Time Zone Handling

#### Design Rationale

Arrow spooling implementation makes specific design choices regarding timestamp and time zone handling:

1. **Stay as close to arrow as possible for serialization/deserialization speed*:
   - For example Most databases, data libraries, and data formats (including Arrow) do not support different time zones within the same column. Time zone information is typically defined per column as part of the schema, the Arrow spooling implementation automatically converts all `TIMESTAMP WITH TIME ZONE` and `TIME WITH TIME ZONE` values to UTC during serialization. Avoiding Data Peeking**: We want to avoid having to peek into the data to guess the timezone used in the column (assuming only 1 is used)
   - JSON is serialized as utf-8


#### Implementation Details

- **TIMESTAMP WITH TIME ZONE**: All values are normalized to UTC timezone (`Z`) during Arrow encoding
- **TIME WITH TIME ZONE**: All values are normalized to UTC offset (`Z`) during Arrow encoding  
- **Precision Support**: All Arrow-supported precision levels (seconds, milliseconds, microseconds, nanoseconds) are implemented and tested for both types
- **Picosecond Handling**: Trino's 12-decimal precision (picoseconds) is automatically cast to 9-decimal precision (nanoseconds) for Arrow compatibility.
- **Loss of Original Timezone**: The original timezone information is not preserved in the Arrow format

### Supported Data Types

The following Trino data types are currently supported and tested with Arrow spooling:

#### Primitive Types
- **BIGINT** - 64-bit signed integers  
- **INTEGER** - 32-bit signed integers
- **SMALLINT** - 16-bit signed integers
- **TINYINT** - 8-bit signed integers
- **DOUBLE** - Double-precision floating-point
- **REAL** - Single-precision floating-point  
- **BOOLEAN** - Boolean values

#### String and Binary Types
- **VARCHAR** - Variable-length character strings
- **CHAR** - Fixed-length character strings
- **VARBINARY** - Variable-length binary data
- **JSON** - JSON documents (stored as UTF-8 strings)

#### Date and Time Types
- **DATE** - Calendar dates
- **DECIMAL** - Fixed-precision decimal numbers
- **TIME** - Time of day (all Arrow-supported precisions: seconds, milliseconds, microseconds, nanoseconds)
- **TIME WITH TIME ZONE** - Time with timezone (all Arrow-supported precisions: seconds, milliseconds, microseconds, nanoseconds)
- **TIMESTAMP** - Date and time (all precisions: seconds, milliseconds, microseconds, nanoseconds; picoseconds cast to nanoseconds)  
- **TIMESTAMP WITH TIME ZONE** - Timestamp with timezone (all precisions: seconds, milliseconds, microseconds, nanoseconds; picoseconds cast to nanoseconds)
- **INTERVAL DAY TIME** - Day-time intervals

*Note: Trino supports up to 12-decimal precision (picoseconds), but for Arrow compatibility, picosecond precision is automatically cast to nanosecond precision with graceful precision reduction.*

#### Special Types
- **UUID** - Universally unique identifiers (encoded as Arrow extension type `arrow.uuid`)
- **UNKNOWN** - Null-only type

#### Complex Types
- **ARRAY** - Variable-length arrays of elements
- **MAP** - Key-value mappings
- **ROW** - Structured records with named fields (structs)

### Unsupported Data Types (TODO)

The following Trino data types are currently **not supported** with Arrow spooling:

#### Advanced Analytics Types
- **HYPERLOGLOG** - Probabilistic cardinality estimation data structure
- **QDIGEST** - Quantile digest for approximate percentile calculations  
- **TDIGEST** - T-Digest for approximate percentile calculations
- **SET_DIGEST** - Set digest for approximate set operations
- **P4_HYPER_LOG_LOG** - P4 HyperLogLog for cardinality estimation

#### Geometric and Spatial Types
- **GEOMETRY** - Geometric shapes and spatial data
- **SPHERICAL_GEOGRAPHY** - Spherical geography data for geographic calculations
- **BING_TILE** - Bing Maps tile system coordinates
- **KDB_TREE** - Spatial indexing tree structure

#### Network and Identifier Types
- **IPADDRESS** - IPv4 and IPv6 address types
- **COLOR** - Color representation type

#### Legacy Types  
- **JSON2016** - Legacy JSON type (superseded by standard JSON)

*Note: These types will throw an `UnsupportedOperationException` when used with Arrow spooling. Consider using alternative supported types or falling back to JSON spooling for queries involving these data types.*

#### Tests

- TestArrowEncodingUtils.java verifies unittest round trip from Trino page to Arrow vectors 
- Ourdesign choice results in expected test failures in `TestArrowSpooledDistributedQueries.java` where assertions check for preservation of original timezone information:

```java
// Example: Expected assertion failure
assertThatThrownBy(super::testTimestampWithTimeZoneLiterals)
    .hasMessageContaining("expected: 1960-01-22T03:04:05+06:00")  // Original timezone
    .hasMessageContaining("but was: 1960-01-21T21:04:05Z");       // UTC normalized
```

#### Alternative Approach

An alternative implementation could serialize timezone-aware values as Arrow structs containing:
- A naive timestamp component (without timezone)
- A separate timezone attribute field

This approach would preserve timezone information but would require additional complexity in both encoding and decoding logic. Most people may prefer performance and simplicity of the serialization/deserialization over 100% feature matching with the existing JSON serialization
Both approaches could coexist based on user configuration. 

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
* Java 24.0.1+, 64-bit
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
* In the SDKs section, ensure that JDK 24 is selected (create one if none exist)
* In the Project section, ensure the Project language level is set to 24

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
* VM Options: `-ea -Dconfig=etc/config.properties -Dlog.levels-file=etc/log.properties -Djdk.attach.allowAttachSelf=true --sun-misc-unsafe-memory-access=allow`
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
