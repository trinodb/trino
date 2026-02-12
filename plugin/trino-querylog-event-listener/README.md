# Trino Query Log Event Listener

This is a Trino plugin that logs query events to a file using Airlift Logger.

## Configuration

The Query Log Event Listener can be configured through Trino properties. Add the following properties to your Trino configuration:

### Basic Configuration

```properties
# Enable the query log event listener plugin
event-listener.type=querylog

# Log query created events
querylog-event-listener.log-created=true

# Log query completed events
querylog-event-listener.log-completed=true

# Log query executed events
querylog-event-listener.log-executed=true

# Path to the log file (optional, default: querylog.log)
querylog-event-listener.log-file-path=/var/log/trino/querylog.log
```

### Advanced Configuration: Field Control

#### Exclude Sensitive Fields

Exclude specific fields from being logged (their values will be replaced with null):

```properties
# Comma-separated list of fields to exclude
# Example: payload,user,sourceCode,password
querylog-event-listener.excluded-fields=payload,user,sourceCode
```

#### Truncate Large Fields

Control the maximum size of field values to prevent excessive log sizes:

```properties
# Maximum field size before truncation (default: 4KB)
# Supported units: B, KB, MB, GB
querylog-event-listener.max-field-size=8KB
```

#### Selective Field Truncation (by Size)

Truncate specific fields (like `query` and `stageInfo`) if they exceed a size limit:

```properties
# Comma-separated list of field names to truncate
# These fields will be truncated if they exceed the truncation-size-limit
querylog-event-listener.truncated-fields=query,stageInfo,sourceCode

# Maximum size for truncated fields (default: 2KB)
# Supported units: B, KB, MB, GB
querylog-event-listener.truncation-size-limit=2KB
```

When a field exceeds the truncation size limit, it will be truncated with a `...[TRUNCATED]` suffix appended.

### Event Filtering

Filter out events from being logged based on specific attributes. This is useful for reducing log noise by excluding events you don't care about.

#### Ignore by Query State

Skip logging events for specific query states:

```properties
# Comma-separated list of query states to ignore
# Examples: RUNNING, QUEUED, WAITING, PLANNING, FINISHING, FINISHED, FAILED, CANCELED
querylog-event-listener.ignored-query-states=RUNNING,QUEUED
```

#### Ignore by Update Type

Skip logging for specific update types (INSERT, UPDATE, DELETE, etc.):

```properties
# Comma-separated list of update types to ignore
querylog-event-listener.ignored-update-types=INSERT,UPDATE,DELETE
```

#### Ignore by Query Type

Skip logging for specific query types (DML, DDL, UTILITY, EXPLAIN, etc.):

```properties
# Comma-separated list of query types to ignore
querylog-event-listener.ignored-query-types=UTILITY,EXPLAIN
```

#### Ignore by Failure Type

Skip logging for specific failure types (only applies to QueryCompletedEvent):

```properties
# Comma-separated list of failure types to ignore
# Examples: USER_ERROR, INTERNAL_ERROR, EXTERNAL, INSUFFICIENT_RESOURCES
querylog-event-listener.ignored-failure-types=USER_ERROR
```

### Complete Configuration Example

```properties
# Enable events
event-listener.type=querylog
querylog-event-listener.log-created=true
querylog-event-listener.log-completed=true
querylog-event-listener.log-executed=false

# Log file path
querylog-event-listener.log-file-path=/var/log/trino/querylog.log

# Exclude sensitive information
querylog-event-listener.excluded-fields=payload,user,password,authorizationToken,sourceCode

# Limit field sizes
querylog-event-listener.max-field-size=16KB

# Truncate specific large fields
querylog-event-listener.truncated-fields=query,stageInfo,plan
querylog-event-listener.truncation-size-limit=2KB

# Ignore certain events
querylog-event-listener.ignored-query-states=RUNNING,QUEUED
querylog-event-listener.ignored-update-types=INSERT
querylog-event-listener.ignored-query-types=UTILITY,EXPLAIN
querylog-event-listener.ignored-failure-types=USER_ERROR
```

## Logged Events

The plugin logs the following event types as JSON:

- **QueryCreatedEvent**: Logged when a query is created
- **QueryCompletedEvent**: Logged when a query completes (successfully or with error)
- **QueryExecutionEvent**: Logged for query execution events

## Log Output

Each event is logged as a JSON string with the prefix indicating the event type:
- `QUERY_CREATED: {json}`
- `QUERY_COMPLETED: {json}`
- `QUERY_EXECUTED: {json}`

## Features

### Field Exclusion
When a field is in the excluded fields list, its value will be replaced with `null` in the logged output. This is useful for:
- Sensitive information (passwords, tokens, API keys)
- PII (personally identifiable information)
- Large payloads that aren't needed for logging

### Field Size Limiting
Generic limit on all field values. Fields larger than the configured `max-field-size` will be completely truncated.

### Selective Field Truncation
Selectively truncate specific fields (like `query`, `stageInfo`, or `plan`) to a configurable byte limit. This allows you to:
- Preserve most of the field content (truncated, not excluded)
- Keep important metadata for debugging
- Control log size for specific verbose fields
- Works independently from field exclusion

### Event Filtering
Filter out events from logging based on specific attributes (query state, update type, query type, failure type). This helps:
- Reduce log noise and storage
- Focus on specific event types
- Skip routine operations (e.g., UTILITY queries, RUNNING state)
- Ignore common error types

## Configuration File Example

Create or modify `etc/event-listener.properties`:

```properties
event-listener.type=querylog
querylog-event-listener.log-created=true
querylog-event-listener.log-completed=true
querylog-event-listener.log-executed=false
querylog-event-listener.log-file-path=/var/log/trino/querylog.log
querylog-event-listener.excluded-fields=payload,sourceCode,password
querylog-event-listener.max-field-size=4KB
querylog-event-listener.truncated-fields=query,stageInfo
querylog-event-listener.truncation-size-limit=2KB
querylog-event-listener.ignored-query-states=RUNNING,QUEUED
querylog-event-listener.ignored-query-types=UTILITY
```

Then include this in `etc/config.properties`:

```properties
event-listener.config-file=/path/to/event-listener.properties
```

## Building

Build the plugin with Maven:

```bash
mvn clean package
```

## Installation

Copy the built JAR file to the Trino plugins directory:

```bash
cp target/trino-querylog-event-listener-*.jar $TRINO_HOME/plugin/trino-querylog-event-listener/
```

## Usage Examples

### Example 1: Minimal Logging Setup

Log all events with minimal configuration:

```properties
event-listener.type=querylog
querylog-event-listener.log-created=true
querylog-event-listener.log-completed=true
querylog-event-listener.log-executed=true
```

### Example 2: Privacy-Focused Setup

Exclude sensitive information while keeping full query details:

```properties
event-listener.type=querylog
querylog-event-listener.log-created=true
querylog-event-listener.log-completed=true

# Exclude PII and sensitive data
querylog-event-listener.excluded-fields=user,principal,password,authorizationToken,clientInfo,remoteClientAddress

# Keep full content for non-sensitive fields
querylog-event-listener.max-field-size=32KB
```

### Example 3: Performance-Focused Setup

Truncate large fields to reduce log size and improve performance:

```properties
event-listener.type=querylog
querylog-event-listener.log-created=true
querylog-event-listener.log-completed=true

# Truncate verbose fields
querylog-event-listener.truncated-fields=query,plan,stageInfo,failures,warnings,operatorSummaries
querylog-event-listener.truncation-size-limit=1KB

# Generic field size limit
querylog-event-listener.max-field-size=8KB
```

### Example 4: Noise Reduction Setup

Filter out routine events to focus on important queries:

```properties
event-listener.type=querylog
querylog-event-listener.log-created=false
querylog-event-listener.log-completed=true
querylog-event-listener.log-executed=false

# Ignore routine query states
querylog-event-listener.ignored-query-states=RUNNING,QUEUED

# Ignore maintenance/utility operations
querylog-event-listener.ignored-query-types=UTILITY

# Ignore only successful completions without errors
# (only log failed queries)
querylog-event-listener.ignored-failure-types=
```

### Example 5: Comprehensive Production Setup

Balanced setup for production environments:

```properties
event-listener.type=querylog
querylog-event-listener.log-created=true
querylog-event-listener.log-completed=true
querylog-event-listener.log-executed=false

# Log file path
querylog-event-listener.log-file-path=/var/log/trino/querylog.log

# Exclude sensitive data
querylog-event-listener.excluded-fields=user,principal,password,session_properties,clientInfo

# Limit overall field sizes
querylog-event-listener.max-field-size=16KB

# Truncate specific large fields
querylog-event-listener.truncated-fields=query,plan,stageInfo,failures,operatorSummaries
querylog-event-listener.truncation-size-limit=2KB

# Filter out noise
querylog-event-listener.ignored-query-states=RUNNING
querylog-event-listener.ignored-query-types=UTILITY,EXPLAIN
querylog-event-listener.ignored-update-types=
querylog-event-listener.ignored-failure-types=USER_ERROR
```

### Example 6: Development/Debugging Setup

Capture full details for debugging:

```properties
event-listener.type=querylog
querylog-event-listener.log-created=true
querylog-event-listener.log-completed=true
querylog-event-listener.log-executed=true

# Log file path
querylog-event-listener.log-file-path=/tmp/trino-querylog.log

# No field exclusions for full visibility
querylog-event-listener.excluded-fields=

# Large field size limit
querylog-event-listener.max-field-size=64KB

# No truncation - keep everything
querylog-event-listener.truncated-fields=

# Don't filter any events
querylog-event-listener.ignored-query-states=
querylog-event-listener.ignored-query-types=
querylog-event-listener.ignored-update-types=
querylog-event-listener.ignored-failure-types=
```

## Sample Log Output

### QueryCreatedEvent

```
QUERY_CREATED: {"queryId":"20260211_123456_00000_abcde","user":"analytics_user","queryState":"CREATED","query":"SELECT * FROM table1 WHERE id > 100","catalog":"hive","schema":"default","queryType":"SELECT"}
```

### QueryCompletedEvent (Successful)

```
QUERY_COMPLETED: {"queryId":"20260211_123456_00000_abcde","user":null,"queryState":"FINISHED","errorCode":null,"errorType":null,"failureType":null,"cpuTimeMillis":1250,"wallTimeMillis":2500,"peakMemoryBytes":5242880,"outputRows":15000}
```

### QueryCompletedEvent (Failed)

```
QUERY_COMPLETED: {"queryId":"20260211_123456_00000_bcdef","user":null,"queryState":"FAILED","errorCode":"OPTIMIZER","errorType":"OPTIMIZER_TIMEOUT","failureType":"SYSTEM_ERROR","failureMessage":"Query optimizer timeout exceeded","cpuTimeMillis":850,"wallTimeMillis":5000}
```

### With Truncated Fields

```
QUERY_CREATED: {"queryId":"20260211_123456_00000_cdefg","query":"SELECT col1, col2 FROM very_long_table_name WHERE condition1 = true AND condition2 = false AND...[TRUNCATED]","plan":"Fragment 0 [source: {2} -> output]\n  Output layout: [col1, col2]\n  Output partitioning: SINGLE\n  - Project[projectLocations: [Column{name...[TRUNCATED]"}
```

## Dependencies

The plugin uses:
- **Airlift**: For logging and configuration
- **Jackson**: For JSON serialization
- **Trino SPI**: For event listener interface

