# OpenTelemetry Java agent integration

This guide explains how Trino integrates with the OpenTelemetry Java agent.

## Overview

Trino can automatically integrate with the [OpenTelemetry Java agent](https://opentelemetry.io/docs/zero-code/java/agent/) when detected. This provides seamless correlation between Trino's manual instrumentation (query planning, metadata operations, optimizer) and auto-instrumented operations (HTTP requests, JDBC calls, etc.).

### Why integrate with the Java agent?

While Trino's built-in tracing provides valuable manual instrumentation for business-level operations, the OpenTelemetry Java agent unlocks comprehensive observability:

**Trino's manual spans** (what you get with `tracing.enabled=true`):
- Query lifecycle (`query`, `analyzer`, `optimize`, `scheduler`)
- Metadata operations (`Metadata.getTableHandle`, `Metadata.listTables`)
- Access control checks (`AccessControl.*`)
- Split processing and data exchange

**Java agent auto-instrumentation** (what you get with `-javaagent`):
- HTTP client/server calls (Jetty, OkHttp, Apache HttpClient)
- JDBC connections and queries
- gRPC calls
- Kafka producers/consumers
- Redis operations
- S3/GCS/Azure storage operations
- JVM metrics, GC metrics, thread pools
- Application logs with trace correlation

**Together**: Business-level spans correlated with infrastructure-level spans via shared trace IDs.
All telemetry signals (traces, metrics, logs) share the same resource attributes, enabling unified
filtering, grouping, and dashboards. Additionally, logs are correlated with traces by trace/span IDs.
You get to see not just that a query ran, but every HTTP call, database connection, and storage
operation involved in executing it - all correlated and filterable.

The Java agent's [library support](https://github.com/open-telemetry/opentelemetry-java-instrumentation/blob/main/docs/supported-libraries.md) is extensive and includes dozens of commonly used libraries. You can [selectively disable instrumentations](https://opentelemetry.io/docs/zero-code/java/agent/disable/) to control overhead.

When the Java agent is present, Trino uses the globally registered OpenTelemetry instance instead of creating a separate one. This means all spans share the same:

- Exporter configuration (configured via `OTEL_*` env vars or `-Dotel.*` system properties)
- Resource attributes
- Span processors
- Sampling configuration

## Architecture

### Without Java agent

```
┌─────────────────────────────────────┐
│ TracingModule (airlift)             │
│ ┌─────────────────────────────────┐ │
│ │ OpenTelemetry SDK               │ │
│ │ - Resource: service.name=trino  │ │
│ │ - Exporter: configured via      │ │
│ │   tracing.exporter.endpoint     │ │
│ └─────────────────────────────────┘ │
└─────────────────────────────────────┘
         │
         ├─→ Manual spans (query, Metadata.*, etc.)
         └─→ Export to collector
```

### With Java agent

```
┌─────────────────────────────────────────────────────┐
│ OpenTelemetry Java Agent (GlobalOpenTelemetry)      │
│ ┌─────────────────────────────────────────────────┐ │
│ │ OpenTelemetry SDK                               │ │
│ │ - Resource: service.name, environment, cluster, │ │
│ │            namespace, custom attributes, etc.   │ │
│ │ - Exporter: configured via OTEL_* env vars or   │ │
│ │            -Dotel.* system properties           │ │
│ └─────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────┘
         │
         ├─→ Auto-instrumented spans (HTTP, JDBC, etc.)
         └─→ Manual spans (query, Metadata.*, etc.)
         └─→ Export to collector (single exporter)
```

## Implementation

### ConditionalTracingModule

The `ConditionalTracingModule` replaces the direct usage of `TracingModule` in `Server.java`. On startup, it:

1. Checks for system property override (`-Dtracing.use-global`)
2. If not set, checks environment variable (`TRACING_USE_GLOBAL`)
3. If not set, auto-detects by calling `GlobalOpenTelemetry.get()`
4. If global instance is not noop, uses `GlobalOpenTelemetryModule`
5. Otherwise, falls back to `TracingModule`

Logs the decision:
```
INFO  ConditionalTracingModule  GlobalOpenTelemetry detected (likely from Java agent)
INFO  ConditionalTracingModule  Using GlobalOpenTelemetry
```

Or:
```
DEBUG ConditionalTracingModule  GlobalOpenTelemetry is noop, will use dedicated instance
INFO  ConditionalTracingModule  Using dedicated TracingModule
```

### GlobalOpenTelemetryModule

Simple Guice module that:
1. Binds `GlobalOpenTelemetry.get()` as the `OpenTelemetry` instance
2. Creates a `Tracer` with Trino's service name and version
3. That's it - all configuration comes from the global instance

```java
OpenTelemetry openTelemetry = GlobalOpenTelemetry.get();
binder.bind(OpenTelemetry.class).toInstance(openTelemetry);

Tracer tracer = openTelemetry.getTracer(serviceName, serviceVersion);
binder.bind(Tracer.class).toInstance(tracer);
```

## Testing

### Unit tests

`TestConditionalTracingModule` verifies:
- Auto-detection when `GlobalOpenTelemetry` is initialized
- Fallback to `TracingModule` when not initialized
- System property override works correctly
- Environment variable override works correctly

### Integration testing

To test with a real Java agent:

```bash
# Download OpenTelemetry Java agent
wget https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/latest/download/opentelemetry-javaagent.jar

# Option 1: Configure via environment variables
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
export OTEL_RESOURCE_ATTRIBUTES=service.name=trino,environment=test

java -javaagent:opentelemetry-javaagent.jar \
     -jar core/trino-server/target/trino-server-*.jar

# Option 2: Configure via system properties
java -javaagent:opentelemetry-javaagent.jar \
     -Dotel.exporter.otlp.endpoint=http://localhost:4317 \
     -Dotel.resource.attributes=service.name=trino,environment=test \
     -jar core/trino-server/target/trino-server-*.jar

# Verify in logs:
# "Using GlobalOpenTelemetry (Java agent or external initialization detected)"

# Run a query and check traces
# All spans should have resource.environment=test
```

## Override mechanism

For advanced use cases or debugging:

### Force use of global instance

```bash
java -Dtracing.use-global=true -jar trino-server.jar
# Or
export TRACING_USE_GLOBAL=true
```

Use case: Force global even if auto-detection fails

### Force use of dedicated TracingModule

```bash
java -Dtracing.use-global=false -javaagent:agent.jar -jar trino-server.jar
# Or
export TRACING_USE_GLOBAL=false
```

Use cases:
- Debugging: Compare behavior with/without shared instance
- Migration: Gradually roll out the change
- Workaround: If there's an issue with Java agent integration

## Troubleshooting

### How do I know which mode is active?

Check the logs on startup:
```
INFO  ConditionalTracingModule  Using GlobalOpenTelemetry
```
Or:
```
INFO  ConditionalTracingModule  Using dedicated TracingModule
```

### Resource attributes from Java agent not appearing on Trino spans

1. Verify Java agent is loaded: check for "Using GlobalOpenTelemetry" in logs
2. Verify resource attributes are set in agent:
   ```bash
   export OTEL_RESOURCE_ATTRIBUTES=test=value
   # OR
   java -Dotel.resource.attributes=test=value ...
   ```
3. Check traces: query for `resource.test=value`

If still not working, try explicit override: `-Dtracing.use-global=true`

### SSL certificate errors with Java agent

Configure SSL once in the JVM (applies to both agent and Trino):
```bash
-Djavax.net.ssl.trustStore=/path/to/truststore.jks
-Djavax.net.ssl.trustStorePassword=password
```

### Want to disable auto-detection

Set `-Dtracing.use-global=false` to always use dedicated TracingModule.

## Migration guide

### Current: Using both Java agent and tracing.enabled=true

**Before:**
```properties
# config.properties
tracing.enabled=true
tracing.exporter.endpoint=https://collector:4317
```

Plus Java agent with duplicate config.

**After (recommended):**
```bash
# Remove from config.properties:
# tracing.enabled=true
# tracing.exporter.endpoint=...

# Configure via Java agent only (environment variables)
export OTEL_EXPORTER_OTLP_ENDPOINT=https://collector:4317
export OTEL_RESOURCE_ATTRIBUTES=service.name=trino,...

# OR via system properties
java -Dotel.exporter.otlp.endpoint=https://collector:4317 \
     -Dotel.resource.attributes=service.name=trino,... \
     -javaagent:agent.jar ...
```

**After (alternative - keep existing config):**

Keep config.properties as-is, but be aware:
- Auto-detection will use Java agent's instance
- `tracing.exporter.endpoint` in config.properties is ignored
- To use config.properties values, set `-Dtracing.use-global=false`

## See also

- [OpenTelemetry configuration](opentelemetry) - User-facing documentation
- [OpenTelemetry Java agent documentation](https://opentelemetry.io/docs/zero-code/java/agent/)
- [GlobalOpenTelemetry JavaDoc](https://www.javadoc.io/doc/io.opentelemetry/opentelemetry-api/latest/io/opentelemetry/api/GlobalOpenTelemetry.html)

