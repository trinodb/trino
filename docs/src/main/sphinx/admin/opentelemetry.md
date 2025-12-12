# Observability with OpenTelemetry

Trino exposes tracing information for observability of a running Trino
deployment for the widely used [OpenTelemetry](https://opentelemetry.io/)
collection of APIs, SDKs, and tools. You can use OpenTelemetry to instrument,
generate, collect, and export telemetry data such as metrics, logs, and traces
to help you analyze application performance and behavior. More information about
the observability and the concepts involved is available in the [OpenTelemetry
documentation](https://opentelemetry.io/docs/concepts/).

The integration of OpenTelemetry with Trino enables tracing Trino behavior and
performance. You can use it to diagnose the overall application as well as
processing of specific queries or other narrower aspects.

Trino emits trace information from the coordinator and the workers. Trace
information includes the core system such as the query planner and the
optimizer, and a wide range of connectors and other plugins.

Trino uses any supplied trace identifiers from client tools across the cluster.
If none are supplied, trace identifiers are created for each query. The
identifiers are propagated to data sources, metastores, and other connected
components. As a result you can use this distributed tracing information to follow
all the processing flow of a query from a client tool, through the
coordinator and all workers to the data sources and other integrations.

If you want to receive traces from data sources and other integrations, these
tools must also support OpenTelemetry tracing and use the supplied identifiers
from Trino to propagate the context. Tracing must be enabled separately on these
tools.

## Configuration

Use tracing with OpenTelemetry by enabling it and configuring the endpoint in
the [config.properties file](config-properties):

```properties
tracing.enabled=true
tracing.exporter.endpoint=http://observe.example.com:4317
```

Tracing is not enabled by default. The exporter endpoint must specify a URL that
is accessible from the coordinator and all workers of the cluster. The preceding
example uses a observability platform deployment available by
HTTP at the host `observe.example.com`, port `4317`.

Use the `tracing.exporter.protocol` property to configure the protocol for exporting traces. 
Defaults to the gRPC protocol with the `grpc` value. Set the value to `http/protobuf` for 
exporting traces using protocol buffers with HTTP transport.

## Integration with OpenTelemetry Java agent

Trino automatically integrates with the [OpenTelemetry Java agent](https://opentelemetry.io/docs/zero-code/java/agent/)
when detected. This provides seamless correlation between:

- Trino's manual instrumentation (query planning, metadata operations, etc.)
- Auto-instrumented operations (HTTP requests, JDBC calls, etc.)

When the Java agent is present, Trino uses the globally registered OpenTelemetry instance
instead of creating a separate one. This means:

- All spans share the same exporter configuration
- Resource attributes configured in the agent (service name, environment, etc.) apply to all spans
- No duplicate OpenTelemetry instances or exporters
- Single point of configuration for telemetry

### Benefits of Java agent integration

The OpenTelemetry Java agent provides **comprehensive, automatic observability** beyond just traces:

- **Traces**: Automatically instruments HTTP clients/servers, JDBC, gRPC, Kafka, Redis, and
  [many more libraries](https://github.com/open-telemetry/opentelemetry-java-instrumentation/blob/main/docs/supported-libraries.md)
- **Metrics**: JVM metrics, HTTP server metrics, connection pool metrics, garbage collection, etc.
- **Logs**: Automatic correlation of application logs with trace IDs

**All three signals share the same resource attributes** (service name, environment, cluster, etc.),
enabling unified filtering and grouping. **Additionally, traces and logs are correlated** by trace
ID and span ID, allowing you to jump from a log line to its corresponding trace.

The agent instruments at the library level, capturing operations such as:
- Network calls to metastores (Hive, Glue, etc.)
- Object storage operations (S3, GCS, Azure)
- Internal HTTP communication between coordinator and workers
- Database connections and queries
- JVM performance metrics

**Granular control**: While the default configuration instruments many libraries, you can
[selectively disable specific instrumentations](https://opentelemetry.io/docs/zero-code/java/agent/disable/)
to balance visibility with overhead.

By integrating with the Java agent, Trino's intentional business-level spans (query lifecycle,
optimizer decisions) are automatically correlated with infrastructure-level spans (HTTP requests,
database calls) via shared trace IDs. All telemetry signals share the same resource attributes,
enabling unified filtering and dashboards.

### Using the Java agent

To use the OpenTelemetry Java agent with Trino:

1. Download the [OpenTelemetry Java agent](https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases)

2. Configure the agent with your observability backend (via environment variables or system properties):

```bash
# Option 1: Environment variables
export OTEL_EXPORTER_OTLP_ENDPOINT=https://your-collector:4317
export OTEL_RESOURCE_ATTRIBUTES=service.name=trino,environment=production,cluster=us-west

# Option 2: System properties
java -Dotel.exporter.otlp.endpoint=https://your-collector:4317 \
     -Dotel.resource.attributes=service.name=trino,environment=production,cluster=us-west \
     ...
```

3. Start Trino with the agent:

```bash
java -javaagent:path/to/opentelemetry-javaagent.jar \
     -jar trino-server.jar
```

4. Do not configure `tracing.enabled` or `tracing.exporter.endpoint` in `config.properties`,
   as these are handled by the Java agent.

When using the Java agent, Trino automatically detects the global OpenTelemetry instance and uses
it for all manual instrumentation. If you need to explicitly control this behavior, you can use
a system property or environment variable:

```bash
# Force use of global OpenTelemetry (auto-detected by default)
java -Dtracing.use-global=true -javaagent:opentelemetry-javaagent.jar ...

# Or via environment variable
export TRACING_USE_GLOBAL=true

# Force use of dedicated TracingModule (ignore Java agent)
java -Dtracing.use-global=false ...
```

### Standalone configuration

If you are not using the OpenTelemetry Java agent, configure tracing directly in Trino as shown
in the previous section. Trino will create a dedicated OpenTelemetry instance with the specified
exporter configuration.

## Example use

The following steps provide a simple demo setup to run the open source
observability platform [Jaeger](https://www.jaegertracing.io/) and Trino locally
in Docker containers.

Create a shared network for both servers called `platform`:

```shell
docker network create platform
```

Start Jaeger in the background:

```shell
docker run -d \
  --name jaeger \
  --network=platform \
  --network-alias=jaeger \
  -e COLLECTOR_OTLP_ENABLED=true \
  -p 16686:16686 \
  -p 4317:4317 \
  jaegertracing/all-in-one:latest
```

The preceding command adds Jaeger to the `platform` network with the hostname
`jaeger`. It also maps the endpoint and Jaeger UI ports.

Create a `config.properties` file that uses the default setup from the Trino
container, and adds the tracing configuration with the `jaeger` hostname:

```properties
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
tracing.enabled=true
tracing.exporter.endpoint=http://jaeger:4317
```

Start Trino in the background:

```shell
docker run -d \
  --name trino \
  --network=platform \
  -p 8080:8080 \
  --mount type=bind,source=$PWD/config.properties,target=/etc/trino/config.properties \
  trinodb/trino:latest
```

The preceding command adds Trino to the `platform` network. It also mounts the
configuration file into the container so that tracing is enabled.

Now everything is running.

Install and run the [Trino CLI](/client/cli) or any other client application and
submit a query such as `SHOW CATALOGS;` or `SELECT * FROM tpch.tiny.nation;`.

Optionally, log into the [Trino Web UI](/admin/web-interface) at
[http://localhost:8080](http://localhost:8080) with a random username. Press
the **Finished** button and inspect the details for the completed queries.

Access the Jaeger UI at [http://localhost:16686/](http://localhost:16686/),
select the service `trino`, and press **Find traces**.

As a next step, run more queries and inspect more traces with the Jaeger UI.

Once you are done you can stop the containers:

```shell
docker stop trino
docker stop jaeger
```

You can start them again for further testing:

```shell
docker start jaeger
docker start trino
```

Use the following commands to completely remove the network and containers:

```shell
docker rm trino
docker rm jaeger
docker network rm platform
```
