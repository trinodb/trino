
# Logging

Trino include numerous features to better understand and monitor a running
system, such as [](/admin/opentelemetry) or [](/admin/jmx). Logging and
configuring logging is one important aspect for operating and troubleshooting
Trino.

(logging-configuration)=
## Configuration

Trino application logging is optional and configured in the `log.properties`
file in your Trino installation `etc` configuration directory as set by the
[launcher](running-trino).

Use it to add specific loggers and configure the minimum log levels. Every
logger has a name, which is typically the fully qualified name of the class that
uses the logger. Loggers have a hierarchy based on the dots in the name, like
Java packages. The four log levels are `DEBUG`, `INFO`, `WARN` and `ERROR`,
sorted by decreasing verbosity.

For example, consider the following log levels file:

```properties
io.trino=WARN
io.trino.plugin.iceberg=DEBUG
io.trino.parquet=DEBUG
```

The preceding configuration sets the changes the level for all loggers in the
`io.trino` namespace to `WARN` as an update from the default `INFO` to make
logging less verbose. The example also increases logging verbosity for the
Iceberg connector using the `io.trino.plugin.iceberg` namespace, and the Parquet
file reader and writer support located in the `io.trino.parquet` namespace to
`DEBUG` for troubleshooting purposes. 

Additional loggers can include other package namespaces from libraries and
dependencies embedded within Trino or part of the Java runtime, for example:

* `io.airlift` for the [Airlift](https://github.com/airlift/airlift) application
  framework used by Trino.
* `org.eclipse.jetty` for the [Eclipse Jetty](https://jetty.org/) web server
  used by Trino.
* `org.postgresql` for the [PostgresSQL JDBC driver](https://github.com/pgjdbc)
  used by the PostgreSQL connector.
* `javax.net.ssl` for TLS from the Java runtime.
* `java.io` for I/O operations.

There are numerous additional properties available to customize logging in
[](config-properties), with details documented in [](/admin/properties-logging)
and in following example sections.

## Log output

By default, logging output is file-based with rotated files in `var/log`:

* `launcher.log` for logging out put from the application startup from the
  [launcher](running-trino). Only used if the launcher starts Trino in the
  background, and therefore not used in the Trino container.
* `http-request.log` for HTTP request logs, mostly from the [client
  protocol](/client/client-protocol) and the [Web UI](/admin/web-interface).
* `server.log` for the main application log of Trino, including logging from all
  plugins.

## JSON and TCP channel logging

Trino supports logging to JSON-formatted output files with the configuration
`log.format=json`. Optionally you can set `node.annotations-file` as path to a
properties file such as the following example:

```properties
host_ip=1.2.3.4
service_name=trino
node_name=${ENV:MY_NODE_NAME}
pod_name=${ENV:MY_POD_NAME}
pod_namespace=${ENV:MY_POD_NAMESPACE}
```

The annotations file supports environment variable substitution, so that the
above example attaches the name of the Trino node as `pod_name` and other
information to every log line. When running Trino on Kubernetes, you have access
to [a lot of information to use in the
log](https://kubernetes.io/docs/tasks/inject-data-application/environment-variable-expose-pod-information/).

TCP logging allows you to log to a TCP socket instead of a file with the
configuration `log.path=tcp://<server_ip>:<server_port>`. The endpoint must be
available at the URL configured with `server_ip` and `server_port` and is
assumed to be stable.

You can use an application such as [fluentbit](https://fluentbit.io/) as a
consumer for these JSON-formatted logs. 

Example fluentbit configuration file `config.yaml`:

```yaml
pipeline:
  inputs:
  - name: tcp
    tag: trino
    listen: 0.0.0.0
    port: 5170
    buffer_size: 2048
    format: json
  outputs:
  - name: stdout
    match: '*'
```

Start the application with the command:

```shell
fluent-bit -c config.yaml
```

Use the following Trino properties configuration:

```properties
log.path=tcp://localhost:5170
log.format=json
node.annotation-file=etc/annotations.properties
```

File `etc/annotation.properties`:

```properties
host_ip=1.2.3.4
service_name=trino
pod_name=${ENV:HOSTNAME}
```

As a result, Trino logs appear as structured JSON log lines in fluentbit in the
user interface, and can also be [forwarded into a configured logging
system](https://docs.fluentbit.io/manual/pipeline/outputs).
