# OpenLineage event listener

The OpenLineage event listener plugin enables streaming of lineage information,
encoded in JSON format and aligned with the OpenLineage specification,
to external OpenLineage-compatible endpoints, including HTTP APIs (via POST to a specified URI)
and Kafka-compatible systems.

## Rationale

This event listener is aiming to capture every query that creates or modifies
Trino tables and transform it into lineage
information. Linage can be understood as relationship/flow between data/tables.
OpenLineage is a widely used open-source
standard for capturing lineage information from variety of system including (but
not limited to) Spark, Airflow, Flink.

:::{list-table} Trino Query attributes mapping to OpenLineage attributes
:widths: 40, 40
:header-rows: 1

*
    - Trino
    - OpenLineage
*
    - `{UUIDv7(Query.createTime, hash(Query.Id))}`
    - Run ID
*
    - `{queryCreatedEvent.getCreateTime()} or {queryCompletedEvent.getEndTime()} `
    - Run Event Time
*
    - Query Id
    - Job Facet Name (default, can be overriden)
*
    - `trino:// + {openlineage-event-listener.trino.uri.getHost()} + ":" + {openlineage-event-listener.trino.uri.getPort()}`
    - Job Facet Namespace (default, can be overridden)
*
    - `{schema}.{table}`
    - Dataset Name
*
    - `trino:// + {openlineage-event-listener.trino.uri.getHost()} + ":" + {openlineage-event-listener.trino.uri.getPort()}`
    - Dataset Namespace

:::

(trino-facets)=
  
### Available Trino Facets

#### Trino Metadata

Facet containing properties (if present):

- `queryPlan`
- `transactionId` - transaction id used for query processing

related to query based on which OpenLineage Run Event was generated.

Available in both `Start` and `Complete/Fail` OpenLineage events.

If you want to disable this facet, add `trino_metadata` to 
`openlineage-event-listener.disabled-facets`.

#### Trino Query Context

Facet containing properties:

- `serverVersion` - version of Trino server that was used to process the query
- `environment` - inherited from `node.environment` of [](node-properties)
- `queryType` - one of query types configured via 
  `openlineage-event-listener.trino.include-query-types`

related to query based on which OpenLineage Run Event was generated.

Available in both `Start` and `Complete/Fail` OpenLineage events.

If you want to disable this facet, add `trino_query_context` to
`openlineage-event-listener.disabled-facets`.

#### Trino Query Statistics

Facet containing full contents of query statistics of completed. Available only
in OpenLineage `Complete/Fail` events.

If you want to disable this facet, add `trino_query_statistics` to
`openlineage-event-listener.disabled-facets`.

(openlineage-event-listener-requirements)=

## Requirements

You need to perform the following steps:
- Provide an HTTP/S service that accepts POST events or Kafka system capable of receiving events in OpenLineage API format.
- Configure `openlineage-event-listener.transport.url` in the event listener
  properties file with the URI of the service
- Configure `openlineage-event-listener.trino.uri` so proper OpenLineage job 
  namespace is render within produced events. Needs to be proper uri with scheme,
  host and port (otherwise plugin will fail to start).
- Configure what events to send as detailed
  in [](openlineage-event-listener-configuration)

(openlineage-event-listener-configuration)=

## Configuration

To configure the OpenLineage event listener, create an event listener properties
file in `etc` named `openlineage-event-listener.properties` with the following
contents as an example of minimal required configuration:

```properties
event-listener.name=openlineage
openlineage-event-listener.trino.uri=<Address of your Trino coordinator>
```

Add `etc/openlineage-event-listener.properties` to `event-listener.config-files`
in [](config-properties):

```properties
event-listener.config-files=etc/openlineage-event-listener.properties,...
```

:::{list-table} OpenLineage event listener configuration properties
:widths: 40, 40, 20
:header-rows: 1

*
    - Property name
    - Description
    - Default
*
    - openlineage-event-listener.transport.type
    - Type of transport to use when emitting lineage information. 
      See [](supported-transport-types) for list of available options with
      descriptions.
    - `CONSOLE`
*
    - openlineage-event-listener.trino.uri
    - Required Trino URL with host and port. Used to render Job Namespace in OpenLineage.
    - None.
*
    - openlineage-event-listener.trino.include-query-types
    - Which types of queries should be taken into account when emitting lineage
      information. List of values split by comma. Each value must be
      matching `io.trino.spi.resourcegroups.QueryType` enum. Query types not
      included here are filtered out.
    - `DELETE,INSERT,MERGE,UPDATE,ALTER_TABLE_EXECUTE`
*
    - openlineage-event-listener.disabled-facets
    - Which [](trino-facets) should be not included in final OpenLineage event. 
      Allowed values: `trino_metadata`, `trino_query_context`, 
      `trino_query_statistics`.
    - None.
*
    - openlineage-event-listener.namespace
    - Custom namespace to be used for Job `namespace` attribute. If blank will
      default to Dataset Namespace.
    - None.
*
    - openlineage-event-listener.job.name-format
    - Custom namespace to use for the job `name` attribute.
      Use any string with, with optional substitution
      variables: `$QUERY_ID`, `$USER`, `$SOURCE`, `$CLIENT_IP`.
      For example: `As $USER from $CLIENT_IP via $SOURCE`.
    - `$QUERY_ID`.

:::

(supported-transport-types)=
### Supported Transport Types

- `CONSOLE` - sends OpenLineage JSON event to Trino coordinator standard output.
- `HTTP` - sends OpenLineage JSON event to OpenLineage compatible HTTP endpoint.
- `KAFKA` - sends OpenLineage JSON event to Kafka topic.

:::{list-table} OpenLineage `HTTP` Transport Configuration properties
:widths: 40, 40, 20
:header-rows: 1

*
    - Property name
    - Description
    - Default
*
    - openlineage-event-listener.transport.url
    - URL of OpenLineage . Required if `HTTP` transport is configured.
    - None.
*
    - openlineage-event-listener.transport.endpoint
    - Custom path for OpenLineage compatible endpoint. If configured, there
      cannot be any custom path within 
      `openlineage-event-listener.transport.url`.
    - `/api/v1`.
*
    - openlineage-event-listener.transport.api-key
    - API key (string value) used to authenticate with the service.
      at `openlineage-event-listener.transport.url`.
    - None.
*
    - openlineage-event-listener.transport.timeout
    - [Timeout](prop-type-duration) when making HTTP Requests.
    - `5000ms`
*
    - openlineage-event-listener.transport.headers
    - List of custom HTTP headers to be sent along with the events. See
    [](openlineage-event-listener-custom-headers) for more details.
    - Empty
*
    - openlineage-event-listener.transport.url-params
    - List of custom url params to be added to final HTTP Request. See
    [](openlineage-event-listener-custom-url-params) for more details.
    - Empty
*
    - openlineage-event-listener.transport.compression
    - Compression codec used for reducing size of HTTP body.
      Allowed values: `none`, `gzip`.
    - `none`

:::

:::{list-table} OpenLineage `KAFKA` Transport Configuration properties
:widths: 40, 40, 20
:header-rows: 1

*
    - Property name
    - Description
    - Default
*
    - openlineage-event-listener.kafka-transport.broker-endpoints
    - Comma-separated list of Kafka broker addresses in the format `host:port`.
      Required if `KAFKA` transport is configured.
    - None.
*
    - openlineage-event-listener.kafka-transport.topic-name
    - Name of the Kafka topic to publish OpenLineage events to.
      Required if `KAFKA` transport is configured.
    - None.
*
    - openlineage-event-listener.kafka-transport.message-key
    - Optional key for all Kafka messages produced by transport. If not specified, OpenLineage will use default value based on event type.
    - None.
*
    - openlineage-event-listener.kafka-transport.client-id
    - Kafka client ID for identifying this producer.
    - None.
*
    - openlineage-event-listener.kafka-transport.request-timeout
    - [Timeout](prop-type-duration) for Kafka requests.
    - `10s`
*
    - openlineage-event-listener.kafka-transport.config.resources
    - A comma-separated list of Kafka client configuration files. These files must exist on the machines running Trino.
      Only specify this if absolutely necessary to access Kafka.
      Example: `/etc/kafka-configuration.properties`
    - None.
*
    - kafka.security-protocol
    - Security protocol to use when connecting to Kafka brokers.
      Allowed values: `PLAINTEXT`, `SSL`.
    - `PLAINTEXT`
*
    - kafka.ssl.keystore.type
    - Type of the SSL keystore. Required when `kafka.security-protocol` is `SSL``.
      Allowed values: `JKS`, `PKCS12`.
    - None.
*
    - kafka.ssl.keystore.location
    - Path to the SSL keystore file. Required when `kafka.security-protocol` is `SSL`.
    - None.
*
    - kafka.ssl.keystore.password
    - Password for the SSL keystore. Required when `kafka.security-protocol` is `SSL`.
    - None.
*
    - kafka.ssl.key.password
    - Password for the private key in the keystore. Required when `kafka.security-protocol` is `SSL`.
    - None.
*
    - kafka.ssl.truststore.type
    - Type of the SSL truststore. Required when `kafka.security-protocol` is `SSL`.
      Allowed values: `JKS`, `PKCS12`.
    - None.
*
    - kafka.ssl.truststore.location
    - Path to the SSL truststore file. Required when `kafka.security-protocol` is `SSL`.
    - None.
*
    - kafka.ssl.truststore.password
    - Password for the SSL truststore. Required when `kafka.security-protocol` is `SSL`.
    - None.
:::

(openlineage-event-listener-custom-kafka-configuration)=
### Custom Kafka Configuration

In some cases, such as when using specialized authentication methods, it is
necessary to specify additional Kafka client properties in order to access
your Kafka cluster. To do so, add the `openlineage-event-listener.kafka-transport.config.resources`
property to reference your Kafka config files. Note that configs can be
overwritten if defined explicitly in `openlineage-event-listener.properties`:

```properties
event-listener.name=kafka
openlineage-event-listener.kafka-transport.broker-endpoints=kafka.example.com:9093
openlineage-event-listener.kafka-transport.topic-name=openlineage-topic
openlineage-event-listener.kafka-transport.config.resources=/etc/kafka-configuration.properties
```

The contents of `/etc/kafka-configuration.properties` can for example be:

```properties
sasl.mechanism=SCRAM-SHA-512
security.protocol=SASL_SSL
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="kafkaclient1" \
  password="kafkaclient1-secret";
```

Kafka transport uses `zstd` compression by default for efficient message delivery.
This can be overridden via the config resources file by setting `compression.type`.

(openlineage-event-listener-custom-headers)=

### Custom HTTP headers

Providing custom HTTP headers is a useful mechanism for sending metadata along 
with event messages.

Providing headers follows the pattern of `key:value` pairs separated by commas:

```text
openlineage-event-listener.transport.headers="Header-Name-1:header value 1,Header-Value-2:header value 2,..."
```

If you need to use a comma(`,`) or colon(`:`) in a header name or value,
escape it using a backslash (`\`).

Keep in mind that these are static, so they can not carry information
taken from the event itself.

(openlineage-event-listener-custom-url-params)=

### Custom URL Params

Providing additional URL Params included in final HTTP Request.

Providing url params follows the pattern of `key:value` pairs separated by commas:

```text
openlineage-event-listener.transport.url-params="Param-Name-1:param value 1,Param-Value-2:param value 2,..."
```

Keep in mind that these are static, so they can not carry information
taken from the event itself.
