# Kafka event listener

The Kafka event listener plugin allows streaming of query events to an external
Kafka-compatible system. The query history in the Kafka topic can then be
accessed directly in Kafka, via Trino in a catalog using the [Kafka
connector](/connector/kafka) or many downstream systems processing and storing
the data.

## Rationale

This event listener is a first step to store the query history of your Trino
cluster. The query events can provide CPU and memory usage metrics, what data is
being accessed with resolution down to specific columns, and metadata about the
query processing.

Running the capture system separate from Trino reduces the performance impact
and avoids downtime for non-client-facing changes.

(kafka-event-listener-requirements)=
## Requirements

You need to perform the following steps:

- Provide an Kafka service that is network-accessible to Trino.
- Configure `kafka-event-listener.broker-endpoints` in the event listener
  properties file with the URI of the service
- Configure what events to send as detailed
  in [](kafka-event-listener-configuration)

(kafka-event-listener-configuration)=
## Configuration

To configure the Kafka event listener, create an event listener properties
file in `etc` named `kafka-event-listener.properties` with the following
contents as an example of a minimal required configuration:

```properties
event-listener.name=kafka
kafka-event-listener.broker-endpoints=kafka.example.com:9093
kafka-event-listener.created-event.topic=query_create
kafka-event-listener.completed-event.topic=query_complete
kafka-event-listener.client-id=trino-example
```

Add `etc/kafka-event-listener.properties` to `event-listener.config-files`
in [](config-properties):

```properties
event-listener.config-files=etc/kafka-event-listener.properties,...
```

Use the following properties for further configuration.

:::{list-table} Kafka event listener configuration properties
:widths: 40, 40, 20
:header-rows: 1

* - Property name
  - Description
  - Default
* - `kafka-event-listener.broker-endpoints`
  - Comma-separated list of Kafka broker endpoints with URL and port, for
    example `kafka-1.example.com:9093,kafka-2.example.com:9093`.
  - 
* - `kafka-event-listener.anonymization.enabled`
  - [Boolean](prop-type-boolean) switch to enable anonymization of the event
    data in Trino before it is sent to Kafka.
  - `false`
* - `kafka-event-listener.client-id`
  - [String identifier](prop-type-string) for the Trino cluster to allow
    distinction in Kafka, if multiple Trino clusters send events to the same
    Kafka system.
  - 
* - `kafka-event-listener.publish-created-event`
  - [Boolean](prop-type-boolean) switch to control publishing of query creation
    events.
  - `true`
* - `kafka-event-listener.created-event.topic`
  - Name of the Kafka topic for the query creation event data.
  - 
* - `kafka-event-listener.publish-split-completed-event`
  - [Boolean](prop-type-boolean) switch to control publishing of
    [split](trino-concept-splits) completion events.
  - `false`
* - `kafka-event-listener.split-completed-event.topic`
  - Name of the Kafka topic for the split completion event data.
  - 
* - `kafka-event-listener.publish-completed-event`
  - [Boolean](prop-type-boolean) switch to control publishing of query
    completion events.
  - `true`
* - `kafka-event-listener.completed-event.topic`
  - Name of the Kafka topic for the query completion event data.
  -
* - `kafka-event-listener.excluded-fields`
  - Comma-separated list of field names to exclude from the Kafka event, for
    example `payload,user`. Values are replaced with null.
  - 
* - `kafka-event-listener.client-config-overrides`
  - Comma-separated list of key-value pairs to specify Kafka client configuration
    overrides, for example `buffer.memory=67108864,compression.type=zstd`.
  -
* - `kafka-event-listener.request-timeout`
  - Timeout [duration](prop-type-duration) to complete a Kafka request. Minimum
    value of `1ms`.
  - `10s`
* - `kafka-event-listener.terminate-on-initialization-failure`
  - Kafka publisher initialization can fail due to network issues reaching the
    Kafka brokers. This [boolean](prop-type-boolean) switch controls whether to
    throw an exception in such cases.
  - `true`
* - `kafka-event-listener.env-var-prefix`
  - When set, Kafka events are sent with additional metadata populated from
    environment variables. For example, if the value is `TRINO_INSIGHTS_` and an
    environment variable on the cluster is set at
    `TRINO_INSIGHTS_CLUSTER_ID=foo`, then the Kafka payload metadata contains
    `CLUSTER_ID=foo`.
  -
:::
