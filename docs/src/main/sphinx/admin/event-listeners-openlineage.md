# OpenLineage event listener

The OpenLineage event listener plugin allows streaming of lineage information, encoded in
JSON format aligned with OpenLineage specification, to an external, OpenLineage copmpatible API, by POSTing them
to a specified URI.

## Rationale

This event listener is aiming to capture every query that creates or modifies trino tables and transform it into lineage
information. Linage can be understood as relationship/flow between data/tables. OpenLineage is a widely used open-source
standard for capturing lineage information from variety of system including (but not limited to) Spark, Airflow, Flink.

### Trino Query attributes mapping to OpenLineage attributes:

| Trino                                                                 | OpenLineage         |
|-----------------------------------------------------------------------|---------------------|
| UUID(Query Id)                                                        | Run Id              |
| queryCreatedEvent.getCreateTime() or queryCompletedEvent.getEndTime() | Run Event Time      |
| Query Id                                                              | Job Facet Name      |
| QueryContext.getEnvironment() or openlineage-event-listener.namespace | Job Facet Namespace |

(openlineage-event-listener-requirements)=
## Requirements

You need to perform the following steps:

- Provide an HTTP/S service that accepts POST events with a JSON body and is compatible with OpenLineage API format.
- Configure `openlineage-event-listener.connect-url` in the event listener properties file
  with the URI of the OpenLineage API.
- Detail the events to send in the {ref}`http-event-listener-configuration` section.

(openlineage-event-listener-configuration)=
## Configuration

To configure the OpenLineage event listener plugin, create an event listener properties
file in `etc` named `openlineage-event-listener.properties` with the following contents
as an example:

```properties
event-listener.name=openlineage
openlineage-event-listener.connect-url=<your openlineage api url>
openlineage-event-listener.connect-sink=API
openlineage-event-listener.connect-retry-count=3
openlineage-event-listener.connect-retry-delay=5s
openlineage-event-listener.connect-backoff-base=0.5
openlineage-event-listener.connect-max-delay=1s
openlineage-event-listener.connect-api-key=__dummy__
openlineage-event-listener.namespace=production
openlineage-event-listener.facets-metadata-enabled=true
openlineage-event-listener.facets-query-context-enabled=true
openlineage-event-listener.facets-query-statistics-enabled=true
```

And set add `etc/openlineage-event-listener.properties` to `event-listener.config-files`
in {ref}`config-properties`:

```properties
event-listener.config-files=etc/openlineage-event-listener.properties,...
```

### Configuration properties

:::{list-table}
:widths: 40, 40, 20
:header-rows: 1

* - Property name
  - Description
  - Default

* - openlineage-event-listener.connect-sink
  - Type of sink to which emit lineage information. Currently only API is supported.
  - `API`

* - openlineage-event-listener.connect-url
  - The URI that the plugin will POST events to. Needs to be OpenLineage compatible API with /api/v1/lineage endpoint present.
  - None. See the [requirements](openlineage-event-listener-requirements) section.

* - openlineage-event-listener.connect-api-key
  - Api Key used to authenticate within `openlineage-event-listener.connect-url` API.
  - None.

* - openlineage-event-listener.connect-retry-count
  - The number of retries on server error. A server is considered to be
    in an error state when the response code is 500 or higher
  - `3`

* - openlineage-event-listener.connect-retry-delay
  - Duration for which to delay between attempts to send a request
  - `1s`

* - openlineage-event-listener.connect-backoff-base
  - The base used for exponential backoff when retrying on server error.
    The formula used to calculate the delay is
    `attemptDelay = retryDelay * backoffBase^{attemptCount}`.
    Attempt count starts from 0. Leave this empty or set to 1 to disable
    exponential backoff and keep constant delays
  - `2`

* - openlineage-event-listener.connect-max-delay
  - The upper bound of a delay between 2 retries. This should be
    used with exponential backoff.
  - `1m`

* - openlineage-event-listener.namespace
  - Namespace that will be used to annotate job facet. If not provided, query environment will be used.
  - `Optional.empty()`

* - openlineage-event-listener.facets-metadata-enabled
  - Should Trino Metadata facet be included into run facet.
  - `true`

* - openlineage-event-listener.facets-query-context-enabled
  - Should Query Context facet be included into run facet.
  - `true`

* - openlineage-event-listener.facets-query-statistics-enabled
  - Should Query Statistics facet be included into run facet.
  - `true`

:::
