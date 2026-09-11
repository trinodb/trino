# OpenLineage event listener

The OpenLineage event listener plugin allows streaming of lineage information,
encoded in
JSON format aligned with OpenLineage specification, to an external, OpenLineage
compatible API, by POSTing them
to a specified URI.

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

(openlineage-event-listener-column-lineage)=

### Column lineage

For queries that write a table (`CREATE TABLE ... AS SELECT`, `INSERT`, and
`REFRESH MATERIALIZED VIEW`), the output dataset carries a standard OpenLineage
`columnLineage` dataset facet. Each output column lists the input fields it was
derived from, and each input field carries a `transformations` list. Because a
source column can reach an output column through several paths (for example the
branches of a `UNION`), the list can hold more than one entry: every distinct
subtype is reported rather than collapsing to one. Each entry has:

- `type` - always `DIRECT`. Trino tracks direct value dependencies; it does not
  currently emit `INDIRECT` transformations (join, filter, group-by or sort
  dependencies).
- `subtype` - how the output column derives from *that* source column:
  - `IDENTITY` - the output column is a straight copy of the source column, for
    example `SELECT a`.
  - `TRANSFORMATION` - the output column is derived through a non-aggregate
    expression in which the raw source value can survive, for example
    `SELECT a + b` or `SELECT concat(a, b)`.
  - `AGGREGATION` - the source column reaches the output only through an aggregate
    expression, so no raw value survives, for example `SELECT sum(a)` or
    `SELECT count(*)`.

Each source→output edge is classified independently, so a single output column
can mix subtypes across its input fields: in `SELECT a + sum(b) ... GROUP BY a`
the edge from `a` is `TRANSFORMATION` (its raw value survives) while the edge from
`b` is `AGGREGATION`. A single source→output edge can itself carry several
subtypes when the column reaches the output through more than one path: in
`SELECT a FROM t UNION ALL SELECT sum(a) FROM t` the edge from `a` reports both
`IDENTITY` (the copy branch) and `AGGREGATION` (the aggregate branch). Along any
one path, once a value is aggregated upstream its subtype stays `AGGREGATION` even
if a later layer transforms it. Subtypes propagate through subqueries, common
table expressions, views and set operations. When Trino cannot determine an
edge's derivation, the `transformations` list is omitted and consumers should
assume a raw source value may survive.

:::{note}
The OpenLineage `AGGREGATION` subtype also covers aggregates such as `min`,
`max` and `first_value`, which return an actual source value (for example
`max(ssn)` is a real value from the `ssn` column). Consumers making
retention or masking decisions based on the subtype should therefore not treat
`AGGREGATION` as blanket-safe; the decision must account for the specific
aggregate function.
:::

(openlineage-event-listener-requirements)=

## Requirements

You need to perform the following steps:

- Provide an HTTP/S service that accepts POST events with a JSON body and is
  compatible with the OpenLineage API format.
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
