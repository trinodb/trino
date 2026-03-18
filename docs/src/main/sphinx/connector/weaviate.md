# Weaviate connector

```{raw} html
<img src="../_static/img/weaviate.png" class="connector-logo">
```

The Weaviate connector allows access to
[Weaviate](https://weaviate.io/) data from Trino.
This document describes how to configure a catalog with the Weaviate
connector to run SQL queries against Weaviate.

## Requirements

- Weaviate v1.32 and above
- Network access from the Trino coordinator and workers to the Weaviate nodes.

## Configuration

To configure the Weaviate connector, create a catalog properties file
`etc/catalog/weaviate.properties` with the following contents, replacing the
properties as appropriate for your setup:

```text
connector.name=weaviate
weaviate.scheme=http
weaviate.http-host=localhost
weaviate.http-port=8080
weaviate.grpc-host=localhost
weaviate.grpc-port=50051
```

Weaviate connector uses [`client6`](https://central.sonatype.com/artifact/io.weaviate/client6) for communication
with the Weaviate server. Some options will use client-side defaults if not set. These defaults are not part of
the library's public API and may be subject to change. It is therefore recommended to set all properties explicitly.

The following table details all general configuration properties:

:::{list-table} Weaviate configuration properties
:widths: 35, 55, 10
:header-rows: 1

*
    - Property name
    - Description
    - Default
*
    - `weaviate.scheme`
    - Connection URL scheme (http / https). Set this to `"https"` to use a TLS connection.
    - `"http"`
*
    - `weaviate.http-host`
    - Hostname of the target HTTP server. Must NOT include the port number.
    - `"localhost"`
*
    - `weaviate.http-port`
    - Port number the HTTP server is listening on.
    - `8080`
*
    - `weaviate.grpc-host`
    - Hostname of the target gRPC server. Must NOT include the port number.
    - `"localhost"`
*
    - `weaviate.grpc-port`
    - Port number the gRPC server is listening on.
    - `50051`
*
    - `weaviate.consisteny-level`
    - Read [consistency](https://docs.weaviate.io/weaviate/concepts/replication-architecture/consistency#tunable-read-consistency). If not set, will use the server-side default.
    - `null`
*
    - `weaviate.timeout`
    - Request timeout. If not set, will use the client-side default.
    - `30s`
*
    - `weaviate.page-size`
    - Number of rows fetched as a single page. If not set, will use the client-side default.
    - `30s`

:::

### Authentication

Weaviate supports several authentication flows:

- With an API key (the preferred option for Weaviate Cloud)
- Using a Bearer token
- Via an OIDC provider (Resource Owner Password and Client Credentials flows)

To enable API key authentication, the key must be provided via the `weaviate.auth.api-key` option.

To enable Bearer Token authentication, the following properties must be configured:

:::{list-table}
:widths: 35, 55, 10
:header-rows: 1

*
    - Property name
    - Description
    - Type
*
    - `weaviate.auth.access-token`
    - Access token. This option is required.
    - `string`
*
    - `weaviate.auth.refresh-token`
    - Refresh token for the access token. This option is required.
    - `string`
*
    - `weaviate.auth.access-token-lifetime`
    - Remaining access token lifetime. If not set, a lifetime of `0s` will be assumed, causing the token to be refreshed the first time its used.
    - `Duration`

:::

To enable Resource Owner Password authentication flow, the following options must be configured:

:::{list-table}
:widths: 35, 55, 10
:header-rows: 1

*
    - Property name
    - Description
    - Type
*
    - `weaviate.oidc.username`
    - Username. This option is required.
    - `string`
*
    - `weaviate.oidc.password`
    - Password. This option is required.
    - `string`
*
    - `weaviate.oidc.scopes`
    - OIDC scopes. An `"offline_access"` scope will be present by default.
    - `string[]`

:::

To enable Client Credentials authentication flow, the following options must be configured:

:::{list-table}
:widths: 35, 55, 10
:header-rows: 1

*
    - Property name
    - Description
    - Type
*
    - `weaviate.oidc.client-secret`
    - Client secret. This option is required.
    - `string`
*
    - `weaviate.oidc.scopes`
    - OIDC scopes. An `"<client_id>/.default"` scope will be present by default for Microsoft IdPs.
    - `string[]`

:::

Note, that for Client Credentials flow to work, the `client_id` must be configured on the server.

Read more about Authentication and Authorization in Weaviate [here](https://docs.weaviate.io/deploy/configuration/env-vars#authentication-and-authorization).

(weaviate-type-mapping)=

## Type mapping

Because Trino and Weaviate each support types that the other does not, this
connector {ref}`maps some types <type-mapping-overview>` when reading data.

### Weaviate type to Trino type mapping

The connector maps Weaviate types to the corresponding Trino types
according to the following table:

:::{list-table} Weaviate type to Trino type mapping
:widths: 30, 30, 50
:header-rows: 1

*
    - Weaviate type
    - Trino type
    - Notes
*
    - `text`
    - `VARCHAR`
    -
*
    - `bool`
    - `BOOLEAN`
    -
*
    - `int`
    - `INTEGER`
    -
*
    - `number`
    - `DOUBLE`
    -
*
    - `date`
    - `TIMESTAMP` (millisecond precision)
    -
*
    - `blob`
    - `VARCHAR`
    - Blob properties are stored as base64-encoded strings.
*
    - `object`
    - `ROW`
    - ROW field names and types are defined according to the `nestedProperties` of the `object`.
*
    - `geoCoordinates`
    - `ROW(DOUBLE latitude, DOUBLE longitude)`
    -
*
    - `phoneNumber`
    - `ROW(VARCHAR defaultCountry, INTEGER countryCode, VARCHAR internationalFormatted, INTEGER national, VARCHAR nationalFormatted, BOOLEAN valid)`
    -
*
    - `text[]`
    - `ARRAY(VARCHAR)`
    -
*
    - `bool[]`
    - `ARRAY(BOOLEAN)`
    -
*
    - `int[]`
    - `ARRAY(INTEGER)`
    -
*
    - `number[]`
    - `ARRAY(DOUBLE)`
    -
*
    - `date[]`
    - `ARRAY(TIMESTAMP)`
    -
*
    - `object[]`
    - `ARRAY(ROW)`
    - ROW field names and types are defined according to the `nestedProperties` of the `object`.
*
    - `number[][]`
    - `ARRAY(ARRAY(DOUBLE))`
    - This data type can only appear in `_vectors` field, if the vector uses multidimensional encoding like ColBERT.

:::

No other types are supported.

(weaviate-nested-objects)=

### `object` properties

Weaviate properties can be arbitrarity nested objects. On fetching a collection definition,
Weaviate connector will create an appropriate `ROW` type definition for each `object` and `object[]`
property.

Consider this `address` property which may be defined in a collection:

```json
{
    "name": "address",
    "dataTypes": [
        "object"
    ],
    "nestedProperties": [
        {
            "name": "street",
            "dataTypes": [
                "text"
            ]
        },
        {
            "name": "building_nr",
            "dataTypes": [
                "int"
            ]
        },
        {
            "name": "geo",
            "dataTypes": [
                "geoCoordinates"
            ]
        },
        {
            "name": "residents",
            "dataTypes": [
                "object[]"
            ],
            "nestedProperties": [
                {
                    "name": "full_name",
                    "dataTypes": [
                        "text"
                    ]
                }
            ]
        }
    ]
}
```

The Trino type which Weaviate connector will report is:

```sql
ROW(
    TEXT street, 
    INTEGER building_nr, 
    ROW(DOUBLE latitude, DOUBLE longitude) geo,
    ARRAY(ROW(TEXT full_name)) residents
) address
```

This allows accessing the arbitrarily nested fields using the standard SQL syntax:

```sql
SELECT address.residents[1].full_name FROM houses WHERE building_nr > 92;
```

## Vector embeddings

Vectors are retrieved for each of the objects in the catalog, provided the collection
has a vectorizer module configured, or the vectors were supplied on ingestion.

All named vectors are made available in the `_vectors` column, which itself is a `ROW`
with fields corresponding to each existing vector.

:::{list-table} Weaviate vector embedding type mapping
:widths: 25, 75
:header-rows: 1

*
    - Vector type
    - Trino data type
*
    - Single vector
    - `ARRAY(NUMBER)`
*
    - Multi-vector (ColBERT)
    - `ARRAY(ARRAY(NUMBER))`

:::

The unnamed vector (supported in older versions of Weaviate) is available under `_vectors.default`.

## Metadata columns

Object metadata is made available via the following properties:

:::{list-table}
:widths: 25, 75
:header-rows: 1

*
    - Column
    - Description
*
    - `_id`
    - Weaviate object internal UUID.
*
    - `_created_at`
    - Object's creation `TIMESTAMP`.
*
    - `_last_updated_at`
    - Object's last update `TIMESTAMP`.

:::

### Multi-tenant collections

The connector provides a way to query multi-tenant collections by modeling
Weaviate tenants as SQL schemas. For example, to query `houses` collection
belonging to the `company_abc` tenant, run this query:

```sql
SELECT * FROM company_abc.tenant;
```

The same connector can read both single-tenant and multi-tenant catalogs.
No additional configuration is required.

(weaviate-sql-support)=

## SQL support

The connector provides [globally available](sql-globally-available) and [read
operation](sql-read-operations) statements to access data and metadata in the
Weaviate catalog.

## Performance

The connector includes a number of performance improvements, detailed in the
following sections.

### Paginated data access

The connector fetches data in batches of configurable size using a performant
and memory-efficient gRPC protocol.

### Limit pushdown

The connector supports `LIMIT` pushdown for all queries.

### COUNT(*)

The connector uses a dedicated aggregation API for servicing queries which
do not request any of the collection's properties. 
