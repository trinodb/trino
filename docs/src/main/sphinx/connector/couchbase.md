# Couchbase connector

```{raw} html
<img src="../_static/img/couchbase.png" class="connector-logo">
```

The `couchbase` connector allows the use of [Couchbase](https://www.couchbase.com/) scopes as read-only schemas in Trino.

## Requirements

To connect to Couchbase, you need:

- Couchbase 7.0.0 or higher.
- Network access from the Trino coordinator and workers to Couchbase cluster.

## Configuration

To configure the couchbase connector, create a catalog properties file
`etc/catalog/example.properties` with the following contents,
replacing the properties as appropriate:

```text
connector.name=couchbase
couchbase.cluster=couchbase://localhost
couchbase.username=Administrator
couchbase.password=password
couchbase.schema-folder=couchbase-schema
couchbase.bucket=_default
couchbase.scope=_default
```

### Multiple Couchbase clusters

You can have as many catalogs as you need, so if you have additional
Couchbase clusters, simply add another properties file to `etc/catalog`
with a different name, making sure it ends in `.properties`). For
example, if you name the property file `sales.properties`, Trino
will create a catalog named `sales` using the configured connector.

## Configuration properties

The following configuration properties are available:

| Property name                | Default Value         | Description                                                                                                                                                                                                                                                 |
|------------------------------|-----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `couchbase.cluster`          | couchbase://localhost | The connection url that the driver uses to connect to a Couchbase cluster.<br/>See [Connection Strings](https://docs.couchbase.com/java-sdk/current/howtos/managing-connections.html#connection-strings) from Couchbase documentation for more information. |
| `couchbase.username`         | Administrator         | The username that the driver uses to connect to the cluster                                                                                                                                                                                                 |
| `couchbase.password`         | password              | The password that the driver uses to connect to the cluster                                                                                                                                                                                                 |
| `couchbase.bucket`           | _default              | The bucket that the driver uses to access the data                                                                                                                                                                                                          |
| `couchbase.scope`            | _default              | The scope that the driver uses to access the data                                                                                                                                                                                                           |
| `couchbase.tls-certificate`  |                       | Path to a file containing TLS certificate that driver uses to connect to the cluster                                                                                                                                                                        |
| `couchbase.tls-key`          |                       | Path to a TLS keystore file                                                                                                                                                                                                                                 |
| `couchbase.tls-key-password` |                       | Keystore file password                                                                                                                                                                                                                                      |
| `couchbase.schema-folder`    | couchbase-schema      | Path to a folder containing schema definition files                                                                                                                                                                                                         |
| `couchbase.timeouts`         | 60                    | Connection and data read timeout in seconds used by the driver                                                                                                                                                                                              |
| `couchbase.page-size`        | 5000                  | Page size that connector uses to load large datasets                                                                                                                                                                                                        |

## Supported Type Mappings

| Trino Type                                        | Couchbase Type | Comments                                                      |
|---------------------------------------------------|----------------|---------------------------------------------------------------|
| DATE                                              | NUMBER         | Stored as number of days since Unix Epoch                     |
| Integer, Floating point types (except DECIMAL)    | NUMBER         | Passed as-is to SDK JSON serializer                           |
| DECIMAL                                           | STRING         | Stored as string to avoid precision loss during serialization |
| String/char types                                 | STRING         | Passed as-is to SDK JSON serializer                           |
| BOOLEAN                                           | BOOLEAN        | Passed as-is to SDK JSON serializer                           |

ARRAY and MAP are mapped directly onto Couchbase JSON document fields. Support for other types will be introduced in future connector versions.

## Schema Definition Files
Couchbase connector requires _schema definition file_ to exist for each collection that should be accessible in Trino.
Connector schema definitions are located in a folder under $TRINO_HOME/data directory that is configurable via “couchbase.schema-folder” catalog property.
Names of schema files follow the pattern `<bucket>.<scope>.<collection>.json`. The connector will return an error if it fails to find schema file for queried collections.

### General Structure of a schema file
```text
{
	"properties" : {
		"<propname>": <propdef>,
			...
	}
}
```
Where `<propdef>` is one of:

- *Scalar Field Definition* — Defines a single scalar field:
    ```text
        { "type": <scalar_type> }
    ```
- *Array Field Definition* — Defines a single ARRAY field:
    ```text
        {
          "type": "array",
          "items": {
              "type": "<type>",
                 …
          }
        }
    ```
- *Object Field Definition* — Defines a single MAP field:
    ```text
        {
          "type": "object",
          "properties": {
              "<propname>": "<propdef>",
                 …
          }
        }
    ```
  
The following scalar types can be used in schema definition files:
- null
- string
- varchar([len]) – with integer [len] as length
- boolean
- number
- integer
- date – as number of days since unix epoch
- bigint
- double

### Pushdown

The connector supports pushdown for following operations:

- {ref}`dereference-pushdown`
- {ref}`limit-pushdown`
- {ref}`topn-pushdown`
- {ref}`predicate-pushdown` for the following operations:
    - `$in` 
    - `$array`
    - `$cast`
    - `$equal`
    - `$modulus`
    - `$add`
    - `$negate`
    - `$greater_than_or_equal`
    - `$less_than_or_equal`
    - `$less_than`
    - `$greater_than`
    - `$not`
    - `$not_equal`
    - `$nullif`
    - `$like`
    - `random()`
    - `concat()`
- {ref}`aggregation-pushdown` for the following functions:
    - `array_agg`
    - `avg`
    - `count`
    - `max`
    - `min`
    - `sum`

