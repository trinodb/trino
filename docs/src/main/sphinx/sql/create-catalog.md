# CREATE CATALOG

## Synopsis

```text
CREATE CATALOG
catalog_name
USING connector_name
[ WITH ( property_name = expression [, ...] ) ]
```

## Description

Create a new catalog using the specified connector.

The optional `WITH` clause is used to set properties on the newly created
catalog. Property names can be double quoted, which is required if they contain
special characters, like `-`. Refer to the [connectors
documentation](/connector) to learn about all available properties. All
property values must be varchars (single quoted), including numbers and boolean
values.

The query fails in the following circumstances:

* A required property is missing.
* An invalid property is set, for example there is a typo in the property name,
  or a property name from a different connector was used.
* The value of the property is invalid, for example a numeric value is out of
  range, or a string value doesn't match the required pattern.
* The value references an environmental variable that is not set on the
  coordinator node.

:::{warning}
The complete `CREATE CATALOG` query is logged, and visible in the [Web
UI](/admin/web-interface). This includes any sensitive properties, like
passwords and other credentials. See [](/security/secrets).
:::

:::{note}
This command requires the [catalog management type](/admin/properties-catalog)
to be set to `dynamic`.
:::

## Examples

Create a new catalog called `tpch` using the [](/connector/tpch):

```sql
CREATE CATALOG tpch USING tpch;
```

Create a new catalog called `brain` using the [](/connector/memory):

```sql
CREATE CATALOG brain USING memory
WITH ("memory.max-data-per-node" = '128MB');
```

Notice that the connector property contains dashes (`-`) and needs to quoted
using a double quote (`"`). The value `128MB` is quoted using single quotes,
because it is a string literal.

Create a new catalog called `example` using the [](/connector/postgresql):

```sql
CREATE CATALOG example USING postgresql
WITH (
  "connection-url" = 'jdbc:pg:localhost:5432',
  "connection-user" = '${ENV:POSTGRES_USER}',
  "connection-password" = '${ENV:POSTGRES_PASSWORD}',
  "case-insensitive-name-matching" = 'true'
);
```

This example assumes that the `POSTGRES_USER` and `POSTGRES_PASSWORD`
environmental variables are set as [secrets](/security/secrets) on the
coordinator node.

## See also

* [](/sql/drop-catalog)
* [](/admin/properties-catalog)
