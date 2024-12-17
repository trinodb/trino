# SQL environment properties

SQL environment properties allow you to globally configure parameters relevant
to all SQL queries and the context they are processed in.

## `sql.forced-session-time-zone`

- **Type:** [](prop-type-string)

Force the time zone for any query processing to the configured value, and
therefore override the time zone of the client. The time zone must be specified
as a string such as `UTC` or [other valid
values](timestamp-p-with-time-zone-data-type).

## `sql.default-catalog`

- **Type:** [](prop-type-string)

Set the default catalog for all clients. Any default catalog configuration
provided by a client overrides this default.

## `sql.default-schema`

- **Type:** [](prop-type-string)

Set the default schema for all clients. Must be set to a schema name that is
valid for the default catalog. Any default schema configuration provided by a
client overrides this default.

## `sql.default-function-catalog`

- **Type:** [](prop-type-string)

Set the default catalog for [](/udf) storage for all clients. The connector used
in the catalog must support [](udf-management). Any usage of a fully qualified
name for a UDF overrides this default.

The default catalog and schema for UDF storage must be configured together, and
the resulting entry must be set as part of the path. For example, the following
section for [](config-properties) uses the `functions` schema in the `brain`
catalog for UDF storage, and adds it as the only entry on the path:

```properties
sql.default-function-catalog=brain
sql.default-function-schema=default
sql.path=brain.default
```

## `sql.default-function-schema`

- **Type:** [](prop-type-string)

Set the default schema for UDF storage for all clients. Must be set to a schema
name that is valid for the default function catalog. Any usage of a fully
qualified name for a UDF overrides this default.

## `sql.path`

- **Type:** [](prop-type-string)

Define the default collection of paths to functions or table functions in
specific catalogs and schemas. Paths are specified as
`catalog_name.schema_name`. Multiple paths must be separated by commas. Find
more details about the path in [](/sql/set-path).
