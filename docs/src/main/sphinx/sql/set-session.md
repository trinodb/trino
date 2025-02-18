# SET SESSION

## Synopsis

```text
SET SESSION name = expression
SET SESSION catalog.name = expression
```

## Description

Set a session property value or a catalog session property.

(session-properties-definition)=
## Session properties

A session property is a {doc}`configuration property </admin/properties>` that
can be temporarily modified by a user for the duration of the current
connection session to the Trino cluster. Many configuration properties have a
corresponding session property that accepts the same values as the config
property.

There are two types of session properties:

- **System session properties** apply to the whole cluster. Most session
  properties are system session properties unless specified otherwise.
- **Catalog session properties** are connector-defined session properties that
  can be set on a per-catalog basis. These properties must be set separately for
  each catalog by including the catalog name as a prefix, such as
  `catalogname.property_name`.

Session properties are tied to the current session, so a user can have multiple
connections to a cluster that each have different values for the same session
properties. Once a session ends, either by disconnecting or creating a new
session, any changes made to session properties during the previous session are
lost.

## Examples

The following example sets a system session property change maximum query run time:

```sql
SET SESSION query_max_run_time = '10m';
```

The following example sets the `incremental_refresh_enabled` catalog session
property for a catalog using the [](/connector/iceberg) named `example`:

```sql
SET SESSION example.incremental_refresh_enabled=false;
```

The related catalog configuration property `iceberg.incremental-refresh-enabled`
defaults to `true`, and the session property allows you to override this setting
in for specific catalog and the current session. The
`example.incremental_refresh_enabled` catalog session property does not apply to
any other catalog, even if another catalog also uses the Iceberg connector.

## See also

{doc}`reset-session`, {doc}`show-session`
