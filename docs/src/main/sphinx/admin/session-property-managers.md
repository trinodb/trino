# Session property managers

Administrators can add session properties to control the behavior for subsets of their workload.
These properties are defaults, and can be overridden by users, if authorized to do so. Session
properties can be used to control resource usage, enable or disable features, and change query
characteristics. Session property managers are pluggable.

Trino ships with two session property managers:

- A file-based manager that reads a JSON config file. Changes require a coordinator restart.
- A database-backed manager that loads the configuration from a relational database and
  reloads it periodically without a restart.

## File session property manager

Add an `etc/session-property-config.properties` file with the following contents to enable
the built-in manager, that reads a JSON config file:

```text
session-property-config.configuration-manager=file
session-property-manager.config-file=etc/session-property-config.json
```

Change the value of `session-property-manager.config-file` to point to a JSON config file,
which can be an absolute path, or a path relative to the Trino data directory.

This configuration file consists of a list of match rules, each of which specify a list of
conditions that the query must meet, and a list of session properties that should be applied
by default. All matching rules contribute to constructing a list of session properties. Rules
are applied in the order they are specified. Rules specified later in the file override values
for properties that have been previously encountered.

(match-rules)=
## Match rules

- `user` (optional): regex to match against username.
- `source` (optional): regex to match against source string.
- `queryType` (optional): string to match against the type of the query submitted:
  : - `DATA_DEFINITION`: Queries that alter/create/drop the metadata of schemas/tables/views, and that manage
      prepared statements, privileges, sessions, and transactions.
    - `DELETE`: `DELETE` queries.
    - `DESCRIBE`: `DESCRIBE`, `DESCRIBE INPUT`, `DESCRIBE OUTPUT`, and `SHOW` queries.
    - `EXPLAIN`: `EXPLAIN` queries.
    - `INSERT`: `INSERT` and `CREATE TABLE AS` queries.
    - `SELECT`: `SELECT` queries.
- `clientTags` (optional): list of tags. To match, every tag in this list must be in the list of
  client-provided tags associated with the query.
- `group` (optional): regex to match against the fully qualified name of the resource group the query is
  routed to.
- `sessionProperties`: map with string keys and values. Each entry is a system or catalog property name and
  corresponding value. Values must be specified as strings, no matter the actual data type.

## Example

Consider the following set of requirements:

- All queries running under the `global` resource group must have an execution time limit of 8 hours.
- All interactive queries are routed to sub-groups under the `global.interactive` group, and have an execution time
  limit of 1 hour (tighter than the constraint on `global`).
- All ETL queries (tagged with 'etl') are routed to sub-groups under the `global.pipeline` group, and must be
  configured with certain properties to control writer behavior and a hive catalog property.

These requirements can be expressed with the following rules:

```json
[
  {
    "group": "global.*",
    "sessionProperties": {
      "query_max_execution_time": "8h"
    }
  },
  {
    "group": "global.interactive.*",
    "sessionProperties": {
      "query_max_execution_time": "1h"
    }
  },
  {
    "group": "global.pipeline.*",
    "clientTags": ["etl"],
    "sessionProperties": {
      "scale_writers": "true",
      "hive.insert_existing_partitions_behavior": "overwrite"
    }
  }
]
```

(db-session-property-manager)=
## Database session property manager

The database session property manager loads the configuration from a relational database.
Unlike the file manager, it reloads the configuration periodically, so changes take effect
for incoming queries without a coordinator restart.

Add an `etc/session-property-config.properties` file with the following contents to enable
the database manager:

```text
session-property-config.configuration-manager=db
session-property-manager.db.url=jdbc:mysql://localhost:3306/session_properties
session-property-manager.db.username=username
session-property-manager.db.password=password
```

The configuration is stored in a MySQL database across three tables, `session_specs`,
`session_client_tags`, and `session_property_values`. If any of the tables do not exist when
Trino starts, they are created automatically.

Each row in `session_specs` is a match rule equivalent to a rule in the file manager's JSON
config. Its child rows in `session_client_tags` and `session_property_values` hold the
`clientTags` list and the `sessionProperties` map for that rule. As with the file manager, all
matching rules contribute to the resulting set of session properties, and rules are applied in
increasing order of the `priority` field, so a rule with a higher `priority` value overrides
values set by a rule with a lower one.

:::{list-table} `session_specs` columns
:widths: 30, 70
:header-rows: 1

* - Column
  - Description
* - `spec_id`
  - Auto-generated primary key of the rule. Referenced by the child tables.
* - `user_regex`
  - Optional regular expression matched against the username.
* - `source_regex`
  - Optional regular expression matched against the source string.
* - `query_type`
  - Optional query type to match, for example `SELECT` or `INSERT`.
* - `group_regex`
  - Optional regular expression matched against the fully qualified name of the
    resource group the query is routed to.
* - `priority`
  - Order in which rules are applied. Rules with a higher value override rules with
    a lower value.
:::

:::{list-table} `session_client_tags` columns
:widths: 30, 70
:header-rows: 1

* - Column
  - Description
* - `tag_spec_id`
  - `spec_id` of the rule this tag belongs to.
* - `client_tag`
  - A client tag that must be present on the query for the rule to match. Every tag
    listed for a rule must be present.
:::

:::{list-table} `session_property_values` columns
:widths: 30, 70
:header-rows: 1

* - Column
  - Description
* - `property_spec_id`
  - `spec_id` of the rule this property belongs to.
* - `session_property_name`
  - Name of a system or catalog session property to set.
* - `session_property_value`
  - Value to apply for the property. Values are always strings, regardless of the
    property's actual data type.
:::

The match conditions and session properties have the same semantics as the corresponding
fields documented for the file manager under [](#match-rules).

:::{list-table} Database session property manager properties
:widths: 40, 50, 10
:header-rows: 1

* - Property name
  - Description
  - Default value
* - `session-property-manager.db.url`
  - JDBC URL of the database to load configuration from.
  - `none`
* - `session-property-manager.db.username`
  - Database user to connect with.
  - `none`
* - `session-property-manager.db.password`
  - Password for the database user to connect with.
  - `none`
* - `session-property-manager.db.refresh-period`
  - How often the configuration is reloaded from the database.
  - `10s`
:::
