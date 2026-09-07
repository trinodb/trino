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

(file-example)=
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
The supported databases are MySQL and PostgreSQL. Unlike the file manager, it reloads the
configuration periodically, so changes take effect for incoming queries without a coordinator
restart.

Add an `etc/session-property-config.properties` file with the following contents to enable
the database manager:

```text
session-property-config.configuration-manager=db
session-property-manager.config-db-url=jdbc:mysql://localhost:3306/session_properties
session-property-manager.config-db-user=username
session-property-manager.config-db-password=password
```

The database dialect is selected from the JDBC URL prefix, so use a `jdbc:mysql:` URL for
MySQL and a `jdbc:postgresql:` URL for PostgreSQL.

The configuration is stored across three tables, `session_specs`, `session_client_tags`, and
`session_property_values`. By default the tables are created and kept up to date automatically
on startup; see `session-property-manager.db-migrations-enabled` below.

The configuration is reloaded from the database every `session-property-manager.refresh-interval`.
If reloads keep failing for longer than `session-property-manager.max-refresh-interval`, the cached
configuration is considered stale and queries fail rather than being run with out-of-date session
property defaults.

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
* - `session-property-manager.config-db-url`
  - JDBC URL of the database to load configuration from. The URL prefix (`jdbc:mysql:`
    or `jdbc:postgresql:`) selects the database dialect.
  - `none`
* - `session-property-manager.config-db-user`
  - Database user to connect with.
  - `none`
* - `session-property-manager.config-db-password`
  - Password for the database user to connect with.
  - `none`
* - `session-property-manager.refresh-interval`
  - How often the configuration is reloaded from the database.
  - `1s`
* - `session-property-manager.max-refresh-interval`
  - Time period for which the cluster keeps serving the cached configuration after
    reloads start failing. Once exceeded, queries fail instead of using stale defaults.
  - `1h`
* - `session-property-manager.db-migrations-enabled`
  - Whether to create and update the schema automatically on startup. Set to `false`
    to manage the schema out-of-band, for example when the database user has no DDL
    privileges.
  - `true`
:::

### Example

The following statements express the same rules as the [file manager example](#file-example)
above. They assume a freshly created schema, so the auto-generated `spec_id` values are `1`,
`2`, and `3`; on a database that already held rules the generated values differ (deleting rows
does not reset the sequence), so use the `spec_id` actually assigned to each parent row when
inserting its child rows:

```sql
-- All queries under the global resource group get an 8h execution time limit.
INSERT INTO session_specs (group_regex, priority) VALUES ('global.*', 1);
INSERT INTO session_property_values (property_spec_id, session_property_name, session_property_value)
VALUES (1, 'query_max_execution_time', '8h');

-- Interactive queries get a tighter 1h limit. The higher priority overrides the rule above.
INSERT INTO session_specs (group_regex, priority) VALUES ('global.interactive.*', 2);
INSERT INTO session_property_values (property_spec_id, session_property_name, session_property_value)
VALUES (2, 'query_max_execution_time', '1h');

-- ETL queries (tagged 'etl') under the pipeline group get writer-related properties.
INSERT INTO session_specs (group_regex, priority) VALUES ('global.pipeline.*', 1);
INSERT INTO session_client_tags (tag_spec_id, client_tag) VALUES (3, 'etl');
INSERT INTO session_property_values (property_spec_id, session_property_name, session_property_value) VALUES
    (3, 'scale_writers', 'true'),
    (3, 'hive.insert_existing_partitions_behavior', 'overwrite');
```
