# Session property managers

Administrators can add session properties to control the behavior for subsets of their workload.
These properties are defaults, and can be overridden by users, if authorized to do so. Session
properties can be used to control resource usage, enable or disable features, and change query
characteristics. Session property managers are pluggable.

## File manager

Add an `etc/session-property-config.properties` file with the following contents to enable
the built-in file manager, which reads a JSON config file:

```text
session-property-config.configuration-manager=file
session-property-manager.config-file=etc/session-property-config.json
```

Change the value of `session-property-manager.config-file` to point to a JSON config file,
which can be an absolute path, or a path relative to the Trino data directory.

The JSON configuration file consists of a list of match rules, each of which specify a list of
conditions that the query must meet, and a list of session properties that should be applied
by default. All matching rules contribute to constructing a list of session properties. Rules
are applied in the order they are specified. Rules specified later in the file override values
for properties that have been previously encountered.

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

## Database manager

The database manager reads the same match rules from a MySQL-compatible
database. Add an `etc/session-property-config.properties` file with the
following contents to enable the database manager:

```text
session-property-config.configuration-manager=db
session-property-manager.db.url=jdbc:mysql://example.net:3306/session_properties
session-property-manager.db.username=trino
session-property-manager.db.password=secret
```

The following configuration properties are available:

:::{list-table}
:widths: 40, 60
:header-rows: 1

* - Property
  - Description
* - `session-property-manager.db.url`
  - JDBC URL for the MySQL database that stores session property rules.
* - `session-property-manager.db.username`
  - Optional username used to connect to the database.
* - `session-property-manager.db.password`
  - Optional password used to connect to the database.
* - `session-property-manager.db.refresh-period`
  - Interval for reloading rules from the database. Defaults to `10s`.
:::

The manager creates the required tables if they do not already exist:

:::{list-table}
:widths: 30, 70
:header-rows: 1

* - Table
  - Columns
* - `session_specs`
  - `spec_id`, `user_regex`, `source_regex`, `query_type`, `group_regex`,
    and `priority`.
* - `session_client_tags`
  - `tag_spec_id` and `client_tag`.
* - `session_property_values`
  - `property_spec_id`, `session_property_name`, and
    `session_property_value`.
:::

Each row in `session_specs` defines one match rule. Use `NULL` for conditions
that should not be part of the match. Client tags and session properties are
stored in the child tables and reference `session_specs.spec_id`. Rules are
loaded in ascending `priority` order, so rules with higher priority values can
override properties from earlier matching rules.

For example, the following rows configure the same rules as the JSON example:

```sql
INSERT INTO session_specs
  (spec_id, user_regex, source_regex, query_type, group_regex, priority)
VALUES
  (1, NULL, NULL, NULL, 'global.*', 0),
  (2, NULL, NULL, NULL, 'global.interactive.*', 1),
  (3, NULL, NULL, NULL, 'global.pipeline.*', 2);

INSERT INTO session_client_tags (tag_spec_id, client_tag)
VALUES (3, 'etl');

INSERT INTO session_property_values
  (property_spec_id, session_property_name, session_property_value)
VALUES
  (1, 'query_max_execution_time', '8h'),
  (2, 'query_max_execution_time', '1h'),
  (3, 'scale_writers', 'true'),
  (3, 'hive.insert_existing_partitions_behavior', 'overwrite');
```
