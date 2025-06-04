# Resource groups

Resource groups place limits on resource usage, and can enforce queueing policies on
queries that run within them, or divide their resources among sub-groups. A query
belongs to a single resource group, and consumes resources from that group (and its ancestors).
Except for the limit on queued queries, when a resource group runs out of a resource
it does not cause running queries to fail; instead new queries become queued.
A resource group may have sub-groups or may accept queries, but may not do both.

The resource groups and associated selection rules are configured by a manager, which is pluggable.

You can use a file-based or a database-based resource group manager:

- Add a file `etc/resource-groups.properties`
- Set the `resource-groups.configuration-manager` property to `file` or `db`
- Add further configuration properties for the desired manager.

## File resource group manager

The file resource group manager reads a JSON configuration file, specified with
`resource-groups.config-file`:

```text
resource-groups.configuration-manager=file
resource-groups.config-file=etc/resource-groups.json
```

The path to the JSON file can be an absolute path, or a path relative to the Trino
data directory. The JSON file only needs to be present on the coordinator.

(db-resource-group-manager)=
## Database resource group manager

The database resource group manager loads the configuration from a relational database. The
supported databases are MySQL, PostgreSQL, and Oracle.

```text
resource-groups.configuration-manager=db
resource-groups.config-db-url=jdbc:mysql://localhost:3306/resource_groups
resource-groups.config-db-user=username
resource-groups.config-db-password=password
```

The resource group configuration must be populated through tables
`resource_groups_global_properties`, `resource_groups`, and
`selectors`. If any of the tables do not exist when Trino starts, they
will be created automatically.

The rules in the `selectors` table are processed in descending order of the
values in the `priority` field.

The `resource_groups` table also contains an `environment` field which is
matched with the value contained in the `node.environment` property in
{ref}`node-properties`. This allows the resource group configuration for different
Trino clusters to be stored in the same database if required.

The configuration is reloaded from the database every second, and the changes
are reflected automatically for incoming queries.

:::{list-table} Database resource group manager properties
:widths: 40, 50, 10
:header-rows: 1

* - Property name
  - Description
  - Default value
* - `resource-groups.config-db-url`
  - Database URL to load configuration from.
  - `none`
* - `resource-groups.config-db-user`
  - Database user to connect with.
  - `none`
* - `resource-groups.config-db-password`
  - Password for database user to connect with.
  - `none`
* - `resource-groups.max-refresh-interval`
  - The maximum time period for which the cluster will continue to accept
    queries after refresh failures, causing configuration to become stale.
  - `1h`
* - `resource-groups.refresh-interval`
  -  How often the cluster reloads from the database
  - `1s`
* - `resource-groups.exact-match-selector-enabled`
  - Setting this flag enables usage of an additional
    `exact_match_source_selectors` table to configure resource group selection
    rules defined exact name based matches for source, environment and query
    type. By default, the rules are only loaded from the `selectors` table, with
    a regex-based filter for `source`, among other filters.
  - `false`
:::

## Resource group properties

- `name` (required): name of the group. May be a template (see below).

- `maxQueued` (required): maximum number of queued queries. Once this limit is reached
  new queries are rejected.

- `softConcurrencyLimit` (optional): number of concurrently running queries after which
  new queries will only run if all peer resource groups below their soft limits are ineligible
  or if all eligible peers are above soft limits.

- `hardConcurrencyLimit` (required): maximum number of running queries.

- `softMemoryLimit` (required): maximum amount of distributed memory this
  group may use, before new queries become queued. May be specified as
  an absolute value (i.e. `1GB`) or as a percentage (i.e. `10%`) of the cluster's memory.

- `softCpuLimit` (optional): maximum amount of CPU time this
  group may use in a period (see `cpuQuotaPeriod`), before a penalty is applied to
  the maximum number of running queries. `hardCpuLimit` must also be specified.

- `hardCpuLimit` (optional): maximum amount of CPU time this
  group may use in a period.

- `schedulingPolicy` (optional): specifies how queued queries are selected to run,
  and how sub-groups become eligible to start their queries. May be one of three values:

  - `fair` (default): queued queries are processed first-in-first-out, and sub-groups
    must take turns starting new queries, if they have any queued.
  - `weighted_fair`: sub-groups are selected based on their `schedulingWeight` and the number of
    queries they are already running concurrently. The expected share of running queries for a
    sub-group is computed based on the weights for all currently eligible sub-groups. The sub-group
    with the least concurrency relative to its share is selected to start the next query.
  - `weighted`: queued queries are selected stochastically in proportion to their priority,
    specified via the `query_priority` {doc}`session property </sql/set-session>`. Sub groups are selected
    to start new queries in proportion to their `schedulingWeight`.
  - `query_priority`: all sub-groups must also be configured with `query_priority`.
    Queued queries are selected strictly according to their priority.

- `schedulingWeight` (optional): weight of this sub-group used in `weighted`
  and the `weighted_fair` scheduling policy. Defaults to `1`. See
  {ref}`scheduleweight-example`.

- `jmxExport` (optional): If true, group statistics are exported to JMX for monitoring.
  Defaults to `false`.

- `subGroups` (optional): list of sub-groups.

(scheduleweight-example)=
### Scheduling weight example

Schedule weighting is a method of assigning a priority to a resource. Sub-groups
with a higher scheduling weight are given higher priority. For example, to
ensure timely execution of scheduled pipelines queries, weight them higher than
adhoc queries.

In the following example, pipeline queries are weighted with a value of `350`,
which is higher than the adhoc queries that have a scheduling weight of `150`.
This means that approximately 70% (350 out of 500 queries) of your queries come
from the pipeline sub-group, and 30% (150 out of 500 queries) come from the adhoc
sub-group in a given timeframe. Alternatively, if you set each sub-group value to
`1`, the weight of the queries for the pipeline and adhoc sub-groups are split
evenly and each receive 50% of the queries in a given timeframe.

```{literalinclude} schedule-weight-example.json
:language: text
```

## Selector rules

The selector rules for pattern matching use Java's regular expression
capabilities. Java implements regular expressions through the `java.util.regex`
package. For more information, see the [Java
documentation](https://docs.oracle.com/en/java/javase/23/docs/api/java.base/java/util/regex/Pattern.html).

- `user` (optional): Java regex to match against user name.

- `originalUser` (optional): Java regex to match against the _original_ user name,
  i.e. before any changes to the session user. For example, if user "foo" runs
  `SET SESSION AUTHORIZATION 'bar'`, `originalUser` is "foo", while `user` is "bar".

- `authenticatedUser` (optional): Java regex to match against the _authenticated_ user name,
  which will always refer to the user that authenticated with the system, regardless of any
  changes made to the session user.

- `userGroup` (optional): Java regex to match against every user group the user belongs to.

- `source` (optional): Java regex to match against source string.

- `queryType` (optional): string to match against the type of the query submitted:

  - `SELECT`: [SELECT](/sql/select) queries.
  - `EXPLAIN`: [EXPLAIN](/sql/explain) queries, but not [EXPLAIN
    ANALYZE](/sql/explain-analyze) queries.
  - `DESCRIBE`: [DESCRIBE](/sql/describe), [DESCRIBE
    INPUT](/sql/describe-input), [DESCRIBE OUTPUT](/sql/describe-output), and
    `SHOW` queries such as [SHOW CATALOGS](/sql/show-catalogs), [SHOW
    SCHEMAS](/sql/show-schemas), and [SHOW TABLES](/sql/show-tables).
  - `INSERT`: [INSERT](/sql/insert), [CREATE TABLE AS](/sql/create-table-as),
    and [REFRESH MATERIALIZED VIEW](/sql/refresh-materialized-view) queries.
  - `UPDATE`: [UPDATE](/sql/update) queries.
  - `MERGE`: [MERGE](/sql/merge) queries.
  - `DELETE`: [DELETE](/sql/delete) queries.
  - `ANALYZE`: [ANALYZE](/sql/analyze) queries.
  - `DATA_DEFINITION`: Queries that affect the data definition. These include
    `CREATE`, `ALTER`, and `DROP` statements for schemas, tables, views, and
    materialized views, as well as statements that manage prepared statements,
    privileges, sessions, and transactions.
  - `ALTER_TABLE_EXECUTE`: Queries that execute table procedures with [ALTER
    TABLE EXECUTE](alter-table-execute).

- `clientTags` (optional): list of tags. To match, every tag in this list must be in the list of
  client-provided tags associated with the query.

- `group` (required): the group these queries will run in.

All rules within a single selector are combined using a logical `AND`. Therefore
all rules must match for a selector to be applied.

Selectors are processed sequentially and the first one that matches will be used.

## Global properties

- `cpuQuotaPeriod` (optional): the period in which cpu quotas are enforced.

## Providing selector properties

The source name can be set as follows:

- CLI: use the `--source` option.
- JDBC driver when used in client apps: add the `source` property to the
  connection configuration and set the value when using a Java application that
  uses the JDBC Driver.
- JDBC driver used with Java programs: add a property with the key `source`
  and the value on the `Connection` instance as shown in {ref}`the example
  <jdbc-java-connection>`.

Client tags can be set as follows:

- CLI: use the `--client-tags` option.
- JDBC driver when used in client apps: add the `clientTags` property to the
  connection configuration and set the value when using a Java application that
  uses the JDBC Driver.
- JDBC driver used with Java programs: add a property with the key
  `clientTags` and the value on the `Connection` instance as shown in
  {ref}`the example <jdbc-parameter-reference>`.

## Example

In the example configuration below, there are several resource groups, some of which are templates.
Templates allow administrators to construct resource group trees dynamically. For example, in
the `pipeline_${USER}` group, `${USER}` is expanded to the name of the user that submitted
the query. `${SOURCE}` is also supported, which is expanded to the source that submitted the
query. You may also use custom named variables in the regular expressions for `user`, `source`,
`originalUser`, and `authenticatedUser`.

There are six selectors, that define which queries run in which resource group:

- The first selector matches queries from `bob` and places them in the admin group.
- The next selector matches queries with an _original_ user of `bob`
  and places them in the admin group.
- The next selector matches queries with an _authenticated_ user of `bob`
  and places them in the admin group.
- The next selector matches queries from `admin` user group and places them in the admin group.
- The next selector matches all data definition (DDL) queries from a source name that includes `pipeline`
  and places them in the `global.data_definition` group. This could help reduce queue times for this
  class of queries, since they are expected to be fast.
- The next selector matches queries from a source name that includes `pipeline`, and places them in a
  dynamically-created per-user pipeline group under the `global.pipeline` group.
- The next selector matches queries that come from BI tools which have a source matching the regular
  expression `jdbc#(?<toolname>.*)` and have client provided tags that are a superset of `hipri`.
  These are placed in a dynamically-created sub-group under the `global.adhoc` group.
  The dynamic sub-groups are created based on the values of named variables `toolname` and `user`.
  The values are derived from the source regular expression and the query user respectively.
  Consider a query with a source `jdbc#powerfulbi`, user `kayla`, and client tags `hipri` and `fast`.
  This query is routed to the `global.adhoc.bi-powerfulbi.kayla` resource group.
- The last selector is a catch-all, which places all queries that have not yet been matched into a per-user
  adhoc group.

Together, these selectors implement the following policy:

- The user `bob` and any user belonging to user group `admin` is an admin and can run up to
  50 concurrent queries. `bob` will be treated as an admin even if they have changed their session
  user to a different user (i.e. via a `SET SESSION AUTHORIZATION` statement or the
  `X-Trino-User` request header). Queries will be run based on user-provided priority.

For the remaining users:

- No more than 100 total queries may run concurrently.
- Up to 5 concurrent DDL queries with a source `pipeline` can run. Queries are run in FIFO order.
- Non-DDL queries will run under the `global.pipeline` group, with a total concurrency of 45, and a per-user
  concurrency of 5. Queries are run in FIFO order.
- For BI tools, each tool can run up to 10 concurrent queries, and each user can run up to 3. If the total demand
  exceeds the limit of 10, the user with the fewest running queries gets the next concurrency slot. This policy
  results in fairness when under contention.
- All remaining queries are placed into a per-user group under `global.adhoc.other` that behaves similarly.

### File resource group manager

```{literalinclude} resource-groups-example.json
:language: json
```

### Database resource group manager

This example is for a MySQL database.

```sql
-- global properties
INSERT INTO resource_groups_global_properties (name, value) VALUES ('cpu_quota_period', '1h');

-- Every row in resource_groups table indicates a resource group.
-- The enviroment name is 'test_environment', make sure it matches `node.environment` in your cluster.
-- The parent-child relationship is indicated by the ID in 'parent' column.

-- create a root group 'global' with NULL parent
INSERT INTO resource_groups (name, soft_memory_limit, hard_concurrency_limit, max_queued, scheduling_policy, jmx_export, environment) VALUES ('global', '80%', 100, 1000, 'weighted', true, 'test_environment');

-- get ID of 'global' group
SELECT resource_group_id FROM resource_groups WHERE name = 'global';  -- 1
-- create two new groups with 'global' as parent
INSERT INTO resource_groups (name, soft_memory_limit, hard_concurrency_limit, max_queued, scheduling_weight, environment, parent) VALUES ('data_definition', '10%', 5, 100, 1, 'test_environment', 1);
INSERT INTO resource_groups (name, soft_memory_limit, hard_concurrency_limit, max_queued, scheduling_weight, environment, parent) VALUES ('adhoc', '10%', 50, 1, 10, 'test_environment', 1);

-- get ID of 'adhoc' group
SELECT resource_group_id FROM resource_groups WHERE name = 'adhoc';   -- 3
-- create 'other' group with 'adhoc' as parent
INSERT INTO resource_groups (name, soft_memory_limit, hard_concurrency_limit, max_queued, scheduling_weight, scheduling_policy, environment, parent) VALUES ('other', '10%', 2, 1, 10, 'weighted_fair', 'test_environment', 3);

-- get ID of 'other' group
SELECT resource_group_id FROM resource_groups WHERE name = 'other';  -- 4
-- create '${USER}' group with 'other' as parent.
INSERT INTO resource_groups (name, soft_memory_limit, hard_concurrency_limit, max_queued, environment, parent) VALUES ('${USER}', '10%', 1, 100, 'test_environment', 4);

-- create 'bi-${toolname}' group with 'adhoc' as parent
INSERT INTO resource_groups (name, soft_memory_limit, hard_concurrency_limit, max_queued, scheduling_weight, scheduling_policy, environment, parent) VALUES ('bi-${toolname}', '10%', 10, 100, 10, 'weighted_fair', 'test_environment', 3);

-- get ID of 'bi-${toolname}' group
SELECT resource_group_id FROM resource_groups WHERE name = 'bi-${toolname}';  -- 6
-- create '${USER}' group with 'bi-${toolname}' as parent. This indicates
-- nested group 'global.adhoc.bi-${toolname}.${USER}', and will have a
-- different ID than 'global.adhoc.other.${USER}' created above.
INSERT INTO resource_groups (name, soft_memory_limit, hard_concurrency_limit, max_queued,  environment, parent) VALUES ('${USER}', '10%', 3, 10, 'test_environment', 6);

-- create 'pipeline' group with 'global' as parent
INSERT INTO resource_groups (name, soft_memory_limit, hard_concurrency_limit, max_queued, scheduling_weight, jmx_export, environment, parent) VALUES ('pipeline', '80%', 45, 100, 1, true, 'test_environment', 1);

-- get ID of 'pipeline' group
SELECT resource_group_id FROM resource_groups WHERE name = 'pipeline'; -- 8
-- create 'pipeline_${USER}' group with 'pipeline' as parent
INSERT INTO resource_groups (name, soft_memory_limit, hard_concurrency_limit, max_queued,  environment, parent) VALUES ('pipeline_${USER}', '50%', 5, 100, 'test_environment', 8);

-- create a root group 'admin' with NULL parent
INSERT INTO resource_groups (name, soft_memory_limit, hard_concurrency_limit, max_queued, scheduling_policy, environment, jmx_export) VALUES ('admin', '100%', 50, 100, 'query_priority', 'test_environment', true);


-- Selectors

-- use ID of 'admin' resource group for selector
INSERT INTO selectors (resource_group_id, user_regex, priority) VALUES ((SELECT resource_group_id FROM resource_groups WHERE name = 'admin'), 'bob', 6);

-- use ID of 'admin' resource group for selector
INSERT INTO selectors (resource_group_id, user_group_regex, priority) VALUES ((SELECT resource_group_id FROM resource_groups WHERE name = 'admin'), 'admin', 5);

-- use ID of 'global.data_definition' resource group for selector
INSERT INTO selectors (resource_group_id, source_regex, query_type, priority) VALUES ((SELECT resource_group_id FROM resource_groups WHERE name = 'data_definition'), '.*pipeline.*', 'DATA_DEFINITION', 4);

-- use ID of 'global.pipeline.pipeline_${USER}' resource group for selector
INSERT INTO selectors (resource_group_id, source_regex, priority) VALUES ((SELECT resource_group_id FROM resource_groups WHERE name = 'pipeline_${USER}'), '.*pipeline.*', 3);

-- get ID of 'global.adhoc.bi-${toolname}.${USER}' resource group by disambiguating group name using parent ID
SELECT A.resource_group_id self_id, B.resource_group_id parent_id, concat(B.name, '.', A.name) name_with_parent
FROM resource_groups A JOIN resource_groups B ON A.parent = B.resource_group_id
WHERE A.name = '${USER}' AND B.name = 'bi-${toolname}';
--  7 |         6 | bi-${toolname}.${USER}
INSERT INTO selectors (resource_group_id, source_regex, client_tags, priority) VALUES (7, 'jdbc#(?<toolname>.*)', '["hipri"]', 2);

-- get ID of 'global.adhoc.other.${USER}' resource group for by disambiguating group name using parent ID
SELECT A.resource_group_id self_id, B.resource_group_id parent_id, concat(B.name, '.', A.name) name_with_parent
FROM resource_groups A JOIN resource_groups B ON A.parent = B.resource_group_id
WHERE A.name = '${USER}' AND B.name = 'other';
-- |       5 |         4 | other.${USER}    |
INSERT INTO selectors (resource_group_id, priority) VALUES (5, 1);
```
