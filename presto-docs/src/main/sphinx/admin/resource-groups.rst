===============
Resource Groups
===============

Resource groups place limits on resource usage, and can enforce queueing policies on
queries that run within them, or divide their resources among sub-groups. A query
belongs to a single resource group, and consumes resources from that group (and its ancestors).
Except for the limit on queued queries, when a resource group runs out of a resource
it does not cause running queries to fail; instead new queries become queued.
A resource group may have sub-groups or may accept queries, but may not do both.

The resource groups and associated selection rules are configured by a manager, which is pluggable.

There are two built-in managers: file-based and database-based. Add an ``etc/resource-groups.properties``
file with the appropriate configurations to enable one of the two managers.

File Resource Group Manager
---------------------------

This manager reads the resource group configuration from a JSON config file.

.. code-block:: none

    resource-groups.configuration-manager=file
    resource-groups.config-file=etc/resource-groups.json

* ``resource-groups.config-file``: a JSON file storing the configurations, which can be an absolute
  path, or a path relative to the Presto data directory.

Db Resource Group Manager
-------------------------

This manager periodically loads the resource group configuration from a database.

.. code-block:: none

    resource-groups.configuration-manager=db
    resource-groups.config-db-url=jdbc:mysql://localhost:3306/config_db?user=presto_admin
    resource-groups.max-refresh-interval=1m
    resource-groups.exact-match-selector-enabled=true

* ``resource-groups.config-db-url`` (required): Database URL to load configuration from.
* ``resource-groups.max-refresh-interval``: Time period for which the cluster will continue to accept
  queries after refresh failures cause configuration to become stale. The default is ``1h``.
* ``resource-groups.exact-match-selector-enabled``: Boolean flag to enable usage of a selector requiring
  an exact match of environment, source and query type.

The configuration is loaded every second from the following tables under the specified db url. If the
configuration ``resource-groups.exact-match-selector-enabled`` is set to ``true``, an additional table
is used for loading exact-match based selectors. If any of the tables do not exist, Presto creates them
on server startup.

+---------------------+-------------------------------------------------------+---------------------------------+
| Table Name          | Schema                                                | Purpose                         |
+=====================+=======================================================+=================================+
| resource\_groups\_  | .. code-block:: sql                                   |Store properties listed in       |
| global\_properties  |                                                       |:ref:`global-properties`         |
|                     |     name VARCHAR(128) NOT NULL PRIMARY KEY,           |                                 |
|                     |     value VARCHAR(512) NULL,                          |                                 |
|                     |     CHECK (name in ('cpu_quota_period'))              |                                 |
+---------------------+-------------------------------------------------------+---------------------------------+
| resource\_groups    | .. code-block:: sql                                   |Store resource group             |
|                     |                                                       |configuration described in       |
|                     |     resource_group_id BIGINT NOT NULL AUTO_INCREMENT, |:ref:`resource-group-properties`.|
|                     |     name VARCHAR(250) NOT NULL,                       |                                 |
|                     |     soft_memory_limit VARCHAR(128) NOT NULL,          |                                 |
|                     |     max_queued INT NOT NULL,                          |                                 |
|                     |     soft_concurrency_limit INT NULL,                  |                                 |
|                     |     hard_concurrency_limit INT NOT NULL,              |                                 |
|                     |     scheduling_policy VARCHAR(128) NULL,              |                                 |
|                     |     scheduling_weight INT NULL,                       |                                 |
|                     |     jmx_export BOOLEAN NULL,                          |                                 |
|                     |     soft_cpu_limit VARCHAR(128) NULL,                 |                                 |
|                     |     hard_cpu_limit VARCHAR(128) NULL,                 |                                 |
|                     |     parent BIGINT NULL,                               |                                 |
|                     |     environment VARCHAR(128) NULL,                    |                                 |
|                     |     PRIMARY KEY (resource_group_id),                  |                                 |
|                     |     FOREIGN KEY (parent) REFERENCES                   |                                 |
|                     |         resource_groups (resource_group_id)           |                                 |
+---------------------+-------------------------------------------------------+---------------------------------+
|   selectors         | .. code-block:: sql                                   |Store selector rules             |
|                     |                                                       |to match input queries           |
|                     |     resource_group_id BIGINT NOT NULL,                |against resource groups, refer   |
|                     |     priority BIGINT NOT NULL,                         |to :ref:`selector-rules`.        |
|                     |     user_regex VARCHAR(512),                          |If there are multiple matches    |
|                     |     source_regex VARCHAR(512),                        |for a query, the one with the    |
|                     |     query_type VARCHAR(512),                          |highest priority is selected.    |
|                     |     client_tags VARCHAR(512),                         |                                 |
|                     |     selector_resource_estimate VARCHAR(1024),         |                                 |
|                     |     FOREIGN KEY (resource_group_id) REFERENCES        |                                 |
|                     |         resource_groups (resource_group_id)           |                                 |
+---------------------+-------------------------------------------------------+---------------------------------+
| exact\_match\_      | .. code-block:: sql                                   |Store selector rules             |
| source\_selectors   |                                                       |for an exact match of source,    |
|                     |     environment VARCHAR(128),                         |environment and query type.      |
|                     |     source VARCHAR(512) NOT NULL,                     |``NULL`` values for query_type   |
|                     |     query_type VARCHAR(512),                          |and environment are treated as   |
|                     |     update_time DATETIME NOT NULL,                    |wildcards, and have a lower      |
|                     |     resource_group_id VARCHAR(256) NOT NULL,          |priority than exact matches.     |
|                     |     PRIMARY KEY (environment, source, query_type),    |                                 |
|                     |     UNIQUE (                                          |                                 |
|                     |         source,                                       |                                 |
|                     |         environment,                                  |                                 |
|                     |         query_type,                                   |                                 |
|                     |         resource_group_id)                            |                                 |
+---------------------+-------------------------------------------------------+---------------------------------+

.. _resource-group-properties:

Resource Group Properties
-------------------------

* ``name`` (required): name of the group. May be a template (see below).

* ``maxQueued`` (required): maximum number of queued queries. Once this limit is reached
  new queries are rejected.

* ``softConcurrencyLimit`` (optional): number of concurrently running queries after which
  new queries will only run if all peer resource groups below their soft limits are ineligible
  or if all eligible peers are above soft limits.

* ``hardConcurrencyLimit`` (required): maximum number of running queries.

* ``softMemoryLimit`` (required): maximum amount of distributed memory this
  group may use, before new queries become queued. May be specified as
  an absolute value (i.e. ``1GB``) or as a percentage (i.e. ``10%``) of the cluster's memory.

* ``softCpuLimit`` (optional): maximum amount of CPU time this
  group may use in a period (see ``cpuQuotaPeriod``), before a penalty is applied to
  the maximum number of running queries. ``hardCpuLimit`` must also be specified.

* ``hardCpuLimit`` (optional): maximum amount of CPU time this
  group may use in a period.

* ``schedulingPolicy`` (optional): specifies how queued queries are selected to run,
  and how sub-groups become eligible to start their queries. May be one of three values:

  * ``fair`` (default): queued queries are processed first-in-first-out, and sub-groups
    must take turns starting new queries, if they have any queued.

  * ``weighted_fair``: sub-groups are selected based on their ``schedulingWeight`` and the number of
    queries they are already running concurrently. The expected share of running queries for a
    sub-group is computed based on the weights for all currently eligible sub-groups. The sub-group
    with the least concurrency relative to its share is selected to start the next query.

  * ``weighted``: queued queries are selected stochastically in proportion to their priority,
    specified via the ``query_priority`` :doc:`session property </sql/set-session>`. Sub groups are selected
    to start new queries in proportion to their ``schedulingWeight``.

  * ``query_priority``: all sub-groups must also be configured with ``query_priority``.
    Queued queries are selected strictly according to their priority.

* ``schedulingWeight`` (optional): weight of this sub-group. See above.
  Defaults to ``1``.

* ``jmxExport`` (optional): If true, group statistics are exported to JMX for monitoring.
  Defaults to ``false``.

* ``subGroups`` (optional): list of sub-groups.

.. _selector-rules:

Selector Rules
--------------

* ``user`` (optional): regex to match against user name.

* ``userGroup`` (optional): regex to match against every user group the user belongs to.

* ``source`` (optional): regex to match against source string.

* ``queryType`` (optional): string to match against the type of the query submitted:

  * ``DATA_DEFINITION``: Queries that alter/create/drop the metadata of schemas/tables/views, and that manage
    prepared statements, privileges, sessions, and transactions.
  * ``DELETE``: ``DELETE`` queries.
  * ``DESCRIBE``: ``DESCRIBE``, ``DESCRIBE INPUT``, ``DESCRIBE OUTPUT``, and ``SHOW`` queries.
  * ``EXPLAIN``: ``EXPLAIN`` queries.
  * ``INSERT``: ``INSERT`` and ``CREATE TABLE AS`` queries.
  * ``SELECT``: ``SELECT`` queries.

* ``clientTags`` (optional): list of tags. To match, every tag in this list must be in the list of
  client-provided tags associated with the query.

* ``group`` (required): the group these queries will run in.

Selectors are processed sequentially and the first one that matches will be used.

.. _global-properties:

Global Properties
-----------------

* ``cpuQuotaPeriod`` (optional): the period in which cpu quotas are enforced.

Example
-------

In the example configuration below, there are several resource groups, some of which are templates.
Templates allow administrators to construct resource group trees dynamically. For example, in
the ``pipeline_${USER}`` group, ``${USER}`` is expanded to the name of the user that submitted
the query. ``${SOURCE}`` is also supported, which is expanded to the source that submitted the
query. You may also use custom named variables in the ``source`` and ``user`` regular expressions.

There are four selectors, that define which queries run in which resource group:

* The first selector matches queries from ``bob`` and places them in the admin group.

* The second selector matches queries from ``admin`` user group and places them in the admin group.

* The third selector matches all data definition (DDL) queries from a source name that includes ``pipeline``
  and places them in the ``global.data_definition`` group. This could help reduce queue times for this
  class of queries, since they are expected to be fast.

* The fourth selector matches queries from a source name that includes ``pipeline``, and places them in a
  dynamically-created per-user pipeline group under the ``global.pipeline`` group.

* The fifth selector matches queries that come from BI tools which have a source matching the regular
  expression ``jdbc#(?<toolname>.*)``, and have client provided tags that are a superset of ``hi-pri``.
  These are placed in a dynamically-created sub-group under the ``global.pipeline.tools`` group. The dynamic
  sub-group are created based on the named variable ``toolname``, which is extracted from the in the
  regular expression for source. Consider a query with a source ``jdbc#powerfulbi``, user ``kayla``, and
  client tags ``hipri`` and ``fast``. This query is routed to the ``global.pipeline.bi-powerfulbi.kayla``
  resource group.

* The last selector is a catch-all, which places all queries that have not yet been matched into a per-user
  adhoc group.

Together, these selectors implement the following policy:

* The user ``bob`` and any user belonging to user group ``admin``
  is an admin and can run up to 50 concurrent queries.
  Queries will be run based on user-provided priority.

For the remaining users:

* No more than 100 total queries may run concurrently.

* Up to 5 concurrent DDL queries with a source ``pipeline`` can run. Queries are run in FIFO order.

* Non-DDL queries will run under the ``global.pipeline`` group, with a total concurrency of 45, and a per-user
  concurrency of 5. Queries are run in FIFO order.

* For BI tools, each tool can run up to 10 concurrent queries, and each user can run up to 3. If the total demand
  exceeds the limit of 10, the user with the fewest running queries gets the next concurrency slot. This policy
  results in fairness when under contention.

* All remaining queries are placed into a per-user group under ``global.adhoc.other`` that behaves similarly.

.. literalinclude:: resource-groups-example.json
    :language: json

Providing Selector Properties
-----------------------------

The source name can be set as follows:

* CLI: use the ``--source`` option.

* JDBC: set the ``ApplicationName`` client info property on the ``Connection`` instance.

Client tags can be set as follows:

* CLI: use the ``--client-tags`` option.

* JDBC: set the ``ClientTags`` client info property on the ``Connection`` instance.
