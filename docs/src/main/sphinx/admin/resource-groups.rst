===============
Resource groups
===============

Resource groups place limits on resource usage, and can enforce queueing policies on
queries that run within them, or divide their resources among sub-groups. A query
belongs to a single resource group, and consumes resources from that group (and its ancestors).
Except for the limit on queued queries, when a resource group runs out of a resource
it does not cause running queries to fail; instead new queries become queued.
A resource group may have sub-groups or may accept queries, but may not do both.

The resource groups and associated selection rules are configured by a manager, which is pluggable.
Add an ``etc/resource-groups.properties`` file with the following contents to enable
the built-in manager that reads a JSON config file:

.. code-block:: text

    resource-groups.configuration-manager=file
    resource-groups.config-file=etc/resource-groups.json

Change the value of ``resource-groups.config-file`` to point to a JSON config file,
which can be an absolute path, or a path relative to the Trino data directory.

Resource group properties
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

* ``schedulingWeight`` (optional): weight of this subgroup used in ``weight`` 
  and the ``weighted_fair`` scheduling policy. Defaults to ``1``. See
  :ref:`scheduleweight-example`.

* ``jmxExport`` (optional): If true, group statistics are exported to JMX for monitoring.
  Defaults to ``false``.

* ``subGroups`` (optional): list of sub-groups.

.. _scheduleweight-example:

Scheduling weight example
^^^^^^^^^^^^^^^^^^^^^^^^^

Schedule weighting is a method of assigning a priority to a resource. Subgroups
with a higher scheduling weight are given higher priority. For example, to
ensure timely execution of scheduled pipelines queries, weight them higher than
adhoc queries.

In the following example, pipeline queries are weighted with a value of ``350``,
which is higher than the adhoc queries that have a scheduling weight of ``150``.
This means that approximately 70% (350 out of 500 queries) of your queries come
from the pipeline subgroup, and 30% (150 out of 500 queries) come from the adhoc
subgroup in a given timeframe. Alternatively, if you set each subgroup value to
``1``, the weight of the queries for the pipeline and adhoc subgroups are split
evenly and each receive 50% of the queries in a given timeframe. 

.. literalinclude:: schedule-weight-example.json
    :language: text

Selector rules
--------------

* ``user`` (optional): regex to match against user name.

* ``userGroup`` (optional): regex to match against every user group the user belongs to.

* ``source`` (optional): regex to match against source string.

* ``queryType`` (optional): string to match against the type of the query submitted:

  * ``SELECT``: ``SELECT`` queries.
  * ``EXPLAIN``: ``EXPLAIN`` queries (but not ``EXPLAIN ANALYZE``).
  * ``DESCRIBE``: ``DESCRIBE``, ``DESCRIBE INPUT``, ``DESCRIBE OUTPUT``, and ``SHOW`` queries.
  * ``INSERT``: ``INSERT``, ``CREATE TABLE AS``, and ``REFRESH MATERIALIZED VIEW`` queries.
  * ``UPDATE``: ``UPDATE`` queries.
  * ``DELETE``: ``DELETE`` queries.
  * ``ANALYZE``: ``ANALYZE`` queries.
  * ``DATA_DEFINITION``: Queries that alter/create/drop the metadata of schemas/tables/views,
    and that manage prepared statements, privileges, sessions, and transactions.

* ``clientTags`` (optional): list of tags. To match, every tag in this list must be in the list of
  client-provided tags associated with the query.

* ``group`` (required): the group these queries will run in.

Selectors are processed sequentially and the first one that matches will be used.

Global properties
-----------------

* ``cpuQuotaPeriod`` (optional): the period in which cpu quotas are enforced.

Providing selector properties
-----------------------------

The source name can be set as follows:

* CLI: use the ``--source`` option.

* JDBC: set the ``ApplicationName`` client info property on the ``Connection`` instance.

Client tags can be set as follows:

* CLI: use the ``--client-tags`` option.

* JDBC: set the ``ClientTags`` client info property on the ``Connection`` instance.

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
  expression ``jdbc#(?<toolname>.*)`` and have client provided tags that are a superset of ``hipri``.
  These are placed in a dynamically-created sub-group under the ``global.adhoc`` group.
  The dynamic sub-groups are created based on the values of named variables ``toolname`` and ``user``.
  The values are derived from the source regular expression and the query user respectively.
  Consider a query with a source ``jdbc#powerfulbi``, user ``kayla``, and client tags ``hipri`` and ``fast``.
  This query is routed to the ``global.adhoc.bi-powerfulbi.kayla`` resource group.

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
