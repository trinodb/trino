================
System connector
================

The System connector provides information and metrics about the currently
running Trino cluster. It makes this available via normal SQL queries.

Configuration
-------------

The System connector doesn't need to be configured: it is automatically
available via a catalog named ``system``.

Using the System connector
--------------------------

List the available system schemas::

    SHOW SCHEMAS FROM system;

List the tables in one of the schemas::

    SHOW TABLES FROM system.runtime;

Query one of the tables::

    SELECT * FROM system.runtime.nodes;

Kill a running query::

    CALL system.runtime.kill_query(query_id => '20151207_215727_00146_tx3nr', message => 'Using too many resources');

System connector tables
-----------------------

``metadata.catalogs``
^^^^^^^^^^^^^^^^^^^^^

The catalogs table contains the list of available catalogs.

``metadata.schema_properties``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The schema properties table contains the list of available properties
that can be set when creating a new schema.

``metadata.table_properties``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The table properties table contains the list of available properties
that can be set when creating a new table.

``metadata.materialized_view_properties``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The materialized view properties table contains the list of available properties
that can be set when creating a new materialized view.

``metadata.table_comments``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The table comments table contains the list of table comment.

``runtime.nodes``
^^^^^^^^^^^^^^^^^

The nodes table contains the list of visible nodes in the Trino
cluster along with their status.

.. _optimizer_rule_stats:

``runtime.optimizer_rule_stats``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``optimizer_rule_stats`` table contains the statistics for optimizer
rule invocations during the query planning phase. The statistics are
aggregated over all queries since the server start-up. The table contains
information about invocation frequency, failure rates and performance for
optimizer rules. For example, you can look at the multiplication of columns
``invocations`` and ``average_time`` to get an idea about which rules
generally impact query planning times the most.

``runtime.queries``
^^^^^^^^^^^^^^^^^^^

The queries table contains information about currently and recently
running queries on the Trino cluster. From this table you can find out
the original query SQL text, the identity of the user who ran the query,
and performance information about the query, including how long the query
was queued and analyzed.

``runtime.tasks``
^^^^^^^^^^^^^^^^^

The tasks table contains information about the tasks involved in a
Trino query, including where they were executed, and how many rows
and bytes each task processed.

``runtime.transactions``
^^^^^^^^^^^^^^^^^^^^^^^^

The transactions table contains the list of currently open transactions
and related metadata. This includes information such as the create time,
idle time, initialization parameters, and accessed catalogs.

System connector procedures
---------------------------

.. function:: runtime.kill_query(query_id, message)

    Kill the query identified by ``query_id``. The query failure message
    includes the specified ``message``.
