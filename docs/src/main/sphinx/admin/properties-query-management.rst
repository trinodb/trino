===========================
Query management properties
===========================

``query.max-execution-time``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-duration`
* **Default value:** ``100d``
* **Session property:** ``query_max_execution_time``

The maximum allowed time for a query to be actively executing on the
cluster, before it is terminated. Compared to the run time below, execution
time does not include analysis, query planning or wait times in a queue.

``query.max-planning-time``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-duration`
* **Default value:** ``10m``
* **Session property:** ``query_max_planning_time``

The maximum allowed time for a query to be actively planning the execution.
After this period the coordinator will make its best effort to stop the
query. Note that some operations in planning phase are not easily cancellable
and may not terminate immediately.

``query.max-run-time``
^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-duration`
* **Default value:** ``100d``
* **Session property:** ``query_max_run_time``

The maximum allowed time for a query to be processed on the cluster, before
it is terminated. The time includes time for analysis and planning, but also
time spend in a queue waiting, so essentially this is the time allowed for a
query to exist since creation.

``query.max-stage-count``
^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-integer`
* **Default value:** ``100``
* **Minimum value:** ``1``

The maximum number of stages allowed to be generated per query. If a query
generates more stages than this it will get killed with error
``QUERY_HAS_TOO_MANY_STAGES``.

.. warning::

    Setting this to a high value can cause queries with large number of
    stages to introduce instability in the cluster causing unrelated queries
    to get killed with ``REMOTE_TASK_ERROR`` and the message
    ``Max requests queued per destination exceeded for HttpDestination ...``

``query.max-history``
^^^^^^^^^^^^^^^^^^^^^
* **Type:** :ref:`prop-type-integer`
* **Default value:** ``100``

The maximum number of queries to keep in the query history to provide
statistics and other information. If this amount is reached, queries are
removed based on age.

``query.min-expire-age``
^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-duration`
* **Default value:** ``15m``

The minimal age of a query in the history before it is expired. An expired
query is removed from the query history buffer and no longer available in
the :doc:`/admin/web-interface`.
