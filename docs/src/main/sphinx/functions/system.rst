==================
System information
==================

Functions providing information about the Trino cluster system environment. More
information is available by querying the various schemas and tables exposed by
the :doc:`/connector/system`.

.. function:: version() -> varchar

    Returns the Trino version used on the cluster. Equivalent to the value of
    the ``node_version`` column in the ``system.runtime.nodes`` table.
