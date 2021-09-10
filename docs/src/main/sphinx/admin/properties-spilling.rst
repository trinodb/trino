===================
Spilling properties
===================

These properties control :doc:`spill`.

``spill-enabled``
^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-boolean`
* **Default value:** ``false``

Try spilling memory to disk to avoid exceeding memory limits for the query.

Spilling works by offloading memory to disk. This process can allow a query with a large memory
footprint to pass at the cost of slower execution times. Spilling is supported for
aggregations, joins (inner and outer), sorting, and window functions. This property does not
reduce memory usage required for other join types.

This config property can be overridden by the ``spill_enabled`` session property.

``spill-order-by``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-boolean`
* **Default value:** ``true``

Try spilling memory to disk to avoid exceeding memory limits for the query when running sorting operators.
This property must be used in conjunction with the ``spill-enabled`` property.

This config property can be overridden by the ``spill_order_by`` session property.

``spill-window-operator``
^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-boolean`
* **Default value:** ``true``

Try spilling memory to disk to avoid exceeding memory limits for the query when running window operators.
This property must be used in conjunction with the ``spill-enabled`` property.

This config property can be overridden by the ``spill_window_operator`` session property.

``spiller-spill-path``
^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-string`
* **No default value.** Must be set when spilling is enabled

Directory where spilled content is written. It can be a comma separated
list to spill simultaneously to multiple directories, which helps to utilize
multiple drives installed in the system.

It is not recommended to spill to system drives. Most importantly, do not spill
to the drive on which the JVM logs are written, as disk overutilization might
cause JVM to pause for lengthy periods, causing queries to fail.

``spiller-max-used-space-threshold``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-double`
* **Default value:** ``0.9``

If disk space usage ratio of a given spill path is above this threshold,
this spill path is not eligible for spilling.

``spiller-threads``
^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-integer`
* **Default value:** ``4``

Number of spiller threads. Increase this value if the default is not able
to saturate the underlying spilling device (for example, when using RAID).

``max-spill-per-node``
^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-data-size`
* **Default value:** ``100GB``

Max spill space to be used by all queries on a single node.

``query-max-spill-per-node``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-data-size`
* **Default value:** ``100GB``

Max spill space to be used by a single query on a single node.

``aggregation-operator-unspill-memory-limit``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-data-size`
* **Default value:** ``4MB``

Limit for memory used for unspilling a single aggregation operator instance.

``spill-compression-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-boolean`
* **Default value:** ``false``

Enables data compression for pages spilled to disk.

``spill-encryption-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** :ref:`prop-type-boolean`
* **Default value:** ``false``

Enables using a randomly generated secret key (per spill file) to encrypt and decrypt
data spilled to disk.
