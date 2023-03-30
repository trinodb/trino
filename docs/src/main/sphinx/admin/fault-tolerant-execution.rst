========================
Fault-tolerant execution
========================

By default, if a Trino node lacks the resources to execute a task or
otherwise fails during query execution, the query fails and must be run again
manually. The longer the runtime of a query, the more likely it is to be
susceptible to such failures.

Fault-tolerant execution is a mechanism in Trino that enables a cluster to
mitigate query failures by retrying queries or their component tasks in
the event of failure. With fault-tolerant execution enabled, intermediate
exchange data is spooled and can be re-used by another worker in the event of a
worker outage or other fault during query execution.

.. note::

    Fault tolerance does not apply to broken queries or other user error. For
    example, Trino does not spend resources retrying a query that fails because
    its SQL cannot be parsed.

    For a step-by-step guide explaining how to configure a Trino cluster with
    fault-tolerant execution to improve query processing resilience, read
    :doc:`/installation/query-resiliency`.

Configuration
-------------

Fault-tolerant execution is disabled by default. To enable the feature, set the
``retry-policy`` configuration property to either ``QUERY`` or ``TASK``
depending on the desired :ref:`retry policy <fte-retry-policy>`.

.. code-block:: properties

    retry-policy=QUERY

.. warning::

  Setting ``retry-policy`` disables :ref:`write operations
  <sql-write-operations>` with connectors that do not support fault-tolerant
  execution of write operations, resulting in a "This connector does not support
  query retries" error message.

  Support for fault-tolerant execution of SQL statements varies on a
  per-connector basis:

  * Fault-tolerant execution of :ref:`read operations <sql-read-operations>` is
    supported by all connectors.
  * Fault-tolerant execution of :ref:`write operations <sql-write-operations>`
    is supported by the following connectors:

    * :doc:`/connector/bigquery`
    * :doc:`/connector/delta-lake`
    * :doc:`/connector/hive`
    * :doc:`/connector/iceberg`
    * :doc:`/connector/mongodb`
    * :doc:`/connector/mysql`
    * :doc:`/connector/postgresql`
    * :doc:`/connector/sqlserver`

The following configuration properties control the behavior of fault-tolerant
execution on a Trino cluster:

.. list-table:: Fault-tolerant execution configuration properties
   :widths: 30, 50, 20
   :header-rows: 1

   * - Property name
     - Description
     - Default value
   * - ``retry-policy``
     - Configures what is retried in the event of failure, either
       ``QUERY`` to retry the whole query, or ``TASK`` to retry tasks
       individually if they fail. See :ref:`retry policy <fte-retry-policy>` for
       more information.
     - ``NONE``
   * - ``exchange.deduplication-buffer-size``
     - Size of the coordinator's in-memory buffer used by fault-tolerant
       execution to store output of query :ref:`stages <trino-concept-stage>`.
       If this buffer is filled during query execution, the query fails with a
       "Task descriptor storage capacity has been exceeded" error message unless
       an :ref:`exchange manager <fte-exchange-manager>` is configured.
     - ``32MB``
   * - ``exchange.compression-enabled``
     - Enable compression of spooling data. Setting to ``true`` is recommended
       when using an :ref:`exchange manager <fte-exchange-manager>`.
     - ``false``

.. _fte-retry-policy:

Retry policy
------------

The ``retry-policy`` configuration property designates whether Trino retries
entire queries or a query's individual tasks in the event of failure.

QUERY
^^^^^

A ``QUERY`` retry policy instructs Trino to automatically retry a query in the
event of an error occuring on a worker node. A ``QUERY`` retry policy is
recommended when the majority of the Trino cluster's workload consists of many
small queries, or if an :ref:`exchange manager <fte-exchange-manager>` is not
configured.

By default Trino does not implement fault tolerance for queries whose result set
exceeds 32MB in size, such as :doc:`/sql/select` statements that return a very
large data set to the user. This limit can be increased by modifying the
``exchange.deduplication-buffer-size`` configuration property to be greater than
the default value of ``32MB``, but this results in higher memory usage on the
coordinator.

To enable fault-tolerant execution on queries with a larger result set, it is
strongly recommended to configure an :ref:`exchange manager
<fte-exchange-manager>` that utilizes external storage for spooled data and
therefore allows for storage of spilled data beyond the in-memory buffer size.

TASK
^^^^

A ``TASK`` retry policy instructs Trino to retry individual query
:ref:`tasks <trino-concept-task>` in the event of failure. This policy is
recommended when executing large batch queries, as the cluster can more
efficiently retry smaller tasks within the query rather than retry the whole
query.

When a cluster is configured with a ``TASK`` retry policy, some relevant
configuration properties have their default values changed to follow best
practices for a fault-tolerant cluster. However, this automatic change does not
affect clusters that have these properties manually configured. If you have
any of the following properties configured in the ``config.properties`` file on
a cluster with a ``TASK`` retry policy, it is strongly recommended to make the
following changes:

* Set the ``task.low-memory-killer.policy``
  :doc:`query management property </admin/properties-query-management>` to
  ``total-reservation-on-blocked-nodes``, or queries may
  need to be manually killed if the cluster runs out of memory.
* Set the ``query.low-memory-killer.delay``
  :doc:`query management property </admin/properties-query-management>` to
  ``0s`` so the cluster immediately unblocks nodes that run out of memory.
* Modify the ``query.remote-task.max-error-duration``
  :doc:`query management property </admin/properties-query-management>`
  to adjust how long Trino allows a remote task to try reconnecting before
  considering it lost and rescheduling.

.. note::

  A ``TASK`` retry policy is best suited for large batch queries, but this
  policy can result in higher latency for short-running queries executed in high
  volume. As a best practice, it is recommended to run a dedicated cluster
  with a ``TASK`` retry policy for large batch queries, separate from another
  cluster that handles short queries.

Advanced configuration
----------------------

You can further configure fault-tolerant execution with the following
configuration properties. The default values for these properties should work
for most deployments, but you can change these values for testing or
troubleshooting purposes.

Retry limits
^^^^^^^^^^^^

The following configuration properties control the thresholds at which
queries/tasks are no longer retried in the event of repeated failures:

.. list-table:: Fault tolerance retry limit configuration properties
   :widths: 30, 50, 20, 30
   :header-rows: 1

   * - Property name
     - Description
     - Default value
     - Retry policy
   * - ``query-retry-attempts``
     - Maximum number of times Trino may attempt to retry a query before
       declaring the query as failed.
     - ``4``
     - Only ``QUERY``
   * - ``task-retry-attempts-per-task``
     - Maximum number of times Trino may attempt to retry a single task before
       declaring the query as failed.
     - ``4``
     - Only ``TASK``
   * - ``retry-initial-delay``
     - Minimum time that a failed query or task must wait before it is retried. May be
       overridden with the ``retry_initial_delay`` :ref:`session property
       <session-properties-definition>`.
     - ``10s``
     - ``QUERY`` and ``TASK``
   * - ``retry-max-delay``
     - Maximum time that a failed query or task must wait before it is retried.
       Wait time is increased on each subsequent  failure. May be
       overridden with the ``retry_max_delay`` :ref:`session property
       <session-properties-definition>`.
     - ``1m``
     - ``QUERY`` and ``TASK``
   * - ``retry-delay-scale-factor``
     - Factor by which retry delay is increased on each query or task failure. May be
       overridden with the ``retry_delay_scale_factor`` :ref:`session property
       <session-properties-definition>`.
     - ``2.0``
     - ``QUERY`` and ``TASK``

Task sizing
^^^^^^^^^^^

With a ``TASK`` retry policy, it is important to manage the amount of data
processed in each task. If tasks are too small, the management of task
coordination can take more processing time and resources than executing the task
itself. If tasks are too large, then a single task may require more resources
than are available on any one node and therefore prevent the query from
completing.

Trino supports limited automatic task sizing. If issues are occurring
during fault-tolerant task execution, you can configure the following
configuration properties to manually control task sizing. These configuration
properties only apply to a ``TASK`` retry policy.

.. list-table:: Task sizing configuration properties
   :widths: 30, 50, 20
   :header-rows: 1

   * - Property name
     - Description
     - Default value
   * - ``fault-tolerant-execution-target-task-input-size``
     - Target size in bytes of all task inputs for a single fault-tolerant task.
       Applies to tasks that read input from spooled data written by other
       tasks.

       May be overridden for the current session with the
       ``fault_tolerant_execution_target_task_input_size``
       :ref:`session property <session-properties-definition>`.
     - ``4GB``
   * - ``fault-tolerant-execution-target-task-split-count``
     - Target number of standard :ref:`splits <trino-concept-splits>` processed
       by a single task that reads data from source tables. Value is interpreted
       with split weight taken into account. If the weight of splits produced by
       a catalog denotes that they are lighter or heavier than "standard" split,
       then the number of splits processed by single task is adjusted
       accordingly.

       May be overridden for the current session with the
       ``fault_tolerant_execution_target_task_split_count``
       :ref:`session property <session-properties-definition>`.
     - ``64``
   * - ``fault-tolerant-execution-max-task-split-count``
     - Maximum number of :ref:`splits <trino-concept-splits>` processed by a
       single task. This value is not split weight-adjusted and serves as
       protection against situations where catalogs report an incorrect split
       weight.

       May be overridden for the current session with the
       ``fault_tolerant_execution_max_task_split_count``
       :ref:`session property <session-properties-definition>`.
     - ``256``

Node allocation
^^^^^^^^^^^^^^^

With a ``TASK`` retry policy, nodes are allocated to tasks based on available
memory and estimated memory usage. If task failure occurs due to exceeding
available memory on a node, the task is restarted with a request to allocate the
full node for its execution.

The initial task memory-requirements estimation is static and configured with
the ``fault-tolerant-task-memory`` configuration property. This property only
applies to a ``TASK`` retry policy.

.. list-table:: Node allocation configuration properties
   :widths: 30, 50, 20
   :header-rows: 1

   * - Property name
     - Description
     - Default value
   * - ``fault-tolerant-execution-task-memory``
     - Initial task memory estimation used for bin-packing when allocating nodes
       for tasks. May be overridden for the current session with the
       ``fault_tolerant_execution_task_memory``
       :ref:`session property <session-properties-definition>`.
     - ``5GB``

Other tuning
^^^^^^^^^^^^

The following additional configuration property can be used to manage
fault-tolerant execution:

.. list-table:: Other fault-tolerant execution configuration properties
   :widths: 30, 50, 20, 30
   :header-rows: 1

   * - Property name
     - Description
     - Default value
     - Retry policy
   * - ``fault-tolerant-execution-task-descriptor-storage-max-memory``
     - Maximum amount of memory to be used to store task descriptors for fault
       tolerant queries on coordinator. Extra memory is needed to be able to
       reschedule tasks in case of a failure.
     - (JVM heap size * 0.15)
     - Only ``TASK``
   * - ``fault-tolerant-execution-partition-count``
     - Number of partitions to use for distributed joins and aggregations,
       similar in function to the ``query.hash-partition-count`` :doc:`query
       management property </admin/properties-query-management>`. It is not
       recommended to increase this property value above the default of ``50``,
       which may result in instability and poor performance. May be overridden
       for the current session with the
       ``fault_tolerant_execution_partition_count`` :ref:`session property
       <session-properties-definition>`.
     - ``50``
     - Only ``TASK``
   * - ``max-tasks-waiting-for-node-per-stage``
     - Allow for up to configured number of tasks to wait for node allocation
       per stage, before pausing scheduling for other tasks from this stage.
     - 5
     - Only ``TASK``

.. _fte-exchange-manager:

Exchange manager
----------------

Exchange spooling is responsible for storing and managing spooled data for
fault-tolerant execution. You can configure a filesystem-based exchange manager
that stores spooled data in a specified location, such as :ref:`AWS S3
<fte-exchange-aws-s3>` and S3-compatible systems, :ref:`Azure Blob Storage
<fte-exchange-azure-blob>`, :ref:`Google Cloud Storage <fte-exchange-gcs>`,
or :ref:`HDFS <fte-exchange-hdfs>`.

Configuration
^^^^^^^^^^^^^

To configure an exchange manager, create a new
``etc/exchange-manager.properties`` configuration file on the coordinator and
all worker nodes. In this file, set the ``exchange-manager.name`` configuration
property to ``filesystem`` or ``hdfs``, and set additional configuration properties as needed
for your storage solution.

The following table lists the available configuration properties for
``exchange-manager.properties``, their default values, and which filesystem(s)
the property may be configured for:

.. list-table:: Exchange manager configuration properties
   :widths: 30, 50, 20, 30
   :header-rows: 1

   * - Property name
     - Description
     - Default value
     - Supported filesystem
   * - ``exchange.base-directories``
     - Comma-separated list of URI locations that the exchange manager uses to
       store spooling data.
     -
     - Any
   * - ``exchange.sink-buffer-pool-min-size``
     - The minimum buffer pool size for an exchange sink. The larger the buffer
       pool size, the larger the write parallelism and memory usage.
     - ``10``
     - Any
   * - ``exchange.sink-buffers-per-partition``
     - The number of buffers per partition in the buffer pool. The larger the
       buffer pool size, the larger the write parallelism and memory usage.
     - ``2``
     - Any
   * - ``exchange.sink-max-file-size``
     - Max size of files written by exchange sinks.
     - ``1GB``
     - Any
   * - ``exchange.source-concurrent-reader``
     - Number of concurrent readers to read from spooling storage. The
       larger the number of concurrent readers, the larger the read parallelism
       and memory usage.
     - ``4``
     - Any
   * - ``exchange.s3.aws-access-key``
     - AWS access key to use. Required for a connection to AWS S3 and GCS, can
       be ignored for other S3 storage systems.
     -
     - AWS S3, GCS
   * - ``exchange.s3.aws-secret-key``
     - AWS secret key to use. Required for a connection to AWS S3 and GCS, can
       be ignored for other S3 storage systems.
     -
     - AWS S3, GCS
   * - ``exchange.s3.iam-role``
     - IAM role to assume.
     -
     - AWS S3, GCS
   * - ``exchange.s3.external-id``
     - External ID for the IAM role trust policy.
     -
     - AWS S3, GCS
   * - ``exchange.s3.region``
     - Region of the S3 bucket.
     -
     - AWS S3, GCS
   * - ``exchange.s3.endpoint``
     - S3 storage endpoint server if using an S3-compatible storage system that
       is not AWS. If using AWS S3, this can be ignored. If using GCS, set it
       to ``https://storage.googleapis.com``.
     -
     - Any S3-compatible storage
   * - ``exchange.s3.max-error-retries``
     - Maximum number of times the exchange manager's S3 client should retry
       a request.
     - ``10``
     - Any S3-compatible storage
   * - ``exchange.s3.path-style-access``
     - Enables using `path-style access <https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html#path-style-access>`_
       for all requests to S3.
     - ``false``
     - Any S3-compatible storage
   * - ``exchange.s3.upload.part-size``
     - Part size for S3 multi-part upload.
     - ``5MB``
     - Any S3-compatible storage
   * - ``exchange.gcs.json-key-file-path``
     - Path to the JSON file that contains your Google Cloud Platform
       service account key. Not to be set together with
       ``exchange.gcs.json-key``
     -
     - GCS
   * - ``exchange.gcs.json-key``
     - Your Google Cloud Platform service account key in JSON format.
       Not to be set together with ``exchange.gcs.json-key-file-path``
     -
     - GCS
   * - ``exchange.azure.connection-string``
     - Connection string used to access the spooling container.
     -
     - Azure Blob Storage
   * - ``exchange.azure.block-size``
     - Block size for Azure block blob parallel upload.
     - ``4MB``
     - Azure Blob Storage
   * - ``exchange.azure.max-error-retries``
     - Maximum number of times the exchange manager's Azure client should
       retry a request.
     - ``10``
     - Azure Blob Storage
   * - ``exchange.hdfs.block-size``
     - Block size for HDFS storage.
     - ``4MB``
     - HDFS
   * - ``hdfs.config.resources``
     - Comma-separated list of paths to HDFS configuration files, for example ``/etc/hdfs-site.xml``.
       The files must exist on all nodes in the Trino cluster.
     -
     - HDFS

It is recommended to set the ``exchange.compression-enabled`` property to
``true`` in the cluster's ``config.properties`` file, to reduce the exchange
manager's overall I/O load. It is also recommended to configure a bucket
lifecycle rule to automatically expire abandoned objects in the event of a node
crash.

.. _fte-exchange-aws-s3:

AWS S3
~~~~~~

The following example ``exchange-manager.properties`` configuration specifies an
AWS S3 bucket as the spooling storage destination. Note that the destination
does not have to be in AWS, but can be any S3-compatible storage system.

.. code-block:: properties

    exchange-manager.name=filesystem
    exchange.base-directories=s3://exchange-spooling-bucket
    exchange.s3.region=us-west-1
    exchange.s3.aws-access-key=example-access-key
    exchange.s3.aws-secret-key=example-secret-key

You can configure multiple S3 buckets for the exchange manager to distribute
spooled data across buckets, reducing the I/O load on any one bucket. If a query
fails with the error message
"software.amazon.awssdk.services.s3.model.S3Exception: Please reduce your
request rate", this indicates that the workload is I/O intensive, and you should
specify multiple S3 buckets in ``exchange.base-directories`` to balance the
load:

.. code-block:: properties

    exchange.base-directories=s3://exchange-spooling-bucket-1,s3://exchange-spooling-bucket-2

.. _fte-exchange-azure-blob:

Azure Blob Storage
~~~~~~~~~~~~~~~~~~

The following example ``exchange-manager.properties`` configuration specifies an
Azure Blob Storage container as the spooling storage destination.

.. code-block:: properties

    exchange-manager.name=filesystem
    exchange.base-directories=abfs://container_name@account_name.dfs.core.windows.net
    exchange.azure.connection-string=connection-string

.. _fte-exchange-gcs:

Google Cloud Storage
~~~~~~~~~~~~~~~~~~~~

To enable exchange spooling on GCS in Trino, change the request endpoint to the
``https://storage.googleapis.com`` Google storage URI, and configure your AWS
access/secret keys to use the GCS HMAC keys. If you deploy Trino on GCP, you
must either create a service account with access to your spooling bucket or
configure the key path to your GCS credential file.

For more information on GCS's S3 compatibility, refer to the `Google Cloud
documentation on S3 migration
<https://cloud.google.com/storage/docs/aws-simple-migration>`_.

The following example ``exchange-manager.properties`` configuration specifies a
GCS bucket as the spooling storage destination.

.. code-block:: properties

    exchange-manager.name=filesystem
    exchange.base-directories=gs://exchange-spooling-bucket
    exchange.s3.region=us-west-1
    exchange.s3.aws-access-key=example-access-key
    exchange.s3.aws-secret-key=example-secret-key
    exchange.s3.endpoint=https://storage.googleapis.com
    exchange.gcs.json-key-file-path=/path/to/gcs_keyfile.json

.. _fte-exchange-hdfs:

HDFS
~~~~

The following ``exchange-manager.properties`` configuration example specifies HDFS
as the spooling storage destination.

.. code-block:: properties

    exchange-manager.name=hdfs
    exchange.base-directories=hadoop-master:9000/exchange-spooling-directory
    hdfs.config.resources=/usr/lib/hadoop/etc/hadoop/core-site.xml

.. _fte-exchange-local-filesystem:

Local filesystem storage
~~~~~~~~~~~~~~~~~~~~~~~~

The following example ``exchange-manager.properties`` configuration specifies a
local directory, ``/tmp/trino-exchange-manager``, as the spooling storage
destination.

.. note::

    It is only recommended to use a local filesystem for exchange in standalone,
    non-production clusters. A local directory can only be used for exchange in
    a distributed cluster if the exchange directory is shared and accessible
    from all worker nodes.

.. code-block:: properties

    exchange-manager.name=filesystem
    exchange.base-directories=/tmp/trino-exchange-manager
