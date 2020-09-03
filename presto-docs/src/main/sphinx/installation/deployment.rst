================
Deploying Presto
================

Requirements
------------

Linux Operating System
^^^^^^^^^^^^^^^^^^^^^^

* 64-bit required
* newer release preferred, especially when running on containers
* adequate ulimits for the user that runs the Presto process. These limits
  may depend on the specific Linux distribution you are using. The number
  of open file descriptors needed for a particular Presto instance scales
  as roughly the number of machines in the cluster, times some factor 
  depending on the workload. We recommend the following limits, which can 
  typically be set in ``/etc/security/limits.conf``:

.. code-block:: none

    presto soft nofile 131072
    presto hard nofile 131072

.. _requirements-java:

Java Runtime Environment
^^^^^^^^^^^^^^^^^^^^^^^^

* 64-bit required
* version 11 recommended (lower versions not supported, higher versions less
  tested)
* Azul Zulu recommended (most tested)

Python
^^^^^^

* version 2.6.x, 2.7.x, or 3.x
* required by the ``bin/launcher`` script only

Installing Presto
-----------------

Download the Presto server tarball, :maven_download:`server`, and unpack it.
The tarball contains a single top-level directory,
|presto_server_release|, which we call the *installation* directory.

Presto needs a *data* directory for storing logs, etc.
We recommend creating a data directory outside of the installation directory,
which allows it to be easily preserved when upgrading Presto.

Configuring Presto
------------------

Create an ``etc`` directory inside the installation directory.
This holds the following configuration:

* Node Properties: environmental configuration specific to each node
* JVM Config: command line options for the Java Virtual Machine
* Config Properties: configuration for the Presto server
* Catalog Properties: configuration for :doc:`/connector` (data sources)

.. _presto_node_properties:

Node Properties
^^^^^^^^^^^^^^^

The node properties file, ``etc/node.properties``, contains configuration
specific to each node. A *node* is a single installed instance of Presto
on a machine. This file is typically created by the deployment system when
Presto is first installed. The following is a minimal ``etc/node.properties``:

.. code-block:: none

    node.environment=production
    node.id=ffffffff-ffff-ffff-ffff-ffffffffffff
    node.data-dir=/var/presto/data

The above properties are described below:

* ``node.environment``:
  The name of the environment. All Presto nodes in a cluster must have the same
  environment name. The name must start with an alphanumeric character and
  only contain alphanumeric, ``-``, or ``_`` characters.

* ``node.id``:
  The unique identifier for this installation of Presto. This must be
  unique for every node. This identifier should remain consistent across
  reboots or upgrades of Presto. If running multiple installations of
  Presto on a single machine (i.e. multiple nodes on the same machine),
  each installation must have a unique identifier. The identifier must start
  with an alphanumeric character and only contain alphanumeric, ``-``, or ``_``
  characters.

* ``node.data-dir``:
  The location (filesystem path) of the data directory. Presto stores
  logs and other data here.

.. _presto_jvm_config:

JVM Config
^^^^^^^^^^

The JVM config file, ``etc/jvm.config``, contains a list of command line
options used for launching the Java Virtual Machine. The format of the file
is a list of options, one per line. These options are not interpreted by
the shell, so options containing spaces or other special characters should
not be quoted.

The following provides a good starting point for creating ``etc/jvm.config``:

.. code-block:: none

    -server
    -Xmx16G
    -XX:-UseBiasedLocking
    -XX:+UseG1GC
    -XX:G1HeapRegionSize=32M
    -XX:+ExplicitGCInvokesConcurrent
    -XX:+ExitOnOutOfMemoryError
    -XX:+HeapDumpOnOutOfMemoryError
    -XX:ReservedCodeCacheSize=512M
    -XX:PerMethodRecompilationCutoff=10000
    -XX:PerBytecodeRecompilationCutoff=10000
    -Djdk.attach.allowAttachSelf=true
    -Djdk.nio.maxCachedBufferSize=2000000

Because an ``OutOfMemoryError`` typically leaves the JVM in an
inconsistent state, we write a heap dump, for debugging, and forcibly
terminate the process when this occurs.

The temporary directory used by the JVM must allow execution of code.
Specifically, the mount must not have the ``noexec`` flag set. The default
``/tmp`` directory is mounted with this flag in some installations, which
prevents Presto from starting. You can workaround this by overriding the
temporary directory by adding ``-Djava.io.tmpdir=/path/to/other/tmpdir`` to the
list of JVM options.

.. _config_properties:

Config Properties
^^^^^^^^^^^^^^^^^

The config properties file, ``etc/config.properties``, contains the
configuration for the Presto server. Every Presto server can function
as both a coordinator and a worker, but dedicating a single machine
to only perform coordination work provides the best performance on
larger clusters.

The following is a minimal configuration for the coordinator:

.. code-block:: none

    coordinator=true
    node-scheduler.include-coordinator=false
    http-server.http.port=8080
    query.max-memory=50GB
    query.max-memory-per-node=1GB
    query.max-total-memory-per-node=2GB
    discovery-server.enabled=true
    discovery.uri=http://example.net:8080

And this is a minimal configuration for the workers:

.. code-block:: none

    coordinator=false
    http-server.http.port=8080
    query.max-memory=50GB
    query.max-memory-per-node=1GB
    query.max-total-memory-per-node=2GB
    discovery.uri=http://example.net:8080

Alternatively, if you are setting up a single machine for testing, that
functions as both a coordinator and worker, use this configuration:

.. code-block:: none

    coordinator=true
    node-scheduler.include-coordinator=true
    http-server.http.port=8080
    query.max-memory=5GB
    query.max-memory-per-node=1GB
    query.max-total-memory-per-node=2GB
    discovery-server.enabled=true
    discovery.uri=http://example.net:8080

These properties require some explanation:

* ``coordinator``:
  Allow this Presto instance to function as a coordinator, so to
  accept queries from clients and manage query execution.

* ``node-scheduler.include-coordinator``:
  Allow scheduling work on the coordinator.
  For larger clusters, processing work on the coordinator
  can impact query performance because the machine's resources are not
  available for the critical task of scheduling, managing and monitoring
  query execution.

* ``http-server.http.port``:
  Specifies the port for the HTTP server. Presto uses HTTP for all
  communication, internal and external.

* ``query.max-memory``:
  The maximum amount of distributed memory, that a query may use.

* ``query.max-memory-per-node``:
  The maximum amount of user memory, that a query may use on any one machine.

* ``query.max-total-memory-per-node``:
  The maximum amount of user and system memory, that a query may use on any one machine,
  where system memory is the memory used during execution by readers, writers, and network buffers, etc.

* ``discovery-server.enabled``:
  Presto uses the Discovery service to find all the nodes in the cluster.
  Every Presto instance registers itself with the Discovery service
  on startup. In order to simplify deployment and avoid running an additional
  service, the Presto coordinator can run an embedded version of the
  Discovery service. It shares the HTTP server with Presto and thus uses
  the same port.

* ``discovery.uri``:
  The URI to the Discovery server. Because we have enabled the embedded
  version of Discovery in the Presto coordinator, this should be the
  URI of the Presto coordinator. Replace ``example.net:8080`` to match
  the host and port of the Presto coordinator. This URI must not end
  in a slash.

The above configuration properties are a minimal set to help you get started.
Please see :doc:`/admin` and :doc:`/security` for a more comprehensive list.
In particular, see :doc:`/admin/resource-groups` for configuring queuing policies.

Log Levels
^^^^^^^^^^

The optional log levels file, ``etc/log.properties``, allows setting the
minimum log level for named logger hierarchies. Every logger has a name,
which is typically the fully qualified name of the class that uses the logger.
Loggers have a hierarchy based on the dots in the name, like Java packages.
For example, consider the following log levels file:

.. code-block:: none

    io.prestosql=INFO

This would set the minimum level to ``INFO`` for both
``io.prestosql.server`` and ``io.prestosql.plugin.hive``.
The default minimum level is ``INFO``,
thus the above example does not actually change anything.
There are four levels: ``DEBUG``, ``INFO``, ``WARN`` and ``ERROR``.

Catalog Properties
^^^^^^^^^^^^^^^^^^

Presto accesses data via *connectors*, which are mounted in catalogs.
The connector provides all of the schemas and tables inside of the catalog.
For example, the Hive connector maps each Hive database to a schema.
If the Hive connector is mounted as the ``hive`` catalog, and Hive
contains a table ``clicks`` in database ``web``, that table can be accessed
in Presto as ``hive.web.clicks``.

Catalogs are registered by creating a catalog properties file
in the ``etc/catalog`` directory.
For example, create ``etc/catalog/jmx.properties`` with the following
contents to mount the ``jmx`` connector as the ``jmx`` catalog:

.. code-block:: none

    connector.name=jmx

See :doc:`/connector` for more information about configuring connectors.

.. _running_presto:

Running Presto
--------------

The installation directory contains the launcher script in ``bin/launcher``.
Presto can be started as a daemon by running the following:

.. code-block:: none

    bin/launcher start

Alternatively, it can be run in the foreground, with the logs and other
output written to stdout/stderr. Both streams should be captured
if using a supervision system like daemontools:

.. code-block:: none

    bin/launcher run

Run the launcher with ``--help`` to see the supported commands and
command line options. In particular, the ``--verbose`` option is
very useful for debugging the installation.

The launcher configures default values for the configuration
directory ``etc``, configuration files, the data directory ``var``,
and log files in the data directory. You can change these values
to adjust your Presto usage to any requirements, such as using a
directory outside the installation directory, specific mount points
or locations, and even using other file names. For example, the Presto
RPM adjusts the used directories to better follow the Linux Filesystem
Hierarchy Standard (FHS).

After starting Presto, you can find log files in the ``log`` directory inside
the data directory ``var``:

* ``launcher.log``:
  This log is created by the launcher and is connected to the stdout
  and stderr streams of the server. It contains a few log messages
  that occur while the server logging is being initialized, and any
  errors or diagnostics produced by the JVM.

* ``server.log``:
  This is the main log file used by Presto. It typically contains
  the relevant information if the server fails during initialization.
  It is automatically rotated and compressed.

* ``http-request.log``:
  This is the HTTP request log which contains every HTTP request
  received by the server. It is automatically rotated and compressed.
