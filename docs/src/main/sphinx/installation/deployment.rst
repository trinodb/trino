================
Deploying Trino
================

.. _requirements:

Requirements
------------

.. _requirements-linux:

Linux operating system
^^^^^^^^^^^^^^^^^^^^^^

* 64-bit required
* newer release preferred, especially when running on containers
* adequate ulimits for the user that runs the Trino process. These limits
  may depend on the specific Linux distribution you are using. The number
  of open file descriptors needed for a particular Trino instance scales
  as roughly the number of machines in the cluster, times some factor
  depending on the workload. We recommend the following limits, which can
  typically be set in ``/etc/security/limits.conf``:

  .. code-block:: text

      trino soft nofile 131072
      trino hard nofile 131072

.. _requirements-java:

Java runtime environment
^^^^^^^^^^^^^^^^^^^^^^^^

Trino requires a 64-bit version of Java 11, with a minimum required version of 11.0.11.
Earlier patch versions such as 11.0.2 do not work, nor will earlier major versions such as Java 8.
Newer major versions such as Java 12 or 13, including Java 17, are not supported -- they may work, but are not tested.

We recommend using `Azul Zulu <https://www.azul.com/downloads/zulu-community/>`_
as the JDK for Trino, as Trino is tested against that distribution.
Zulu is also the JDK used by the
`Trino Docker image <https://hub.docker.com/r/trinodb/trino>`_.

.. _requirements-python:

Python
^^^^^^

* version 2.6.x, 2.7.x, or 3.x
* required by the ``bin/launcher`` script only

Installing Trino
-----------------

Download the Trino server tarball, :maven_download:`server`, and unpack it.
The tarball contains a single top-level directory,
|trino_server_release|, which we call the *installation* directory.

Trino needs a *data* directory for storing logs, etc.
We recommend creating a data directory outside of the installation directory,
which allows it to be easily preserved when upgrading Trino.

Configuring Trino
------------------

Create an ``etc`` directory inside the installation directory.
This holds the following configuration:

* Node Properties: environmental configuration specific to each node
* JVM Config: command line options for the Java Virtual Machine
* Config Properties: configuration for the Trino server. See the
  :doc:`/admin/properties` for available configuration properties.
* Catalog Properties: configuration for :doc:`/connector` (data sources).
  The available catalog configuration properties for a connector are described
  in the respective connector documentation.

.. _node_properties:

Node properties
^^^^^^^^^^^^^^^

The node properties file, ``etc/node.properties``, contains configuration
specific to each node. A *node* is a single installed instance of Trino
on a machine. This file is typically created by the deployment system when
Trino is first installed. The following is a minimal ``etc/node.properties``:

.. code-block:: text

    node.environment=production
    node.id=ffffffff-ffff-ffff-ffff-ffffffffffff
    node.data-dir=/var/trino/data

The above properties are described below:

* ``node.environment``:
  The name of the environment. All Trino nodes in a cluster must have the same
  environment name. The name must start with a lowercase alphanumeric character
  and only contain lowercase alphanumeric or underscore (``_``) characters.

* ``node.id``:
  The unique identifier for this installation of Trino. This must be
  unique for every node. This identifier should remain consistent across
  reboots or upgrades of Trino. If running multiple installations of
  Trino on a single machine (i.e. multiple nodes on the same machine),
  each installation must have a unique identifier. The identifier must start
  with an alphanumeric character and only contain alphanumeric, ``-``, or ``_``
  characters.

* ``node.data-dir``:
  The location (filesystem path) of the data directory. Trino stores
  logs and other data here.

.. _jvm_config:

JVM config
^^^^^^^^^^

The JVM config file, ``etc/jvm.config``, contains a list of command line
options used for launching the Java Virtual Machine. The format of the file
is a list of options, one per line. These options are not interpreted by
the shell, so options containing spaces or other special characters should
not be quoted.

The following provides a good starting point for creating ``etc/jvm.config``:

.. code-block:: text

    -server
    -Xmx16G
    -XX:-UseBiasedLocking
    -XX:+UseG1GC
    -XX:G1HeapRegionSize=32M
    -XX:+ExplicitGCInvokesConcurrent
    -XX:+ExitOnOutOfMemoryError
    -XX:+HeapDumpOnOutOfMemoryError
    -XX:-OmitStackTraceInFastThrow
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
prevents Trino from starting. You can workaround this by overriding the
temporary directory by adding ``-Djava.io.tmpdir=/path/to/other/tmpdir`` to the
list of JVM options.

.. _config_properties:

Config properties
^^^^^^^^^^^^^^^^^

The config properties file, ``etc/config.properties``, contains the
configuration for the Trino server. Every Trino server can function
as both a coordinator and a worker, but dedicating a single machine
to only perform coordination work provides the best performance on
larger clusters.

The following is a minimal configuration for the coordinator:

.. code-block:: text

    coordinator=true
    node-scheduler.include-coordinator=false
    http-server.http.port=8080
    query.max-memory=50GB
    query.max-memory-per-node=1GB
    discovery.uri=http://example.net:8080

And this is a minimal configuration for the workers:

.. code-block:: text

    coordinator=false
    http-server.http.port=8080
    query.max-memory=50GB
    query.max-memory-per-node=1GB
    discovery.uri=http://example.net:8080

Alternatively, if you are setting up a single machine for testing, that
functions as both a coordinator and worker, use this configuration:

.. code-block:: text

    coordinator=true
    node-scheduler.include-coordinator=true
    http-server.http.port=8080
    query.max-memory=5GB
    query.max-memory-per-node=1GB
    discovery.uri=http://example.net:8080

These properties require some explanation:

* ``coordinator``:
  Allow this Trino instance to function as a coordinator, so to
  accept queries from clients and manage query execution.

* ``node-scheduler.include-coordinator``:
  Allow scheduling work on the coordinator.
  For larger clusters, processing work on the coordinator
  can impact query performance because the machine's resources are not
  available for the critical task of scheduling, managing and monitoring
  query execution.

* ``http-server.http.port``:
  Specifies the port for the HTTP server. Trino uses HTTP for all
  communication, internal and external.

* ``query.max-memory``:
  The maximum amount of distributed memory, that a query may use.

* ``query.max-memory-per-node``:
  The maximum amount of user memory, that a query may use on any one machine.

* ``discovery.uri``:
  The Trino coordinator has a discovery service that is used by all the nodes
  to find each other. Every Trino instance registers itself with the discovery
  service on startup and continuously heartbeats to keep its registration
  active. The discovery service shares the HTTP server with Trino and thus
  uses the same port. Replace ``example.net:8080`` to match the host and
  port of the Trino coordinator. If you have disabled HTTP on the coordinator,
  the URI scheme must be ``https``, not ``http``.

The above configuration properties are a minimal set to help you get started.
Please see :doc:`/admin` and :doc:`/security` for a more comprehensive list.
In particular, see :doc:`/admin/resource-groups` for configuring queuing policies.

.. _log-levels:

Log levels
^^^^^^^^^^

The optional log levels file, ``etc/log.properties``, allows setting the
minimum log level for named logger hierarchies. Every logger has a name,
which is typically the fully qualified name of the class that uses the logger.
Loggers have a hierarchy based on the dots in the name, like Java packages.
For example, consider the following log levels file:

.. code-block:: text

    io.trino=INFO

This would set the minimum level to ``INFO`` for both
``io.trino.server`` and ``io.trino.plugin.hive``.
The default minimum level is ``INFO``,
thus the above example does not actually change anything.
There are four levels: ``DEBUG``, ``INFO``, ``WARN`` and ``ERROR``.

.. _catalog_properties:

Catalog properties
^^^^^^^^^^^^^^^^^^

Trino accesses data via *connectors*, which are mounted in catalogs.
The connector provides all of the schemas and tables inside of the catalog.
For example, the Hive connector maps each Hive database to a schema.
If the Hive connector is mounted as the ``hive`` catalog, and Hive
contains a table ``clicks`` in database ``web``, that table can be accessed
in Trino as ``hive.web.clicks``.

Catalogs are registered by creating a catalog properties file
in the ``etc/catalog`` directory.
For example, create ``etc/catalog/jmx.properties`` with the following
contents to mount the ``jmx`` connector as the ``jmx`` catalog:

.. code-block:: text

    connector.name=jmx

See :doc:`/connector` for more information about configuring connectors.

.. _running_trino:

Running Trino
--------------

The installation directory contains the launcher script in ``bin/launcher``.
Trino can be started as a daemon by running the following:

.. code-block:: text

    bin/launcher start

Alternatively, it can be run in the foreground, with the logs and other
output written to stdout/stderr. Both streams should be captured
if using a supervision system like daemontools:

.. code-block:: text

    bin/launcher run

Run the launcher with ``--help`` to see the supported commands and
command line options. In particular, the ``--verbose`` option is
very useful for debugging the installation.

The launcher configures default values for the configuration
directory ``etc``, configuration files, the data directory ``var``,
and log files in the data directory. You can change these values
to adjust your Trino usage to any requirements, such as using a
directory outside the installation directory, specific mount points
or locations, and even using other file names.

After starting Trino, you can find log files in the ``log`` directory inside
the data directory ``var``:

* ``launcher.log``:
  This log is created by the launcher and is connected to the stdout
  and stderr streams of the server. It contains a few log messages
  that occur while the server logging is being initialized, and any
  errors or diagnostics produced by the JVM.

* ``server.log``:
  This is the main log file used by Trino. It typically contains
  the relevant information if the server fails during initialization.
  It is automatically rotated and compressed.

* ``http-request.log``:
  This is the HTTP request log which contains every HTTP request
  received by the server. It is automatically rotated and compressed.
