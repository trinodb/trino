===========================
Hive connector with Alluxio
===========================

The :doc:`hive` can read and write tables stored in the `Alluxio Data Orchestration
System <https://www.alluxio.io/?utm_source=trino&utm_medium=trinodocs>`_,
leveraging Alluxio's distributed block-level read/write caching functionality.
The tables must be created in the Hive metastore with the ``alluxio://``
location prefix (see `Running Apache Hive with Alluxio
<https://docs.alluxio.io/os/user/2.1/en/compute/Hive.html?utm_source=trino&utm_medium=trinodocs>`_
for details and examples).

Trino queries will then transparently retrieve and cache files or objects from
a variety of disparate storage systems including HDFS and S3.

Alluxio client-side configuration
---------------------------------

To configure Alluxio client-side properties on Trino, append the Alluxio
configuration directory (``${ALLUXIO_HOME}/conf``) to the Trino JVM classpath,
so that the Alluxio properties file ``alluxio-site.properties`` can be loaded as
a resource. Update the Trino :ref:`jvm-config` file ``etc/jvm.config``
to include the following:

.. code-block:: text

  -Xbootclasspath/a:<path-to-alluxio-conf>

The advantage of this approach is that all the Alluxio properties are set in
the single ``alluxio-site.properties`` file. For details, see `Customize Alluxio Presto Properties
<https://docs.alluxio.io/os/user/2.1/en/compute/Presto.html#customize-alluxio-user-properties?utm_source=trino&utm_medium=trinodocs>`_.

Alternatively, add Alluxio configuration properties to the Hadoop configuration
files (``core-site.xml``, ``hdfs-site.xml``) and configure the Hive connector
to use the `Hadoop configuration files <#hdfs-configuration>`__ via the
``hive.config.resources`` connector property.

Deploy Alluxio with Trino
--------------------------

To achieve the best performance running Trino on Alluxio, it is recommended
to collocate Trino workers with Alluxio workers. This allows reads and writes
to bypass the network (*short-circuit*). See `Performance Tuning Tips for Presto with Alluxio
<https://www.alluxio.io/blog/top-5-performance-tuning-tips-for-running-presto-on-alluxio-1/?utm_source=trino&utm_medium=trinodocs>`_
for more details.

.. _alluxio-catalog-service:

Alluxio catalog service
-----------------------

An alternative way for Trino to interact with Alluxio is via the
`Alluxio catalog service <https://docs.alluxio.io/os/user/stable/en/core-services/Catalog.html?utm_source=trino&utm_medium=trinodocs>`_.
The primary benefits for using the Alluxio catalog service are simpler
deployment of Alluxio with Trino, and enabling schema-aware optimizations
such as transparent caching and transformations. Currently, the catalog service
supports read-only workloads.

The Alluxio catalog service is a metastore that can cache the information
from different underlying metastores. It currently supports the Hive metastore
as an underlying metastore. In order for the Alluxio catalog to manage the metadata
of other existing metastores, the other metastores must be "attached" to the
Alluxio catalog. To attach an existing Hive metastore to the Alluxio
catalog, simply use the
`Alluxio CLI attachdb command <https://docs.alluxio.io/os/user/stable/en/operation/User-CLI.html?utm_source=trino&utm_medium=trinodocs#attachdb>`_.
The appropriate Hive metastore location and Hive database name need to be
provided.

.. code-block:: text

    ./bin/alluxio table attachdb hive thrift://HOSTNAME:9083 hive_db_name

Once a metastore is attached, the Alluxio catalog can manage and serve the
information to Trino. To configure the Hive connector for Alluxio
catalog service, simply configure the connector to use the Alluxio
metastore type, and provide the location to the Alluxio cluster.
For example, your ``etc/catalog/alluxio.properties`` should include
the following:

.. code-block:: text

    connector.name=hive
    hive.metastore=alluxio-deprecated
    hive.metastore.alluxio.master.address=HOSTNAME:PORT

Replace ``HOSTNAME`` with the Alluxio master hostname, and replace ``PORT``
with the Alluxio master port.
An example of an Alluxio master address is ``master-node:19998``.
Now, Trino queries can take advantage of the Alluxio catalog service, such as
transparent caching and transparent transformations, without any modifications
to existing Hive metastore deployments.
