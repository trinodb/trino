=================
Phoenix Connector
=================

The Phoenix connector allows querying data stored in
`Apache HBase <https://hbase.apache.org/>`_ using
`Apache Phoenix <https://phoenix.apache.org/>`_.

Compatibility
-------------

The Phoenix connector is compatible with all Phoenix versions starting from 4.14.1.

Configuration
-------------

To configure the Phoenix connector, create a catalog properties file
``etc/catalog/phoenix.properties`` with the following contents,
replacing ``host1,host2,host3`` with a comma-separated list of the ZooKeeper
nodes used for discovery of the HBase cluster:

.. code-block:: none

    connector.name=phoenix
    phoenix.connection-url=jdbc:phoenix:host1,host2,host3:2181:/hbase
    phoenix.config.resources=/path/to/hbase-site.xml

The optional paths to Hadoop resource files, such as ``hbase-site.xml`` are used
to load custom Phoenix client connection properties.

Configuration Properties
------------------------

The following configuration properties are available:

================================================== ========== ===================================================================================
Property Name                                      Required   Description
================================================== ========== ===================================================================================
``phoenix.connection-url``                         Yes        ``jdbc:phoenix[:zk_quorum][:zk_port][:zk_hbase_path]``.
                                                              The ``zk_quorum`` is a comma separated list of ZooKeeper servers.
                                                              The ``zk_port`` is the ZooKeeper port. The ``zk_hbase_path`` is the HBase
                                                              root znode path, that is configurable using ``hbase-site.xml``.  By
                                                              default the location is ``/hbase``
``phoenix.config.resources``                       No         Comma-separated list of configuration files (e.g. ``hbase-site.xml``) to use for
                                                              connection properties.  These files must exist on the machines running Presto.
================================================== ========== ===================================================================================

Querying Phoenix Tables
-------------------------

The default empty schema in Phoenix maps to a schema named ``default`` in Presto.
You can see the available Phoenix schemas by running ``SHOW SCHEMAS``::

    SHOW SCHEMAS FROM phoenix;

If you have a Phoenix schema named ``web``, you can view the tables
in this schema by running ``SHOW TABLES``::

    SHOW TABLES FROM phoenix.web;

You can see a list of the columns in the ``clicks`` table in the ``web`` schema
using either of the following::

    DESCRIBE phoenix.web.clicks;
    SHOW COLUMNS FROM phoenix.web.clicks;

Finally, you can access the ``clicks`` table in the ``web`` schema::

    SELECT * FROM phoenix.web.clicks;

If you used a different name for your catalog properties file, use
that catalog name instead of ``phoenix`` in the above examples.

Data types
----------

The data type mappings are as follows:

==========================   ============
Phoenix                      Presto
==========================   ============
``BOOLEAN``                  (same)
``BIGINT``                   (same)
``INTEGER``                  (same)
``SMALLINT``                 (same)
``TINYINT``                  (same)
``DOUBLE``                   (same)
``FLOAT``                    ``REAL``
``DECIMAL``                  (same)
``BINARY``                   ``VARBINARY``
``VARBINARY``                (same)
``DATE``                     (same)
``TIME``                     (same)
``VARCHAR``                  (same)
``CHAR``                     (same)
==========================   ============

The Phoenix fixed length ``BINARY`` data type is mapped to the Presto
variable length ``VARBINARY`` data type. There is no way to create a
Phoenix table in Presto that uses the ``BINARY`` data type, as Presto
does not have an equivalent type.


Table Properties - Phoenix
--------------------------

Table property usage example::

    CREATE TABLE myschema.scientists (
      recordkey VARCHAR,
      birthday DATE,
      name VARCHAR,
      age BIGINT
    )
    WITH (
      rowkeys = 'recordkey,birthday',
      salt_buckets = 10
    );

The following are supported Phoenix table properties from `<https://phoenix.apache.org/language/index.html#options>`_

=========================== ================ ==============================================================================================================
Property Name               Default Value    Description
=========================== ================ ==============================================================================================================
``rowkeys``                 ``ROWKEY``       Comma-separated list of primary key columns.  See further description below

``split_on``                (none)           List of keys to presplit the table on.
                                             See `Split Point <https://phoenix.apache.org/language/index.html#split_point>`_.

``salt_buckets``            (none)           Number of salt buckets for this table.

``disable_wal``             false            Whether to disable WAL writes in HBase for this table.

``immutable_rows``          false            Declares whether this table has rows which are write-once, append-only.

``default_column_family``   ``0``            Default column family name to use for this table.
=========================== ================ ==============================================================================================================

``rowkeys``
^^^^^^^^^^^
This is a comma-separated list of columns to be used as the table's primary key. If not specified, a ``BIGINT`` primary key column named ``ROWKEY`` is generated
, as well as a sequence with the same name as the table suffixed with ``_seq`` (i.e. ``<schema>.<table>_seq``)
, which is used to automatically populate the ``ROWKEY`` for each row during insertion.

Table Properties - HBase
------------------------
The following are the supported HBase table properties that are passed through by Phoenix during table creation.
Use them in the the same way as above: in the ``WITH`` clause of the ``CREATE TABLE`` statement.

=========================== ================ ==============================================================================================================
Property Name               Default Value    Description
=========================== ================ ==============================================================================================================
``versions``                ``1``            The maximum number of versions of each cell to keep.

``min_versions``            ``0``            The minimum number of cell versions to keep.

``compression``             ``NONE``         Compression algorithm to use.  Valid values are ``NONE`` (default), ``SNAPPY``, ``LZO``, ``LZ4``, or ``GZ``.

``data_block_encoding``     ``FAST_DIFF``    Block encoding algorithm to use. Valid values are: ``NONE``, ``PREFIX``, ``DIFF``, ``FAST_DIFF`` (default), or ``ROW_INDEX_V1``.

``ttl``                     ``FOREVER``      Time To Live for each cell.

``bloomfilter``             ``NONE``         Bloomfilter to use. Valid values are ``NONE`` (default), ``ROW``, or ``ROWCOL``.
=========================== ================ ==============================================================================================================

