==========
Connectors
==========

Connectors are the source of all data for queries in Trino. Even if
your data source doesn't have underlying tables backing it, as long as
you adapt your data source to the API expected by Trino, you can write
queries against this data.

ConnectorFactory
----------------

Instances of your connector are created by a ``ConnectorFactory``
instance which is created when Trino calls ``getConnectorFactory()`` on the
plugin. The connector factory is a simple interface responsible for creating an
instance of a ``Connector`` object that returns instances of the
following services:

* ``ConnectorMetadata``
* ``ConnectorSplitManager``
* ``ConnectorHandleResolver``
* ``ConnectorRecordSetProvider``

ConnectorMetadata
^^^^^^^^^^^^^^^^^

The connector metadata interface has a large number of important
methods that are responsible for allowing Trino to look at lists of
schemas, lists of tables, lists of columns, and other metadata about a
particular data source.

This interface is too big to list in this documentation, but if you
are interested in seeing strategies for implementing these methods,
look at the :doc:`example-http` and the Cassandra connector. If
your underlying data source supports schemas, tables and columns, this
interface should be straightforward to implement. If you are attempting
to adapt something that is not a relational database (as the Example HTTP
connector does), you may need to get creative about how you map your
data source to Trino's schema, table, and column concepts.

ConnectorSplitManager
^^^^^^^^^^^^^^^^^^^^^

The split manager partitions the data for a table into the individual
chunks that Trino will distribute to workers for processing.
For example, the Hive connector lists the files for each Hive
partition and creates one or more split per file.
For data sources that don't have partitioned data, a good strategy
here is to simply return a single split for the entire table. This
is the strategy employed by the Example HTTP connector.

ConnectorRecordSetProvider
^^^^^^^^^^^^^^^^^^^^^^^^^^

Given a split and a list of columns, the record set provider is
responsible for delivering data to the Trino execution engine.
It creates a ``RecordSet``, which in turn creates a ``RecordCursor``
that is used by Trino to read the column values for each row.
