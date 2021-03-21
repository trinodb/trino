====================
Supporting ``MERGE``
====================

The Trino engine provides APIs to support row-level SQL ``MERGE``.
To implement ``MERGE``, a connector must provide an implementation
of ``ConnectorMergeSink``, which is typically layered on top of a
``ConnectorPageSink``, and define ``ConnectorMetadata``
methods to get a "rowId" column handle; get the row change paradigm;
and to start and complete the ``MERGE`` operation.

Standard SQL ``MERGE``
----------------------

Different query engines support varying definitions of SQL ``MERGE``.
Trino supports the strict SQL specification ``ISO/IEC 9075``, published
in 2016.  As a simple example, given tables ``target_table`` and
``source_table`` defined as::

    CREATE TABLE target_table (
        customer VARCHAR,
        purchases DECIMAL,
        address VARCHAR);
    INSERT INTO target_table (customer, purchases, address) VALUES ...;
    CREATE TABLE source_table (
        customer VARCHAR,
        purchases DECIMAL,
        address VARCHAR);
    INSERT INTO source_table (customer, purchases, address) VALUES ...;

Here is a possible ``MERGE`` operation, from ``source_table`` to
``target_table``::

    MERGE INTO target_table t USING source_table s
        ON (t.customer = s.customer)
        WHEN MATCHED AND s.address = 'Berkeley' THEN
            DELETE
        WHEN MATCHED AND s.customer = 'Joe Shmoe' THEN
            UPDATE SET purchases = purchases + 100.0
        WHEN MATCHED THEN
            UPDATE
                SET purchases = s.purchases + t.purchases, address = s.address
        WHEN NOT MATCHED THEN
            INSERT (customer, purchases, address)
                VALUES (s.customer, s.purchases, s.address);

SQL ``MERGE`` supports two distinct operations on the target table and source
when a row from the source query matches a row in the target table:

* ``UPDATE``, in which the columns in the target row are updated.
* ``DELETE``, in which the target row is deleted.

In the ``NOT MATCHED`` case, SQL ``MERGE`` supports only ``INSERT`` of the
unmatched row from the source query into the target table.

``RowChangeParadigm``
---------------------

Different connectors have different ways of representing row updates,
imposed by the underlying storage systems.  The Trino engine classifies
these different paradigms as elements of the ``RowChangeParadigm``
enumeration, returned by ``ConnectorMetadata`` method
``getRowChangeParadigm(...)``.

The ``RowChangeParadigm`` enumeration values are:

* ``CHANGE_ONLY_UPDATED_COLUMNS``, intended for connectors like
  ``trino-jdbc`` and ``trino-kudu`` that can update individual columns of
  rows identified by a ``rowId``.  The corresponding merge processor class is
  ``ChangeOnlyUpdatedColumnsMergeProcessor``.
* ``DELETE_ROW_AND_INSERT_ROW``, intended for connectors like ``trino-hive``
  and ``trino-iceberg`` that represent a row change as a row deletion paired
  with a row insertion.  The corresponding merge processor class is
  ``DeleteAndInsertMergeProcessor``.
* ``COPY_FILE_ON_CHANGE``, intended for connectors that must copy an entire
  file to update one or more rows in the file.

Overview of ``MERGE`` processing
--------------------------------

A ``MERGE`` statement is processed by creating a ``RIGHT JOIN`` between the
target table and the source query, on the ``MERGE`` criteria, returning a
``ROW`` containing the column values from the ``UPDATE`` or ``INSERT``
cases, and ``NULL`` for the ``DELETE`` cases; an integer identifying the
``MERGE`` case matched; and whether the merge case operation is
``UPDATE``, ``DELETE`` or ``INSERT``.  The example above is executed as
if it were written as::

   SELECT
    FROM (SELECT *, true AS present FROM target_table) t
    RIGHT JOIN source_table s ON s.customer = t.customer
    CASE
      WHEN present AND s.address = 'Berkeley' THEN
          // Null values for delete; case #0; operation DELETE=2
          row(null, null, null, 0, 2)
      WHEN present AND s.customer = 'Joe Shmoe' THEN
          // Update column values; case #1; operation UPDATE=3
          row(null, t.purchases + 100.0, null, 1, 3)
      WHEN present THEN
          // Update column values; case #2; operation UPDATE=3
          row(null, s.purchases + t.purchases, s.address, 2, 3)
      WHEN (NOT present) THEN
          // Insert column values; case #3; operation INSERT=1
          row(s.customer, s.purchases, s.address, 4, 1)
      ELSE
          // Null values for no case matched; case #-1; operation=-1
          row(null, null, null, -1, -1)
    END;

The Trino engine executes the ``RIGHT JOIN`` and ``SELECT CASE``,
creating a sequence of pages to be routed to the node that runs the
``ConnectorMergeSink`` ``storeMergedRows`` method.  To ensure that all
``NOT MATCHED`` rows for a given partition end up on a single node, a
redistribution hash on the partition key/bucket column(s) is applied
to the page partition key(s).  As a result of the hash, all rows for a
specific partition/bucket will hash together, whether they were
``MATCHED`` rows or ``NOT MATCHED`` rows.

Like ``DELETE`` and ``UPDATE``, ``MERGE`` target table rows are identified by
a connector-specific ``rowId`` column handle, returned by
``ConnectorMetadata.getMergeRowIdColumnHandle(...)``.  For ``MERGE``,
that ``rowId`` column contains whatever the connector needs in order
to process all the possible ``MERGE`` cases for that row.  For example,
in the case of the Hive connector, the merge handle contains the
``InsertTableHandle`` for the target table.

Representation of ``MERGE`` cases
---------------------------------

The Trino engine provide a ``MergeDetails`` instance to describe the ``MERGE``
cases to the connector.  ``MergeDetails`` contains a list of
``MergeCaseDetails`` instances.  ``MergeCaseDetails`` has these members:

* The ``int`` ``caseNumber``, starting from 0, in syntactic order.
* The ``MergeCaseKind`` ``caseKind``: One of ``UPDATE``, ``DELETE`` or
  ``INSERT``.
* The ``Set<String>`` ``updatedColumns``: The columns updated by the case.
  For an ``INSERT``, all data columns targeted in the insert are included.

``MERGE`` Redistribution
------------------------

The Trino ``MERGE`` implementation allows ``UPDATE`` and ``INSERT`` to change
the values of columns that determine partitioning and/or bucketing, and so
the Trino engine must "redistribute" rows from the ``MERGE`` operation to
the worker nodes responsible for writing rows with the merged partitioning
and/or bucketing columns.

Connector support for ``MERGE``
===============================

To start start ``MERGE`` processing, the Trino engine calls:

*  ``ConnectorMetadata.getMergeRowIdColumnHandle(...)`` to get the
   ``rowId`` column handle.
* ``ConnectorMetadata.getRowChangeParadigm(...)`` to get the paradigm
  supported by the connectoor for changing existing table rows.
* ``ConnectorMetadata.beginMerge(...)`` to get the a
  ``ConnectorMergeTableHandle`` for the merge operation.  That
  ``ConnectorMergeTableHandle`` object contains whatever information the
  connector needs to specify the ``MERGE`` operation.
* ``ConnectorMetadata.getWriteRedistributionColumns(...)`` to get the list
  of partition or table columns that impact write redistribution.

On nodes that are targets of the hash, the Trino engine calls
``ConnectorPageSinkProvider.createMergeSink(...)`` to create a
``ConnectorMergeSink``.

To write out each page of merged rows, the Trino engine calls
``ConnectorMergeSink.storeMergedRows(Page)``.  The ``storeMergedRows(Page)``
method iterates over the rows in the page, performing updates and deletes
in the ``MATCHED`` cases, and inserts in the ``NOT MATCHED`` cases.

For some ``RowChangeParadigms``, ``UPDATE`` operations may have been
translated into the corresponding ``DELETE`` and ``INSERT`` operations
before ``storeMergedRows(Page)`` was called.

To complete the ``MERGE`` operation, the Trino engine calls
``ConnectorMetadata.finishMerge(...)``, passing the table
and that collection of ``Slice`` fragments with information about what
was changed, and the connector takes appropriate actions, if
any.

``RowChangeProcessor`` implementation for ``MERGE``
---------------------------------------------------

In SQL ``MERGE``, each supported ``RowChangeParadigm``
corresponds to an internal Trino engine class that implements interface
``RowChangeProcessor``.  ``RowChangeProcessor`` has one interesting method:
``Page transformPage(Page)``.  The format of the input and output page depend
on the the SQL operation being implemented.

The connector has no access to the ``RowChangeProcessor`` instance - - it
is used inside the Trino engine to transform the merge page rows into rows
to be stored, based on the connector's choice of ``RowChangeParadigm``.

For SQL MERGE, the page supplied to ``transformPage()`` consists of:

* The write redistribution columns if any
* The ``rowId`` column for the row from the target table if matched, or
  null if not matched
* The merge case ``RowBlock``
* For partitioned or bucketed tables, a hash value column.

The merge case ``RowBlock`` has this layout:

* Blocks for each column in the table, including partition columns, in
  table column order.
* A block containing the merge case number of the matching merge case for
  the row starting at 0, or -1 no merge case matched for the row.
* A block containing the merge case operation, encoded as ``INSERT`` = 1,
  ``DELETE`` = 2, ``UPDATE`` = 3 and if no merge case matched, -1.

The page returned from ``transformPage`` consists of all table columns,
in table column order, followed by the rowId column, followed by the
operation column from the merge case ``RowBlock``.

Interface ``RowChangeProcessor`` now supports SQL MERGE, and will later be
used to upgrade the SQL UPDATE engine implementation.

Detecting duplicate matching target rows
----------------------------------------

The SQL ``MERGE`` specification requires that in each ``MERGE`` case,
a single target table row must match at most one source row, after
applying the ``MERGE`` case condition expression.  This is done in
class ``DuplicateRowFinder.checkForDuplicateTargetRows()``.  That method
examines successive rows produced by the ``SELECT CASE`` expression,
excluding ``INSERT``ed rows, and if the ``writeRedistributionColumns``
and the ``rowId`` column are the identical in successive rows.
If so, it raises the ``MERGE_TARGET_ROW_MULTIPLE_MATCHES``
exception.

``ConnectorMergeTableHandle`` API
---------------------------------

Interface ``ConnectorMergeTableHandle`` defines one method,
``getTableHandle()`` to retrieve the ``ConnectorTableHandle``
returned originally created by ``ConnectorMetadata.beginMerge()``.

``ConnectorPageSinkProvider`` API
---------------------------------

To support ``SQL MERGE``, ``ConnectorPageSinkProvider`` must implement
the method that creates the ``ConnectorMergeSink``:

* ``createMergeSink``::

    ConnectorMergeSink createMergeSink(
        ConnectorTransactionHandle transactionHandle,
        ConnectorSession session,
        ConnectorMergeTableHandle mergeHandle)

  Create a ``ConnectorMergeSink`` for the ``transactionHandle``,
  ``session`` and ``mergeHandle``
  ``Session`` and

``ConnectorMergeSink`` API
--------------------------

As mentioned above, to support ``MERGE``, the connector must define an
implementation of ``ConnectorMergeSink``, usually layered over the
connector's ``ConnectorPageSink``.

The ``ConnectorMergeSink`` is created by a call to
``ConnectorPageSinkProvider.createMergeSink()``.

The only interesting methods are:

* ``storeMergedRows``::

    void storeMergedRows(Page page)

  The Trino engine calls the
  ``storeMergedRows(Page)`` method of the ``ConnectorMergeSink``
  instance returned by ``ConnectorPageSinkProvider.createMergeSink()``,
  passing the page generated by the ``RowChangeProcessor.transformPage()
  method.  That page consists of all table columns, in table column
  order, followed by the rowId column, followed by the operation column
  from the merge case ``RowBlock``.

  The job of ``storeMergedRows()`` is iterate over the rows in the page,
  and based on the value of the operation column, ``INSERT``, ``DELETE``,
  ``UPDATE``, or ignore the page row.  For some connnectors, the ``UPDATE``
  operations is transformed into ``DELETE`` and ``INSERT`` operations.

* ``finish``::

    ``CompletableFuture<Collection<Slice>> finish()``

  The Trino engine
  calls ``finish()`` when all the data has been processed by a specific
  ``ConnectorMergeSink`` instance.  The connector returns a future containing
  a collection of ``Slice``, representing connector-specific information
  about the rows processed.  Usually this includes the row count, and
  might include information like the files or partitions created or changed.

``ConnectorMetadata`` ``MERGE`` API
-----------------------------------

A connector implementing ``MERGE`` must implement these ``ConnectorMetadata``
methods.

* ``getRowChangeParadigm()``::

    RowChangeParadigm getRowChangeParadigm(
        ConnectorSession session,
        ConnectorTableHandle tableHandle)

  This method is called as the engine starts processing a ``MERGE`` statement.
  The connector must return a ``RowChangeParadigm`` enum instance.  If the
  connector does not support ``MERGE`` it should throw a ``NOT_SUPPORTED, meaning
  that ``SQL MERGE`` is not supported by the connector.

* ``getMergeRowIdColumnHandle()``::

    ColumnHandle getMergeRowIdColumnHandle(
        ConnectorSession session,
        ConnectorTableHandle tableHandle,
        MergeDetails mergeDetails)

  This method is called in the early stages of query planning for ``MERGE``
  statements.  The ColumnHandle returned provides the ``rowId`` used by the
  connector to identify rows to be merged, as well as any other fields of
  the row that the connector needs to complete the ``MERGE`` operation.

* ``getWriteRedistributionColumns()``::

    List<ColumnHandle> getWriteRedistributionColumns(
        ConnectorSession session,
        ConnectorTableHandle table)

  This method returns a list of ``ColumnHandles`` for table columns that
  impact write redistribution, e.g., columns that impact partitioning or
  bucketing.  By default, this method returns an empty list.

* ``beginMerge()``::

    ConnectorMergeTableHandle beginDelete(
         ConnectorSession session,
         ConnectorTableHandle tableHandle,
         MergeDetails mergeDetails)

  As the last step in creating the ``MERGE`` execution plan, the connector's
  ``beginMerge()`` method is called, passing the ``session``, the
  ``tableHandle`` and the ``MergeDetails`` object.

  ``beginMerge()`` performs any orchestration needed in the connector to
  start processing the ``MERGE``.  This orchestration varies from connector
  to connector.  In the Hive ACID connector, for example, ``beginMerge()``
  checks that the table is transactional and that all updated columns are
  writable, and starts a Hive Metastore transaction.

  ``beginMerge()`` returns a ``ConnectorMergeTableHandle`` with any added
  information the connector needs when the handle is passed back to
  ``finishMerge()`` and the split generation machinery.  For most
  connectors, the returned table handle contains at least a flag identifying
  the table handle as a table handle for a ``MERGE`` operation.

* ``finishMerge()``::

      void finishMerge(
          ConnectorSession session,
          ConnectorMergeTableHandle tableHandle,
          Collection<Slice> fragments)

  During ``MERGE`` processing, the Trino engine accumulates the ``Slice``
  collections returned by ``ConnectorMergeSink.finish()``.  The engine calls
  ``finishMerge()``, passing the table handle and that collection of
  ``Slice`` fragments.  In response, the connector takes appropriate actions
  to complete the ``MERGE`` operation.  Those actions might include
  committing the transaction, assuming the connector supports a transaction
  paradigm.
