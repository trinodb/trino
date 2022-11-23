====================================
Supporting ``DELETE`` and ``UPDATE``
====================================

The Trino engine provides APIs to support row-level SQL ``DELETE`` and ``UPDATE``.
To implement ``DELETE`` or ``UPDATE``, a connector must layer an ``UpdatablePageSource``
on top of the connector's usual ``ConnectorPageSource``, and define ``ConnectorMetadata``
methods to get a "rowId" column handle; to start the operation; and to finish the operation.

``DELETE`` and ``UPDATE`` Data Flow
===================================

``DELETE`` and ``UPDATE`` have a similar flow:

* For each split, the connector will create an ``UpdatablePageSource`` instance, layered over the
  connector's ``ConnectorPageSource``, to read pages on behalf of the Trino engine, and to
  write deletions and/or updates to the underlying data store.
* The connector's ``UpdatablePageSource.getNextPage()`` implementation fetches the next page
  from the underlying ``ConnectorPageSource``, optionally rebuild the page, and returns it
  to the Trino engine.
* The Trino engine performs filtering and projection on the page read, producing a page of filtered,
  projected results.
* The Trino engine passes that filtered, projected page of results to the connector's
  ``UpdatablePageSource`` ``deleteRows()`` or ``updateRows()`` method. Those methods persist
  the deletions or updates in the underlying data store.
* When all the pages for a specific split have been processed, the Trino engine calls
  ``UpdatablePageSource.finish()``, which returns a ``Collection<Slice>`` of fragments
  representing connector-specific information about the rows processed by the calls to
  ``deleteRows`` or ``updateRows``.
* When all pages for all splits have been processed, the Trino engine calls ``ConnectorMetadata.finishDelete()`` or
  ``finishUpdate``, passing a collection containing all the fragments from all the splits. The connector
  does what is required to finalize the operation, for example, committing the transaction.

The rowId Column Abstraction
============================

The Trino engine and connectors use a "rowId" column handle abstraction to agree on the identities of rows
to be updated or deleted. The rowId column handle is opaque to the Trino engine. Depending on the connector,
the rowId column handle abstraction could represent several physical columns. For the JDBC connector, the rowId
column handle points might be the primary key for the table. For deletion in Hive ACID tables, the rowId consists
of the three ACID columns that uniquely identify rows.

The rowId Column for ``DELETE``
-------------------------------

The Trino engine identifies the rows to be deleted using a connector-specific
rowId column handle, returned by the connector's ``ConnectorMetadata.getDeleteRowIdColumnHandle()``
method, whose full signature is::

    ColumnHandle getDeleteRowIdColumnHandle(
        ConnectorSession session,
        ConnectorTableHandle tableHandle)

The rowId Column for ``UPDATE``
-------------------------------

The Trino engine identifies rows to be updated using a connector-specific rowId column handle,
returned by the connector's ``ConnectorMetadata.getUpdateRowIdColumnHandle()``
method. In addition to the columns that identify the row, for ``UPDATE`` the rowId column will contain
any columns that the connector requires in order to perform the ``UPDATE`` operation. In Hive ACID, for example,
the rowId column contains the values of all columns *not* updated by the ``UPDATE`` operation, since Hive ACID
implements ``UPDATE`` as a ``DELETE`` paired with an INSERT.

UpdatablePageSource API
=======================

As mentioned above, to support ``DELETE`` or ``UPDATE``, the connector must define a subclass of
``UpdatablePageSource``, layered over the connector's usual ``ConnectorPageSource``. The interesting methods are:

* ``Page getNextPage()``. When the Trino engine calls ``getNextPage()``, the ``UpdatablePageSource`` calls
  its underlying ``ConnectorPageSource.getNextPage()`` method to get a page. Some connectors will rebuild
  the page before returning it to the Trino engine.

* ``void deleteRows(Block rowIds)``. The Trino engine calls the ``deleteRows()`` method of the same ``UpdatablePageSource``
  instance that supplied the original page, passing a block of ``rowIds``, created by the Trino engine based on the column
  handle returned by ``ConnectorMetadata.getDeleteRowIdColumnHandle()``

* ``void updateRows(Page page, List<Integer> columnValueAndRowIdChannels)``. The Trino engine calls the ``updateRows()``
  method of the same ``UpdatablePageSource`` instance that supplied the original page, passing a page of projected columns,
  one for each updated column and the last one for the rowId column. The order of projected columns is defined by the Trino engine,
  and that order is reflected in the ``columnValueAndRowIdChannels`` argument. The job of ``updateRows()`` is to:

  * Extract the updated column blocks and the rowId block from the projected page.
  * Assemble them in whatever order is required by the connector for storage.
  * Store the update result in the underlying file store.

  In the case of Hive ACID, ``updateRows()`` stores a file of records that delete the
  previous contents of the updated rows, and a separate file that inserts completely
  new rows containing the updated and non-updated column values.

* ``CompletableFuture<Collection<Slice>> finish()``. The Trino engine calls ``finish()`` when all the pages
  of a split have been processed. The connector returns a future containing a collection of ``Slice``, representing
  connector-specific information about the rows processed. Usually this will include the row count, and might
  include information like the files or partitions created or changed.

``ConnectorMetadata`` ``DELETE`` API
====================================

A connector implementing ``DELETE`` must specify three ``ConnectorMetadata`` methods.

* ``getDeleteRowIdColumnHandle()``::

   ColumnHandle getDeleteRowIdColumnHandle(
        ConnectorSession session,
        ConnectorTableHandle tableHandle)

  The ColumnHandle returned by this method provides the "rowId" used by the connector to identify rows to be deleted, as
  well as any other fields of the row that the connector will need to complete the ``DELETE`` operation.
  For a JDBC connector, that rowId is usually the primary key for the table and no other fields are required.
  For other connectors, the information needed to identify a row usually consists of multiple physical columns.
  With the Hive connector and a Hive ACID table, for example, the ``DELETE`` rowId consists of the three ORC ACID columns
  that identify the row.

* ``beginDelete()``::

    ConnectorTableHandle beginDelete(
         ConnectorSession session,
         ConnectorTableHandle tableHandle)

  As the last step in creating the ``DELETE`` execution plan, the connector's ``beginDelete()`` method is called,
  passing the ``session`` and ``tableHandle``.

  ``beginDelete()`` performs any orchestration needed in the connector to start processing the ``DELETE``.
  This orchestration varies from connector to connector. In the Hive ACID connector, for example, ``beginDelete()``
  checks that the table is transactional and starts a Hive Metastore transaction.

  ``beginDelete()`` returns a ``ConnectorTableHandle`` with any added information the connector needs when the handle
  is passed back to ``finishDelete()`` and the split generation machinery. For most connectors, the returned table
  handle contains a flag identifying the table handle as a table handle for a ``DELETE`` operation.

* ``finishDelete()``::

      void finishDelete(
          ConnectorSession session,
          ConnectorTableHandle tableHandle,
          Collection<Slice> fragments)

  During ``DELETE`` processing, the Trino engine accumulates the ``Slice`` collections returned by ``UpdatablePageSource.finish()``.
  After all splits have been processed, the engine calls ``finishDelete()``, passing the table handle and that
  collection of ``Slice`` fragments. In response, the connector takes appropriate actions to complete the ``Delete`` operation.
  Those actions might include committing the transaction, assuming the connector supports a transaction paradigm.

``ConnectorMetadata`` ``UPDATE`` API
====================================

A connector implementing ``UPDATE`` must specify three ``ConnectorMetadata`` methods.

* ``getUpdateRowIdColumnHandle``::

   ColumnHandle getUpdateRowIdColumnHandle(
        ConnectorSession session,
        ConnectorTableHandle tableHandle,
        List<ColumnHandle> updatedColumns)

  The ``updatedColumns`` list contains column handles for all columns updated by the ``UPDATE`` operation in table column order.

  The ColumnHandle returned by this method provides the "rowId" used by the connector to identify rows to be updated, as
  well as any other fields of the row that the connector will need to complete the ``UPDATE`` operation.
  For a JDBC connector, that rowId is usually the primary key for the table and no other fields are required.
  For other connectors, the information needed to identify a row usually consists of multiple physical columns.
  Moreover, some connectors may need the values of columns that are not updated to complete the ``UPDATE`` operation.
  With the Hive connector and a Hive ACID table, for example, the ``UPDATE`` rowId consists of the three ORC ACID columns
  that identify the row, plus the values of all the data columns not updated.

* ``beginUpdate``::

    ConnectorTableHandle beginUpdate(
         ConnectorSession session,
         ConnectorTableHandle tableHandle,
         List<ColumnHandle> updatedColumns)

  As the last step in creating the ``UPDATE`` execution plan, the connector's ``beginUpdate()`` method is called,
  passing arguments that define the ``UPDATE`` to the connector. In addition to the ``session``
  and ``tableHandle``, the arguments includes the list of the updated columns handles, in table column order.

  ``beginUpdate()`` performs any orchestration needed in the connector to start processing the ``UPDATE``.
  This orchestration varies from connector to connector. In the Hive ACID connector, for example, ``beginUpdate()``
  starts the Hive Metastore transaction; checks that the updated table is transactional and that neither
  partition columns nor bucket columns are updated.

  ``beginUpdate`` returns a ``ConnectorTableHandle`` with any added information the connector needs when the handle
  is passed back to ``finishUpdate()`` and the split generation machinery. For most connectors, the returned table
  handle contains a flag identifying the table handle as a table handle for a ``UPDATE`` operation. For some connectors
  that support partitioning, the table handle will reflect that partitioning.

* ``finishUpdate``::

      void finishUpdate(
          ConnectorSession session,
          ConnectorTableHandle tableHandle,
          Collection<Slice> fragments)

  During ``UPDATE`` processing, the Trino engine accumulates the ``Slice`` collections returned by ``UpdatablePageSource.finish()``.
  After all splits have been processed, the engine calls ``finishUpdate()``, passing the table handle and that
  collection of ``Slice`` fragments. In response, the connector takes appropriate actions to complete the ``UPDATE`` operation.
  Those actions might include committing the transaction, assuming the connector supports a transaction paradigm.
