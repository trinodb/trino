# Supporting `MERGE`

The Trino engine provides APIs to support row-level SQL `MERGE`.
To implement `MERGE`, a connector must provide the following:

- An implementation of `ConnectorMergeSink`, which is typically
  layered on top of a `ConnectorPageSink`.
- Methods in `ConnectorMetadata` to get a "rowId" column handle, get the
  row change paradigm, and to start and complete the `MERGE` operation.

The Trino engine machinery used to implement SQL `MERGE` is also used to
support SQL `DELETE` and `UPDATE`. This means that all a connector needs to
do is implement support for SQL `MERGE`, and the connector gets all the Data
Modification Language (DML) operations.

## Standard SQL `MERGE`

Different query engines support varying definitions of SQL `MERGE`.
Trino supports the strict SQL specification `ISO/IEC 9075`, published
in 2016. As a simple example, given tables `target_table` and
`source_table` defined as:

```
CREATE TABLE accounts (
    customer VARCHAR,
    purchases DECIMAL,
    address VARCHAR);
INSERT INTO accounts (customer, purchases, address) VALUES ...;
CREATE TABLE monthly_accounts_update (
    customer VARCHAR,
    purchases DECIMAL,
    address VARCHAR);
INSERT INTO monthly_accounts_update (customer, purchases, address) VALUES ...;
```

Here is a possible `MERGE` operation, from `monthly_accounts_update` to
`accounts`:

```
MERGE INTO accounts t USING monthly_accounts_update s
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
```

SQL `MERGE` tries to match each `WHEN` clause in source order. When
a match is found, the corresponding `DELETE`, `INSERT` or `UPDATE`
is executed and subsequent `WHEN` clauses are ignored.

SQL `MERGE` supports two operations on the target table and source
when a row from the source table or query matches a row in the target table:

- `UPDATE`, in which the columns in the target row are updated.
- `DELETE`, in which the target row is deleted.

In the `NOT MATCHED` case, SQL `MERGE` supports only `INSERT`
operations. The values inserted are arbitrary but usually come from
the unmatched row of the source table or query.

## `RowChangeParadigm`

Different connectors have different ways of representing row updates,
imposed by the underlying storage systems. The  Trino engine classifies
these different paradigms as elements of the `RowChangeParadigm`
enumeration, returned by enumeration, returned by method
`ConnectorMetadata.getRowChangeParadigm(...)`.

The `RowChangeParadigm` enumeration values are:

- `CHANGE_ONLY_UPDATED_COLUMNS`, intended for connectors that can update
  individual columns of rows identified by a `rowId`. The corresponding
  merge processor class is `ChangeOnlyUpdatedColumnsMergeProcessor`.
- `DELETE_ROW_AND_INSERT_ROW`, intended for connectors that represent a
  row change as a row deletion paired with a row insertion. The corresponding
  merge processor class is `DeleteAndInsertMergeProcessor`.

## Overview of `MERGE` processing

A `MERGE` statement is processed by creating a `RIGHT JOIN` between the
target table and the source, on the `MERGE` criteria. The source may be
a table or an arbitrary query. For each row in the source table or query,
`MERGE` produces a `ROW` object containing:

- the data column values from the `UPDATE` or `INSERT` cases. For the
  `DELETE` cases, only the partition columns, which determine
  partitioning and bucketing, are non-null.
- a boolean column containing `true` for source rows that matched some
  target row, and `false` otherwise.
- an integer that identifies whether the merge case operation is `UPDATE`,
  `DELETE` or `INSERT`, or a source row for which no case matched. If a
  source row doesn't match any merge case, all data column values except
  those that determine distribution are null, and the operation number
  is -1.

A `SearchedCaseExpression` is constructed from `RIGHT JOIN` result
to represent the `WHEN` clauses of the `MERGE`. In the example preceding
the `MERGE` is executed as if the `SearchedCaseExpression` were written as:

```
SELECT
 CASE
   WHEN present AND s.address = 'Berkeley' THEN
       -- Null values for delete; present=true; operation DELETE=2, case_number=0
       row(null, null, null, true, 2, 0)
   WHEN present AND s.customer = 'Joe Shmoe' THEN
       -- Update column values; present=true; operation UPDATE=3, case_number=1
       row(t.customer, t.purchases + 100.0, t.address, true, 3, 1)
   WHEN present THEN
       -- Update column values; present=true; operation UPDATE=3, case_number=2
       row(t.customer, s.purchases + t.purchases, s.address, true, 3, 2)
   WHEN (present IS NULL) THEN
       -- Insert column values; present=false; operation INSERT=1, case_number=3
       row(s.customer, s.purchases, s.address, false, 1, 3)
   ELSE
       -- Null values for no case matched; present=false; operation=-1,
       --     case_number=-1
       row(null, null, null, false, -1, -1)
 END
 FROM (SELECT *, true AS present FROM target_table) t
   RIGHT JOIN source_table s ON s.customer = t.customer;
```

The Trino engine executes the `RIGHT JOIN` and `CASE` expression,
and ensures that no target table row matches more than one source expression
row, and ultimately creates a sequence of pages to be routed to the node that
runs the `ConnectorMergeSink.storeMergedRows(...)` method.

Like `DELETE` and `UPDATE`, `MERGE` target table rows are identified by
a connector-specific `rowId` column handle. For `MERGE`, the `rowId`
handle is returned by `ConnectorMetadata.getMergeRowIdColumnHandle(...)`.

## `MERGE` redistribution

The Trino `MERGE` implementation allows `UPDATE` to change
the values of columns that determine partitioning and/or bucketing, and so
it must "redistribute" rows from the `MERGE` operation to the worker
nodes responsible for writing rows with the merged partitioning and/or
bucketing columns.

Since the `MERGE` process in general requires redistribution of
merged rows among Trino nodes, the order of rows in pages to be stored
are indeterminate. Connectors like Hive that depend on an ascending
rowId order for deleted rows must sort the deleted rows before storing
them.

To ensure that all inserted rows for a given partition end up on a
single node, the redistribution hash on the partition key/bucket columns
is applied to the page partition keys. As a result of the hash, all
rows for a specific partition/bucket hash together, whether they
were `MATCHED` rows or `NOT MATCHED` rows.

For connectors whose `RowChangeParadigm` is `DELETE_ROW_AND_INSERT_ROW`,
inserted rows are distributed using the layout supplied by
`ConnectorMetadata.getInsertLayout()`. For some connectors, the same
layout is used for updated rows. Other connectors require a special
layout for updated rows, supplied by `ConnectorMetadata.getUpdateLayout()`.

### Connector support for `MERGE`

To start `MERGE` processing, the Trino engine calls:

- `ConnectorMetadata.getMergeRowIdColumnHandle(...)` to get the
  `rowId` column handle.
- `ConnectorMetadata.getRowChangeParadigm(...)` to get the paradigm
  supported by the connector for changing existing table rows.
- `ConnectorMetadata.beginMerge(...)` to get the a
  `ConnectorMergeTableHandle` for the merge operation. That
  `ConnectorMergeTableHandle` object contains whatever information the
  connector needs to specify the `MERGE` operation.
- `ConnectorMetadata.getInsertLayout(...)`, from which it extracts the
  the list of partition or table columns that impact write redistribution.
- `ConnectorMetadata.getUpdateLayout(...)`. If that layout is non-empty,
  it is used to distribute updated rows resulting from the `MERGE`
  operation.

On nodes that are targets of the hash, the Trino engine calls
`ConnectorPageSinkProvider.createMergeSink(...)` to create a
`ConnectorMergeSink`.

To write out each page of merged rows, the Trino engine calls
`ConnectorMergeSink.storeMergedRows(Page)`. The `storeMergedRows(Page)`
method iterates over the rows in the page, performing updates and deletes
in the `MATCHED` cases, and inserts in the `NOT MATCHED` cases.

When using `RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW`, the engine
translates `UPDATE` operations into a pair of `DELETE` and `INSERT`
operations before `storeMergedRows(Page)` is called.

To complete the `MERGE` operation, the Trino engine calls
`ConnectorMetadata.finishMerge(...)`, passing the table handle
and a collection of JSON objects encoded as `Slice` instances. These
objects contain connector-specific information specifying what was changed
by the `MERGE` operation. Typically this JSON object contains the files
written and table and partition statistics generated by the `MERGE`
operation. The connector takes appropriate actions, if any.

## `RowChangeProcessor` implementation for `MERGE`

In the `MERGE` implementation, each `RowChangeParadigm`
corresponds to an internal Trino engine class that implements interface
`RowChangeProcessor`. `RowChangeProcessor` has one interesting method:
`Page transformPage(Page)`. The format of the output page depends
on the `RowChangeParadigm`.

The connector has no access to the `RowChangeProcessor` instance -- it
is used inside the Trino engine to transform the merge page rows into rows
to be stored, based on the connector's choice of `RowChangeParadigm`.

The page supplied to `transformPage()` consists of:

- The write redistribution columns if any
- For partitioned or bucketed tables, a long hash value column.
- The `rowId` column for the row from the target table if matched, or
  null if not matched
- The merge case `RowBlock`
- The integer case number block
- The byte `is_distinct` block, with value 0 if not distinct.

The merge case `RowBlock` has the following layout:

- Blocks for each column in the table, including partition columns, in
  table column order.
- A block containing the boolean "present" value which is true if the
  source row matched a target row, and false otherwise.
- A block containing the `MERGE` case operation number, encoded as
  `INSERT` = 1, `DELETE` = 2, `UPDATE` = 3 and if no `MERGE`
  case matched, -1.
- A block containing the number, starting with 0, for the
  `WHEN` clause that matched for the row, or -1 if no clause
  matched.

The page returned from `transformPage` consists of:

- All table columns, in table column order.
- The merge case operation block.
- The rowId block.
- A byte block containing 1 if the row is an insert derived from an
  update operation, and 0 otherwise. This block is used to correctly
  calculate the count of rows changed for connectors that represent
  updates and deletes plus inserts.

`transformPage`
must ensure that there are no rows whose operation number is -1 in
the page it returns.

## Detecting duplicate matching target rows

The SQL `MERGE` specification requires that in each `MERGE` case,
a single target table row must match at most one source row, after
applying the `MERGE` case condition expression. The first step
toward finding these error is done by labeling each row in the target
table with a unique id, using an `AssignUniqueId` node above the
target table scan. The projected results from the `RIGHT JOIN`
have these unique ids for matched target table rows as well as
the `WHEN` clause number. A `MarkDistinct` node adds an
`is_distinct` column which is true if no other row has the same
unique id and `WHEN` clause number, and false otherwise. If
any row has `is_distinct` equal to false, a
`MERGE_TARGET_ROW_MULTIPLE_MATCHES` exception is raised and
the `MERGE` operation fails.

## `ConnectorMergeTableHandle` API

Interface `ConnectorMergeTableHandle` defines one method,
`getTableHandle()` to retrieve the `ConnectorTableHandle`
originally passed to `ConnectorMetadata.beginMerge()`.

## `ConnectorPageSinkProvider` API

To support SQL `MERGE`, `ConnectorPageSinkProvider` must implement
the method that creates the `ConnectorMergeSink`:

- `createMergeSink`:

  ```
  ConnectorMergeSink createMergeSink(
      ConnectorTransactionHandle transactionHandle,
      ConnectorSession session,
      ConnectorMergeTableHandle mergeHandle)
  ```

## `ConnectorMergeSink` API

To support `MERGE`, the connector must define an
implementation of `ConnectorMergeSink`, usually layered over the
connector's `ConnectorPageSink`.

The `ConnectorMergeSink` is created by a call to
`ConnectorPageSinkProvider.createMergeSink()`.

The only interesting methods are:

- `storeMergedRows`:

  ```
  void storeMergedRows(Page page)
  ```

  The Trino engine calls the `storeMergedRows(Page)` method of the
  `ConnectorMergeSink` instance returned by
  `ConnectorPageSinkProvider.createMergeSink()`, passing the page
  generated by the `RowChangeProcessor.transformPage()` method.
  That page consists of all table columns, in table column order,
  followed by the `TINYINT` operation column, followed by the rowId column.

  The job of `storeMergedRows()` is iterate over the rows in the page,
  and process them based on the value of the operation column, `INSERT`,
  `DELETE`, `UPDATE`, or ignore the row. By choosing appropriate
  paradigm, the connector can request that the UPDATE operation be
  transformed into `DELETE` and `INSERT` operations.

- `finish`:

  ```
  CompletableFuture<Collection<Slice>> finish()
  ```

  The Trino engine calls `finish()` when all the data has been processed by
  a specific `ConnectorMergeSink` instance. The connector returns a future
  containing a collection of `Slice`, representing connector-specific
  information about the rows processed. Usually this includes the row count,
  and might include information like the files or partitions created or
  changed.

## `ConnectorMetadata` `MERGE` API

A connector implementing `MERGE` must implement these `ConnectorMetadata`
methods.

- `getRowChangeParadigm()`:

  ```
  RowChangeParadigm getRowChangeParadigm(
      ConnectorSession session,
      ConnectorTableHandle tableHandle)
  ```

  This method is called as the engine starts processing a `MERGE` statement.
  The connector must return a `RowChangeParadigm` enumeration instance. If
  the connector doesn't support `MERGE`, then it should throw a
  `NOT_SUPPORTED` exception to indicate that SQL `MERGE` isn't supported by
  the connector. Note that the default implementation already throws this
  exception when the method isn't implemented.

- `getMergeRowIdColumnHandle()`:

  ```
  ColumnHandle getMergeRowIdColumnHandle(
      ConnectorSession session,
      ConnectorTableHandle tableHandle)
  ```

  This method is called in the early stages of query planning for `MERGE`
  statements. The ColumnHandle returned provides the `rowId` used by the
  connector to identify rows to be merged, as well as any other fields of
  the row that the connector needs to complete the `MERGE` operation.

- `getInsertLayout()`:

  ```
  Optional<ConnectorTableLayout> getInsertLayout(
      ConnectorSession session,
      ConnectorTableHandle tableHandle)
  ```

  This method is called during query planning to get the table layout to be
  used for rows inserted by the `MERGE` operation. For some connectors,
  this layout is used for rows deleted as well.

- `getUpdateLayout()`:

  ```
  Optional<ConnectorTableLayout> getUpdateLayout(
      ConnectorSession session,
      ConnectorTableHandle tableHandle)
  ```

  This method is called during query planning to get the table layout to be
  used for rows deleted by the `MERGE` operation. If the optional return
  value is present, the Trino engine uses the layout for updated rows.
  Otherwise, it uses the result of `ConnectorMetadata.getInsertLayout` to
  distribute updated rows.

- `beginMerge()`:

  ```
  ConnectorMergeTableHandle beginMerge(
       ConnectorSession session,
       ConnectorTableHandle tableHandle)
  ```

  As the last step in creating the `MERGE` execution plan, the connector's
  `beginMerge()` method is called, passing the `session`, and the
  `tableHandle`.

  `beginMerge()` performs any orchestration needed in the connector to
  start processing the `MERGE`. This orchestration varies from connector
  to connector. In the case of Hive connector operating on transactional tables,
  for example, `beginMerge()` checks that the table is transactional and
  starts a Hive Metastore transaction.

  `beginMerge()` returns a `ConnectorMergeTableHandle` with any added
  information the connector needs when the handle is passed back to
  `finishMerge()` and the split generation machinery. For most
  connectors, the returned table handle contains at least a flag identifying
  the table handle as a table handle for a `MERGE` operation.

- `finishMerge()`:

  ```
  void finishMerge(
      ConnectorSession session,
      ConnectorMergeTableHandle tableHandle,
      Collection<Slice> fragments)
  ```

  During `MERGE` processing, the Trino engine accumulates the `Slice`
  collections returned by `ConnectorMergeSink.finish()`. The engine calls
  `finishMerge()`, passing the table handle and that collection of
  `Slice` fragments. In response, the connector takes appropriate actions
  to complete the `MERGE` operation. Those actions might include
  committing an underlying transaction, if any, or freeing any other
  resources.
