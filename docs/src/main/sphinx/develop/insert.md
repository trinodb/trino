# Supporting `INSERT` and `CREATE TABLE AS`

To support `INSERT`, a connector must implement:

- `beginInsert()` and `finishInsert()` from the `ConnectorMetadata`
  interface;
- a `ConnectorPageSinkProvider` that receives a table handle and returns
  a `ConnectorPageSink`.

When executing an `INSERT` statement, the engine calls the `beginInsert()`
method in the connector, which receives a table handle and a list of columns.
It should return a `ConnectorInsertTableHandle`, that can carry any
connector specific information, and it's passed to the page sink provider.
The `PageSinkProvider` creates a page sink, that accepts `Page` objects.

When all the pages for a specific split have been processed, Trino calls
`ConnectorPageSink.finish()`, which returns a `Collection<Slice>`
of fragments representing connector-specific information about the processed
rows.

When all pages for all splits have been processed, Trino calls
`ConnectorMetadata.finishInsert()`, passing a collection containing all
the fragments from all the splits. The connector does what is required
to finalize the operation, for example, committing the transaction.

To support `CREATE TABLE AS`, the `ConnectorPageSinkProvider` must also
return a page sink when receiving a `ConnectorOutputTableHandle`. This handle
is returned from `ConnectorMetadata.beginCreateTable()`.
