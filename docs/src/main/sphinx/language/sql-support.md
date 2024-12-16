# SQL statement support

The SQL statement support in Trino can be categorized into several topics. Many
statements are part of the core engine and therefore available in all use cases.
For example, you can always set session properties or inspect an explain plan
and perform other actions with the {ref}`globally available statements
<sql-globally-available>`.

However, the details and architecture of the connected data sources can limit
some SQL functionality. For example, if the data source does not support any
write operations, then a {doc}`/sql/delete` statement cannot be executed against
the data source.

Similarly, if the underlying system does not have any security concepts, SQL
statements like {doc}`/sql/create-role` cannot be supported by Trino and the
connector.

The categories of these different topics are related to {ref}`read operations
<sql-read-operations>`, {ref}`write operations <sql-write-operations>`,
{ref}`security operations <sql-security-operations>` and {ref}`transactions
<sql-transactions>`.

Details of the support for specific statements is available with the
documentation for each connector.

(sql-globally-available)=
## Globally available statements

The following statements are implemented in the core engine and available with
any connector:

- {doc}`/sql/call`
- {doc}`/sql/deallocate-prepare`
- {doc}`/sql/describe-input`
- {doc}`/sql/describe-output`
- {doc}`/sql/execute`
- {doc}`/sql/execute-immediate`
- {doc}`/sql/explain`
- {doc}`/sql/explain-analyze`
- {doc}`/sql/prepare`
- {doc}`/sql/reset-session`
- {doc}`/sql/set-session`
- {doc}`/sql/set-time-zone`
- {doc}`/sql/show-functions`
- {doc}`/sql/show-session`
- {doc}`/sql/use`
- {doc}`/sql/values`

(sql-catalog-management)=
### Catalog management

The following statements are used to [manage dynamic
catalogs](/admin/properties-catalog):

- {doc}`/sql/create-catalog`
- {doc}`/sql/drop-catalog`

(sql-read-operations)=
## Read operations

The following statements provide read access to data and meta data exposed by a
connector accessing a data source. They are supported by all connectors:

- {doc}`/sql/select` including {doc}`/sql/match-recognize`
- {doc}`/sql/describe`
- {doc}`/sql/show-catalogs`
- {doc}`/sql/show-columns`
- {doc}`/sql/show-create-materialized-view`
- {doc}`/sql/show-create-schema`
- {doc}`/sql/show-create-table`
- {doc}`/sql/show-create-view`
- {doc}`/sql/show-grants`
- {doc}`/sql/show-roles`
- {doc}`/sql/show-schemas`
- {doc}`/sql/show-tables`
- {doc}`/sql/show-stats`

(sql-write-operations)=
## Write operations

The following statements provide write access to data and meta data exposed
by a connector accessing a data source. Availability varies widely from
connector to connector:

(sql-data-management)=
### Data management

- {doc}`/sql/insert`
- {doc}`/sql/update`
- {doc}`/sql/delete`
- {doc}`/sql/truncate`
- {doc}`/sql/merge`

(sql-schema-table-management)=
### Schema and table management

- {doc}`/sql/create-table`
- {doc}`/sql/create-table-as`
- {doc}`/sql/drop-table`
- {doc}`/sql/alter-table`
- {doc}`/sql/create-schema`
- {doc}`/sql/drop-schema`
- {doc}`/sql/alter-schema`
- {doc}`/sql/comment`

(sql-view-management)=
### View management

- {doc}`/sql/create-view`
- {doc}`/sql/drop-view`
- {doc}`/sql/alter-view`

(sql-materialized-view-management)=
### Materialized view management

- {doc}`/sql/create-materialized-view`
- {doc}`/sql/alter-materialized-view`
- {doc}`/sql/drop-materialized-view`
- {doc}`/sql/refresh-materialized-view`

(udf-management)=
### User-defined function management

The following statements are used to manage [](udf-catalog):

- [](/sql/create-function)
- [](/sql/drop-function)
- [](/sql/show-functions)

(sql-security-operations)=
## Security operations

The following statements provide security-related operations to security
configuration, data, and meta data exposed by a connector accessing a data
source. Most connectors do not support these operations:

Connector roles:

- {doc}`/sql/create-role`
- {doc}`/sql/drop-role`
- {doc}`/sql/grant-roles`
- {doc}`/sql/revoke-roles`
- {doc}`/sql/set-role`
- {doc}`/sql/show-role-grants`

Grants management:

- {doc}`/sql/deny`
- {doc}`/sql/grant`
- {doc}`/sql/revoke`

(sql-transactions)=
## Transactions

The following statements manage transactions. Most connectors do not support
transactions:

- {doc}`/sql/start-transaction`
- {doc}`/sql/commit`
- {doc}`/sql/rollback`
