# Presto Query History Extension

This module provides an implementation for query history extension, relying on a RDBMS to persist completed queries.

## How to use it

To enable this extension implementation for query history, you should:

* Add the jar of the extension implementation (`io.prestosql:presto-history-extension`) into the presto coordinator classpath. (We don't need to add mariadb jdbc because we can use mysql driver already bundled with presto server)
* Create a properties file `${PRESTO_ROOT}/etc/extension/query-history-store.properties` for presto coordinator.
* In the properties file, add the extension implementation property `io.prestosql.server.extension.query.history.QueryHistoryStore.impl` in the property file (e.g. `io.prestosql.server.extension.query.history.QueryHistoryStore.impl = io.prestosql.server.extension.query.history.QueryHistorySQLStore`). This is the implementation that we will use for the history extension
* In the properties file, add all the necessary jdbc properties under the namespace `sql.`, especially, `sql.jdbcUrl`, that's used to determine both jdbc connection and jdbc driver (e.g. `sql.jdbcUrl = jdbc:mysql:://localhost:3306/PrestoQuery_DB?user=USER_NAME&password=PASSWORD`)
* In the properties file, define appropriate `presto.cluster` which will be used to filter the origin of the query

## Behind the scenes

`QueryTracker` will look up `${PRESTO_ROOT}/etc/extension/query-history-store.properties` to create a query history store. In the properties file, it discover which implementation to use for `QueryHistoryStore`. In the case of the implementation `io.prestosql.server.extension.query.history.QueryHistorySQLStore`, it uses the section `sql.` to find all jdbc connection configurations.

If the properties file is not present or it failed to create the extension implementation (either some necessary key-value pair is missing, or any error during the instantiation such as `ClassNotFoundException` or `SQLException`), no extension will be used.

## Sample of the extension properties file

```
io.prestosql.server.extension.query.history.QueryHistoryStore.impl = io.prestosql.server.extension.query.history.QueryHistorySQLStore
sql.jdbcUrl = jdbc:mysql://localhost:3306/PrestoQuery_DB?user=root&password=myPassword
presto.cluster=preprod-pa4
```