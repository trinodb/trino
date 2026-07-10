/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.metastore.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogNotFoundException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.metastore.HetuMetastore;
import io.trino.spi.metastore.model.CatalogEntity;
import io.trino.spi.metastore.model.ColumnEntity;
import io.trino.spi.metastore.model.DatabaseEntity;
import io.trino.spi.metastore.model.TableEntity;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.MoreCollectors.toOptional;
import static io.trino.spi.metastore.HetuErrorCode.HETU_METASTORE_CODE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * jdbc hetu metastore
 *
 * @since 2020-02-27
 */
public class JdbcHetuMetastore
        implements HetuMetastore
{
    private static final String DATABASE_ERROR_MESSAGE = "Database '%s.%s' does not exist:";
    private static final String CATALOG_ERROR_MESSAGE = "Catalog '%s' does not exist:";
    private final Jdbi jdbi;
    private final JdbcMetadataDao dao;

    /**
     * jdbc hetu metastore
     *
     * @param config config
     */
    @Inject
    public JdbcHetuMetastore(JdbcMetastoreConfig config)
    {
        requireNonNull(config, "jdbc metastore config is null");
        jdbi = Jdbi.create(config.getDbUrl(), config.getDbUser(), config.getDbPassword())
                .installPlugin(new SqlObjectPlugin());
        JdbcMetadataUtil.createTablesWithRetry(jdbi);
        this.dao = JdbcMetadataUtil.onDemand(jdbi, JdbcMetadataDao.class);
    }

    @Override
    public void createCatalog(CatalogEntity catalog)
    {
        JdbcMetadataUtil.runTransactionWithLock(jdbi, handle -> {
            JdbcMetadataDao transactionDao = handle.attach(JdbcMetadataDao.class);
            long catalogId = transactionDao.insertCatalog(catalog);
            Optional<List<PropertyEntity>> properties = mapToList(catalog.getParameters());
            properties.ifPresent(parameters -> transactionDao.insertCatalogProperty(catalogId, parameters));
        });
    }

    @Override
    public void createCatalogIfNotExist(CatalogEntity catalog)
    {
        try {
            createCatalog(catalog);
        }
        catch (TrinoException e) {
            Optional<CatalogEntity> existedCatalog = getCatalog(catalog.getName());
            if (!(existedCatalog.isPresent() && existedCatalog.get().equals(catalog))) {
                throw new TrinoException(HETU_METASTORE_CODE, e.getMessage(), e.getCause());
            }
        }
    }

    @Override
    public void alterCatalog(String catalogName, CatalogEntity newCatalog)
    {
        if (!catalogName.equals(newCatalog.getName())) {
            throw new TrinoException(HETU_METASTORE_CODE, "Cannot alter a catalog's name");
        }

        JdbcMetadataUtil.runTransactionWithLock(jdbi, handle -> {
            JdbcMetadataDao transactionDao = handle.attach(JdbcMetadataDao.class);

            Long catalogId = getCatalogId(transactionDao, catalogName);

            CatalogEntity catalog = CatalogEntity.builder(newCatalog).build();
            transactionDao.dropCatalogProperty(catalogId);
            Optional<List<PropertyEntity>> properties = mapToList(catalog.getParameters());
            properties.ifPresent(parameters -> transactionDao.insertCatalogProperty(catalogId, parameters));

            transactionDao.alterCatalog(catalogName, catalog);
        });
    }

    @Override
    public void dropCatalog(String catalogName)
    {
        JdbcMetadataUtil.runTransactionWithLock(jdbi, handle -> {
            JdbcMetadataDao transactionDao = handle.attach(JdbcMetadataDao.class);

            Long catalogId = getCatalogId(transactionDao, catalogName);
            transactionDao.dropCatalogProperty(catalogId);
            transactionDao.dropCatalog(catalogName);
        });
    }

    private Long getCatalogId(JdbcMetadataDao metadataDao, String catalogName)
    {
        Long catalogId = metadataDao.getCatalogId(catalogName);
        if (catalogId == null) {
            throw new TrinoException(HETU_METASTORE_CODE, format(CATALOG_ERROR_MESSAGE, catalogName));
        }

        return catalogId;
    }

    private void checkCatalogExists(JdbcMetadataDao metadataDao, String catalogName)
    {
        getCatalogId(metadataDao, catalogName);
    }

    @Override
    public Optional<CatalogEntity> getCatalog(String catalogName)
    {
        return dao.getCatalog(catalogName);
    }

    @Override
    public List<CatalogEntity> getCatalogs()
    {
        return dao.getAllCatalogs();
    }

    @Override
    public void createDatabase(DatabaseEntity database)
    {
        JdbcMetadataUtil.runTransactionWithLock(jdbi, handle -> {
            JdbcMetadataDao transactionDao = handle.attach(JdbcMetadataDao.class);
            checkCatalogExists(transactionDao, database.getCatalogName());
            long databaseId = transactionDao.insertDatabase(database);
            Optional<List<PropertyEntity>> properties = mapToList(database.getParameters());
            properties.ifPresent(parameters -> transactionDao.insertDatabaseProperty(databaseId, parameters));
        });
    }

    @Override
    public void createDatabaseIfNotExist(DatabaseEntity database)
    {
        try {
            createDatabase(database);
        }
        catch (TrinoException e) {
            Optional<DatabaseEntity> existedDatabase = getDatabase(database.getCatalogName(), database.getName());
            if (!(existedDatabase.isPresent() && existedDatabase.get().equals(database))) {
                throw new TrinoException(HETU_METASTORE_CODE, e.getMessage(), e.getCause());
            }
        }
    }

    @Override
    public void alterDatabase(String catalogName, String databaseName, DatabaseEntity newDatabase)
    {
        if (!catalogName.equals(newDatabase.getCatalogName())) {
            throw new TrinoException(HETU_METASTORE_CODE,
                    format("alter database cannot cross catalog[%s,%s]", catalogName, newDatabase.getCatalogName()));
        }

        JdbcMetadataUtil.runTransactionWithLock(jdbi, handle -> {
            JdbcMetadataDao transactionDao = handle.attach(JdbcMetadataDao.class);

            Long databaseId = transactionDao.getDatabaseId(catalogName, databaseName);
            DatabaseEntity database = DatabaseEntity.builder(newDatabase).build();

            transactionDao.dropDatabaseProperty(databaseId);
            Optional<List<PropertyEntity>> properties = mapToList(database.getParameters());
            properties.ifPresent(parameters -> transactionDao.insertDatabaseProperty(databaseId, parameters));
            transactionDao.alterDatabase(catalogName, databaseName, database);
        });
    }

    @Override
    public void dropDatabase(String catalogName, String databaseName)
    {
        JdbcMetadataUtil.runTransactionWithLock(jdbi, handle -> {
            JdbcMetadataDao transactionDao = handle.attach(JdbcMetadataDao.class);
            Long databaseId = getDatabaseId(catalogName, databaseName, transactionDao);
            transactionDao.dropDatabaseProperty(databaseId);
            transactionDao.dropDatabase(catalogName, databaseName);
        });
    }

    @Override
    public Optional<DatabaseEntity> getDatabase(String catalogName, String databaseName)
    {
        requireNonNull(databaseName, "database name is null");
        return dao.getDatabase(catalogName, databaseName);
    }

    @Override
    public List<DatabaseEntity> getAllDatabases(String catalogName)
    {
        return dao.getAllDatabases(catalogName);
    }

    @Override
    public void createTable(TableEntity table)
    {
        JdbcMetadataUtil.runTransactionWithLock(jdbi, handle -> {
            JdbcMetadataDao transactionDao = handle.attach(JdbcMetadataDao.class);

            Long databaseId = getDatabaseId(table.getCatalogName(), table.getDatabaseName(), transactionDao);

            long tableId = transactionDao.insertTable(databaseId, table);
            Optional<List<PropertyEntity>> tableProps = mapToList(table.getParameters());
            tableProps.ifPresent(props -> transactionDao.insertTableProperty(tableId, props));

            insertColumnInfo(transactionDao, tableId, table.getColumns());
        });
    }

    @Override
    public void createTableIfNotExist(TableEntity table)
    {
        try {
            createTable(table);
        }
        catch (TrinoException e) {
            Optional<TableEntity> existedTable = getTable(table.getCatalogName(), table.getDatabaseName(), table.getName());
            if (!(existedTable.isPresent() && existedTable.get().equals(table))) {
                throw new TrinoException(HETU_METASTORE_CODE, e.getMessage(), e.getCause());
            }
        }
    }

    @Override
    public void dropTable(String catalogName, String databaseName, String tableName)
    {
        JdbcMetadataUtil.runTransactionWithLock(jdbi, handle -> {
            JdbcMetadataDao transactionDao = handle.attach(JdbcMetadataDao.class);
            // get table id
            Long tableId = transactionDao.getTableId(catalogName, databaseName, tableName);
            if (tableId == null) {
                throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
            }

            // drop table property
            transactionDao.dropTableProperty(tableId);
            // get column id
            transactionDao.getColumnId(tableId).forEach(transactionDao::dropColumnProperty);
            // drop column
            transactionDao.dropColumn(tableId);
            // drop table
            transactionDao.dropTable(tableId);
        });
    }

    @Override
    public void alterTable(String catalogName, String databaseName, String oldTableName, TableEntity newTable)
    {
        JdbcMetadataUtil.runTransactionWithLock(jdbi, handle -> {
            JdbcMetadataDao transactionDao = handle.attach(JdbcMetadataDao.class);

            // get table id
            Long tableId = getTableId(transactionDao, catalogName, databaseName, oldTableName);

            // drop table property
            transactionDao.dropTableProperty(tableId);
            // get column id and drop column property
            transactionDao.getColumnId(tableId).forEach(transactionDao::dropColumnProperty);
            // drop column
            transactionDao.dropColumn(tableId);

            TableEntity table = TableEntity.builder(newTable).build();
            Optional<List<PropertyEntity>> tableProps = mapToList(table.getParameters());
            tableProps.ifPresent(props -> transactionDao.insertTableProperty(tableId, props));

            insertColumnInfo(transactionDao, tableId, table.getColumns());

            transactionDao.alterTable(tableId, table);
        });
    }

    private void insertColumnInfo(JdbcMetadataDao metadataDao, long tableId, List<ColumnEntity> columns)
    {
        if (columns == null) {
            return;
        }

        columns.forEach(column -> {
            long columnId = metadataDao.insertColumn(tableId, column);
            Optional<List<PropertyEntity>> columnParams = mapToList(column.getParameters());
            columnParams.ifPresent(parameters -> metadataDao.insertColumnProperty(columnId, parameters));
        });
    }

    private Long getTableId(JdbcMetadataDao metadataDao, String catalogName, String databaseName, String tableName)
    {
        Long tableId = metadataDao.getTableId(catalogName, databaseName, tableName);
        if (tableId == null) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        return tableId;
    }

    @Override
    public Optional<TableEntity> getTable(String catalogName, String databaseName, String tableName)
    {
        requireNonNull(tableName, "table name is null");
        ImmutableList.Builder<TableEntity> tables = ImmutableList.builder();
        JdbcMetadataUtil.runTransaction(jdbi, handle -> {
            JdbcMetadataDao transactionDao = handle.attach(JdbcMetadataDao.class);
            List<Map.Entry<Long, TableEntity>> entries = transactionDao.getTable(catalogName, databaseName, tableName);

            if (entries.size() > 1) {
                throw new TrinoException(HETU_METASTORE_CODE, "Multiple table appear.");
            }
            getTableColumns(entries, tables, transactionDao);
        });
        return tables.build().stream().collect(toOptional());
    }

    @Override
    public List<TableEntity> getAllTables(String catalogName, String databaseName)
    {
        ImmutableList.Builder<TableEntity> tables = ImmutableList.builder();
        JdbcMetadataUtil.runTransaction(jdbi, handle -> {
            JdbcMetadataDao transactionDao = handle.attach(JdbcMetadataDao.class);
            List<Map.Entry<Long, TableEntity>> entries = transactionDao.getAllTables(catalogName, databaseName);
            getTableColumns(entries, tables, transactionDao);
        });
        return new ArrayList<>(tables.build());
    }

    @Override
    public void alterCatalogParameter(String catalogName, String key, String value)
    {
        JdbcMetadataUtil.runTransactionWithLock(jdbi, handle -> {
            JdbcMetadataDao transactionDao = handle.attach(JdbcMetadataDao.class);

            // get catalog id and catalog entity
            long catalogId = transactionDao.getCatalogId(catalogName);
            CatalogEntity catalogEntity = transactionDao.getCatalog(catalogName).orElseThrow(() -> new CatalogNotFoundException(catalogName));

            // drop table property
            transactionDao.dropCatalogProperty(catalogId);

            if (value == null) {
                catalogEntity.getParameters().remove(key);
            }
            else {
                catalogEntity.getParameters().put(key, value);
            }
            Optional<List<PropertyEntity>> catalogProps = mapToList(catalogEntity.getParameters());
            catalogProps.ifPresent(props -> transactionDao.insertCatalogProperty(catalogId, props));
        });
    }

    @Override
    public void alterDatabaseParameter(String catalogName, String databaseName, String key, String value)
    {
        JdbcMetadataUtil.runTransactionWithLock(jdbi, handle -> {
            JdbcMetadataDao transactionDao = handle.attach(JdbcMetadataDao.class);

            // get catalog id and catalog entity
            long databaseId = transactionDao.getDatabaseId(catalogName, databaseName);
            DatabaseEntity databaseEntity = transactionDao.getDatabase(catalogName, databaseName).orElseThrow(() -> new SchemaNotFoundException(catalogName + "." + databaseName));

            // drop table property
            transactionDao.dropDatabaseProperty(databaseId);

            if (value == null) {
                databaseEntity.getParameters().remove(key);
            }
            else {
                databaseEntity.getParameters().put(key, value);
            }
            Optional<List<PropertyEntity>> databaseProps = mapToList(databaseEntity.getParameters());
            databaseProps.ifPresent(props -> transactionDao.insertDatabaseProperty(databaseId, props));
        });
    }

    @Override
    public void alterTableParameter(String catalogName, String databaseName, String tableName, String key, String value)
    {
        JdbcMetadataUtil.runTransactionWithLock(jdbi, handle -> {
            JdbcMetadataDao transactionDao = handle.attach(JdbcMetadataDao.class);
            List<Map.Entry<Long, TableEntity>> entries = transactionDao.getTable(catalogName, databaseName, tableName);
            if (entries.size() != 1) {
                throw new TrinoException(HETU_METASTORE_CODE, "Get table failed.");
            }

            // get table id and table entity
            long tableId = entries.get(0).getKey();
            TableEntity tableEntity = entries.get(0).getValue();

            // drop table property
            transactionDao.dropTableProperty(tableId);

            if (value == null) {
                tableEntity.getParameters().remove(key);
            }
            else {
                tableEntity.getParameters().put(key, value);
            }
            Optional<List<PropertyEntity>> tableProps = mapToList(tableEntity.getParameters());
            tableProps.ifPresent(props -> transactionDao.insertTableProperty(tableId, props));
        });
    }

    private void getTableColumns(List<Map.Entry<Long, TableEntity>> tableEntries,
            ImmutableList.Builder<TableEntity> tables, JdbcMetadataDao transactionDao)
    {
        tableEntries.forEach(entry -> {
            TableEntity table = entry.getValue();
            List<ColumnEntity> columns = transactionDao.getAllColumns(entry.getKey());
            table.setColumns(columns);
            tables.add(table);
        });
    }

    private Optional<List<PropertyEntity>> mapToList(Map<String, String> map)
    {
        if (map == null || map.isEmpty()) {
            return Optional.empty();
        }
        ImmutableList.Builder<PropertyEntity> propBuilder = ImmutableList.builder();
        map.forEach((key, value) -> propBuilder.add(new PropertyEntity(key, value)));
        return Optional.of(propBuilder.build());
    }

    private Long getDatabaseId(String catalogName, String databaseName, JdbcMetadataDao metadataDao)
    {
        Long databaseId = metadataDao.getDatabaseId(catalogName, databaseName);
        if (databaseId == null) {
            throw new TrinoException(HETU_METASTORE_CODE,
                    format(DATABASE_ERROR_MESSAGE, catalogName, databaseName));
        }
        return databaseId;
    }
}
