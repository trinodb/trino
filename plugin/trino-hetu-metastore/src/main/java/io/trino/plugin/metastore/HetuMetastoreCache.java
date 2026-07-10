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
package io.trino.plugin.metastore;

import io.airlift.log.Logger;
import io.trino.spi.metastore.HetuCache;
import io.trino.spi.metastore.HetuMetastore;
import io.trino.spi.metastore.model.CatalogEntity;
import io.trino.spi.metastore.model.DatabaseEntity;
import io.trino.spi.metastore.model.TableEntity;

import java.util.List;
import java.util.Optional;

public class HetuMetastoreCache
        implements HetuMetastore
{
    private static final Logger log = Logger.get(HetuMetastoreCache.class);

    private HetuMetastore delegate;

    private HetuCache<String, Optional<CatalogEntity>> catalogCache;
    private HetuCache<String, List<CatalogEntity>> catalogsCache;
    private HetuCache<String, Optional<DatabaseEntity>> databaseCache;
    private HetuCache<String, List<DatabaseEntity>> databasesCache;
    private HetuCache<String, Optional<TableEntity>> tableCache;
    private HetuCache<String, List<TableEntity>> tablesCache;

    public HetuMetastoreCache(HetuMetastore delegate,
            HetuCache<String, Optional<CatalogEntity>> catalogCache,
            HetuCache<String, List<CatalogEntity>> catalogsCache,
            HetuCache<String, Optional<DatabaseEntity>> databaseCache,
            HetuCache<String, List<DatabaseEntity>> databasesCache,
            HetuCache<String, Optional<TableEntity>> tableCache,
            HetuCache<String, List<TableEntity>> tablesCache)
    {
        this.delegate = delegate;
        this.catalogCache = catalogCache;
        this.catalogsCache = catalogsCache;
        this.databaseCache = databaseCache;
        this.databasesCache = databasesCache;
        this.tableCache = tableCache;
        this.tablesCache = tablesCache;
    }

    @Override
    public void createCatalog(CatalogEntity catalog)
    {
        try {
            delegate.createCatalog(catalog);
        }
        finally {
            catalogsCache.invalidateAll();
            catalogCache.invalidate(catalog.getName());
        }
    }

    @Override
    public void createCatalogIfNotExist(CatalogEntity catalog)
    {
        try {
            delegate.createCatalogIfNotExist(catalog);
        }
        finally {
            catalogsCache.invalidateAll();
            catalogCache.invalidate(catalog.getName());
        }
    }

    @Override
    public void alterCatalog(String catalogName, CatalogEntity newCatalog)
    {
        try {
            delegate.alterCatalog(catalogName, newCatalog);
        }
        finally {
            catalogCache.invalidate(catalogName);
            catalogCache.invalidate(newCatalog.getName());
            catalogsCache.invalidateAll();
        }
    }

    @Override
    public void dropCatalog(String catalogName)
    {
        try {
            delegate.dropCatalog(catalogName);
        }
        finally {
            catalogCache.invalidate(catalogName);
            catalogsCache.invalidateAll();
        }
    }

    @Override
    public Optional<CatalogEntity> getCatalog(String catalogName)
    {
        try {
            return catalogCache.getIfAbsent(catalogName, () -> delegate.getCatalog(catalogName));
        }
        catch (Exception executionException) {
            log.debug(executionException.getCause(),
                    String.format("Error while caching catalog[%s] metadata. Falling back to default flow", catalogName));
            return delegate.getCatalog(catalogName);
        }
    }

    @Override
    public List<CatalogEntity> getCatalogs()
    {
        try {
            return catalogsCache.getIfAbsent("", () -> delegate.getCatalogs());
        }
        catch (Exception executionException) {
            log.debug(executionException.getCause(),
                    "Error while caching all catalogs metadata. Falling back to default flow");
            return delegate.getCatalogs();
        }
    }

    @Override
    public void createDatabase(DatabaseEntity database)
    {
        try {
            delegate.createDatabase(database);
        }
        finally {
            databasesCache.invalidate(database.getCatalogName());
            databaseCache.invalidate(database.getCatalogName() + "." + database.getName());
        }
    }

    @Override
    public void createDatabaseIfNotExist(DatabaseEntity database)
    {
        try {
            delegate.createDatabaseIfNotExist(database);
        }
        finally {
            databasesCache.invalidate(database.getCatalogName());
            databaseCache.invalidate(database.getCatalogName() + "." + database.getName());
        }
    }

    @Override
    public void alterDatabase(String catalogName, String databaseName, DatabaseEntity newDatabase)
    {
        try {
            delegate.alterDatabase(catalogName, databaseName, newDatabase);
        }
        finally {
            String key = catalogName + "." + databaseName;
            String newKey = catalogName + "." + newDatabase.getName();
            databaseCache.invalidate(key);
            databaseCache.invalidate(newKey);
            databasesCache.invalidate(catalogName);
        }
    }

    @Override
    public void dropDatabase(String catalogName, String databaseName)
    {
        try {
            delegate.dropDatabase(catalogName, databaseName);
        }
        finally {
            String key = catalogName + "." + databaseName;
            databaseCache.invalidate(key);
            databasesCache.invalidate(catalogName);
        }
    }

    @Override
    public Optional<DatabaseEntity> getDatabase(String catalogName, String databaseName)
    {
        try {
            String key = catalogName + "." + databaseName;
            return databaseCache.getIfAbsent(key, () -> delegate.getDatabase(catalogName, databaseName));
        }
        catch (Exception executionException) {
            log.debug(executionException.getCause(),
                    String.format("Error while caching database[%s.%s] metadata. Falling back to default flow"), catalogName, databaseName);
            return delegate.getDatabase(catalogName, databaseName);
        }
    }

    @Override
    public List<DatabaseEntity> getAllDatabases(String catalogName)
    {
        try {
            return databasesCache.getIfAbsent(catalogName, () -> delegate.getAllDatabases(catalogName));
        }
        catch (Exception executionException) {
            log.debug(executionException.getCause(),
                    String.format("Error while caching all databases metadata in catalog[%s]. Falling back to default flow", catalogName));
            return delegate.getAllDatabases(catalogName);
        }
    }

    @Override
    public void createTable(TableEntity table)
    {
        try {
            delegate.createTable(table);
        }
        finally {
            String databaseKey = table.getCatalogName() + "." + table.getDatabaseName();
            String tableKey = databaseKey + '.' + table.getName();
            tableCache.invalidate(tableKey);
            tablesCache.invalidate(databaseKey);
        }
    }

    @Override
    public void createTableIfNotExist(TableEntity table)
    {
        try {
            delegate.createTableIfNotExist(table);
        }
        finally {
            String databaseKey = table.getCatalogName() + "." + table.getDatabaseName();
            String tableKey = databaseKey + '.' + table.getName();
            tableCache.invalidate(tableKey);
            tablesCache.invalidate(databaseKey);
        }
    }

    @Override
    public void dropTable(String catalogName, String databaseName, String tableName)
    {
        try {
            delegate.dropTable(catalogName, databaseName, tableName);
        }
        finally {
            String databaseKey = catalogName + '.' + databaseName;
            String tableKey = catalogName + '.' + databaseName + '.' + tableName;
            tableCache.invalidate(tableKey);
            tablesCache.invalidate(databaseKey);
        }
    }

    @Override
    public void alterTable(String catalogName, String databaseName, String oldTableName, TableEntity newTable)
    {
        try {
            delegate.alterTable(catalogName, databaseName, oldTableName, newTable);
        }
        finally {
            String databaseKey = catalogName + '.' + databaseName;
            String tableKey = databaseKey + "." + oldTableName;
            String newTableKey = databaseKey + "." + newTable.getName();
            tableCache.invalidate(tableKey);
            tableCache.invalidate(newTableKey);
            tablesCache.invalidate(databaseKey);
        }
    }

    @Override
    public void alterCatalogParameter(String catalogName, String key, String value)
    {
        try {
            delegate.alterCatalogParameter(catalogName, key, value);
        }
        finally {
            catalogCache.invalidate(catalogName);
            catalogsCache.invalidateAll();
        }
    }

    @Override
    public void alterDatabaseParameter(String catalogName, String databaseName, String key, String value)
    {
        try {
            delegate.alterDatabaseParameter(catalogName, databaseName, key, value);
        }
        finally {
            String databaseKey = catalogName + '.' + databaseName;
            databaseCache.invalidate(databaseKey);
            databasesCache.invalidate(catalogName);
        }
    }

    @Override
    public void alterTableParameter(String catalogName, String databaseName, String tableName, String key, String value)
    {
        try {
            delegate.alterTableParameter(catalogName, databaseName, tableName, key, value);
        }
        finally {
            String databaseKey = catalogName + '.' + databaseName;
            String tableKey = catalogName + "." + databaseName + "." + tableName;
            tableCache.invalidate(tableKey);
            tablesCache.invalidate(databaseKey);
        }
    }

    @Override
    public Optional<TableEntity> getTable(String catalogName, String databaseName, String tableName)
    {
        try {
            String key = catalogName + '.' + databaseName + '.' + tableName;
            return tableCache.getIfAbsent(key, () -> delegate.getTable(catalogName, databaseName, tableName));
        }
        catch (Exception executionException) {
            log.debug(executionException.getCause(),
                    String.format("Error while caching table[%s.%s.%s] metadata. Falling back to default flow", catalogName, databaseName, tableName));
            return delegate.getTable(catalogName, databaseName, tableName);
        }
    }

    @Override
    public List<TableEntity> getAllTables(String catalogName, String databaseName)
    {
        try {
            String key = catalogName + "." + databaseName;
            return tablesCache.getIfAbsent(key, () -> delegate.getAllTables(catalogName, databaseName));
        }
        catch (Exception executionException) {
            log.debug(executionException.getCause(),
                    String.format("Error while caching all tables metadata in %s.%s. Falling back to default flow", catalogName, databaseName));
            return delegate.getAllTables(catalogName, databaseName);
        }
    }
}
