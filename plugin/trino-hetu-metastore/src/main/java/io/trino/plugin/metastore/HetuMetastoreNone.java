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

import com.google.inject.Inject;
import io.trino.spi.metastore.HetuMetastore;
import io.trino.spi.metastore.model.CatalogEntity;
import io.trino.spi.metastore.model.DatabaseEntity;
import io.trino.spi.metastore.model.TableEntity;

import java.util.List;
import java.util.Optional;

public class HetuMetastoreNone
        implements HetuMetastore
{
    private HetuMetastore delegate;

    @Inject
    public HetuMetastoreNone(@ForHetuMetastoreCache HetuMetastore delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public void createCatalog(CatalogEntity catalog)
    {
        delegate.createCatalog(catalog);
    }

    @Override
    public void createCatalogIfNotExist(CatalogEntity catalog)
    {
        delegate.createCatalogIfNotExist(catalog);
    }

    @Override
    public void alterCatalog(String catalogName, CatalogEntity newCatalog)
    {
        delegate.alterCatalog(catalogName, newCatalog);
    }

    @Override
    public void dropCatalog(String catalogName)
    {
        delegate.dropCatalog(catalogName);
    }

    @Override
    public Optional<CatalogEntity> getCatalog(String catalogName)
    {
        return delegate.getCatalog(catalogName);
    }

    @Override
    public List<CatalogEntity> getCatalogs()
    {
        return delegate.getCatalogs();
    }

    @Override
    public void createDatabase(DatabaseEntity database)
    {
        delegate.createDatabase(database);
    }

    @Override
    public void createDatabaseIfNotExist(DatabaseEntity database)
    {
        delegate.createDatabaseIfNotExist(database);
    }

    @Override
    public void alterDatabase(String catalogName, String databaseName, DatabaseEntity newDatabase)
    {
        delegate.alterDatabase(catalogName, databaseName, newDatabase);
    }

    @Override
    public void dropDatabase(String catalogName, String databaseName)
    {
        delegate.dropDatabase(catalogName, databaseName);
    }

    @Override
    public Optional<DatabaseEntity> getDatabase(String catalogName, String databaseName)
    {
        return delegate.getDatabase(catalogName, databaseName);
    }

    @Override
    public List<DatabaseEntity> getAllDatabases(String catalogName)
    {
        return delegate.getAllDatabases(catalogName);
    }

    @Override
    public void createTable(TableEntity table)
    {
        delegate.createTable(table);
    }

    @Override
    public void createTableIfNotExist(TableEntity table)
    {
        delegate.createTableIfNotExist(table);
    }

    @Override
    public void dropTable(String catalogName, String databaseName, String tableName)
    {
        delegate.dropTable(catalogName, databaseName, tableName);
    }

    @Override
    public void alterTable(String catalogName, String databaseName, String oldTableName, TableEntity newTable)
    {
        delegate.alterTable(catalogName, databaseName, oldTableName, newTable);
    }

    @Override
    public Optional<TableEntity> getTable(String catalogName, String databaseName, String table)
    {
        return delegate.getTable(catalogName, databaseName, table);
    }

    @Override
    public List<TableEntity> getAllTables(String catalogName, String databaseName)
    {
        return delegate.getAllTables(catalogName, databaseName);
    }

    @Override
    public void alterCatalogParameter(String catalogName, String key, String value)
    {
        delegate.alterCatalogParameter(catalogName, key, value);
    }

    @Override
    public void alterDatabaseParameter(String catalogName, String databaseName, String key, String value)
    {
        delegate.alterDatabaseParameter(catalogName, databaseName, key, value);
    }

    @Override
    public void alterTableParameter(String catalogName, String databaseName, String tableName, String key, String value)
    {
        delegate.alterTableParameter(catalogName, databaseName, tableName, key, value);
    }
}
