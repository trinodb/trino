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
package io.trino.spi.metastore;

import io.trino.spi.metastore.model.CatalogEntity;
import io.trino.spi.metastore.model.DatabaseEntity;
import io.trino.spi.metastore.model.TableEntity;

import java.util.List;
import java.util.Optional;

/**
 * hetu metastore
 *
 * @since 2020-02-27
 */
public interface HetuMetastore
{
    /**
     * create catalog
     *
     * @param catalog catalog
     */
    void createCatalog(CatalogEntity catalog);

    /**
     * create catalog if not exist
     *
     * @param catalog catalog
     */
    void createCatalogIfNotExist(CatalogEntity catalog);

    /**
     * alter the catalog entity in hetu metastore,
     * Currently only the owner,type,comment and parameters of the database can be changed.
     *
     * @param catalogName catalog name
     * @param newCatalog  new catalog
     */
    void alterCatalog(String catalogName, CatalogEntity newCatalog);

    /**
     * drop catalog
     *
     * @param catalogName catalog name
     */
    void dropCatalog(String catalogName);

    /**
     * get catalog
     *
     * @param catalogName catalog name
     * @return catalog entity
     */
    Optional<CatalogEntity> getCatalog(String catalogName);

    /**
     * get catalogs
     *
     * @return catalog
     */
    List<CatalogEntity> getCatalogs();

    /**
     * create database
     *
     * @param database database
     */
    void createDatabase(DatabaseEntity database);

    /**
     * create database if not exist
     *
     * @param database database
     */
    void createDatabaseIfNotExist(DatabaseEntity database);

    /**
     * alter the database entity in hetu metastore,
     * Currently only the name,owner,comment and parameters of the database can be changed.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param newDatabase  new database
     */
    void alterDatabase(String catalogName, String databaseName, DatabaseEntity newDatabase);

    /**
     * drop database
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     */
    void dropDatabase(String catalogName, String databaseName);

    /**
     * get database
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @return database
     */
    Optional<DatabaseEntity> getDatabase(String catalogName, String databaseName);

    /**
     * get all databases
     *
     * @param catalogName catalog name
     * @return databases
     */
    List<DatabaseEntity> getAllDatabases(String catalogName);

    /**
     * create table
     *
     * @param table table
     */
    void createTable(TableEntity table);

    /**
     * create table if not exist
     *
     * @param table table
     */
    void createTableIfNotExist(TableEntity table);

    /**
     * drop table
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     */
    void dropTable(String catalogName, String databaseName, String tableName);

    /**
     * alter the table entity in hetu metastore,
     * Currently only the name,owner,comment, viewOriginal, comment parameters
     * and columns of the database can be changed.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param oldTableName old table name
     * @param newTable     new table
     */
    void alterTable(String catalogName, String databaseName, String oldTableName, TableEntity newTable);

    /**
     * get table
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param table        table
     * @return table
     */
    Optional<TableEntity> getTable(String catalogName, String databaseName, String table);

    /**
     * get all table
     *
     * @param catalogName catalog name
     * @param databaseName database name
     * @return tables
     */
    List<TableEntity> getAllTables(String catalogName, String databaseName);

    /**
     * alter catalog parameters in hetu metastore
     *
     * @param catalogName catalog name
     * @param key parameter key to change
     * @param value parameter value to put. If value is {@code null}, the given key will be removed from parameter list
     */
    void alterCatalogParameter(String catalogName, String key, String value);

    /**
     * alter database parameters in hetu metastore
     *
     * @param catalogName catalog name
     * @param databaseName database name
     * @param key parameter key to change
     * @param value parameter value to put. If value is {@code null}, the given key will be removed from parameter list
     */
    void alterDatabaseParameter(String catalogName, String databaseName, String key, String value);

    /**
     * alter table parameters in hetu metastore
     *
     * @param catalogName catalog name
     * @param databaseName database name
     * @param tableName table name
     * @param key parameter key to change
     * @param value parameter value to put. If value is {@code null}, the given key will be removed from parameter list
     */
    void alterTableParameter(String catalogName, String databaseName, String tableName, String key, String value);
}
