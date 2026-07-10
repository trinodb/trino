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

import io.trino.spi.metastore.model.CatalogEntity;
import io.trino.spi.metastore.model.ColumnEntity;
import io.trino.spi.metastore.model.DatabaseEntity;
import io.trino.spi.metastore.model.TableEntity;
import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlBatch;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.statement.UseRowReducer;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * jdbc metadata dao
 *
 * @since 2020-03-03
 */
@RegisterBeanMapper(value = CatalogEntity.class, prefix = "ct")
@RegisterBeanMapper(value = DatabaseEntity.class, prefix = "d")
@RegisterBeanMapper(value = TableEntity.class, prefix = "t")
@RegisterBeanMapper(value = ColumnEntity.class, prefix = "c")
@RegisterBeanMapper(value = PropertyEntity.class, prefix = "p")

public interface JdbcMetadataDao
{
    /**
     * select all catalog
     */
    String SELECT_ALL_CATALOGS = "SELECT ctlgs.id ct_id, ctlgs.catalog_name ct_name,\n"
            + " ctlgs.create_time ct_createTime, ctlgs.owner ct_owner, ctlgs.comment ct_comment,\n"
            + "  ps.catalog_id p_id, ps.param_key p_key,ps.param_value p_value\n"
            + "FROM hetu_ctlgs ctlgs\n"
            + "LEFT JOIN hetu_catalog_params ps ON ctlgs.id = ps.catalog_id\n";

    /**
     * select all database in a catalog
     */
    String SELECT_ALL_DATABASES = "SELECT dbs.id d_id, dbs.catalog_name d_catalogName,dbs.database_name d_name,\n"
            + " dbs.create_time d_createTime, dbs.owner d_owner,dbs.comment d_comment,\n"
            + "  ps.database_id p_id, ps.param_key p_key,ps.param_value p_value\n"
            + "FROM hetu_dbs dbs\n"
            + "LEFT JOIN hetu_database_params ps ON dbs.id = ps.database_id\n"
            + "WHERE (dbs.catalog_name = :catalogName)\n";

    /**
     * select all table in database
     */
    String SELECT_ALL_TABLES = "SELECT \n"
            + " tbls.id t_id, tbls.table_name t_name, tbls.view_original_text t_viewOriginalText,\n"
            + " tbls.type t_type, tbls.create_time t_createTime, tbls.owner t_owner, tbls.comment t_comment,\n"
            + " dbs.catalog_name t_catalogName, dbs.database_Name t_databaseName,\n"
            + " ps.table_id p_id, ps.param_key p_key,ps.param_value p_value\n"
            + " FROM hetu_tbls tbls\n"
            + " LEFT JOIN hetu_dbs dbs ON dbs.id = tbls.database_id \n"
            + " LEFT JOIN hetu_table_params ps ON tbls.id=ps.table_id\n"
            + " WHERE (dbs.catalog_name = :catalogName)\n"
            + " AND (dbs.database_name = :databaseName)\n";

    /**
     * insert catalog
     *
     * @param catalog catalog
     * @return catalog id
     */
    @SqlUpdate("INSERT INTO hetu_ctlgs (\n"
            + "  catalog_name, create_time, owner, comment)\n"
            + "VALUES (\n"
            + "  :name, :createTime, :owner, :comment)")
    @GetGeneratedKeys
    long insertCatalog(@BindBean CatalogEntity catalog);

    /**
     * insert catalog property
     *
     * @param catalogId  catalogId
     * @param properties property of catalog
     */
    @SqlBatch("INSERT INTO hetu_catalog_params (\n"
            + "  catalog_id, param_key, param_value)\n"
            + "VALUES (\n"
            + "  :catalogId, :key, :value)")
    void insertCatalogProperty(@Bind("catalogId") long catalogId, @BindBean List<PropertyEntity> properties);

    /**
     * get catalog
     *
     * @param catalogName catalog name
     * @return catalog
     */
    @SqlQuery(SELECT_ALL_CATALOGS
            + "WHERE (ctlgs.catalog_name = :catalogName)\n"
            + "ORDER BY ctlgs.id ASC")
    @UseRowReducer(CatalogEntityReducer.class)
    Optional<CatalogEntity> getCatalog(@Bind("catalogName") String catalogName);

    /**
     * get all catalogs
     *
     * @return catalogs
     */
    @SqlQuery(SELECT_ALL_CATALOGS
            + " ORDER BY ctlgs.id ASC")
    @UseRowReducer(CatalogEntityReducer.class)
    List<CatalogEntity> getAllCatalogs();

    /**
     * drop catalog
     *
     * @param catalogName catalog name
     * @return row of dropped
     */
    @SqlUpdate("DELETE FROM hetu_ctlgs WHERE (catalog_name = :catalogName)")
    int dropCatalog(@Bind("catalogName") String catalogName);

    /**
     * drop catalog property
     *
     * @param catalogId catalog id
     * @return row of dropped
     */
    @SqlUpdate("DELETE FROM hetu_catalog_params WHERE catalog_id = :catalogId")
    int dropCatalogProperty(@Bind("catalogId") long catalogId);

    /**
     * catalog
     *
     * @param oldCatalogName old catalog name
     * @param newCatalog     catalog
     */
    @SqlUpdate("UPDATE hetu_ctlgs SET owner = :owner, comment = :comment\n"
            + " WHERE (catalog_name = :oldCatalogName)")
    void alterCatalog(
            @Bind("oldCatalogName") String oldCatalogName,
            @BindBean CatalogEntity newCatalog);

    /**
     * get catalog id
     *
     * @param catalogName catalog name
     * @return database id
     */
    @SqlQuery("SELECT id\n"
            + "FROM hetu_ctlgs\n"
            + "WHERE (catalog_name = :catalogName)")
    Long getCatalogId(@Bind("catalogName") String catalogName);

    /**
     * insert database
     *
     * @param database database
     * @return database id
     */
    @SqlUpdate("INSERT INTO hetu_dbs (\n"
            + "  catalog_name, database_name, create_time, owner, comment)\n"
            + "VALUES (\n"
            + "  :catalogName, :name, :createTime, :owner, :comment)")
    @GetGeneratedKeys
    long insertDatabase(@BindBean DatabaseEntity database);

    /**
     * insert database property
     *
     * @param databaseId databaseId
     * @param properties property of database
     */
    @SqlBatch("INSERT INTO hetu_database_params (\n"
            + "  database_id, param_key, param_value)\n"
            + "VALUES (\n"
            + "  :databaseId, :key, :value)")
    void insertDatabaseProperty(@Bind("databaseId") long databaseId, @BindBean List<PropertyEntity> properties);

    /**
     * get database
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @return database
     */
    @SqlQuery(SELECT_ALL_DATABASES
            + "  AND (dbs.database_name = :databaseName)\n"
            + "ORDER BY dbs.id ASC")
    @UseRowReducer(DatabaseEntityReducer.class)
    Optional<DatabaseEntity> getDatabase(
            @Bind("catalogName") String catalogName,
            @Bind("databaseName") String databaseName);

    /**
     * get all database in a catalog
     *
     * @param catalogName catalog name
     * @return databases
     */
    @SqlQuery(SELECT_ALL_DATABASES
            + "ORDER BY dbs.id ASC")
    @UseRowReducer(DatabaseEntityReducer.class)
    List<DatabaseEntity> getAllDatabases(@Bind("catalogName") String catalogName);

    /**
     * get database id
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @return database id
     */
    @SqlQuery("SELECT id\n"
            + "FROM hetu_dbs\n"
            + "WHERE (catalog_name = :catalogName)\n"
            + "   AND (database_name = :databaseName)")
    Long getDatabaseId(
            @Bind("catalogName") String catalogName,
            @Bind("databaseName") String databaseName);

    /**
     * drop database property
     *
     * @param databaseId database id
     * @return row of dropped
     */
    @SqlUpdate("DELETE FROM hetu_database_params WHERE database_id = :databaseId")
    int dropDatabaseProperty(@Bind("databaseId") long databaseId);

    /**
     * drop database
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @return row of dropped
     */
    @SqlUpdate("DELETE FROM hetu_dbs WHERE (catalog_name = :catalogName) AND (database_name = :databaseName)")
    int dropDatabase(@Bind("catalogName") String catalogName, @Bind("databaseName") String databaseName);

    /**
     * alter database
     *
     * @param catalogName     catalog name
     * @param oldDatabaseName old database name
     * @param database        database
     * @return row of updating
     */
    @SqlUpdate("UPDATE hetu_dbs SET\n"
            + " database_name = :name,\n"
            + " owner = :owner, comment = :comment\n"
            + "WHERE (catalog_name = :catalogName) AND (database_name = :oldDatabaseName)")
    int alterDatabase(
            @Bind("catalogName") String catalogName,
            @Bind("oldDatabaseName") String oldDatabaseName,
            @BindBean DatabaseEntity database);

    /**
     * insert table
     *
     * @param databaseId database id
     * @param table      table
     * @return table id
     */
    @SqlUpdate("INSERT INTO hetu_tbls (\n"
            + "  database_id, table_name, type, view_original_text, create_time, owner, comment)\n"
            + "VALUES (\n"
            + "  :databaseId, :name, :type, :viewOriginalText, :createTime, :owner, :comment)")
    @GetGeneratedKeys
    long insertTable(@Bind("databaseId") long databaseId, @BindBean TableEntity table);

    /**
     * insert table property
     *
     * @param tableId    table id
     * @param properties properties of table
     */
    @SqlBatch("INSERT INTO hetu_table_params (\n"
            + "  table_id, param_key, param_value)\n"
            + "VALUES (\n"
            + "  :tableId, :key, :value)")
    void insertTableProperty(@Bind("tableId") long tableId, @BindBean List<PropertyEntity> properties);

    /**
     * insert column
     *
     * @param tableId table id
     * @param column  column
     * @return column id
     */
    @SqlUpdate("INSERT INTO hetu_tab_cols (\n"
            + "  table_id, column_name, type, comment)\n"
            + "VALUES (\n"
            + "  :tableId, :name, :type, :comment)")
    @GetGeneratedKeys
    long insertColumn(@Bind("tableId") long tableId, @BindBean ColumnEntity column);

    /**
     * insert column property
     *
     * @param columnId   column id
     * @param properties properties
     */
    @SqlBatch("INSERT INTO hetu_column_params (\n"
            + "  column_id, param_key, param_value)\n"
            + "VALUES (\n"
            + "  :columnId, :key, :value)")
    void insertColumnProperty(@Bind("columnId") long columnId, @BindBean List<PropertyEntity> properties);

    /**
     * get table
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @return table
     */
    @SqlQuery(SELECT_ALL_TABLES
            + " AND (tbls.table_name = :tableName)\n"
            + " ORDER BY tbls.id ASC")
    @UseRowReducer(TableEntityReducer.class)
    List<Map.Entry<Long, TableEntity>> getTable(
            @Bind("catalogName") String catalogName,
            @Bind("databaseName") String databaseName,
            @Bind("tableName") String tableName);

    /**
     * get all tables
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @return tables
     */
    @SqlQuery(SELECT_ALL_TABLES
            + " ORDER BY tbls.id ASC")
    @UseRowReducer(TableEntityReducer.class)
    List<Map.Entry<Long, TableEntity>> getAllTables(
            @Bind("catalogName") String catalogName,
            @Bind("databaseName") String databaseName);

    /**
     * get all columns of table
     *
     * @param tableId table id
     * @return columns
     */
    @SqlQuery("SELECT cols.table_id c_tid, cols.id c_id, cols.type c_type,"
            + "cols.column_name c_name, cols.comment c_comment,\n"
            + "ps.column_id p_id, ps.param_key p_key,ps.param_value p_value\n"
            + "FROM hetu_tab_cols cols\n"
            + "LEFT JOIN hetu_column_params ps ON cols.id=ps.column_id \n"
            + "WHERE (cols.table_id = :tableId)\n"
            + "ORDER BY cols.table_id,cols.id ASC")
    @UseRowReducer(ColumnEntityReducer.class)
    List<ColumnEntity> getAllColumns(@Bind("tableId") long tableId);

    /**
     * get table id
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @return table id
     */
    @SqlQuery("SELECT id\n"
            + "FROM hetu_tbls\n"
            + "WHERE database_id IN (\n"
            + " SELECT id\n"
            + " FROM hetu_dbs\n"
            + " WHERE(catalog_name = :catalogName)\n"
            + "  AND (database_name = :databaseName))\n"
            + "AND (table_name = :tableName)")
    Long getTableId(
            @Bind("catalogName") String catalogName,
            @Bind("databaseName") String databaseName,
            @Bind("tableName") String tableName);

    /**
     * drop table
     *
     * @param tableId table id
     * @return row dropped
     */
    @SqlUpdate("DELETE FROM hetu_tbls WHERE id = :tableId")
    int dropTable(@Bind("tableId") long tableId);

    /**
     * drop table property
     *
     * @param tableId table id
     * @return row dropped
     */
    @SqlUpdate("DELETE FROM hetu_table_params WHERE table_id = :tableId")
    int dropTableProperty(@Bind("tableId") long tableId);

    /**
     * alter table
     *
     * @param tableId  table id
     * @param newTable table
     * @return row update
     */
    @SqlUpdate("UPDATE hetu_tbls SET\n"
            + "table_name = :name, view_original_text = :viewOriginalText,\n"
            + "owner = :owner, comment = :comment\n"
            + "WHERE (id = :tableId)")
    int alterTable(@Bind("tableId") long tableId, @BindBean TableEntity newTable);

    /**
     * get column id
     *
     * @param tableId table id
     * @return column ids
     */
    @SqlQuery("SELECT id FROM hetu_tab_cols WHERE (table_id = :tableId)")
    List<Long> getColumnId(@Bind("tableId") long tableId);

    /**
     * column id
     *
     * @param columnId column id
     * @return row dropped
     */
    @SqlUpdate("DELETE FROM hetu_column_params WHERE column_id = :columnId")
    int dropColumnProperty(@Bind("columnId") long columnId);

    /**
     * drop column
     *
     * @param tableId table id
     * @return row dropped
     */
    @SqlUpdate("DELETE FROM hetu_tab_cols WHERE table_id = :tableId")
    int dropColumn(@Bind("tableId") long tableId);

    /**
     * get lock
     */
    @SqlUpdate("INSERT INTO hetu_tab_lock (\n"
            + "  resource, description)\n"
            + "VALUES (\n"
            + "  1, 'lock')")
    void tryLock();

    /**
     * release lock
     */
    @SqlUpdate("DELETE FROM hetu_tab_lock WHERE resource=1")
    void releaseLock();

    /**
     * get lock id
     *
     * @return lock id
     */
    @SqlQuery("SELECT id FROM hetu_tab_lock WHERE resource=1")
    Long getLockId();
}
