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

import org.jdbi.v3.sqlobject.statement.SqlUpdate;

/**
 * metadata table dao
 *
 * @since 2020-03-03
 */
public interface MetadataTableDao
{
    /**
     * create table of hetu_ctlgs
     */
    @SqlUpdate("CREATE TABLE IF NOT EXISTS hetu_ctlgs (\n"
            + " id BIGINT NOT NULL AUTO_INCREMENT,\n"
            + "  catalog_name VARCHAR(256) NOT NULL,\n"
            + "  create_time BIGINT NOT NULL,\n"
            + "  owner VARCHAR(767),\n"
            + "  comment VARCHAR(256),\n"
            + "  PRIMARY KEY (id),\n"
            + "  UNIQUE (catalog_name)\n"
            + ")")
    void createTableCatalogs();

    /**
     * create table of hetu_catalog_params
     */
    @SqlUpdate("CREATE TABLE IF NOT EXISTS hetu_catalog_params (\n"
            + " catalog_id BIGINT NOT NULL,\n"
            + "  param_key varchar(256) NOT NULL,\n"
            + "  param_value mediumtext,\n"
            + "  PRIMARY KEY (catalog_id, param_key),\n"
            + "  FOREIGN KEY (catalog_id) REFERENCES hetu_ctlgs (id)\n"
            + ")")
    void createTableCatalogParameters();

    /**
     * create table of hetu_dbs
     */
    @SqlUpdate("CREATE TABLE IF NOT EXISTS hetu_dbs (\n"
            + " id BIGINT NOT NULL AUTO_INCREMENT,\n"
            + "  catalog_name VARCHAR(256) NOT NULL,\n"
            + "  database_name VARCHAR(256) NOT NULL,\n"
            + "  create_time BIGINT NOT NULL,\n"
            + "  owner VARCHAR(767),\n"
            + "  comment VARCHAR(256),\n"
            + "  PRIMARY KEY (id),\n"
            + "  UNIQUE (catalog_name, database_name),\n"
            + "  FOREIGN KEY (catalog_name) REFERENCES hetu_ctlgs (catalog_name)\n"
            + ")")
    void createTableDatabases();

    /**
     * create table of hetu_database_params
     */
    @SqlUpdate("CREATE TABLE IF NOT EXISTS hetu_database_params (\n"
            + " database_id BIGINT NOT NULL,\n"
            + "  param_key varchar(256) NOT NULL,\n"
            + "  param_value mediumtext,\n"
            + "  PRIMARY KEY (database_id, param_key),\n"
            + "  FOREIGN KEY (database_id) REFERENCES hetu_dbs (id)\n"
            + ")")
    void createTableDatabaseParameters();

    /**
     * create table of hetu_tbls
     */
    @SqlUpdate("CREATE TABLE IF NOT EXISTS hetu_tbls (\n"
            + " id BIGINT NOT NULL AUTO_INCREMENT,\n"
            + "  database_id BIGINT NOT NULL,\n"
            + "  table_name VARCHAR(256) NOT NULL,\n"
            + "  type VARCHAR(128) NOT NULL,\n"
            + "  view_original_text mediumtext,\n"
            + "  create_time BIGINT NOT NULL,\n"
            + "  owner VARCHAR(767),\n"
            + "  comment VARCHAR(4000),\n"
            + "  PRIMARY KEY (id),\n"
            + "  UNIQUE (database_id, table_name),"
            + "FOREIGN KEY (database_id) REFERENCES hetu_dbs (id)\n"
            + ")")
    void createTableTables();

    /**
     * create table of hetu_table_params
     */
    @SqlUpdate("CREATE TABLE IF NOT EXISTS hetu_table_params (\n"
            + "table_id BIGINT,\n"
            + "param_key varchar(256) NOT NULL,\n"
            + "param_value mediumtext,\n"
            + "PRIMARY KEY (table_id, param_key),\n"
            + "FOREIGN KEY (table_id) REFERENCES hetu_tbls (id)\n"
            + ")")
    void createTableTableParameters();

    /**
     * create table of hetu_tab_cols
     */
    @SqlUpdate("CREATE TABLE IF NOT EXISTS hetu_tab_cols (\n"
            + "id BIGINT NOT NULL AUTO_INCREMENT,\n"
            + "table_id BIGINT NOT NULL,\n"
            + "column_name VARCHAR(256) NOT NULL,\n"
            + "type VARCHAR(128) NOT NULL,\n"
            + "comment VARCHAR(4000),\n"
            + "PRIMARY KEY (id),\n"
            + "UNIQUE (table_id, column_name),"
            + "FOREIGN KEY (table_id) REFERENCES hetu_tbls (id)\n"
            + ")")
    void createTableColumns();

    /**
     * create table of hetu_column_params
     */
    @SqlUpdate("CREATE TABLE IF NOT EXISTS hetu_column_params (\n"
            + " column_id BIGINT,\n"
            + " param_key varchar(256) NOT NULL,\n"
            + " param_value mediumtext,\n"
            + " PRIMARY KEY (column_id, param_key),\n"
            + " FOREIGN KEY (column_id) REFERENCES hetu_tab_cols (id)\n"
            + ")")
    void createTableColumnParameters();

    /**
     * create table of lock
     */
    @SqlUpdate("CREATE TABLE IF NOT EXISTS hetu_tab_lock (\n"
            + "id BIGINT NOT NULL AUTO_INCREMENT,\n"
            + "resource INT NOT NULL,\n"
            + "description VARCHAR(128) NOT NULL,\n"
            + "PRIMARY KEY (id),\n"
            + "UNIQUE (resource)\n"
            + ")")
    void createTableLock();
}
