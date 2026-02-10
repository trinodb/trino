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
package io.trino.connector;

import org.jdbi.v3.core.mapper.reflect.ColumnName;
import org.jdbi.v3.sqlobject.config.RegisterConstructorMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;

@RegisterConstructorMapper(JdbcCatalogStoreDao.CatalogRecord.class)
public interface JdbcCatalogStoreDao {
    @SqlUpdate("""
            CREATE TABLE IF NOT EXISTS catalogs (
                catalog_name VARCHAR(256) NOT NULL PRIMARY KEY,
                connector_name VARCHAR(256) NOT NULL,
                properties TEXT NOT NULL
            )
            """)
    void createCatalogsTable();

    @SqlQuery("SELECT catalog_name, connector_name, properties FROM catalogs")
    List<CatalogRecord> getCatalogs();

    @SqlQuery("SELECT catalog_name, connector_name, properties FROM catalogs WHERE catalog_name = :catalogName")
    CatalogRecord getCatalog(@Bind("catalogName") String catalogName);

    @SqlUpdate("""
            INSERT INTO catalogs (catalog_name, connector_name, properties)
            VALUES (:catalogName, :connectorName, :properties)
            """)
    void insertCatalog(
            @Bind("catalogName") String catalogName,
            @Bind("connectorName") String connectorName,
            @Bind("properties") String properties);

    @SqlUpdate("""
            UPDATE catalogs
            SET connector_name = :connectorName, properties = :properties
            WHERE catalog_name = :catalogName
            """)
    void updateCatalog(
            @Bind("catalogName") String catalogName,
            @Bind("connectorName") String connectorName,
            @Bind("properties") String properties);

    @SqlUpdate("DELETE FROM catalogs WHERE catalog_name = :catalogName")
    void deleteCatalog(@Bind("catalogName") String catalogName);

    record CatalogRecord(
            @ColumnName("catalog_name") String catalogName,
            @ColumnName("connector_name") String connectorName,
            @ColumnName("properties") String properties) {
    }
}
