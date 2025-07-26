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
package io.trino.plugin.base.catalog;

import io.trino.spi.catalog.CatalogProperties;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Defines how catalog information is mapped to a backing SQL store for a concrete implementation
 * of {@link JdbcCatalogStore}.
 */
public interface JdbcSchemaMapping
{
    /**
     * SQL required to bring the target database to an operable state. This is executed
     * at plugin start-up (e.g., use «CREATE IF NOT EXISTS»).
     */
    List<String> getTableCreationStatements();

    /**
     * SQL used to upsert a catalog
     *
     * @return string SQL statement used to upsert a catalog
     */
    String getUpsertCatalogSql();

    /**
     * SQL used to delete a catalog
     *
     * @return string SQL statement used to delete a catalog
     */
    String getDeleteCatalogSql();

    /**
     * SQL used to select all catalogs
     *
     * @return string SQL statement used to select all catalogs
     */
    String getSelectCatalogsSql();

    /**
     * The column used to identify a catalog
     *
     * @return column name
     */
    String getCatalogNameColumn();

    /**
     * SQL used to select a catalog by name
     *
     * @return string SQL statement used to select a catalog
     */
    default String getSelectCatalogByNameSql()
    {
        return getSelectCatalogsSql() + " WHERE " + getCatalogNameColumn() + " = ?";
    }

    /** Convert a {@link ResultSet} row to neutral {@link JdbcCatalogData}. */
    JdbcCatalogData extractCatalogData(ResultSet rs)
            throws SQLException, IOException;

    /** Bind parameters for the upsert statement. */
    void bindCatalogParameters(PreparedStatement statement, CatalogProperties catalogProperties)
            throws SQLException;

    /** Optional transformation before persistence (e.g., encryption). */
    default Map<String, String> transformPropertiesForStorage(Map<String, String> properties)
    {
        return properties;
    }

    /** Optional transformation after retrieval (e.g., decryption). */
    default Map<String, String> transformPropertiesFromStorage(Map<String, String> properties)
    {
        return properties;
    }
}
