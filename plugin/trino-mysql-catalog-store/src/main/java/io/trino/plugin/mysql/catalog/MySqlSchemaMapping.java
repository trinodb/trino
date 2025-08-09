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
package io.trino.plugin.mysql.catalog;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.base.catalog.JdbcCatalogData;
import io.trino.plugin.base.catalog.JdbcSchemaMapping;
import io.trino.plugin.base.util.JsonUtils;
import io.trino.spi.catalog.CatalogProperties;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MySqlSchemaMapping
        implements JdbcSchemaMapping
{
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public List<String> getTableCreationStatements()
    {
        return List.of(
                """
                CREATE TABLE IF NOT EXISTS connectors (
                    connector_name VARCHAR(255) PRIMARY KEY,
                    properties_schema JSON NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    modified_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )""",
                """
                CREATE TABLE IF NOT EXISTS catalog_configurations (
                    catalog_name VARCHAR(255) NOT NULL,
                    version_identifier VARCHAR(255) NOT NULL,
                    org_id VARCHAR(255),
                    catalog_config JSON,
                    connector_name VARCHAR(255),
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    modified_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (catalog_name, version_identifier),
                    FOREIGN KEY (connector_name) REFERENCES connectors(connector_name)
                )"""
        );
    }

    @Override
    public String getUpsertCatalogSql()
    {
        return """
               INSERT INTO catalog_configurations (catalog_name, version_identifier, catalog_config, connector_name)
               VALUES (?, ?, ?, ?)
               ON DUPLICATE KEY UPDATE
                   catalog_config = VALUES(catalog_config),
                   connector_name = VALUES(connector_name),
                   modified_at = CURRENT_TIMESTAMP
               """;
    }

    @Override
    public String getDeleteCatalogSql()
    {
        return "DELETE FROM catalog_configurations WHERE catalog_name = ?";
    }

    @Override
    public String getSelectCatalogsSql()
    {
        return "SELECT catalog_name, version_identifier, catalog_config, connector_name FROM catalog_configurations";
    }

    @Override
    public String getCatalogNameColumn()
    {
        return "catalog_name";
    }

    @Override
    public JdbcCatalogData extractCatalogData(ResultSet rs)
            throws SQLException, IOException
    {
        String catalogName = rs.getString("catalog_name");
        String versionIdentifier = rs.getString("version_identifier");
        String catalogConfig = rs.getString("catalog_config");
        String connectorName = rs.getString("connector_name");

        Map<String, String> properties = fromJson(catalogConfig);
        // Remove metadata key if present
        properties = new HashMap<>(properties);
        properties.remove("connector.name");

        return new JdbcCatalogData(catalogName, versionIdentifier, connectorName, properties);
    }

    @Override
    public void bindCatalogParameters(PreparedStatement statement, CatalogProperties catalogProperties)
            throws SQLException
    {
        String catalogName = catalogProperties.catalogHandle().getCatalogName().toString();
        String versionId = catalogProperties.catalogHandle().getVersion().toString();
        String connectorName = catalogProperties.connectorName().toString();

        String catalogConfig = toJson(catalogProperties.properties());

        statement.setString(1, catalogName);
        statement.setString(2, versionId);
        statement.setString(3, catalogConfig);
        statement.setString(4, connectorName);
    }

    private Map<String, String> fromJson(String json)
            throws IOException
    {
        if (json == null || json.trim().isEmpty()) {
            return ImmutableMap.of();
        }

        try {
            // Use Trino's JsonUtils to parse JSON safely
            Map<String, Object> rawMap = JsonUtils.parseJson(json, Map.class);
            Map<String, String> properties = new HashMap<>();
            
            for (Map.Entry<String, Object> entry : rawMap.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                properties.put(key, value != null ? value.toString() : null);
            }
            
            return ImmutableMap.copyOf(properties);
        }
        catch (Exception e) {
            throw new IOException("Failed to parse JSON: " + json, e);
        }
    }

    private String toJson(Map<String, String> properties)
    {
        try {
            return objectMapper.writeValueAsString(properties);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to serialize properties to JSON", e);
        }
    }
}
