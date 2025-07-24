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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorName;

import javax.sql.DataSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static io.trino.spi.connector.CatalogHandle.createRootCatalogHandle;
import static java.util.Objects.requireNonNull;

/**
 * Generic JDBC implementation of {@link CatalogStore}. It relies entirely on a {@link JdbcSchemaMapping}
 * provided by the concrete plugin to perform all SQL.
 */
public class JdbcCatalogStore
        implements CatalogStore
{
    private final DataSource dataSource;
    private final JdbcSchemaMapping schemaMapping;
    private final AtomicLong versionCounter = new AtomicLong(System.currentTimeMillis());

    public JdbcCatalogStore(DataSource dataSource, JdbcSchemaMapping schemaMapping)
    {
        this.dataSource = requireNonNull(dataSource, "dataSource is null");
        this.schemaMapping = requireNonNull(schemaMapping, "schemaMapping is null");
        initializeDatabase();
    }

    private void initializeDatabase()
    {
        try (Connection connection = dataSource.getConnection()) {
            for (String statement : schemaMapping.getTableCreationStatements()) {
                try (Statement stmt = connection.createStatement()) {
                    stmt.execute(statement);
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Error initializing catalog database", e);
        }
    }

    @Override
    public CatalogProperties createCatalogProperties(CatalogName catalogName, ConnectorName connectorName, Map<String, String> properties)
    {
        String version = String.valueOf(versionCounter.incrementAndGet());
        CatalogHandle handle = createRootCatalogHandle(catalogName, new CatalogHandle.CatalogVersion(version));
        Map<String, String> transformed = schemaMapping.transformPropertiesForStorage(properties);
        return new CatalogProperties(handle, connectorName, ImmutableMap.copyOf(transformed));
    }

    @Override
    public void addOrReplaceCatalog(CatalogProperties catalogProperties)
    {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(schemaMapping.getUpsertCatalogSql())) {
            schemaMapping.bindCatalogParameters(statement, catalogProperties);
            statement.executeUpdate();
        }
        catch (SQLException e) {
            throw new RuntimeException("Error storing catalog", e);
        }
    }

    @Override
    public void removeCatalog(CatalogName catalogName)
    {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(schemaMapping.getDeleteCatalogSql())) {
            statement.setString(1, catalogName.toString());
            statement.executeUpdate();
        }
        catch (SQLException e) {
            throw new RuntimeException("Error removing catalog", e);
        }
    }

    @Override
    public Collection<StoredCatalog> getCatalogs()
    {
        List<StoredCatalog> catalogs = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(schemaMapping.getSelectCatalogsSql())) {
            while (resultSet.next()) {
                catalogs.add(new JdbcStoredCatalog(schemaMapping.extractCatalogData(resultSet)));
            }
        }
        catch (SQLException | IOException e) {
            throw new RuntimeException("Error loading catalogs", e);
        }
        return catalogs;
    }

    public StoredCatalog getCatalog(CatalogName catalogName)
    {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(schemaMapping.getSelectCatalogByNameSql())) {
            statement.setString(1, catalogName.toString());
            try (ResultSet rs = statement.executeQuery()) {
                if (rs.next()) {
                    return new JdbcStoredCatalog(schemaMapping.extractCatalogData(rs));
                }
                return null;
            }
        }
        catch (SQLException | IOException e) {
            throw new RuntimeException("Error loading catalog", e);
        }
    }

    private static class JdbcStoredCatalog
            implements StoredCatalog
    {
        private final JdbcCatalogData data;

        JdbcStoredCatalog(JdbcCatalogData data)
        {
            this.data = requireNonNull(data, "data is null");
        }

        @Override
        public CatalogName name()
        {
            return new CatalogName(data.catalogName);
        }

        @Override
        public CatalogProperties loadProperties()
        {
            CatalogHandle handle = createRootCatalogHandle(
                    new CatalogName(data.catalogName),
                    new CatalogHandle.CatalogVersion(data.versionIdentifier));
            return new CatalogProperties(handle, new ConnectorName(data.connectorName), ImmutableMap.copyOf(data.properties));
        }
    }
}
