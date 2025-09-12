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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.trino.plugin.base.catalog.JdbcCatalogConfig;
import io.trino.plugin.base.catalog.JdbcCatalogStore;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.catalog.CatalogStoreFactory;

import javax.sql.DataSource;

import java.util.Map;

public class MySqlCatalogStoreFactory
        implements CatalogStoreFactory
{
    @Override
    public String getName()
    {
        return "mysql";
    }

    @Override
    public CatalogStore create(Map<String, String> config)
    {
        JdbcCatalogConfig catalogConfig = createConfig(config);
        DataSource dataSource = createDataSource(catalogConfig);
        MySqlSchemaMapping schemaMapping = new MySqlSchemaMapping();

        return new JdbcCatalogStore(dataSource, schemaMapping);
    }

    private JdbcCatalogConfig createConfig(Map<String, String> properties)
    {
        JdbcCatalogConfig.Builder builder = JdbcCatalogConfig.builder();

        String jdbcUrl = properties.get("catalog-store.mysql.jdbc-url");
        if (jdbcUrl != null) {
            builder.jdbcUrl(jdbcUrl);
        }

        String username = properties.get("catalog-store.mysql.username");
        if (username != null) {
            builder.username(username);
        }

        String password = properties.get("catalog-store.mysql.password");
        if (password != null) {
            builder.password(password);
        }

        String maxPoolSize = properties.get("catalog-store.mysql.max-pool-size");
        if (maxPoolSize != null) {
            builder.maxPoolSize(Integer.parseInt(maxPoolSize));
        }

        String minIdle = properties.get("catalog-store.mysql.min-idle");
        if (minIdle != null) {
            builder.minIdle(Integer.parseInt(minIdle));
        }

        String connectionTimeout = properties.get("catalog-store.mysql.connection-timeout");
        if (connectionTimeout != null) {
            builder.connectionTimeout(parseDurationToMillis(connectionTimeout));
        }

        String idleTimeout = properties.get("catalog-store.mysql.idle-timeout");
        if (idleTimeout != null) {
            builder.idleTimeout(parseDurationToMillis(idleTimeout));
        }

        String maxLifetime = properties.get("catalog-store.mysql.max-lifetime");
        if (maxLifetime != null) {
            builder.maxLifetime(parseDurationToMillis(maxLifetime));
        }

        return builder.build();
    }

    /**
     * Hook for subclasses to customise the connection-pool settings.
     */
    protected DataSource createDataSource(JdbcCatalogConfig config)
    {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.getJdbcUrl());
        hikariConfig.setUsername(config.getUsername());
        hikariConfig.setPassword(config.getPassword());
        hikariConfig.setMaximumPoolSize(config.getMaxPoolSize());
        hikariConfig.setMinimumIdle(config.getMinIdle());
        hikariConfig.setConnectionTimeout(config.getConnectionTimeout());
        hikariConfig.setIdleTimeout(config.getIdleTimeout());
        hikariConfig.setMaxLifetime(config.getMaxLifetime());

        return new HikariDataSource(hikariConfig);
    }

    private long parseDurationToMillis(String duration)
    {
        // Simple parser for duration strings like "30s", "10m", etc.
        if (duration.endsWith("ms")) {
            return Long.parseLong(duration.substring(0, duration.length() - 2));
        }
        else if (duration.endsWith("s")) {
            return Long.parseLong(duration.substring(0, duration.length() - 1)) * 1000;
        }
        else if (duration.endsWith("m")) {
            return Long.parseLong(duration.substring(0, duration.length() - 1)) * 60 * 1000;
        }
        else {
            // Default to milliseconds
            return Long.parseLong(duration);
        }
    }
}
