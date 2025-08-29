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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import jakarta.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class MySqlCatalogStoreConfig
{
    private String jdbcUrl;
    private String username;
    private String password = "";
    private int maxPoolSize = 10;
    private int minIdle = 2;
    private Duration connectionTimeout = new Duration(30, TimeUnit.SECONDS);
    private Duration idleTimeout = new Duration(10, TimeUnit.MINUTES);
    private Duration maxLifetime = new Duration(30, TimeUnit.MINUTES);

    @NotNull
    public String getJdbcUrl()
    {
        return jdbcUrl;
    }

    @Config("catalog-store.mysql.jdbc-url")
    @ConfigDescription("MySQL JDBC URL for catalog store")
    public MySqlCatalogStoreConfig setJdbcUrl(String jdbcUrl)
    {
        this.jdbcUrl = jdbcUrl;
        return this;
    }

    @NotNull
    public String getUsername()
    {
        return username;
    }

    @Config("catalog-store.mysql.username")
    @ConfigDescription("MySQL username for catalog store")
    public MySqlCatalogStoreConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    @NotNull
    public String getPassword()
    {
        return password;
    }

    @Config("catalog-store.mysql.password")
    @ConfigDescription("MySQL password for catalog store")
    public MySqlCatalogStoreConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    public int getMaxPoolSize()
    {
        return maxPoolSize;
    }

    @Config("catalog-store.mysql.max-pool-size")
    @ConfigDescription("Maximum number of connections in the connection pool")
    public MySqlCatalogStoreConfig setMaxPoolSize(int maxPoolSize)
    {
        this.maxPoolSize = maxPoolSize;
        return this;
    }

    public int getMinIdle()
    {
        return minIdle;
    }

    @Config("catalog-store.mysql.min-idle")
    @ConfigDescription("Minimum number of idle connections in the connection pool")
    public MySqlCatalogStoreConfig setMinIdle(int minIdle)
    {
        this.minIdle = minIdle;
        return this;
    }

    @NotNull
    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("catalog-store.mysql.connection-timeout")
    @ConfigDescription("Maximum time to wait for a connection from the pool")
    public MySqlCatalogStoreConfig setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    @NotNull
    public Duration getIdleTimeout()
    {
        return idleTimeout;
    }

    @Config("catalog-store.mysql.idle-timeout")
    @ConfigDescription("Maximum time a connection can remain idle in the pool")
    public MySqlCatalogStoreConfig setIdleTimeout(Duration idleTimeout)
    {
        this.idleTimeout = idleTimeout;
        return this;
    }

    @NotNull
    public Duration getMaxLifetime()
    {
        return maxLifetime;
    }

    @Config("catalog-store.mysql.max-lifetime")
    @ConfigDescription("Maximum lifetime of a connection in the pool")
    public MySqlCatalogStoreConfig setMaxLifetime(Duration maxLifetime)
    {
        this.maxLifetime = maxLifetime;
        return this;
    }
}
