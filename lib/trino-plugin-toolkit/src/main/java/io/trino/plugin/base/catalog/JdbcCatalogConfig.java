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

import static java.util.Objects.requireNonNull;

/**
 * Immutable configuration for JDBC-based catalog stores. Use the builder for convenience.
 */
public final class JdbcCatalogConfig
{
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final long refreshInterval;
    private final int maxPoolSize;
    private final int minIdle;
    private final long connectionTimeout;
    private final long idleTimeout;
    private final long maxLifetime;

    private JdbcCatalogConfig(Builder builder)
    {
        this.jdbcUrl = requireNonNull(builder.jdbcUrl, "jdbcUrl is null");
        this.username = requireNonNull(builder.username, "username is null");
        this.password = builder.password == null ? "" : builder.password;
        this.refreshInterval = builder.refreshInterval;
        this.maxPoolSize = builder.maxPoolSize;
        this.minIdle = builder.minIdle;
        this.connectionTimeout = builder.connectionTimeout;
        this.idleTimeout = builder.idleTimeout;
        this.maxLifetime = builder.maxLifetime;
    }

    public String getJdbcUrl()
    {
        return jdbcUrl;
    }

    public String getUsername()
    {
        return username;
    }

    public String getPassword()
    {
        return password;
    }

    public long getRefreshInterval()
    {
        return refreshInterval;
    }

    public int getMaxPoolSize()
    {
        return maxPoolSize;
    }

    public int getMinIdle()
    {
        return minIdle;
    }

    public long getConnectionTimeout()
    {
        return connectionTimeout;
    }

    public long getIdleTimeout()
    {
        return idleTimeout;
    }

    public long getMaxLifetime()
    {
        return maxLifetime;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String jdbcUrl;
        private String username;
        private String password;
        private long refreshInterval = 30_000L;
        private int maxPoolSize = 10;
        private int minIdle = 2;
        private long connectionTimeout = 30_000L;
        private long idleTimeout = 600_000L;
        private long maxLifetime = 1_800_000L;

        public Builder jdbcUrl(String jdbcUrl)
        {
            this.jdbcUrl = jdbcUrl;
            return this;
        }

        public Builder username(String username)
        {
            this.username = username;
            return this;
        }

        public Builder password(String password)
        {
            this.password = password;
            return this;
        }

        public Builder refreshInterval(long refreshInterval)
        {
            this.refreshInterval = refreshInterval;
            return this;
        }

        public Builder maxPoolSize(int maxPoolSize)
        {
            this.maxPoolSize = maxPoolSize;
            return this;
        }

        public Builder minIdle(int minIdle)
        {
            this.minIdle = minIdle;
            return this;
        }

        public Builder connectionTimeout(long connectionTimeout)
        {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        public Builder idleTimeout(long idleTimeout)
        {
            this.idleTimeout = idleTimeout;
            return this;
        }

        public Builder maxLifetime(long maxLifetime)
        {
            this.maxLifetime = maxLifetime;
            return this;
        }

        public JdbcCatalogConfig build()
        {
            return new JdbcCatalogConfig(this);
        }
    }
}
