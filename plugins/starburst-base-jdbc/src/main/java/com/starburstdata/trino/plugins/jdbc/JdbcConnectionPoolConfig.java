/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.jdbc;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class JdbcConnectionPoolConfig
{
    private boolean isConnectionPoolEnabled;
    private int maxPoolSize = 10;
    private Duration maxConnectionLifetime = new Duration(30, TimeUnit.MINUTES);
    private Duration poolCacheTtl = new Duration(30, TimeUnit.MINUTES);
    private int poolCacheMaxSize = 1000;

    public boolean isConnectionPoolEnabled()
    {
        return isConnectionPoolEnabled;
    }

    @Config("connection-pool.enabled")
    @ConfigDescription("Enables JDBC connection pooling")
    public JdbcConnectionPoolConfig setConnectionPoolEnabled(boolean isConnectionPoolingEnabled)
    {
        this.isConnectionPoolEnabled = isConnectionPoolingEnabled;
        return this;
    }

    @Min(1)
    public int getMaxPoolSize()
    {
        return maxPoolSize;
    }

    @Config("connection-pool.max-size")
    @ConfigDescription("Maximum size of JDBC connection pool")
    public JdbcConnectionPoolConfig setMaxPoolSize(Integer maxPoolSize)
    {
        this.maxPoolSize = maxPoolSize;
        return this;
    }

    @NotNull
    public Duration getMaxConnectionLifetime()
    {
        return maxConnectionLifetime;
    }

    @Config("connection-pool.max-connection-lifetime")
    public JdbcConnectionPoolConfig setMaxConnectionLifetime(Duration maxConnectionLifetime)
    {
        this.maxConnectionLifetime = maxConnectionLifetime;
        return this;
    }

    @NotNull
    public Duration getPoolCacheTtl()
    {
        return poolCacheTtl;
    }

    @Config("connection-pool.pool-cache-ttl")
    public JdbcConnectionPoolConfig setPoolCacheTtl(Duration poolCacheTtl)
    {
        this.poolCacheTtl = poolCacheTtl;
        return this;
    }

    public int getPoolCacheMaxSize()
    {
        return poolCacheMaxSize;
    }

    @Config("connection-pool.pool-cache-max-size")
    public JdbcConnectionPoolConfig setPoolCacheMaxSize(int poolCacheMaxSize)
    {
        this.poolCacheMaxSize = poolCacheMaxSize;
        return this;
    }
}
