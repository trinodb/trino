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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.airlift.units.Duration;
import io.trino.cache.NonKeyEvictableCache;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ForwardingConnection;
import io.trino.plugin.jdbc.IdentityCacheMapping;
import io.trino.plugin.jdbc.IdentityCacheMapping.IdentityCacheKey;
import io.trino.plugin.jdbc.credential.CredentialPropertiesProvider;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.DefaultCredentialPropertiesProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static io.trino.cache.SafeCaches.buildNonEvictableCacheWithWeakInvalidateAll;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class PoolingConnectionFactory
        implements ConnectionFactory
{
    private final String catalogName;
    private final String connectionUrl;
    private final Class<? extends Driver> driverClass;
    private final Map<String, String> connectionProperties;
    private final Duration maxConnectionLifetime;
    private final int maxPoolSize;
    private final CredentialPropertiesProvider<String, String> credentialPropertiesProvider;
    private final NonKeyEvictableCache<IdentityCacheKey, HikariDataSource> dataSourceCache;
    private final Queue<HikariDataSource> evictedDataSources = new ConcurrentLinkedQueue<>();
    private final ScheduledExecutorService executorService = newSingleThreadScheduledExecutor();
    private final IdentityCacheMapping identityCacheMapping;

    public PoolingConnectionFactory(
            String catalogName,
            Class<? extends Driver> driverClass,
            BaseJdbcConfig config,
            JdbcConnectionPoolConfig poolConfig,
            CredentialProvider credentialProvider,
            IdentityCacheMapping identityCacheMapping)
    {
        this(catalogName, driverClass, new Properties(), config, poolConfig, new DefaultCredentialPropertiesProvider(credentialProvider), identityCacheMapping);
    }

    public PoolingConnectionFactory(
            String catalogName,
            Class<? extends Driver> driverClass,
            Properties properties,
            BaseJdbcConfig config,
            JdbcConnectionPoolConfig poolConfig,
            CredentialProvider credentialProvider,
            IdentityCacheMapping identityCacheMapping)
    {
        this(catalogName, driverClass, properties, config, poolConfig, new DefaultCredentialPropertiesProvider(credentialProvider), identityCacheMapping);
    }

    public PoolingConnectionFactory(
            String catalogName,
            Class<? extends Driver> driverClass,
            Properties connectionProperties,
            BaseJdbcConfig config,
            JdbcConnectionPoolConfig poolConfig,
            CredentialPropertiesProvider<String, String> credentialPropertiesProvider,
            IdentityCacheMapping identityCacheMapping)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.connectionUrl = requireNonNull(config, "config is null").getConnectionUrl();
        this.driverClass = requireNonNull(driverClass, "driverClass is null");
        this.connectionProperties = Maps.fromProperties(requireNonNull(connectionProperties, "connectionProperties is null"));
        this.maxConnectionLifetime = requireNonNull(poolConfig, "poolConfig is null").getMaxConnectionLifetime();
        this.maxPoolSize = requireNonNull(poolConfig, "poolConfig is null").getMaxPoolSize();
        this.credentialPropertiesProvider = requireNonNull(credentialPropertiesProvider, "credentialPropertiesProvider is null");
        this.identityCacheMapping = requireNonNull(identityCacheMapping, "identityCacheMapping is null");
        this.dataSourceCache = buildNonEvictableCacheWithWeakInvalidateAll(
                CacheBuilder.newBuilder()
                        .expireAfterAccess(poolConfig.getPoolCacheTtl().toMillis(), MILLISECONDS)
                        .maximumSize(poolConfig.getPoolCacheMaxSize())
                        .removalListener(notification -> evictedDataSources.add(notification.getValue())));
        executorService.scheduleAtFixedRate(this::cleanupEvictedDataSources, 0, 5, SECONDS);
    }

    private HikariDataSource createHikariDataSource(Map<String, String> properties)
    {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDriverClassName(driverClass.getName());
        hikariConfig.setJdbcUrl(connectionUrl);
        hikariConfig.setRegisterMbeans(true);
        hikariConfig.setPoolName(catalogName);
        hikariConfig.setMaximumPoolSize(maxPoolSize);
        hikariConfig.setMaxLifetime(maxConnectionLifetime.toMillis());
        hikariConfig.setDataSourceProperties(toProperties(properties));
        return new HikariDataSource(hikariConfig);
    }

    private void cleanupEvictedDataSources()
    {
        dataSourceCache.cleanUp();
        for (Iterator<HikariDataSource> dataSourceIterator = evictedDataSources.iterator(); dataSourceIterator.hasNext(); ) {
            HikariDataSource dataSource = dataSourceIterator.next();
            // prevent race condition on data source between cleanup and openConnection
            // noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (dataSource) {
                if (dataSource.getHikariPoolMXBean().getActiveConnections() == 0) {
                    dataSource.close();
                    dataSourceIterator.remove();
                }
            }
        }
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        try {
            while (true) {
                HikariDataSource dataSource = dataSourceCache.get(
                        identityCacheMapping.getRemoteUserCacheKey(session),
                        () -> createHikariDataSource(getConnectionProperties(session)));
                // prevent race condition on data source between cleanup and openConnection
                // noinspection SynchronizationOnLocalVariableOrMethodParameter
                synchronized (dataSource) {
                    if (dataSource.isClosed()) {
                        // data source has been evicted and closed, repeat
                        continue;
                    }

                    Connection connection = dataSource.getConnection();
                    verify(connection.getAutoCommit(), "autoCommit must be enabled");
                    // we call Connection#abort to prevent some drivers from draining any remaining records,
                    // and Hikari does not handle it correctly, so we have to:
                    return wrapConnection(connection, dataSource);
                }
            }
        }
        catch (ExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new UncheckedExecutionException(e);
        }
    }

    protected Connection wrapConnection(Connection connection, HikariDataSource dataSource)
    {
        return new ForwardingConnection() {
            @Override
            protected Connection delegate()
            {
                return connection;
            }

            @Override
            public void abort(Executor executor)
                    throws SQLException
            {
                super.abort(executor);
                // aborted connections are implicitly closed, so we have to remove them from the pool
                dataSource.evictConnection(connection);
            }
        };
    }

    protected Map<String, String> getConnectionProperties(ConnectorSession session)
    {
        return ImmutableMap.<String, String>builder()
                .putAll(connectionProperties)
                .putAll(credentialPropertiesProvider.getCredentialProperties(session.getIdentity()))
                .buildOrThrow();
    }

    @VisibleForTesting
    int getEvictedDataSourcesSize()
    {
        return evictedDataSources.size();
    }

    @Override
    public void close()
    {
        dataSourceCache.invalidateAll();
        executorService.shutdownNow();
    }

    private static Properties toProperties(Map<String, String> map)
    {
        Properties properties = new Properties();
        properties.putAll(requireNonNull(map, "map is null"));
        return properties;
    }
}
