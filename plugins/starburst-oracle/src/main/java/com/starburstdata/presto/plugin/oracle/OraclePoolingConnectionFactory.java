/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import io.airlift.log.Logger;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.credential.CredentialProvider;
import io.prestosql.spi.PrestoException;
import oracle.jdbc.pool.OracleDataSource;
import oracle.ucp.UniversalConnectionPoolException;
import oracle.ucp.admin.UniversalConnectionPoolManager;
import oracle.ucp.admin.UniversalConnectionPoolManagerImpl;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.util.Locale.ENGLISH;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.SECONDS;

public class OraclePoolingConnectionFactory
        implements ConnectionFactory
{
    private static final Logger log = Logger.get(OraclePoolingConnectionFactory.class);

    private final UniversalConnectionPoolManager poolManager;
    private final PoolDataSource dataSource;

    public OraclePoolingConnectionFactory(String catalogName, BaseJdbcConfig config, Properties properties, Optional<CredentialProvider> credentialProvider, OracleConnectionPoolingConfig poolingConfig)
    {
        try {
            poolManager = UniversalConnectionPoolManagerImpl.getUniversalConnectionPoolManager();
            poolManager.setJmxEnabled(true);
            dataSource = PoolDataSourceFactory.getPoolDataSource();
            // generate a unique pool name so that we avoid clashes when tests instantiate a connector
            // for the same catalog more than once (e.g. when running multiple nodes)
            dataSource.setConnectionPoolName(catalogName + "_" + randomUuidValue());
            dataSource.setConnectionFactoryClassName(OracleDataSource.class.getName());
            dataSource.setURL(config.getConnectionUrl());
            dataSource.setConnectionProperties(properties);
            dataSource.setMaxPoolSize(poolingConfig.getMaxPoolSize());
            dataSource.setMinPoolSize(poolingConfig.getMinPoolSize());
            dataSource.setInactiveConnectionTimeout((int) poolingConfig.getInactiveConnectionTimeout().roundTo(SECONDS));
            credentialProvider.flatMap(provider -> provider.getConnectionUser(Optional.empty()))
                    .ifPresent(user -> attempt(() -> dataSource.setUser(user)));
            credentialProvider.flatMap(provider -> provider.getConnectionPassword(Optional.empty()))
                    .ifPresent(pwd -> attempt(() -> dataSource.setPassword(pwd)));
            dataSource.setValidateConnectionOnBorrow(true);
        }
        catch (SQLException | UniversalConnectionPoolException e) {
            throw fail(e);
        }
    }

    @Override
    public Connection openConnection(JdbcIdentity identity)
            throws SQLException
    {
        return dataSource.getConnection();
    }

    @Override
    public void close()
    {
        try {
            poolManager.stopConnectionPool(dataSource.getDataSourceName());
        }
        catch (UniversalConnectionPoolException e) {
            log.error(e, "Failed to stop UCP pool " + dataSource.getDataSourceName());
        }
    }

    private static void attempt(ConfigOperation operation)
    {
        try {
            operation.apply();
        }
        catch (SQLException e) {
            throw fail(e);
        }
    }

    private static RuntimeException fail(Exception e)
    {
        return new PrestoException(JDBC_ERROR, "Failed to create Oracle Universal Connection Pool", e);
    }

    @FunctionalInterface
    interface ConfigOperation
    {
        void apply()
                throws SQLException;
    }

    private static String randomUuidValue()
    {
        return randomUUID().toString().toLowerCase(ENGLISH).replace("-", "");
    }
}
