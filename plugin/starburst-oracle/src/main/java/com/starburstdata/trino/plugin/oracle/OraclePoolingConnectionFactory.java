/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.oracle;

import io.airlift.log.Logger;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.oracle.OracleConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorSession;
import oracle.jdbc.pool.OracleDataSource;
import oracle.ucp.UniversalConnectionPoolAdapter;
import oracle.ucp.UniversalConnectionPoolException;
import oracle.ucp.admin.UniversalConnectionPoolManager;
import oracle.ucp.admin.UniversalConnectionPoolManagerImpl;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.lang.Math.toIntExact;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.SECONDS;

public class OraclePoolingConnectionFactory
        implements ConnectionFactory
{
    private static final Logger log = Logger.get(OraclePoolingConnectionFactory.class);

    private final UniversalConnectionPoolManager poolManager;
    private final Optional<CredentialProvider> credentialProvider;
    private final OracleConnectionProvider oracleConnectionProvider;
    private final CatalogName catalogName;
    private final Properties connectionProperties;
    private final OracleConfig oracleConfig;

    private PoolDataSource dataSource;

    public OraclePoolingConnectionFactory(
            CatalogName catalogName,
            BaseJdbcConfig config,
            Properties connectionProperties,
            Optional<CredentialProvider> credentialProvider,
            OracleConnectionProvider oracleConnectionProvider,
            OracleConfig oracleConfig)
    {
        this.catalogName = catalogName;
        this.connectionProperties = requireNonNull(connectionProperties, "connectionProperties is null");
        this.credentialProvider = requireNonNull(credentialProvider, "credentialProvider is null");
        this.oracleConfig = oracleConfig;
        this.oracleConnectionProvider = requireNonNull(oracleConnectionProvider, "oracleConnectionProvider is null");
        try {
            poolManager = UniversalConnectionPoolManagerImpl.getUniversalConnectionPoolManager();
            poolManager.setJmxEnabled(true);
            createDatasource(config.getConnectionUrl());
        }
        catch (SQLException | UniversalConnectionPoolException e) {
            throw fail(e);
        }
    }

    protected void createDatasource(String connectionUrl)
            throws UniversalConnectionPoolException, SQLException
    {
        String poolUuid = randomUuidValue();
        dataSource = PoolDataSourceFactory.getPoolDataSource();
        // UCP does not reset autocommit state
        dataSource.registerConnectionInitializationCallback(this::restoreAutoCommit);
        // generate a unique pool and data source name so that we avoid clashes when tests instantiate a connector
        // for the same catalog more than once (e.g. when running multiple nodes)
        dataSource.setConnectionPoolName(catalogName + "_pool_" + poolUuid);
        dataSource.setDataSourceName(catalogName + "_" + poolUuid);
        dataSource.setConnectionFactoryClassName(OracleDataSource.class.getName());
        dataSource.setURL(connectionUrl);
        dataSource.setConnectionProperties(connectionProperties);
        dataSource.setMaxPoolSize(oracleConfig.getConnectionPoolMaxSize());
        dataSource.setMinPoolSize(oracleConfig.getConnectionPoolMinSize());
        dataSource.setInactiveConnectionTimeout(toIntExact(oracleConfig.getInactiveConnectionTimeout().roundTo(SECONDS)));
        credentialProvider.flatMap(provider -> provider.getConnectionUser(Optional.empty()))
                .ifPresent(user -> attempt(() -> dataSource.setUser(user)));
        credentialProvider.flatMap(provider -> provider.getConnectionPassword(Optional.empty()))
                .ifPresent(pwd -> attempt(() -> dataSource.setPassword(pwd)));
        dataSource.setValidateConnectionOnBorrow(true);

        oracleConnectionProvider.validateConnectionCredentials(dataSource);

        log.debug("Opening Oracle UCP %s", dataSource.getConnectionPoolName());
        poolManager.createConnectionPool((UniversalConnectionPoolAdapter) dataSource);
    }

    private void restoreAutoCommit(Connection connection)
            throws SQLException
    {
        if (!connection.getAutoCommit()) {
            connection.rollback();
            connection.setAutoCommit(true);
        }
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        Connection connection = oracleConnectionProvider.getConnection(credentialProvider, session, dataSource);
        verify(connection.getAutoCommit(), "autoCommit must be enabled");

        return connection;
    }

    @Override
    public void close()
    {
        try {
            log.debug("Closing Oracle UCP %s", dataSource.getConnectionPoolName());
            poolManager.destroyConnectionPool(dataSource.getConnectionPoolName());
        }
        catch (UniversalConnectionPoolException e) {
            log.error(e, "Failed to destroy UCP pool %s", dataSource.getDataSourceName());
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
        return new TrinoException(JDBC_ERROR, "Failed to create Oracle Universal Connection Pool", e);
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
