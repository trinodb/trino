/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.oracle;

import io.airlift.log.Logger;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.oracle.OracleConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
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
import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
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
    private final PoolDataSource dataSource;
    private final Optional<CredentialProvider> credentialProvider;

    public OraclePoolingConnectionFactory(
            CatalogName catalogName,
            BaseJdbcConfig config,
            Properties properties,
            Optional<CredentialProvider> credentialProvider,
            OracleConfig oracleConfig)
    {
        this.credentialProvider = requireNonNull(credentialProvider, "credentialProvider is null");
        String poolUuid = randomUuidValue();

        try {
            poolManager = UniversalConnectionPoolManagerImpl.getUniversalConnectionPoolManager();
            poolManager.setJmxEnabled(true);
            dataSource = PoolDataSourceFactory.getPoolDataSource();
            // UCP does not reset autocommit state
            dataSource.registerConnectionInitializationCallback(this::restoreAutoCommit);
            // generate a unique pool and data source name so that we avoid clashes when tests instantiate a connector
            // for the same catalog more than once (e.g. when running multiple nodes)
            dataSource.setConnectionPoolName(catalogName + "_pool_" + poolUuid);
            dataSource.setDataSourceName(catalogName + "_" + poolUuid);
            dataSource.setConnectionFactoryClassName(OracleDataSource.class.getName());
            dataSource.setURL(config.getConnectionUrl());
            dataSource.setConnectionProperties(properties);
            dataSource.setMaxPoolSize(oracleConfig.getConnectionPoolMaxSize());
            dataSource.setMinPoolSize(oracleConfig.getConnectionPoolMinSize());
            dataSource.setInactiveConnectionTimeout(toIntExact(oracleConfig.getInactiveConnectionTimeout().roundTo(SECONDS)));
            credentialProvider.flatMap(provider -> provider.getConnectionUser(Optional.empty()))
                    .ifPresent(user -> attempt(() -> dataSource.setUser(user)));
            credentialProvider.flatMap(provider -> provider.getConnectionPassword(Optional.empty()))
                    .ifPresent(pwd -> attempt(() -> dataSource.setPassword(pwd)));
            dataSource.setValidateConnectionOnBorrow(true);

            if (dataSource.getUser() == null || dataSource.getUser().isEmpty()) {
                throw new TrinoException(CONFIGURATION_INVALID, "Require connection-user to discover cluster topology");
            }
            if (isPasswordAuthentication()) {
                verifyPasswordIsPresent(dataSource);
            }

            log.debug("Opening Oracle UCP %s", dataSource.getConnectionPoolName());
            poolManager.createConnectionPool((UniversalConnectionPoolAdapter) dataSource);
        }
        catch (SQLException | UniversalConnectionPoolException e) {
            throw fail(e);
        }
    }

    private static void verifyPasswordIsPresent(PoolDataSource dataSource)
    {
        if (dataSource.getPassword() == null || dataSource.getPassword().isEmpty()) {
            throw new TrinoException(CONFIGURATION_INVALID, "Password and password pass-through modes require connection-password to discover cluster topology");
        }
    }

    /*
     * PoolDataSource#getPassword is a deprecated method with the latest Oracle UCP
     * version used here, hence must be ignored. Will be extended by SEP to override the validation process
     * in case of PASSWORD/PASSWORD_PASS_THROUGH authentication methods supported.
     */
    protected boolean isPasswordAuthentication()
    {
        return false;
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
        Connection connection = getConnectionWithCredentials(session);

        verify(connection.getAutoCommit(), "autoCommit must be enabled");

        return connection;
    }

    protected Connection getConnectionWithCredentials(ConnectorSession session)
            throws SQLException
    {
        verify(credentialProvider.isPresent(), "Credential provider is missing");

        Optional<ConnectorIdentity> identity = Optional.of(session.getIdentity());
        String username = credentialProvider.get().getConnectionUser(identity).orElse("");
        String password = credentialProvider.get().getConnectionPassword(identity).orElse("");

        if (username.isEmpty() || password.isEmpty()) {
            throw new TrinoException(GENERIC_USER_ERROR, "Password authentication modes require user credentials");
        }

        return dataSource.getConnection(username, password);
    }

    @Override
    public void close()
    {
        try {
            log.debug("Closing Oracle UCP %s", dataSource.getConnectionPoolName());
            poolManager.stopConnectionPool(dataSource.getConnectionPoolName());
        }
        catch (UniversalConnectionPoolException e) {
            log.error(e, "Failed to stop UCP pool %s", dataSource.getDataSourceName());
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
