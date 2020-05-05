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
import static com.starburstdata.presto.plugin.oracle.OracleAuthenticationType.PASSWORD_PASS_THROUGH;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static io.prestosql.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.SECONDS;

public class OraclePoolingConnectionFactory
        implements ConnectionFactory
{
    private static final String PASSWORD_PASSTHROUGH = "starburst.password.passthrough";
    private static final String USERNAME_PASSTHROUGH = "starburst.username.passthrough";

    private static final Logger log = Logger.get(OraclePoolingConnectionFactory.class);

    private final UniversalConnectionPoolManager poolManager;
    private final PoolDataSource dataSource;
    private final OracleAuthenticationType authenticationType;

    public OraclePoolingConnectionFactory(
            String catalogName,
            BaseJdbcConfig config,
            Properties properties,
            Optional<CredentialProvider> credentialProvider,
            OracleConnectionPoolingConfig poolingConfig,
            OracleAuthenticationType authenticationType)
    {
        this.authenticationType = requireNonNull(authenticationType, "authenticationType is null");
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
            dataSource.setMaxPoolSize(poolingConfig.getMaxPoolSize());
            dataSource.setMinPoolSize(poolingConfig.getMinPoolSize());
            dataSource.setInactiveConnectionTimeout((int) poolingConfig.getInactiveConnectionTimeout().roundTo(SECONDS));
            credentialProvider.flatMap(provider -> provider.getConnectionUser(Optional.empty()))
                    .ifPresent(user -> attempt(() -> dataSource.setUser(user)));
            credentialProvider.flatMap(provider -> provider.getConnectionPassword(Optional.empty()))
                    .ifPresent(pwd -> attempt(() -> dataSource.setPassword(pwd)));
            dataSource.setValidateConnectionOnBorrow(true);

            if (authenticationType == PASSWORD_PASS_THROUGH) {
                verifyPassThroughDataSourceConfiguration(dataSource);
            }

            log.debug("Opening Oracle UCP %s", dataSource.getConnectionPoolName());
            poolManager.createConnectionPool((UniversalConnectionPoolAdapter) dataSource);
        }
        catch (SQLException | UniversalConnectionPoolException e) {
            throw fail(e);
        }
    }

    private static void verifyPassThroughDataSourceConfiguration(PoolDataSource dataSource)
    {
        if (dataSource.getUser() == null || dataSource.getUser().isEmpty()) {
            throw new PrestoException(CONFIGURATION_INVALID, "Password pass-through mode requires connection-user to discover cluster topology");
        }

        if (dataSource.getPassword() == null || dataSource.getPassword().isEmpty()) {
            throw new PrestoException(CONFIGURATION_INVALID, "Password pass-through mode requires connection-password to discover cluster topology");
        }
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
    public Connection openConnection(JdbcIdentity identity)
            throws SQLException
    {
        Connection connection;

        if (authenticationType == PASSWORD_PASS_THROUGH) {
            connection = getPassThroughConnection(identity);
        }
        else {
            connection = dataSource.getConnection();
        }

        verify(connection.getAutoCommit(), "autoCommit must be enabled");

        return connection;
    }

    private Connection getPassThroughConnection(JdbcIdentity identity)
            throws SQLException
    {
        String username = identity.getExtraCredentials().getOrDefault(USERNAME_PASSTHROUGH, "");
        String password = identity.getExtraCredentials().getOrDefault(PASSWORD_PASSTHROUGH, "");

        if (username.isEmpty() || password.isEmpty()) {
            throw new PrestoException(GENERIC_USER_ERROR, "Password pass-through authentication requires user credentials");
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
