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

import io.airlift.units.Duration;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ExtraCredentialsBasedIdentityCacheMapping;
import io.trino.plugin.jdbc.IdentityCacheMapping;
import io.trino.plugin.jdbc.credential.CredentialPropertiesProvider;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.DefaultCredentialPropertiesProvider;
import io.trino.plugin.jdbc.credential.ExtraCredentialConfig;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import org.testng.annotations.Test;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static io.trino.spi.block.TestingSession.SESSION;
import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPoolingConnectionFactory
{
    @Test(timeOut = 60_000)
    public void testPoolingConnectionFactory()
            throws SQLException, InterruptedException
    {
        try (PoolingConnectionFactory connectionFactory = createPoolingConnectionFactory("testPoolingConnectionFactory")) {
            Connection connection = openConnection(connectionFactory);
            TestConnection testConnection = connection.unwrap(TestConnection.class);
            assertFalse(testConnection.isClosed);

            // make sure we get same pooled connection
            int connectionNumber = testConnection.connectionNumber.get();
            connection.close();
            connection = openConnection(connectionFactory);
            testConnection = connection.unwrap(TestConnection.class);
            assertEquals(connectionNumber, (int) testConnection.connectionNumber.get());

            // connection should not be closed after cache invalidation
            sleep(10_000);
            assertEquals(connectionFactory.getEvictedDataSourcesSize(), 1);
            sleep(10_000);
            assertFalse(testConnection.isClosed);

            // make sure invalidated pool is evicted eventually
            connection.close();
            sleep(10_000);
            assertEquals(connectionFactory.getEvictedDataSourcesSize(), 0);
            assertTrue(testConnection.isClosed);

            // we should get new connection after cache is invalidated
            connection = openConnection(connectionFactory);
            testConnection = connection.unwrap(TestConnection.class);

            assertEquals((int) testConnection.connectionNumber.get(), connectionNumber + 1);
        }
    }

    @Test
    public void testAutoCommitReset()
            throws SQLException
    {
        try (PoolingConnectionFactory connectionFactory = createPoolingConnectionFactory("testAutoCommitReset")) {
            Connection connection = openConnection(connectionFactory);
            TestConnection testConnection = connection.unwrap(TestConnection.class);
            assertTrue(connection.getAutoCommit(), "autoCommit should be true by default");

            connection.setAutoCommit(false);
            connection.close();

            Connection sameConnection = openConnection(connectionFactory);
            TestConnection sameTestConnection = sameConnection.unwrap(TestConnection.class);

            assertEquals(testConnection.connectionNumber, sameTestConnection.connectionNumber);
            assertTrue(sameConnection.getAutoCommit(), "autoCommit should be reset to true");
        }
    }

    private Connection openConnection(ConnectionFactory connectionFactory)
            throws SQLException
    {
        return connectionFactory.openConnection(SESSION);
    }

    private PoolingConnectionFactory createPoolingConnectionFactory(String catalogName)
    {
        return new TestingPoolingConnectionFactory(
                catalogName,
                TestDriver.class,
                new Properties(),
                new BaseJdbcConfig().setConnectionUrl("jdbc:url"),
                new JdbcConnectionPoolConfig()
                        .setPoolCacheTtl(new Duration(5, SECONDS))
                        .setMaxPoolSize(1),
                new DefaultCredentialPropertiesProvider(new CredentialProvider()
                {
                    @Override
                    public Optional<String> getConnectionUser(Optional<ConnectorIdentity> jdbcIdentity)
                    {
                        return Optional.of("user");
                    }

                    @Override
                    public Optional<String> getConnectionPassword(Optional<ConnectorIdentity> jdbcIdentity)
                    {
                        return Optional.of("password");
                    }
                }),
                new ExtraCredentialsBasedIdentityCacheMapping(new ExtraCredentialConfig().setUserCredentialName("user").setPasswordCredentialName("password")));
    }

    private static class TestConnection
            implements Connection
    {
        private Optional<Integer> connectionNumber = Optional.empty();
        private boolean isClosed;
        private boolean autoCommit = true;

        @Override
        public Statement createStatement()
        {
            return null;
        }

        @Override
        public PreparedStatement prepareStatement(String sql)
        {
            return null;
        }

        @Override
        public CallableStatement prepareCall(String sql)
        {
            return null;
        }

        @Override
        public String nativeSQL(String sql)
        {
            return null;
        }

        @Override
        public void setAutoCommit(boolean autoCommit)
        {
            this.autoCommit = autoCommit;
        }

        @Override
        public boolean getAutoCommit()
        {
            return autoCommit;
        }

        @Override
        public void commit()
        {
        }

        @Override
        public void rollback()
        {
        }

        @Override
        public void close()
        {
            isClosed = true;
        }

        @Override
        public boolean isClosed()
        {
            return false;
        }

        @Override
        public DatabaseMetaData getMetaData()
        {
            return null;
        }

        @Override
        public void setReadOnly(boolean readOnly)
        {
        }

        @Override
        public boolean isReadOnly()
        {
            return false;
        }

        @Override
        public void setCatalog(String catalog)
        {
        }

        @Override
        public String getCatalog()
        {
            return null;
        }

        @Override
        public void setTransactionIsolation(int level)
        {
        }

        @Override
        public int getTransactionIsolation()
        {
            return 0;
        }

        @Override
        public SQLWarning getWarnings()
        {
            return null;
        }

        @Override
        public void clearWarnings()
        {
        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency)
        {
            return null;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
        {
            return null;
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
        {
            return null;
        }

        @Override
        public Map<String, Class<?>> getTypeMap()
        {
            return null;
        }

        @Override
        public void setTypeMap(Map<String, Class<?>> map)
        {
        }

        @Override
        public void setHoldability(int holdability)
        {
        }

        @Override
        public int getHoldability()
        {
            return 0;
        }

        @Override
        public Savepoint setSavepoint()
        {
            return null;
        }

        @Override
        public Savepoint setSavepoint(String name)
        {
            return null;
        }

        @Override
        public void rollback(Savepoint savepoint)
        {
        }

        @Override
        public void releaseSavepoint(Savepoint savepoint)
        {
        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
        {
            return null;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
        {
            return null;
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
        {
            return null;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
        {
            return null;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
        {
            return null;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, String[] columnNames)
        {
            return null;
        }

        @Override
        public Clob createClob()
        {
            return null;
        }

        @Override
        public Blob createBlob()
        {
            return null;
        }

        @Override
        public NClob createNClob()
        {
            return null;
        }

        @Override
        public SQLXML createSQLXML()
        {
            return null;
        }

        @Override
        public boolean isValid(int timeout)
        {
            return false;
        }

        @Override
        public void setClientInfo(String name, String value)
        {
        }

        @Override
        public void setClientInfo(Properties properties)
        {
        }

        @Override
        public String getClientInfo(String name)
        {
            return null;
        }

        @Override
        public Properties getClientInfo()
        {
            return null;
        }

        @Override
        public Array createArrayOf(String typeName, Object[] elements)
        {
            return null;
        }

        @Override
        public Struct createStruct(String typeName, Object[] attributes)
        {
            return null;
        }

        @Override
        public void setSchema(String schema)
        {
        }

        @Override
        public String getSchema()
        {
            return null;
        }

        @Override
        public void abort(Executor executor)
        {
        }

        @Override
        public void setNetworkTimeout(Executor executor, int milliseconds)
        {
        }

        @Override
        public int getNetworkTimeout()
        {
            return 0;
        }

        @Override
        public <T> T unwrap(Class<T> iface)
        {
            return iface.cast(this);
        }

        @Override
        public boolean isWrapperFor(Class<?> iface)
        {
            return iface.isInstance(this);
        }
    }

    public static class TestDriver
            implements Driver
    {
        @Override
        public Connection connect(String url, Properties info)
        {
            return new TestConnection();
        }

        @Override
        public boolean acceptsURL(String url)
        {
            return true;
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
        {
            return new DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion()
        {
            return 0;
        }

        @Override
        public int getMinorVersion()
        {
            return 0;
        }

        @Override
        public boolean jdbcCompliant()
        {
            return false;
        }

        @Override
        public Logger getParentLogger()
        {
            return null;
        }
    }

    public static class TestingPoolingConnectionFactory
            extends PoolingConnectionFactory
    {
        private final AtomicInteger connectionCounter = new AtomicInteger(1);

        public TestingPoolingConnectionFactory(String catalogName, Class<? extends Driver> driverClass, Properties connectionProperties, BaseJdbcConfig config, JdbcConnectionPoolConfig poolConfig, CredentialPropertiesProvider<String, String> credentialPropertiesProvider, IdentityCacheMapping identityCacheMapping)
        {
            super(catalogName, driverClass, connectionProperties, config, poolConfig, credentialPropertiesProvider, identityCacheMapping);
        }

        @Override
        public Connection openConnection(ConnectorSession session)
                throws SQLException
        {
            Connection connection = super.openConnection(session);
            TestConnection testConnection = connection.unwrap(TestConnection.class);

            if (testConnection.connectionNumber.isEmpty()) {
                testConnection.connectionNumber = Optional.of(connectionCounter.incrementAndGet());
            }

            return connection;
        }
    }
}
