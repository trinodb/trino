/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake.jdbc;

import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.credential.CredentialPropertiesProvider;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.DefaultCredentialPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static com.starburstdata.trino.plugins.snowflake.jdbc.SnowflakeJdbcSessionProperties.WAREHOUSE;
import static java.util.Objects.requireNonNull;

public class WarehouseAwareDriverConnectionFactory
        implements ConnectionFactory
{
    private final Driver driver;
    private final String connectionUrl;
    protected final Properties connectionProperties;
    protected final CredentialPropertiesProvider<String, String> credentialPropertiesProvider;

    public WarehouseAwareDriverConnectionFactory(Driver driver, String connectionUrl, Properties connectionProperties, CredentialProvider credentialProvider)
    {
        this(driver, connectionUrl, connectionProperties, new DefaultCredentialPropertiesProvider(credentialProvider));
    }

    public WarehouseAwareDriverConnectionFactory(Driver driver, String connectionUrl, Properties connectionProperties, CredentialPropertiesProvider<String, String> credentialPropertiesProvider)
    {
        this.driver = requireNonNull(driver, "driver is null");
        this.connectionUrl = requireNonNull(connectionUrl, "connectionUrl is null");
        this.connectionProperties = new Properties();
        this.connectionProperties.putAll(requireNonNull(connectionProperties, "connectionProperties is null"));
        this.credentialPropertiesProvider = requireNonNull(credentialPropertiesProvider, "credentialPropertiesProvider is null");
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        Properties properties = getConnectionProperties(session);
        Connection connection = driver.connect(connectionUrl, properties);
        checkState(connection != null, "Driver returned null connection, make sure the connection URL '%s' is valid for the driver %s", connectionUrl, driver);
        return connection;
    }

    protected Properties getConnectionProperties(ConnectorSession session)
    {
        ConnectorIdentity identity = session.getIdentity();
        Properties properties = new Properties();
        properties.putAll(connectionProperties);
        properties.putAll(credentialPropertiesProvider.getCredentialProperties(identity));
        SnowflakeJdbcSessionProperties.getWarehouse(session).ifPresent(warehouse -> properties.put(WAREHOUSE, warehouse));
        return properties;
    }
}
