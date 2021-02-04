/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.salesforce;

import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcIdentity;
import io.trino.plugin.jdbc.credential.CredentialPropertiesProvider;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.DefaultCredentialPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;

/**
 * We implement our own ConnectionFactory in order to add our OEM key to the connection URL but prevent it from being logged
 * CData does not want our customers to know what the OEM key is as it will unlock all their drivers
 */
public class SalesforceConnectionFactory
        implements ConnectionFactory
{
    private static final String DRIVER_CLASS_NAME = "cdata.jdbc.salesforce.SalesforceDriver";
    private static final String CDATA_OEM_KEY = "StarburstData_6b415f4923004c64ac4da9223bfc053f_v1.0";

    public static String getConnectionUrl(SalesforceConfig salesforceConfig)
    {
        StringBuilder builder = new StringBuilder("jdbc:salesforce:")
                .append("User=\"").append(salesforceConfig.getUser()).append("\";")
                .append("Password=\"").append(salesforceConfig.getPassword()).append("\";")
                .append("SecurityToken=\"").append(salesforceConfig.getSecurityToken()).append("\";")
                .append("UseSandbox=\"").append(salesforceConfig.isSandboxEnabled()).append("\";")
                .append("OEMKey=\"").append(CDATA_OEM_KEY).append("\";");

        if (salesforceConfig.isDriverLoggingEnabled()) {
            builder.append("LogFile=\"").append(salesforceConfig.getDriverLoggingLocation()).append("\";")
                    .append("Verbosity=\"").append(salesforceConfig.getDriverLoggingVerbosity()).append("\";");
        }

        salesforceConfig.getExtraJdbcProperties().ifPresent(builder::append);
        return builder.toString();
    }

    // We use Driver here rather than delegating to another ConnectionFactory in order to prevent
    // the URL from being logged or thrown in an error message
    private final Driver driver;
    private final String connectionUrl;
    private final CredentialPropertiesProvider credentialPropertiesProvider;

    public SalesforceConnectionFactory(SalesforceConfig salesforceConfig, CredentialProvider credentialProvider)
    {
        Class<? extends Driver> driverClass;
        try {
            driverClass = Class.forName(DRIVER_CLASS_NAME).asSubclass(Driver.class);
        }
        catch (ClassNotFoundException | ClassCastException e) {
            throw new RuntimeException("Failed to load Driver class: " + DRIVER_CLASS_NAME, e);
        }

        try {
            driver = driverClass.getConstructor().newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to create instance of Driver: " + DRIVER_CLASS_NAME, e);
        }

        connectionUrl = getConnectionUrl(salesforceConfig);
        credentialPropertiesProvider = new DefaultCredentialPropertiesProvider(credentialProvider);
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        JdbcIdentity identity = JdbcIdentity.from(session);
        Properties properties = getCredentialProperties(identity);
        Connection connection = driver.connect(connectionUrl, properties);
        checkState(connection != null, "Driver returned null connection, make sure the connection URL '%s' is valid for the driver %s", connectionUrl, driver);
        return connection;
    }

    private Properties getCredentialProperties(JdbcIdentity identity)
    {
        Properties properties = new Properties();
        properties.putAll(credentialPropertiesProvider.getCredentialProperties(identity));
        return properties;
    }
}
