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
import static java.util.Objects.requireNonNull;

/**
 * We implement our own ConnectionFactory in order to add our OEM key to the connection URL but prevent it from being logged
 * CData does not want our customers to know what the OEM key is as it will unlock all their drivers
 */
public class SalesforceConnectionFactory
        implements ConnectionFactory
{
    private static final String DRIVER_CLASS_NAME = "cdata.jdbc.salesforce.SalesforceDriver";
    public static final String CDATA_OEM_KEY = "StarburstData_6b415f4923004c64ac4da9223bfc053f_v1.0";

    // We use Driver here rather than delegating to another ConnectionFactory in order to prevent
    // the URL from being logged or thrown in an error message
    private final Driver driver;
    private final String connectionUrl;
    private final CredentialPropertiesProvider<String, String> credentialPropertiesProvider;

    public SalesforceConnectionFactory(CredentialProvider credentialProvider, SalesforceModule.ConnectionUrlProvider connectionUrlProvider)
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

        connectionUrl = requireNonNull(connectionUrlProvider.get(), "Connection URL provider returned null connection string");
        credentialPropertiesProvider = new DefaultCredentialPropertiesProvider(credentialProvider);
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        ConnectorIdentity identity = session.getIdentity();
        Properties properties = getCredentialProperties(identity);
        Connection connection = driver.connect(connectionUrl, properties);

        // Remove the OEM key from the log
        checkState(connection != null, "Driver returned null connection, make sure the connection URL '%s' is valid for the driver %s", connectionUrl.replaceAll("OEMKey=\".+?\";", ""), driver);
        return connection;
    }

    private Properties getCredentialProperties(ConnectorIdentity identity)
    {
        Properties properties = new Properties();
        properties.putAll(credentialPropertiesProvider.getCredentialProperties(identity));
        return properties;
    }
}
