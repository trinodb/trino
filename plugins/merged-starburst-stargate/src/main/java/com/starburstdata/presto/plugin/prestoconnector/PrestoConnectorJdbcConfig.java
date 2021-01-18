/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.prestoconnector;

import io.trino.jdbc.TrinoDriverUri;
import io.trino.plugin.jdbc.BaseJdbcConfig;

import javax.validation.constraints.AssertTrue;

import java.sql.SQLException;
import java.util.Properties;

public class PrestoConnectorJdbcConfig
        extends BaseJdbcConfig
{
    @AssertTrue(message = "Invalid Starburst JDBC URL")
    public boolean isValidConnectionUrl()
    {
        String connectionUrl = getConnectionUrl();
        if (connectionUrl == null) {
            // It's other validations responsibility to determine whether the property is required
            return true;
        }
        try {
            TrinoDriverUri.create(connectionUrl, getUserProperties());
            return true;
        }
        catch (SQLException e) {
            return false;
        }
    }

    @AssertTrue(message = "Invalid Starburst JDBC URL: catalog is not provided")
    public boolean isConnectionUrlCatalogValid()
    {
        String connectionUrl = getConnectionUrl();
        if (connectionUrl == null) {
            // It's other validations responsibility to determine whether the property is required
            return true;
        }
        try {
            TrinoDriverUri driverUri = TrinoDriverUri.create(connectionUrl, getUserProperties());
            return driverUri.getCatalog().isPresent();
        }
        catch (SQLException e) {
            // It's #isValidConnectionUrl() responsibility to determine whether the property is well-formed
            return true;
        }
    }

    @AssertTrue(message = "Invalid Starburst JDBC URL: schema must not be provided")
    public boolean isConnectionUrlSchemaValid()
    {
        String connectionUrl = getConnectionUrl();
        if (connectionUrl == null) {
            // It's other validations responsibility to determine whether the property is required
            return true;
        }
        try {
            TrinoDriverUri driverUri = TrinoDriverUri.create(connectionUrl, getUserProperties());
            return driverUri.getSchema().isEmpty();
        }
        catch (SQLException e) {
            // It's #isValidConnectionUrl() responsibility to determine whether the property is well-formed
            return true;
        }
    }

    private static Properties getUserProperties()
    {
        // connection user is required by TrinoDriverUri validations
        Properties properties = new Properties();
        properties.put("user", "fake_username");
        return properties;
    }
}
