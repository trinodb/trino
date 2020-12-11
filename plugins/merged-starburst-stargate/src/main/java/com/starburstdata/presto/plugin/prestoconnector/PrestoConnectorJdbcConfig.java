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

import io.prestosql.jdbc.PrestoDriverUri;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;

import javax.validation.constraints.AssertTrue;

import java.sql.SQLException;
import java.util.Properties;

public class PrestoConnectorJdbcConfig
        extends BaseJdbcConfig
{
    @AssertTrue(message = "Invalid Presto JDBC url")
    public boolean isValidConnectionUrl()
    {
        try {
            PrestoDriverUri.create(getConnectionUrl(), getUserProperties());
            return true;
        }
        catch (SQLException e) {
            return false;
        }
    }

    @AssertTrue(message = "Invalid Presto JDBC URL: catalog and/or schema is not provided")
    public boolean isValidConnectionUrlCatalog()
    {
        try {
            PrestoDriverUri driverUri = PrestoDriverUri.create(getConnectionUrl(), getUserProperties());
            return driverUri.getCatalog().isPresent();
        }
        catch (SQLException e) {
            return false;
        }
    }

    private static Properties getUserProperties()
    {
        // connection user is required by PrestoDriverUri validations
        Properties properties = new Properties();
        properties.put("user", "presto");
        return properties;
    }
}
