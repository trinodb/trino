/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake;

import com.google.common.io.Closer;
import io.trino.testng.services.ManageTestResources;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static com.starburstdata.trino.plugins.snowflake.SnowflakeServer.JDBC_URL;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeServer.PASSWORD;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeServer.ROLE;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeServer.TEST_WAREHOUSE;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeServer.USER;
import static java.lang.String.format;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestSnowflakeJdbcDriver
{
    @ManageTestResources.Suppress(because = "Mock to remote server")
    protected final SnowflakeServer server = new SnowflakeServer();
    @ManageTestResources.Suppress(because = "Used by mocks")
    protected final Closer closer = Closer.create();
    @ManageTestResources.Suppress(because = "Mock to remote database")
    protected final TestDatabase testDatabase = closer.register(server.createTestDatabase());

    @AfterClass(alwaysRun = true)
    public void cleanup()
            throws IOException
    {
        closer.close();
    }

    @Test
    public void testSnowflakeDriverUseRoleResetsCatalog()
            throws SQLException
    {
        // See: https://github.com/snowflakedb/snowflake-jdbc/issues/719
        Properties properties = new Properties();
        properties.setProperty("user", USER);
        properties.setProperty("password", PASSWORD);
        properties.setProperty("warehouse", TEST_WAREHOUSE);
        properties.setProperty("role", ROLE);
        properties.setProperty("db", testDatabase.getName());

        try (Connection connection = DriverManager.getConnection(JDBC_URL, properties)) {
            // Verify that getCatalog and current_database() are both set to the configured database
            assertThat(connection.getCatalog()).isEqualTo(testDatabase.getName());
            ResultSet beforeResultSet = connection.prepareStatement("select current_database()").executeQuery();
            beforeResultSet.next();
            assertThat(beforeResultSet.getString(1)).isEqualTo(testDatabase.getName());

            // USE ROLE triggers the snowflake jdbc driver bug
            connection.prepareStatement(format("USE ROLE %s", ROLE)).executeQuery();

            // Now verify getCatalog and current_database() do not match
            assertThat(connection.getCatalog()).isEqualTo(null);
            ResultSet afterResultSet = connection.prepareStatement("select current_database()").executeQuery();
            afterResultSet.next();
            assertThat(afterResultSet.getString(1)).isEqualTo(testDatabase.getName());
        }
    }
}
