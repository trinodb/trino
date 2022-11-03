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

import io.airlift.log.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Provide information on an external Snowflake Database server based on
 * configurable system properties.
 * <p>
 * The configurable properties are {@code test.server.url}, {@code
 * test.server.user}, {@code test.server.role} and {@code test.server.password}. All three must be
 * provided, and they must connect to a database server, for this class to be
 * used.
 */
public class SnowflakeServer
{
    private static final Logger LOG = Logger.get(SnowflakeServer.class);

    public static final String ROLE = requireNonNull(System.getProperty("snowflake.test.server.role"), "snowflake.test.server.role is not set");

    public static final String JDBC_URL = requireNonNull(System.getProperty("snowflake.test.server.url"), "snowflake.test.server.url is not set");
    public static final String USER = requireNonNull(System.getProperty("snowflake.test.server.user"), "snowflake.test.server.user is not set");
    public static final String PASSWORD = requireNonNull(System.getProperty("snowflake.test.server.password"), "snowflake.test.server.password is not set");
    public static final String TEST_WAREHOUSE = requireNonNull(System.getProperty("snowflake.test.warehouse"), "snowflake.test.warehouse is not set");
    public static final String TEST_DATABASE = "TEST_DB";

    void init()
            throws SQLException
    {
        LOG.info("Using %s", JDBC_URL);

        // make sure Snowflake is accessible
        execute("SELECT 1");
    }

    public TestDatabase createDatabase(String databaseSuffix)
    {
        return new TestDatabase(this::safeExecute, databaseSuffix);
    }

    public TestDatabase createTestDatabase()
    {
        return createDatabase("TEST");
    }

    public void createSchema(String databaseName, String schemaName)
            throws SQLException
    {
        executeOnDatabase(databaseName, format("CREATE SCHEMA IF NOT EXISTS %s", schemaName));
    }

    void execute(String... sqls)
            throws SQLException
    {
        executeOnDatabase(TEST_DATABASE, sqls);
    }

    public void executeOnDatabase(String database, String... sqls)
            throws SQLException
    {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            LOG.info("Using role: %s, warehouse: %s, database: %s", ROLE, TEST_WAREHOUSE, database);
            stmt.execute(format("USE ROLE %s", ROLE));
            stmt.execute(format("USE WAREHOUSE %s", TEST_WAREHOUSE));
            stmt.execute(format("USE DATABASE %s", database));

            for (String sql : sqls) {
                LOG.info("Executing [%s]: %s", USER, sql);
                stmt.execute(sql);
            }
        }
    }

    private void safeExecute(String sql)
    {
        try {
            execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void safeExecuteOnDatabase(String database, String... sqls)
    {
        try {
            executeOnDatabase(database, sqls);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Connection getConnection()
            throws SQLException
    {
        return DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
    }
}
