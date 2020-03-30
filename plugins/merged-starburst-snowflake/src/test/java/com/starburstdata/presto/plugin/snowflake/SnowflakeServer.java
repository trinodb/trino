/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake;

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
class SnowflakeServer
{
    private static final Logger LOG = Logger.get(SnowflakeServer.class);

    private static final String ROLE = requireNonNull(System.getProperty("snowflake.test.server.role"), "snowflake.test.server.role is not set");

    static final String JDBC_URL = requireNonNull(System.getProperty("snowflake.test.server.url"), "snowflake.test.server.url is not set");
    static final String USER = requireNonNull(System.getProperty("snowflake.test.server.user"), "snowflake.test.server.user is not set");
    static final String PASSWORD = requireNonNull(System.getProperty("snowflake.test.server.password"), "snowflake.test.server.password is not set");

    static final String OKTA_USER = requireNonNull(System.getProperty("snowflake.test.okta.user"), "snowflake.test.okta.user is not set");
    static final String OKTA_PASSWORD = requireNonNull(System.getProperty("snowflake.test.okta.password"), "snowflake.test.okta.password is not set");
    static final String OKTA_URL = requireNonNull(System.getProperty("snowflake.test.okta-url"), "snowflake.test.okta-url is not set");
    static final String ACCOUNT_URL = requireNonNull(System.getProperty("snowflake.test.account-url"), "snowflake.test.account-url is not set");
    static final String ACCOUNT_NAME = requireNonNull(System.getProperty("snowflake.test.account-name"), "snowflake.test.account-name is not set");
    static final String CLIENT_ID = requireNonNull(System.getProperty("snowflake.test.client-id"), "snowflake.test.client-id is not set");
    static final String CLIENT_SECRET = requireNonNull(System.getProperty("snowflake.test.client-secret"), "snowflake.test.client-secret is not set");

    static final String TEST_WAREHOUSE = "TEST_WH";
    static final String TEST_DATABASE = "TEST_DB";

    void init()
            throws SQLException
    {
        LOG.info("Using %s", JDBC_URL);

        // make sure Snowflake is accessible
        execute("SELECT 1");
    }

    void createSchema(String schemaName)
            throws SQLException
    {
        execute(format("CREATE SCHEMA %s", schemaName));
    }

    void dropSchemaIfExistsCascade(String schemaName)
            throws SQLException
    {
        execute(format("DROP SCHEMA IF EXISTS %s CASCADE", schemaName));
    }

    void execute(String... sqls)
            throws SQLException
    {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            LOG.info("Using role: %s, warehouse: %s, database: %s", ROLE, TEST_WAREHOUSE, TEST_DATABASE);
            stmt.execute(format("USE ROLE %s", ROLE));
            stmt.execute(format("USE WAREHOUSE %s", TEST_WAREHOUSE));
            stmt.execute(format("USE DATABASE %s", TEST_DATABASE));

            for (String sql : sqls) {
                LOG.info("Executing [%s]: %s", USER, sql);
                stmt.execute(sql);
            }
        }
    }

    private Connection getConnection()
            throws SQLException
    {
        return DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
    }
}
