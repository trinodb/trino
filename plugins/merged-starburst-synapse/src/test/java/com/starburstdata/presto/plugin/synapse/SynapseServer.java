/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.synapse;

import io.airlift.log.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import static java.util.Objects.requireNonNull;

public class SynapseServer
{
    private static final Logger LOG = Logger.get(SynapseServer.class);

    private static final String ENDPOINT = requireNonNull(System.getProperty("test.synapse.jdbc.endpoint"), "test.synapse.jdbc.endpoint is not set");
    static final String USERNAME = requireNonNull(System.getProperty("test.synapse.jdbc.user"), "test.synapse.jdbc.user is not set");
    static final String PASSWORD = requireNonNull(System.getProperty("test.synapse.jdbc.password"), "test.synapse.jdbc.password is not set");

    private static final String DATABASE = "SQLPOOL1";
    private static final String PORT = "1433";

    static final String JDBC_URL = "jdbc:sqlserver://" + ENDPOINT + ":" + PORT + ";database=" + DATABASE;

    public static Connection createConnection(String url, String username, String password)
            throws SQLException
    {
        LOG.info("Initialize connection to %s", url);
        return DriverManager.getConnection(url, username, password);
    }

    public void execute(String... query)
    {
        try (Connection conn = openConnection();
                Statement statement = conn.createStatement()) {
            for (String sql : query) {
                LOG.debug("Executing query %s", sql);
                statement.execute(sql);
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute statement: " + Arrays.toString(query), e);
        }
    }

    private static Connection openConnection()
            throws SQLException
    {
        return createConnection(JDBC_URL, USERNAME, PASSWORD);
    }
}
