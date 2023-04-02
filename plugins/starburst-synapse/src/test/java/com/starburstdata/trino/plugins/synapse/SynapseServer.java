/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.synapse;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.airlift.log.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;

public class SynapseServer
        implements AutoCloseable
{
    private static final Logger LOG = Logger.get(SynapseServer.class);

    private static final String ENDPOINT = requireNonNull(System.getProperty("test.synapse.jdbc.endpoint"), "test.synapse.jdbc.endpoint is not set");
    static final String USERNAME = requireNonNull(System.getProperty("test.synapse.jdbc.user"), "test.synapse.jdbc.user is not set");
    static final String PASSWORD = requireNonNull(System.getProperty("test.synapse.jdbc.password"), "test.synapse.jdbc.password is not set");

    private static final String DATABASE = "SQLPOOL1";
    private static final String PORT = "1433";

    static final String JDBC_URL = "jdbc:sqlserver://" + ENDPOINT + ":" + PORT + ";database=" + DATABASE;

    private final HikariDataSource dataSource;

    public SynapseServer()
    {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(JDBC_URL);
        hikariConfig.setUsername(USERNAME);
        hikariConfig.setPassword(PASSWORD);
        hikariConfig.setRegisterMbeans(false);
        hikariConfig.setMaxLifetime(MINUTES.toMillis(1));

        this.dataSource = new HikariDataSource(hikariConfig);
    }

    public void execute(String... query)
    {
        try (Connection conn = dataSource.getConnection();
                Statement statement = conn.createStatement()) {
            for (String sql : query) {
                statement.execute(sql);
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute statement: " + Arrays.toString(query), e);
        }
    }

    public <T> T executeQuery(String query, Function<ResultSet, T> resultConsumer)
    {
        LOG.debug("Executing query %s", query);
        try (Connection conn = dataSource.getConnection();
                Statement statement = conn.createStatement();
                ResultSet resultSet = statement.executeQuery(query)) {
            return resultConsumer.apply(resultSet);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute statement: " + query, e);
        }
    }

    @Override
    public void close()
            throws Exception
    {
        dataSource.close();
    }
}
