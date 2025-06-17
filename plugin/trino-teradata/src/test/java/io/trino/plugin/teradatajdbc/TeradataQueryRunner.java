/**
 * Unpublished work.
 * Copyright 2025 by Teradata Corporation. All rights reserved
 * TERADATA CORPORATION CONFIDENTIAL AND TRADE SECRET
 */

package io.trino.plugin.teradatajdbc;

import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.plugin.base.mapping.DefaultIdentifierMapping;
import io.trino.plugin.base.util.Closables;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.intellij.lang.annotations.Language;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;

public final class TeradataQueryRunner
{
    static TestTeradataDatabase database;
    static {
        JdbcClient client =  new TeradataClient(
                new BaseJdbcConfig(),
                new TeradataConfig(),
                session -> {
                    throw new UnsupportedOperationException();
                },
                new DefaultQueryBuilder(RemoteQueryModifier.NONE),
                new DefaultIdentifierMapping(),
                RemoteQueryModifier.NONE);
        try {
            database = new TestTeradataDatabase(client);
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private TeradataQueryRunner()
    {
        // do nothing
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final Map<String, String> connectorProperties = new HashMap<>();

        protected Builder()
        {
            super(testSessionBuilder()
                    .setCatalog("teradata")
                    .setSchema("trino")
                    .build());
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new TeradataPlugin());
                queryRunner.createCatalog("teradata", "teradatajdbc", database.getConnectionProperties());
                return queryRunner;
            }
            catch (Throwable e) {
                Closables.closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    public static class TeradataJDBCSqlExecutor
            implements SqlExecutor
    {
        @Override
        public void execute(@Language("SQL") String sql)
        {
            try {
                Connection conn = createTeradataJDBCConnection();
                Statement stmt = conn.createStatement();
                stmt.executeQuery(sql);
                stmt.close();
                conn.close();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private static Connection createTeradataJDBCConnection()
        {
            try {
                return database.getConnection();
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to create Teradata JDBC connection", e);
            }
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging logger = Logging.initialize();
        logger.setLevel("io.trino.plugin.teradatajdbc", Level.DEBUG);
        logger.setLevel("io.trino", Level.INFO);

        QueryRunner queryRunner = builder()
                .addCoordinatorProperty("http-server.http.port", "8080")
                .build();

        Logger log = Logger.get(TeradataQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
