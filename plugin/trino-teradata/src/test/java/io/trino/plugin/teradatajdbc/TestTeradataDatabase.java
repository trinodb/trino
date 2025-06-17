package io.trino.plugin.teradatajdbc;

import io.trino.plugin.jdbc.*;
import io.trino.plugin.jdbc.credential.EmptyCredentialProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.SchemaTableName;
import org.h2.Driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.function.Function.identity;

public class TestTeradataDatabase implements AutoCloseable {

        private final String databaseName;
        private final Connection connection;
        private final JdbcClient jdbcClient;

    private final Map<String, String> connectionProperties = new HashMap<>();

        public TestTeradataDatabase(JdbcClient jdbcClient)
                throws SQLException, ClassNotFoundException {
            this.jdbcClient = jdbcClient;
            databaseName = "TEST" + System.nanoTime() + ThreadLocalRandom.current().nextLong();
            String jdbcUrl = "";
            if (System.getenv("hostname") != null
                    && System.getenv("user") != null
                    && System.getenv("password") != null) {
                jdbcUrl = String.format("jdbc:teradata://%s/TMODE=ANSI,CHARSET=UTF8", connectionProperties.get("hostname"));
                connectionProperties.put("connection-url", jdbcUrl);
                connectionProperties.put("connection-user", System.getenv("user"));
                connectionProperties.put("connection-password", System.getenv("password"));
            }

            Class.forName("com.teradata.jdbc.TeraDriver");
            connection = DriverManager.getConnection(jdbcUrl, connectionProperties.get("user"), connectionProperties.get("password"));
            connection.createStatement().execute("CREATE DATABASE \"" + databaseName + "\" as perm=10e6;");


        }

    public TestTeradataDatabase(String databaseName, Connection connection, JdbcClient jdbcClient) {
        this.databaseName = databaseName;
        this.connection = connection;
        this.jdbcClient = jdbcClient;
    }

    @Override
        public void close()
                throws SQLException
        {
            connection.close();
        }

        public Map<String, String> getConnectionProperties() { return  connectionProperties; }

        public String getDatabaseName()
        {
            return databaseName;
        }

        public Connection getConnection()
        {
            return connection;
        }

        public JdbcClient getJdbcClient()
        {
            return jdbcClient;
        }

        public JdbcTableHandle getTableHandle(ConnectorSession session, SchemaTableName table)
        {
            return jdbcClient.getTableHandle(session, table)
                    .orElseThrow(() -> new IllegalArgumentException("table not found: " + table));
        }

        public JdbcSplit getSplit(ConnectorSession session, JdbcTableHandle table)
        {
            ConnectorSplitSource splits = jdbcClient.getSplits(session, table);
            return (JdbcSplit) getOnlyElement(getFutureValue(splits.getNextBatch(1000)).getSplits());
        }

        public Map<String, JdbcColumnHandle> getColumnHandles(ConnectorSession session, JdbcTableHandle table)
        {
            return jdbcClient.getColumns(session, table.getRequiredNamedRelation().getSchemaTableName(), table.getRequiredNamedRelation().getRemoteTableName()).stream()
                    .collect(toImmutableMap(column -> column.getColumnMetadata().getName(), identity()));
        }
    }
