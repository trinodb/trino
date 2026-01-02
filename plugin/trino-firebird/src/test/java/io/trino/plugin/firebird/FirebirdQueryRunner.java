/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.firebird;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.type.Type;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public final class FirebirdQueryRunner
{
    private FirebirdQueryRunner() {}

    static {
        Logging logging = Logging.initialize();
        logging.setLevel("org.firebirdsql.jdbc", Level.OFF);
    }

    private static final Logger log = Logger.get(FirebirdQueryRunner.class);

    public static final int MAX_BATCH = 500;

    public static final String FIREBIRD = "firebird";

    public static final String SCHEMA = "default";

    public static Builder builder(TestingFirebirdServer firebirdServer)
    {
        return new Builder(firebirdServer)
                .addConnectorProperty("connection-url", firebirdServer.getJdbcUrl())
                .addConnectorProperty("connection-user", firebirdServer.getUsername())
                .addConnectorProperty("connection-password", firebirdServer.getPassword());
                // FIXME: The following lines are necessary if the connector cannot rename the tables
                //.addConnectorProperty("statistics.enabled", "false")
                //.addConnectorProperty("insert.non-transactional-insert.enabled", "true")
                //.addConnectorProperty("merge.non-transactional-merge.enabled", "true");
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final TestingFirebirdServer server;
        private final Map<String, String> connectorProperties = new HashMap<>();
        private List<TpchTable<?>> initialTables = ImmutableList.of();

        private Builder(TestingFirebirdServer server)
        {
            super(testSessionBuilder()
                    .setCatalog(FIREBIRD)
                    .setSchema(SCHEMA)
                    .build());
            this.server = server;
        }

        @CanIgnoreReturnValue
        public Builder addConnectorProperties(Map<String, String> connectorProperties)
        {
            this.connectorProperties.putAll(requireNonNull(connectorProperties, "connectorProperties is null"));
            return this;
        }

        @CanIgnoreReturnValue
        public Builder addConnectorProperty(String key, String value)
        {
            this.connectorProperties.put(key, value);
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setInitialTables(Iterable<TpchTable<?>> initialTables)
        {
            this.initialTables = ImmutableList.copyOf(requireNonNull(initialTables, "initialTables is null"));
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                queryRunner.installPlugin(new FirebirdPlugin());
                queryRunner.createCatalog(FIREBIRD, FIREBIRD, connectorProperties);
                log.info("%s catalog properties: %s", FIREBIRD, connectorProperties);
                copyTpchTables(server, queryRunner, initialTables);

                return queryRunner;
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    private static void copyTpchTables(
            TestingFirebirdServer server,
            QueryRunner queryRunner,
            List<TpchTable<?>> initialTables)
            throws SQLException
    {
        try (Connection connection = server.getConnection();
                Statement statement = connection.createStatement()) {
            for (TpchTable<?> table : initialTables) {
                long start = System.nanoTime();
                String tableName = table.getTableName();
                log.info("Running import for %s", tableName);

                String tpchTableName = format("tpch.%s.%s", TINY_SCHEMA_NAME, tableName);
                MaterializedResult rows = queryRunner.execute(format("SELECT * FROM %s", tpchTableName));
                copyAndIngestTpchData(connection, statement, rows, tableName);

                assertThat(queryRunner.execute("SELECT count(*) FROM firebird.default." + tableName).getOnlyValue())
                        .as("Table is not loaded properly: %s", tableName)
                        .isEqualTo(queryRunner.execute("SELECT count(*) FROM " + tpchTableName).getOnlyValue());

                log.info("Imported %s rows from %s in %s", rows.getRowCount(), tpchTableName, nanosSince(start));
            }
        }
    }

    private static void copyAndIngestTpchData(Connection connection, Statement statement, MaterializedResult rows, String tableName)
            throws SQLException
    {
        int columnSize = rows.getTypes().size();
        ImmutableList.Builder<String> columnNames = ImmutableList.builderWithExpectedSize(columnSize);
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builderWithExpectedSize(columnSize);
        ImmutableList.Builder<String> columnHolders = ImmutableList.builderWithExpectedSize(columnSize);
        ImmutableList.Builder<String> columnDefinitions = ImmutableList.builderWithExpectedSize(columnSize);

        for (int i = 0; i < columnSize; i++) {
            String columnName = rows.getColumnNames().get(i);
            Type columnType = rows.getTypes().get(i);
            columnNames.add(columnName);
            columnTypes.add(columnType);
            columnHolders.add("?");
            columnDefinitions.add(columnName + " " + convertType(columnType));
        }

        String createTableStatement = format(
                "CREATE TABLE %s (%s)",
                tableName,
                String.join(",", columnDefinitions.build()));
        log.info("Creating table %s using definition '%s'", tableName, createTableStatement);
        statement.executeUpdate(createTableStatement);

        String insertStatement = format(
                "INSERT INTO %s (%s) VALUES (%s)",
                tableName,
                String.join(",", columnNames.build()),
                String.join(",", columnHolders.build()));
        try (PreparedStatement preparedStatement = connection.prepareStatement(insertStatement)) {
            copyAndIngestTpchData(preparedStatement, rows, columnTypes.build());
        }
    }

    private static void copyAndIngestTpchData(PreparedStatement preparedStatement, MaterializedResult rows, List<Type> columnTypes)
            throws SQLException
    {
        int batch = 0;
        for (MaterializedRow row : rows.getMaterializedRows()) {
            copyAndIngestTpchData(preparedStatement, row, columnTypes);
            preparedStatement.addBatch();
            batch++;
            if (batch >= MAX_BATCH) {
                preparedStatement.executeBatch();
                batch = 0;
            }
        }
        if (batch > 0) {
            preparedStatement.executeBatch();
        }
    }

    private static void copyAndIngestTpchData(PreparedStatement preparedStatement, MaterializedRow row, List<Type> columnTypes)
            throws SQLException
    {
        for (int i = 0; i < row.getFieldCount(); i++) {
            copyAndIngestTpchData(preparedStatement, row.getField(i), columnTypes.get(i), i + 1);
        }
    }

    private static void copyAndIngestTpchData(PreparedStatement preparedStatement, Object value, Type columnType, int index)
            throws SQLException
    {
        switch (columnType.getBaseName()) {
            case "varchar":
                preparedStatement.setString(index, value.toString());
                break;
            case "integer":
                preparedStatement.setInt(index, Integer.parseInt(value.toString()));
                break;
            case "bigint":
                preparedStatement.setLong(index, Long.parseLong(value.toString()));
                break;
            case "double":
                preparedStatement.setDouble(index, Double.parseDouble(value.toString()));
                break;
            case "date":
                preparedStatement.setDate(index, Date.valueOf(value.toString()));
                break;
            default:
                preparedStatement.setObject(index, value);
        }
    }

    private static String convertType(Type type)
    {
        if (type.getBaseName().equals("double")) {
            // Firebird column type is double precision
            return "double precision";
        }
        return type.getDisplayName();
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging logger = Logging.initialize();
        logger.setLevel("io.trino.plugin.firebird", Level.DEBUG);
        logger.setLevel("io.trino", Level.INFO);

        QueryRunner queryRunner = builder(new TestingFirebirdServer())
                .addCoordinatorProperty("http-server.http.port", "8080")
                .setInitialTables(TpchTable.getTables())
                .build();

        Logger log = Logger.get(FirebirdQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
