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
package io.trino.plugin.sqlite;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.spi.type.Type;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.sqlite.SqliteQueryRunner.SCHEMA;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

final class SqliteTpchTables
{
    private static final Logger log = Logger.get(SqliteTpchTables.class);
    private static final int MAX_BATCH = 500;

    private SqliteTpchTables() {}

    public static void copyTpchTables(
            TestingSqliteServer server,
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

                assertThat(queryRunner.execute("SELECT count(*) FROM sqlite." + SCHEMA + "." + tableName).getOnlyValue())
                        .as("Table is not loaded properly: %s", tableName)
                        .isEqualTo(queryRunner.execute("SELECT count(*) FROM " + tpchTableName).getOnlyValue());

                log.info("Imported %s rows from %s in %s", rows.getRowCount(), tpchTableName, nanosSince(start));
            }
        }
    }

    public static void copyAndIngestTpchDataFromSourceToTarget(
            MaterializedResult rows,
            TestingSqliteServer server,
            String tableName)
            throws SQLException
    {
        try (Connection connection = server.getConnection();
                Statement statement = connection.createStatement()) {
            copyAndIngestTpchData(connection, statement, rows, tableName);
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
            columnDefinitions.add(columnName + " " + columnType.getDisplayName());
        }

        // FIXME: If the Sqlite database file already exist then the table already exist
        statement.executeUpdate("DROP TABLE IF EXISTS " + tableName);

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
            default:
                preparedStatement.setObject(index, value);
        }
    }
}
