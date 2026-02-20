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
package io.trino.tests.product.jdbc;

import io.trino.jdbc.TrinoConnection;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.TpchTableResults;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static java.util.Locale.CHINESE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for core JDBC driver functionality.
 * <p>
 * Migrated from the Tempto-based TestJdbc to JUnit 5.
 * Uses TPCH catalog (built-in) instead of Hive for test data.
 */
@ProductTest
@RequiresEnvironment(JdbcBasicEnvironment.class)
@TestGroup.Jdbc
class TestJdbc
{
    @Test
    void shouldExecuteQuery(JdbcBasicEnvironment env)
    {
        QueryResult result = env.executeTrino("SELECT * FROM tpch.tiny.nation");
        assertThat(result).containsOnly(TpchTableResults.NATION_ROWS);
    }

    @Test
    void shouldInsertSelectQuery(JdbcBasicEnvironment env)
            throws SQLException
    {
        // Create a schema in the memory catalog for testing
        try (Connection connection = env.createTrinoConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA IF NOT EXISTS memory.test");
            statement.execute("DROP TABLE IF EXISTS memory.test.nation_copy");

            // Verify table doesn't exist yet (or is empty after drop)
            int insertCount = statement.executeUpdate(
                    "CREATE TABLE memory.test.nation_copy AS SELECT * FROM tpch.tiny.nation");
            assertThat(insertCount).isEqualTo(TpchTableResults.NATION_ROW_COUNT);

            // Verify data was inserted with full row validation
            QueryResult result = env.executeTrino("SELECT * FROM memory.test.nation_copy");
            assertThat(result).containsOnly(TpchTableResults.NATION_ROWS);

            // Cleanup
            statement.execute("DROP TABLE memory.test.nation_copy");
        }
    }

    @Test
    void shouldExecuteQueryWithSelectedCatalogAndSchema(JdbcBasicEnvironment env)
            throws SQLException
    {
        try (Connection connection = env.createTrinoConnection()) {
            connection.setCatalog("tpch");
            connection.setSchema("tiny");

            try (Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery("SELECT * FROM nation")) {
                QueryResult result = QueryResult.forResultSet(rs);
                assertThat(result).containsOnly(TpchTableResults.NATION_ROWS);
            }
        }
    }

    @Test
    void shouldSetTimezone(JdbcBasicEnvironment env)
            throws SQLException
    {
        String timeZoneId = "Indian/Kerguelen";
        try (Connection connection = env.createTrinoConnection()) {
            ((TrinoConnection) connection).setTimeZoneId(timeZoneId);

            try (Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery("SELECT current_timezone()")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString(1)).isEqualTo(timeZoneId);
            }
        }
    }

    @Test
    void shouldSetLocale(JdbcBasicEnvironment env)
            throws SQLException
    {
        try (Connection connection = env.createTrinoConnection()) {
            ((TrinoConnection) connection).setLocale(CHINESE);

            try (Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery("SELECT date_format(TIMESTAMP '2001-01-09 09:04', '%M')")) {
                assertThat(rs.next()).isTrue();
                // Chinese month name for January
                assertThat(rs.getString(1)).isEqualTo("\u4e00\u6708");
            }
        }
    }

    @Test
    void shouldGetSchemas(JdbcBasicEnvironment env)
            throws SQLException
    {
        try (Connection connection = env.createTrinoConnection();
                ResultSet rs = connection.getMetaData().getSchemas("tpch", null)) {
            boolean foundTiny = false;
            while (rs.next()) {
                if ("tiny".equals(rs.getString("TABLE_SCHEM")) &&
                        "tpch".equals(rs.getString("TABLE_CATALOG"))) {
                    foundTiny = true;
                }
            }
            assertThat(foundTiny).isTrue();
        }
    }

    @Test
    void shouldGetTables(JdbcBasicEnvironment env)
            throws SQLException
    {
        try (Connection connection = env.createTrinoConnection();
                ResultSet rs = connection.getMetaData().getTables("tpch", "tiny", "nation", null)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("TABLE_CAT")).isEqualTo("tpch");
            assertThat(rs.getString("TABLE_SCHEM")).isEqualTo("tiny");
            assertThat(rs.getString("TABLE_NAME")).isEqualTo("nation");
            assertThat(rs.getString("TABLE_TYPE")).isEqualTo("TABLE");
        }
    }

    @Test
    void shouldGetColumns(JdbcBasicEnvironment env)
            throws SQLException
    {
        try (Connection connection = env.createTrinoConnection();
                ResultSet rs = connection.getMetaData().getColumns("tpch", "tiny", "nation", null)) {
            // Verify we get the expected columns: nationkey, name, regionkey, comment
            int columnCount = 0;
            while (rs.next()) {
                columnCount++;
                String columnName = rs.getString("COLUMN_NAME");
                assertThat(columnName).isIn("nationkey", "name", "regionkey", "comment");
            }
            assertThat(columnCount).isEqualTo(4);
        }
    }

    @Test
    void shouldGetTableTypes(JdbcBasicEnvironment env)
            throws SQLException
    {
        try (Connection connection = env.createTrinoConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet rs = metaData.getTableTypes()) {
                boolean hasTable = false;
                boolean hasView = false;
                while (rs.next()) {
                    String tableType = rs.getString("TABLE_TYPE");
                    if ("TABLE".equals(tableType)) {
                        hasTable = true;
                    }
                    if ("VIEW".equals(tableType)) {
                        hasView = true;
                    }
                }
                assertThat(hasTable).isTrue();
                assertThat(hasView).isTrue();
            }
        }
    }

    @Test
    void testSessionProperties(JdbcBasicEnvironment env)
            throws SQLException
    {
        String joinDistributionType = "join_distribution_type";
        String defaultValue = "AUTOMATIC";

        try (Connection connection = env.createTrinoConnection()) {
            // Get current value
            assertThat(getSessionProperty(connection, joinDistributionType)).isEqualTo(defaultValue);

            // Set new value
            setSessionProperty(connection, joinDistributionType, "BROADCAST");
            assertThat(getSessionProperty(connection, joinDistributionType)).isEqualTo("BROADCAST");

            // Reset to default
            resetSessionProperty(connection, joinDistributionType);
            assertThat(getSessionProperty(connection, joinDistributionType)).isEqualTo(defaultValue);
        }
    }

    /**
     * Tests that prepared statements are properly deallocated to avoid resource leaks.
     * Same as io.trino.jdbc.TestJdbcPreparedStatement#testDeallocate().
     */
    @Test
    void testDeallocate(JdbcBasicEnvironment env)
            throws SQLException
    {
        try (Connection connection = env.createTrinoConnection()) {
            for (int i = 0; i < 200; i++) {
                try (PreparedStatement preparedStatement = connection.prepareStatement(
                        "SELECT '" + "a".repeat(300) + "'");
                        ResultSet rs = preparedStatement.executeQuery()) {
                    // Just execute and close - testing that resources are released
                    assertThat(rs.next()).isTrue();
                }
            }
        }
    }

    // Helper methods for session properties

    private static String getSessionProperty(Connection connection, String key)
            throws SQLException
    {
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SHOW SESSION")) {
            while (rs.next()) {
                if (rs.getString("Name").equals(key)) {
                    return rs.getString("Value");
                }
            }
        }
        return null;
    }

    private static String getSessionPropertyDefault(Connection connection, String key)
            throws SQLException
    {
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SHOW SESSION")) {
            while (rs.next()) {
                if (rs.getString("Name").equals(key)) {
                    return rs.getString("Default");
                }
            }
        }
        return null;
    }

    private static void setSessionProperty(Connection connection, String key, String value)
            throws SQLException
    {
        @SuppressWarnings("resource")
        TrinoConnection trinoConnection = connection.unwrap(TrinoConnection.class);
        trinoConnection.setSessionProperty(key, value);
    }

    private static void resetSessionProperty(Connection connection, String key)
            throws SQLException
    {
        setSessionProperty(connection, key, getSessionPropertyDefault(connection, key));
    }
}
