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

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for JDBC prepared statements.
 * <p>
 * Migrated from the Tempto-based TestPreparedStatements to JUnit 5.
 * Uses memory catalog for write tests and TPCH for read tests.
 */
@ProductTest
@RequiresEnvironment(JdbcBasicEnvironment.class)
@TestGroup.Jdbc
class TestPreparedStatements
{
    private static String allTypesDdl(String tableName)
    {
        return """
            CREATE TABLE %s (
                c_tinyint TINYINT,
                c_smallint SMALLINT,
                c_int INTEGER,
                c_bigint BIGINT,
                c_real REAL,
                c_double DOUBLE,
                c_decimal DECIMAL(10,0),
                c_decimal_w_params DECIMAL(10,5),
                c_timestamp TIMESTAMP(3),
                c_date DATE,
                c_string VARCHAR,
                c_varchar VARCHAR(10),
                c_char CHAR(10),
                c_boolean BOOLEAN,
                c_varbinary VARBINARY
            )
            """.formatted(tableName);
    }

    private static String uniqueTableName(TestInfo testInfo)
    {
        return "memory.test.all_types_" + testInfo.getTestMethod().orElseThrow().getName();
    }

    @Test
    void preparedSelectApi(JdbcBasicEnvironment env)
            throws SQLException
    {
        // Test prepared statement with parameter for SELECT
        try (Connection connection = env.createTrinoConnection();
                PreparedStatement ps = connection.prepareStatement(
                        "SELECT nationkey FROM tpch.tiny.nation WHERE nationkey = ?")) {
            // Test with matching value (nationkey 10 = IRAN)
            ps.setLong(1, 10);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong(1)).isEqualTo(10);
                assertThat(rs.next()).isFalse();
            }

            // Test with null - should return no rows
            ps.setNull(1, Types.BIGINT);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isFalse();
            }

            // Test with non-matching value
            ps.setLong(1, 999);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isFalse();
            }
        }
    }

    @Test
    void preparedSelectSql(JdbcBasicEnvironment env)
            throws SQLException
    {
        // Test PREPARE/EXECUTE SQL syntax
        try (Connection connection = env.createTrinoConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("PREPARE ps1 FROM SELECT nationkey FROM tpch.tiny.nation WHERE nationkey = ?");

            // Test with matching value
            try (ResultSet rs = statement.executeQuery("EXECUTE ps1 USING 10")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong(1)).isEqualTo(10);
                assertThat(rs.next()).isFalse();
            }

            // Test with NULL
            try (ResultSet rs = statement.executeQuery("EXECUTE ps1 USING NULL")) {
                assertThat(rs.next()).isFalse();
            }

            // Test with non-matching value
            try (ResultSet rs = statement.executeQuery("EXECUTE ps1 USING 999")) {
                assertThat(rs.next()).isFalse();
            }
        }
    }

    @Test
    void executeImmediateSelectSql(JdbcBasicEnvironment env)
            throws SQLException
    {
        // Test EXECUTE IMMEDIATE SQL syntax
        try (Connection connection = env.createTrinoConnection();
                Statement statement = connection.createStatement()) {
            // Test with matching value
            try (ResultSet rs = statement.executeQuery(
                    "EXECUTE IMMEDIATE 'SELECT nationkey FROM tpch.tiny.nation WHERE nationkey = ?' USING 10")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong(1)).isEqualTo(10);
                assertThat(rs.next()).isFalse();
            }

            // Test with NULL
            try (ResultSet rs = statement.executeQuery(
                    "EXECUTE IMMEDIATE 'SELECT nationkey FROM tpch.tiny.nation WHERE nationkey = ?' USING NULL")) {
                assertThat(rs.next()).isFalse();
            }

            // Test with non-matching value
            try (ResultSet rs = statement.executeQuery(
                    "EXECUTE IMMEDIATE 'SELECT nationkey FROM tpch.tiny.nation WHERE nationkey = ?' USING 999")) {
                assertThat(rs.next()).isFalse();
            }
        }
    }

    @Test
    void preparedInsertVarbinaryApi(JdbcBasicEnvironment env, TestInfo testInfo)
            throws SQLException
    {
        String tableName = uniqueTableName(testInfo);
        try (Connection connection = env.createTrinoConnection();
                Statement statement = connection.createStatement()) {
            // Setup
            statement.execute("CREATE SCHEMA IF NOT EXISTS memory.test");
            statement.execute("DROP TABLE IF EXISTS " + tableName);
            statement.execute(allTypesDdl(tableName));

            // Insert with nulls except for varbinary
            byte[] varbinaryData = new byte[] {0, 1, 2, 3, 0, 42, -7};
            try (PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO " + tableName + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
                // Set all to null except varbinary
                for (int i = 1; i <= 14; i++) {
                    ps.setNull(i, Types.NULL);
                }
                ps.setBytes(15, varbinaryData);
                ps.executeUpdate();
            }

            // Verify
            try (ResultSet rs = statement.executeQuery("SELECT * FROM " + tableName)) {
                assertThat(rs.next()).isTrue();
                // All values should be null except c_varbinary
                for (int i = 1; i <= 14; i++) {
                    rs.getObject(i);
                    assertThat(rs.wasNull()).isTrue();
                }
                assertThat(rs.getBytes(15)).isEqualTo(varbinaryData);
            }

            // Cleanup
            statement.execute("DROP TABLE " + tableName);
        }
    }

    @Test
    void preparedInsertApi(JdbcBasicEnvironment env, TestInfo testInfo)
            throws SQLException
    {
        String tableName = uniqueTableName(testInfo);
        try (Connection connection = env.createTrinoConnection();
                Statement statement = connection.createStatement()) {
            // Setup
            statement.execute("CREATE SCHEMA IF NOT EXISTS memory.test");
            statement.execute("DROP TABLE IF EXISTS " + tableName);
            statement.execute(allTypesDdl(tableName));

            byte[] varbinaryData = new byte[] {0, 1, 2, 3, 0, 42, -7};

            // Insert first row with all values
            try (PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO " + tableName + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
                ps.setByte(1, (byte) 127);
                ps.setShort(2, (short) 32767);
                ps.setInt(3, 2147483647);
                ps.setLong(4, 9223372036854775807L);
                ps.setFloat(5, 123.345f);
                ps.setDouble(6, 234.567);
                ps.setBigDecimal(7, BigDecimal.valueOf(345));
                ps.setBigDecimal(8, new BigDecimal("345.678"));
                ps.setTimestamp(9, Timestamp.valueOf("2015-05-10 12:15:35"));
                ps.setDate(10, Date.valueOf("2015-05-10"));
                ps.setString(11, "ala ma kota");
                ps.setString(12, "ala ma kot");
                ps.setString(13, "    ala ma");
                ps.setBoolean(14, true);
                ps.setBytes(15, varbinaryData);
                ps.executeUpdate();
            }

            // Insert second row with different values
            try (PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO " + tableName + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
                ps.setByte(1, (byte) 1);
                ps.setShort(2, (short) 2);
                ps.setInt(3, 3);
                ps.setLong(4, 4L);
                ps.setFloat(5, 5.6f);
                ps.setDouble(6, 7.8);
                ps.setBigDecimal(7, BigDecimal.valueOf(91));
                ps.setBigDecimal(8, BigDecimal.valueOf(2.3));
                ps.setTimestamp(9, Timestamp.valueOf("2012-05-10 01:35:15"));
                ps.setDate(10, Date.valueOf("2014-03-10"));
                ps.setString(11, "abc");
                ps.setString(12, "def");
                ps.setString(13, "       ghi");
                ps.setBoolean(14, false);
                ps.setBytes(15, varbinaryData);
                ps.executeUpdate();
            }

            // Insert third row with all nulls
            try (PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO " + tableName + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
                for (int i = 1; i <= 15; i++) {
                    ps.setNull(i, Types.NULL);
                }
                ps.executeUpdate();
            }

            // Verify we have 3 rows
            try (ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt(1)).isEqualTo(3);
            }

            // Verify first row values (all 15 columns)
            try (ResultSet rs = statement.executeQuery(
                    "SELECT * FROM " + tableName + " WHERE c_tinyint = 127")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getByte(1)).isEqualTo((byte) 127);
                assertThat(rs.getShort(2)).isEqualTo((short) 32767);
                assertThat(rs.getInt(3)).isEqualTo(2147483647);
                assertThat(rs.getLong(4)).isEqualTo(9223372036854775807L);
                assertThat(rs.getFloat(5)).isEqualTo(123.345f);
                assertThat(rs.getDouble(6)).isEqualTo(234.567);
                assertThat(rs.getBigDecimal(7)).isEqualByComparingTo(BigDecimal.valueOf(345));
                assertThat(rs.getBigDecimal(8)).isEqualByComparingTo(new BigDecimal("345.678"));
                assertThat(rs.getTimestamp(9)).isEqualTo(Timestamp.valueOf("2015-05-10 12:15:35"));
                assertThat(rs.getDate(10)).isEqualTo(Date.valueOf("2015-05-10"));
                assertThat(rs.getString(11)).isEqualTo("ala ma kota");
                assertThat(rs.getString(12)).isEqualTo("ala ma kot");
                assertThat(rs.getString(13)).isEqualTo("    ala ma");
                assertThat(rs.getBoolean(14)).isTrue();
                assertThat(rs.getBytes(15)).isEqualTo(varbinaryData);
            }

            // Cleanup
            statement.execute("DROP TABLE " + tableName);
        }
    }

    @Test
    void preparedInsertSql(JdbcBasicEnvironment env, TestInfo testInfo)
            throws SQLException
    {
        String tableName = uniqueTableName(testInfo);
        try (Connection connection = env.createTrinoConnection();
                Statement statement = connection.createStatement()) {
            // Setup
            statement.execute("CREATE SCHEMA IF NOT EXISTS memory.test");
            statement.execute("DROP TABLE IF EXISTS " + tableName);
            statement.execute(allTypesDdl(tableName));

            // Use PREPARE/EXECUTE for insert
            statement.execute("PREPARE ps1 FROM INSERT INTO " + tableName + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

            // Insert with values using EXECUTE
            statement.execute("""
                    EXECUTE ps1 USING
                    CAST(127 AS TINYINT),
                    CAST(32767 AS SMALLINT),
                    2147483647,
                    9223372036854775807,
                    CAST(123.345 AS REAL),
                    CAST(234.567 AS DOUBLE),
                    CAST(345 AS DECIMAL(10)),
                    CAST(345.678 AS DECIMAL(10,5)),
                    TIMESTAMP '2015-05-10 12:15:35',
                    DATE '2015-05-10',
                    'ala ma kota',
                    'ala ma kot',
                    CAST('ala ma' AS CHAR(10)),
                    TRUE,
                    X'00010203002AF9'
                    """);

            // Insert second row with different values
            statement.execute("""
                    EXECUTE ps1 USING
                    CAST(1 AS TINYINT),
                    CAST(2 AS SMALLINT),
                    3,
                    4,
                    CAST(5.6 AS REAL),
                    CAST(7.8 AS DOUBLE),
                    CAST(91 AS DECIMAL(10)),
                    CAST(2.3 AS DECIMAL(10,5)),
                    TIMESTAMP '2012-05-10 01:35:15',
                    DATE '2014-03-10',
                    'abc',
                    'def',
                    CAST('ghi' AS CHAR(10)),
                    FALSE,
                    X'00010203002AF9'
                    """);

            // Insert third row with all nulls
            statement.execute("""
                    EXECUTE ps1 USING
                    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
                    """);

            // Verify we have 3 rows
            try (ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt(1)).isEqualTo(3);
            }

            // Verify first row values (all 15 columns)
            try (ResultSet rs = statement.executeQuery(
                    "SELECT * FROM " + tableName + " WHERE c_tinyint = 127")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getByte(1)).isEqualTo((byte) 127);
                assertThat(rs.getShort(2)).isEqualTo((short) 32767);
                assertThat(rs.getInt(3)).isEqualTo(2147483647);
                assertThat(rs.getLong(4)).isEqualTo(9223372036854775807L);
                assertThat(rs.getFloat(5)).isEqualTo(123.345f);
                assertThat(rs.getDouble(6)).isEqualTo(234.567);
                assertThat(rs.getBigDecimal(7)).isEqualByComparingTo(BigDecimal.valueOf(345));
                assertThat(rs.getBigDecimal(8)).isEqualByComparingTo(new BigDecimal("345.678"));
                assertThat(rs.getTimestamp(9)).isEqualTo(Timestamp.valueOf("2015-05-10 12:15:35"));
                assertThat(rs.getDate(10)).isEqualTo(Date.valueOf("2015-05-10"));
                assertThat(rs.getString(11)).isEqualTo("ala ma kota");
                assertThat(rs.getString(12)).isEqualTo("ala ma kot");
                assertThat(rs.getString(13)).isEqualTo("ala ma    ");
                assertThat(rs.getBoolean(14)).isTrue();
                assertThat(rs.getBytes(15)).isEqualTo(new byte[] {0, 1, 2, 3, 0, 42, -7});
            }

            // Cleanup
            statement.execute("DROP TABLE " + tableName);
        }
    }

    @Test
    void executeImmediateInsertSql(JdbcBasicEnvironment env, TestInfo testInfo)
            throws SQLException
    {
        String tableName = uniqueTableName(testInfo);
        try (Connection connection = env.createTrinoConnection();
                Statement statement = connection.createStatement()) {
            // Setup
            statement.execute("CREATE SCHEMA IF NOT EXISTS memory.test");
            statement.execute("DROP TABLE IF EXISTS " + tableName);
            statement.execute(allTypesDdl(tableName));

            // Use EXECUTE IMMEDIATE for insert - first row with values
            statement.execute("EXECUTE IMMEDIATE 'INSERT INTO " + tableName + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)' USING " +
                    "CAST(127 AS TINYINT), " +
                    "CAST(32767 AS SMALLINT), " +
                    "2147483647, " +
                    "9223372036854775807, " +
                    "CAST(123.345 AS REAL), " +
                    "CAST(234.567 AS DOUBLE), " +
                    "CAST(345 AS DECIMAL(10)), " +
                    "CAST(345.678 AS DECIMAL(10,5)), " +
                    "TIMESTAMP '2015-05-10 12:15:35', " +
                    "DATE '2015-05-10', " +
                    "'ala ma kota', " +
                    "'ala ma kot', " +
                    "CAST('ala ma' AS CHAR(10)), " +
                    "TRUE, " +
                    "X'00010203002AF9'");

            // Insert second row with all nulls
            statement.execute("EXECUTE IMMEDIATE 'INSERT INTO " + tableName + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)' USING " +
                    "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL");

            // Verify we have 2 rows
            try (ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt(1)).isEqualTo(2);
            }

            // Verify first row values (all 15 columns)
            try (ResultSet rs = statement.executeQuery(
                    "SELECT * FROM " + tableName + " WHERE c_tinyint = 127")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getByte(1)).isEqualTo((byte) 127);
                assertThat(rs.getShort(2)).isEqualTo((short) 32767);
                assertThat(rs.getInt(3)).isEqualTo(2147483647);
                assertThat(rs.getLong(4)).isEqualTo(9223372036854775807L);
                assertThat(rs.getFloat(5)).isEqualTo(123.345f);
                assertThat(rs.getDouble(6)).isEqualTo(234.567);
                assertThat(rs.getBigDecimal(7)).isEqualByComparingTo(BigDecimal.valueOf(345));
                assertThat(rs.getBigDecimal(8)).isEqualByComparingTo(new BigDecimal("345.678"));
                assertThat(rs.getTimestamp(9)).isEqualTo(Timestamp.valueOf("2015-05-10 12:15:35"));
                assertThat(rs.getDate(10)).isEqualTo(Date.valueOf("2015-05-10"));
                assertThat(rs.getString(11)).isEqualTo("ala ma kota");
                assertThat(rs.getString(12)).isEqualTo("ala ma kot");
                assertThat(rs.getString(13)).isEqualTo("ala ma    ");
                assertThat(rs.getBoolean(14)).isTrue();
                assertThat(rs.getBytes(15)).isEqualTo(new byte[] {0, 1, 2, 3, 0, 42, -7});
            }

            // Cleanup
            statement.execute("DROP TABLE " + tableName);
        }
    }

    @Test
    void preparedInsertVarbinarySql(JdbcBasicEnvironment env, TestInfo testInfo)
            throws SQLException
    {
        String tableName = uniqueTableName(testInfo);
        try (Connection connection = env.createTrinoConnection();
                Statement statement = connection.createStatement()) {
            // Setup
            statement.execute("CREATE SCHEMA IF NOT EXISTS memory.test");
            statement.execute("DROP TABLE IF EXISTS " + tableName);
            statement.execute(allTypesDdl(tableName));

            // Use PREPARE/EXECUTE with varbinary
            statement.execute("PREPARE ps1 FROM INSERT INTO " + tableName + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

            statement.execute("""
                    EXECUTE ps1 USING
                    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                    X'00010203002AF9'
                    """);

            // Verify varbinary value
            try (ResultSet rs = statement.executeQuery("SELECT c_varbinary FROM " + tableName)) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getBytes(1)).isEqualTo(new byte[] {0, 1, 2, 3, 0, 42, -7});
            }

            // Cleanup
            statement.execute("DROP TABLE " + tableName);
        }
    }

    @Test
    void executeImmediateVarbinarySql(JdbcBasicEnvironment env, TestInfo testInfo)
            throws SQLException
    {
        String tableName = uniqueTableName(testInfo);
        try (Connection connection = env.createTrinoConnection();
                Statement statement = connection.createStatement()) {
            // Setup
            statement.execute("CREATE SCHEMA IF NOT EXISTS memory.test");
            statement.execute("DROP TABLE IF EXISTS " + tableName);
            statement.execute(allTypesDdl(tableName));

            // Use EXECUTE IMMEDIATE with varbinary
            statement.execute("EXECUTE IMMEDIATE 'INSERT INTO " + tableName + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)' USING " +
                    "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, " +
                    "X'00010203002AF9'");

            // Verify varbinary value
            try (ResultSet rs = statement.executeQuery("SELECT c_varbinary FROM " + tableName)) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getBytes(1)).isEqualTo(new byte[] {0, 1, 2, 3, 0, 42, -7});
            }

            // Cleanup
            statement.execute("DROP TABLE " + tableName);
        }
    }
}
