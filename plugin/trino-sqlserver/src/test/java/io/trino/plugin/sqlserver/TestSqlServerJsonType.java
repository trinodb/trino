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
package io.trino.plugin.sqlserver;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for native JSON data type handling in SQL Server connector.
 *
 * The native JSON data type is available in:
 * - SQL Server 2025 (17.x)
 * - Azure SQL Database
 * - Azure SQL Managed Instance
 *
 * This test uses SQL Server 2025 container to test native JSON type support.
 */
public class TestSqlServerJsonType
        extends AbstractTestQueryFramework
{
    // 2025-latest has AVX issues with Rosetta: https://github.com/microsoft/mssql-docker/issues/940
    private static final String SQL_SERVER_2025_VERSION = "2025-latest";

    private TestingSqlServer sqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        sqlServer = new TestingSqlServer(SQL_SERVER_2025_VERSION);
        closeAfterClass(sqlServer);
        return SqlServerQueryRunner.builder(sqlServer)
                .build();
    }

    /**
     * Test that native JSON columns are visible and queryable.
     * This is the main reproduction test for the JSON column filtering bug.
     */
    @Test
    public void testNativeJsonColumnIsVisible()
    {
        try (TestTable table = new TestTable(
                sqlServer::execute,
                "test_native_json",
                "(id int, data json)")) {
            sqlServer.execute("INSERT INTO " + table.getName() + " VALUES (1, '{\"key\": \"value\"}')");

            // With native JSON support, the 'data' column should be visible as JSON type
            assertThat(query("DESCRIBE " + table.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES ('id', 'integer', '', ''), ('data', 'json', '', '')");
        }
    }

    /**
     * Test that SELECT * works on tables with JSON columns.
     */
    @Test
    public void testSelectFromJsonColumn()
    {
        try (TestTable table = new TestTable(
                sqlServer::execute,
                "test_json_select",
                "(id int, data json)")) {
            sqlServer.execute("INSERT INTO " + table.getName() + " VALUES (1, '{\"name\": \"test\"}')");

            // With native JSON support, JSON should be readable as JSON type
            assertThat(query("SELECT id, data FROM " + table.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES (1, JSON '{\"name\": \"test\"}')");
        }
    }

    /**
     * Verify JDBC metadata correctly reports native JSON columns.
     * This validates that the Microsoft JDBC driver fix (PR #2883) works correctly
     * by checking that DatabaseMetaData.getColumns() returns both regular and JSON columns.
     */
    @Test
    public void testJdbcMetadataReportsJsonColumns()
            throws Exception
    {
        try (TestTable table = new TestTable(
                sqlServer::execute,
                "test_json_metadata",
                "(id int, json_col json)")) {
            sqlServer.execute("INSERT INTO " + table.getName() + " VALUES (1, '{\"key\": \"value\"}')");

            try (Connection connection = DriverManager.getConnection(
                        sqlServer.getJdbcUrl(),
                        sqlServer.getUsername(),
                        sqlServer.getPassword())) {
                // Check DatabaseMetaData.getColumns()
                System.out.println("=== DatabaseMetaData.getColumns() for native JSON ===");
                DatabaseMetaData metadata = connection.getMetaData();
                try (ResultSet rs = metadata.getColumns(null, "dbo", table.getName().replace("dbo.", ""), null)) {
                    int columnCount = 0;
                    while (rs.next()) {
                        columnCount++;
                        String columnName = rs.getString("COLUMN_NAME");
                        int dataType = rs.getInt("DATA_TYPE");
                        String typeName = rs.getString("TYPE_NAME");
                        int columnSize = rs.getInt("COLUMN_SIZE");
                        System.out.printf("Column: %s, DATA_TYPE: %d, TYPE_NAME: %s, COLUMN_SIZE: %d%n",
                                columnName, dataType, typeName, columnSize);
                    }
                    System.out.println("Total columns from getColumns(): " + columnCount);
                    // Should find 2 columns (id and json_col)
                    assertThat(columnCount).isEqualTo(2);
                }

                // Check ResultSetMetaData from SELECT *
                System.out.println("\n=== ResultSetMetaData from SELECT * ===");
                try (Statement stmt = connection.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT * FROM " + table.getName())) {
                    ResultSetMetaData rsmd = rs.getMetaData();
                    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                        String columnName = rsmd.getColumnName(i);
                        int dataType = rsmd.getColumnType(i);
                        String typeName = rsmd.getColumnTypeName(i);
                        int precision = rsmd.getPrecision(i);
                        System.out.printf("Column: %s, DATA_TYPE: %d, TYPE_NAME: %s, PRECISION: %d%n",
                                columnName, dataType, typeName, precision);
                    }
                    assertThat(rsmd.getColumnCount()).isEqualTo(2);
                }
            }
        }
    }

    /**
     * Verify passthrough query works with JSON columns even if DESCRIBE doesn't.
     */
    @Test
    public void testPassthroughQueryWithJson()
    {
        try (TestTable table = new TestTable(
                sqlServer::execute,
                "test_json_passthrough",
                "(id int, data json)")) {
            sqlServer.execute("INSERT INTO " + table.getName() + " VALUES (1, '{\"test\": true}')");

            // Passthrough should work and return JSON type
            // SQL Server may format JSON without spaces after colons
            assertThat(query("SELECT * FROM TABLE(sqlserver.system.query(query => 'SELECT * FROM " + table.getName() + "'))"))
                    .skippingTypesCheck()
                    .matches("VALUES (1, JSON '{\"test\":true}')"); // Note: no space after colon
        }
    }
}
