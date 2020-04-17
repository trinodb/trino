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
package io.prestosql.plugin.phoenix;

import io.prestosql.Session;
import io.prestosql.plugin.jdbc.UnsupportedTypeHandling;
import io.prestosql.testing.AbstractTestIntegrationSmokeTest;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static io.prestosql.plugin.jdbc.TypeHandlingJdbcPropertiesProvider.UNSUPPORTED_TYPE_HANDLING;
import static io.prestosql.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.prestosql.plugin.phoenix.PhoenixQueryRunner.createPhoenixQueryRunner;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestPhoenixIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private TestingPhoenixServer testingPhoenixServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingPhoenixServer = TestingPhoenixServer.getInstance();
        return createPhoenixQueryRunner(testingPhoenixServer);
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        TestingPhoenixServer.shutDown();
    }

    @Override
    public void testShowCreateTable()
    {
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("CREATE TABLE phoenix.tpch.orders (\n" +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar(1),\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar(15),\n" +
                        "   clerk varchar(15),\n" +
                        "   shippriority integer,\n" +
                        "   comment varchar(79)\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   rowkeys = 'ROWKEY',\n" +
                        "   salt_buckets = 10\n" +
                        ")");
    }

    @Test
    public void testSchemaOperations()
    {
        assertUpdate("CREATE SCHEMA new_schema");
        assertUpdate("CREATE TABLE new_schema.test (x bigint)");

        try {
            getQueryRunner().execute("DROP SCHEMA new_schema");
            fail("Should not be able to drop non-empty schema");
        }
        catch (RuntimeException e) {
        }

        assertUpdate("DROP TABLE new_schema.test");
        assertUpdate("DROP SCHEMA new_schema");
    }

    @Test
    public void testMultipleSomeColumnsRangesPredicate()
    {
        assertQuery("SELECT orderkey, shippriority, clerk, totalprice, custkey FROM orders WHERE orderkey BETWEEN 10 AND 50 OR orderkey BETWEEN 100 AND 150");
    }

    @Test
    public void testUnsupportedType()
            throws Exception
    {
        executeInPhoenix("CREATE TABLE tpch.test_timestamp (pk bigint primary key, val1 timestamp)");
        executeInPhoenix("UPSERT INTO tpch.test_timestamp (pk, val1) VALUES (1, null)");
        executeInPhoenix("UPSERT INTO tpch.test_timestamp (pk, val1) VALUES (2, '2002-05-30T09:30:10.5')");
        assertUpdate("INSERT INTO test_timestamp VALUES (3)", 1);
        assertQuery("SELECT * FROM test_timestamp", "VALUES 1, 2, 3");
        assertQuery(
                withUnsupportedType(CONVERT_TO_VARCHAR),
                "SELECT * FROM test_timestamp",
                "VALUES " +
                        "(1, null), " +
                        "(2, '2002-05-30 09:30:10.500'), " +
                        "(3, null)");
        assertQueryFails(
                withUnsupportedType(CONVERT_TO_VARCHAR),
                "INSERT INTO test_timestamp VALUES (4, '2002-05-30 09:30:10.500')",
                "Underlying type that is mapped to VARCHAR is not supported for INSERT: TIMESTAMP");
        assertUpdate("DROP TABLE tpch.test_timestamp");
    }

    private Session withUnsupportedType(UnsupportedTypeHandling unsupportedTypeHandling)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("phoenix", UNSUPPORTED_TYPE_HANDLING, unsupportedTypeHandling.name())
                .build();
    }

    @Test
    public void testCreateTableWithProperties()
    {
        assertUpdate("CREATE TABLE test_create_table_with_properties (created_date date, a bigint, b double, c varchar(10), d varchar(10)) WITH(rowkeys = 'created_date row_timestamp,a,b,c', salt_buckets=10)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_table_with_properties"));
        assertTableColumnNames("test_create_table_with_properties", "created_date", "a", "b", "c", "d");
        assertUpdate("DROP TABLE test_create_table_with_properties");
    }

    @Test
    public void testCreateTableWithPresplits()
    {
        assertUpdate("CREATE TABLE test_create_table_with_presplits (rid varchar(10), val1 varchar(10)) with(rowkeys = 'rid', SPLIT_ON='\"1\",\"2\",\"3\"')");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_table_with_presplits"));
        assertTableColumnNames("test_create_table_with_presplits", "rid", "val1");
        assertUpdate("DROP TABLE test_create_table_with_presplits");
    }

    @Test
    public void testSecondaryIndex()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_primary_table (pk bigint, val1 double, val2 double, val3 double) with(rowkeys = 'pk')");
        executeInPhoenix("CREATE LOCAL INDEX test_local_index ON tpch.test_primary_table (val1)");
        executeInPhoenix("CREATE INDEX test_global_index ON tpch.test_primary_table (val2)");
        assertUpdate("INSERT INTO test_primary_table VALUES (1, 1.1, 1.2, 1.3)", 1);
        assertQuery("SELECT val1,val3 FROM test_primary_table where val1 < 1.2", "SELECT 1.1,1.3");
        assertQuery("SELECT val2,val3 FROM test_primary_table where val2 < 1.3", "SELECT 1.2,1.3");
        assertUpdate("DROP TABLE test_primary_table");
    }

    @Test
    public void testCaseInsensitiveNameMatching()
            throws Exception
    {
        executeInPhoenix("CREATE TABLE tpch.\"TestCaseInsensitive\" (\"pK\" bigint primary key, \"Val1\" double)");
        assertUpdate("INSERT INTO testcaseinsensitive VALUES (1, 1.1)", 1);
        assertQuery("SELECT Val1 FROM testcaseinsensitive where Val1 < 1.2", "SELECT 1.1");
    }

    private void executeInPhoenix(String sql)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(testingPhoenixServer.getJdbcUrl());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
            connection.commit();
        }
    }
}
