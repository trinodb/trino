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

import io.prestosql.testing.AbstractTestIntegrationSmokeTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static io.prestosql.plugin.phoenix.PhoenixQueryRunner.createPhoenixQueryRunner;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test
public class TestPhoenixIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private TestingPhoenixServer testingPhoenixServer;

    public TestPhoenixIntegrationSmokeTest()
    {
        this(TestingPhoenixServer.getInstance());
    }

    public TestPhoenixIntegrationSmokeTest(TestingPhoenixServer server)
    {
        super(() -> createPhoenixQueryRunner(server));
        this.testingPhoenixServer = server;
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        TestingPhoenixServer.shutDown();
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
        }
    }
}
