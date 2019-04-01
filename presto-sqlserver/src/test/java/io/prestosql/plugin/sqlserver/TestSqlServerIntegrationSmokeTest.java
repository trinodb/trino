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
package io.prestosql.plugin.sqlserver;

import io.prestosql.tests.AbstractTestIntegrationSmokeTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static io.airlift.tpch.TpchTable.ORDERS;
import static io.prestosql.plugin.sqlserver.SqlServerQueryRunner.createSqlServerQueryRunner;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

@Test
public class TestSqlServerIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private final TestingSqlServer sqlServer;

    public TestSqlServerIntegrationSmokeTest()
    {
        this(new TestingSqlServer());
    }

    public TestSqlServerIntegrationSmokeTest(TestingSqlServer testingSqlServer)
    {
        super(() -> createSqlServerQueryRunner(testingSqlServer, ORDERS));
        this.sqlServer = testingSqlServer;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        sqlServer.close();
    }

    @Test
    public void testView()
    {
        sqlServer.execute("CREATE VIEW test_view AS SELECT * FROM orders");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_view"));
        assertQuery("SELECT orderkey FROM test_view", "SELECT orderkey FROM orders");
        sqlServer.execute("DROP VIEW IF EXISTS test_view");
    }

    @Test
    public void testDecimalPredicatePushdown()
            throws Exception
    {
        try (AutoCloseable ignoreTable = withTable("test_decimal_pushdown",
                "(short_decimal decimal(9, 3), long_decimal decimal(30, 10))")) {
            sqlServer.execute("INSERT INTO test_decimal_pushdown VALUES (123.321, 123456789.987654321)");

            assertQuery("SELECT * FROM test_decimal_pushdown WHERE short_decimal = 123.321",
                    "VALUES (123.321, 123456789.987654321)");
            assertQuery("SELECT * FROM test_decimal_pushdown WHERE long_decimal = 123456789.987654321",
                    "VALUES (123.321, 123456789.987654321)");
        }
    }

    private AutoCloseable withTable(String tableName, String tableDefinition)
    {
        sqlServer.execute(format("CREATE TABLE %s %s", tableName, tableDefinition));
        return () -> sqlServer.execute("DROP TABLE " + tableName);
    }
}
