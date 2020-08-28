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
package io.prestosql.plugin.oracle;

import com.google.common.collect.ImmutableList;
import io.prestosql.testing.AbstractTestIntegrationSmokeTest;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static io.prestosql.tpch.TpchTable.CUSTOMER;
import static io.prestosql.tpch.TpchTable.NATION;
import static io.prestosql.tpch.TpchTable.ORDERS;
import static io.prestosql.tpch.TpchTable.REGION;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

abstract class BaseOracleIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private TestingOracleServer oracleServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        oracleServer = new TestingOracleServer();
        return createOracleQueryRunner(oracleServer, ImmutableList.of(CUSTOMER, NATION, ORDERS, REGION));
    }

    protected abstract QueryRunner createOracleQueryRunner(TestingOracleServer server, Iterable<TpchTable<?>> tables)
            throws Exception;

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        oracleServer.close();
    }

    @Test
    @Override
    public void testDescribeTable()
    {
        MaterializedResult expectedColumns = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "decimal(19,0)", "", "")
                .row("custkey", "decimal(19,0)", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "timestamp(3)", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "decimal(10,0)", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        assertEquals(actualColumns, expectedColumns);
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                // If the connector reports additional column properties, the expected value needs to be adjusted in the test subclass
                .matches("CREATE TABLE \\w+\\.\\w+\\.orders \\Q(\n" +
                        "   orderkey decimal(19, 0),\n" +
                        "   custkey decimal(19, 0),\n" +
                        "   orderstatus varchar(1),\n" +
                        "   totalprice double,\n" +
                        "   orderdate timestamp(3),\n" +
                        "   orderpriority varchar(15),\n" +
                        "   clerk varchar(15),\n" +
                        "   shippriority decimal(10, 0),\n" +
                        "   comment varchar(79)\n" +
                        ")");
    }

    @Test
    public void testPredicatePushdown()
            throws Exception
    {
        // varchar equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'ROMANIA'"))
                .matches("VALUES (CAST(3 AS DECIMAL(19,0)), CAST(19 AS DECIMAL(19,0)), CAST('ROMANIA' AS varchar(25)))")
                .isCorrectlyPushedDown();

        // varchar range
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name BETWEEN 'POLAND' AND 'RPA'"))
                .matches("VALUES (CAST(3 AS DECIMAL(19,0)), CAST(19 AS DECIMAL(19,0)), CAST('ROMANIA' AS varchar(25)))")
                .isCorrectlyPushedDown();

        // varchar different case
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'romania'"))
                .returnsEmptyResult()
                .isCorrectlyPushedDown();

        // date equality
        assertThat(query("SELECT orderkey FROM orders WHERE orderdate = DATE '1992-09-29'"))
                .matches("VALUES CAST(1250 AS DECIMAL(19,0)), 34406, 38436, 57570")
                .isCorrectlyPushedDown();

        predicatePushdownTest("decimal(9, 3)", "123.321", "<=", "124", "CAST(123.321 AS decimal(9, 3))");
        predicatePushdownTest("decimal(9, 3)", "123.321", "<=", "123.321", "CAST(123.321 AS decimal(9, 3))");
        predicatePushdownTest("decimal(9, 3)", "123.321", "=", "123.321", "CAST(123.321 AS decimal(9, 3))");
        predicatePushdownTest("decimal(30, 10)", "123456789.987654321", "<=", "123456790", "CAST(123456789.987654321 AS decimal(30, 10))");
        predicatePushdownTest("decimal(30, 10)", "123456789.987654321", "<=", "123456789.987654321", "CAST(123456789.987654321 AS decimal(30, 10))");
        predicatePushdownTest("decimal(30, 10)", "123456789.987654321", "=", "123456789.987654321", "CAST(123456789.987654321 AS decimal(30, 10))");
        predicatePushdownTest("float(63)", "123456789.987654321", "<=", "CAST(123456789.99 AS REAL)", "CAST(123456789.987654321 AS DOUBLE)");
        predicatePushdownTest("float(63)", "123456789.987654321", "<=", "CAST(123456789.99 AS DOUBLE)", "CAST(123456789.987654321 AS DOUBLE)");
        predicatePushdownTest("float(126)", "123456789.987654321", "<=", "CAST(123456789.99 AS REAL)", "CAST(123456789.987654321 AS DOUBLE)");
        predicatePushdownTest("float(126)", "123456789.987654321", "<=", "CAST(123456789.99 AS DOUBLE)", "CAST(123456789.987654321 AS DOUBLE)");
        predicatePushdownTest("CHAR(1)", "'0'", "=", "'0'", "CHAR'0'");
        predicatePushdownTest("CHAR(1)", "'0'", "<=", "'0'", "CHAR'0'");
        predicatePushdownTest("CHAR(5)", "'0'", "=", "CHAR'0'", "CHAR'0    '");
    }

    private void predicatePushdownTest(String oracleType, String oracleLiteral, String operator, String filterLiteral, String compareLiteral)
            throws Exception
    {
        String tableName = "test_pushdown_" + oracleType.replaceAll("[^a-zA-Z0-9]", "");
        try (AutoCloseable ignored = withTable(tableName, format("(c %s)", oracleType))) {
            oracleServer.execute(format("INSERT INTO %s VALUES (%s)", tableName, oracleLiteral));

            assertThat(query(format("SELECT * FROM %s WHERE c %s %s", tableName, operator, filterLiteral)))
                    .matches(format("VALUES (%s)", compareLiteral))
                    .isCorrectlyPushedDown();
        }
    }

    private AutoCloseable withTable(String tableName, String tableDefinition)
            throws Exception
    {
        oracleServer.execute(format("CREATE TABLE %s%s", tableName, tableDefinition));
        return () -> oracleServer.execute(format("DROP TABLE %s", tableName));
    }
}
