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
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.testing.AbstractTestIntegrationSmokeTest;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.sql.SqlExecutor;
import io.prestosql.testing.sql.TestTable;
import org.testng.annotations.Test;

import static io.prestosql.plugin.oracle.TestingOracleServer.TEST_USER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static io.prestosql.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public abstract class BaseOracleIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
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
    public void testDropTable()
    {
        String tableName = "test_drop" + randomTableSuffix();
        assertUpdate(format("CREATE TABLE %s AS SELECT 1 test_drop", tableName), 1);
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testViews()
    {
        try (TestView view = new TestView(onOracle(), getUser() + ".test_view", "AS SELECT 'O' as status FROM dual")) {
            assertQuery("SELECT status FROM " + view.getName(), "SELECT 'O'");
        }
    }

    @Test
    public void testSynonyms()
    {
        try (TestSynonym synonym = new TestSynonym(onOracle(), getUser() + ".test_synonym", "FOR ORDERS")) {
            assertQueryFails("SELECT orderkey FROM " + synonym.getName(), "line 1:22: Table 'oracle.*' does not exist");
        }
    }

    @Test
    public void testPredicatePushdown()
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
    }

    @Test
    public void testPredicatePushdownForNumerics()
    {
        predicatePushdownTest("DECIMAL(9, 3)", "123.321", "<=", "124");
        predicatePushdownTest("DECIMAL(9, 3)", "123.321", "<=", "123.321");
        predicatePushdownTest("DECIMAL(9, 3)", "123.321", "=", "123.321");
        predicatePushdownTest("DECIMAL(30, 10)", "123456789.987654321", "<=", "123456790");
        predicatePushdownTest("DECIMAL(30, 10)", "123456789.987654321", "<=", "123456789.987654321");
        predicatePushdownTest("DECIMAL(30, 10)", "123456789.987654321", "=", "123456789.987654321");
        predicatePushdownTest("FLOAT(63)", "123456789.987654321", "<=", "CAST(123456789.99 AS REAL)");
        predicatePushdownTest("FLOAT(63)", "123456789.987654321", "<=", "CAST(123456789.99 AS DOUBLE)");
        predicatePushdownTest("FLOAT(126)", "123456789.987654321", "<=", "CAST(123456789.99 AS REAL)");
        predicatePushdownTest("FLOAT(126)", "123456789.987654321", "<=", "CAST(123456789.99 AS DOUBLE)");
        predicatePushdownTest("BINARY_FLOAT", "5.0f", "=", "CAST(5.0 AS REAL)");
        predicatePushdownTest("BINARY_DOUBLE", "20.233", "=", "CAST(20.233 AS DOUBLE)");
        predicatePushdownTest("NUMBER(5,3)", "5.0", "=", "CAST(5.0 AS DECIMAL(5,3))");
    }

    @Test
    public void testPredicatePushdownForChars()
    {
        predicatePushdownTest("CHAR(1)", "'0'", "=", "'0'");
        predicatePushdownTest("CHAR(1)", "'0'", "<=", "'0'");
        predicatePushdownTest("CHAR(5)", "'0'", "=", "CHAR'0'");
        predicatePushdownTest("CHAR(7)", "'my_char'", "=", "CAST('my_char' AS CHAR(7))");
        predicatePushdownTest("NCHAR(7)", "'my_char'", "=", "CAST('my_char' AS CHAR(7))");
        predicatePushdownTest("VARCHAR2(7)", "'my_char'", "=", "CAST('my_char' AS VARCHAR(7))");
        predicatePushdownTest("NVARCHAR2(7)", "'my_char'", "=", "CAST('my_char' AS VARCHAR(7))");

        try (TestTable table = new TestTable(
                onOracle(),
                getUser() + ".test_pdown_",
                "(c_clob CLOB, c_nclob NCLOB)",
                ImmutableList.of("'my_clob', 'my_nclob'"))) {
            assertThat(query(format("SELECT c_clob FROM %s WHERE c_clob = cast('my_clob' as varchar)", table.getName()))).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(format("SELECT c_nclob FROM %s WHERE c_nclob = cast('my_nclob' as varchar)", table.getName()))).isNotFullyPushedDown(FilterNode.class);
        }
    }

    private void predicatePushdownTest(String oracleType, String oracleLiteral, String operator, String filterLiteral)
    {
        String tableName = "test_pdown_" + oracleType.replaceAll("[^a-zA-Z0-9]", "");
        try (TestTable table = new TestTable(onOracle(), getUser() + "." + tableName, format("(c %s)", oracleType))) {
            onOracle().execute(format("INSERT INTO %s VALUES (%s)", table.getName(), oracleLiteral));

            assertThat(query(format("SELECT * FROM %s WHERE c %s %s", table.getName(), operator, filterLiteral)))
                    .isCorrectlyPushedDown();
        }
    }

    protected String getUser()
    {
        return TEST_USER;
    }

    protected abstract SqlExecutor onOracle();
}
