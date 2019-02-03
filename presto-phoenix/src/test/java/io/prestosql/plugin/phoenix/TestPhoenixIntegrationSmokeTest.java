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

import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;
import io.prestosql.tests.AbstractTestIntegrationSmokeTest;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.regex.Pattern;

import static io.prestosql.plugin.phoenix.PhoenixQueryRunner.createPhoenixQueryRunner;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class TestPhoenixIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    public TestPhoenixIntegrationSmokeTest()
    {
        this(TestingPhoenixServer.getInstance());
    }

    public TestPhoenixIntegrationSmokeTest(TestingPhoenixServer server)
    {
        super(() -> createPhoenixQueryRunner(server));
    }

    @Test
    public void testSchemaOperations()
    {
        assertUpdate("CREATE SCHEMA new_schema");

        assertUpdate("CREATE TABLE new_schema.test (x bigint)");

        assertQueryFails("DROP SCHEMA new_schema", Pattern.quote("Error while executing statement"));

        assertUpdate("DROP TABLE new_schema.test");

        assertUpdate("DROP SCHEMA new_schema");
    }

    @Test
    public void testMultipleSomeColumnsRangesPredicate()
    {
        assertQuery("SELECT orderkey, shippriority, clerk, totalprice, custkey  FROM ORDERS WHERE orderkey BETWEEN 10 AND 50 or orderkey BETWEEN 100 AND 150");
    }

    @Test
    public void testCreateTableWithProperties()
    {
        assertUpdate("CREATE TABLE test_create_table_as_if_not_exists (created_date date, a bigint, b double, c varchar(10), d varchar(10)) with(rowkeys = 'created_date row_timestamp,a,b,c', SALT_BUCKETS=10)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_table_as_if_not_exists"));
        assertTableColumnNames("test_create_table_as_if_not_exists", "created_date", "a", "b", "c", "d");
    }

    @Test
    public void testCreateTableWithPresplits()
    {
        assertUpdate("CREATE TABLE test_create_presplits_table_as_if_not_exists (rid varchar(10), val1 varchar(10)) with(rowkeys = 'rid', SPLIT_ON='\"1\",\"2\",\"3\"')");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_presplits_table_as_if_not_exists"));
        assertTableColumnNames("test_create_presplits_table_as_if_not_exists", "rid", "val1");
    }

    @Test
    public void createTableWithEveryType()
    {
        @Language("SQL")
        String query = "" +
                "CREATE TABLE test_types_table AS " +
                "SELECT" +
                " 'foo' col_varchar" +
                ", cast('bar' as varbinary) col_varbinary" +
                ", cast(1 as bigint) col_bigint" +
                ", 2 col_integer" +
                ", CAST('3.14' AS DOUBLE) col_double" +
                ", true col_boolean" +
                ", DATE '1980-05-07' col_date" +
                ", CAST('3.14' AS DECIMAL(3,2)) col_decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) col_decimal_long" +
                ", CAST('bar' AS CHAR(10)) col_char";

        assertUpdate(query, 1);

        MaterializedResult results = getQueryRunner().execute(getSession(), "SELECT * FROM test_types_table where CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) = col_decimal_long").toTestTypes();
        assertEquals(results.getRowCount(), 1);
        MaterializedRow row = results.getMaterializedRows().get(0);
        assertEquals(row.getField(0), "foo");
        assertEquals(row.getField(1), "bar".getBytes(UTF_8));
        assertEquals(row.getField(2), 1L);
        assertEquals(row.getField(3), 2);
        assertEquals(row.getField(4), 3.14);
        assertEquals(row.getField(5), true);
        assertEquals(row.getField(6), LocalDate.of(1980, 5, 7));
        assertEquals(row.getField(7), new BigDecimal("3.14"));
        assertEquals(row.getField(8), new BigDecimal("12345678901234567890.0123456789"));
        assertEquals(row.getField(9), "bar       ");

        // test types in WHERE clause
        assertQueryWhere("test_types_table", "col_varchar", "'foo'");
        assertQueryWhere("test_types_table", "col_varbinary", "cast('bar' as varbinary)");
        assertQueryWhere("test_types_table", "col_bigint", "cast(1 as bigint)");
        assertQueryWhere("test_types_table", "col_integer", "2");
        assertQueryWhere("test_types_table", "col_double", "CAST('3.14' AS DOUBLE)");
        assertQueryWhere("test_types_table", "col_boolean", "true");
        assertQueryWhere("test_types_table", "col_date", "date('1980-05-07')");
        assertQueryWhere("test_types_table", "col_decimal_short", "CAST('3.14' AS DECIMAL(3,2))");
        assertQueryWhere("test_types_table", "col_decimal_long", "CAST('12345678901234567890.0123456789' AS DECIMAL(30,10))");
        assertQueryWhere("test_types_table", "col_char", "CAST('bar' AS CHAR(10))");

        assertUpdate("DROP TABLE test_types_table");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_types_table"));
    }

    private void assertQueryWhere(String tableName, String column, String value)
    {
        assertQuery(format("SELECT count(*) FROM %s WHERE %s = %s", tableName, column, value), "select 1");
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        TestingPhoenixServer.shutDown();
    }
}
