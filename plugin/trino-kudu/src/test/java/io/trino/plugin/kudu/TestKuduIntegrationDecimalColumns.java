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
package io.trino.plugin.kudu;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static io.trino.plugin.kudu.KuduQueryRunnerFactory.createKuduQueryRunner;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestKuduIntegrationDecimalColumns
        extends AbstractTestQueryFramework
{
    private static final TestDecimal[] TEST_DECIMALS = {
            new TestDecimal(10, 0),
            new TestDecimal(15, 4),
            new TestDecimal(18, 6),
            new TestDecimal(18, 7),
            new TestDecimal(19, 8),
            new TestDecimal(24, 14),
            new TestDecimal(38, 20),
            new TestDecimal(38, 28),
    };

    private TestingKuduServer kuduServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createKuduQueryRunner(closeAfterClass(new TestingKuduServer()), "decimal");
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        if (kuduServer != null) {
            kuduServer.close();
            kuduServer = null;
        }
    }

    @Test
    public void testCreateTableWithDecimalColumn()
    {
        for (TestDecimal decimal : TEST_DECIMALS) {
            doTestCreateTableWithDecimalColumn(decimal);
        }
    }

    @Test
    public void testDecimalColumn()
    {
        try (TestTable testTable = new TestTable(
                new TrinoSqlExecutor(getQueryRunner()),
                "test_decimal",
                "(id INT WITH (primary_key=true), col_decimal decimal(10, 6)) " +
                        "WITH (partition_by_hash_columns = ARRAY['id'], partition_by_hash_buckets = 2)")) {
            assertUpdate(format("INSERT INTO %s VALUES (0, 0.0), (2, 2.2), (1, 1.1)", testTable.getName()), 3);
            assertQuery(format("SELECT * FROM %s WHERE col_decimal = 1.1", testTable.getName()), "VALUES (1, 1.1)");
            assertUpdate(format("DELETE FROM %s WHERE col_decimal = 1.1", testTable.getName()), 1);
            assertQueryReturnsEmptyResult(format("SELECT * FROM %s WHERE col_decimal = 1.1", testTable.getName()));
        }
    }

    @Test
    public void testDeleteByPrimaryKeyDecimalColumn()
    {
        try (TestTable testTable = new TestTable(
                new TrinoSqlExecutor(getQueryRunner()),
                "test_decimal",
                "(decimal_id decimal(18, 3) WITH (primary_key=true), col_decimal decimal(18, 3)) " +
                        "WITH (partition_by_hash_columns = ARRAY['decimal_id'], partition_by_hash_buckets = 2)")) {
            assertUpdate(format("INSERT INTO %s VALUES (1.1, 1.1), (2.2, 2.2)", testTable.getName()), 2);
            assertUpdate(format("DELETE FROM %s WHERE decimal_id = 2.2", testTable.getName()), 1);
            assertQuery(format("SELECT * FROM %s", testTable.getName()), "VALUES (1.1, 1.1)");
        }
    }

    private void doTestCreateTableWithDecimalColumn(TestDecimal decimal)
    {
        String tableDefinition = format(
                "(id INT WITH (primary_key=true), dec DECIMAL(%s, %s)) " +
                "WITH (partition_by_hash_columns = ARRAY['id'], partition_by_hash_buckets = 2)",
                decimal.precision,
                decimal.scale);

        try (TestTable testTable = new TestTable(new TrinoSqlExecutor(getQueryRunner()), decimal.getTableName(), tableDefinition)) {
            String fullPrecisionValue = "1234567890.1234567890123456789012345678";
            int maxScale = decimal.precision - 10;
            int valuePrecision = decimal.precision - maxScale + Math.min(maxScale, decimal.scale);
            String insertValue = fullPrecisionValue.substring(0, valuePrecision + 1);
            assertUpdate(format("INSERT INTO %s VALUES(1, DECIMAL '%s')", testTable.getName(), insertValue), 1);

            MaterializedResult result = computeActual(format("SELECT id, CAST((dec - (DECIMAL '%s')) as DOUBLE) FROM %s", insertValue, testTable.getName()));
            assertEquals(result.getRowCount(), 1);
            Object obj = result.getMaterializedRows().get(0).getField(1);
            assertTrue(obj instanceof Double);
            Double actual = (Double) obj;
            assertEquals(0, actual, 0.3 * Math.pow(0.1, decimal.scale), "p=" + decimal.precision + ",s=" + decimal.scale + " => " + actual + ",insert = " + insertValue);
        }
    }

    static class TestDecimal
    {
        final int precision;
        final int scale;

        TestDecimal(int precision, int scale)
        {
            this.precision = precision;
            this.scale = scale;
        }

        String getTableName()
        {
            return format("test_decimal_%s_%s", precision, scale);
        }
    }
}
