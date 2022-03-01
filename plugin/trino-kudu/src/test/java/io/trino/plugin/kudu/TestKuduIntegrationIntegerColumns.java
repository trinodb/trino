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
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static io.trino.plugin.kudu.KuduQueryRunnerFactory.createKuduQueryRunner;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestKuduIntegrationIntegerColumns
        extends AbstractTestQueryFramework
{
    private static final TestInt[] TEST_INTS = {
            new TestInt("TINYINT", 8),
            new TestInt("SMALLINT", 16),
            new TestInt("INTEGER", 32),
            new TestInt("BIGINT", 64),
    };

    private TestingKuduServer kuduServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        kuduServer = new TestingKuduServer();
        return createKuduQueryRunner(kuduServer, "test_integer");
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
    public void testCreateTableWithIntegerColumn()
    {
        for (TestInt test : TEST_INTS) {
            doTestCreateTableWithIntegerColumn(test);
        }
    }

    private void doTestCreateTableWithIntegerColumn(TestInt test)
    {
        String dropTable = "DROP TABLE IF EXISTS test_int";
        String createTable = "" +
                "CREATE TABLE test_int (\n" +
                "  id INT WITH (primary_key=true),\n" +
                "  intcol " + test.type + "\n" +
                ") WITH (\n" +
                " partition_by_hash_columns = ARRAY['id'],\n" +
                " partition_by_hash_buckets = 2\n" +
                ")";

        assertUpdate(dropTable);
        assertUpdate(createTable);

        long maxValue = Long.MAX_VALUE;
        long casted = maxValue >> (64 - test.bits);
        assertUpdate("INSERT INTO test_int VALUES(1, CAST(" + casted + " AS " + test.type + "))", 1);

        MaterializedResult result = computeActual("SELECT id, intcol FROM test_int");
        assertEquals(result.getRowCount(), 1);
        Object obj = result.getMaterializedRows().get(0).getField(1);
        switch (test.bits) {
            case 64:
                assertTrue(obj instanceof Long);
                assertEquals(((Long) obj).longValue(), casted);
                break;
            case 32:
                assertTrue(obj instanceof Integer);
                assertEquals(((Integer) obj).longValue(), casted);
                break;
            case 16:
                assertTrue(obj instanceof Short);
                assertEquals(((Short) obj).longValue(), casted);
                break;
            case 8:
                assertTrue(obj instanceof Byte);
                assertEquals(((Byte) obj).longValue(), casted);
                break;
            default:
                fail("Unexpected bits: " + test.bits);
                break;
        }
    }

    static class TestInt
    {
        final String type;
        final int bits;

        TestInt(String type, int bits)
        {
            this.type = type;
            this.bits = bits;
        }
    }
}
