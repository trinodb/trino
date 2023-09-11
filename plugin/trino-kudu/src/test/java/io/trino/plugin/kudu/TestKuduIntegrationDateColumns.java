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

import java.time.LocalDate;

import static io.trino.plugin.kudu.KuduQueryRunnerFactory.createKuduQueryRunner;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestKuduIntegrationDateColumns
        extends AbstractTestQueryFramework
{
    private static final TestDate[] TEST_DATE = {new TestDate("DATE")};

    private TestingKuduServer kuduServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        kuduServer = new TestingKuduServer();
        return createKuduQueryRunner(kuduServer, "test_date");
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
        for (TestDate test : TEST_DATE) {
            doTestCreateTableWithIntegerColumn(test);
        }
    }

    private void doTestCreateTableWithIntegerColumn(TestDate test)
    {
        String dropTable = "DROP TABLE IF EXISTS test_date";
        String createTable = "" +
                "CREATE TABLE test_date (\n" +
                "  id INT WITH (primary_key=true),\n" +
                "  datecol " + test.type + "\n" +
                ") WITH (\n" +
                " partition_by_hash_columns = ARRAY['id'],\n" +
                " partition_by_hash_buckets = 2\n" +
                ")";

        assertUpdate(dropTable);
        assertUpdate(createTable);

        assertUpdate("INSERT INTO test_date VALUES(1, DATE '1977-07-14')", 1);
        MaterializedResult result = computeActual("SELECT id, datecol FROM test_date");
        assertEquals(result.getRowCount(), 1);
        Object obj = result.getMaterializedRows().get(0).getField(1);
        assertTrue(obj instanceof LocalDate);
    }

    static class TestDate
    {
        final String type;

        TestDate(String type)
        {
            this.type = type;
        }
    }
}
