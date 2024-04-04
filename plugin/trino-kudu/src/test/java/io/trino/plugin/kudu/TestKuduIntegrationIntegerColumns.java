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
import org.junit.jupiter.api.Test;

import static io.trino.plugin.kudu.KuduQueryRunnerFactory.createKuduQueryRunner;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

public class TestKuduIntegrationIntegerColumns
        extends AbstractTestQueryFramework
{
    private static final TestInt[] TEST_INTS = {
            new TestInt("TINYINT", 8),
            new TestInt("SMALLINT", 16),
            new TestInt("INTEGER", 32),
            new TestInt("BIGINT", 64),
    };

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createKuduQueryRunner(closeAfterClass(new TestingKuduServer()), "test_integer");
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
        assertThat(result.getRowCount()).isEqualTo(1);
        Object obj = result.getMaterializedRows().get(0).getField(1);
        switch (test.bits) {
            case 64:
                assertThat(obj instanceof Long).isTrue();
                assertThat(((Long) obj).longValue()).isEqualTo(casted);
                break;
            case 32:
                assertThat(obj instanceof Integer).isTrue();
                assertThat(((Integer) obj).longValue()).isEqualTo(casted);
                break;
            case 16:
                assertThat(obj instanceof Short).isTrue();
                assertThat(((Short) obj).longValue()).isEqualTo(casted);
                break;
            case 8:
                assertThat(obj instanceof Byte).isTrue();
                assertThat(((Byte) obj).longValue()).isEqualTo(casted);
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
