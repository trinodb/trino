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
package io.trino.plugin.mongodb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.testing.BaseComplexTypesPredicatePushDownTest;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.mongodb.MongoQueryRunner.createMongoQueryRunner;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMongoComplexTypePredicatePushDown
        extends BaseComplexTypesPredicatePushDownTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        MongoServer server = closeAfterClass(new MongoServer());
        return createMongoQueryRunner(server, ImmutableMap.of(), ImmutableList.of());
    }

    @Test
    public void testArrayContainsPushdown()
    {
        String tableName = "test_array_contains_pushdown_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (colArray ARRAY(BIGINT))");
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM unnest(transform(SEQUENCE(1, 10000), x -> ROW(ARRAY[100, 200])))", 10000);

        assertNoDataRead("SELECT * FROM " + tableName + " WHERE contains(colArray, -1)");

        assertNoDataRead("SELECT * FROM " + tableName + " WHERE NOT contains(colArray, 100)");

        assertNoDataRead("SELECT * FROM " + tableName + " WHERE contains(colArray, 100) AND contains(colArray, -1)");

        assertNoDataRead("SELECT * FROM " + tableName + " WHERE NOT contains(colArray, 100) OR NOT contains(colArray, 200)");

        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE contains(colArray, 100)",
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0),
                results -> assertThat(results.getRowCount()).isEqualTo(10000));

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testArrayContainsPushdownDatatype()
    {
        String tableName = "test_array_contains_pushdown_datatype" + randomNameSuffix();
        String createSql = """
                CREATE TABLE %s (\
                  boolArray ARRAY(BOOLEAN),\
                  intArray ARRAY(BIGINT),\
                  doubleArray ARRAY(DOUBLE),\
                  decimalArray ARRAY(DECIMAL(10,3)),\
                  tsArray ARRAY(TIMESTAMP(3)),\
                  varcharArray ARRAY(VARCHAR),\
                  varbinArray ARRAY(VARBINARY),\
                  oidArray ARRAY(ObjectId))
                """.formatted(tableName);
        assertUpdate(createSql);
        String insertSql = """
                INSERT INTO %s SELECT * FROM unnest(transform(SEQUENCE(1, 10), x -> ROW(\
                ARRAY[true, true],
                ARRAY[100, 200],
                ARRAY[10.3, 20.3],
                ARRAY[10.3, 20.3],
                ARRAY[TIMESTAMP '2024-03-19 15:28:23'],
                ARRAY['hello', 'world'],
                ARRAY[X'0D0A'],
                ARRAY[ObjectId('55b151633864d6438c61a9ce')]
                )))
                """.formatted(tableName);
        assertUpdate(insertSql, 10);

        testFilterCount(tableName, "contains(boolArray, true)", 10);
        testFilterCount(tableName,"contains(intArray, 100)", 10);
        testFilterCount(tableName, "contains(doubleArray, 10.3)", 10);
        testFilterCount(tableName, "contains(decimalArray, 10.3)", 10);
        testFilterCount(tableName, "contains(tsArray, TIMESTAMP '2024-03-19 15:28:23')", 10);
        testFilterCount(tableName, "contains(varcharArray, 'hello')", 10);
        // testFilterCount(tableName, "contains(varbinArray, X'0D0A')", 10);
        testFilterCount(tableName, "contains(oidArray, ObjectId('55b151633864d6438c61a9ce'))", 10);

        assertUpdate("DROP TABLE " + tableName);
    }

    private void testFilterCount(String tableName, String predicate, int expectCount)
    {
        assertQueryStats(
                getSession(),
                "SELECT * FROM %s WHERE %s".formatted(tableName, predicate),
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0),
                results -> assertThat(results.getRowCount()).isEqualTo(expectCount));

        assertNoDataRead("SELECT * FROM %s WHERE not %s".formatted(tableName, predicate));
    }
}
