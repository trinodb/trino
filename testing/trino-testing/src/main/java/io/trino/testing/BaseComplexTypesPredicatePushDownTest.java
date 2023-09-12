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
package io.trino.testing;

import org.testng.annotations.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseComplexTypesPredicatePushDownTest
        extends AbstractTestQueryFramework
{
    @Test
    public void testRowTypeOnlyNullsRowGroupPruning()
    {
        String tableName = "test_primitive_column_nulls_pruning_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (col BIGINT)");
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM unnest(repeat(NULL, 4096))", 4096);
        assertNoDataRead("SELECT * FROM " + tableName + " WHERE col IS NOT NULL");

        tableName = "test_nested_column_nulls_pruning_" + randomNameSuffix();
        // Nested column `a` has nulls count of 4096 and contains only nulls
        // Nested column `b` also has nulls count of 4096, but it contains non nulls as well
        assertUpdate("CREATE TABLE " + tableName + " (col ROW(a BIGINT, b ARRAY(DOUBLE)))");
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM unnest(transform(repeat(1, 4096), x -> ROW(ROW(NULL, ARRAY [NULL, rand()]))))", 4096);

        assertNoDataRead("SELECT * FROM " + tableName + " WHERE col.a IS NOT NULL");

        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE col.a IS NULL",
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0),
                results -> assertThat(results.getRowCount()).isEqualTo(4096));

        // no predicate push down for the entire array type
        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE col.b IS NOT NULL",
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0),
                results -> assertThat(results.getRowCount()).isEqualTo(4096));

        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE col.b IS NULL",
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0),
                results -> assertThat(results.getRowCount()).isEqualTo(0));

        // no predicate push down for entire ROW
        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE col IS NOT NULL",
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0),
                results -> assertThat(results.getRowCount()).isEqualTo(4096));

        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE col IS NULL",
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0),
                results -> assertThat(results.getRowCount()).isEqualTo(0));

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testRowTypeRowGroupPruning()
    {
        String tableName = "test_nested_column_pruning_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (col1Row ROW(a BIGINT, b BIGINT, c ROW(c1 BIGINT, c2 ROW(c21 BIGINT, c22 BIGINT))))");
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM unnest(transform(SEQUENCE(1, 10000), x -> ROW(ROW(x*2, 100, ROW(x, ROW(x*5, x*6))))))", 10000);

        // no data read since the row dereference predicate is pushed down
        assertNoDataRead("SELECT * FROM " + tableName + " WHERE col1Row.a = -1");
        assertNoDataRead("SELECT * FROM " + tableName + " WHERE col1Row.a IS NULL");
        assertNoDataRead("SELECT * FROM " + tableName + " WHERE col1Row.c.c2.c22 = -1");
        assertNoDataRead("SELECT * FROM " + tableName + " WHERE col1Row.a = -1 AND col1ROW.b = -1 AND col1ROW.c.c1 = -1 AND col1Row.c.c2.c22 = -1");

        // read all since predicate case matches with the data
        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE col1Row.b = 100",
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0),
                results -> assertThat(results.getRowCount()).isEqualTo(10000));

        // no predicate push down for matching with ROW type, as file format only stores stats for primitives
        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE col1Row.c = ROW(-1, ROW(-1, -1))",
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0),
                results -> assertThat(results.getRowCount()).isEqualTo(0));

        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE col1Row.c = ROW(-1, ROW(-1, -1)) OR col1Row.a = -1 ",
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0),
                results -> assertThat(results.getRowCount()).isEqualTo(0));

        // no data read since the row group get filtered by primitives in the predicate
        assertNoDataRead("SELECT * FROM " + tableName + " WHERE col1Row.c = ROW(-1, ROW(-1, -1)) AND col1Row.a = -1 ");

        // no predicate push down for entire ROW, as file format only stores stats for primitives
        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE col1Row = ROW(-1, -1, ROW(-1, ROW(-1, -1)))",
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0),
                results -> assertThat(results.getRowCount()).isEqualTo(0));

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testMapTypeRowGroupPruning()
    {
        String tableName = "test_nested_column_pruning_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (colMap Map(VARCHAR, BIGINT))");
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM unnest(transform(SEQUENCE(1, 10000), x -> ROW(MAP(ARRAY['FOO', 'BAR'], ARRAY[100, 200]))))", 10000);

        // no predicate push down for MAP type dereference
        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE colMap['FOO'] = -1",
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0),
                results -> assertThat(results.getRowCount()).isEqualTo(0));

        // no predicate push down for entire Map type
        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE colMap = MAP(ARRAY['FOO', 'BAR'], ARRAY[-1, -1])",
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0),
                results -> assertThat(results.getRowCount()).isEqualTo(0));

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testArrayTypeRowGroupPruning()
    {
        String tableName = "test_nested_column_pruning_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (colArray ARRAY(BIGINT))");
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM unnest(transform(SEQUENCE(1, 10000), x -> ROW(ARRAY[100, 200])))", 10000);

        // no predicate push down for ARRAY type dereference
        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE colArray[1] = -1",
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0),
                results -> assertThat(results.getRowCount()).isEqualTo(0));

        // no predicate push down for entire ARRAY type
        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE colArray = ARRAY[-1, -1]",
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0),
                results -> assertThat(results.getRowCount()).isEqualTo(0));

        assertUpdate("DROP TABLE " + tableName);
    }
}
