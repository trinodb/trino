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

public abstract class BaseTestParquetComplexTypePredicatePushDown
        extends AbstractTestQueryFramework
{
    @Test
    public void testParquetOnlyNullsRowGroupPruning()
    {
        String tableName = "test_primitive_column_nulls_pruning_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (col BIGINT) WITH (format = 'PARQUET')");
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM unnest(repeat(NULL, 4096))", 4096);
        assertNoDataRead("SELECT * FROM " + tableName + " WHERE col IS NOT NULL");

        tableName = "test_nested_column_nulls_pruning_" + randomNameSuffix();
        // Nested column `a` has nulls count of 4096 and contains only nulls
        // Nested column `b` also has nulls count of 4096, but it contains non nulls as well
        assertUpdate("CREATE TABLE " + tableName + " (col ROW(a BIGINT, b ARRAY(DOUBLE))) WITH (format = 'PARQUET')");
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM unnest(transform(repeat(1, 4096), x -> ROW(ROW(NULL, ARRAY [NULL, rand()]))))", 4096);
        // TODO replace with assertNoDataRead after nested column predicate pushdown
        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE col.a IS NOT NULL",
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0),
                results -> assertThat(results.getRowCount()).isEqualTo(0));
        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE col.b IS NOT NULL",
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0),
                results -> assertThat(results.getRowCount()).isEqualTo(4096));
    }
}
