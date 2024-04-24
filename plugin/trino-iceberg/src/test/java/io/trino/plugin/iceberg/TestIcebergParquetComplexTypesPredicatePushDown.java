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
package io.trino.plugin.iceberg;

import io.trino.Session;
import io.trino.testing.BaseComplexTypesPredicatePushDownTest;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.iceberg.IcebergTestUtils.withSmallRowGroups;
import static io.trino.testing.TestingNames.randomNameSuffix;

public class TestIcebergParquetComplexTypesPredicatePushDown
        extends BaseComplexTypesPredicatePushDownTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.file-format", "PARQUET")
                .build();
    }

    @Override
    protected final Session getSession()
    {
        return withSmallRowGroups(super.getSession());
    }

    // The Iceberg table scan differs from Hive in that the coordinator also uses file statistics when generating the splits .
    // As a result, if the predicates fall outside the bounds of the file statistics,
    // the split is not created for the worker and worker won't call getParquetTupleDomain().
    // The test increased the number of row groups and introduced predicates that are within the file statistics but outside the statistics of the row groups.
    @Test
    public void testIcebergParquetRowTypeRowGroupPruning()
    {
        String tableName = "test_nested_column_pruning_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (col1Row ROW(a BIGINT, b BIGINT), col2 BIGINT) WITH (sorted_by=ARRAY['col2'])");
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM unnest(transform(SEQUENCE(1, 10000), x -> ROW(ROW(x*2, 100), x)))", 10000);

        // col1Row.a only contains even numbers, in the range of [2, 20000].
        // The test has roughly 50 rows per row group due to withSmallRowGroups, [2, 100], [102, 200], ... [19902, 20000]
        // 101 is a value between [2, 20000] but is an odd number, so won't be discarded by Iceberg table's statistics.
        // At the same time, 101 is not within the bound of any row group. So can be discarded by Parquet's row group statistics.
        assertNoDataRead("SELECT * FROM " + tableName + " WHERE col1Row.a = 101");
        assertNoDataRead("SELECT * FROM " + tableName + " WHERE col1Row.a IS NULL");

        assertUpdate("DROP TABLE " + tableName);
    }
}
