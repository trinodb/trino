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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import static io.trino.tpch.TpchTable.NATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Percentage.withPercentage;

final class TestIcebergHllStatistics
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.extended-statistics.ndv-sketch-algorithm", "HLL")
                .setInitialTables(NATION)
                .build();
    }

    @Test
    void testAnalyzePopulatesNdv()
    {
        try (TestTable table = newTrinoTable("test_hll_analyze", "AS SELECT * FROM tpch.sf1.nation")) {
            assertUpdate("ANALYZE " + table.getName());

            assertDistinctValuesCountApproximately(table.getName(), "nationkey", 25);
            assertDistinctValuesCountApproximately(table.getName(), "regionkey", 5);
        }
    }

    @Test
    void testInsertIncrementallyUpdatesNdv()
    {
        try (TestTable table = newTrinoTable("test_hll_insert", "AS SELECT * FROM tpch.sf1.nation")) {
            assertDistinctValuesCountApproximately(table.getName(), "nationkey", 25);

            // insert modified rows: doubles the distinct nationkey/regionkey values
            assertUpdate("INSERT INTO " + table.getName() + " SELECT nationkey + 25, name, regionkey + 5, comment FROM tpch.sf1.nation", 25);
            assertDistinctValuesCountApproximately(table.getName(), "nationkey", 50);
            assertDistinctValuesCountApproximately(table.getName(), "regionkey", 10);
        }
    }

    @Test
    void testDropExtendedStats()
    {
        try (TestTable table = newTrinoTable("test_hll_drop_extended_stats", "AS SELECT * FROM tpch.sf1.nation")) {
            assertUpdate("ANALYZE " + table.getName());
            assertDistinctValuesCountApproximately(table.getName(), "nationkey", 25);

            assertThat(query("ALTER TABLE " + table.getName() + " EXECUTE DROP_EXTENDED_STATS"))
                    .matches("VALUES (VARCHAR 'removed_statistics_count', BIGINT '1')");
            assertThat(distinctValuesCount(table.getName(), "nationkey")).isNull();

            // re-analyzing under the same (HLL) algorithm should work
            assertUpdate("ANALYZE " + table.getName());
            assertDistinctValuesCountApproximately(table.getName(), "nationkey", 25);
        }
    }

    private void assertDistinctValuesCountApproximately(String tableName, String columnName, double expected)
    {
        Double actual = distinctValuesCount(tableName, columnName);
        assertThat(actual).isNotNull();
        // HLL is an approximate sketch; allow generous tolerance since exactness is not the point of this test
        assertThat(actual).isCloseTo(expected, withPercentage(20));
    }

    private Double distinctValuesCount(String tableName, String columnName)
    {
        MaterializedResult result = computeActual("SHOW STATS FOR " + tableName);
        for (MaterializedRow row : result.getMaterializedRows()) {
            if (columnName.equals(row.getField(0))) {
                return (Double) row.getField(2);
            }
        }
        throw new AssertionError("Column not found in SHOW STATS: " + columnName);
    }
}
