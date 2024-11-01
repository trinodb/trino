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
package io.trino.plugin.vertica;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jdbc.BaseJdbcTableStatisticsTest;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingMetadata.TestingColumnHandle;
import io.trino.testing.sql.TestTable;
import io.trino.tpch.TpchTable;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.SoftAssertions;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static io.trino.plugin.vertica.TestingVerticaServer.LATEST_IMAGE;
import static io.trino.plugin.vertica.VerticaQueryRunner.TPCH_SCHEMA;
import static io.trino.testing.sql.TestTable.fromColumns;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.from;
import static org.assertj.core.api.Assertions.withinPercentage;
import static org.junit.jupiter.api.Assumptions.abort;

public class TestVerticaTableStatistics
        extends BaseJdbcTableStatisticsTest
{
    private TestingVerticaServer verticaServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // Use the latest image to avoid "Must be superuser to run export_statistics"
        verticaServer = closeAfterClass(new TestingVerticaServer(LATEST_IMAGE));
        return VerticaQueryRunner.builder(verticaServer)
                .addConnectorProperty("statistics.enabled", "true")
                .setTables(ImmutableList.of(TpchTable.ORDERS, TpchTable.REGION, TpchTable.NATION))
                .build();
    }

    @Override
    protected void checkEmptyTableStats(String tableName)
    {
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('orderkey', null, null, null, null, null, null)," +
                        "('custkey', null, null, null, null, null, null)," +
                        "('orderpriority', null, null, null, null, null, null)," +
                        "('comment', null, null, null, null, null, null)," +
                        "(null, null, null, null, null, null, null)");
    }

    @Test
    @Override
    public void testNotAnalyzed()
    {
        String tableName = "test_stats_not_analyzed";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        computeActual(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.orders", tableName));
        try {
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, null, null, null, null, null)," +
                            "('custkey', null, null, null, null, null, null)," +
                            "('orderstatus', null, null, null, null, null, null)," +
                            "('totalprice', null, null, null, null, null, null)," +
                            "('orderdate', null, null, null, null, null, null)," +
                            "('orderpriority', null, null, null, null, null, null)," +
                            "('clerk', null, null, null, null, null, null)," +
                            "('shippriority', null, null, null, null, null, null)," +
                            "('comment', null, null, null, null, null, null)," +
                            "(null, null, null, null, null, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    @Override
    public void testBasic()
    {
        String tableName = "test_stats_orders";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        computeActual(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.orders", tableName));
        try {
            gatherStats(tableName);
            assertThat(showStats(tableName))
                    .get()
                    .returns(Estimate.of(15000), from(TableStatistics::getRowCount))
                    .extracting(TableStatistics::getColumnStatistics, InstanceOfAssertFactories.map(ColumnHandle.class, ColumnStatistics.class))
                    .hasEntrySatisfying(handle("orderkey"), statsCloseTo(null, 15000, 0, 1.0, 60000.0))
                    .hasEntrySatisfying(handle("custkey"), statsCloseTo(null, 1000, 0, 1.0, 1499.0))
                    .hasEntrySatisfying(handle("orderstatus"), statsCloseTo(9605.0, 3, 0, null, null))
                    .hasEntrySatisfying(handle("totalprice"), statsCloseTo(null, 14996, 0, 874.89, 466001.0))
                    .hasEntrySatisfying(handle("orderdate"), statsCloseTo(null, 2401, 0, null, null))
                    .hasEntrySatisfying(handle("orderpriority"), statsCloseTo(28284.0, 5, 0, null, null))
                    .hasEntrySatisfying(handle("clerk"), statsCloseTo(56521.0, 1000, 0, null, null))
                    .hasEntrySatisfying(handle("shippriority"), statsCloseTo(null, 1, 0, 0.0, 0.0))
                    .hasEntrySatisfying(handle("comment"), statsCloseTo(289736.0, 14995, 0, null, null));
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    @Override
    public void testAllNulls()
    {
        String tableName = "test_stats_table_all_nulls";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        computeActual(format("CREATE TABLE %s AS SELECT orderkey, custkey, orderpriority, comment FROM tpch.tiny.orders WHERE false", tableName));
        try {
            computeActual(format("INSERT INTO %s (orderkey) VALUES NULL, NULL, NULL", tableName));
            gatherStats(tableName);
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', 0, 0, 1, null, null, null)," +
                            "('custkey', 0, 0, 1, null, null, null)," +
                            "('orderpriority', 0, 0, 1, null, null, null)," +
                            "('comment', 0, 0, 1, null, null, null)," +
                            "(null, null, null, null, 3, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    @Override
    public void testNullsFraction()
    {
        String tableName = "test_stats_table_with_nulls";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        assertUpdate("" +
                        "CREATE TABLE " + tableName + " AS " +
                        "SELECT " +
                        "    orderkey, " +
                        "    if(orderkey % 3 = 0, NULL, custkey) custkey, " +
                        "    if(orderkey % 5 = 0, NULL, orderpriority) orderpriority " +
                        "FROM tpch.tiny.orders",
                15000);
        try {
            gatherStats(tableName);
            assertThat(showStats(tableName))
                    .get()
                    .returns(Estimate.of(15000), from(TableStatistics::getRowCount))
                    .extracting(TableStatistics::getColumnStatistics, InstanceOfAssertFactories.map(ColumnHandle.class, ColumnStatistics.class))
                    .hasEntrySatisfying(handle("orderkey"), statsCloseTo(null, 15000, 0, 1.0, 60000.0))
                    .hasEntrySatisfying(handle("custkey"), statsCloseTo(null, 1000, 0.3333333333333333, null, 1499.0))
                    .hasEntrySatisfying(handle("orderpriority"), statsCloseTo(28284.0, 5, 0.2, null, null));
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    @Override
    public void testAverageColumnLength()
    {
        abort("Vertica connector does not report average column length");
    }

    @Test
    @Override
    public void testPartitionedTable()
    {
        String tableName = "tpch.test_stats_orders_part";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        assertUpdate(format("CREATE TABLE %s AS SELECT * FROM orders", tableName), 15000);
        onVertica(format("ALTER TABLE %s PARTITION BY YEAR(orderdate) REORGANIZE", tableName));
        try {
            gatherStats(tableName);
            assertThat(showStats(tableName))
                    .get()
                    .returns(Estimate.of(15000), from(TableStatistics::getRowCount))
                    .extracting(TableStatistics::getColumnStatistics, InstanceOfAssertFactories.map(ColumnHandle.class, ColumnStatistics.class))
                    .hasEntrySatisfying(handle("orderkey"), statsCloseTo(null, 15000, 0, 1.0, 60000.0))
                    .hasEntrySatisfying(handle("custkey"), statsCloseTo(null, 1000, 0, 1.0, 1499.0))
                    .hasEntrySatisfying(handle("orderstatus"), statsCloseTo(1529.0, 3, 0, null, null)) // Note that the data size is different from non-partitioned tables
                    .hasEntrySatisfying(handle("totalprice"), statsCloseTo(null, 14996, 0, 874.89, 466001.0))
                    .hasEntrySatisfying(handle("orderdate"), statsCloseTo(null, 2401, 0, null, null))
                    .hasEntrySatisfying(handle("orderpriority"), statsCloseTo(28284.0, 5, 0, null, null))
                    .hasEntrySatisfying(handle("clerk"), statsCloseTo(56521.0, 1000, 0, null, null))
                    .hasEntrySatisfying(handle("shippriority"), statsCloseTo(null, 1, 0, 0.0, 0.0))
                    .hasEntrySatisfying(handle("comment"), statsCloseTo(289736.0, 14995, 0, null, null));
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    @Override
    public void testView()
    {
        String tableName = "tpch.test_stats_view";
        onVertica("CREATE OR REPLACE VIEW " + tableName + " AS SELECT orderkey, custkey, orderpriority, \"COMMENT\" FROM tpch.orders");
        try {
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, null, null, null, null, null)," +
                            "('custkey', null, null, null, null, null, null)," +
                            "('orderpriority', null, null, null, null, null, null)," +
                            "('comment', null, null, null, null, null, null)," +
                            "(null, null, null, null, null, null, null)");
        }
        finally {
            onVertica("DROP VIEW " + tableName);
        }
    }

    @Test
    @Override
    public void testMaterializedView()
    {
        abort("Vertica does not have Materialized Views");
    }

    @Override
    protected void testCaseColumnNames(String tableName)
    {
        onVertica("" +
                "CREATE TABLE tpch." + tableName + " " +
                "AS SELECT " +
                "  orderkey AS CASE_UNQUOTED_UPPER, " +
                "  custkey AS case_unquoted_lower, " +
                "  orderstatus AS cASe_uNQuoTeD_miXED, " +
                "  totalprice AS \"CASE_QUOTED_UPPER\", " +
                "  orderdate AS \"case_quoted_lower\"," +
                "  orderpriority AS \"CasE_QuoTeD_miXED\" " +
                "FROM tpch.orders");
        try {
            gatherStats(tableName);
            assertThat(showStats(tableName))
                    .get()
                    .returns(Estimate.of(15000), from(TableStatistics::getRowCount))
                    .extracting(TableStatistics::getColumnStatistics, InstanceOfAssertFactories.map(ColumnHandle.class, ColumnStatistics.class))
                    .hasEntrySatisfying(handle("case_unquoted_upper"), statsCloseTo(null, 15000, 0, 1.0, 60000.0))
                    .hasEntrySatisfying(handle("case_unquoted_lower"), statsCloseTo(null, 1000, 0, 1.0, 1499.0))
                    .hasEntrySatisfying(handle("case_unquoted_mixed"), statsCloseTo(9605.0, 3, 0, null, null))
                    .hasEntrySatisfying(handle("case_quoted_upper"), statsCloseTo(null, 14996, 0, 874.89, 466001.0))
                    .hasEntrySatisfying(handle("case_quoted_lower"), statsCloseTo(null, 2401, 0, null, null))
                    .hasEntrySatisfying(handle("case_quoted_mixed"), statsCloseTo(28284.0, 5, 0, null, null));
        }
        finally {
            onVertica("DROP TABLE tpch." + tableName);
        }
    }

    @Test
    @Override
    public void testStatsWithAggregationPushdown()
    {
        assertThatThrownBy(super::testStatsWithAggregationPushdown)
                .hasMessageContaining("Plan does not match");
        abort("Aggregate pushdown is unsupported in Vertica connector");
    }

    @Test
    @Override
    public void testStatsWithTopNPushdown()
    {
        assertThatThrownBy(super::testStatsWithTopNPushdown)
                .hasMessageContaining("Plan does not match");
        abort("TopN pushdown is unsupported in Vertica connector");
    }

    @Test
    @Override
    public void testNumericCornerCases()
    {
        try (TestTable table = fromColumns(
                getQueryRunner()::execute,
                "test_numeric_corner_cases_",
                ImmutableMap.<String, List<String>>builder()
                        .put("only_negative_infinity double", List.of("-infinity()", "-infinity()", "-infinity()", "-infinity()"))
                        .put("only_positive_infinity double", List.of("infinity()", "infinity()", "infinity()", "infinity()"))
                        .put("mixed_infinities double", List.of("-infinity()", "infinity()", "-infinity()", "infinity()"))
                        .put("mixed_infinities_and_numbers double", List.of("-infinity()", "infinity()", "-5.0", "7.0"))
                        .put("nans_only double", List.of("nan()", "nan()"))
                        .put("nans_and_numbers double", List.of("nan()", "nan()", "-5.0", "7.0"))
                        .put("nans_and_numbers_and_null double", List.of("nan()", "10.0"))
                        .put("large_doubles double", List.of("CAST(-50371909150609548946090.0 AS DOUBLE)", "CAST(50371909150609548946090.0 AS DOUBLE)")) // 2^77 DIV 3
                        .put("short_decimals_big_fraction decimal(16,15)", List.of("-1.234567890123456", "1.234567890123456"))
                        .put("short_decimals_big_integral decimal(16,1)", List.of("-123456789012345.6", "123456789012345.6"))
                        .put("long_decimals_big_fraction decimal(38,37)", List.of("-1.2345678901234567890123456789012345678", "1.2345678901234567890123456789012345678"))
                        .put("long_decimals_middle decimal(38,16)", List.of("-1234567890123456.7890123456789012345678", "1234567890123456.7890123456789012345678"))
                        .put("long_decimals_big_integral decimal(38,1)", List.of("-1234567890123456789012345678901234567.8", "1234567890123456789012345678901234567.8"))
                        .buildOrThrow(),
                "null")) {
            gatherStats(table.getName());
            assertThat(showStats(table.getName()))
                    .get()
                    .returns(Estimate.of(4), from(TableStatistics::getRowCount))
                    .extracting(TableStatistics::getColumnStatistics, InstanceOfAssertFactories.map(ColumnHandle.class, ColumnStatistics.class))
                    .hasEntrySatisfying(handle("only_negative_infinity"), statsCloseTo(null, 1, 0, null, null))
                    .hasEntrySatisfying(handle("only_positive_infinity"), statsCloseTo(null, 1, 0, null, null))
                    .hasEntrySatisfying(handle("mixed_infinities"), statsCloseTo(null, 2, 0, null, null))
                    .hasEntrySatisfying(handle("mixed_infinities_and_numbers"), statsCloseTo(null, 4, 0, null, null))
                    .hasEntrySatisfying(handle("nans_only"), statsCloseTo(null, 2, 0.1, null, null)) // nulls faction is 0.1 (unknown) because we can't calculate it when the rows are only nan and null
                    .hasEntrySatisfying(handle("nans_and_numbers"), statsCloseTo(null, 3, 0, null, null))
                    .hasEntrySatisfying(handle("nans_and_numbers_and_null"), statsCloseTo(null, 3, 0, null, null))
                    .hasEntrySatisfying(handle("large_doubles"), statsCloseTo(null, 2, 0.5, -5.03719E22, null))
                    .hasEntrySatisfying(handle("short_decimals_big_fraction"), statsCloseTo(null, 2, 0.5, null, 1.234567890123456))
                    .hasEntrySatisfying(handle("short_decimals_big_integral"), statsCloseTo(null, 2, 0.5, null, 123456789012345.6))
                    .hasEntrySatisfying(handle("long_decimals_big_fraction"), statsCloseTo(null, 2, 0.5, null, 1.2345678901234567890123456789012345678))
                    .hasEntrySatisfying(handle("long_decimals_middle"), statsCloseTo(null, 2, 0.5, null, 1234567890123456.7890123456789012345678))
                    .hasEntrySatisfying(handle("long_decimals_big_integral"), statsCloseTo(null, 2, 0.5, null, 1234567890123456789012345678901234567.8));
        }
    }

    @Override
    protected void gatherStats(String tableName)
    {
        onVertica(format("SELECT ANALYZE_STATISTICS('%s.%s')", TPCH_SCHEMA, tableName));
    }

    private void onVertica(@Language("SQL") String sql)
    {
        verticaServer.execute(sql);
    }

    private Optional<TableStatistics> showStats(String tableName)
    {
        List<MaterializedRow> showStatsResult = computeActual("SHOW STATS FOR " + tableName).getMaterializedRows();
        double rowCount = (double) showStatsResult.get(showStatsResult.size() - 1).getField(4);

        TableStatistics.Builder tableStatistics = TableStatistics.builder();
        tableStatistics.setRowCount(Estimate.of(rowCount));

        for (MaterializedRow materializedRow : showStatsResult) {
            if (materializedRow.getField(0) != null) {
                ColumnStatistics statistics = ColumnStatistics.builder()
                        .setDataSize(asEstimate(materializedRow.getField(1)))
                        .setDistinctValuesCount(Estimate.of((Double) materializedRow.getField(2)))
                        .setNullsFraction(Estimate.of((Double) materializedRow.getField(3)))
                        .build();

                tableStatistics.setColumnStatistics(
                        handle(String.valueOf(materializedRow.getField(0))),
                        statistics);
            }
        }
        return Optional.of(tableStatistics.build());
    }

    private ColumnHandle handle(String name)

    {
        return new TestingColumnHandle(name);
    }

    private Estimate asEstimate(Object value)
    {
        if (value == null) {
            return Estimate.unknown();
        }
        return Estimate.of((Double) value);
    }

    private static Consumer<ColumnStatistics> statsCloseTo(Double dataSize, double distinctValues, double nullsFraction, Double lowValue, Double highValue)
    {
        return stats -> {
            SoftAssertions softly = new SoftAssertions();

            if (dataSize != null) {
                softly.assertThat(stats.getDataSize().getValue())
                        .isCloseTo(dataSize, withinPercentage(80.0));
            }

            assertThat(stats.getDistinctValuesCount().getValue()).isEqualTo(distinctValues);
            assertThat(stats.getNullsFraction().getValue()).isEqualTo(nullsFraction);

            if (stats.getRange().isPresent()) {
                softly.assertThat(stats.getRange().get().getMin())
                        .isCloseTo(lowValue, withinPercentage(80.0));
                softly.assertThat(stats.getRange().get().getMax())
                        .isCloseTo(highValue, withinPercentage(80.0));
            }

            softly.assertThat(stats.getRange()).isEmpty();
            softly.assertAll();
        };
    }
}
