/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.synapse;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jdbc.BaseJdbcTableStatisticsTest;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.SoftAssertions;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static com.starburstdata.presto.plugin.synapse.SynapseQueryRunner.createSynapseQueryRunner;
import static io.trino.testing.sql.TestTable.fromColumns;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static io.trino.tpch.TpchTable.NATION;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.from;
import static org.assertj.core.api.Assertions.withinPercentage;

public class TestSynapseTableStatistics
        extends BaseJdbcTableStatisticsTest
{
    private SynapseServer synapseServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        synapseServer = new SynapseServer();
        return createSynapseQueryRunner(synapseServer, Map.of(), List.of(NATION));
    }

    @Override
    @Test
    public void testNotAnalyzed()
    {
        String tableName = "test_stats_not_analyzed_" + randomTableSuffix();
        computeActual(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.nation", tableName));
        try {
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('nationkey', null, null, null, null, null, null)," +
                            "('name', null, null, null, null, null, null)," +
                            "('regionkey', null, null, null, null, null, null)," +
                            "('comment', null, null, null, null, null, null)," +
                            "(null, null, null, null, 1000, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
    @Test
    public void testBasic()
    {
        String tableName = "test_stats_" + randomTableSuffix();
        computeActual(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.nation", tableName));
        try {
            gatherStats(tableName);
            assertThat(showStats(tableName))
                    .get()
                    .returns(Estimate.of(25), from(TableStatistics::getRowCount))
                    .extracting(TableStatistics::getColumnStatistics, InstanceOfAssertFactories.map(ColumnHandle.class, ColumnStatistics.class))
                    .hasEntrySatisfying(handle("nationkey"), statsCloseTo(25, 0, Double.NaN))
                    .hasEntrySatisfying(handle("name"), statsCloseTo(25, 0, 353))
                    .hasEntrySatisfying(handle("regionkey"), statsCloseTo(5, 0, Double.NaN))
                    .hasEntrySatisfying(handle("comment"), statsCloseTo(25, 0, 3713));
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
    protected void checkEmptyTableStats(String tableName)
    {
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('orderkey', 0, 0, 1, null, null, null)," +
                        "('custkey', 0, 0, 1, null, null, null)," +
                        "('orderpriority', 0, 0, 1, null, null, null)," +
                        "('comment', 0, 0, 1, null, null, null)," +
                        // TODO: Empty tables should have total row count as 0 (https://starburstdata.atlassian.net/browse/SEP-5963)
                        "(null, null, null, null, 1, null, null)");
    }

    @Override
    @Test
    public void testAllNulls()
    {
        String tableName = "test_stats_table_all_nulls_" + randomTableSuffix();
        computeActual(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.nation WHERE false", tableName));
        try {
            computeActual(format("INSERT INTO %s (nationkey) VALUES NULL, NULL, NULL", tableName));
            gatherStats(tableName);
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('nationkey', 0, 0, 1, null, null, null)," +
                            "('name', 0, 0, 1, null, null, null)," +
                            "('regionkey', 0, 0, 1, null, null, null)," +
                            "('comment', 0, 0, 1, null, null, null)," +
                            "(null, null, null, null, 3, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
    @Test
    public void testNullsFraction()
    {
        String tableName = "test_stats_table_with_nulls_" + randomTableSuffix();
        assertUpdate("" +
                        "CREATE TABLE " + tableName + " AS " +
                        "SELECT " +
                        "    if(nationkey % 3 = 0, NULL, nationkey) nationkey, " +
                        "    regionkey " +
                        "FROM tpch.tiny.nation",
                25);
        try {
            gatherStats(tableName);
            assertThat(showStats(tableName))
                    .get()
                    .returns(Estimate.of(25), from(TableStatistics::getRowCount))
                    .extracting(TableStatistics::getColumnStatistics, InstanceOfAssertFactories.map(ColumnHandle.class, ColumnStatistics.class))
                    .hasEntrySatisfying(handle("nationkey"), statsCloseTo(16, 0.36, Double.NaN))
                    .hasEntrySatisfying(handle("regionkey"), statsCloseTo(5, 0, Double.NaN));
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
    @Test
    public void testAverageColumnLength()
    {
        String tableName = "test_stats_table_avg_col_len_" + randomTableSuffix();
        computeActual("" +
                "CREATE TABLE " + tableName + " AS SELECT " +
                "  nationkey, " +
                "  'abc' v3_in_3, " +
                "  CAST('abc' AS varchar(42)) v3_in_42, " +
                "  if(nationkey = 1, '0123456789', NULL) single_10v_value, " +
                "  if(nationkey % 2 = 0, '0123456789', NULL) half_10v_value, " +
                "  if(nationkey % 2 = 0, CAST((1000000 - nationkey) * (1000000 - nationkey) AS varchar(20)), NULL) half_distinct_20v_value, " + // 12 chars each
                "  CAST(NULL AS varchar(10)) all_nulls " +
                "FROM tpch.tiny.nation");
        try {
            gatherStats(tableName);
            assertThat(showStats(tableName))
                    .get()
                    .returns(Estimate.of(25), from(TableStatistics::getRowCount))
                    .extracting(TableStatistics::getColumnStatistics, InstanceOfAssertFactories.map(ColumnHandle.class, ColumnStatistics.class))
                    .hasEntrySatisfying(handle("nationkey"), statsCloseTo(25, 0, Double.NaN))
                    .hasEntrySatisfying(handle("v3_in_3"), statsCloseTo(1, 0, 150))
                    .hasEntrySatisfying(handle("v3_in_42"), statsCloseTo(1, 0, 150))
                    .hasEntrySatisfying(handle("single_10v_value"), statsCloseTo(1, 0.96, 20))
                    .hasEntrySatisfying(handle("half_10v_value"), statsCloseTo(1, 0.48, 259))
                    .hasEntrySatisfying(handle("half_distinct_20v_value"), statsCloseTo(13, 0.48, 314))
                    .hasEntrySatisfying(handle("all_nulls"), statsCloseTo(0, 1, 0));
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
    @Test
    public void testPartitionedTable()
    {
        String tableName = "test_stats_partitioned_table_" + randomTableSuffix();
        synapseServer.execute(format("CREATE TABLE %s WITH " +
                "(DISTRIBUTION = ROUND_ROBIN, " +
                "PARTITION (nationkey RANGE LEFT FOR VALUES (12))) " +
                "AS SELECT * FROM nation", tableName));
        try {
            gatherStats(tableName);
            assertThat(showStats(tableName))
                    .get()
                    .returns(Estimate.of(25), from(TableStatistics::getRowCount))
                    .extracting(TableStatistics::getColumnStatistics, InstanceOfAssertFactories.map(ColumnHandle.class, ColumnStatistics.class))
                    .hasEntrySatisfying(handle("nationkey"), statsCloseTo(25, 0, Double.NaN))
                    .hasEntrySatisfying(handle("name"), statsCloseTo(25, 0, 353))
                    .hasEntrySatisfying(handle("regionkey"), statsCloseTo(5, 0, Double.NaN))
                    .hasEntrySatisfying(handle("comment"), statsCloseTo(25, 0, 3713));
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
    @Test
    public void testView()
    {
        String tableName = "dbo.test_stats_view_" + randomTableSuffix();
        synapseServer.execute("DROP VIEW IF EXISTS " + tableName);
        synapseServer.execute("CREATE VIEW " + tableName + " AS SELECT * FROM nation");
        try {
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('nationkey', null, null, null, null, null, null)," +
                            "('name', null, null, null, null, null, null)," +
                            "('regionkey', null, null, null, null, null, null)," +
                            "('comment', null, null, null, null, null, null)," +
                            "(null, null, null, null, null, null, null)");
            // It's not possible to ANALYZE a VIEW in Synapse
        }
        finally {
            synapseServer.execute("DROP VIEW IF EXISTS " + tableName);
        }
    }

    @Override
    @Test
    public void testMaterializedView()
    {
        throw new SkipException("Synapse does not support statistics on materialized views");
    }

    @Override
    @Test(dataProvider = "testCaseColumnNamesDataProvider")
    public void testCaseColumnNames(String tableName)
    {
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        synapseServer.execute("" +
                "SELECT " +
                "  nationkey CASE_UNQUOTED_UPPER, " +
                "  name case_unquoted_lower, " +
                "  regionkey cASe_uNQuoTeD_miXED, " +
                "  comment \"CASE_QUOTED_UPPER\", " +
                "  nationkey \"case_quoted_lower\", " +
                "  name \"CasE_QuoTeD_miXED\" " +
                "INTO " + tableName + " " +
                "FROM nation");
        try {
            gatherStats(tableName);
            assertThat(showStats(tableName))
                    .get()
                    .returns(Estimate.of(25), from(TableStatistics::getRowCount))
                    .extracting(TableStatistics::getColumnStatistics, InstanceOfAssertFactories.map(ColumnHandle.class, ColumnStatistics.class))
                    .hasEntrySatisfying(handle("case_unquoted_upper"), statsCloseTo(25, 0, Double.NaN))
                    .hasEntrySatisfying(handle("case_unquoted_lower"), statsCloseTo(25, 0, 353))
                    .hasEntrySatisfying(handle("case_unquoted_mixed"), statsCloseTo(5, 0, Double.NaN))
                    .hasEntrySatisfying(handle("case_quoted_upper"), statsCloseTo(25, 0, 3713))
                    .hasEntrySatisfying(handle("case_quoted_lower"), statsCloseTo(25, 0, Double.NaN))
                    .hasEntrySatisfying(handle("case_quoted_mixed"), statsCloseTo(25, 0, 353));
        }
        finally {
            synapseServer.execute("DROP TABLE " + tableName);
        }
    }

    @Override
    @DataProvider
    public Object[][] testCaseColumnNamesDataProvider()
    {
        return new Object[][] {
                {format("TEST_STATS_MIXED_UNQUOTED_UPPER_%s", randomTableSuffix())},
                {format("test_stats_mixed_unquoted_lower_%s", randomTableSuffix())},
                {format("test_stats_mixed_uNQuoTeD_miXED_%s", randomTableSuffix())},
                {format("\"TEST_STATS_MIXED_QUOTED_UPPER_%s\"", randomTableSuffix())},
                {format("\"test_stats_mixed_quoted_lower_%s\"", randomTableSuffix())},
                {format("\"test_stats_mixed_QuoTeD_miXED_%s\"", randomTableSuffix())},
        };
    }

    @Override
    @Test
    public void testNumericCornerCases()
    {
        try (TestTable table = fromColumns(
                getQueryRunner()::execute,
                "test_numeric_corner_cases_",
                ImmutableMap.<String, List<String>>builder()
                        .put("large_doubles double", List.of("CAST(-50371909150609548946090.0 AS DOUBLE)", "CAST(50371909150609548946090.0 AS DOUBLE)")) // 2^77 DIV 3
                        .put("short_decimals_big_fraction decimal(16,15)", List.of("-1.234567890123456", "1.234567890123456"))
                        .put("short_decimals_big_integral decimal(16,1)", List.of("-123456789012345.6", "123456789012345.6"))
                        .put("long_decimals_big_fraction decimal(38,37)", List.of("-1.2345678901234567890123456789012345678", "1.2345678901234567890123456789012345678"))
                        .put("long_decimals_middle decimal(38,16)", List.of("-1234567890123456.7890123456789012345678", "1234567890123456.7890123456789012345678"))
                        .put("long_decimals_big_integral decimal(38,1)", List.of("-1234567890123456789012345678901234567.8", "1234567890123456789012345678901234567.8"))
                        .buildOrThrow(),
                "null")) {
            gatherStats(table.getName());
            assertQuery(
                    "SHOW STATS FOR " + table.getName(),
                    "VALUES " +
                            "('large_doubles', null, 2.0, 0.0, null, null, null)," +
                            "('short_decimals_big_fraction', null, 2.0, 0.0, null, null, null)," +
                            "('short_decimals_big_integral', null, 2.0, 0.0, null, null, null)," +
                            "('long_decimals_big_fraction', null, 2.0, 0.0, null, null, null)," +
                            "('long_decimals_middle', null, 2.0, 0.0, null, null, null)," +
                            "('long_decimals_big_integral', null, 2.0, 0.0, null, null, null)," +
                            "(null, null, null, null, 2, null, null)");
        }
    }

    @Test
    public void testShowStatsAfterCreateIndex()
    {
        String tableName = "test_stats_create_index_" + randomTableSuffix();
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        computeActual(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.nation", tableName));

        try {
            gatherStats(tableName);
            assertThat(showStats(tableName))
                    .get()
                    .returns(Estimate.of(25), from(TableStatistics::getRowCount))
                    .extracting(TableStatistics::getColumnStatistics, InstanceOfAssertFactories.map(ColumnHandle.class, ColumnStatistics.class))
                    .hasEntrySatisfying(handle("nationkey"), statsCloseTo(25, 0, Double.NaN))
                    .hasEntrySatisfying(handle("name"), statsCloseTo(25, 0, 353))
                    .hasEntrySatisfying(handle("regionkey"), statsCloseTo(5, 0, Double.NaN))
                    .hasEntrySatisfying(handle("comment"), statsCloseTo(25, 0, 3713));

            // CREATE INDEX statement updates sys.partitions table
            synapseServer.execute(format("CREATE INDEX unique_index ON %s (nationkey)", tableName));

            assertThat(showStats(tableName))
                    .get()
                    .returns(Estimate.of(25), from(TableStatistics::getRowCount))
                    .extracting(TableStatistics::getColumnStatistics, InstanceOfAssertFactories.map(ColumnHandle.class, ColumnStatistics.class))
                    .hasEntrySatisfying(handle("nationkey"), statsCloseTo(25, 0, Double.NaN))
                    .hasEntrySatisfying(handle("name"), statsCloseTo(25, 0, 353))
                    .hasEntrySatisfying(handle("regionkey"), statsCloseTo(5, 0, Double.NaN))
                    .hasEntrySatisfying(handle("comment"), statsCloseTo(25, 0, 3713));
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
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

    @Override
    protected void gatherStats(String tableName)
    {
        List<String> columnNames = stream(computeActual("SHOW COLUMNS FROM " + tableName))
                .map(row -> (String) row.getField(0))
                .collect(toImmutableList());
        for (Object columnName : columnNames) {
            synapseServer.execute(format("CREATE STATISTICS %1$s ON %2$s (%1$s)", columnName, tableName));
        }
        synapseServer.execute("UPDATE STATISTICS " + tableName);
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
            else {
                continue;
            }
        }
        return Optional.of(tableStatistics.build());
    }

    private static Consumer<ColumnStatistics> statsCloseTo(double distinctValues, double nullsFraction, double dataSize)
    {
        return stats -> {
            SoftAssertions softly = new SoftAssertions();

            softly.assertThat(stats.getDistinctValuesCount().getValue())
                    .isCloseTo(distinctValues, withinPercentage(80.0));

            softly.assertThat(stats.getNullsFraction().getValue())
                    .isCloseTo(nullsFraction, withinPercentage(80.0));

            softly.assertThat(stats.getDataSize().getValue())
                    .isCloseTo(dataSize, withinPercentage(80.0));

            softly.assertThat(stats.getRange()).isEmpty();
            softly.assertAll();
        };
    }
}
