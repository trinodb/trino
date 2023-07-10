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
package io.trino.plugin.mysql;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jdbc.BaseJdbcTableStatisticsTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.assertj.core.api.AbstractDoubleAssert;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static io.trino.plugin.mysql.MySqlQueryRunner.createMySqlQueryRunner;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.sql.TestTable.fromColumns;
import static io.trino.tpch.TpchTable.ORDERS;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withinPercentage;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public abstract class BaseTestMySqlTableStatisticsTest
        extends BaseJdbcTableStatisticsTest
{
    protected final String dockerImageName;
    protected final Function<Double, Double> nullFractionToExpected;
    protected final Function<Integer, Integer> varcharNdvToExpected;
    protected TestingMySqlServer mysqlServer;

    protected BaseTestMySqlTableStatisticsTest(
            String dockerImageName,
            Function<Double, Double> nullFractionToExpected,
            Function<Integer, Integer> varcharNdvToExpected)
    {
        this.dockerImageName = requireNonNull(dockerImageName, "dockerImageName is null");
        this.nullFractionToExpected = requireNonNull(nullFractionToExpected, "nullFractionToExpected is null");
        this.varcharNdvToExpected = requireNonNull(varcharNdvToExpected, "varcharNdvToExpected is null");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        mysqlServer = closeAfterClass(new TestingMySqlServer(dockerImageName, false));

        return createMySqlQueryRunner(
                mysqlServer,
                Map.of(),
                Map.of("case-insensitive-name-matching", "true"),
                List.of(ORDERS));
    }

    @Override
    @Test
    public void testNotAnalyzed()
    {
        String tableName = "test_not_analyzed_" + randomNameSuffix();
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        computeActual(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.orders", tableName));
        try {
            MaterializedResult statsResult = computeActual("SHOW STATS FOR " + tableName);
            assertColumnStats(statsResult, new MapBuilder<String, Integer>()
                    .put("orderkey", null)
                    .put("custkey", null)
                    .put("orderstatus", null)
                    .put("totalprice", null)
                    .put("orderdate", null)
                    .put("orderpriority", null)
                    .put("clerk", null)
                    .put("shippriority", null)
                    .put("comment", null)
                    .build());

            double cardinality = getTableCardinalityFromStats(statsResult);
            // sometimes MySQL will return INFORMATION_SCHEMA.TABLES.TABLE_ROWS as 2 even after loading data
            if (cardinality > 7) {
                assertThat(cardinality).isCloseTo(15000, withinPercentage(50));
            }
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
    @Test
    public void testBasic()
    {
        String tableName = "test_stats_orders_" + randomNameSuffix();
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        computeActual(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.orders", tableName));
        try {
            gatherStats(tableName);
            MaterializedResult statsResult = computeActual("SHOW STATS FOR " + tableName);
            assertColumnStats(statsResult, new MapBuilder<String, Integer>()
                    .put("orderkey", 15000)
                    .put("custkey", 1000)
                    .put("orderstatus", varcharNdvToExpected.apply(3))
                    .put("totalprice", 14996)
                    .put("orderdate", 2401)
                    .put("orderpriority", varcharNdvToExpected.apply(5))
                    .put("clerk", varcharNdvToExpected.apply(1000))
                    .put("shippriority", 1)
                    .put("comment", varcharNdvToExpected.apply(14995))
                    .build());
            assertThat(getTableCardinalityFromStats(statsResult)).isCloseTo(15000, withinPercentage(20));
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
    @Test
    public void testAllNulls()
    {
        String tableName = "test_stats_table_all_nulls_" + randomNameSuffix();
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        computeActual(format("CREATE TABLE %s AS SELECT orderkey, custkey, orderpriority, comment FROM tpch.tiny.orders WHERE false", tableName));
        try {
            computeActual(format("INSERT INTO %s (orderkey) VALUES NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL", tableName));
            gatherStats(tableName);
            MaterializedResult statsResult = computeActual("SHOW STATS FOR " + tableName);
            for (MaterializedRow row : statsResult) {
                String columnName = (String) row.getField(0);
                if (columnName == null) {
                    // table summary row
                    return;
                }
                assertThat(columnName).isIn("orderkey", "custkey", "orderpriority", "comment");

                Double dataSize = (Double) row.getField(1);
                if (dataSize != null) {
                    assertThat(dataSize).as("Data size for " + columnName)
                            .isEqualTo(0);
                }

                if ((columnName.equals("orderpriority") || columnName.equals("comment")) && varcharNdvToExpected.apply(2) == null) {
                    assertNull(row.getField(2), "NDV for " + columnName);
                    assertNull(row.getField(3), "null fraction for " + columnName);
                }
                else {
                    assertNotNull(row.getField(2), "NDV for " + columnName);
                    assertThat(((Number) row.getField(2)).doubleValue()).as("NDV for " + columnName).isBetween(0.0, 2.0);
                    assertEquals(row.getField(3), nullFractionToExpected.apply(1.0), "null fraction for " + columnName);
                }

                assertNull(row.getField(4), "min");
                assertNull(row.getField(5), "max");
            }
            double cardinality = getTableCardinalityFromStats(statsResult);
            if (cardinality != 15.0) {
                // sometimes all-NULLs tables are reported as containing 0-2 rows
                assertThat(cardinality).isBetween(0.0, 2.0);
            }
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
    @Test
    public void testNullsFraction()
    {
        String tableName = "test_stats_table_with_nulls_" + randomNameSuffix();
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
            MaterializedResult statsResult = computeActual("SHOW STATS FOR " + tableName);
            assertColumnStats(
                    statsResult,
                    new MapBuilder<String, Integer>()
                            .put("orderkey", 15000)
                            .put("custkey", 1000)
                            .put("orderpriority", varcharNdvToExpected.apply(5))
                            .build(),
                    new MapBuilder<String, Double>()
                            .put("orderkey", nullFractionToExpected.apply(0.0))
                            .put("custkey", nullFractionToExpected.apply(1.0 / 3))
                            .put("orderpriority", nullFractionToExpected.apply(1.0 / 5))
                            .build());
            assertThat(getTableCardinalityFromStats(statsResult)).isCloseTo(15000, withinPercentage(20));
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
    @Test
    public void testAverageColumnLength()
    {
        throw new SkipException("MySQL connector does not report average column length");
    }

    @Override
    @Test
    public void testPartitionedTable()
    {
        throw new SkipException("Not implemented"); // TODO
    }

    @Override
    @Test
    public void testView()
    {
        String tableName = "test_stats_view_" + randomNameSuffix();
        executeInMysql("CREATE OR REPLACE VIEW " + tableName + " AS SELECT orderkey, custkey, orderpriority, comment FROM orders");
        try {
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, null, null, null, null, null)," +
                            "('custkey', null, null, null, null, null, null)," +
                            "('orderpriority', null, null, null, null, null, null)," +
                            "('comment', null, null, null, null, null, null)," +
                            "(null, null, null, null, null, null, null)");
            // It's not possible to ANALYZE a VIEW in MySQL
        }
        finally {
            executeInMysql("DROP VIEW " + tableName);
        }
    }

    @Override
    @Test
    public void testMaterializedView()
    {
        throw new SkipException(""); // TODO is there a concept like materialized view in MySQL?
    }

    @Override
    @Test(dataProvider = "testCaseColumnNamesDataProvider")
    public void testCaseColumnNames(String tableName)
    {
        executeInMysql(("" +
                "CREATE TABLE " + tableName + " " +
                "AS SELECT " +
                "  orderkey AS CASE_UNQUOTED_UPPER, " +
                "  custkey AS case_unquoted_lower, " +
                "  orderstatus AS cASe_uNQuoTeD_miXED, " +
                "  totalprice AS \"CASE_QUOTED_UPPER\", " +
                "  orderdate AS \"case_quoted_lower\"," +
                "  orderpriority AS \"CasE_QuoTeD_miXED\" " +
                "FROM orders")
                .replace("\"", "`"));
        try {
            gatherStats(tableName);
            MaterializedResult statsResult = computeActual("SHOW STATS FOR " + tableName);
            assertColumnStats(statsResult, new MapBuilder<String, Integer>()
                    .put("case_unquoted_upper", 15000)
                    .put("case_unquoted_lower", 1000)
                    .put("case_unquoted_mixed", varcharNdvToExpected.apply(3))
                    .put("case_quoted_upper", 14996)
                    .put("case_quoted_lower", 2401)
                    .put("case_quoted_mixed", varcharNdvToExpected.apply(5))
                    .build());
            assertThat(getTableCardinalityFromStats(statsResult)).isCloseTo(15000, withinPercentage(20));
        }
        finally {
            executeInMysql("DROP TABLE " + tableName.replace("\"", "`"));
        }
    }

    @Override
    @Test
    public void testNumericCornerCases()
    {
        try (TestTable table = fromColumns(
                getQueryRunner()::execute,
                "test_numeric_corner_cases_",
                ImmutableMap.<String, List<String>>builder()
                        // TODO Infinity and NaNs not supported by MySQL
//                        .put("only_negative_infinity double", List.of("-infinity()", "-infinity()", "-infinity()", "-infinity()"))
//                        .put("only_positive_infinity double", List.of("infinity()", "infinity()", "infinity()", "infinity()"))
//                        .put("mixed_infinities double", List.of("-infinity()", "infinity()", "-infinity()", "infinity()"))
//                        .put("mixed_infinities_and_numbers double", List.of("-infinity()", "infinity()", "-5.0", "7.0"))
//                        .put("nans_only double", List.of("nan()", "nan()"))
//                        .put("nans_and_numbers double", List.of("nan()", "nan()", "-5.0", "7.0"))
                        .put("large_doubles double", List.of("CAST(-50371909150609548946090.0 AS DOUBLE)", "CAST(50371909150609548946090.0 AS DOUBLE)")) // 2^77 DIV 3
                        .put("short_decimals_big_fraction decimal(16,15)", List.of("-1.234567890123456", "1.234567890123456"))
                        .put("short_decimals_big_integral decimal(16,1)", List.of("-123456789012345.6", "123456789012345.6"))
                        // DECIMALS up to precision 30 are supported
                        .put("long_decimals_big_fraction decimal(30,29)", List.of("-1.23456789012345678901234567890", "1.23456789012345678901234567890"))
                        .put("long_decimals_middle decimal(30,16)", List.of("-12345678901234.5678901234567890", "12345678901234.5678901234567890"))
                        .put("long_decimals_big_integral decimal(30,1)", List.of("-12345678901234567890123456789.0", "12345678901234567890123456789.0"))
                        .buildOrThrow(),
                "null")) {
            gatherStats(table.getName());
            assertQuery(
                    "SHOW STATS FOR " + table.getName(),
                    "VALUES " +
                            // TODO Infinity and NaNs not supported by MySQL
//                            "('only_negative_infinity', null, 1, 0, null, null, null)," +
//                            "('only_positive_infinity', null, 1, 0, null, null, null)," +
//                            "('mixed_infinities', null, 2, 0, null, null, null)," +
//                            "('mixed_infinities_and_numbers', null, 4.0, 0.0, null, null, null)," +
//                            "('nans_only', null, 1.0, 0.5, null, null, null)," +
//                            "('nans_and_numbers', null, 3.0, 0.0, null, null, null)," +
                            "('large_doubles', null, 2.0, 0.0, null, null, null)," +
                            "('short_decimals_big_fraction', null, 2.0, 0.0, null, null, null)," +
                            "('short_decimals_big_integral', null, 2.0, 0.0, null, null, null)," +
                            "('long_decimals_big_fraction', null, 2.0, 0.0, null, null, null)," +
                            "('long_decimals_middle', null, 2.0, 0.0, null, null, null)," +
                            "('long_decimals_big_integral', null, 2.0, 0.0, null, null, null)," +
                            "(null, null, null, null, 2, null, null)");
        }
    }

    protected void executeInMysql(String sql)
    {
        try (Handle handle = Jdbi.open(() -> mysqlServer.createConnection())) {
            handle.execute("USE tpch");
            handle.execute(sql);
        }
    }

    protected void assertColumnStats(MaterializedResult statsResult, Map<String, Integer> columnNdvs)
    {
        assertColumnStats(statsResult, columnNdvs, nullFractionToExpected.apply(0.0));
    }

    protected void assertColumnStats(MaterializedResult statsResult, Map<String, Integer> columnNdvs, double nullFraction)
    {
        Map<String, Double> columnNullFractions = new HashMap<>();
        columnNdvs.forEach((columnName, ndv) -> columnNullFractions.put(columnName, ndv == null ? null : nullFraction));

        assertColumnStats(statsResult, columnNdvs, columnNullFractions);
    }

    protected void assertColumnStats(MaterializedResult statsResult, Map<String, Integer> columnNdvs, Map<String, Double> columnNullFractions)
    {
        assertEquals(columnNdvs.keySet(), columnNullFractions.keySet());
        List<String> reportedColumns = stream(statsResult)
                .map(row -> row.getField(0)) // column name
                .filter(Objects::nonNull)
                .map(String.class::cast)
                .collect(toImmutableList());
        assertThat(reportedColumns)
                .containsOnlyOnce(columnNdvs.keySet().toArray(new String[0]));

        double tableCardinality = getTableCardinalityFromStats(statsResult);
        for (MaterializedRow row : statsResult) {
            if (row.getField(0) == null) {
                continue;
            }
            String columnName = (String) row.getField(0);
            verify(columnNdvs.containsKey(columnName));
            Integer expectedNdv = columnNdvs.get(columnName);
            verify(columnNullFractions.containsKey(columnName));
            Double expectedNullFraction = columnNullFractions.get(columnName);

            Double dataSize = (Double) row.getField(1);
            if (dataSize != null) {
                assertThat(dataSize).as("Data size for " + columnName)
                        .isEqualTo(0);
            }

            Double distinctCount = (Double) row.getField(2);
            Double nullsFraction = (Double) row.getField(3);
            AbstractDoubleAssert<?> ndvAssertion = assertThat(distinctCount).as("NDV for " + columnName);
            if (expectedNdv == null) {
                ndvAssertion.isNull();
                assertNull(nullsFraction, "null fraction for " + columnName);
            }
            else {
                ndvAssertion.isBetween(expectedNdv * 0.5, min(expectedNdv * 4.0, tableCardinality)); // [-50%, +300%] but no more than row count
                AbstractDoubleAssert<?> nullsAssertion = assertThat(nullsFraction).as("Null fraction for " + columnName);
                if (distinctCount.compareTo(tableCardinality) >= 0) {
                    nullsAssertion.isEqualTo(0);
                }
                else {
                    double maxNullsFraction = (tableCardinality - distinctCount) / tableCardinality;
                    expectedNullFraction = Math.min(expectedNullFraction, maxNullsFraction);
                    nullsAssertion.isBetween(expectedNullFraction * 0.4, expectedNullFraction * 1.1);
                }
            }

            assertNull(row.getField(4), "min");
            assertNull(row.getField(5), "max");
        }
    }

    protected static double getTableCardinalityFromStats(MaterializedResult statsResult)
    {
        MaterializedRow lastRow = statsResult.getMaterializedRows().get(statsResult.getRowCount() - 1);
        assertNull(lastRow.getField(0));
        assertNull(lastRow.getField(1));
        assertNull(lastRow.getField(2));
        assertNull(lastRow.getField(3));
        assertNull(lastRow.getField(5));
        assertNull(lastRow.getField(6));
        assertEquals(lastRow.getFieldCount(), 7);
        assertNotNull(lastRow.getField(4));
        return ((Number) lastRow.getField(4)).doubleValue();
    }

    protected static class MapBuilder<K, V>
    {
        private final Map<K, V> map = new HashMap<>();

        public MapBuilder<K, V> put(K key, V value)
        {
            checkArgument(!map.containsKey(key), "Key already present: %s", key);
            map.put(requireNonNull(key, "key is null"), value);
            return this;
        }

        public Map<K, V> build()
        {
            return new HashMap<>(map);
        }
    }
}
