/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.saphana;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jdbc.BaseJdbcTableStatisticsTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;

import static com.starburstdata.presto.plugin.saphana.SapHanaQueryRunner.createSapHanaQueryRunner;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.sql.TestTable.fromColumns;
import static io.trino.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assumptions.abort;

public abstract class AbstractTestSapHanaTableStatistics
        extends BaseJdbcTableStatisticsTest
{
    protected TestingSapHanaServer sapHanaServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        sapHanaServer = closeAfterClass(TestingSapHanaServer.create());
        return createSapHanaQueryRunner(
                sapHanaServer,
                ImmutableMap.<String, String>builder()
                        .put("case-insensitive-name-matching", "true")
                        .buildOrThrow(),
                ImmutableMap.of(),
                ImmutableList.of(ORDERS));
    }

    @Override
    @Test
    public void testNotAnalyzed()
    {
        String tableName = "test_stats_not_analyzed_" + randomNameSuffix();
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        computeActual(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.orders", tableName));
        try {
            // SAP HANA doesn't have automatic statistics
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
                            "(null, null, null, null, 15000, null, null)");
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
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, 15000, 0, null, 1, 60000)," +
                            "('custkey', null, 1000, 0, null, 1, 1499)," +
                            "('orderstatus', null, 3, 0, null, null, null)," +
                            "('totalprice', null, 14996, 0, null, 874.89, 466001.28)," +
                            "('orderdate', null, 2401, 0, null, null, null)," +
                            "('orderpriority', null, 5, 0, null, null, null)," +
                            "('clerk', null, 1000, 0, null, null, null)," +
                            "('shippriority', null, 1, 0, null, 0, 0)," +
                            "('comment', null, 14995, 0, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
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
            assertQuery("SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, 15000, 0, null, 1, 60000)," +
                            "('custkey', null, 1000, 0.3333333432674408, null, 1, 1499)," +
                            "('orderpriority', null, 5, 0.20000000298023224, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Override
    @Test
    public void testAverageColumnLength()
    {
        abort("SAP HANA connector does not report average column length");
    }

    @Override
    @Test
    public void testPartitionedTable()
    {
        // TODO test with partitioned table
        abort("Not implemented");
    }

    @Override
    @Test
    public void testView()
    {
        abort("SAP HANA doesn't support statistics for views");
    }

    @Override
    @Test
    public void testMaterializedView()
    {
        abort("SAP HANA does not have Materialized Views");
    }

    @Test
    public void testCachedView()
    {
        // TODO test with cached view
        abort("Not implemented");
    }

    @Test
    @Override
    public void testCaseColumnNames()
    {
        String suffix = randomNameSuffix();
        testCaseColumnNames("TEST_STATS_MIXED_UNQUOTED_UPPER_" + suffix.toUpperCase(Locale.ENGLISH));
        testCaseColumnNames("test_stats_mixed_unquoted_lower_" + suffix.toLowerCase(Locale.ENGLISH));
        testCaseColumnNames("test_stats_mixed_uNQuoTeD_miXED_" + suffix);
        testCaseColumnNames(format("\"TEST_STATS_MIXED_QUOTED_UPPER_%s\"", suffix.toUpperCase(Locale.ENGLISH)));
        testCaseColumnNames(format("\"test_stats_mixed_quoted_lower_%s\"", suffix.toLowerCase(Locale.ENGLISH)));
        testCaseColumnNames(format("\"test_stats_mixed_QuoTeD_miXED_%s\"", suffix));
    }

    @Override
    protected void testCaseColumnNames(String tableName)
    {
        String schemaName = getSession().getSchema().orElseThrow();
        sapHanaServer.execute(("" +
                "CREATE TABLE " + format("%s.%s", schemaName, tableName) + " " +
                "AS (SELECT " +
                "  orderkey AS CASE_UNQUOTED_UPPER, " +
                "  custkey AS case_unquoted_lower, " +
                "  orderstatus AS cASe_uNQuoTeD_miXED, " +
                "  totalprice AS \"CASE_QUOTED_UPPER\", " +
                "  orderdate AS \"case_quoted_lower\"," +
                "  orderpriority AS \"CasE_QuoTeD_miXED\" " +
                "FROM " + format("%s.orders", schemaName) + ")"));
        try {
            gatherStats(tableName);
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('case_unquoted_upper', null, 15000, 0, null, 1, 60000)," +
                            "('case_unquoted_lower', null, 1000, 0, null, 1, 1499)," +
                            "('case_unquoted_mixed', null, 3, 0, null, null, null)," +
                            "('case_quoted_upper', null, 14996, 0, null, 874.89, 466001.28)," +
                            "('case_quoted_lower', null, 2401, 0, null, null, null)," +
                            "('case_quoted_mixed', null, 5, 0, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            sapHanaServer.execute(format("DROP TABLE %s.%s", schemaName, tableName));
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
                        //TODO SAP Hana does not support infinity
//                        .put("only_negative_infinity double", List.of("-infinity()", "-infinity()", "-infinity()", "-infinity()"))
//                        .put("only_positive_infinity double", List.of("infinity()", "infinity()", "infinity()", "infinity()"))
//                        .put("mixed_infinities double", List.of("-infinity()", "infinity()", "-infinity()", "infinity()"))
//                        .put("mixed_infinities_and_numbers double", List.of("-infinity()", "infinity()", "-5.0", "7.0"))
                        //TODO SAP Hana does not support NaN
//                        .put("nans_only double", List.of("nan()", "nan()"))
//                        .put("nans_and_numbers double", List.of("nan()", "nan()", "-5.0", "7.0"))
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
                            //TODO SAP Hana does not support infinity
//                            "('only_negative_infinity', null, 1, 0, null, null, null)," +
//                            "('only_positive_infinity', null, 1, 0, null, null, null)," +
//                            "('mixed_infinities', null, 2, 0, null, null, null)," +
//                            "('mixed_infinities_and_numbers', null, 4.0, 0.0, null, null, null)," +
                            //TODO SAP Hana does not support NaN
//                            "('nans_only', null, 1.0, 0.5, null, null, null)," +
//                            "('nans_and_numbers', null, 3.0, 0.0, null, null, null)," +
                            // Min/Max are represented as double values for decimals by SAP Hana
                            "('large_doubles', null, 2.0, 0.0, null, '-5.037190915060955E22', '5.037190915060955E22')," +
                            "('short_decimals_big_fraction', null, 2.0, 0.0, null,  '-1.234567890123456', '1.234567890123456')," +
                            "('short_decimals_big_integral', null, 2.0, 0.0, null, '-1.234567890123456E14', '1.234567890123456E14')," +
                            "('long_decimals_big_fraction', null, 2.0, 0.0, null, '-1.2345678901234567', '1.2345678901234567')," +
                            "('long_decimals_middle', null, 2.0, 0.0, null, '-1.2345678901234568E15', '1.2345678901234568E15')," +
                            "('long_decimals_big_integral', null, 2.0, 0.0, null, '-1.2345678901234568E36', '1.2345678901234568E36')," +
                            "(null, null, null, null, 2, null, null)");
        }
    }
}
