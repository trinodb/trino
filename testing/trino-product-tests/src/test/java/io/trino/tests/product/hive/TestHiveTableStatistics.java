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
package io.trino.tests.product.hive;

import com.google.common.collect.ImmutableList;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.testing.services.junit.Flaky;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_ISSUES;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_MATCH;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * JUnit 5 port of TestHiveTableStatistics.
 * <p>
 * Tests Hive table statistics functionality including ANALYZE TABLE
 * and column statistics for various data types and partitioning schemes.
 */
@ProductTest
@RequiresEnvironment(MultinodeHive4Environment.class)
class TestHiveTableStatistics
{
    // All simple types table DDL for creating test tables
    private static final String ALL_TYPES_TABLE_DDL = """
            c_tinyint            TINYINT,
            c_smallint           SMALLINT,
            c_int                INT,
            c_bigint             BIGINT,
            c_float              REAL,
            c_double             DOUBLE,
            c_decimal            DECIMAL(10,0),
            c_decimal_w_params   DECIMAL(10,5),
            c_timestamp          TIMESTAMP,
            c_date               DATE,
            c_string             VARCHAR,
            c_varchar            VARCHAR(10),
            c_char               CHAR(10),
            c_boolean            BOOLEAN,
            c_binary             VARBINARY
            """;

    // Data for all_types table
    private static final String ALL_TYPES_DATA_ROW_1 = """
            (TINYINT '121', SMALLINT '32761', INTEGER '2147483641', BIGINT '9223372036854775801',
             REAL '123.341', DOUBLE '234.561', CAST(344.0 AS DECIMAL(10,0)), CAST(345.671 AS DECIMAL(10,5)),
             TIMESTAMP '2015-05-10 12:15:31.123456', DATE '2015-05-09',
             'ela ma kota', CAST('ela ma kot' AS VARCHAR(10)), CAST('ela ma    ' AS CHAR(10)), false,
             FROM_BASE64('cGllcyBiaW5hcm55'))
            """;

    private static final String ALL_TYPES_DATA_ROW_2 = """
            (TINYINT '127', SMALLINT '32767', INTEGER '2147483647', BIGINT '9223372036854775807',
             REAL '123.345', DOUBLE '235.567', CAST(345.0 AS DECIMAL(10,0)), CAST(345.678 AS DECIMAL(10,5)),
             TIMESTAMP '2015-05-10 12:15:35.123456', DATE '2015-06-10',
             'ala ma kota', CAST('ala ma kot' AS VARCHAR(10)), CAST('ala ma    ' AS CHAR(10)), true,
             FROM_BASE64('a290IGJpbmFybnk='))
            """;

    private static final String ALL_TYPES_ALL_NULL_ROW = """
            (null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)
            """;

    private static List<Row> getAllTypesTableStatistics()
    {
        return ImmutableList.of(
                row("c_tinyint", null, 2.0, 0.0, null, "121", "127"),
                row("c_smallint", null, 2.0, 0.0, null, "32761", "32767"),
                row("c_int", null, 2.0, 0.0, null, "2147483641", "2147483647"),
                row("c_bigint", null, 1.0, 0.0, null, "9223372036854775807", "9223372036854775807"),
                row("c_float", null, 2.0, 0.0, null, "123.341", "123.345"),
                row("c_double", null, 2.0, 0.0, null, "234.561", "235.567"),
                row("c_decimal", null, 2.0, 0.0, null, "345.0", "346.0"),
                row("c_decimal_w_params", null, 2.0, 0.0, null, "345.671", "345.678"),
                row("c_timestamp", null, 2.0, 0.0, null, null, null),
                row("c_date", null, 2.0, 0.0, null, "2015-05-09", "2015-06-10"),
                row("c_string", 22.0, 2.0, 0.0, null, null, null),
                row("c_varchar", 20.0, 2.0, 0.0, null, null, null),
                row("c_char", 12.0, 2.0, 0.0, null, null, null),
                row("c_boolean", null, 2.0, 0.0, null, null, null),
                row("c_binary", 23.0, null, 0.0, null, null, null),
                row(null, null, null, null, 2.0, null, null));
    }

    private static List<Row> getAllTypesAllNullTableStatistics()
    {
        return ImmutableList.of(
                row("c_tinyint", 0.0, 0.0, 1.0, null, null, null),
                row("c_smallint", 0.0, 0.0, 1.0, null, null, null),
                row("c_int", 0.0, 0.0, 1.0, null, null, null),
                row("c_bigint", 0.0, 0.0, 1.0, null, null, null),
                row("c_float", 0.0, 0.0, 1.0, null, null, null),
                row("c_double", 0.0, 0.0, 1.0, null, null, null),
                row("c_decimal", 0.0, 0.0, 1.0, null, null, null),
                row("c_decimal_w_params", 0.0, 0.0, 1.0, null, null, null),
                row("c_timestamp", 0.0, 0.0, 1.0, null, null, null),
                row("c_date", 0.0, 0.0, 1.0, null, null, null),
                row("c_string", 0.0, 0.0, 1.0, null, null, null),
                row("c_varchar", 0.0, 0.0, 1.0, null, null, null),
                row("c_char", 0.0, 0.0, 1.0, null, null, null),
                row("c_boolean", 0.0, 0.0, 1.0, null, null, null),
                row("c_binary", 0.0, null, 1.0, null, null, null),
                row(null, null, null, null, 1.0, null, null));
    }

    private static List<Row> getAllTypesEmptyTableStatistics()
    {
        return ImmutableList.of(
                row("c_tinyint", 0.0, 0.0, 1.0, null, null, null),
                row("c_smallint", 0.0, 0.0, 1.0, null, null, null),
                row("c_int", 0.0, 0.0, 1.0, null, null, null),
                row("c_bigint", 0.0, 0.0, 1.0, null, null, null),
                row("c_float", 0.0, 0.0, 1.0, null, null, null),
                row("c_double", 0.0, 0.0, 1.0, null, null, null),
                row("c_decimal", 0.0, 0.0, 1.0, null, null, null),
                row("c_decimal_w_params", 0.0, 0.0, 1.0, null, null, null),
                row("c_timestamp", 0.0, 0.0, 1.0, null, null, null),
                row("c_date", 0.0, 0.0, 1.0, null, null, null),
                row("c_string", 0.0, 0.0, 1.0, null, null, null),
                row("c_varchar", 0.0, 0.0, 1.0, null, null, null),
                row("c_char", 0.0, 0.0, 1.0, null, null, null),
                row("c_boolean", 0.0, 0.0, 1.0, null, null, null),
                row("c_binary", 0.0, 0.0, 1.0, null, null, null),
                row(null, null, null, null, 0.0, null, null));
    }

    private static List<Row> skewedTableStatsWithoutColumnStatistics()
    {
        return ImmutableList.of(
                row("c_string", null, null, null, null, null, null),
                row("c_int", null, null, null, null, null, null),
                row(null, null, null, null, 2.0, null, null));
    }

    private static List<Row> skewedTableStatsWithColumnStatistics()
    {
        return ImmutableList.of(
                row("c_string", 4.0, 1.0, 0.0, null, null, null),
                row("c_int", null, 2.0, 0.0, null, "1", "2"),
                row(null, null, null, null, 2.0, null, null));
    }

    private static void assertSkewedTableStats(MultinodeHive4Environment env, String tableName, boolean allowWithoutColumnStatistics)
    {
        List<Row> actualRows = env.executeTrino("SHOW STATS FOR hive.default." + tableName).getRows();
        List<Row> withColumnStats = skewedTableStatsWithColumnStatistics();
        if (allowWithoutColumnStatistics) {
            List<Row> withoutColumnStats = skewedTableStatsWithoutColumnStatistics();
            boolean matchesWithColumnStats = actualRows.size() == withColumnStats.size() &&
                    actualRows.containsAll(withColumnStats) &&
                    withColumnStats.containsAll(actualRows);
            boolean matchesWithoutColumnStats = actualRows.size() == withoutColumnStats.size() &&
                    actualRows.containsAll(withoutColumnStats) &&
                    withoutColumnStats.containsAll(actualRows);
            assertThat(matchesWithColumnStats || matchesWithoutColumnStats)
                    .describedAs("Expected skewed table stats with or without column stats, but got: %s", actualRows)
                    .isTrue();
            return;
        }

        assertThat(actualRows)
                .containsExactlyInAnyOrderElementsOf(withColumnStats);
    }

    // Helper class to simulate anyOf() behavior for statistics comparison
    private static class AnyOf
    {
        private final List<Double> values;

        private AnyOf(Double... values)
        {
            this.values = Arrays.asList(values);
        }

        static AnyOf anyOf(Double... values)
        {
            return new AnyOf(values);
        }

        boolean matches(Object actual)
        {
            if (actual == null) {
                return values.stream().anyMatch(Objects::isNull);
            }
            if (actual instanceof Double) {
                return values.contains(actual);
            }
            if (actual instanceof Number number) {
                return values.contains(number.doubleValue());
            }
            return false;
        }

        @Override
        public String toString()
        {
            return "anyOf" + values;
        }
    }

    // Helper method to create a row that accepts AnyOf values
    private static void assertStatsContainsOnly(MultinodeHive4Environment env, String showStatsQuery, List<Object[]> expectedRows)
    {
        var result = env.executeTrino(showStatsQuery);
        var actualRows = result.getRows();

        if (actualRows.size() != expectedRows.size()) {
            throw new AssertionError(format("Expected %d rows but got %d.\nExpected: %s\nActual: %s",
                    expectedRows.size(), actualRows.size(), expectedRows, actualRows));
        }

        for (int i = 0; i < expectedRows.size(); i++) {
            Object[] expected = expectedRows.get(i);
            Row actual = actualRows.get(i);
            for (int j = 0; j < expected.length; j++) {
                Object expectedVal = expected[j];
                Object actualVal = actual.getValue(j);
                if (expectedVal instanceof AnyOf anyOf) {
                    if (!anyOf.matches(actualVal)) {
                        throw new AssertionError(format("Row %d, column %d: expected %s but got %s", i, j, anyOf, actualVal));
                    }
                }
                else if (!Objects.equals(expectedVal, actualVal)) {
                    // Handle Double comparison with some tolerance
                    if (expectedVal instanceof Double expectedDouble && actualVal instanceof Double actualDouble) {
                        if (Math.abs(expectedDouble - actualDouble) > 0.001) {
                            throw new AssertionError(format("Row %d, column %d: expected %s but got %s", i, j, expectedVal, actualVal));
                        }
                    }
                    else {
                        throw new AssertionError(format("Row %d, column %d: expected %s but got %s", i, j, expectedVal, actualVal));
                    }
                }
            }
        }
    }

    private static void ensureNationTable(MultinodeHive4Environment env)
    {
        env.executeTrinoUpdate("""
                CREATE TABLE IF NOT EXISTS hive.default.nation AS
                SELECT
                    CAST(nationkey AS BIGINT) AS n_nationkey,
                    CAST(name AS VARCHAR(25)) AS n_name,
                    CAST(regionkey AS BIGINT) AS n_regionkey,
                    CAST(comment AS VARCHAR(152)) AS n_comment
                FROM tpch.tiny.nation
                """);
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testStatisticsForUnpartitionedTable(MultinodeHive4Environment env)
    {
        String tableName = "test_stats_unpartitioned_nation";
        ensureNationTable(env);
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);

        try {
            // Create nation table without stats
            env.executeTrinoUpdate(format("""
                    CREATE TABLE hive.default.%s AS
                    SELECT
                        CAST(n_nationkey AS BIGINT) AS n_nationkey,
                        CAST(n_name AS VARCHAR(25)) AS n_name,
                        CAST(n_regionkey AS BIGINT) AS n_regionkey,
                        CAST(n_comment AS VARCHAR(152)) AS n_comment
                    FROM hive.default.nation
                    """, tableName));

            // Drop stats to start fresh
            env.executeTrinoUpdate(format("CALL system.drop_stats('default', '%s')", tableName));

            String showStatsWholeTable = "SHOW STATS FOR hive.default." + tableName;

            // table not analyzed
            assertThat(env.executeTrino(showStatsWholeTable)).containsOnly(
                    row("n_nationkey", 0.0, 0.0, 1.0, null, null, null),
                    row("n_name", 0.0, 0.0, 1.0, null, null, null),
                    row("n_regionkey", 0.0, 0.0, 1.0, null, null, null),
                    row("n_comment", 0.0, 0.0, 1.0, null, null, null),
                    row(null, null, null, null, 0.0, null, null));

            // basic analysis
            env.executeHiveUpdate("ANALYZE TABLE default." + tableName + " COMPUTE STATISTICS");
            env.executeTrinoUpdate("CALL system.flush_metadata_cache()");

            assertThat(env.executeTrino(showStatsWholeTable)).containsOnly(
                    row("n_nationkey", null, null, null, null, null, null),
                    row("n_name", null, null, null, null, null, null),
                    row("n_regionkey", null, null, null, null, null, null),
                    row("n_comment", null, null, null, null, null, null),
                    row(null, null, null, null, 25.0, null, null));

            // column analysis
            env.executeHiveUpdate("ANALYZE TABLE default." + tableName + " COMPUTE STATISTICS FOR COLUMNS");
            env.executeTrinoUpdate("CALL system.flush_metadata_cache()");

            // Note: The exact ndv values may vary slightly between Hive versions
            assertStatsContainsOnly(env, showStatsWholeTable, ImmutableList.of(
                    new Object[] {"n_nationkey", null, AnyOf.anyOf(19., 25.), 0.0, null, "0", "24"},
                    new Object[] {"n_name", 177.0, AnyOf.anyOf(24., 25.), 0.0, null, null, null},
                    new Object[] {"n_regionkey", null, 5.0, 0.0, null, "0", "4"},
                    new Object[] {"n_comment", 1857.0, 25.0, 0.0, null, null, null},
                    new Object[] {null, null, null, null, 25.0, null, null}));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testStatisticsForTablePartitionedByBigint(MultinodeHive4Environment env)
    {
        String tableName = "test_stats_partitioned_bigint";
        ensureNationTable(env);
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);

        try {
            // Create partitioned nation table
            env.executeTrinoUpdate(format("""
                    CREATE TABLE hive.default.%s
                    WITH (partitioned_by = ARRAY['p_regionkey'])
                    AS
                    SELECT
                        CAST(n_nationkey AS BIGINT) AS p_nationkey,
                        CAST(n_name AS VARCHAR(25)) AS p_name,
                        CAST(n_comment AS VARCHAR(152)) AS p_comment,
                        CAST(n_regionkey AS BIGINT) AS p_regionkey
                    FROM hive.default.nation
                    WHERE n_regionkey IN (1, 2, 3)
                    """, tableName));

            // Drop stats
            env.executeTrinoUpdate(format("CALL system.drop_stats('default', '%s')", tableName));

            String showStatsWholeTable = "SHOW STATS FOR hive.default." + tableName;
            String showStatsPartitionOne = "SHOW STATS FOR (SELECT * FROM hive.default." + tableName + " WHERE p_regionkey = 1)";
            String showStatsPartitionTwo = "SHOW STATS FOR (SELECT * FROM hive.default." + tableName + " WHERE p_regionkey = 2)";

            // table not analyzed
            assertThat(env.executeTrino(showStatsWholeTable)).containsOnly(
                    row("p_nationkey", null, null, null, null, null, null),
                    row("p_name", null, null, null, null, null, null),
                    row("p_regionkey", null, 3.0, 0.0, null, "1", "3"),
                    row("p_comment", null, null, null, null, null, null),
                    row(null, null, null, null, null, null, null));

            assertThat(env.executeTrino(showStatsPartitionOne)).containsOnly(
                    row("p_nationkey", null, null, null, null, null, null),
                    row("p_name", null, null, null, null, null, null),
                    row("p_regionkey", null, 1.0, 0.0, null, "1", "1"),
                    row("p_comment", null, null, null, null, null, null),
                    row(null, null, null, null, null, null, null));

            // basic analysis for single partition
            env.executeHiveUpdate("ANALYZE TABLE default." + tableName + " PARTITION (p_regionkey = 1) COMPUTE STATISTICS");
            env.executeTrinoUpdate("CALL system.flush_metadata_cache()");

            assertThat(env.executeTrino(showStatsWholeTable)).containsOnly(
                    row("p_nationkey", null, null, null, null, null, null),
                    row("p_name", null, null, null, null, null, null),
                    row("p_regionkey", null, 3.0, 0.0, null, "1", "3"),
                    row("p_comment", null, null, null, null, null, null),
                    row(null, null, null, null, 15.0, null, null));

            assertThat(env.executeTrino(showStatsPartitionOne)).containsOnly(
                    row("p_nationkey", null, null, null, null, null, null),
                    row("p_name", null, null, null, null, null, null),
                    row("p_regionkey", null, 1.0, 0.0, null, "1", "1"),
                    row("p_comment", null, null, null, null, null, null),
                    row(null, null, null, null, 5.0, null, null));

            assertThat(env.executeTrino(showStatsPartitionTwo)).containsOnly(
                    row("p_nationkey", null, null, null, null, null, null),
                    row("p_name", null, null, null, null, null, null),
                    row("p_regionkey", null, 1.0, 0.0, null, "2", "2"),
                    row("p_comment", null, null, null, null, null, null),
                    row(null, null, null, null, null, null, null));

            // basic analysis for all partitions
            env.executeHiveUpdate("ANALYZE TABLE default." + tableName + " PARTITION (p_regionkey) COMPUTE STATISTICS");
            env.executeTrinoUpdate("CALL system.flush_metadata_cache()");

            assertThat(env.executeTrino(showStatsWholeTable)).containsOnly(
                    row("p_nationkey", null, null, null, null, null, null),
                    row("p_name", null, null, null, null, null, null),
                    row("p_regionkey", null, 3.0, 0.0, null, "1", "3"),
                    row("p_comment", null, null, null, null, null, null),
                    row(null, null, null, null, 15.0, null, null));

            assertThat(env.executeTrino(showStatsPartitionOne)).containsOnly(
                    row("p_nationkey", null, null, null, null, null, null),
                    row("p_name", null, null, null, null, null, null),
                    row("p_regionkey", null, 1.0, 0.0, null, "1", "1"),
                    row("p_comment", null, null, null, null, null, null),
                    row(null, null, null, null, 5.0, null, null));

            assertThat(env.executeTrino(showStatsPartitionTwo)).containsOnly(
                    row("p_nationkey", null, null, null, null, null, null),
                    row("p_name", null, null, null, null, null, null),
                    row("p_regionkey", null, 1.0, 0.0, null, "2", "2"),
                    row("p_comment", null, null, null, null, null, null),
                    row(null, null, null, null, 5.0, null, null));

            // column analysis for single partition
            env.executeHiveUpdate("ANALYZE TABLE default." + tableName + " PARTITION (p_regionkey = 1) COMPUTE STATISTICS FOR COLUMNS");
            env.executeTrinoUpdate("CALL system.flush_metadata_cache()");

            assertThat(env.executeTrino(showStatsWholeTable)).containsOnly(
                    row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                    row("p_name", 114.0, 5.0, 0.0, null, null, null),
                    row("p_regionkey", null, 3.0, 0.0, null, "1", "3"),
                    row("p_comment", 1497.0, 5.0, 0.0, null, null, null),
                    row(null, null, null, null, 15.0, null, null));

            assertThat(env.executeTrino(showStatsPartitionOne)).containsOnly(
                    row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                    row("p_name", 38.0, 5.0, 0.0, null, null, null),
                    row("p_regionkey", null, 1.0, 0.0, null, "1", "1"),
                    row("p_comment", 499.0, 5.0, 0.0, null, null, null),
                    row(null, null, null, null, 5.0, null, null));

            assertThat(env.executeTrino(showStatsPartitionTwo)).containsOnly(
                    row("p_nationkey", null, null, null, null, null, null),
                    row("p_name", null, null, null, null, null, null),
                    row("p_regionkey", null, 1.0, 0.0, null, "2", "2"),
                    row("p_comment", null, null, null, null, null, null),
                    row(null, null, null, null, 5.0, null, null));

            // column analysis for all partitions
            env.executeHiveUpdate("ANALYZE TABLE default." + tableName + " PARTITION (p_regionkey) COMPUTE STATISTICS FOR COLUMNS");
            env.executeTrinoUpdate("CALL system.flush_metadata_cache()");

            assertStatsContainsOnly(env, showStatsPartitionTwo, ImmutableList.of(
                    new Object[] {"p_nationkey", null, AnyOf.anyOf(4., 5.), 0.0, null, "8", "21"},
                    new Object[] {"p_name", 31.0, 5.0, 0.0, null, null, null},
                    new Object[] {"p_regionkey", null, 1.0, 0.0, null, "2", "2"},
                    new Object[] {"p_comment", 351.0, 5.0, 0.0, null, null, null},
                    new Object[] {null, null, null, null, 5.0, null, null}));

            assertThat(env.executeTrino(showStatsPartitionOne)).containsOnly(
                    row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                    row("p_name", 38.0, 5.0, 0.0, null, null, null),
                    row("p_regionkey", null, 1.0, 0.0, null, "1", "1"),
                    row("p_comment", 499.0, 5.0, 0.0, null, null, null),
                    row(null, null, null, null, 5.0, null, null));

            assertThat(env.executeTrino(showStatsWholeTable)).containsOnly(
                    row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                    row("p_name", 109.0, 5.0, 0.0, null, null, null),
                    row("p_regionkey", null, 3.0, 0.0, null, "1", "3"),
                    row("p_comment", 1197.0, 5.0, 0.0, null, null, null),
                    row(null, null, null, null, 15.0, null, null));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testStatisticsForTablePartitionedByVarchar(MultinodeHive4Environment env)
    {
        String tableName = "test_stats_partitioned_varchar";
        ensureNationTable(env);
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);

        try {
            // First create partitions using Hive to get proper partition key values
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
            env.executeHiveUpdate(format("""
                    CREATE TABLE default.%s (
                        p_nationkey BIGINT,
                        p_name VARCHAR(25),
                        p_comment VARCHAR(152)
                    )
                    PARTITIONED BY (p_regionkey STRING)
                    STORED AS ORC
                    """, tableName));

            // Insert data via Trino after table exists
            env.executeTrinoUpdate(format("""
                    INSERT INTO hive.default.%s
                    SELECT
                        CAST(n.n_nationkey AS BIGINT),
                        CAST(n.n_name AS VARCHAR(25)),
                        CAST(n.n_comment AS VARCHAR(152)),
                        r.name
                    FROM hive.default.nation n
                    JOIN tpch.tiny.region r ON n.n_regionkey = r.regionkey
                    WHERE r.name IN ('AMERICA', 'ASIA', 'EUROPE')
                    """, tableName));

            // Drop stats
            env.executeTrinoUpdate(format("CALL system.drop_stats('default', '%s')", tableName));

            String showStatsWholeTable = "SHOW STATS FOR hive.default." + tableName;
            String showStatsPartitionOne = "SHOW STATS FOR (SELECT * FROM hive.default." + tableName + " WHERE p_regionkey = 'AMERICA')";
            String showStatsPartitionTwo = "SHOW STATS FOR (SELECT * FROM hive.default." + tableName + " WHERE p_regionkey = 'ASIA')";

            // table not analyzed
            assertThat(env.executeTrino(showStatsWholeTable)).containsOnly(
                    row("p_nationkey", null, null, null, null, null, null),
                    row("p_name", null, null, null, null, null, null),
                    row("p_regionkey", null, 3.0, 0.0, null, null, null),
                    row("p_comment", null, null, null, null, null, null),
                    row(null, null, null, null, null, null, null));

            assertThat(env.executeTrino(showStatsPartitionOne)).containsOnly(
                    row("p_nationkey", null, null, null, null, null, null),
                    row("p_name", null, null, null, null, null, null),
                    row("p_regionkey", null, 1.0, 0.0, null, null, null),
                    row("p_comment", null, null, null, null, null, null),
                    row(null, null, null, null, null, null, null));

            // basic analysis for single partition
            env.executeHiveUpdate("ANALYZE TABLE default." + tableName + " PARTITION (p_regionkey = 'AMERICA') COMPUTE STATISTICS");
            env.executeTrinoUpdate("CALL system.flush_metadata_cache()");

            assertThat(env.executeTrino(showStatsWholeTable)).containsOnly(
                    row("p_nationkey", null, null, null, null, null, null),
                    row("p_name", null, null, null, null, null, null),
                    row("p_regionkey", 85.0, 3.0, 0.0, null, null, null),
                    row("p_comment", null, null, null, null, null, null),
                    row(null, null, null, null, 15.0, null, null));

            assertThat(env.executeTrino(showStatsPartitionOne)).containsOnly(
                    row("p_nationkey", null, null, null, null, null, null),
                    row("p_name", null, null, null, null, null, null),
                    row("p_regionkey", 35.0, 1.0, 0.0, null, null, null),
                    row("p_comment", null, null, null, null, null, null),
                    row(null, null, null, null, 5.0, null, null));

            assertThat(env.executeTrino(showStatsPartitionTwo)).containsOnly(
                    row("p_nationkey", null, null, null, null, null, null),
                    row("p_name", null, null, null, null, null, null),
                    row("p_regionkey", null, 1.0, 0.0, null, null, null),
                    row("p_comment", null, null, null, null, null, null),
                    row(null, null, null, null, null, null, null));

            // basic analysis for all partitions
            env.executeHiveUpdate("ANALYZE TABLE default." + tableName + " PARTITION (p_regionkey) COMPUTE STATISTICS");
            env.executeTrinoUpdate("CALL system.flush_metadata_cache()");

            assertThat(env.executeTrino(showStatsWholeTable)).containsOnly(
                    row("p_nationkey", null, null, null, null, null, null),
                    row("p_name", null, null, null, null, null, null),
                    row("p_regionkey", 85.0, 3.0, 0.0, null, null, null),
                    row("p_comment", null, null, null, null, null, null),
                    row(null, null, null, null, 15.0, null, null));

            assertThat(env.executeTrino(showStatsPartitionOne)).containsOnly(
                    row("p_nationkey", null, null, null, null, null, null),
                    row("p_name", null, null, null, null, null, null),
                    row("p_regionkey", 35.0, 1.0, 0.0, null, null, null),
                    row("p_comment", null, null, null, null, null, null),
                    row(null, null, null, null, 5.0, null, null));

            assertThat(env.executeTrino(showStatsPartitionTwo)).containsOnly(
                    row("p_nationkey", null, null, null, null, null, null),
                    row("p_name", null, null, null, null, null, null),
                    row("p_regionkey", 20.0, 1.0, 0.0, null, null, null),
                    row("p_comment", null, null, null, null, null, null),
                    row(null, null, null, null, 5.0, null, null));

            // column analysis for single partition
            env.executeHiveUpdate("ANALYZE TABLE default." + tableName + " PARTITION (p_regionkey = 'AMERICA') COMPUTE STATISTICS FOR COLUMNS");
            env.executeTrinoUpdate("CALL system.flush_metadata_cache()");

            assertThat(env.executeTrino(showStatsWholeTable)).containsOnly(
                    row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                    row("p_name", 114.0, 5.0, 0.0, null, null, null),
                    row("p_regionkey", 85.0, 3.0, 0.0, null, null, null),
                    row("p_comment", 1497.0, 5.0, 0.0, null, null, null),
                    row(null, null, null, null, 15.0, null, null));

            assertThat(env.executeTrino(showStatsPartitionOne)).containsOnly(
                    row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                    row("p_name", 38.0, 5.0, 0.0, null, null, null),
                    row("p_regionkey", 35.0, 1.0, 0.0, null, null, null),
                    row("p_comment", 499.0, 5.0, 0.0, null, null, null),
                    row(null, null, null, null, 5.0, null, null));

            assertThat(env.executeTrino(showStatsPartitionTwo)).containsOnly(
                    row("p_nationkey", null, null, null, null, null, null),
                    row("p_name", null, null, null, null, null, null),
                    row("p_regionkey", 20.0, 1.0, 0.0, null, null, null),
                    row("p_comment", null, null, null, null, null, null),
                    row(null, null, null, null, 5.0, null, null));

            // column analysis for all partitions
            env.executeHiveUpdate("ANALYZE TABLE default." + tableName + " PARTITION (p_regionkey) COMPUTE STATISTICS FOR COLUMNS");
            env.executeTrinoUpdate("CALL system.flush_metadata_cache()");

            assertThat(env.executeTrino(showStatsWholeTable)).containsOnly(
                    row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                    row("p_name", 109.0, 5.0, 0.0, null, null, null),
                    row("p_regionkey", 85.0, 3.0, 0.0, null, null, null),
                    row("p_comment", 1197.0, 5.0, 0.0, null, null, null),
                    row(null, null, null, null, 15.0, null, null));

            assertThat(env.executeTrino(showStatsPartitionOne)).containsOnly(
                    row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                    row("p_name", 38.0, 5.0, 0.0, null, null, null),
                    row("p_regionkey", 35.0, 1.0, 0.0, null, null, null),
                    row("p_comment", 499.0, 5.0, 0.0, null, null, null),
                    row(null, null, null, null, 5.0, null, null));

            assertStatsContainsOnly(env, showStatsPartitionTwo, ImmutableList.of(
                    new Object[] {"p_nationkey", null, AnyOf.anyOf(4., 5.), 0.0, null, "8", "21"},
                    new Object[] {"p_name", 31.0, 5.0, 0.0, null, null, null},
                    new Object[] {"p_regionkey", 20.0, 1.0, 0.0, null, null, null},
                    new Object[] {"p_comment", 351.0, 5.0, 0.0, null, null, null},
                    new Object[] {null, null, null, null, 5.0, null, null}));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testStatisticsForAllDataTypes(MultinodeHive4Environment env)
    {
        String tableName = "test_stats_all_types";
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);

        try {
            env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s (%s)", tableName, ALL_TYPES_TABLE_DDL));
            env.executeTrinoUpdate(format("INSERT INTO hive.default.%s VALUES %s, %s",
                    tableName, ALL_TYPES_DATA_ROW_1, ALL_TYPES_DATA_ROW_2));

            // Drop stats
            env.executeTrinoUpdate(format("CALL system.drop_stats('default', '%s')", tableName));

            env.executeHiveUpdate("ANALYZE TABLE default." + tableName + " COMPUTE STATISTICS");

            assertThat(env.executeTrino("SHOW STATS FOR hive.default." + tableName)).containsOnly(
                    row("c_tinyint", null, null, null, null, null, null),
                    row("c_smallint", null, null, null, null, null, null),
                    row("c_int", null, null, null, null, null, null),
                    row("c_bigint", null, null, null, null, null, null),
                    row("c_float", null, null, null, null, null, null),
                    row("c_double", null, null, null, null, null, null),
                    row("c_decimal", null, null, null, null, null, null),
                    row("c_decimal_w_params", null, null, null, null, null, null),
                    row("c_timestamp", null, null, null, null, null, null),
                    row("c_date", null, null, null, null, null, null),
                    row("c_string", null, null, null, null, null, null),
                    row("c_varchar", null, null, null, null, null, null),
                    row("c_char", null, null, null, null, null, null),
                    row("c_boolean", null, null, null, null, null, null),
                    row("c_binary", null, null, null, null, null, null),
                    row(null, null, null, null, 2.0, null, null));

            env.executeHiveUpdate("ANALYZE TABLE default." + tableName + " COMPUTE STATISTICS FOR COLUMNS");
            env.executeTrinoUpdate("CALL system.flush_metadata_cache()");

            assertThat(env.executeTrino("SHOW STATS FOR hive.default." + tableName)).containsOnly(
                    row("c_tinyint", null, 2.0, 0.0, null, "121", "127"),
                    row("c_smallint", null, 2.0, 0.0, null, "32761", "32767"),
                    row("c_int", null, 2.0, 0.0, null, "2147483641", "2147483647"),
                    row("c_bigint", null, 1.0, 0.0, null, "9223372036854775807", "9223372036854775807"),
                    row("c_float", null, 2.0, 0.0, null, "123.341", "123.345"),
                    row("c_double", null, 2.0, 0.0, null, "234.561", "235.567"),
                    row("c_decimal", null, 2.0, 0.0, null, "345.0", "346.0"),
                    row("c_decimal_w_params", null, 2.0, 0.0, null, "345.671", "345.678"),
                    row("c_timestamp", null, 2.0, 0.0, null, null, null),
                    row("c_date", null, 2.0, 0.0, null, "2015-05-09", "2015-06-10"),
                    row("c_string", 22.0, 2.0, 0.0, null, null, null),
                    row("c_varchar", 20.0, 2.0, 0.0, null, null, null),
                    row("c_char", 12.0, 2.0, 0.0, null, null, null),
                    row("c_boolean", null, 2.0, 0.0, null, null, null),
                    row("c_binary", 23.0, null, 0.0, null, null, null),
                    row(null, null, null, null, 2.0, null, null));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testStatisticsForAllDataTypesNoData(MultinodeHive4Environment env)
    {
        String tableName = "test_stats_all_types_empty";
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);

        try {
            env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s (%s)", tableName, ALL_TYPES_TABLE_DDL));

            env.executeHiveUpdate("ANALYZE TABLE default." + tableName + " COMPUTE STATISTICS");

            assertThat(env.executeTrino("SHOW STATS FOR hive.default." + tableName)).containsOnly(
                    row("c_tinyint", 0.0, 0.0, 1.0, null, null, null),
                    row("c_smallint", 0.0, 0.0, 1.0, null, null, null),
                    row("c_int", 0.0, 0.0, 1.0, null, null, null),
                    row("c_bigint", 0.0, 0.0, 1.0, null, null, null),
                    row("c_float", 0.0, 0.0, 1.0, null, null, null),
                    row("c_double", 0.0, 0.0, 1.0, null, null, null),
                    row("c_decimal", 0.0, 0.0, 1.0, null, null, null),
                    row("c_decimal_w_params", 0.0, 0.0, 1.0, null, null, null),
                    row("c_timestamp", 0.0, 0.0, 1.0, null, null, null),
                    row("c_date", 0.0, 0.0, 1.0, null, null, null),
                    row("c_string", 0.0, 0.0, 1.0, null, null, null),
                    row("c_varchar", 0.0, 0.0, 1.0, null, null, null),
                    row("c_char", 0.0, 0.0, 1.0, null, null, null),
                    row("c_boolean", 0.0, 0.0, 1.0, null, null, null),
                    row("c_binary", 0.0, 0.0, 1.0, null, null, null),
                    row(null, null, null, null, 0.0, null, null));

            env.executeHiveUpdate("ANALYZE TABLE default." + tableName + " COMPUTE STATISTICS FOR COLUMNS");
            env.executeTrinoUpdate("CALL system.flush_metadata_cache()");

            assertThat(env.executeTrino("SHOW STATS FOR hive.default." + tableName)).containsOnly(
                    row("c_tinyint", 0.0, 0.0, 1.0, null, null, null),
                    row("c_smallint", 0.0, 0.0, 1.0, null, null, null),
                    row("c_int", 0.0, 0.0, 1.0, null, null, null),
                    row("c_bigint", 0.0, 0.0, 1.0, null, null, null),
                    row("c_float", 0.0, 0.0, 1.0, null, null, null),
                    row("c_double", 0.0, 0.0, 1.0, null, null, null),
                    row("c_decimal", 0.0, 0.0, 1.0, null, null, null),
                    row("c_decimal_w_params", 0.0, 0.0, 1.0, null, null, null),
                    row("c_timestamp", 0.0, 0.0, 1.0, null, null, null),
                    row("c_date", 0.0, 0.0, 1.0, null, null, null),
                    row("c_string", 0.0, 0.0, 1.0, null, null, null),
                    row("c_varchar", 0.0, 0.0, 1.0, null, null, null),
                    row("c_char", 0.0, 0.0, 1.0, null, null, null),
                    row("c_boolean", 0.0, 0.0, 1.0, null, null, null),
                    row("c_binary", 0.0, 0.0, 1.0, null, null, null),
                    row(null, null, null, null, 0.0, null, null));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testStatisticsForAllDataTypesOnlyNulls(MultinodeHive4Environment env)
    {
        String tableName = "test_stats_all_types_nulls";
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);

        try {
            env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s (%s)", tableName, ALL_TYPES_TABLE_DDL));
            env.executeHiveUpdate("INSERT INTO TABLE default." + tableName + " VALUES(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)");

            env.executeHiveUpdate("ANALYZE TABLE default." + tableName + " COMPUTE STATISTICS");

            assertThat(env.executeTrino("SHOW STATS FOR hive.default." + tableName)).containsOnly(
                    row("c_tinyint", null, null, null, null, null, null),
                    row("c_smallint", null, null, null, null, null, null),
                    row("c_int", null, null, null, null, null, null),
                    row("c_bigint", null, null, null, null, null, null),
                    row("c_float", null, null, null, null, null, null),
                    row("c_double", null, null, null, null, null, null),
                    row("c_decimal", null, null, null, null, null, null),
                    row("c_decimal_w_params", null, null, null, null, null, null),
                    row("c_timestamp", null, null, null, null, null, null),
                    row("c_date", null, null, null, null, null, null),
                    row("c_string", null, null, null, null, null, null),
                    row("c_varchar", null, null, null, null, null, null),
                    row("c_char", null, null, null, null, null, null),
                    row("c_boolean", null, null, null, null, null, null),
                    row("c_binary", null, null, null, null, null, null),
                    row(null, null, null, null, 1.0, null, null));

            env.executeHiveUpdate("ANALYZE TABLE default." + tableName + " COMPUTE STATISTICS FOR COLUMNS");
            env.executeTrinoUpdate("CALL system.flush_metadata_cache()");

            assertThat(env.executeTrino("SHOW STATS FOR hive.default." + tableName)).containsOnly(
                    row("c_tinyint", 0.0, 0.0, 1.0, null, null, null),
                    row("c_smallint", 0.0, 0.0, 1.0, null, null, null),
                    row("c_int", 0.0, 0.0, 1.0, null, null, null),
                    row("c_bigint", 0.0, 0.0, 1.0, null, null, null),
                    row("c_float", 0.0, 0.0, 1.0, null, null, null),
                    row("c_double", 0.0, 0.0, 1.0, null, null, null),
                    row("c_decimal", 0.0, 0.0, 1.0, null, null, null),
                    row("c_decimal_w_params", 0.0, 0.0, 1.0, null, null, null),
                    row("c_timestamp", 0.0, 0.0, 1.0, null, null, null),
                    row("c_date", 0.0, 0.0, 1.0, null, null, null),
                    row("c_string", 0.0, 0.0, 1.0, null, null, null),
                    row("c_varchar", 0.0, 0.0, 1.0, null, null, null),
                    row("c_char", 0.0, 0.0, 1.0, null, null, null),
                    row("c_boolean", 0.0, 0.0, 1.0, null, null, null),
                    row("c_binary", 0.0, null, 1.0, null, null, null),
                    row(null, null, null, null, 1.0, null, null));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testStatisticsForSkewedTable(MultinodeHive4Environment env)
    {
        String tableName = "test_hive_skewed_table_statistics";
        env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        env.executeHiveUpdate("CREATE TABLE default." + tableName + " (c_string STRING, c_int INT) SKEWED BY (c_string) ON ('c1')");
        try {
            env.executeHiveUpdate("INSERT INTO TABLE default." + tableName + " VALUES ('c1', 1), ('c1', 2)");
            assertSkewedTableStats(env, tableName, true);

            env.executeHiveUpdate("ANALYZE TABLE default." + tableName + " COMPUTE STATISTICS");
            env.executeTrinoUpdate("CALL system.flush_metadata_cache()");
            assertSkewedTableStats(env, tableName, true);

            env.executeHiveUpdate("ANALYZE TABLE default." + tableName + " COMPUTE STATISTICS FOR COLUMNS");
            env.executeTrinoUpdate("CALL system.flush_metadata_cache()");
            assertSkewedTableStats(env, tableName, false);
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testAnalyzesForSkewedTable(MultinodeHive4Environment env)
    {
        String tableName = "test_analyze_skewed_table";
        env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        env.executeHiveUpdate("CREATE TABLE default." + tableName + " (c_string STRING, c_int INT) SKEWED BY (c_string) ON ('c1')");
        try {
            env.executeHiveUpdate("INSERT INTO TABLE default." + tableName + " VALUES ('c1', 1), ('c1', 2)");
            assertSkewedTableStats(env, tableName, true);

            assertThat(env.executeTrino("ANALYZE hive.default." + tableName)).containsExactlyInOrder(row(2L));
            assertSkewedTableStats(env, tableName, false);
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testAnalyzeForUnpartitionedTable(MultinodeHive4Environment env)
    {
        String tableName = "test_analyze_unpartitioned";
        ensureNationTable(env);
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);

        try {
            env.executeTrinoUpdate(format("""
                    CREATE TABLE hive.default.%s AS
                    SELECT
                        CAST(n_nationkey AS BIGINT) AS n_nationkey,
                        CAST(n_name AS VARCHAR(25)) AS n_name,
                        CAST(n_regionkey AS BIGINT) AS n_regionkey,
                        CAST(n_comment AS VARCHAR(152)) AS n_comment
                    FROM hive.default.nation
                    """, tableName));

            // Drop stats
            env.executeTrinoUpdate(format("CALL system.drop_stats('default', '%s')", tableName));

            String showStatsWholeTable = "SHOW STATS FOR hive.default." + tableName;

            // table not analyzed
            assertThat(env.executeTrino(showStatsWholeTable)).containsOnly(
                    row("n_nationkey", 0.0, 0.0, 1.0, null, null, null),
                    row("n_name", 0.0, 0.0, 1.0, null, null, null),
                    row("n_regionkey", 0.0, 0.0, 1.0, null, null, null),
                    row("n_comment", 0.0, 0.0, 1.0, null, null, null),
                    row(null, null, null, null, 0.0, null, null));

            assertThat(env.executeTrino("ANALYZE hive.default." + tableName)).containsExactlyInOrder(row(25L));

            assertThat(env.executeTrino(showStatsWholeTable)).containsOnly(
                    row("n_nationkey", null, 25.0, 0.0, null, "0", "24"),
                    row("n_name", 177.0, 25.0, 0.0, null, null, null),
                    row("n_regionkey", null, 5.0, 0.0, null, "0", "4"),
                    row("n_comment", 1857.0, 25.0, 0.0, null, null, null),
                    row(null, null, null, null, 25.0, null, null));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    void testAnalyzeForTablePartitionedByBigint(MultinodeHive4Environment env)
    {
        String tableName = "test_analyze_partitioned_bigint";
        ensureNationTable(env);
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);

        try {
            env.executeTrinoUpdate(format("""
                    CREATE TABLE hive.default.%s
                    WITH (partitioned_by = ARRAY['p_regionkey'])
                    AS
                    SELECT
                        CAST(n_nationkey AS BIGINT) AS p_nationkey,
                        CAST(n_name AS VARCHAR(25)) AS p_name,
                        CAST(n_comment AS VARCHAR(152)) AS p_comment,
                        CAST(n_regionkey AS BIGINT) AS p_regionkey
                    FROM hive.default.nation
                    WHERE n_regionkey IN (1, 2, 3)
                    """, tableName));

            // Drop stats
            env.executeTrinoUpdate(format("CALL system.drop_stats('default', '%s')", tableName));

            String showStatsWholeTable = "SHOW STATS FOR hive.default." + tableName;
            String showStatsPartitionOne = "SHOW STATS FOR (SELECT * FROM hive.default." + tableName + " WHERE p_regionkey = 1)";
            String showStatsPartitionTwo = "SHOW STATS FOR (SELECT * FROM hive.default." + tableName + " WHERE p_regionkey = 2)";

            // table not analyzed
            assertThat(env.executeTrino(showStatsWholeTable)).containsOnly(
                    row("p_nationkey", null, null, null, null, null, null),
                    row("p_name", null, null, null, null, null, null),
                    row("p_regionkey", null, 3.0, 0.0, null, "1", "3"),
                    row("p_comment", null, null, null, null, null, null),
                    row(null, null, null, null, null, null, null));

            assertThat(env.executeTrino(showStatsPartitionOne)).containsOnly(
                    row("p_nationkey", null, null, null, null, null, null),
                    row("p_name", null, null, null, null, null, null),
                    row("p_regionkey", null, 1.0, 0.0, null, "1", "1"),
                    row("p_comment", null, null, null, null, null, null),
                    row(null, null, null, null, null, null, null));

            // analyze for single partition
            assertThat(env.executeTrino("ANALYZE hive.default." + tableName + " WITH (partitions = ARRAY[ARRAY['1']])")).containsExactlyInOrder(row(5L));

            assertThat(env.executeTrino(showStatsWholeTable)).containsOnly(
                    row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                    row("p_name", 114.0, 5.0, 0.0, null, null, null),
                    row("p_regionkey", null, 3.0, 0.0, null, "1", "3"),
                    row("p_comment", 1497.0, 5.0, 0.0, null, null, null),
                    row(null, null, null, null, 15.0, null, null));

            assertThat(env.executeTrino(showStatsPartitionOne)).containsOnly(
                    row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                    row("p_name", 38.0, 5.0, 0.0, null, null, null),
                    row("p_regionkey", null, 1.0, 0.0, null, "1", "1"),
                    row("p_comment", 499.0, 5.0, 0.0, null, null, null),
                    row(null, null, null, null, 5.0, null, null));

            assertThat(env.executeTrino(showStatsPartitionTwo)).containsOnly(
                    row("p_nationkey", null, null, null, null, null, null),
                    row("p_name", null, null, null, null, null, null),
                    row("p_regionkey", null, 1.0, 0.0, null, "2", "2"),
                    row("p_comment", null, null, null, null, null, null),
                    row(null, null, null, null, null, null, null));

            // analyze for all partitions
            assertThat(env.executeTrino("ANALYZE hive.default." + tableName)).containsExactlyInOrder(row(15L));

            assertThat(env.executeTrino(showStatsWholeTable)).containsOnly(
                    row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                    row("p_name", 109.0, 5.0, 0.0, null, null, null),
                    row("p_regionkey", null, 3.0, 0.0, null, "1", "3"),
                    row("p_comment", 1197.0, 5.0, 0.0, null, null, null),
                    row(null, null, null, null, 15.0, null, null));

            assertThat(env.executeTrino(showStatsPartitionOne)).containsOnly(
                    row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                    row("p_name", 38.0, 5.0, 0.0, null, null, null),
                    row("p_regionkey", null, 1.0, 0.0, null, "1", "1"),
                    row("p_comment", 499.0, 5.0, 0.0, null, null, null),
                    row(null, null, null, null, 5.0, null, null));

            assertThat(env.executeTrino(showStatsPartitionTwo)).containsOnly(
                    row("p_nationkey", null, 5.0, 0.0, null, "8", "21"),
                    row("p_name", 31.0, 5.0, 0.0, null, null, null),
                    row("p_regionkey", null, 1.0, 0.0, null, "2", "2"),
                    row("p_comment", 351.0, 5.0, 0.0, null, null, null),
                    row(null, null, null, null, 5.0, null, null));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    void testAnalyzeForTablePartitionedByVarchar(MultinodeHive4Environment env)
    {
        String tableName = "test_analyze_partitioned_varchar";
        ensureNationTable(env);
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);

        try {
            // Create partitioned table via Hive first
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
            env.executeHiveUpdate(format("""
                    CREATE TABLE default.%s (
                        p_nationkey BIGINT,
                        p_name VARCHAR(25),
                        p_comment VARCHAR(152)
                    )
                    PARTITIONED BY (p_regionkey STRING)
                    STORED AS ORC
                    """, tableName));

            // Insert data via Trino
            env.executeTrinoUpdate(format("""
                    INSERT INTO hive.default.%s
                    SELECT
                        CAST(n.n_nationkey AS BIGINT),
                        CAST(n.n_name AS VARCHAR(25)),
                        CAST(n.n_comment AS VARCHAR(152)),
                        r.name
                    FROM hive.default.nation n
                    JOIN tpch.tiny.region r ON n.n_regionkey = r.regionkey
                    WHERE r.name IN ('AMERICA', 'ASIA', 'EUROPE')
                    """, tableName));

            // Drop stats
            env.executeTrinoUpdate(format("CALL system.drop_stats('default', '%s')", tableName));

            String showStatsWholeTable = "SHOW STATS FOR hive.default." + tableName;
            String showStatsPartitionOne = "SHOW STATS FOR (SELECT * FROM hive.default." + tableName + " WHERE p_regionkey = 'AMERICA')";
            String showStatsPartitionTwo = "SHOW STATS FOR (SELECT * FROM hive.default." + tableName + " WHERE p_regionkey = 'ASIA')";

            // table not analyzed
            assertThat(env.executeTrino(showStatsWholeTable)).containsOnly(
                    row("p_nationkey", null, null, null, null, null, null),
                    row("p_name", null, null, null, null, null, null),
                    row("p_regionkey", null, 3.0, 0.0, null, null, null),
                    row("p_comment", null, null, null, null, null, null),
                    row(null, null, null, null, null, null, null));

            assertThat(env.executeTrino(showStatsPartitionOne)).containsOnly(
                    row("p_nationkey", null, null, null, null, null, null),
                    row("p_name", null, null, null, null, null, null),
                    row("p_regionkey", null, 1.0, 0.0, null, null, null),
                    row("p_comment", null, null, null, null, null, null),
                    row(null, null, null, null, null, null, null));

            // analyze for single partition
            assertThat(env.executeTrino("ANALYZE hive.default." + tableName + " WITH (partitions = ARRAY[ARRAY['AMERICA']])")).containsExactlyInOrder(row(5L));

            assertThat(env.executeTrino(showStatsWholeTable)).containsOnly(
                    row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                    row("p_name", 114.0, 5.0, 0.0, null, null, null),
                    row("p_regionkey", 85.0, 3.0, 0.0, null, null, null),
                    row("p_comment", 1497.0, 5.0, 0.0, null, null, null),
                    row(null, null, null, null, 15.0, null, null));

            assertThat(env.executeTrino(showStatsPartitionOne)).containsOnly(
                    row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                    row("p_name", 38.0, 5.0, 0.0, null, null, null),
                    row("p_regionkey", 35.0, 1.0, 0.0, null, null, null),
                    row("p_comment", 499.0, 5.0, 0.0, null, null, null),
                    row(null, null, null, null, 5.0, null, null));

            assertThat(env.executeTrino(showStatsPartitionTwo)).containsOnly(
                    row("p_nationkey", null, null, null, null, null, null),
                    row("p_name", null, null, null, null, null, null),
                    row("p_regionkey", null, 1.0, 0.0, null, null, null),
                    row("p_comment", null, null, null, null, null, null),
                    row(null, null, null, null, null, null, null));

            // column analysis for all partitions
            assertThat(env.executeTrino("ANALYZE hive.default." + tableName)).containsExactlyInOrder(row(15L));

            assertThat(env.executeTrino(showStatsWholeTable)).containsOnly(
                    row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                    row("p_name", 109.0, 5.0, 0.0, null, null, null),
                    row("p_regionkey", 85.0, 3.0, 0.0, null, null, null),
                    row("p_comment", 1197.0, 5.0, 0.0, null, null, null),
                    row(null, null, null, null, 15.0, null, null));

            assertThat(env.executeTrino(showStatsPartitionOne)).containsOnly(
                    row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                    row("p_name", 38.0, 5.0, 0.0, null, null, null),
                    row("p_regionkey", 35.0, 1.0, 0.0, null, null, null),
                    row("p_comment", 499.0, 5.0, 0.0, null, null, null),
                    row(null, null, null, null, 5.0, null, null));

            assertThat(env.executeTrino(showStatsPartitionTwo)).containsOnly(
                    row("p_nationkey", null, 5.0, 0.0, null, "8", "21"),
                    row("p_name", 31.0, 5.0, 0.0, null, null, null),
                    row("p_regionkey", 20.0, 1.0, 0.0, null, null, null),
                    row("p_comment", 351.0, 5.0, 0.0, null, null, null),
                    row(null, null, null, null, 5.0, null, null));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    void testAnalyzeForAllDataTypes(MultinodeHive4Environment env)
    {
        String tableName = "test_analyze_all_types";
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);

        try {
            env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s (%s)", tableName, ALL_TYPES_TABLE_DDL));
            env.executeTrinoUpdate(format("INSERT INTO hive.default.%s VALUES %s, %s",
                    tableName, ALL_TYPES_DATA_ROW_1, ALL_TYPES_DATA_ROW_2));

            // Drop stats
            env.executeTrinoUpdate(format("CALL system.drop_stats('default', '%s')", tableName));

            assertThat(env.executeTrino("SHOW STATS FOR hive.default." + tableName)).containsOnly(
                    row("c_tinyint", 0.0, 0.0, 1.0, null, null, null),
                    row("c_smallint", 0.0, 0.0, 1.0, null, null, null),
                    row("c_int", 0.0, 0.0, 1.0, null, null, null),
                    row("c_bigint", 0.0, 0.0, 1.0, null, null, null),
                    row("c_float", 0.0, 0.0, 1.0, null, null, null),
                    row("c_double", 0.0, 0.0, 1.0, null, null, null),
                    row("c_decimal", 0.0, 0.0, 1.0, null, null, null),
                    row("c_decimal_w_params", 0.0, 0.0, 1.0, null, null, null),
                    row("c_timestamp", 0.0, 0.0, 1.0, null, null, null),
                    row("c_date", 0.0, 0.0, 1.0, null, null, null),
                    row("c_string", 0.0, 0.0, 1.0, null, null, null),
                    row("c_varchar", 0.0, 0.0, 1.0, null, null, null),
                    row("c_char", 0.0, 0.0, 1.0, null, null, null),
                    row("c_boolean", 0.0, 0.0, 1.0, null, null, null),
                    row("c_binary", 0.0, 0.0, 1.0, null, null, null),
                    row(null, null, null, null, 0.0, null, null));

            assertThat(env.executeTrino("ANALYZE hive.default." + tableName)).containsExactlyInOrder(row(2L));

            assertThat(env.executeTrino("SHOW STATS FOR hive.default." + tableName)).containsOnly(
                    row("c_tinyint", null, 2.0, 0.0, null, "121", "127"),
                    row("c_smallint", null, 2.0, 0.0, null, "32761", "32767"),
                    row("c_int", null, 2.0, 0.0, null, "2147483641", "2147483647"),
                    row("c_bigint", null, 1.0, 0.0, null, "9223372036854775807", "9223372036854775807"),
                    row("c_float", null, 2.0, 0.0, null, "123.341", "123.345"),
                    row("c_double", null, 2.0, 0.0, null, "234.561", "235.567"),
                    row("c_decimal", null, 2.0, 0.0, null, "345.0", "346.0"),
                    row("c_decimal_w_params", null, 2.0, 0.0, null, "345.671", "345.678"),
                    row("c_timestamp", null, 2.0, 0.0, null, null, null),
                    row("c_date", null, 2.0, 0.0, null, "2015-05-09", "2015-06-10"),
                    row("c_string", 22.0, 2.0, 0.0, null, null, null),
                    row("c_varchar", 20.0, 2.0, 0.0, null, null, null),
                    row("c_char", 12.0, 2.0, 0.0, null, null, null),
                    row("c_boolean", null, 2.0, 0.0, null, null, null),
                    row("c_binary", 23.0, null, 0.0, null, null, null),
                    row(null, null, null, null, 2.0, null, null));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    void testAnalyzeForAllDataTypesNoData(MultinodeHive4Environment env)
    {
        String tableName = "test_analyze_all_types_empty";
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);

        try {
            env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s (%s)", tableName, ALL_TYPES_TABLE_DDL));

            assertThat(env.executeTrino("SHOW STATS FOR hive.default." + tableName)).containsOnly(
                    row("c_tinyint", 0.0, 0.0, 1.0, null, null, null),
                    row("c_smallint", 0.0, 0.0, 1.0, null, null, null),
                    row("c_int", 0.0, 0.0, 1.0, null, null, null),
                    row("c_bigint", 0.0, 0.0, 1.0, null, null, null),
                    row("c_float", 0.0, 0.0, 1.0, null, null, null),
                    row("c_double", 0.0, 0.0, 1.0, null, null, null),
                    row("c_decimal", 0.0, 0.0, 1.0, null, null, null),
                    row("c_decimal_w_params", 0.0, 0.0, 1.0, null, null, null),
                    row("c_timestamp", 0.0, 0.0, 1.0, null, null, null),
                    row("c_date", 0.0, 0.0, 1.0, null, null, null),
                    row("c_string", 0.0, 0.0, 1.0, null, null, null),
                    row("c_varchar", 0.0, 0.0, 1.0, null, null, null),
                    row("c_char", 0.0, 0.0, 1.0, null, null, null),
                    row("c_boolean", 0.0, 0.0, 1.0, null, null, null),
                    row("c_binary", 0.0, 0.0, 1.0, null, null, null),
                    row(null, null, null, null, 0.0, null, null));

            assertThat(env.executeTrino("ANALYZE hive.default." + tableName)).containsExactlyInOrder(row(0L));

            assertThat(env.executeTrino("SHOW STATS FOR hive.default." + tableName)).containsOnly(
                    row("c_tinyint", 0.0, 0.0, 1.0, null, null, null),
                    row("c_smallint", 0.0, 0.0, 1.0, null, null, null),
                    row("c_int", 0.0, 0.0, 1.0, null, null, null),
                    row("c_bigint", 0.0, 0.0, 1.0, null, null, null),
                    row("c_float", 0.0, 0.0, 1.0, null, null, null),
                    row("c_double", 0.0, 0.0, 1.0, null, null, null),
                    row("c_decimal", 0.0, 0.0, 1.0, null, null, null),
                    row("c_decimal_w_params", 0.0, 0.0, 1.0, null, null, null),
                    row("c_timestamp", 0.0, 0.0, 1.0, null, null, null),
                    row("c_date", 0.0, 0.0, 1.0, null, null, null),
                    row("c_string", 0.0, 0.0, 1.0, null, null, null),
                    row("c_varchar", 0.0, 0.0, 1.0, null, null, null),
                    row("c_char", 0.0, 0.0, 1.0, null, null, null),
                    row("c_boolean", 0.0, 0.0, 1.0, null, null, null),
                    row("c_binary", 0.0, 0.0, 1.0, null, null, null),
                    row(null, null, null, null, 0.0, null, null));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testAnalyzeForAllDataTypesOnlyNulls(MultinodeHive4Environment env)
    {
        String tableName = "test_analyze_all_types_nulls";
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);

        try {
            env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s (%s)", tableName, ALL_TYPES_TABLE_DDL));

            // insert from Hive to prevent Trino collecting statistics on insert
            env.executeHiveUpdate("INSERT INTO TABLE default." + tableName + " VALUES(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)");

            assertThat(env.executeTrino("SHOW STATS FOR hive.default." + tableName)).containsOnly(
                    row("c_tinyint", null, null, null, null, null, null),
                    row("c_smallint", null, null, null, null, null, null),
                    row("c_int", null, null, null, null, null, null),
                    row("c_bigint", null, null, null, null, null, null),
                    row("c_float", null, null, null, null, null, null),
                    row("c_double", null, null, null, null, null, null),
                    row("c_decimal", null, null, null, null, null, null),
                    row("c_decimal_w_params", null, null, null, null, null, null),
                    row("c_timestamp", null, null, null, null, null, null),
                    row("c_date", null, null, null, null, null, null),
                    row("c_string", null, null, null, null, null, null),
                    row("c_varchar", null, null, null, null, null, null),
                    row("c_char", null, null, null, null, null, null),
                    row("c_boolean", null, null, null, null, null, null),
                    row("c_binary", null, null, null, null, null, null),
                    row(null, null, null, null, 1.0, null, null));

            assertThat(env.executeTrino("ANALYZE hive.default." + tableName)).containsExactlyInOrder(row(1L));

            assertThat(env.executeTrino("SHOW STATS FOR hive.default." + tableName)).containsOnly(
                    row("c_tinyint", 0.0, 0.0, 1.0, null, null, null),
                    row("c_smallint", 0.0, 0.0, 1.0, null, null, null),
                    row("c_int", 0.0, 0.0, 1.0, null, null, null),
                    row("c_bigint", 0.0, 0.0, 1.0, null, null, null),
                    row("c_float", 0.0, 0.0, 1.0, null, null, null),
                    row("c_double", 0.0, 0.0, 1.0, null, null, null),
                    row("c_decimal", 0.0, 0.0, 1.0, null, null, null),
                    row("c_decimal_w_params", 0.0, 0.0, 1.0, null, null, null),
                    row("c_timestamp", 0.0, 0.0, 1.0, null, null, null),
                    row("c_date", 0.0, 0.0, 1.0, null, null, null),
                    row("c_string", 0.0, 0.0, 1.0, null, null, null),
                    row("c_varchar", 0.0, 0.0, 1.0, null, null, null),
                    row("c_char", 0.0, 0.0, 1.0, null, null, null),
                    row("c_boolean", 0.0, 0.0, 1.0, null, null, null),
                    row("c_binary", 0.0, null, 1.0, null, null, null),
                    row(null, null, null, null, 1.0, null, null));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    void testComputeTableStatisticsOnCreateTable(MultinodeHive4Environment env)
    {
        String tableName = "test_ctas_stats";
        String emptyTableName = "test_ctas_stats_empty";
        String allNullTableName = "test_ctas_stats_null";

        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + emptyTableName);
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + allNullTableName);

        try {
            // Create table with data
            env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s (%s)", tableName, ALL_TYPES_TABLE_DDL));
            env.executeTrinoUpdate(format("INSERT INTO hive.default.%s VALUES %s, %s",
                    tableName, ALL_TYPES_DATA_ROW_1, ALL_TYPES_DATA_ROW_2));

            // Create empty table
            env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s (%s)", emptyTableName, ALL_TYPES_TABLE_DDL));

            // Create table with only nulls
            env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s (%s)", allNullTableName, ALL_TYPES_TABLE_DDL));
            env.executeTrinoUpdate(format("INSERT INTO hive.default.%s VALUES %s", allNullTableName, ALL_TYPES_ALL_NULL_ROW));

            // Verify statistics computed on CREATE TABLE AS SELECT
            String ctasTableName = "test_ctas_result";
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + ctasTableName);
            env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s AS SELECT * FROM hive.default.%s", ctasTableName, tableName));
            assertThat(env.executeTrino("SHOW STATS FOR hive.default." + ctasTableName)).containsOnly(getAllTypesTableStatistics());
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + ctasTableName);

            // Empty table
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + ctasTableName);
            env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s AS SELECT * FROM hive.default.%s", ctasTableName, emptyTableName));
            assertThat(env.executeTrino("SHOW STATS FOR hive.default." + ctasTableName)).containsOnly(getAllTypesEmptyTableStatistics());
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + ctasTableName);

            // Null table
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + ctasTableName);
            env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s AS SELECT * FROM hive.default.%s", ctasTableName, allNullTableName));
            assertThat(env.executeTrino("SHOW STATS FOR hive.default." + ctasTableName)).containsOnly(getAllTypesAllNullTableStatistics());
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + ctasTableName);
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + emptyTableName);
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + allNullTableName);
        }
    }

    @ParameterizedTest
    @MethodSource("floatingPointDataTypes")
    void testComputeFloatingPointStatistics(String dataType, MultinodeHive4Environment env)
    {
        String tableName = "test_floating_point_stats";
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);

        try {
            env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s(c_basic %s, c_minmax %s, c_inf %s, c_ninf %s, c_nan %s, c_nzero %s)",
                    tableName, dataType, dataType, dataType, dataType, dataType, dataType));
            env.executeTrinoUpdate("ANALYZE hive.default." + tableName);

            env.executeTrinoUpdate(format(
                    "INSERT INTO hive.default.%s(c_basic, c_minmax, c_inf, c_ninf, c_nan, c_nzero) VALUES " +
                            "  (%s '42.3', %s '576234.567',  %s 'Infinity', %s '-Infinity', %s 'NaN', %s '-0')," +
                            "  (%s '42.3', %s '-1234567.89', %s '-15', %s '45', %s '12345', %s '-47'), " +
                            "  (NULL, NULL, NULL, NULL, NULL, NULL)",
                    tableName,
                    dataType, dataType, dataType, dataType, dataType, dataType,
                    dataType, dataType, dataType, dataType, dataType, dataType));

            List<Row> expectedStatistics;
            if ("double".equals(dataType)) {
                expectedStatistics = ImmutableList.of(
                        row("c_basic", null, 1., 0.3333333333333333, null, "42.3", "42.3"),
                        row("c_minmax", null, 2., 0.3333333333333333, null, "-1234567.89", "576234.567"),
                        row("c_inf", null, 2., 0.3333333333333333, null, null, null),
                        row("c_ninf", null, 2., 0.3333333333333333, null, null, null),
                        row("c_nan", null, 1., 0.3333333333333333, null, "12345.0", "12345.0"),
                        row("c_nzero", null, 2., 0.3333333333333333, null, "-47.0", "0.0"),
                        row(null, null, null, null, 3., null, null));
            }
            else {
                expectedStatistics = ImmutableList.of(
                        row("c_basic", null, 1., 0.3333333333333333, null, "42.3", "42.3"),
                        row("c_minmax", null, 2., 0.3333333333333333, null, "-1234567.9", "576234.56"),
                        row("c_inf", null, 2., 0.3333333333333333, null, null, null),
                        row("c_ninf", null, 2., 0.3333333333333333, null, null, null),
                        row("c_nan", null, 1., 0.3333333333333333, null, "12345.0", "12345.0"),
                        row("c_nzero", null, 2., 0.3333333333333333, null, "-47.0", "0.0"),
                        row(null, null, null, null, 3., null, null));
            }

            assertThat(env.executeTrino("SHOW STATS FOR hive.default." + tableName)).containsOnly(expectedStatistics);

            env.executeTrinoUpdate("ANALYZE hive.default." + tableName);
            assertThat(env.executeTrino("SHOW STATS FOR hive.default." + tableName)).containsOnly(expectedStatistics);
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    static Stream<Arguments> floatingPointDataTypes()
    {
        return Stream.of(
                Arguments.of("real"),
                Arguments.of("double"));
    }

    @Test
    void testComputeStatisticsForTableWithOnlyDateColumns(MultinodeHive4Environment env)
    {
        String tableName = "test_date_only_stats";
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);

        try {
            env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s AS SELECT date'2019-12-02' c_date", tableName));

            List<Row> expectedStatistics = ImmutableList.of(
                    row("c_date", null, 1.0, 0.0, null, "2019-12-02", "2019-12-02"),
                    row(null, null, null, null, 1.0, null, null));

            assertThat(env.executeTrino("SHOW STATS FOR hive.default." + tableName)).containsOnly(expectedStatistics);

            env.executeTrinoUpdate("ANALYZE hive.default." + tableName);
            assertThat(env.executeTrino("SHOW STATS FOR hive.default." + tableName)).containsOnly(expectedStatistics);
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testMixedHiveAndTrinoStatistics(MultinodeHive4Environment env)
    {
        String tableName = "test_mixed_stats";
        env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        env.executeHiveUpdate(format("CREATE TABLE default.%s (a INT) PARTITIONED BY (p INT) STORED AS ORC TBLPROPERTIES ('transactional' = 'false')", tableName));

        try {
            env.executeHiveUpdate(format("INSERT OVERWRITE TABLE default.%s PARTITION (p=1) VALUES (1),(2),(3),(4)", tableName));
            env.executeHiveUpdate(format("INSERT OVERWRITE TABLE default.%s PARTITION (p=2) VALUES (10),(11),(12)", tableName));

            String showStatsPartitionOne = format("SHOW STATS FOR (SELECT * FROM hive.default.%s WHERE p = 1)", tableName);
            String showStatsPartitionTwo = format("SHOW STATS FOR (SELECT * FROM hive.default.%s WHERE p = 2)", tableName);
            String showStatsWholeTable = format("SHOW STATS FOR hive.default.%s", tableName);

            // drop all stats
            env.executeTrinoUpdate(format("CALL system.drop_stats('default', '%s')", tableName));

            // sanity check that there are no statistics
            assertThat(env.executeTrino(showStatsPartitionOne)).containsOnly(
                    row("p", null, 1.0, 0.0, null, "1", "1"),
                    row("a", null, null, null, null, null, null),
                    row(null, null, null, null, null, null, null));
            assertThat(env.executeTrino(showStatsPartitionTwo)).containsOnly(
                    row("p", null, 1.0, 0.0, null, "2", "2"),
                    row("a", null, null, null, null, null, null),
                    row(null, null, null, null, null, null, null));
            assertThat(env.executeTrino(showStatsWholeTable)).containsOnly(
                    row("p", null, 2.0, 0.0, null, "1", "2"),
                    row("a", null, null, null, null, null, null),
                    row(null, null, null, null, null, null, null));

            // analyze first partition with Trino and second with Hive
            env.executeTrinoUpdate(format("ANALYZE hive.default.%s WITH (partitions = ARRAY[ARRAY['1']])", tableName));
            env.executeHiveUpdate(format("ANALYZE TABLE default.%s PARTITION (p = 2) COMPUTE STATISTICS", tableName));
            env.executeHiveUpdate(format("ANALYZE TABLE default.%s PARTITION (p = 2) COMPUTE STATISTICS FOR COLUMNS", tableName));
            env.executeTrinoUpdate("CALL system.flush_metadata_cache()");

            // we can get stats for individual partitions
            assertThat(env.executeTrino(showStatsPartitionOne)).containsOnly(
                    row("p", null, 1.0, 0.0, null, "1", "1"),
                    row("a", null, 4.0, 0.0, null, "1", "4"),
                    row(null, null, null, null, 4.0, null, null));
            assertThat(env.executeTrino(showStatsPartitionTwo)).containsOnly(
                    row("p", null, 1.0, 0.0, null, "2", "2"),
                    row("a", null, 3.0, 0.0, null, "10", "12"),
                    row(null, null, null, null, 3.0, null, null));

            // as well as for whole table
            assertThat(env.executeTrino(showStatsWholeTable)).containsOnly(
                    row("p", null, 2.0, 0.0, null, "1", "2"),
                    row("a", null, 4.0, 0.0, null, "1", "12"),
                    row(null, null, null, null, 7.0, null, null));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testEmptyPartitionedHiveStatistics(MultinodeHive4Environment env)
    {
        String tableName = "test_empty_partitioned_stats";
        try {
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
            env.executeHiveUpdate(format("CREATE TABLE default.%s (a INT) PARTITIONED BY (p INT)", tableName));

            // disable computation of statistics
            env.executeHiveUpdate("set hive.stats.autogather=false");

            env.executeHiveUpdate(format("INSERT INTO TABLE default.%s PARTITION (p=1) VALUES (11),(12),(13),(14)", tableName));
            env.executeHiveUpdate(format("INSERT INTO TABLE default.%s PARTITION (p=2) VALUES (21),(22),(23)", tableName));

            String showStatsPartitionOne = format("SHOW STATS FOR (SELECT * FROM hive.default.%s WHERE p = 1)", tableName);
            String showStatsPartitionTwo = format("SHOW STATS FOR (SELECT * FROM hive.default.%s WHERE p = 2)", tableName);
            String showStatsWholeTable = format("SHOW STATS FOR hive.default.%s", tableName);

            assertThat(env.executeTrino(showStatsPartitionOne)).containsOnly(
                    row("p", null, 1.0, 0.0, null, "1", "1"),
                    row("a", null, null, null, null, null, null),
                    row(null, null, null, null, null, null, null));
            assertThat(env.executeTrino(showStatsPartitionTwo)).containsOnly(
                    row("p", null, 1.0, 0.0, null, "2", "2"),
                    row("a", null, null, null, null, null, null),
                    row(null, null, null, null, null, null, null));
            assertThat(env.executeTrino(showStatsWholeTable)).containsOnly(
                    row("p", null, 2.0, 0.0, null, "1", "2"),
                    row("a", null, null, null, null, null, null),
                    row(null, null, null, null, null, null, null));
        }
        finally {
            // enable computation of statistics
            env.executeHiveUpdate("set hive.stats.autogather=true");
            env.executeHiveUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testComputeTableStatisticsOnInsert(MultinodeHive4Environment env)
    {
        // Create source tables
        String allTypesTable = "test_insert_stats_all_types";
        String emptyAllTypesTable = "test_insert_stats_all_types_empty";
        String allTypesAllNullTable = "test_insert_stats_all_types_null";

        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + allTypesTable);
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + emptyAllTypesTable);
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + allTypesAllNullTable);

        try {
            // Create tables with data
            env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s (%s)", allTypesTable, ALL_TYPES_TABLE_DDL));
            env.executeTrinoUpdate(format("INSERT INTO hive.default.%s VALUES %s, %s",
                    allTypesTable, ALL_TYPES_DATA_ROW_1, ALL_TYPES_DATA_ROW_2));

            env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s (%s)", emptyAllTypesTable, ALL_TYPES_TABLE_DDL));

            env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s (%s)", allTypesAllNullTable, ALL_TYPES_TABLE_DDL));
            env.executeTrinoUpdate(format("INSERT INTO hive.default.%s VALUES %s", allTypesAllNullTable, ALL_TYPES_ALL_NULL_ROW));

            // Test assertComputeTableStatisticsOnInsert for all_types table
            assertComputeTableStatisticsOnInsert(env, allTypesTable, getAllTypesTableStatistics());

            // Test assertComputeTableStatisticsOnInsert for empty table
            assertComputeTableStatisticsOnInsert(env, emptyAllTypesTable, getAllTypesEmptyTableStatistics());

            // Test assertComputeTableStatisticsOnInsert for all-null table
            assertComputeTableStatisticsOnInsert(env, allTypesAllNullTable, getAllTypesAllNullTableStatistics());

            // Test combined insert scenario
            String tableName = "test_update_table_statistics";
            env.executeTrinoUpdate(format("DROP TABLE IF EXISTS hive.default.%s", tableName));
            try {
                env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s AS SELECT * FROM hive.default.%s WITH NO DATA", tableName, allTypesTable));
                env.executeTrinoUpdate(format("INSERT INTO hive.default.%s SELECT * FROM hive.default.%s", tableName, allTypesTable));
                env.executeTrinoUpdate(format("INSERT INTO hive.default.%s SELECT * FROM hive.default.%s", tableName, allTypesAllNullTable));
                env.executeTrinoUpdate(format("INSERT INTO hive.default.%s SELECT * FROM hive.default.%s", tableName, allTypesAllNullTable));
                assertThat(env.executeTrino("SHOW STATS FOR hive.default." + tableName)).containsOnly(
                        row("c_tinyint", null, 2.0, 0.5, null, "121", "127"),
                        row("c_smallint", null, 2.0, 0.5, null, "32761", "32767"),
                        row("c_int", null, 2.0, 0.5, null, "2147483641", "2147483647"),
                        row("c_bigint", null, 1.0, 0.5, null, "9223372036854775807", "9223372036854775807"),
                        row("c_float", null, 2.0, 0.5, null, "123.341", "123.345"),
                        row("c_double", null, 2.0, 0.5, null, "234.561", "235.567"),
                        row("c_decimal", null, 2.0, 0.5, null, "345.0", "346.0"),
                        row("c_decimal_w_params", null, 2.0, 0.5, null, "345.671", "345.678"),
                        row("c_timestamp", null, 2.0, 0.5, null, null, null),
                        row("c_date", null, 2.0, 0.5, null, "2015-05-09", "2015-06-10"),
                        row("c_string", 22.0, 2.0, 0.5, null, null, null),
                        row("c_varchar", 20.0, 2.0, 0.5, null, null, null),
                        row("c_char", 12.0, 2.0, 0.5, null, null, null),
                        row("c_boolean", null, 2.0, 0.5, null, null, null),
                        row("c_binary", 23.0, null, 0.5, null, null, null),
                        row(null, null, null, null, 4.0, null, null));

                env.executeTrinoUpdate(format("INSERT INTO hive.default.%s VALUES( " +
                        "TINYINT '120', " +
                        "SMALLINT '32760', " +
                        "INTEGER '2147483640', " +
                        "BIGINT '9223372036854775800', " +
                        "REAL '123.340', " +
                        "DOUBLE '234.560', " +
                        "CAST(343.0 AS DECIMAL(10, 0)), " +
                        "CAST(345.670 AS DECIMAL(10, 5)), " +
                        "TIMESTAMP '2015-05-10 12:15:30', " +
                        "DATE '2015-05-08', " +
                        "VARCHAR 'ela ma kot', " +
                        "CAST('ela ma ko' AS VARCHAR(10)), " +
                        "CAST('ela m     ' AS CHAR(10)), " +
                        "false, " +
                        "CAST('cGllcyBiaW5hcm54' as VARBINARY))", tableName));

                assertThat(env.executeTrino("SHOW STATS FOR hive.default." + tableName)).containsOnly(ImmutableList.of(
                        row("c_tinyint", null, 2.0, 0.4, null, "120", "127"),
                        row("c_smallint", null, 2.0, 0.4, null, "32760", "32767"),
                        row("c_int", null, 2.0, 0.4, null, "2147483640", "2147483647"),
                        row("c_bigint", null, 1.0, 0.4, null, "9223372036854775807", "9223372036854775807"),
                        row("c_float", null, 2.0, 0.4, null, "123.34", "123.345"),
                        row("c_double", null, 2.0, 0.4, null, "234.56", "235.567"),
                        row("c_decimal", null, 2.0, 0.4, null, "343.0", "346.0"),
                        row("c_decimal_w_params", null, 2.0, 0.4, null, "345.67", "345.678"),
                        row("c_timestamp", null, 2.0, 0.4, null, null, null),
                        row("c_date", null, 2.0, 0.4, null, "2015-05-08", "2015-06-10"),
                        row("c_string", 32.0001, 2.0, 0.4, null, null, null),
                        row("c_varchar", 29.0001, 2.0, 0.4, null, null, null),
                        row("c_char", 17.0001, 2.0, 0.4, null, null, null),
                        row("c_boolean", null, 2.0, 0.4, null, null, null),
                        row("c_binary", 39.0, null, 0.4, null, null, null),
                        row(null, null, null, null, 5.0, null, null)));
            }
            finally {
                env.executeTrinoUpdate(format("DROP TABLE IF EXISTS hive.default.%s", tableName));
            }
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + allTypesTable);
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + emptyAllTypesTable);
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + allTypesAllNullTable);
        }
    }

    private void assertComputeTableStatisticsOnInsert(MultinodeHive4Environment env, String sourceTableName, List<Row> expectedStatistics)
    {
        String copiedTableName = "assert_insert_stats_" + sourceTableName;
        env.executeTrinoUpdate(format("DROP TABLE IF EXISTS hive.default.%s", copiedTableName));
        try {
            env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s AS SELECT * FROM hive.default.%s WITH NO DATA", copiedTableName, sourceTableName));
            assertThat(env.executeTrino("SHOW STATS FOR hive.default." + copiedTableName)).containsOnly(getAllTypesEmptyTableStatistics());
            env.executeTrinoUpdate(format("INSERT INTO hive.default.%s SELECT * FROM hive.default.%s", copiedTableName, sourceTableName));
            assertThat(env.executeTrino("SHOW STATS FOR hive.default." + copiedTableName)).containsOnly(expectedStatistics);
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE IF EXISTS hive.default.%s", copiedTableName));
        }
    }

    @Test
    void testComputePartitionStatisticsOnCreateTable(MultinodeHive4Environment env)
    {
        String tableName = "test_compute_partition_statistics_on_create_table";
        env.executeTrinoUpdate(format("DROP TABLE IF EXISTS hive.default.%s", tableName));
        try {
            env.executeTrinoUpdate(format("""
                    CREATE TABLE hive.default.%s WITH (
                        partitioned_by = ARRAY['p_bigint', 'p_varchar']
                    ) AS
                    SELECT * FROM (
                        VALUES
                            (
                               TINYINT '120',
                               SMALLINT '32760',
                               INTEGER '2147483640',
                               BIGINT '9223372036854775800',
                               REAL '123.340', DOUBLE '234.560',
                               CAST(343.0 AS DECIMAL(10, 0)),
                               CAST(345.670 AS DECIMAL(10, 5)),
                               TIMESTAMP '2015-05-10 12:15:30.000',
                               DATE '2015-05-08',
                               VARCHAR 'p1 varchar',
                               CAST('p1 varchar10' AS VARCHAR(10)),
                               CAST('p1 char10' AS CHAR(10)), false,
                               CAST('p1 binary' as VARBINARY),
                               BIGINT '1',
                               VARCHAR 'partition1'
                            ),
                            (null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, BIGINT '1', 'partition1'),
                            (
                               TINYINT '99',
                               SMALLINT '333',
                               INTEGER '444',
                               BIGINT '555',
                               REAL '666.340',
                               DOUBLE '777.560',
                               CAST(888.0 AS DECIMAL(10, 0)),
                               CAST(999.670 AS DECIMAL(10, 5)),
                               TIMESTAMP '2015-05-10 12:45:30.000',
                               DATE '2015-05-09',
                               VARCHAR 'p2 varchar',
                               CAST('p2 varchar10' AS VARCHAR(10)),
                               CAST('p2 char10' AS CHAR(10)),
                               true,
                               CAST('p2 binary' as VARBINARY),
                               BIGINT '2',
                               VARCHAR 'partition2'
                            ),
                            (null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, BIGINT '2', 'partition2')
                    ) AS t (c_tinyint, c_smallint, c_int, c_bigint, c_float, c_double, c_decimal, c_decimal_w_params, c_timestamp, c_date, c_string, c_varchar, c_char, c_boolean, c_binary, p_bigint, p_varchar)
                    """, tableName));

            assertThat(env.executeTrino(format("SHOW STATS FOR (SELECT * FROM hive.default.%s WHERE p_bigint = 1 AND p_varchar = 'partition1')", tableName))).containsOnly(ImmutableList.of(
                    row("c_tinyint", null, 1.0, 0.5, null, "120", "120"),
                    row("c_smallint", null, 1.0, 0.5, null, "32760", "32760"),
                    row("c_int", null, 1.0, 0.5, null, "2147483640", "2147483640"),
                    row("c_bigint", null, 1.0, 0.5, null, "9223372036854775807", "9223372036854775807"),
                    row("c_float", null, 1.0, 0.5, null, "123.34", "123.34"),
                    row("c_double", null, 1.0, 0.5, null, "234.56", "234.56"),
                    row("c_decimal", null, 1.0, 0.5, null, "343.0", "343.0"),
                    row("c_decimal_w_params", null, 1.0, 0.5, null, "345.67", "345.67"),
                    row("c_timestamp", null, 1.0, 0.5, null, null, null),
                    row("c_date", null, 1.0, 0.5, null, "2015-05-08", "2015-05-08"),
                    row("c_string", 10.0, 1.0, 0.5, null, null, null),
                    row("c_varchar", 10.0, 1.0, 0.5, null, null, null),
                    row("c_char", 9.0, 1.0, 0.5, null, null, null),
                    row("c_boolean", null, 1.0, 0.5, null, null, null),
                    row("c_binary", 9.0, null, 0.5, null, null, null),
                    row("p_bigint", null, 1.0, 0.0, null, "1", "1"),
                    row("p_varchar", 20.0, 1.0, 0.0, null, null, null),
                    row(null, null, null, null, 2.0, null, null)));

            assertThat(env.executeTrino(format("SHOW STATS FOR (SELECT * FROM hive.default.%s WHERE p_bigint = 2 AND p_varchar = 'partition2')", tableName))).containsOnly(ImmutableList.of(
                    row("c_tinyint", null, 1.0, 0.5, null, "99", "99"),
                    row("c_smallint", null, 1.0, 0.5, null, "333", "333"),
                    row("c_int", null, 1.0, 0.5, null, "444", "444"),
                    row("c_bigint", null, 1.0, 0.5, null, "555", "555"),
                    row("c_float", null, 1.0, 0.5, null, "666.34", "666.34"),
                    row("c_double", null, 1.0, 0.5, null, "777.56", "777.56"),
                    row("c_decimal", null, 1.0, 0.5, null, "888.0", "888.0"),
                    row("c_decimal_w_params", null, 1.0, 0.5, null, "999.67", "999.67"),
                    row("c_timestamp", null, 1.0, 0.5, null, null, null),
                    row("c_date", null, 1.0, 0.5, null, "2015-05-09", "2015-05-09"),
                    row("c_string", 10.0, 1.0, 0.5, null, null, null),
                    row("c_varchar", 10.0, 1.0, 0.5, null, null, null),
                    row("c_char", 9.0, 1.0, 0.5, null, null, null),
                    row("c_boolean", null, 1.0, 0.5, null, null, null),
                    row("c_binary", 9.0, null, 0.5, null, null, null),
                    row("p_bigint", null, 1.0, 0.0, null, "2", "2"),
                    row("p_varchar", 20.0, 1.0, 0.0, null, null, null),
                    row(null, null, null, null, 2.0, null, null)));
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE IF EXISTS hive.default.%s", tableName));
        }
    }

    @Test
    void testComputePartitionStatisticsOnInsert(MultinodeHive4Environment env)
    {
        String tableName = "test_compute_partition_statistics_on_insert";

        env.executeTrinoUpdate(format("DROP TABLE IF EXISTS hive.default.%s", tableName));
        try {
            env.executeTrinoUpdate(format("""
                    CREATE TABLE hive.default.%s(
                        c_tinyint            TINYINT,
                        c_smallint           SMALLINT,
                        c_int                INT,
                        c_bigint             BIGINT,
                        c_float              REAL,
                        c_double             DOUBLE,
                        c_decimal            DECIMAL(10,0),
                        c_decimal_w_params   DECIMAL(10,5),
                        c_timestamp          TIMESTAMP,
                        c_date               DATE,
                        c_string             VARCHAR,
                        c_varchar            VARCHAR(10),
                        c_char               CHAR(10),
                        c_boolean            BOOLEAN,
                        c_binary             VARBINARY,
                        p_bigint             BIGINT,
                        p_varchar            VARCHAR
                    ) WITH (
                        partitioned_by = ARRAY['p_bigint', 'p_varchar']
                    )
                    """, tableName));

            env.executeTrinoUpdate(format("INSERT INTO hive.default.%s VALUES " +
                    "(TINYINT '120', SMALLINT '32760', INTEGER '2147483640', BIGINT '9223372036854775800', REAL '123.340', DOUBLE '234.560', CAST(343.0 AS DECIMAL(10, 0)), CAST(345.670 AS DECIMAL(10, 5)), TIMESTAMP '2015-05-10 12:15:30', DATE '2015-05-08', 'p1 varchar', CAST('p1 varchar10' AS VARCHAR(10)), CAST('p1 char10' AS CHAR(10)), false, CAST('p1 binary' as VARBINARY), BIGINT '1', 'partition1')," +
                    "(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, BIGINT '1', 'partition1')", tableName));

            env.executeTrinoUpdate(format("INSERT INTO hive.default.%s VALUES " +
                    "(TINYINT '99', SMALLINT '333', INTEGER '444', BIGINT '555', REAL '666.340', DOUBLE '777.560', CAST(888.0 AS DECIMAL(10, 0)), CAST(999.670 AS DECIMAL(10, 5)), TIMESTAMP '2015-05-10 12:45:30', DATE '2015-05-09', 'p2 varchar', CAST('p2 varchar10' AS VARCHAR(10)), CAST('p2 char10' AS CHAR(10)), true, CAST('p2 binary' as VARBINARY), BIGINT '2', 'partition2')," +
                    "(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, BIGINT '2', 'partition2')", tableName));

            String showStatsPartitionOne = format("SHOW STATS FOR (SELECT * FROM hive.default.%s WHERE p_bigint = 1 AND p_varchar = 'partition1')", tableName);
            String showStatsPartitionTwo = format("SHOW STATS FOR (SELECT * FROM hive.default.%s WHERE p_bigint = 2 AND p_varchar = 'partition2')", tableName);

            assertThat(env.executeTrino(showStatsPartitionOne)).containsOnly(ImmutableList.of(
                    row("c_tinyint", null, 1.0, 0.5, null, "120", "120"),
                    row("c_smallint", null, 1.0, 0.5, null, "32760", "32760"),
                    row("c_int", null, 1.0, 0.5, null, "2147483640", "2147483640"),
                    row("c_bigint", null, 1.0, 0.5, null, "9223372036854775807", "9223372036854775807"),
                    row("c_float", null, 1.0, 0.5, null, "123.34", "123.34"),
                    row("c_double", null, 1.0, 0.5, null, "234.56", "234.56"),
                    row("c_decimal", null, 1.0, 0.5, null, "343.0", "343.0"),
                    row("c_decimal_w_params", null, 1.0, 0.5, null, "345.67", "345.67"),
                    row("c_timestamp", null, 1.0, 0.5, null, null, null),
                    row("c_date", null, 1.0, 0.5, null, "2015-05-08", "2015-05-08"),
                    row("c_string", 10.0, 1.0, 0.5, null, null, null),
                    row("c_varchar", 10.0, 1.0, 0.5, null, null, null),
                    row("c_char", 9.0, 1.0, 0.5, null, null, null),
                    row("c_boolean", null, 1.0, 0.5, null, null, null),
                    row("c_binary", 9.0, null, 0.5, null, null, null),
                    row("p_bigint", null, 1.0, 0.0, null, "1", "1"),
                    row("p_varchar", 20.0, 1.0, 0.0, null, null, null),
                    row(null, null, null, null, 2.0, null, null)));

            assertThat(env.executeTrino(showStatsPartitionTwo)).containsOnly(ImmutableList.of(
                    row("c_tinyint", null, 1.0, 0.5, null, "99", "99"),
                    row("c_smallint", null, 1.0, 0.5, null, "333", "333"),
                    row("c_int", null, 1.0, 0.5, null, "444", "444"),
                    row("c_bigint", null, 1.0, 0.5, null, "555", "555"),
                    row("c_float", null, 1.0, 0.5, null, "666.34", "666.34"),
                    row("c_double", null, 1.0, 0.5, null, "777.56", "777.56"),
                    row("c_decimal", null, 1.0, 0.5, null, "888.0", "888.0"),
                    row("c_decimal_w_params", null, 1.0, 0.5, null, "999.67", "999.67"),
                    row("c_timestamp", null, 1.0, 0.5, null, null, null),
                    row("c_date", null, 1.0, 0.5, null, "2015-05-09", "2015-05-09"),
                    row("c_string", 10.0, 1.0, 0.5, null, null, null),
                    row("c_varchar", 10.0, 1.0, 0.5, null, null, null),
                    row("c_char", 9.0, 1.0, 0.5, null, null, null),
                    row("c_boolean", null, 1.0, 0.5, null, null, null),
                    row("c_binary", 9.0, null, 0.5, null, null, null),
                    row("p_bigint", null, 1.0, 0.0, null, "2", "2"),
                    row("p_varchar", 20.0, 1.0, 0.0, null, null, null),
                    row(null, null, null, null, 2.0, null, null)));

            env.executeTrinoUpdate(format("INSERT INTO hive.default.%s VALUES( TINYINT '119', SMALLINT '32759', INTEGER '2147483639', BIGINT '9223372036854775799', REAL '122.340', DOUBLE '233.560', CAST(342.0 AS DECIMAL(10, 0)), CAST(344.670 AS DECIMAL(10, 5)), TIMESTAMP '2015-05-10 12:15:29', DATE '2015-05-07', 'p1 varchar', CAST('p1 varchar10' AS VARCHAR(10)), CAST('p1 char10' AS CHAR(10)), true, CAST('p1 binary' as VARBINARY), BIGINT '1', 'partition1')", tableName));
            env.executeTrinoUpdate(format("INSERT INTO hive.default.%s VALUES( null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, BIGINT '1', 'partition1')", tableName));

            assertThat(env.executeTrino(showStatsPartitionOne)).containsOnly(ImmutableList.of(
                    row("c_tinyint", null, 1.0, 0.5, null, "119", "120"),
                    row("c_smallint", null, 1.0, 0.5, null, "32759", "32760"),
                    row("c_int", null, 1.0, 0.5, null, "2147483639", "2147483640"),
                    row("c_bigint", null, 1.0, 0.5, null, "9223372036854775807", "9223372036854775807"),
                    row("c_float", null, 1.0, 0.5, null, "122.34", "123.34"),
                    row("c_double", null, 1.0, 0.5, null, "233.56", "234.56"),
                    row("c_decimal", null, 1.0, 0.5, null, "342.0", "343.0"),
                    row("c_decimal_w_params", null, 1.0, 0.5, null, "344.67", "345.67"),
                    row("c_timestamp", null, 1.0, 0.5, null, null, null),
                    row("c_date", null, 1.0, 0.5, null, "2015-05-07", "2015-05-08"),
                    row("c_string", 20.0, 1.0, 0.5, null, null, null),
                    row("c_varchar", 20.0, 1.0, 0.5, null, null, null),
                    row("c_char", 18.0, 1.0, 0.5, null, null, null),
                    row("c_boolean", null, 2.0, 0.5, null, null, null),
                    row("c_binary", 18.0, null, 0.5, null, null, null),
                    row("p_bigint", null, 1.0, 0.0, null, "1", "1"),
                    row("p_varchar", 40.0, 1.0, 0.0, null, null, null),
                    row(null, null, null, null, 4.0, null, null)));

            env.executeTrinoUpdate(format("INSERT INTO hive.default.%s VALUES( TINYINT '100', SMALLINT '334', INTEGER '445', BIGINT '556', REAL '667.340', DOUBLE '778.560', CAST(889.0 AS DECIMAL(10, 0)), CAST(1000.670 AS DECIMAL(10, 5)), TIMESTAMP '2015-05-10 12:45:31', DATE '2015-05-10', VARCHAR 'p2 varchar', CAST('p2 varchar10' AS VARCHAR(10)), CAST('p2 char10' AS CHAR(10)), true, CAST('p2 binary' as VARBINARY), BIGINT '2', 'partition2')", tableName));
            env.executeTrinoUpdate(format("INSERT INTO hive.default.%s VALUES( null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, BIGINT '2', 'partition2')", tableName));

            assertThat(env.executeTrino(showStatsPartitionTwo)).containsOnly(ImmutableList.of(
                    row("c_tinyint", null, 1.0, 0.5, null, "99", "100"),
                    row("c_smallint", null, 1.0, 0.5, null, "333", "334"),
                    row("c_int", null, 1.0, 0.5, null, "444", "445"),
                    row("c_bigint", null, 1.0, 0.5, null, "555", "556"),
                    row("c_float", null, 1.0, 0.5, null, "666.34", "667.34"),
                    row("c_double", null, 1.0, 0.5, null, "777.56", "778.56"),
                    row("c_decimal", null, 1.0, 0.5, null, "888.0", "889.0"),
                    row("c_decimal_w_params", null, 1.0, 0.5, null, "999.67", "1000.67"),
                    row("c_timestamp", null, 1.0, 0.5, null, null, null),
                    row("c_date", null, 1.0, 0.5, null, "2015-05-09", "2015-05-10"),
                    row("c_string", 20.0, 1.0, 0.5, null, null, null),
                    row("c_varchar", 20.0, 1.0, 0.5, null, null, null),
                    row("c_char", 18.0, 1.0, 0.5, null, null, null),
                    row("c_boolean", null, 1.0, 0.5, null, null, null),
                    row("c_binary", 18.0, null, 0.5, null, null, null),
                    row("p_bigint", null, 1.0, 0.0, null, "2", "2"),
                    row("p_varchar", 40.0, 1.0, 0.0, null, null, null),
                    row(null, null, null, null, 4.0, null, null)));
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE IF EXISTS hive.default.%s", tableName));
        }
    }
}
