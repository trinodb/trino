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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.trino.execution.QueryStats;
import io.trino.operator.OperatorStats;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.createDeltaLakeQueryRunner;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestSplitPruning
        extends AbstractTestQueryFramework
{
    private static final List<String> TABLES = ImmutableList.of(
            "double_inf",
            "double_nan",
            "part",
            "float_nan",
            "float_inf",
            "no_stats",
            "nested_fields",
            "timestamp",
            "test_partitioning",
            "parquet_struct_statistics",
            "uppercase_columns_partitions",
            "uppercase_columns_json_statistics",
            "uppercase_columns_struct_statistics");

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createDeltaLakeQueryRunner(DELTA_CATALOG, ImmutableMap.of(), ImmutableMap.of("delta.register-table-procedure.enabled", "true"));
    }

    @BeforeClass
    public void registerTables()
    {
        for (String table : TABLES) {
            String dataPath = Resources.getResource("databricks/pruning/" + table).toExternalForm();
            getQueryRunner().execute(
                    format("CALL system.register_table('%s', '%s', '%s')", getSession().getSchema().orElseThrow(), table, dataPath));
        }
    }

    @DataProvider
    public Object[][] types()
    {
        return new Object[][] {{"float"}, {"double"}};
    }

    @Test(dataProvider = "types")
    public void testStatsPruningInfinity(String type)
    {
        String tableName = type + "_inf";
        // Data generated using:
        // INSERT INTO pruning_inf_test VALUES
        //   (1.0, 'a1', CAST('-Infinity' as DOUBLE)),
        //   (1.0, 'b1', 100.0),
        //   (2.0, 'a2', 200.0),
        //   (2.0, 'b2', CAST('+Infinity' as DOUBLE)),
        //   (3.0, 'a3', CAST('-Infinity' as DOUBLE)),
        //   (3.0, 'b3', 150.0),
        //   (3.0, 'c3', null),
        //   (3.0, 'd3', CAST('+Infinity' as DOUBLE)),
        //   (4.0, 'a4', null)

        // a1, b1, a3, b3, c3 and d3 were processed, across 2 splits
        assertResultAndSplitCount(
                format("SELECT name FROM %s WHERE val < 200", tableName),
                Set.of("a1", "b1", "a3", "b3"),
                2);

        // a1, b1, a3, b3, c3 and d3 were processed, across 2 splits
        assertResultAndSplitCount(
                format("SELECT name FROM %s WHERE val > 100", tableName),
                Set.of("a2", "b2", "b3", "d3"),
                2);

        // 2 out of 4 splits
        assertResultAndSplitCount(
                format("SELECT name FROM %s WHERE val IS NULL", tableName),
                Set.of("c3", "a4"),
                2);
    }

    @Test(dataProvider = "types")
    public void testStatsPruningNaN(String type)
    {
        String tableName = type + "_nan";
        // Data generated using:
        // INSERT INTO pruning_nan_test VALUES
        //   (5.0, 'a5', CAST('NaN' as DOUBLE)),
        //   (5.0, 'b5', 100),
        //   (6.0, 'a6', CAST('NaN' as DOUBLE)),
        //   (6.0, 'b6', CAST('+Infinity' as DOUBLE))

        // no pruning, because the domain contains NaN
        assertResultAndSplitCount(
                format("SELECT name FROM %s WHERE val < 100", tableName),
                Set.of(),
                2);

        // pruning works with the IS NULL predicate
        assertResultAndSplitCount(
                format("SELECT name FROM %s WHERE val IS NULL", tableName),
                Set.of(),
                0);

        MaterializedResult result = getDistributedQueryRunner().execute(
                getSession(),
                format("SELECT name FROM %s WHERE val IS NOT NULL", tableName));
        assertEquals(result.getOnlyColumnAsSet(), Set.of("a5", "b5", "a6", "b6"));
    }

    @Test
    public void testNoStats()
    {
        String tableName = "no_stats";
        // Same data and queries as for the inf test, but no stats (or invalid stats) in the metadata, so no pruning

        assertResultAndSplitCount(
                format("SELECT name FROM %s WHERE val < 200", tableName),
                Set.of("a1", "b1", "a3", "b3"),
                4);

        assertResultAndSplitCount(
                format("SELECT name FROM %s WHERE val > 100", tableName),
                Set.of("a2", "b2", "b3", "d3"),
                4);

        assertResultAndSplitCount(
                format("SELECT name FROM %s WHERE val IS NULL", tableName),
                Set.of("c3", "a4"),
                4);
    }

    @Test
    public void testPruningUsingPartitions()
    {
        String tableName = "part";
        // Data generated using:
        // INSERT INTO pruning_part_test VALUES
        //   (7.0, 'a7', 100.0),
        //   (null, 'null', 100.0),
        //   (CAST('-Infinity' as DOUBLE), '-Infinity', 100.0),
        //   (CAST('+Infinity' as DOUBLE), '+Infinity', 100.0),
        //   (CAST('NaN' as DOUBLE), 'NaN', 100.0)

        assertResultAndSplitCount(
                format("SELECT name FROM %s WHERE part_key = 7", tableName),
                Set.of("a7"),
                1);

        assertResultAndSplitCount(
                format("SELECT name FROM %s WHERE part_key IS NOT NULL", tableName),
                Set.of("a7", "-Infinity", "+Infinity", "NaN"),
                4);

        assertResultAndSplitCount(
                format("SELECT name FROM %s WHERE part_key IS NULL", tableName),
                Set.of("null"),
                1);

        assertResultAndSplitCount(
                format("SELECT name FROM %s WHERE part_key > 0", tableName),
                Set.of("a7", "+Infinity"),
                2);

        assertResultAndSplitCount(
                format("SELECT name FROM %s WHERE part_key < 10", tableName),
                Set.of("a7", "-Infinity"),
                2);
    }

    @Test
    public void testPruningUsingPartitionsUppercase()
    {
        String tableName = "uppercase_columns_partitions";

        assertResultAndSplitCount(
                format("SELECT ala FROM %s WHERE ala > 0", tableName),
                result -> {
                    assertThat(result.getOnlyColumnAsSet()).containsOnly(1L, 2L, 3L);
                    assertThat(result.getRowCount()).isEqualTo(5);
                },
                3);

        assertResultAndSplitCount(
                format("SELECT ala FROM %s WHERE ala = 1", tableName),
                result -> {
                    assertThat(result.getOnlyColumnAsSet()).containsOnly(1L);
                    assertThat(result.getRowCount()).isEqualTo(2);
                },
                1);

        assertResultAndSplitCount(
                format("SELECT ala FROM %s WHERE ala > 1", tableName),
                result -> {
                    assertThat(result.getOnlyColumnAsSet()).containsOnly(2L, 3L);
                    assertThat(result.getRowCount()).isEqualTo(3);
                },
                2);

        assertResultAndSplitCount(
                format("SELECT kota FROM %s WHERE ala = 1", tableName),
                result -> {
                    assertThat(result.getOnlyColumnAsSet()).containsOnly(1L, 2L);
                    assertThat(result.getRowCount()).isEqualTo(2);
                },
                1);
    }

    @Test
    public void testPartitionPruningWithExpression()
    {
        // Data generated by:
        // CREATE TABLE test_partitioning (id int, t_varchar VARCHAR) PARTITIONED BY (t_varchar);
        // INSERT INTO test_partitioning VALUES (1, 'a'), (2, 'b'), (3, 'c');
        assertResultAndSplitCount(
                "SELECT id FROM test_partitioning WHERE t_varchar LIKE '%a%'",
                Set.of(1),
                1);
    }

    /**
     * Test that partition filter that cannot be converted to a {@link io.trino.spi.predicate.Domain}
     * gets applied (and not forgotten) when there is another, Domain-convertable filter.
     * <p>
     * In the past, that caused a significant decrease in the connector's performance.
     */
    @Test
    public void testPartitionPruningWithExpressionAndDomainFilter()
    {
        // Data generated by:
        // CREATE TABLE test_partitioning (id int, t_varchar VARCHAR) PARTITIONED BY (t_varchar);
        // INSERT INTO test_partitioning VALUES (1, 'a'), (2, 'b'), (3, 'c');
        assertResultAndSplitCount(
                "SELECT id FROM test_partitioning WHERE t_varchar LIKE '%a%' AND id > 0",
                Set.of(1),
                1);
    }

    @Test
    public void testSplitGenerationError()
    {
        // log entry with invalid stats (low > high)
        String dataPath = Resources.getResource("databricks/pruning/invalid_log").toExternalForm();
        getQueryRunner().execute(
                format("CALL system.register_table('%s', 'person', '%s')", getSession().getSchema().orElseThrow(), dataPath));
        assertQueryFails("SELECT name FROM person WHERE income < 1000", "Failed to generate splits for tpch.person");
    }

    @Test
    public void testTimestampPruning()
    {
        String tableName = "timestamp";
        // Data generated using:
        // INSERT INTO pruning_part_test ('col_0', 'col_1', 'col_2', 'col_3') VALUES
        //        ('UTC', CAST('1900-01-01 00:00:00.000 UTC' AS TIMESTAMP WITH TIME ZONE), '1900-01-01 00:00:00.000', '1899-12-31T16:59:00.000-0701'),
        //        ('UTC', CAST('1952-04-03T01:02:03.456 UTC' AS TIMESTAMP WITH TIME ZONE), '1952-04-03 01:02:03.456789', '1952-04-02T17:02:03.456-0800'),
        //        ('UTC', CAST('1969-12-31T23:05:00.123 UTC' AS TIMESTAMP WITH TIME ZONE), '1969-12-31 23:05:00.123456789', '1969-12-31T15:05:00.123-0800'),
        //        ('UTC', CAST('1970-01-01T01:05:00.123 UTC' AS TIMESTAMP WITH TIME ZONE), '1970-01-01 01:05:00.123456789', '1969-12-31T17:05:00.123-0800'),
        //        ('UTC', CAST('1970-01-01T01:05:00.123 UTC' AS TIMESTAMP WITH TIME ZONE), '1970-01-01 00:05:00.123456789', '1969-12-31T16:05:00.123-0800'),
        //        ('UTC', CAST('1970-01-01T01:00:00.000 UTC' AS TIMESTAMP WITH TIME ZONE), '1970-01-01 00:00:00.000', '1969-12-31T16:00:00.000-0800'),
        //        ('UTC', CAST('1970-02-03T04:05:06.789 UTC' AS TIMESTAMP WITH TIME ZONE), '1970-02-03 04:05:06.789', '1970-02-02T21:05:06.789-0700'),
        //        ('UTC', CAST('1983-03-31T23:05:00.345 UTC' AS TIMESTAMP WITH TIME ZONE), '1983-03-31 23:05:00.3456789', '1983-03-31T16:05:00.345-0700'),
        //        ('UTC', CAST('1983-04-01T01:05:00.345 UTC' AS TIMESTAMP WITH TIME ZONE), '1983-04-01 01:05:00.3456789', '1983-03-31T18:05:00.345-0700'),
        //        ('UTC', CAST('1983-04-01T00:05:00.345 UTC' AS TIMESTAMP WITH TIME ZONE), '1983-04-01 00:05:00.3456789', '1983-03-31T17:05:00.345-0700'),
        //        ('UTC', CAST('1983-09-30T22:59:00.654 UTC' AS TIMESTAMP WITH TIME ZONE), '1983-09-30 22:59:00.654321', '1983-09-30T15:59:00.654-0700'),
        //        ('UTC', CAST('1983-09-30T23:59:00.654 UTC' AS TIMESTAMP WITH TIME ZONE), '1983-09-30 23:59:00.654321', '1983-09-30T16:59:00.654-0700'),
        //        ('UTC', CAST('1983-10-01T00:59:00.654 UTC' AS TIMESTAMP WITH TIME ZONE), '1983-10-01 00:59:00.654321', '1983-09-30T17:59:00.654-0700'),
        //        ('UTC', CAST('1996-10-27T01:05:00.987 UTC' AS TIMESTAMP WITH TIME ZONE), '1996-10-27 01:05:00.987', '1996-10-26T19:05:00.987-0600'),
        //        ('UTC', CAST('1996-10-27T02:05:00.987 UTC' AS TIMESTAMP WITH TIME ZONE), '1996-10-27 02:05:00.987', '1996-10-26T20:05:00.987-0600'),
        //        ('UTC', CAST('1996-10-27T00:05:00.987 UTC' AS TIMESTAMP WITH TIME ZONE), '1996-10-27 00:05:00.987', '1996-10-26T18:05:00.987-0600'),
        //        ('UTC', CAST('2017-07-01T00:00:00.000 UTC' AS TIMESTAMP WITH TIME ZONE), '2017-07-01 00:00:00.000', '2017-06-30T19:00:00.000-0500'),
        //        ('UTC', CAST('9999-12-31T23:59:59.999 UTC' AS TIMESTAMP WITH TIME ZONE), '9999-12-31 23:59:59.999999999', '9999-12-31T17:59:59.999-0600'),
        assertResultAndSplitCount(
                format("SELECT col_2 FROM %s WHERE col_0 = 'UTC' AND col_1 = CAST('1952-04-03 01:02:03.456 UTC' AS TIMESTAMP WITH TIME ZONE)", tableName),
                Set.of("1952-04-03 01:02:03.456789"),
                1);

        assertResultAndSplitCount(
                format("SELECT col_2 FROM %s WHERE col_0 = 'UTC' AND col_1 > CAST('1996-10-27 00:05:00.987 UTC' AS TIMESTAMP WITH TIME ZONE) AND col_1 < CAST('1996-10-27 02:05:00.987 UTC' AS TIMESTAMP WITH TIME ZONE)", tableName),
                Set.of("1996-10-27 01:05:00.987"),
                1);

        assertResultAndSplitCount(
                format("SELECT col_2 FROM %s WHERE col_0 = 'UTC' AND col_1 = ANY (VALUES CAST('1900-01-01 UTC' AS TIMESTAMP WITH TIME ZONE), CAST('1983-04-01 01:05:00.345 UTC' AS TIMESTAMP WITH TIME ZONE), CAST('1996-10-27 02:05:00.987 UTC' AS TIMESTAMP WITH TIME ZONE))", tableName),
                Set.of("1900-01-01 00:00:00.000", "1983-04-01 01:05:00.3456789", "1996-10-27 02:05:00.987"),
                3);

        assertResultAndSplitCount(
                format("SELECT col_2 FROM %s WHERE col_0 = 'UTC' AND " +
                        "col_1 BETWEEN CAST('1952-04-03 UTC' AS TIMESTAMP WITH TIME ZONE) AND CAST('1970-02-04 UTC' AS TIMESTAMP WITH TIME ZONE) AND " +
                        "col_3 >= CAST('1970-01-01 UTC' AS TIMESTAMP WITH TIME ZONE)", tableName),
                Set.of("1970-01-01 01:05:00.123456789", "1970-01-01 00:05:00.123456789", "1970-01-01 00:00:00.000", "1970-02-03 04:05:06.789"),
                4);

        assertResultAndSplitCount(
                format("SELECT col_2 FROM %s WHERE col_2 LIKE '2%%' AND col_3 > CAST('1999-12-31 UTC' AS TIMESTAMP WITH TIME ZONE)", tableName),
                Set.of("2017-07-01 00:00:00.000"),
                1);

        assertResultAndSplitCount(
                format("SELECT col_2 FROM %s WHERE col_2 > '1999'", tableName),
                Set.of("2017-07-01 00:00:00.000", "9999-12-31 23:59:59.999999999"),
                2);
    }

    @Test
    // See README.md for table values
    public void testParquetStatisticsPruning()
    {
        testCountQuery("SELECT count(*) FROM parquet_struct_statistics WHERE ts = TIMESTAMP '2960-10-31 01:00:00.000 UTC'", 3, 3);
        testCountQuery("SELECT count(*) FROM parquet_struct_statistics WHERE ts = TIMESTAMP '2960-10-31 01:00:00.000 UTC'", 3, 3);
        testCountQuery("SELECT count(*) FROM parquet_struct_statistics WHERE str = 'a'", 3, 3);
        testCountQuery("SELECT count(*) FROM parquet_struct_statistics WHERE dec_short = 10.1", 3, 3);
        testCountQuery("SELECT count(*) FROM parquet_struct_statistics WHERE dec_long = -999999999999.123", 3, 3);
        testCountQuery("SELECT count(*) FROM parquet_struct_statistics WHERE l = 0", 3, 3);
        testCountQuery("SELECT count(*) FROM parquet_struct_statistics WHERE \"in\" = -20000000", 3, 3);
        testCountQuery("SELECT count(*) FROM parquet_struct_statistics WHERE byt = 42", 3, 3);
        testCountQuery("SELECT count(*) FROM parquet_struct_statistics WHERE fl = 0.123", 3, 3);
        testCountQuery("SELECT count(*) FROM parquet_struct_statistics WHERE dou = -0.321", 3, 3);
        testCountQuery("SELECT count(*) FROM parquet_struct_statistics WHERE bool = true", 9, 9);
        testCountQuery("SELECT count(*) FROM parquet_struct_statistics WHERE bin = X'00 02'", 3, 9);
        testCountQuery("SELECT count(*) FROM parquet_struct_statistics WHERE dat = DATE '5000-01-01'", 3, 3);
        testCountQuery("SELECT count(*) FROM parquet_struct_statistics WHERE arr = ARRAY[5]", 3, 9);
        testCountQuery("SELECT count(*) FROM parquet_struct_statistics WHERE m = MAP(ARRAY[1], ARRAY['a'])", 3, 9);
        testCountQuery("SELECT count(*) FROM parquet_struct_statistics WHERE row = ROW(2, 'b')", 3, 9);
    }

    @Test
    public void testPrimitiveFieldsInsideRowColumnPruning()
    {
        assertResultAndSplitCount(
                "SELECT grandparent.parent1.child1 FROM nested_fields WHERE id > 6",
                Set.of(70.99, 80.99, 90.99, 100.99),
                1);

        assertResultAndSplitCount(
                "SELECT grandparent.parent1.child1 FROM nested_fields WHERE id > 10",
                Set.of(),
                0);

        // TODO pruning does not work on primitive fields inside a struct, expected splits should be 1 after file pruning (https://github.com/trinodb/trino/issues/17164)
        assertResultAndSplitCount(
                "SELECT grandparent.parent1.child1 FROM nested_fields WHERE parent.child1 > 600",
                Set.of(70.99, 80.99, 90.99, 100.99),
                2);

        // TODO pruning does not work on primitive fields inside a struct, expected splits should be 0 after file pruning (https://github.com/trinodb/trino/issues/17164)
        assertResultAndSplitCount(
                "SELECT grandparent.parent1.child1 FROM nested_fields WHERE parent.child1 > 1000",
                Set.of(),
                2);
    }

    @Test
    public void testJsonStatisticsPruningUppercaseColumn()
    {
        testCountQuery("SELECT count(*) FROM uppercase_columns_json_statistics WHERE blah = 2", 1, 1);
        testCountQuery("SELECT count(*) FROM uppercase_columns_json_statistics WHERE blah = 3", 2, 2);
        testCountQuery("SELECT count(*) FROM uppercase_columns_json_statistics WHERE blah <= 10", 8, 3);
    }

    @Test
    public void testStructStatisticsPruningUppercaseColumn()
    {
        testCountQuery("SELECT count(*) FROM uppercase_columns_struct_statistics WHERE blah = 2", 1, 1);
        testCountQuery("SELECT count(*) FROM uppercase_columns_struct_statistics WHERE blah = 3", 2, 2);
        testCountQuery("SELECT count(*) FROM uppercase_columns_struct_statistics WHERE blah <= 10", 8, 3);
    }

    /**
     * Test that a {@code SELECT count(*)} query returns the expected result an splits.
     */
    private void testCountQuery(@Language("SQL") String sql, long expectedRowCount, long expectedSplitCount)
    {
        assertResultAndSplitCount(
                sql,
                Set.of(expectedRowCount),
                expectedSplitCount);
    }

    private void assertResultAndSplitCount(String query, Set<?> expectedResultColumn, long expectedSplits)
    {
        assertResultAndSplitCount(
                query,
                result -> assertThat(result.getOnlyColumnAsSet()).isEqualTo(expectedResultColumn),
                expectedSplits);
    }

    private void assertResultAndSplitCount(String query, Consumer<MaterializedResult> resultAssertions, long expectedSplits)
    {
        if (expectedSplits == 0) {
            // asserting on number of splits being 0 doesn't work because even though all splits are pruned,
            // SourcePartitionedScheduler still produces an EmptySplit
            assertQueryStats(
                    getSession(),
                    query,
                    stats -> assertThat(getOperatorStats(stats).getInputDataSize().toBytes()).isEqualTo(0),
                    resultAssertions);
        }
        else {
            assertQueryStats(
                    getSession(),
                    query,
                    stats -> assertThat(getOperatorStats(stats).getTotalDrivers()).isEqualTo(expectedSplits),
                    resultAssertions);
        }
    }

    private OperatorStats getOperatorStats(QueryStats stats)
    {
        return stats.getOperatorSummaries()
                .stream()
                .filter(summary -> summary.getOperatorType().startsWith("Scan") || summary.getOperatorType().startsWith("TableScan"))
                .collect(onlyElement());
    }
}
