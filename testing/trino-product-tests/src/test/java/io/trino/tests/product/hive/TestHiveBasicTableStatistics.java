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

import com.google.common.primitives.Longs;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Verify.verify;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_ISSUES;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_MATCH;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * JUnit 5 port of TestHiveBasicTableStatistics.
 * <p>
 * Tests basic table statistics functionality in Hive connector.
 */
@ProductTest
@RequiresEnvironment(HiveBasicEnvironment.class)
@TestGroup.HdfsNoImpersonation
@TestInstance(PER_CLASS)
class TestHiveBasicTableStatistics
{
    private static final String NATION_TABLE = "nation_for_stats_tests";

    @BeforeAll
    void setUp(HiveBasicEnvironment env)
    {
        // Create nation table from TPCH data (similar to ImmutableNationTable requirement)
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + NATION_TABLE);
        env.executeTrinoUpdate("CREATE TABLE hive.default." + NATION_TABLE + " AS SELECT * FROM hive.default.nation");
    }

    @Test
    void testCreateUnpartitioned(HiveBasicEnvironment env)
    {
        String tableName = "test_basic_statistics_unpartitioned_ctas_trino";

        env.executeTrinoUpdate(format("DROP TABLE IF EXISTS hive.default.%s", tableName));
        env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s AS SELECT * FROM hive.default.%s", tableName, NATION_TABLE));

        try {
            BasicStatistics statistics = getBasicStatisticsForTable(env, tableName);
            assertThatStatisticsAreNonZero(statistics);
            assertThat(statistics.getNumRows().getAsLong()).isEqualTo(25);
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE IF EXISTS hive.default.%s", tableName));
        }
    }

    @Test
    void testCreateExternalUnpartitioned(HiveBasicEnvironment env)
    {
        String tableName = "test_basic_statistics_external_unpartitioned_trino";

        env.executeTrinoUpdate(format("DROP TABLE IF EXISTS hive.default.%s", tableName));

        try {
            String location = getTableLocation(env, "hive.default." + NATION_TABLE);
            env.executeTrinoUpdate(format("" +
                            "CREATE TABLE hive.default.%s (" +
                            "   n_nationkey bigint, " +
                            "   n_regionkey bigint, " +
                            "   n_name varchar(25), " +
                            "   n_comment varchar(152)) " +
                            "WITH (external_location = '%s', format = 'TEXTFILE', textfile_field_separator = '|')",
                    tableName,
                    location));
            BasicStatistics statistics = getBasicStatisticsForTable(env, tableName);
            assertThatStatisticsAreNotPresent(statistics);
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE IF EXISTS hive.default.%s", tableName));
        }
    }

    @Test
    void testCreateTableWithNoData(HiveBasicEnvironment env)
    {
        String tableName = "test_basic_statistics_unpartitioned_ctas_trino_with_no_data";

        env.executeTrinoUpdate(format("DROP TABLE IF EXISTS hive.default.%s", tableName));
        env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s AS SELECT * FROM hive.default.%s WITH NO DATA", tableName, NATION_TABLE));

        try {
            BasicStatistics statistics = getBasicStatisticsForTable(env, tableName);
            assertThatStatisticsAreZero(statistics);
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE IF EXISTS hive.default.%s", tableName));
        }
    }

    @Test
    void testInsertUnpartitioned(HiveBasicEnvironment env)
    {
        String tableName = "test_basic_statistics_unpartitioned_insert_trino";

        env.executeTrinoUpdate(format("DROP TABLE IF EXISTS hive.default.%s", tableName));
        env.executeTrinoUpdate(format("" +
                "CREATE TABLE hive.default.%s (" +
                "   n_nationkey bigint, " +
                "   n_regionkey bigint, " +
                "   n_name varchar(25), " +
                "   n_comment varchar(152)" +
                ")", tableName));

        try {
            BasicStatistics statisticsEmpty = getBasicStatisticsForTable(env, tableName);
            assertThatStatisticsAreZero(statisticsEmpty);

            insertNationData(env, tableName);

            BasicStatistics statisticsFirstInsert = getBasicStatisticsForTable(env, tableName);
            assertThatStatisticsAreNonZero(statisticsFirstInsert);
            assertThat(statisticsFirstInsert.getNumRows().getAsLong()).isEqualTo(25);

            insertNationData(env, tableName);

            BasicStatistics statisticsSecondInsert = getBasicStatisticsForTable(env, tableName);
            assertThatStatisticsAreNonZero(statisticsSecondInsert);
            assertThatStatisticsValuesHaveIncreased(statisticsFirstInsert, statisticsSecondInsert);
            assertThat(statisticsSecondInsert.getNumRows().getAsLong()).isEqualTo(50);
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE IF EXISTS hive.default.%s", tableName));
        }
    }

    @Test
    void testCreatePartitioned(HiveBasicEnvironment env)
    {
        String tableName = "test_basic_statistics_partitioned_ctas_trino";

        env.executeTrinoUpdate(format("DROP TABLE IF EXISTS hive.default.%s", tableName));
        env.executeTrinoUpdate(format("" +
                "CREATE TABLE hive.default.%s " +
                "WITH (" +
                "   partitioned_by = ARRAY['n_regionkey'] " +
                ") " +
                "AS " +
                "SELECT n_nationkey, n_name, n_comment, n_regionkey " +
                "FROM hive.default.%s " +
                // turns out there are exactly 5 countries in each region
                // let's change records count for one of the regions to verify statistics are different
                "WHERE n_nationkey <> 23", tableName, NATION_TABLE));

        try {
            BasicStatistics tableStatistics = getBasicStatisticsForTable(env, tableName);
            // Metastore can auto-gather table statistics. This is not relevant for Trino, since we do not use table-level statistics in case of a partitioned table.
            if (tableStatistics.getNumRows().isEmpty()) {
                assertThatStatisticsAreNotPresent(tableStatistics);
            }

            BasicStatistics firstPartitionStatistics = getBasicStatisticsForPartition(env, tableName, "n_regionkey=1");
            assertThatStatisticsAreNonZero(firstPartitionStatistics);
            assertThat(firstPartitionStatistics.getNumRows().getAsLong()).isEqualTo(5);

            BasicStatistics secondPartitionStatistics = getBasicStatisticsForPartition(env, tableName, "n_regionkey=3");
            assertThatStatisticsAreNonZero(secondPartitionStatistics);
            assertThat(secondPartitionStatistics.getNumRows().getAsLong()).isEqualTo(4);
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE IF EXISTS hive.default.%s", tableName));
        }
    }

    @Test
    void testAnalyzePartitioned(HiveBasicEnvironment env)
    {
        String tableName = "test_basic_statistics_analyze_partitioned";

        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        env.executeTrinoUpdate(format("" +
                "CREATE TABLE hive.default.%s " +
                "WITH ( " +
                "   partitioned_by = ARRAY['n_regionkey'], " +
                "   bucketed_by = ARRAY['n_nationkey'], " +
                "   bucket_count = 10 " +
                ") " +
                "AS " +
                "SELECT n_nationkey, n_name, n_comment, n_regionkey " +
                "FROM hive.default.%s " +
                "WHERE n_regionkey = 1", tableName, NATION_TABLE));

        try {
            BasicStatistics tableStatistics = getBasicStatisticsForTable(env, tableName);
            // Metastore can auto-gather table statistics. This is not relevant for Trino, since we do not use table-level statistics in case of a partitioned table.
            if (tableStatistics.getNumRows().isEmpty()) {
                assertThatStatisticsAreNotPresent(tableStatistics);
            }

            BasicStatistics partitionStatisticsBefore = getBasicStatisticsForPartition(env, tableName, "n_regionkey=1");
            assertThatStatisticsArePresent(partitionStatisticsBefore);

            // run ANALYZE
            env.executeTrinoUpdate(format("ANALYZE hive.default.%s", tableName));
            BasicStatistics partitionStatisticsAfter = getBasicStatisticsForPartition(env, tableName, "n_regionkey=1");

            assertThat(partitionStatisticsAfter.getNumRows()).isEqualTo(partitionStatisticsBefore.getNumRows());
            assertThat(partitionStatisticsAfter.getNumFiles()).isEqualTo(partitionStatisticsBefore.getNumFiles());
            assertThat(partitionStatisticsAfter.getRawDataSize()).isEmpty();
            assertThat(partitionStatisticsAfter.getTotalSize()).isEqualTo(partitionStatisticsBefore.getTotalSize());
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE hive.default.%s", tableName));
        }
    }

    @Test
    void testAnalyzeUnpartitioned(HiveBasicEnvironment env)
    {
        String tableName = "test_basic_statistics_analyze_unpartitioned";

        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        env.executeTrinoUpdate(format("" +
                "CREATE TABLE hive.default.%s " +
                "AS " +
                "SELECT n_nationkey, n_name, n_comment, n_regionkey " +
                "FROM hive.default.%s " +
                "WHERE n_regionkey = 1", tableName, NATION_TABLE));

        try {
            BasicStatistics tableStatisticsBefore = getBasicStatisticsForTable(env, tableName);
            assertThatStatisticsArePresent(tableStatisticsBefore);

            // run ANALYZE
            env.executeTrinoUpdate(format("ANALYZE hive.default.%s", tableName));
            BasicStatistics tableStatisticsAfter = getBasicStatisticsForTable(env, tableName);

            // ANALYZE will clear all basic stats except for the number of rows, which should be unchanged since no data has been added
            assertThat(tableStatisticsAfter.getNumRows()).isEqualTo(tableStatisticsBefore.getNumRows());
            assertThat(tableStatisticsAfter.getNumFiles()).isEmpty();
            assertThat(tableStatisticsAfter.getRawDataSize()).isEmpty();
            assertThat(tableStatisticsAfter.getTotalSize()).isEmpty();
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE hive.default.%s", tableName));
        }
    }

    @Test
    void testInsertPartitioned(HiveBasicEnvironment env)
    {
        String tableName = "test_basic_statistics_partitioned_insert_trino";

        env.executeTrinoUpdate(format("DROP TABLE IF EXISTS hive.default.%s", tableName));
        env.executeTrinoUpdate(format("" +
                "CREATE TABLE hive.default.%s (" +
                "   n_nationkey bigint, " +
                "   n_name varchar(25), " +
                "   n_comment varchar(152), " +
                "   n_regionkey bigint " +
                ")" +
                "WITH (" +
                "   partitioned_by = ARRAY['n_regionkey'] " +
                ") ", tableName));

        try {
            BasicStatistics tableStatistics = getBasicStatisticsForTable(env, tableName);
            // Metastore can auto-gather table statistics. This is not relevant for Trino, since we do not use table-level statistics in case of a partitioned table.
            if (tableStatistics.getNumRows().isEmpty()) {
                assertThatStatisticsAreNotPresent(tableStatistics);
            }

            insertNationData(env, tableName);

            BasicStatistics partitionStatisticsFirstInsert = getBasicStatisticsForPartition(env, tableName, "n_regionkey=3");
            assertThatStatisticsAreNonZero(partitionStatisticsFirstInsert);
            assertThat(partitionStatisticsFirstInsert.getNumRows().getAsLong()).isEqualTo(5);

            insertNationData(env, tableName);

            BasicStatistics statisticsSecondInsert = getBasicStatisticsForPartition(env, tableName, "n_regionkey=3");
            assertThat(statisticsSecondInsert.getNumRows().getAsLong()).isEqualTo(10);
            assertThatStatisticsValuesHaveIncreased(partitionStatisticsFirstInsert, statisticsSecondInsert);
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE IF EXISTS hive.default.%s", tableName));
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testInsertBucketed(HiveBasicEnvironment env)
    {
        String tableName = "test_basic_statistics_bucketed_insert_trino";

        env.executeTrinoUpdate(format("DROP TABLE IF EXISTS hive.default.%s", tableName));
        env.executeTrinoUpdate(format("" +
                "CREATE TABLE hive.default.%s " +
                "WITH ( " +
                "   bucketed_by = ARRAY['n_nationkey'], " +
                "   bucket_count = 50 " +
                ") " +
                "AS " +
                "SELECT n_nationkey, n_name, n_comment, n_regionkey " +
                "FROM hive.default.%s", tableName, NATION_TABLE));

        try {
            BasicStatistics statisticsAfterCreate = getBasicStatisticsForTable(env, tableName);
            assertThatStatisticsAreNonZero(statisticsAfterCreate);
            assertThat(statisticsAfterCreate.getNumRows().getAsLong()).isEqualTo(25);
            assertThat(statisticsAfterCreate.getNumFiles().getAsLong()).isEqualTo(25); // no files for empty buckets

            insertNationData(env, tableName);

            BasicStatistics statisticsAfterInsert = getBasicStatisticsForTable(env, tableName);
            assertThat(statisticsAfterInsert.getNumRows().getAsLong()).isEqualTo(50);
            assertThat(statisticsAfterInsert.getNumFiles().getAsLong()).isEqualTo(50); // no files for empty buckets

            insertNationData(env, tableName);

            BasicStatistics statisticsAfterInsert2 = getBasicStatisticsForTable(env, tableName);
            assertThat(statisticsAfterInsert2.getNumRows().getAsLong()).isEqualTo(75);
            assertThat(statisticsAfterInsert2.getNumFiles().getAsLong()).isEqualTo(75); // no files for empty buckets
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE IF EXISTS hive.default.%s", tableName));
        }
    }

    @Test
    void testInsertBucketedPartitioned(HiveBasicEnvironment env)
    {
        String tableName = "test_basic_statistics_bucketed_partitioned_insert_trino";

        env.executeTrinoUpdate(format("DROP TABLE IF EXISTS hive.default.%s", tableName));
        env.executeTrinoUpdate(format("" +
                "CREATE TABLE hive.default.%s " +
                "WITH ( " +
                "   partitioned_by = ARRAY['n_regionkey'], " +
                "   bucketed_by = ARRAY['n_nationkey'], " +
                "   bucket_count = 10 " +
                ") " +
                "AS " +
                "SELECT n_nationkey, n_name, n_comment, n_regionkey " +
                "FROM hive.default.%s " +
                "WHERE n_regionkey = 1", tableName, NATION_TABLE));

        try {
            BasicStatistics tableStatistics = getBasicStatisticsForTable(env, tableName);
            // Metastore can auto-gather table statistics. This is not relevant for Trino, since we do not use table-level statistics in case of a partitioned table.
            if (tableStatistics.getNumRows().isEmpty()) {
                assertThatStatisticsAreNotPresent(tableStatistics);
            }

            BasicStatistics firstPartitionStatistics = getBasicStatisticsForPartition(env, tableName, "n_regionkey=1");
            assertThatStatisticsAreNonZero(firstPartitionStatistics);
            assertThat(firstPartitionStatistics.getNumRows().getAsLong()).isEqualTo(5);
            assertThat(firstPartitionStatistics.getNumFiles().getAsLong()).isEqualTo(5); // no files for empty buckets

            String insert = format("INSERT INTO hive.default.%s (n_nationkey, n_regionkey, n_name, n_comment) " +
                    "SELECT n_nationkey, n_regionkey, n_name, n_comment " +
                    "FROM hive.default.%s " +
                    "WHERE n_regionkey = 2", tableName, NATION_TABLE);

            env.executeTrinoUpdate(insert);

            BasicStatistics secondPartitionStatistics = getBasicStatisticsForPartition(env, tableName, "n_regionkey=2");
            assertThat(secondPartitionStatistics.getNumRows().getAsLong()).isEqualTo(5);
            assertThat(secondPartitionStatistics.getNumFiles().getAsLong()).isEqualTo(4); // no files for empty buckets

            env.executeTrinoUpdate(insert);

            BasicStatistics secondPartitionUpdatedStatistics = getBasicStatisticsForPartition(env, tableName, "n_regionkey=2");
            assertThat(secondPartitionUpdatedStatistics.getNumRows().getAsLong()).isEqualTo(10);
            assertThat(secondPartitionUpdatedStatistics.getNumFiles().getAsLong()).isEqualTo(8); // no files for empty buckets
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE IF EXISTS hive.default.%s", tableName));
        }
    }

    private void insertNationData(HiveBasicEnvironment env, String tableName)
    {
        env.executeTrinoUpdate(format("" +
                "INSERT INTO hive.default.%s (n_nationkey, n_regionkey, n_name, n_comment) " +
                "SELECT n_nationkey, n_regionkey, n_name, n_comment FROM hive.default.%s", tableName, NATION_TABLE));
    }

    private static void assertThatStatisticsAreNonZero(BasicStatistics statistics)
    {
        assertThatStatisticsArePresent(statistics);
        assertThat(statistics.getNumRows().getAsLong()).isGreaterThan(0);
        assertThat(statistics.getNumFiles().getAsLong()).isGreaterThan(0);
        assertThat(statistics.getRawDataSize().getAsLong()).isGreaterThan(0);
        assertThat(statistics.getTotalSize().getAsLong()).isGreaterThan(0);
    }

    private static void assertThatStatisticsAreZero(BasicStatistics statistics)
    {
        assertThatStatisticsArePresent(statistics);
        assertThat(statistics.getNumRows().getAsLong()).isEqualTo(0);
        assertThat(statistics.getNumFiles().getAsLong()).isEqualTo(0);
        assertThat(statistics.getRawDataSize().getAsLong()).isEqualTo(0);
        assertThat(statistics.getTotalSize().getAsLong()).isEqualTo(0);
    }

    private static void assertThatStatisticsArePresent(BasicStatistics statistics)
    {
        assertThat(statistics.getNumRows()).isPresent();
        assertThat(statistics.getNumFiles()).isPresent();
        assertThat(statistics.getRawDataSize()).isPresent();
        assertThat(statistics.getTotalSize()).isPresent();
    }

    private static void assertThatStatisticsAreNotPresent(BasicStatistics statistics)
    {
        assertThat(statistics.getNumRows()).isNotPresent();
        assertThat(statistics.getNumFiles()).isNotPresent();
        assertThat(statistics.getRawDataSize()).isNotPresent();
        assertThat(statistics.getTotalSize()).isNotPresent();
    }

    private static void assertThatStatisticsValuesHaveIncreased(BasicStatistics first, BasicStatistics second)
    {
        assertThat(second.getNumRows().getAsLong()).isGreaterThan(first.getNumRows().getAsLong());
        assertThat(second.getNumFiles().getAsLong()).isGreaterThan(first.getNumFiles().getAsLong());
        assertThat(second.getTotalSize().getAsLong()).isGreaterThan(first.getTotalSize().getAsLong());
        assertThat(second.getRawDataSize().getAsLong()).isGreaterThan(first.getRawDataSize().getAsLong());
    }

    private static String getTableLocation(HiveBasicEnvironment env, String tableName)
    {
        QueryResult result = env.executeTrino(format("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM %s", tableName));
        return (String) result.rows().get(0).get(0);
    }

    private static BasicStatistics getBasicStatisticsForTable(HiveBasicEnvironment env, String table)
    {
        QueryResult result = env.executeHive(format("DESCRIBE FORMATTED %s", table));
        return basicStatisticsFromDescribeResult(result);
    }

    private static BasicStatistics getBasicStatisticsForPartition(HiveBasicEnvironment env, String table, String partition)
    {
        QueryResult result = env.executeHive(format("DESCRIBE FORMATTED %s partition (%s)", table, partition));
        return basicStatisticsFromDescribeResult(result);
    }

    private static BasicStatistics basicStatisticsFromDescribeResult(QueryResult result)
    {
        OptionalLong numFiles = getTableParameterValue(result, "numFiles");
        OptionalLong numRows = getTableParameterValue(result, "numRows");
        OptionalLong rawDataSize = getTableParameterValue(result, "rawDataSize");
        OptionalLong totalSize = getTableParameterValue(result, "totalSize");
        return new BasicStatistics(numFiles, numRows, rawDataSize, totalSize);
    }

    private static OptionalLong getTableParameterValue(QueryResult describeResult, String key)
    {
        verify(describeResult.getColumnCount() == 3, "describe result is expected to have 3 columns");
        for (List<Object> row : describeResult.rows()) {
            Optional<String> parameterKey = Optional.ofNullable(row.get(1))
                    .map(Object::toString)
                    .map(String::trim);

            if (parameterKey.isPresent() && key.equals(parameterKey.get())) {
                return Optional.ofNullable(row.get(2))
                        .map(Object::toString)
                        .map(String::trim)
                        .map(TestHiveBasicTableStatistics::tryParse)
                        .orElse(OptionalLong.empty());
            }
        }
        return OptionalLong.empty();
    }

    private static OptionalLong tryParse(String value)
    {
        Long number = Longs.tryParse(value);
        if (number != null) {
            return OptionalLong.of(number);
        }
        return OptionalLong.empty();
    }

    private static class BasicStatistics
    {
        private final OptionalLong numFiles;
        private final OptionalLong numRows;
        private final OptionalLong rawDataSize;
        private final OptionalLong totalSize;

        public BasicStatistics(OptionalLong numFiles, OptionalLong numRows, OptionalLong rawDataSize, OptionalLong totalSize)
        {
            this.numFiles = requireNonNull(numFiles, "numFiles is null");
            this.numRows = requireNonNull(numRows, "numRows is null");
            this.rawDataSize = requireNonNull(rawDataSize, "rawDataSize is null");
            this.totalSize = requireNonNull(totalSize, "totalSize is null");
        }

        public OptionalLong getNumFiles()
        {
            return numFiles;
        }

        public OptionalLong getNumRows()
        {
            return numRows;
        }

        public OptionalLong getRawDataSize()
        {
            return rawDataSize;
        }

        public OptionalLong getTotalSize()
        {
            return totalSize;
        }
    }
}
