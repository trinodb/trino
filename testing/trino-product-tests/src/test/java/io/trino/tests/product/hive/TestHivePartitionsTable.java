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

import com.google.common.math.IntMath;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.math.RoundingMode;
import java.util.Optional;
import java.util.stream.IntStream;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for Hive partitions table ($partitions system table).
 * <p>
 * Ported from the Tempto-based TestHivePartitionsTable.
 */
@ProductTest
@RequiresEnvironment(HiveBasicEnvironment.class)
@TestGroup.HdfsNoImpersonation
class TestHivePartitionsTable
{
    // We use hive.max-partitions-per-scan=100 in tests, so this many partitions is too many
    private static final int TOO_MANY_PARTITIONS = 105;

    private static final String RETRYABLE_FAILURES_ISSUES = "RETRYABLE_FAILURES_ISSUES";
    private static final String RETRYABLE_FAILURES_MATCH = "RETRYABLE_FAILURES_MATCH";

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testShowPartitionsFromHiveTable(HiveBasicEnvironment env)
    {
        String tableName = "partitioned_table_" + randomNameSuffix();
        try {
            createPartitionedTable(env, tableName);
            String partitionsTable = "\"" + tableName + "$partitions\"";

            QueryResult partitionListResult;

            partitionListResult = env.executeTrino("SELECT * FROM hive.default." + partitionsTable);
            assertThat(partitionListResult).containsExactlyInOrder(row(1), row(2));
            assertColumnNames(partitionListResult, "part_col");

            partitionListResult = env.executeTrino(format("SELECT * FROM hive.default.%s WHERE part_col = 1", partitionsTable));
            assertThat(partitionListResult).containsExactlyInOrder(row(1));
            assertColumnNames(partitionListResult, "part_col");

            assertThatThrownBy(() -> env.executeTrino(format("SELECT * FROM hive.default.%s WHERE no_such_column = 1", partitionsTable)))
                    .hasMessageContaining("Column 'no_such_column' cannot be resolved");
            assertThatThrownBy(() -> env.executeTrino(format("SELECT * FROM hive.default.%s WHERE col = 1", partitionsTable)))
                    .hasMessageContaining("Column 'col' cannot be resolved");
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testShowPartitionsFromUnpartitionedTable(HiveBasicEnvironment env)
    {
        String tableName = "unpartitioned_table_" + randomNameSuffix();
        try {
            createUnpartitionedTable(env, tableName);
            assertThatThrownBy(() -> env.executeTrino("SELECT * FROM hive.default.\"" + tableName + "$partitions\""))
                    .hasMessageMatching(".*Table 'hive.default.\"" + tableName + "\\$partitions\"' does not exist");
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testShowPartitionsFromHiveTableWithTooManyPartitions(HiveBasicEnvironment env)
    {
        String tableName = "partitioned_table_many_partitions_" + randomNameSuffix();
        String partitionsTable = "\"" + tableName + "$partitions\"";
        try {
            createPartitionedTableWithVariablePartitions(env, tableName);
            createPartitions(env, tableName, TOO_MANY_PARTITIONS);

            // Verify we created enough partitions for the test to be meaningful
            assertThatThrownBy(() -> env.executeTrino("SELECT * FROM hive.default." + tableName))
                    .hasMessageMatching(".*: Query over table '\\S+' can potentially read more than \\d+ partitions");

            QueryResult partitionListResult;

            partitionListResult = env.executeTrino(format("SELECT * FROM hive.default.%s WHERE part_col < 7", partitionsTable));
            assertThat(partitionListResult).containsExactlyInOrder(row(0), row(1), row(2), row(3), row(4), row(5), row(6));
            assertColumnNames(partitionListResult, "part_col");

            partitionListResult = env.executeTrino(format("SELECT a.part_col FROM (SELECT * FROM hive.default.%s WHERE part_col = 1) a, (SELECT * FROM hive.default.%s WHERE part_col = 1) b WHERE a.col = b.col", tableName, tableName));
            assertThat(partitionListResult).containsExactlyInOrder(row(1));

            partitionListResult = env.executeTrino(format("SELECT * FROM hive.default.%s WHERE part_col < -10", partitionsTable));
            assertThat(partitionListResult).hasNoRows();

            partitionListResult = env.executeTrino(format("SELECT * FROM hive.default.%s ORDER BY part_col LIMIT 7", partitionsTable));
            assertThat(partitionListResult).containsExactlyInOrder(row(0), row(1), row(2), row(3), row(4), row(5), row(6));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    private void createPartitions(HiveBasicEnvironment env, String tableName, int partitionsToCreate)
    {
        // This is done in tests rather than as a requirement, because TableRequirements.immutableTable cannot be partitioned
        // and mutable table is recreated before every test (and this takes a lot of time).

        int maxPartitionsAtOnce = 100;

        IntStream.range(0, IntMath.divide(partitionsToCreate, maxPartitionsAtOnce, RoundingMode.UP))
                .forEach(batch -> {
                    int rangeStart = batch * maxPartitionsAtOnce;
                    int rangeEndInclusive = Math.min((batch + 1) * maxPartitionsAtOnce, partitionsToCreate) - 1;
                    env.executeTrinoUpdate(format(
                            "INSERT INTO hive.default.%s (part_col, col) " +
                                    "SELECT CAST(id AS integer), 42 FROM UNNEST (sequence(%s, %s)) AS u(id)",
                            tableName,
                            rangeStart,
                            rangeEndInclusive));
                });
    }

    private void createPartitionedTable(HiveBasicEnvironment env, String tableName)
    {
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        env.executeTrinoUpdate(
                "CREATE TABLE hive.default." + tableName + " (col INTEGER, part_col INTEGER) " +
                        "WITH (format = 'ORC', partitioned_by = ARRAY['part_col'])");
        // Insert data into partitions
        env.executeTrinoUpdate("INSERT INTO hive.default." + tableName + " VALUES (1, 1), (2, 2)");
    }

    private void createPartitionedTableWithVariablePartitions(HiveBasicEnvironment env, String tableName)
    {
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        env.executeTrinoUpdate(
                "CREATE TABLE hive.default." + tableName + " (col INTEGER, part_col INTEGER) " +
                        "WITH (format = 'ORC', partitioned_by = ARRAY['part_col'])");
    }

    private void createUnpartitionedTable(HiveBasicEnvironment env, String tableName)
    {
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        env.executeTrinoUpdate(
                "CREATE TABLE hive.default." + tableName + " (col INTEGER) " +
                        "WITH (format = 'ORC')");
    }

    private static void assertColumnNames(QueryResult queryResult, String... columnNames)
    {
        for (int i = 0; i < columnNames.length; i++) {
            Optional<Integer> columnIndex = tryFindColumnIndex(queryResult, columnNames[i]);
            assertThat(columnIndex)
                    .as("Index of column " + columnNames[i])
                    .isEqualTo(Optional.of(i + 1));
        }
        assertThat(queryResult.getColumnCount()).isEqualTo(columnNames.length);
    }

    private static Optional<Integer> tryFindColumnIndex(QueryResult queryResult, String columnName)
    {
        int index = queryResult.getColumnNames().indexOf(columnName);
        if (index == -1) {
            return Optional.empty();
        }
        return Optional.of(index + 1); // 1-indexed
    }
}
