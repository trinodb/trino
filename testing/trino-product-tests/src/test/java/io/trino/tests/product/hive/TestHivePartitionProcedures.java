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

import io.trino.testing.containers.HdfsClient;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for Hive partition procedures (register_partition, unregister_partition).
 * <p>
 * Ported from the Tempto-based TestHivePartitionProcedures.
 */
@ProductTest
@RequiresEnvironment(HiveTransactionalEnvironment.class)
@TestGroup.HiveTransactional
class TestHivePartitionProcedures
{
    private static final String OUTSIDE_TABLES_DIRECTORY_PATH = "/user/hive/dangling";
    private static final Pattern ACID_LOCATION_PATTERN = Pattern.compile("(.*)/delta_[^/]+");

    @Test
    @Flaky(issue = "RETRYABLE_FAILURES_ISSUES", match = "RETRYABLE_FAILURES_MATCH")
    void testUnregisterPartition(HiveTransactionalEnvironment env)
    {
        String tableName = "first_table_" + randomNameSuffix();
        try {
            createPartitionedTable(env, tableName);

            assertThat(getTableCount(env, tableName)).isEqualTo(3L);
            assertThat(getPartitionValues(env, tableName)).containsOnly("a", "b", "c");

            dropPartition(env, tableName, "col", "a");

            assertThat(getTableCount(env, tableName)).isEqualTo(2L);
            assertThat(getPartitionValues(env, tableName)).containsOnly("b", "c");

            // should not drop data
            HdfsClient hdfsClient = env.createHdfsClient();
            assertThat(hdfsClient.exist(getTablePath(env, tableName, 1) + "/col=a/")).isTrue();
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    @Flaky(issue = "RETRYABLE_FAILURES_ISSUES", match = "RETRYABLE_FAILURES_MATCH")
    void testUnregisterViewTableShouldFail(HiveTransactionalEnvironment env)
    {
        String tableName = "first_table_" + randomNameSuffix();
        String viewName = "view_table_" + randomNameSuffix();
        try {
            createPartitionedTable(env, tableName);
            createView(env, viewName, tableName);

            assertThatThrownBy(() -> dropPartition(env, viewName, "col", "a"))
                    .hasMessageContaining("Table is a view: default." + viewName);
        }
        finally {
            env.executeTrinoUpdate("DROP VIEW IF EXISTS hive.default." + viewName);
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    @Flaky(issue = "RETRYABLE_FAILURES_ISSUES", match = "RETRYABLE_FAILURES_MATCH")
    void testUnregisterMissingTableShouldFail(HiveTransactionalEnvironment env)
    {
        String tableName = "first_table_" + randomNameSuffix();
        try {
            createPartitionedTable(env, tableName);

            assertThatThrownBy(() -> dropPartition(env, "missing_table", "col", "f"))
                    .hasMessageContaining("Table 'default.missing_table' not found");
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    @Flaky(issue = "RETRYABLE_FAILURES_ISSUES", match = "RETRYABLE_FAILURES_MATCH")
    void testUnregisterUnpartitionedTableShouldFail(HiveTransactionalEnvironment env)
    {
        String tableName = "second_table_" + randomNameSuffix();
        try {
            createUnpartitionedTable(env, tableName);

            assertThatThrownBy(() -> dropPartition(env, tableName, "col", "a"))
                    .hasMessageContaining("Table is not partitioned: default." + tableName);
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    @Flaky(issue = "RETRYABLE_FAILURES_ISSUES", match = "RETRYABLE_FAILURES_MATCH")
    void testUnregisterInvalidPartitionColumnsShouldFail(HiveTransactionalEnvironment env)
    {
        String tableName = "first_table_" + randomNameSuffix();
        try {
            createPartitionedTable(env, tableName);

            assertThatThrownBy(() -> dropPartition(env, tableName, "not_existing_partition_col", "a"))
                    .hasMessageContaining("Provided partition column names do not match actual partition column names: [col]");
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    @Flaky(issue = "RETRYABLE_FAILURES_ISSUES", match = "RETRYABLE_FAILURES_MATCH")
    void testUnregisterMissingPartitionShouldFail(HiveTransactionalEnvironment env)
    {
        String tableName = "first_table_" + randomNameSuffix();
        try {
            createPartitionedTable(env, tableName);

            assertThatThrownBy(() -> dropPartition(env, tableName, "col", "f"))
                    .hasMessageContaining("Partition 'col=f' does not exist");
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    @Flaky(issue = "RETRYABLE_FAILURES_ISSUES", match = "RETRYABLE_FAILURES_MATCH")
    void testRegisterPartitionMissingTableShouldFail(HiveTransactionalEnvironment env)
    {
        assertThatThrownBy(() -> addPartition(env, "missing_table", "col", "f", "/"))
                .hasMessageContaining("Table 'default.missing_table' not found");
    }

    @Test
    @Flaky(issue = "RETRYABLE_FAILURES_ISSUES", match = "RETRYABLE_FAILURES_MATCH")
    void testRegisterUnpartitionedTableShouldFail(HiveTransactionalEnvironment env)
    {
        String tableName = "second_table_" + randomNameSuffix();
        try {
            createUnpartitionedTable(env, tableName);

            assertThatThrownBy(() -> addPartition(env, tableName, "col", "a", "/"))
                    .hasMessageContaining("Table is not partitioned: default." + tableName);
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    @Flaky(issue = "RETRYABLE_FAILURES_ISSUES", match = "RETRYABLE_FAILURES_MATCH")
    void testRegisterViewTableShouldFail(HiveTransactionalEnvironment env)
    {
        String tableName = "first_table_" + randomNameSuffix();
        String viewName = "view_table_" + randomNameSuffix();
        try {
            createPartitionedTable(env, tableName);
            createView(env, viewName, tableName);

            assertThatThrownBy(() -> addPartition(env, viewName, "col", "a", "/"))
                    .hasMessageContaining("Table is a view: default." + viewName);
        }
        finally {
            env.executeTrinoUpdate("DROP VIEW IF EXISTS hive.default." + viewName);
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    @Flaky(issue = "RETRYABLE_FAILURES_ISSUES", match = "RETRYABLE_FAILURES_MATCH")
    void testRegisterPartitionCollisionShouldFail(HiveTransactionalEnvironment env)
    {
        String tableName = "first_table_" + randomNameSuffix();
        try {
            createPartitionedTable(env, tableName);

            assertThatThrownBy(() -> addPartition(env, tableName, "col", "a", "/"))
                    .hasMessageContaining("Partition [col=a] is already registered");
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    @Flaky(issue = "RETRYABLE_FAILURES_ISSUES", match = "RETRYABLE_FAILURES_MATCH")
    void testRegisterPartitionInvalidPartitionColumnsShouldFail(HiveTransactionalEnvironment env)
    {
        String tableName = "first_table_" + randomNameSuffix();
        try {
            createPartitionedTable(env, tableName);

            assertThatThrownBy(() -> addPartition(env, tableName, "not_existing_partition_col", "a", "/"))
                    .hasMessageContaining("Provided partition column names do not match actual partition column names: [col]");
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    @Flaky(issue = "RETRYABLE_FAILURES_ISSUES", match = "RETRYABLE_FAILURES_MATCH")
    void testRegisterPartitionInvalidLocationShouldFail(HiveTransactionalEnvironment env)
    {
        String tableName = "first_table_" + randomNameSuffix();
        try {
            createPartitionedTable(env, tableName);

            assertThatThrownBy(() -> addPartition(env, tableName, "col", "f", "/some/non/existing/path"))
                    .hasMessageContaining("Partition location does not exist: /some/non/existing/path");
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    @Flaky(issue = "RETRYABLE_FAILURES_ISSUES", match = "RETRYABLE_FAILURES_MATCH")
    void testRegisterPartitionWithDefaultPartitionLocation(HiveTransactionalEnvironment env)
    {
        String tableName = "first_table_" + randomNameSuffix();
        try {
            createPartitionedTable(env, tableName);
            dropPartition(env, tableName, "col", "a");
            dropPartition(env, tableName, "col", "c");

            assertThat(getTableCount(env, tableName)).isEqualTo(1L);
            assertThat(getPartitionValues(env, tableName)).containsOnly("b");

            // Re-register partition using it's default location
            addPartition(env, tableName, "col", "c");

            assertThat(getTableCount(env, tableName)).isEqualTo(2L);
            assertThat(getPartitionValues(env, tableName)).containsOnly("b", "c");
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    @Flaky(issue = "RETRYABLE_FAILURES_ISSUES", match = "RETRYABLE_FAILURES_MATCH")
    void testRegisterPartition(HiveTransactionalEnvironment env)
    {
        String firstTable = "first_table_" + randomNameSuffix();
        String secondTable = "second_table_" + randomNameSuffix();
        try {
            createPartitionedTable(env, firstTable);
            createPartitionedTable(env, secondTable);

            assertThat(getPartitionValues(env, firstTable)).containsOnly("a", "b", "c");

            env.executeTrinoUpdate(format("INSERT INTO hive.default.%s (val, col) VALUES (10, 'f')", secondTable));
            assertThat(getPartitionValues(env, secondTable)).containsOnly("a", "b", "c", "f");

            // Move partition f from secondTable to firstTable
            addPartition(env, firstTable, "col", "f", getTablePath(env, secondTable, 1) + "/col=f");
            dropPartition(env, secondTable, "col", "f");

            assertThat(getPartitionValues(env, secondTable)).containsOnly("a", "b", "c");
            assertThat(getPartitionValues(env, firstTable)).containsOnly("a", "b", "c", "f");
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + firstTable);
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + secondTable);
        }
    }

    @Test
    @Flaky(issue = "RETRYABLE_FAILURES_ISSUES", match = "RETRYABLE_FAILURES_MATCH")
    void testRegisterPartitionFromAnyLocation(HiveTransactionalEnvironment env)
    {
        String tableName = "first_table_" + randomNameSuffix();
        String danglingPath = OUTSIDE_TABLES_DIRECTORY_PATH + "_" + randomNameSuffix();
        try {
            createPartitionedTable(env, tableName);
            createDanglingLocationWithData(env, danglingPath);

            assertThat(getPartitionValues(env, tableName)).containsOnly("a", "b", "c");
            addPartition(env, tableName, "col", "f", danglingPath);

            assertThat(getPartitionValues(env, tableName)).containsOnly("a", "b", "c", "f");
            assertThat(getValues(env, tableName)).containsOnly(1, 2, 3, 42);

            dropPartition(env, tableName, "col", "f");

            assertThat(getPartitionValues(env, tableName)).containsOnly("a", "b", "c");
            assertThat(getValues(env, tableName)).containsOnly(1, 2, 3);
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
            // Clean up the dangling directory
            try {
                env.createHdfsClient().delete(danglingPath);
            }
            catch (Exception e) {
                // Ignore cleanup errors
            }
        }
    }

    private QueryResult dropPartition(HiveTransactionalEnvironment env, String tableName, String partitionCol, String partition)
    {
        return env.executeTrino(format("CALL hive.system.unregister_partition(\n" +
                        "    schema_name => '%s',\n" +
                        "    table_name => '%s',\n" +
                        "    partition_columns => ARRAY['%s'],\n" +
                        "    partition_values => ARRAY['%s'])",
                "default", tableName, partitionCol, partition));
    }

    private QueryResult addPartition(HiveTransactionalEnvironment env, String tableName, String partitionCol, String partition, String location)
    {
        return env.executeTrino(format("CALL hive.system.register_partition(\n" +
                        "    schema_name => '%s',\n" +
                        "    table_name => '%s',\n" +
                        "    partition_columns => ARRAY['%s'],\n" +
                        "    partition_values => ARRAY['%s'],\n" +
                        "    location => '%s')",
                "default", tableName, partitionCol, partition, location));
    }

    private QueryResult addPartition(HiveTransactionalEnvironment env, String tableName, String partitionCol, String partition)
    {
        return env.executeTrino(format("CALL hive.system.register_partition(\n" +
                        "    schema_name => '%s',\n" +
                        "    table_name => '%s',\n" +
                        "    partition_columns => ARRAY['%s'],\n" +
                        "    partition_values => ARRAY['%s'])",
                "default", tableName, partitionCol, partition));
    }

    private void createDanglingLocationWithData(HiveTransactionalEnvironment env, String path)
    {
        HdfsClient hdfsClient = env.createHdfsClient();
        hdfsClient.createDirectory(path);
        // Write a simple text file with the value "42" (matching the original test data)
        hdfsClient.saveFile(path + "/data.textfile", "42\n");
    }

    private void createPartitionedTable(HiveTransactionalEnvironment env, String tableName)
    {
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);

        env.executeTrinoUpdate("CREATE TABLE hive.default." + tableName + " (val int, col varchar) WITH (format = 'TEXTFILE', partitioned_by = ARRAY['col'])");
        env.executeTrinoUpdate("INSERT INTO hive.default." + tableName + " VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    }

    private void createView(HiveTransactionalEnvironment env, String viewName, String tableName)
    {
        env.executeTrinoUpdate("DROP VIEW IF EXISTS hive.default." + viewName);
        env.executeTrinoUpdate(format("CREATE VIEW hive.default.%s AS SELECT val, col FROM hive.default.%s", viewName, tableName));
    }

    private void createUnpartitionedTable(HiveTransactionalEnvironment env, String tableName)
    {
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);

        env.executeTrinoUpdate("CREATE TABLE hive.default." + tableName + " (val int, col varchar) WITH (format = 'TEXTFILE')");
        env.executeTrinoUpdate("INSERT INTO hive.default." + tableName + " VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    }

    private Long getTableCount(HiveTransactionalEnvironment env, String tableName)
    {
        return (Long) env.executeTrino("SELECT count(*) FROM hive.default." + tableName).getOnlyValue();
    }

    private Set<String> getPartitionValues(HiveTransactionalEnvironment env, String tableName)
    {
        return env.executeTrino("SELECT col FROM hive.default." + tableName).rows().stream()
                .map(row -> row.get(0))
                .map(String.class::cast)
                .collect(Collectors.toSet());
    }

    private Set<Integer> getValues(HiveTransactionalEnvironment env, String tableName)
    {
        return env.executeTrino("SELECT val FROM hive.default." + tableName).column(1).stream()
                .map(Integer.class::cast)
                .collect(toImmutableSet());
    }

    private String getTableLocation(HiveTransactionalEnvironment env, String tableName, int partitionColumns)
    {
        StringBuilder regex = new StringBuilder("/[^/]*$");
        for (int i = 0; i < partitionColumns; i++) {
            regex.insert(0, "/[^/]*");
        }
        String tableLocation = (String) env.executeTrino(
                format("SELECT DISTINCT regexp_replace(\"$path\", '%s', '') FROM hive.default.%s", regex, tableName))
                .getOnlyValue();

        // trim the /delta_... suffix for ACID tables
        Matcher acidLocationMatcher = ACID_LOCATION_PATTERN.matcher(tableLocation);
        if (acidLocationMatcher.matches()) {
            tableLocation = acidLocationMatcher.group(1);
        }
        return tableLocation;
    }

    private String getTablePath(HiveTransactionalEnvironment env, String tableName, int partitionColumns)
    {
        String location = getTableLocation(env, tableName, partitionColumns);
        return URI.create(location).getPath();
    }
}
