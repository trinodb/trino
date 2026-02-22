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

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import io.trino.tests.product.azure.AzureEnvironment;
import org.junit.jupiter.api.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_ISSUES;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_MATCH;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for Hive sync_partition_metadata procedure on ABFS.
 * <p>
 * Ported from the Tempto-based TestAbfsSyncPartitionMetadata.
 */
@ProductTest
@RequiresEnvironment(AzureEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.Azure
class TestAbfsSyncPartitionMetadata
{
    private static final Pattern ACID_LOCATION_PATTERN = Pattern.compile("(.*)/delta_[^/]+");

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testAddPartition(AzureEnvironment env)
    {
        String tableName = "test_sync_partition_metadata_add_partition";
        prepare(env, tableName);

        env.executeTrinoUpdate("CALL hive.system.sync_partition_metadata('" + schemaName(env) + "', '" + tableName + "', 'ADD')");
        assertPartitions(env, tableName, row("a", "1"), row("b", "2"), row("f", "9"));
        assertThatThrownBy(() -> env.executeTrino("SELECT payload, col_x, col_y FROM " + qualifiedTableName(env, tableName) + " ORDER BY 1, 2, 3 ASC"))
                .hasMessageContaining("Partition location does not exist: " + tableLocation(env, tableName) + "/col_x=b/col_y=2");
        cleanup(env, tableName);
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testAddPartitionContainingCharactersThatNeedUrlEncoding(AzureEnvironment env)
    {
        String tableName = "test_sync_partition_metadata_add_partition_urlencode";
        String mirrorTableName = "test_sync_partition_metadata_add_partition_urlencode_mirror";
        ensureSchemaExists(env);
        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + qualifiedTableName(env, tableName));
        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + qualifiedTableName(env, mirrorTableName));

        env.executeTrinoUpdate(format("" +
                        "CREATE TABLE %s (payload bigint, col_date varchar, col_time varchar)" +
                        "WITH (format = 'ORC', partitioned_by = ARRAY[ 'col_date', 'col_time' ])",
                qualifiedTableName(env, tableName)));
        env.executeTrinoUpdate("INSERT INTO " + qualifiedTableName(env, tableName) + " VALUES (1024, '2022-02-01', '19:00:15'), (1024, '2022-01-17', '20:00:12')");
        String sharedTableLocation = getTableLocation(env, qualifiedTableName(env, tableName), 2);
        env.executeTrinoUpdate(format("" +
                        "CREATE TABLE %s (payload bigint, col_date varchar, col_time varchar)" +
                        "WITH (external_location = '%s', format = 'ORC', partitioned_by = ARRAY[ 'col_date', 'col_time' ])",
                qualifiedTableName(env, mirrorTableName),
                sharedTableLocation));
        env.executeTrinoUpdate("CALL hive.system.sync_partition_metadata('" + schemaName(env) + "', '" + mirrorTableName + "', 'ADD')");

        assertPartitions(env, tableName, row("2022-01-17", "20:00:12"), row("2022-02-01", "19:00:15"));
        assertPartitions(env, mirrorTableName, row("2022-01-17", "20:00:12"), row("2022-02-01", "19:00:15"));

        env.executeTrinoUpdate("INSERT INTO " + qualifiedTableName(env, tableName) + " VALUES (2048, '2022-04-04', '16:59:13')");
        assertPartitions(env, tableName, row("2022-01-17", "20:00:12"), row("2022-02-01", "19:00:15"), row("2022-04-04", "16:59:13"));
        assertPartitions(env, mirrorTableName, row("2022-01-17", "20:00:12"), row("2022-02-01", "19:00:15"));

        env.executeTrinoUpdate("CALL hive.system.sync_partition_metadata('" + schemaName(env) + "', '" + mirrorTableName + "', 'ADD')");
        assertPartitions(env, mirrorTableName, row("2022-01-17", "20:00:12"), row("2022-02-01", "19:00:15"), row("2022-04-04", "16:59:13"));

        cleanup(env, mirrorTableName);
        cleanup(env, tableName);
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testDropPartition(AzureEnvironment env)
    {
        String tableName = "test_sync_partition_metadata_drop_partition";
        prepare(env, tableName);

        env.executeTrinoUpdate("CALL hive.system.sync_partition_metadata('" + schemaName(env) + "', '" + tableName + "', 'DROP')");
        assertPartitions(env, tableName, row("a", "1"));
        assertData(env, tableName, row(1, "a", "1"));

        cleanup(env, tableName);
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testDropPartitionContainingCharactersThatNeedUrlEncoding(AzureEnvironment env)
    {
        String tableName = "test_sync_partition_metadata_drop_partition_urlencode";
        String mirrorTableName = "test_sync_partition_metadata_drop_partition_urlencode_mirror";
        ensureSchemaExists(env);
        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + qualifiedTableName(env, tableName));
        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + qualifiedTableName(env, mirrorTableName));

        env.executeTrinoUpdate(format("" +
                        "CREATE TABLE %s (payload bigint, col_date varchar, col_time varchar)" +
                        "WITH (format = 'ORC', partitioned_by = ARRAY[ 'col_date', 'col_time' ])",
                qualifiedTableName(env, tableName)));
        env.executeTrinoUpdate("INSERT INTO " + qualifiedTableName(env, tableName) + " VALUES (1024, '2022-01-17', '20:00:12') , (4096, '2022-01-18', '10:40:16')");

        String sharedTableLocation = getTableLocation(env, qualifiedTableName(env, tableName), 2);
        env.executeTrinoUpdate(format("" +
                        "CREATE TABLE %s (payload bigint, col_date varchar, col_time varchar)" +
                        "WITH (external_location = '%s', format = 'ORC', partitioned_by = ARRAY[ 'col_date', 'col_time' ])",
                qualifiedTableName(env, mirrorTableName),
                sharedTableLocation));
        env.executeTrinoUpdate("CALL hive.system.sync_partition_metadata('" + schemaName(env) + "', '" + mirrorTableName + "', 'ADD')");

        assertPartitions(env, tableName, row("2022-01-17", "20:00:12"), row("2022-01-18", "10:40:16"));
        assertPartitions(env, mirrorTableName, row("2022-01-17", "20:00:12"), row("2022-01-18", "10:40:16"));

        env.executeTrinoUpdate("DELETE FROM " + qualifiedTableName(env, tableName) + " WHERE col_date = '2022-01-17' AND col_time='20:00:12'");

        assertPartitions(env, tableName, row("2022-01-18", "10:40:16"));
        assertPartitions(env, mirrorTableName, row("2022-01-17", "20:00:12"), row("2022-01-18", "10:40:16"));

        env.executeTrinoUpdate("CALL hive.system.sync_partition_metadata('" + schemaName(env) + "', '" + mirrorTableName + "', 'DROP')");
        assertPartitions(env, mirrorTableName, row("2022-01-18", "10:40:16"));

        cleanup(env, mirrorTableName);
        cleanup(env, tableName);
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testFullSyncPartition(AzureEnvironment env)
    {
        String tableName = "test_sync_partition_metadata_add_drop_partition";
        prepare(env, tableName);

        env.executeTrinoUpdate("CALL hive.system.sync_partition_metadata('" + schemaName(env) + "', '" + tableName + "', 'FULL')");
        assertPartitions(env, tableName, row("a", "1"), row("f", "9"));
        assertData(env, tableName, row(1, "a", "1"), row(42, "f", "9"));

        cleanup(env, tableName);
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testInvalidSyncMode(AzureEnvironment env)
    {
        String tableName = "test_repair_invalid_mode";
        prepare(env, tableName);

        assertThatThrownBy(() -> env.executeTrino("CALL hive.system.sync_partition_metadata('" + schemaName(env) + "', '" + tableName + "', 'INVALID')"))
                .hasMessageContaining("Invalid partition metadata sync mode: INVALID");

        cleanup(env, tableName);
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testMixedCasePartitionNames(AzureEnvironment env)
    {
        String tableName = "test_sync_partition_mixed_case";
        prepare(env, tableName);
        String tableLocation = tableLocation(env, tableName);

        makeDirectory(env, format("%s/col_x=h/col_Y=11", tableLocation));
        copyOrcFileToDirectory(env, format("%s/col_x=h/col_Y=11", tableLocation));

        makeDirectory(env, format("%s/COL_X=UPPER/COL_Y=12", tableLocation));
        copyOrcFileToDirectory(env, format("%s/COL_X=UPPER/COL_Y=12", tableLocation));

        env.executeTrinoUpdate("CALL hive.system.sync_partition_metadata('" + schemaName(env) + "', '" + tableName + "', 'FULL', false)");
        assertPartitions(env, tableName, row("UPPER", "12"), row("a", "1"), row("f", "9"), row("g", "10"), row("h", "11"));
        assertData(env, tableName, row(1, "a", "1"), row(42, "UPPER", "12"), row(42, "f", "9"), row(42, "g", "10"), row(42, "h", "11"));
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testConflictingMixedCasePartitionNames(AzureEnvironment env)
    {
        String tableName = "test_sync_partition_mixed_case";
        String tableLocation = tableLocation(env, tableName);
        prepare(env, tableName);

        makeDirectory(env, format("%s/COL_X=a/cOl_y=1", tableLocation));
        copyOrcFileToDirectory(env, format("%s/COL_X=a/cOl_y=1", tableLocation));

        assertThatThrownBy(() -> env.executeTrino("CALL hive.system.sync_partition_metadata('" + schemaName(env) + "', '" + tableName + "', 'ADD', false)"))
                .hasMessageContaining(format("One or more partitions already exist for table '%s.%s'", schemaName(env), tableName));
        assertPartitions(env, tableName, row("a", "1"), row("b", "2"));
    }

    @Test
    void testSyncPartitionMetadataWithNullArgument(AzureEnvironment env)
    {
        assertThatThrownBy(() -> env.executeTrino("CALL hive.system.sync_partition_metadata(NULL, 'page_views', 'ADD')"))
                .hasMessageMatching(".*schema_name cannot be null.*");
        assertThatThrownBy(() -> env.executeTrino("CALL hive.system.sync_partition_metadata('web', NULl, 'ADD')"))
                .hasMessageMatching(".*table_name cannot be null.*");
        assertThatThrownBy(() -> env.executeTrino("CALL hive.system.sync_partition_metadata('web', 'page_views', NULL)"))
                .hasMessageMatching(".*mode cannot be null.*");
    }

    private void prepare(AzureEnvironment env, String tableName)
    {
        ensureSchemaExists(env);
        String tableLocation = tableLocation(env, tableName);
        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + qualifiedTableName(env, tableName));
        removeDirectory(env, tableLocation);

        createTable(env, tableName);
        env.executeTrinoUpdate("INSERT INTO " + qualifiedTableName(env, tableName) + " VALUES (1, 'a', '1'), (2, 'b', '2')");

        removeDirectory(env, format("%s/col_x=b/col_y=2", tableLocation));
        makeDirectory(env, format("%s/col_x=f/col_y=9", tableLocation));
        copyOrcFileToDirectory(env, format("%s/col_x=f/col_y=9", tableLocation));

        makeDirectory(env, format("%s/COL_X=g/col_y=10", tableLocation));
        copyOrcFileToDirectory(env, format("%s/COL_X=g/col_y=10", tableLocation));

        makeDirectory(env, format("%s/col_x=d", tableLocation));
        makeDirectory(env, format("%s/col_y=3/col_x=h", tableLocation));
        makeDirectory(env, format("%s/col_y=3", tableLocation));
        makeDirectory(env, format("%s/xyz", tableLocation));

        assertPartitions(env, tableName, row("a", "1"), row("b", "2"));
    }

    private static void cleanup(AzureEnvironment env, String tableName)
    {
        env.executeTrinoUpdate("DROP TABLE " + qualifiedTableName(env, tableName));
    }

    private String tableLocation(AzureEnvironment env, String tableName)
    {
        return env.getSchemaLocation() + '/' + tableName;
    }

    private static void ensureSchemaExists(AzureEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive." + schemaName(env) + " WITH (location = '" + env.getSchemaLocation() + "')");
    }

    private void createTable(AzureEnvironment env, String tableName)
    {
        env.executeTrinoUpdate("CREATE TABLE " + qualifiedTableName(env, tableName) + " (payload bigint, col_x varchar, col_y varchar) WITH (format = 'ORC', partitioned_by = ARRAY[ 'col_x', 'col_y' ])");
    }

    private static String schemaName(AzureEnvironment env)
    {
        return env.getSchemaName();
    }

    private static String qualifiedTableName(AzureEnvironment env, String tableName)
    {
        return "hive." + schemaName(env) + "." + tableName;
    }

    private static void assertPartitions(AzureEnvironment env, String tableName, Row... rows)
    {
        QueryResult partitionListResult = env.executeTrino("SELECT * FROM hive." + schemaName(env) + ".\"" + tableName + "$partitions\" ORDER BY 1, 2");
        assertThat(partitionListResult).containsExactlyInOrder(rows);
    }

    private static void assertData(AzureEnvironment env, String tableName, Row... rows)
    {
        QueryResult dataResult = env.executeTrino("SELECT payload, col_x, col_y FROM " + qualifiedTableName(env, tableName) + " ORDER BY 1, 2, 3 ASC");
        assertThat(dataResult).containsExactlyInOrder(rows);
    }

    private static void removeDirectory(AzureEnvironment env, String path)
    {
        env.executeHadoopFsCommand("hadoop fs -rm -f -r '" + path + "'");
    }

    private static void makeDirectory(AzureEnvironment env, String path)
    {
        env.executeHadoopFsCommand("hadoop fs -mkdir -p '" + path + "'");
    }

    private void copyOrcFileToDirectory(AzureEnvironment env, String targetDirectory)
    {
        String tableName = "single_int_column_" + randomNameSuffix();
        env.executeTrinoUpdate("CREATE TABLE " + qualifiedTableName(env, tableName) + " (payload bigint) WITH (format = 'ORC')");
        try {
            env.executeTrinoUpdate("INSERT INTO " + qualifiedTableName(env, tableName) + " VALUES (42)");
            String orcFilePath = (String) env.executeTrino("SELECT \"$path\" FROM " + qualifiedTableName(env, tableName)).getOnlyValue();
            env.executeHadoopFsCommand(format("hadoop fs -cp '%s' '%s'", orcFilePath, targetDirectory));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + qualifiedTableName(env, tableName));
        }
    }

    private String getTableLocation(AzureEnvironment env, String tableName, int partitionColumns)
    {
        StringBuilder regex = new StringBuilder("/[^/]*$");
        for (int i = 0; i < partitionColumns; i++) {
            regex.insert(0, "/[^/]*");
        }
        String tableLocation = (String) env.executeTrino(
                        format("SELECT DISTINCT regexp_replace(\"$path\", '%s', '') FROM %s", regex, tableName))
                .getOnlyValue();

        Matcher acidLocationMatcher = ACID_LOCATION_PATTERN.matcher(tableLocation);
        if (acidLocationMatcher.matches()) {
            tableLocation = acidLocationMatcher.group(1);
        }
        return tableLocation;
    }
}
