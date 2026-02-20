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
import io.trino.testing.containers.environment.Row;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.io.Resources.getResource;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_ISSUES;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_MATCH;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for Hive sync_partition_metadata procedure on HDFS.
 * <p>
 * Ported from the Tempto-based TestHdfsSyncPartitionMetadata.
 */
@ProductTest
@RequiresEnvironment(HiveBasicEnvironment.class)
@TestGroup.HdfsNoImpersonation
class TestHdfsSyncPartitionMetadata
{
    private static final Pattern ACID_LOCATION_PATTERN = Pattern.compile("(.*)/delta_[^/]+");

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testAddPartition(HiveBasicEnvironment env)
    {
        String tableName = "test_sync_partition_metadata_add_partition";
        prepare(env, tableName);

        env.executeTrinoUpdate("CALL hive.system.sync_partition_metadata('default', '" + tableName + "', 'ADD')");
        assertPartitions(env, tableName, row("a", "1"), row("b", "2"), row("f", "9"));
        assertThatThrownBy(() -> env.executeTrino("SELECT payload, col_x, col_y FROM hive.default." + tableName + " ORDER BY 1, 2, 3 ASC"))
                .hasMessageMatching(format(".*Partition location does not exist: .*%s/col_x=b/col_y=2", tableName));
        cleanup(env, tableName);
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testAddPartitionContainingCharactersThatNeedUrlEncoding(HiveBasicEnvironment env)
    {
        String tableName = "test_sync_partition_metadata_add_partition_urlencode";
        String mirrorTableName = "test_sync_partition_metadata_add_partition_urlencode_mirror";
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + mirrorTableName);

        env.executeTrinoUpdate(format("" +
                        "CREATE TABLE hive.default.%s (payload bigint, col_date varchar, col_time varchar)" +
                        "WITH (format = 'ORC', partitioned_by = ARRAY[ 'col_date', 'col_time' ])",
                tableName));
        env.executeTrinoUpdate("INSERT INTO hive.default." + tableName + " VALUES (1024, '2022-02-01', '19:00:15'), (1024, '2022-01-17', '20:00:12')");
        String sharedTableLocation = getTableLocation(env, "hive.default." + tableName, 2);
        // avoid dealing with the intricacies of adding content on the file system level
        // and possibly url encoding the file path by using
        // an external table which mirrors the previously created table
        env.executeTrinoUpdate(format("" +
                        "CREATE TABLE hive.default.%s (payload bigint, col_date varchar, col_time varchar)" +
                        "WITH (external_location = '%s', format = 'ORC', partitioned_by = ARRAY[ 'col_date', 'col_time' ])",
                mirrorTableName,
                sharedTableLocation));
        env.executeTrinoUpdate("CALL hive.system.sync_partition_metadata('default', '" + mirrorTableName + "', 'ADD')");

        assertPartitions(env, tableName, row("2022-01-17", "20:00:12"), row("2022-02-01", "19:00:15"));
        assertPartitions(env, mirrorTableName, row("2022-01-17", "20:00:12"), row("2022-02-01", "19:00:15"));

        env.executeTrinoUpdate("INSERT INTO hive.default." + tableName + " VALUES (2048, '2022-04-04', '16:59:13')");
        assertPartitions(env, tableName, row("2022-01-17", "20:00:12"), row("2022-02-01", "19:00:15"), row("2022-04-04", "16:59:13"));
        assertPartitions(env, mirrorTableName, row("2022-01-17", "20:00:12"), row("2022-02-01", "19:00:15"));

        env.executeTrinoUpdate("CALL hive.system.sync_partition_metadata('default', '" + mirrorTableName + "', 'ADD')");
        assertPartitions(env, mirrorTableName, row("2022-01-17", "20:00:12"), row("2022-02-01", "19:00:15"), row("2022-04-04", "16:59:13"));

        cleanup(env, mirrorTableName);
        cleanup(env, tableName);
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testDropPartition(HiveBasicEnvironment env)
    {
        String tableName = "test_sync_partition_metadata_drop_partition";
        prepare(env, tableName);

        env.executeTrinoUpdate("CALL hive.system.sync_partition_metadata('default', '" + tableName + "', 'DROP')");
        assertPartitions(env, tableName, row("a", "1"));
        assertData(env, tableName, row(1, "a", "1"));

        cleanup(env, tableName);
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testDropPartitionContainingCharactersThatNeedUrlEncoding(HiveBasicEnvironment env)
    {
        String tableName = "test_sync_partition_metadata_drop_partition_urlencode";
        String mirrorTableName = "test_sync_partition_metadata_drop_partition_urlencode_mirror";
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + mirrorTableName);

        env.executeTrinoUpdate(format("" +
                        "CREATE TABLE hive.default.%s (payload bigint, col_date varchar, col_time varchar)" +
                        "WITH (format = 'ORC', partitioned_by = ARRAY[ 'col_date', 'col_time' ])",
                tableName));
        env.executeTrinoUpdate("INSERT INTO hive.default." + tableName + " VALUES (1024, '2022-01-17', '20:00:12') , (4096, '2022-01-18', '10:40:16')");

        // avoid dealing with the intricacies of adding/removing content on the file system level
        // and possibly url encoding the file path by using
        // an external table which mirrors the previously created table
        String sharedTableLocation = getTableLocation(env, "hive.default." + tableName, 2);
        env.executeTrinoUpdate(format("" +
                        "CREATE TABLE hive.default.%s (payload bigint, col_date varchar, col_time varchar)" +
                        "WITH (external_location = '%s', format = 'ORC', partitioned_by = ARRAY[ 'col_date', 'col_time' ])",
                mirrorTableName,
                sharedTableLocation));
        env.executeTrinoUpdate("CALL hive.system.sync_partition_metadata('default', '" + mirrorTableName + "', 'ADD')");

        assertPartitions(env, tableName, row("2022-01-17", "20:00:12"), row("2022-01-18", "10:40:16"));
        assertPartitions(env, mirrorTableName, row("2022-01-17", "20:00:12"), row("2022-01-18", "10:40:16"));

        // remove a partition from the shared table location
        env.executeTrinoUpdate("DELETE FROM hive.default." + tableName + " WHERE col_date = '2022-01-17' AND col_time='20:00:12'");

        assertPartitions(env, tableName, row("2022-01-18", "10:40:16"));
        assertPartitions(env, mirrorTableName, row("2022-01-17", "20:00:12"), row("2022-01-18", "10:40:16"));

        env.executeTrinoUpdate("CALL hive.system.sync_partition_metadata('default', '" + mirrorTableName + "', 'DROP')");
        assertPartitions(env, mirrorTableName, row("2022-01-18", "10:40:16"));

        cleanup(env, mirrorTableName);
        cleanup(env, tableName);
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testFullSyncPartition(HiveBasicEnvironment env)
    {
        String tableName = "test_sync_partition_metadata_add_drop_partition";
        prepare(env, tableName);

        env.executeTrinoUpdate("CALL hive.system.sync_partition_metadata('default', '" + tableName + "', 'FULL')");
        assertPartitions(env, tableName, row("a", "1"), row("f", "9"));
        assertData(env, tableName, row(1, "a", "1"), row(42, "f", "9"));

        cleanup(env, tableName);
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testInvalidSyncMode(HiveBasicEnvironment env)
    {
        String tableName = "test_repair_invalid_mode";
        prepare(env, tableName);

        assertThatThrownBy(() -> env.executeTrino("CALL hive.system.sync_partition_metadata('default', '" + tableName + "', 'INVALID')"))
                .hasMessageMatching("Query failed \\(.*\\): Invalid partition metadata sync mode: INVALID");

        cleanup(env, tableName);
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testMixedCasePartitionNames(HiveBasicEnvironment env)
            throws IOException
    {
        String tableName = "test_sync_partition_mixed_case";
        prepare(env, tableName);
        String tableLocation = tableLocation(env, tableName);
        HdfsClient hdfsClient = env.createHdfsClient();

        hdfsClient.createDirectory(format("%s/col_x=h/col_Y=11", tableLocation));
        copyOrcFileToHdfsDirectory(hdfsClient, format("%s/col_x=h/col_Y=11", tableLocation));

        hdfsClient.createDirectory(format("%s/COL_X=UPPER/COL_Y=12", tableLocation));
        copyOrcFileToHdfsDirectory(hdfsClient, format("%s/COL_X=UPPER/COL_Y=12", tableLocation));

        env.executeTrinoUpdate("CALL hive.system.sync_partition_metadata('default', '" + tableName + "', 'FULL', false)");
        assertPartitions(env, tableName, row("UPPER", "12"), row("a", "1"), row("f", "9"), row("g", "10"), row("h", "11"));
        assertData(env, tableName, row(1, "a", "1"), row(42, "UPPER", "12"), row(42, "f", "9"), row(42, "g", "10"), row(42, "h", "11"));
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testConflictingMixedCasePartitionNames(HiveBasicEnvironment env)
            throws IOException
    {
        String tableName = "test_sync_partition_mixed_case";
        String tableLocation = tableLocation(env, tableName);
        prepare(env, tableName);
        HdfsClient hdfsClient = env.createHdfsClient();

        // this conflicts with a partition that already exits in the metastore
        hdfsClient.createDirectory(format("%s/COL_X=a/cOl_y=1", tableLocation));
        copyOrcFileToHdfsDirectory(hdfsClient, format("%s/COL_X=a/cOl_y=1", tableLocation));

        assertThatThrownBy(() -> env.executeTrino("CALL hive.system.sync_partition_metadata('default', '" + tableName + "', 'ADD', false)"))
                .hasMessageContaining(format("One or more partitions already exist for table 'default.%s'", tableName));
        assertPartitions(env, tableName, row("a", "1"), row("b", "2"));
    }

    @Test
    void testSyncPartitionMetadataWithNullArgument(HiveBasicEnvironment env)
    {
        assertThatThrownBy(() -> env.executeTrino("CALL hive.system.sync_partition_metadata(NULL, 'page_views', 'ADD')"))
                .hasMessageMatching(".*schema_name cannot be null.*");
        assertThatThrownBy(() -> env.executeTrino("CALL hive.system.sync_partition_metadata('web', NULl, 'ADD')"))
                .hasMessageMatching(".*table_name cannot be null.*");
        assertThatThrownBy(() -> env.executeTrino("CALL hive.system.sync_partition_metadata('web', 'page_views', NULL)"))
                .hasMessageMatching(".*mode cannot be null.*");
    }

    @Test
    void testAddNonConventionalHivePartition(HiveBasicEnvironment env)
            throws IOException
    {
        String tableName = "test_sync_partition_metadata_add_partition_nonconventional";
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);

        String warehouseDirectory = env.getWarehouseDirectory();
        String tableLocation = warehouseDirectory + "/" + tableName;
        HdfsClient hdfsClient = env.createHdfsClient();
        hdfsClient.createDirectory(tableLocation);
        env.executeTrinoUpdate(format("" +
                        "CREATE TABLE hive.default.%s (payload bigint, col_x varchar, col_y varchar) " +
                        "WITH (external_location = '%s', format = 'ORC', partitioned_by = ARRAY[ 'col_x', 'col_y' ])",
                tableName, tableLocation));

        env.executeHiveUpdate("INSERT INTO " + tableName + " VALUES (1024, '10', '1'), (2048, '20', '11')");
        String unconventionalPartitionLocation = warehouseDirectory + "/unconventionalpartition";
        hdfsClient.createDirectory(unconventionalPartitionLocation);
        copyOrcFileToHdfsDirectory(hdfsClient, unconventionalPartitionLocation);
        env.executeHiveUpdate("ALTER TABLE %s ADD PARTITION (col_x = '30', col_y = '31') LOCATION '%s'".formatted(tableName, unconventionalPartitionLocation));
        assertPartitions(env, tableName, row("10", "1"), row("20", "11"), row("30", "31"));

        // Dropping an external table will not drop its contents
        cleanup(env, tableName);

        env.executeTrinoUpdate(format("" +
                        "CREATE TABLE hive.default.%s (payload bigint, col_x varchar, col_y varchar) " +
                        "WITH (external_location = '%s', format = 'ORC', partitioned_by = ARRAY[ 'col_x', 'col_y' ])",
                tableName, tableLocation));

        env.executeTrinoUpdate("CALL hive.system.sync_partition_metadata('default', '" + tableName + "', 'FULL')");
        assertPartitions(env, tableName, row("10", "1"), row("20", "11"));

        env.executeTrinoUpdate("CALL hive.system.sync_partition_metadata('default', '" + tableName + "', 'FULL')");
        assertPartitions(env, tableName, row("10", "1"), row("20", "11"));

        hdfsClient.delete(unconventionalPartitionLocation);
        cleanup(env, tableName);
        hdfsClient.delete(tableLocation);
    }

    private String schemaLocation(HiveBasicEnvironment env)
    {
        return env.getWarehouseDirectory();
    }

    private String tableLocation(HiveBasicEnvironment env, String tableName)
    {
        return schemaLocation(env) + '/' + tableName;
    }

    private void prepare(HiveBasicEnvironment env, String tableName)
    {
        String tableLocation = tableLocation(env, tableName);
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        HdfsClient hdfsClient = env.createHdfsClient();
        hdfsClient.delete(tableLocation);

        createTable(env, tableName);
        env.executeTrinoUpdate("INSERT INTO hive.default." + tableName + " VALUES (1, 'a', '1'), (2, 'b', '2')");

        try {
            // remove partition col_x=b/col_y=2
            hdfsClient.delete(format("%s/col_x=b/col_y=2", tableLocation));
            // add partition directory col_x=f/col_y=9 with single_int_column/data.orc file
            hdfsClient.createDirectory(format("%s/col_x=f/col_y=9", tableLocation));
            copyOrcFileToHdfsDirectory(hdfsClient, format("%s/col_x=f/col_y=9", tableLocation));

            // should only be picked up when not in case sensitive mode
            hdfsClient.createDirectory(format("%s/COL_X=g/col_y=10", tableLocation));
            copyOrcFileToHdfsDirectory(hdfsClient, format("%s/COL_X=g/col_y=10", tableLocation));

            // add invalid partition path
            hdfsClient.createDirectory(format("%s/col_x=d", tableLocation));
            hdfsClient.createDirectory(format("%s/col_y=3/col_x=h", tableLocation));
            hdfsClient.createDirectory(format("%s/col_y=3", tableLocation));
            hdfsClient.createDirectory(format("%s/xyz", tableLocation));
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to prepare test data on HDFS", e);
        }

        assertPartitions(env, tableName, row("a", "1"), row("b", "2"));
    }

    private void createTable(HiveBasicEnvironment env, String tableName)
    {
        env.executeTrinoUpdate("CREATE TABLE hive.default." + tableName + " (payload bigint, col_x varchar, col_y varchar) WITH (format = 'ORC', partitioned_by = ARRAY[ 'col_x', 'col_y' ])");
    }

    private static void cleanup(HiveBasicEnvironment env, String tableName)
    {
        env.executeTrinoUpdate("DROP TABLE hive.default." + tableName);
    }

    private static void assertPartitions(HiveBasicEnvironment env, String tableName, Row... rows)
    {
        QueryResult partitionListResult = env.executeTrino("SELECT * FROM hive.default.\"" + tableName + "$partitions\" ORDER BY 1, 2");
        assertThat(partitionListResult).containsExactlyInOrder(rows);
    }

    private static void assertData(HiveBasicEnvironment env, String tableName, Row... rows)
    {
        QueryResult dataResult = env.executeTrino("SELECT payload, col_x, col_y FROM hive.default." + tableName + " ORDER BY 1, 2, 3 ASC");
        assertThat(dataResult).containsExactlyInOrder(rows);
    }

    private void copyOrcFileToHdfsDirectory(HdfsClient hdfsClient, String targetDirectory)
            throws IOException
    {
        String targetPath = targetDirectory + "/data.orc";
        try (InputStream inputStream = getResource(Path.of("io/trino/tests/product/hive/data/single_int_column/data.orc").toString()).openStream()) {
            byte[] content = inputStream.readAllBytes();
            hdfsClient.saveFile(targetPath, content);
        }
    }

    private String getTableLocation(HiveBasicEnvironment env, String tableName, int partitionColumns)
    {
        StringBuilder regex = new StringBuilder("/[^/]*$");
        for (int i = 0; i < partitionColumns; i++) {
            regex.insert(0, "/[^/]*");
        }
        String tableLocation = (String) env.executeTrino(
                        format("SELECT DISTINCT regexp_replace(\"$path\", '%s', '') FROM %s", regex, tableName))
                .getOnlyValue();

        // trim the /delta_... suffix for ACID tables
        Matcher acidLocationMatcher = ACID_LOCATION_PATTERN.matcher(tableLocation);
        if (acidLocationMatcher.matches()) {
            tableLocation = acidLocationMatcher.group(1);
        }
        return tableLocation;
    }

    private String getTablePath(HiveBasicEnvironment env, String tableName, int partitionColumns)
    {
        String location = getTableLocation(env, tableName, partitionColumns);
        return URI.create(location).getPath();
    }
}
