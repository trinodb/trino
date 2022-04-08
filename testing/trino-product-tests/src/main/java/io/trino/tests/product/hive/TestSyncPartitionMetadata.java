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

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert;
import io.trino.tempto.fulfillment.table.hive.HiveDataSource;
import io.trino.tempto.hadoop.hdfs.HdfsClient;
import io.trino.tempto.internal.hadoop.hdfs.HdfsDataSourceWriter;
import io.trino.tempto.query.QueryResult;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.fulfillment.table.hive.InlineDataSource.createResourceDataSource;
import static io.trino.tests.product.TestGroups.HIVE_PARTITIONING;
import static io.trino.tests.product.TestGroups.SMOKE;
import static io.trino.tests.product.TestGroups.TRINO_JDBC;
import static io.trino.tests.product.hive.HiveProductTest.ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE;
import static io.trino.tests.product.hive.HiveProductTest.ERROR_COMMITTING_WRITE_TO_HIVE_MATCH;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSyncPartitionMetadata
        extends ProductTest
{
    @Inject
    @Named("databases.hive.warehouse_directory_path")
    private String warehouseDirectory;

    @Inject
    private HdfsClient hdfsClient;

    @Inject
    private HdfsDataSourceWriter hdfsDataSourceWriter;

    @Test(groups = {HIVE_PARTITIONING, SMOKE, TRINO_JDBC})
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testAddPartition()
    {
        String tableName = "test_sync_partition_metadata_add_partition";
        prepare(hdfsClient, hdfsDataSourceWriter, tableName);

        onTrino().executeQuery("CALL system.sync_partition_metadata('default', '" + tableName + "', 'ADD')");
        assertPartitions(tableName, row("a", "1"), row("b", "2"), row("f", "9"));
        assertQueryFailure(() -> onTrino().executeQuery("SELECT payload, col_x, col_y FROM " + tableName + " ORDER BY 1, 2, 3 ASC"))
                .hasMessageContaining(format("Partition location does not exist: hdfs://hadoop-master:9000%s/%s/col_x=b/col_y=2", warehouseDirectory, tableName));
        cleanup(tableName);
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE, TRINO_JDBC})
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testAddPartitionContainingCharactersThatNeedUrlEncoding()
    {
        String tableName = "test_sync_partition_metadata_add_partition_urlencode";
        onTrino().executeQuery("DROP TABLE IF EXISTS " + tableName);

        onTrino().executeQuery("CREATE TABLE " + tableName + " (views bigint, col_issue varchar) WITH (format = 'ORC', partitioned_by = ARRAY['col_issue'])");
        onTrino().executeQuery("INSERT INTO " + tableName + " VALUES (1024, '#8120')");

        String tableLocation = tableLocation(tableName);
        // add partition directory col_issue=#9154 with single_int_column/data.orc file
        hdfsClient.createDirectory(tableLocation + "/col_issue=#9154");
        HiveDataSource dataSource = createResourceDataSource(tableName, "io/trino/tests/product/hive/data/single_int_column/data.orc");
        hdfsDataSourceWriter.ensureDataOnHdfs(tableLocation + "/col_issue=#9154", dataSource);

        QueryResult partitionListResult = onTrino().executeQuery("SELECT * FROM \"" + tableName + "$partitions\" ORDER BY 1");
        assertThat(partitionListResult).containsExactlyInOrder(row("#8120"));

        onTrino().executeQuery("CALL system.sync_partition_metadata('default', '" + tableName + "', 'ADD')");

        partitionListResult = onTrino().executeQuery("SELECT * FROM \"" + tableName + "$partitions\" ORDER BY 1");
        assertThat(partitionListResult).containsExactlyInOrder(row("#8120"), row("#9154"));

        cleanup(tableName);
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE})
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testDropPartition()
    {
        String tableName = "test_sync_partition_metadata_drop_partition";
        prepare(hdfsClient, hdfsDataSourceWriter, tableName);

        onTrino().executeQuery("CALL system.sync_partition_metadata('default', '" + tableName + "', 'DROP')");
        assertPartitions(tableName, row("a", "1"));
        assertData(tableName, row(1, "a", "1"));

        cleanup(tableName);
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE, TRINO_JDBC})
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testDropPartitionContainingCharactersThatNeedUrlEncoding()
    {
        String tableName = "test_sync_partition_metadata_drop_partition_urlencode";
        onTrino().executeQuery("DROP TABLE IF EXISTS " + tableName);

        onTrino().executeQuery("CREATE TABLE " + tableName + " (views bigint, col_issue varchar) WITH (format = 'ORC', partitioned_by = ARRAY['col_issue'])");
        onTrino().executeQuery("INSERT INTO " + tableName + " VALUES (1024, '#8120'), (2048, '#9154')");

        String tableLocation = tableLocation(tableName);
        // Delete partition directory col_issue=#8120 . Note that the partition directory location is sent to Web HDFS REST API and needs to be url encoded.
        hdfsClient.delete(tableLocation + "/col_issue=%238120");

        QueryResult partitionListResult = onTrino().executeQuery("SELECT * FROM \"" + tableName + "$partitions\" ORDER BY 1");
        assertThat(partitionListResult).containsExactlyInOrder(row("#8120"), row("#9154"));

        onTrino().executeQuery("CALL system.sync_partition_metadata('default', '" + tableName + "', 'DROP')");

        partitionListResult = onTrino().executeQuery("SELECT * FROM \"" + tableName + "$partitions\" ORDER BY 1");
        assertThat(partitionListResult).containsExactlyInOrder(row("#9154"));

        cleanup(tableName);
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE})
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testFullSyncPartition()
    {
        String tableName = "test_sync_partition_metadata_add_drop_partition";
        prepare(hdfsClient, hdfsDataSourceWriter, tableName);

        onTrino().executeQuery("CALL system.sync_partition_metadata('default', '" + tableName + "', 'FULL')");
        assertPartitions(tableName, row("a", "1"), row("f", "9"));
        assertData(tableName, row(1, "a", "1"), row(42, "f", "9"));

        cleanup(tableName);
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE, TRINO_JDBC})
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testInvalidSyncMode()
    {
        String tableName = "test_repair_invalid_mode";
        prepare(hdfsClient, hdfsDataSourceWriter, tableName);

        assertQueryFailure(() -> onTrino().executeQuery("CALL system.sync_partition_metadata('default', '" + tableName + "', 'INVALID')"))
                .hasMessageMatching("Query failed (.*): Invalid partition metadata sync mode: INVALID");

        cleanup(tableName);
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE})
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testMixedCasePartitionNames()
    {
        String tableName = "test_sync_partition_mixed_case";
        prepare(hdfsClient, hdfsDataSourceWriter, tableName);
        String tableLocation = tableLocation(tableName);
        HiveDataSource dataSource = createResourceDataSource(tableName, "io/trino/tests/product/hive/data/single_int_column/data.orc");
        hdfsDataSourceWriter.ensureDataOnHdfs(tableLocation + "/col_x=h/col_Y=11", dataSource);
        hdfsClient.createDirectory(tableLocation + "/COL_X=UPPER/COL_Y=12");
        hdfsDataSourceWriter.ensureDataOnHdfs(tableLocation + "/COL_X=UPPER/COL_Y=12", dataSource);

        onTrino().executeQuery("CALL system.sync_partition_metadata('default', '" + tableName + "', 'FULL', false)");
        assertPartitions(tableName, row("UPPER", "12"), row("a", "1"), row("f", "9"), row("g", "10"), row("h", "11"));
        assertData(tableName, row(1, "a", "1"), row(42, "UPPER", "12"), row(42, "f", "9"), row(42, "g", "10"), row(42, "h", "11"));
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE})
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testConflictingMixedCasePartitionNames()
    {
        String tableName = "test_sync_partition_mixed_case";
        prepare(hdfsClient, hdfsDataSourceWriter, tableName);
        HiveDataSource dataSource = createResourceDataSource(tableName, "io/trino/tests/product/hive/data/single_int_column/data.orc");
        // this conflicts with a partition that already exits in the metastore
        hdfsDataSourceWriter.ensureDataOnHdfs(tableLocation(tableName) + "/COL_X=a/cOl_y=1", dataSource);

        assertThatThrownBy(() -> onTrino().executeQuery("CALL system.sync_partition_metadata('default', '" + tableName + "', 'ADD', false)"))
                .hasMessageContaining(format("One or more partitions already exist for table 'default.%s'", tableName));
        assertPartitions(tableName, row("a", "1"), row("b", "2"));
    }

    private String tableLocation(String tableName)
    {
        return warehouseDirectory + '/' + tableName;
    }

    private void prepare(HdfsClient hdfsClient, HdfsDataSourceWriter hdfsDataSourceWriter, String tableName)
    {
        onTrino().executeQuery("DROP TABLE IF EXISTS " + tableName);

        onTrino().executeQuery("CREATE TABLE " + tableName + " (payload bigint, col_x varchar, col_y varchar) WITH (format = 'ORC', partitioned_by = ARRAY[ 'col_x', 'col_y' ])");
        onTrino().executeQuery("INSERT INTO " + tableName + " VALUES (1, 'a', '1'), (2, 'b', '2')");

        String tableLocation = tableLocation(tableName);
        // remove partition col_x=b/col_y=2
        hdfsClient.delete(tableLocation + "/col_x=b/col_y=2");
        // add partition directory col_x=f/col_y=9 with single_int_column/data.orc file
        hdfsClient.createDirectory(tableLocation + "/col_x=f/col_y=9");
        HiveDataSource dataSource = createResourceDataSource(tableName, "io/trino/tests/product/hive/data/single_int_column/data.orc");
        hdfsDataSourceWriter.ensureDataOnHdfs(tableLocation + "/col_x=f/col_y=9", dataSource);
        // should only be picked up when not in case sensitive mode
        hdfsClient.createDirectory(tableLocation + "/COL_X=g/col_y=10");
        hdfsDataSourceWriter.ensureDataOnHdfs(tableLocation + "/COL_X=g/col_y=10", dataSource);

        // add invalid partition path
        hdfsClient.createDirectory(tableLocation + "/col_x=d");
        hdfsClient.createDirectory(tableLocation + "/col_y=3/col_x=h");
        hdfsClient.createDirectory(tableLocation + "/col_y=3");
        hdfsClient.createDirectory(tableLocation + "/xyz");

        assertPartitions(tableName, row("a", "1"), row("b", "2"));
    }

    private static void cleanup(String tableName)
    {
        onTrino().executeQuery("DROP TABLE " + tableName);
    }

    private static void assertPartitions(String tableName, QueryAssert.Row... rows)
    {
        QueryResult partitionListResult = onTrino().executeQuery("SELECT * FROM \"" + tableName + "$partitions\" ORDER BY 1, 2");
        assertThat(partitionListResult).containsExactlyInOrder(rows);
    }

    private static void assertData(String tableName, QueryAssert.Row... rows)
    {
        QueryResult dataResult = onTrino().executeQuery("SELECT payload, col_x, col_y FROM " + tableName + " ORDER BY 1, 2, 3 ASC");
        assertThat(dataResult).containsExactlyInOrder(rows);
    }
}
