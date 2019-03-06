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
package io.prestosql.tests.hive;

import com.google.inject.Inject;
import io.prestosql.tempto.ProductTest;
import io.prestosql.tempto.assertions.QueryAssert;
import io.prestosql.tempto.fulfillment.table.hive.HiveDataSource;
import io.prestosql.tempto.hadoop.hdfs.HdfsClient;
import io.prestosql.tempto.internal.hadoop.hdfs.HdfsDataSourceWriter;
import io.prestosql.tempto.query.QueryResult;
import org.testng.annotations.Test;

import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.fulfillment.table.hive.InlineDataSource.createResourceDataSource;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.TestGroups.HIVE_PARTITIONING;
import static io.prestosql.tests.TestGroups.SMOKE;

public class TestSyncPartitionMetadata
        extends ProductTest
{
    private static final String WAREHOUSE_DIRECTORY_PATH = "/user/hive/warehouse/";

    @Inject
    private HdfsClient hdfsClient;

    @Inject
    private HdfsDataSourceWriter hdfsDataSourceWriter;

    @Test(groups = {HIVE_PARTITIONING, SMOKE})
    public void testAddPartition()
    {
        String tableName = "test_sync_partition_metadata_add_partition";
        prepare(hdfsClient, hdfsDataSourceWriter, tableName);

        query("CALL system.sync_partition_metadata('default', '" + tableName + "', 'ADD')");
        assertPartitions(tableName, row("a", "1"), row("b", "2"), row("f", "9"));
        assertThat(() -> query("SELECT payload, x, y FROM " + tableName + " ORDER BY 1, 2, 3 ASC"))
                .failsWithMessage("Partition location does not exist: hdfs://hadoop-master:9000/user/hive/warehouse/" + tableName + "/x=b/y=2");
        cleanup(tableName);
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE})
    public void testDropPartition()
    {
        String tableName = "test_sync_partition_metadata_drop_partition";
        prepare(hdfsClient, hdfsDataSourceWriter, tableName);

        query("CALL system.sync_partition_metadata('default', '" + tableName + "', 'DROP')");
        assertPartitions(tableName, row("a", "1"));
        assertData(tableName, row(1, "a", "1"));

        cleanup(tableName);
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE})
    public void testFullSyncPartition()
    {
        String tableName = "test_sync_partition_metadata_add_drop_partition";
        prepare(hdfsClient, hdfsDataSourceWriter, tableName);

        query("CALL system.sync_partition_metadata('default', '" + tableName + "', 'FULL')");
        assertPartitions(tableName, row("a", "1"), row("f", "9"));
        assertData(tableName, row(1, "a", "1"), row(42, "f", "9"));

        cleanup(tableName);
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE})
    public void testInvalidSyncMode()
    {
        String tableName = "test_repair_invalid_mode";
        prepare(hdfsClient, hdfsDataSourceWriter, tableName);

        assertThat(() -> query("CALL system.sync_partition_metadata('default', '" + tableName + "', 'INVALID')"))
                .failsWithMessageMatching("java.sql.SQLException: Query failed (.*): Invalid partition metadata sync mode: INVALID");

        cleanup(tableName);
    }

    private static void prepare(HdfsClient hdfsClient, HdfsDataSourceWriter hdfsDataSourceWriter, String tableName)
    {
        query("DROP TABLE IF EXISTS " + tableName);

        query("CREATE TABLE " + tableName + " (payload bigint, x varchar, y varchar) WITH (format = 'ORC', partitioned_by = ARRAY[ 'x', 'y' ])");
        query("INSERT INTO " + tableName + " VALUES (1, 'a', '1'), (2, 'b', '2')");

        String tableLocation = WAREHOUSE_DIRECTORY_PATH + tableName;
        // remove partition x=b/y=2
        hdfsClient.delete(tableLocation + "/x=b/y=2");
        // add partition directory x=f/y=9 with single_int_column/data.orc file
        hdfsClient.createDirectory(tableLocation + "/x=f/y=9");
        HiveDataSource dataSource = createResourceDataSource(tableName, "io/prestosql/tests/hive/data/single_int_column/data.orc");
        hdfsDataSourceWriter.ensureDataOnHdfs(tableLocation + "/x=f/y=9", dataSource);

        // add invalid partition path
        hdfsClient.createDirectory(tableLocation + "/x=d");
        hdfsClient.createDirectory(tableLocation + "/y=3/x=h");
        hdfsClient.createDirectory(tableLocation + "/y=3");
        hdfsClient.createDirectory(tableLocation + "/xyz");

        assertPartitions(tableName, row("a", "1"), row("b", "2"));
    }

    private static void cleanup(String tableName)
    {
        query("DROP TABLE " + tableName);
    }

    private static void assertPartitions(String tableName, QueryAssert.Row... rows)
    {
        QueryResult partitionListResult = query("SELECT * FROM \"" + tableName + "$partitions\"");
        assertThat(partitionListResult).containsExactly(rows);
    }

    private static void assertData(String tableName, QueryAssert.Row... rows)
    {
        QueryResult dataResult = query("SELECT payload, x, y FROM " + tableName + " ORDER BY 1, 2, 3 ASC");
        assertThat(dataResult).containsExactly(rows);
    }
}
