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
import io.trino.tempto.fulfillment.table.hive.HiveDataSource;
import io.trino.tempto.hadoop.hdfs.HdfsClient;
import io.trino.tempto.internal.hadoop.hdfs.HdfsDataSourceWriter;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.fulfillment.table.hive.InlineDataSource.createResourceDataSource;
import static io.trino.tests.product.TestGroups.SMOKE;
import static io.trino.tests.product.TestGroups.TRINO_JDBC;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_ISSUES;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_MATCH;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;

public class TestHdfsSyncPartitionMetadata
        extends BaseTestSyncPartitionMetadata
{
    @Inject
    @Named("databases.hive.warehouse_directory_path")
    private String warehouseDirectory;
    @Inject
    private HdfsClient hdfsClient;
    @Inject
    private HdfsDataSourceWriter hdfsDataSourceWriter;

    @Override
    protected String schemaLocation()
    {
        return warehouseDirectory;
    }

    @Test(groups = {SMOKE, TRINO_JDBC})
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    @Override
    public void testAddPartition()
    {
        super.testAddPartition();
    }

    @Test(groups = {SMOKE, TRINO_JDBC})
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    @Override
    public void testAddPartitionContainingCharactersThatNeedUrlEncoding()
    {
        super.testAddPartitionContainingCharactersThatNeedUrlEncoding();
    }

    @Test(groups = SMOKE)
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    @Override
    public void testDropPartition()
    {
        super.testDropPartition();
    }

    @Test(groups = {SMOKE, TRINO_JDBC})
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    @Override
    public void testDropPartitionContainingCharactersThatNeedUrlEncoding()
    {
        super.testDropPartitionContainingCharactersThatNeedUrlEncoding();
    }

    @Test(groups = SMOKE)
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    @Override
    public void testFullSyncPartition()
    {
        super.testFullSyncPartition();
    }

    @Test(groups = {SMOKE, TRINO_JDBC})
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    @Override
    public void testInvalidSyncMode()
    {
        super.testInvalidSyncMode();
    }

    @Test(groups = SMOKE)
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    @Override
    public void testMixedCasePartitionNames()
    {
        super.testMixedCasePartitionNames();
    }

    @Test(groups = SMOKE)
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    @Override
    public void testConflictingMixedCasePartitionNames()
    {
        super.testConflictingMixedCasePartitionNames();
    }

    @Test(groups = SMOKE)
    @Override
    public void testSyncPartitionMetadataWithNullArgument()
    {
        super.testSyncPartitionMetadataWithNullArgument();
    }

    @Test(groups = SMOKE)
    public void testAddNonConventionalHivePartition()
    {
        String tableName = "test_sync_partition_metadata_add_partition_nonconventional";
        onTrino().executeQuery("DROP TABLE IF EXISTS " + tableName);

        String tableLocation = tableLocation(tableName);
        makeHdfsDirectory(tableLocation);
        onTrino().executeQuery(format("" +
                        "CREATE TABLE %s (payload bigint, col_x varchar, col_y varchar) " +
                        "WITH (external_location = '%s', format = 'ORC', partitioned_by = ARRAY[ 'col_x', 'col_y' ])",
                tableName, tableLocation));

        onHive().executeQuery("INSERT INTO " + tableName + " VALUES (1024, '10', '1'), (2048, '20', '11')");
        String unconventionalPartitionLocation = schemaLocation() + "/unconventionalpartition";
        makeHdfsDirectory(unconventionalPartitionLocation);
        copyOrcFileToHdfsDirectory(tableName, unconventionalPartitionLocation);
        onHive().executeQuery("ALTER TABLE %s ADD PARTITION (col_x = '30', col_y = '31') LOCATION '%s'".formatted(tableName, unconventionalPartitionLocation));
        assertPartitions(tableName, row("10", "1"), row("20", "11"), row("30", "31"));

        // Dropping an external table will not drop its contents
        cleanup(tableName);

        onTrino().executeQuery(format("" +
                        "CREATE TABLE %s (payload bigint, col_x varchar, col_y varchar) " +
                        "WITH (external_location = '%s', format = 'ORC', partitioned_by = ARRAY[ 'col_x', 'col_y' ])",
                tableName, tableLocation));

        onTrino().executeQuery("CALL system.sync_partition_metadata('default', '" + tableName + "', 'FULL')");
        assertPartitions(tableName, row("10", "1"), row("20", "11"));

        onTrino().executeQuery("CALL system.sync_partition_metadata('default', '" + tableName + "', 'FULL')");
        assertPartitions(tableName, row("10", "1"), row("20", "11"));

        removeHdfsDirectory(unconventionalPartitionLocation);
        cleanup(tableName);
        removeHdfsDirectory(tableLocation);
    }

    @Override
    protected void removeHdfsDirectory(String path)
    {
        hdfsClient.delete(path);
    }

    @Override
    protected void makeHdfsDirectory(String path)
    {
        hdfsClient.createDirectory(path);
    }

    @Override
    protected void copyOrcFileToHdfsDirectory(String tableName, String targetDirectory)
    {
        HiveDataSource dataSource = createResourceDataSource(tableName, "io/trino/tests/product/hive/data/single_int_column/data.orc");
        hdfsDataSourceWriter.ensureDataOnHdfs(targetDirectory, dataSource);
    }

    @Override
    protected void createTable(String tableName, String tableLocation)
    {
        onTrino().executeQuery("CREATE TABLE " + tableName + " (payload bigint, col_x varchar, col_y varchar) WITH (format = 'ORC', partitioned_by = ARRAY[ 'col_x', 'col_y' ])");
    }
}
