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

import static io.trino.tempto.fulfillment.table.hive.InlineDataSource.createResourceDataSource;
import static io.trino.tests.product.TestGroups.HIVE_PARTITIONING;
import static io.trino.tests.product.TestGroups.SMOKE;
import static io.trino.tests.product.TestGroups.TRINO_JDBC;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_ISSUES;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_MATCH;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;

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

    @Test(groups = {HIVE_PARTITIONING, SMOKE, TRINO_JDBC})
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    @Override
    public void testAddPartition()
    {
        super.testAddPartition();
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE, TRINO_JDBC})
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    @Override
    public void testAddPartitionContainingCharactersThatNeedUrlEncoding()
    {
        super.testAddPartitionContainingCharactersThatNeedUrlEncoding();
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE})
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    @Override
    public void testDropPartition()
    {
        super.testDropPartition();
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE, TRINO_JDBC})
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    @Override
    public void testDropPartitionContainingCharactersThatNeedUrlEncoding()
    {
        super.testDropPartitionContainingCharactersThatNeedUrlEncoding();
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE})
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    @Override
    public void testFullSyncPartition()
    {
        super.testFullSyncPartition();
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE, TRINO_JDBC})
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    @Override
    public void testInvalidSyncMode()
    {
        super.testInvalidSyncMode();
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE})
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    @Override
    public void testMixedCasePartitionNames()
    {
        super.testMixedCasePartitionNames();
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE})
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    @Override
    public void testConflictingMixedCasePartitionNames()
    {
        super.testConflictingMixedCasePartitionNames();
    }

    @Test(groups = {HIVE_PARTITIONING, SMOKE})
    @Override
    public void testSyncPartitionMetadataWithNullArgument()
    {
        super.testSyncPartitionMetadataWithNullArgument();
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
