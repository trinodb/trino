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
package io.trino.plugin.hive;

import alluxio.client.table.TableMasterClient;
import alluxio.conf.PropertyKey;
import io.trino.plugin.hive.metastore.HiveMetastoreConfig;
import io.trino.plugin.hive.metastore.alluxio.AlluxioHiveMetastore;
import io.trino.plugin.hive.metastore.alluxio.AlluxioHiveMetastoreConfig;
import io.trino.plugin.hive.metastore.alluxio.AlluxioMetastoreModule;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;

public class TestHiveAlluxioMetastore
        extends AbstractTestHive
{
    @Parameters({"test.alluxio.host", "test.alluxio.port"})
    @BeforeClass
    public void setup(String host, String port)
    {
        System.setProperty(PropertyKey.Name.SECURITY_LOGIN_USERNAME, "presto");
        System.setProperty(PropertyKey.Name.MASTER_HOSTNAME, host);
        HiveConfig hiveConfig = new HiveConfig()
                .setParquetTimeZone("UTC")
                .setRcfileTimeZone("UTC");

        AlluxioHiveMetastoreConfig alluxioConfig = new AlluxioHiveMetastoreConfig();
        alluxioConfig.setMasterAddress(host + ":" + port);
        TableMasterClient client = AlluxioMetastoreModule.createCatalogMasterClient(alluxioConfig);
        setup("default", hiveConfig, new AlluxioHiveMetastore(client, new HiveMetastoreConfig()), HDFS_ENVIRONMENT);
    }

    @Override
    public void testBucketSortedTables()
    {
        // Alluxio metastore does not support create operations
    }

    @Override
    public void testBucketedTableEvolution()
    {
        // Alluxio metastore does not support create/insert/update operations
    }

    @Override
    public void testBucketedSortedTableEvolution()
    {
        // Alluxio metastore does not support create/insert/update operations
    }

    @Override
    public void testBucketedTableValidation()
            throws Exception
    {
        // Alluxio metastore does not support create operations
    }

    @Override
    public void testBucketedTableEvolutionWithDifferentReadBucketCount()
    {
        // Alluxio metastore does not support create operations
    }

    @Override
    public void testEmptyOrcFile()
    {
        // Alluxio metastore does not support create operations
    }

    @Override
    public void testPerTransactionDirectoryListerCache()
    {
        // Alluxio metastore does not support create operations
    }

    // specifically disable so that expected exception on the superclass don't fail this test
    @Override
    @Test(enabled = false)
    public void testEmptyRcBinaryFile()
    {
        // Alluxio metastore does not support create operations
    }

    // specifically disable so that expected exception on the superclass don't fail this test
    @Override
    @Test(enabled = false)
    public void testEmptyRcTextFile()
    {
        // Alluxio metastore does not support create operations
    }

    // specifically disable so that expected exception on the superclass don't fail this test
    @Override
    @Test(enabled = false)
    public void testEmptySequenceFile()
    {
        // Alluxio metastore does not support create operations
    }

    @Override
    public void testEmptyTableCreation()
    {
        // Alluxio metastore does not support create operations
    }

    @Override
    public void testEmptyTextFile()
    {
        // Alluxio metastore does not support create operations
    }

    @Override
    public void testGetPartitions()
    {
        // Alluxio metastore treats null comment as empty comment
    }

    @Override
    public void testGetPartitionsWithBindings()
    {
        // Alluxio metastore treats null comment as empty comment
    }

    @Override
    public void testGetPartitionsWithFilter()
    {
        // Alluxio metastore returns incorrect results
    }

    @Override
    public void testHideDeltaLakeTables()
    {
        // Alluxio metastore does not support create operations
        throw new SkipException("not supported");
    }

    @Override
    public void testDisallowQueryingOfIcebergTables()
    {
        // Alluxio metastore does not support create operations
        throw new SkipException("not supported");
    }

    @Override
    public void testIllegalStorageFormatDuringTableScan()
    {
        // Alluxio metastore does not support create operations
    }

    @Override
    public void testInsert()
    {
        // Alluxio metastore does not support insert/update operations
    }

    @Override
    public void testInsertIntoExistingPartition()
    {
        // Alluxio metastore does not support insert/update operations
    }

    @Override
    public void testInsertIntoExistingPartitionEmptyStatistics()
    {
        // Alluxio metastore does not support insert/update operations
    }

    @Override
    public void testInsertIntoNewPartition()
    {
        // Alluxio metastore does not support insert/update operations
    }

    @Override
    public void testInsertOverwriteUnpartitioned()
    {
        // Alluxio metastore does not support insert/update operations
    }

    @Override
    public void testInsertUnsupportedWriteType()
    {
        // Alluxio metastore does not support insert/update operations
    }

    @Override
    public void testMetadataDelete()
    {
        // Alluxio metastore does not support create/delete operations
    }

    @Override
    public void testMismatchSchemaTable()
    {
        // Alluxio metastore does not support create/delete operations
    }

    @Override
    public void testPartitionStatisticsSampling()
    {
        // Alluxio metastore does not support create operations
    }

    @Override
    public void testApplyProjection()
    {
        // Alluxio metastore does not support create/delete operations
    }

    @Override
    public void testApplyRedirection()
    {
        // Alluxio metastore does not support create/delete operations
    }

    @Override
    public void testMaterializedViewMetadata()
    {
        // Alluxio metastore does not support create/delete operations
    }

    @Override
    public void testOrcPageSourceMetrics()
    {
        // Alluxio metastore does not support create/insert/delete operations
    }

    @Override
    public void testParquetPageSourceMetrics()
    {
        // Alluxio metastore does not support create/insert/delete operations
    }

    @Override
    public void testPreferredInsertLayout()
    {
        // Alluxio metastore does not support insert layout operations
    }

    @Override
    public void testInsertBucketedTableLayout()
    {
        // Alluxio metastore does not support insert layout operations
    }

    @Override
    public void testInsertPartitionedBucketedTableLayout()
    {
        // Alluxio metastore does not support insert layout operations
    }

    @Override
    public void testStorePartitionWithStatistics()
    {
        // Alluxio metastore does not support create operations
    }

    @Override
    public void testRenameTable()
    {
        // Alluxio metastore does not support update operations
    }

    @Override
    public void testTableCreation()
    {
        // Alluxio metastore does not support create operations
    }

    @Override
    public void testTableCreationWithTrailingSpaceInLocation()
    {
        // Alluxio metastore does not support create operations
    }

    @Override
    public void testTableCreationIgnoreExisting()
    {
        // Alluxio metastore does not support create operations
    }

    @Override
    public void testTableCreationRollback()
    {
        // Alluxio metastore does not support create operations
    }

    @Override
    public void testTransactionDeleteInsert()
    {
        // Alluxio metastore does not support insert/update/delete operations
    }

    @Override
    public void testTypesOrc()
            throws Exception
    {
        super.testTypesOrc();
    }

    @Override
    public void testUpdateBasicPartitionStatistics()
    {
        // Alluxio metastore does not support create operations
    }

    @Override
    public void testUpdateBasicTableStatistics()
    {
        // Alluxio metastore does not support create operations
    }

    @Override
    public void testUpdatePartitionColumnStatistics()
    {
        // Alluxio metastore does not support create operations
    }

    @Override
    public void testUpdatePartitionColumnStatisticsEmptyOptionalFields()
    {
        // Alluxio metastore does not support create operations
    }

    @Override
    public void testInputInfoWhenTableIsPartitioned()
    {
        // Alluxio metastore does not support create/delete operations
    }

    @Override
    public void testInputInfoWhenTableIsNotPartitioned()
    {
        // Alluxio metastore does not support create/delete operations
    }

    @Override
    public void testInputInfoWithParquetTableFormat()
    {
        // Alluxio metastore does not support create/delete operations
    }

    @Override
    public void testUpdateTableColumnStatistics()
    {
        // Alluxio metastore does not support create operations
    }

    @Override
    public void testUpdateTableColumnStatisticsEmptyOptionalFields()
    {
        // Alluxio metastore does not support create operations
    }

    @Override
    public void testViewCreation()
    {
        // Alluxio metastore does not support create operations
    }

    @Override
    public void testNewDirectoryPermissions()
    {
        // Alluxio metastore does not support create operations
    }

    @Override
    public void testInsertBucketedTransactionalTableLayout()
            throws Exception
    {
        // Alluxio metastore does not support insert/update/delete operations
    }

    @Override
    public void testInsertPartitionedBucketedTransactionalTableLayout()
            throws Exception
    {
        // Alluxio metastore does not support insert/update/delete operations
    }

    @Override
    public void testCreateEmptyTableShouldNotCreateStagingDirectory()
    {
        // Alluxio metastore does not support create operations
    }
}
