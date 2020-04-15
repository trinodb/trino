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
package io.prestosql.plugin.hive.metastore.glue;

import com.google.common.collect.ImmutableList;
import io.airlift.concurrent.BoundedExecutor;
import io.prestosql.plugin.hive.AbstractTestHiveLocal;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;

import static io.prestosql.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static java.util.Locale.ENGLISH;
import static java.util.UUID.randomUUID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

/*
 * GlueHiveMetastore currently uses AWS Default Credential Provider Chain,
 * See https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default
 * on ways to set your AWS credentials which will be needed to run this test.
 */
@Test(singleThreaded = true)
public class TestHiveGlueMetastore
        extends AbstractTestHiveLocal
{
    private static final HiveIdentity HIVE_CONTEXT = new HiveIdentity(SESSION);

    public TestHiveGlueMetastore()
    {
        super("test_glue" + randomUUID().toString().toLowerCase(ENGLISH).replace("-", ""));
    }

    @Override
    protected HiveMetastore createMetastore(File tempDir)
    {
        GlueHiveMetastoreConfig glueConfig = new GlueHiveMetastoreConfig();
        glueConfig.setDefaultWarehouseDir(tempDir.toURI().toString());

        Executor executor = new BoundedExecutor(this.executor, 10);
        return new GlueHiveMetastore(HDFS_ENVIRONMENT, glueConfig, new DisabledGlueColumnStatisticsProvider(), executor, Optional.empty());
    }

    @Override
    public void testRenameTable()
    {
        // rename table is not yet supported by Glue
    }

    @Override
    public void testPartitionStatisticsSampling()
            throws Exception
    {
        // Glue metastore does not support column level statistics
    }

    @Override
    public void testUpdateTableColumnStatistics()
    {
        // column statistics are not supported by Glue
    }

    @Override
    public void testUpdateTableColumnStatisticsEmptyOptionalFields()
    {
        // column statistics are not supported by Glue
    }

    @Override
    public void testUpdatePartitionColumnStatistics()
    {
        // column statistics are not supported by Glue
    }

    @Override
    public void testUpdatePartitionColumnStatisticsEmptyOptionalFields()
    {
        // column statistics are not supported by Glue
    }

    @Override
    public void testStorePartitionWithStatistics()
            throws Exception
    {
        testStorePartitionWithStatistics(STATISTICS_PARTITIONED_TABLE_COLUMNS, BASIC_STATISTICS_1, BASIC_STATISTICS_2, BASIC_STATISTICS_1, EMPTY_TABLE_STATISTICS);
    }

    @Override
    public void testGetPartitions()
            throws Exception
    {
        try {
            createDummyPartitionedTable(tablePartitionFormat, CREATE_TABLE_COLUMNS_PARTITIONED);
            Optional<List<String>> partitionNames = getMetastoreClient().getPartitionNames(HIVE_CONTEXT, tablePartitionFormat.getSchemaName(), tablePartitionFormat.getTableName());
            assertTrue(partitionNames.isPresent());
            assertEquals(partitionNames.get(), ImmutableList.of("ds=2016-01-01", "ds=2016-01-02"));
        }
        finally {
            dropTable(tablePartitionFormat);
        }
    }

    @Test
    public void testGetDatabasesLogsStats()
    {
        GlueHiveMetastore metastore = (GlueHiveMetastore) getMetastoreClient();
        GlueMetastoreStats stats = metastore.getStats();
        double initialCallCount = stats.getGetAllDatabases().getTime().getAllTime().getCount();
        long initialFailureCount = stats.getGetAllDatabases().getTotalFailures().getTotalCount();
        getMetastoreClient().getAllDatabases();
        assertEquals(stats.getGetAllDatabases().getTime().getAllTime().getCount(), initialCallCount + 1.0);
        assertTrue(stats.getGetAllDatabases().getTime().getAllTime().getAvg() > 0.0);
        assertEquals(stats.getGetAllDatabases().getTotalFailures().getTotalCount(), initialFailureCount);
    }

    @Test
    public void testGetDatabaseFailureLogsStats()
    {
        GlueHiveMetastore metastore = (GlueHiveMetastore) getMetastoreClient();
        GlueMetastoreStats stats = metastore.getStats();
        long initialFailureCount = stats.getGetDatabase().getTotalFailures().getTotalCount();
        assertThrows(() -> getMetastoreClient().getDatabase(null));
        assertEquals(stats.getGetDatabase().getTotalFailures().getTotalCount(), initialFailureCount + 1);
    }
}
