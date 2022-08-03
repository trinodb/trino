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
package io.trino.plugin.hive.s3select;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.json.JsonCodec;
import io.airlift.stats.CounterStat;
import io.trino.hdfs.ConfigurationInitializer;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.AbstractTestHiveFileSystem;
import io.trino.plugin.hive.DefaultHiveMaterializedViewMetadataFactory;
import io.trino.plugin.hive.GenericHiveRecordCursorProvider;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveLocationService;
import io.trino.plugin.hive.HiveMetadataFactory;
import io.trino.plugin.hive.HivePageSourceProvider;
import io.trino.plugin.hive.HivePartitionManager;
import io.trino.plugin.hive.HiveSplitManager;
import io.trino.plugin.hive.HiveTransactionManager;
import io.trino.plugin.hive.LocationService;
import io.trino.plugin.hive.NamenodeStats;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.NoneHiveRedirectionsProvider;
import io.trino.plugin.hive.PartitionUpdate;
import io.trino.plugin.hive.PartitionsSystemTableProvider;
import io.trino.plugin.hive.PropertiesSystemTableProvider;
import io.trino.plugin.hive.aws.athena.PartitionProjectionService;
import io.trino.plugin.hive.fs.FileSystemDirectoryLister;
import io.trino.plugin.hive.metastore.HiveMetastoreConfig;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.plugin.hive.s3.HiveS3Config;
import io.trino.plugin.hive.s3.TrinoS3ConfigurationInitializer;
import io.trino.plugin.hive.security.SqlStandardAccessControlMetadata;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TestingTypeManager;
import io.trino.testing.MaterializedResult;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveFileSystemTestUtils.filterTable;
import static io.trino.plugin.hive.HiveFileSystemTestUtils.newSession;
import static io.trino.plugin.hive.HiveFileSystemTestUtils.readTable;
import static io.trino.plugin.hive.HiveTestUtils.getDefaultHivePageSourceFactories;
import static io.trino.plugin.hive.HiveTestUtils.getDefaultHiveRecordCursorProviders;
import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.spi.connector.MetadataProvider.NOOP_METADATA_PROVIDER;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.util.Strings.isNullOrEmpty;

public class TestHiveFileSystemS3SelectJsonPushdown
{
    private SchemaTableName tableJson;

    private HdfsEnvironment hdfsEnvironment;
    private LocationService locationService;
    private AbstractTestHiveFileSystem.TestingHiveMetastore metastoreClient;
    private HiveMetadataFactory metadataFactory;
    private HiveTransactionManager transactionManager;
    private ConnectorSplitManager splitManager;
    private ConnectorPageSourceProvider pageSourceProvider;

    private ExecutorService executor;
    private HiveConfig config;
    private ScheduledExecutorService heartbeatService;

    @Parameters({
            "hive.hadoop2.metastoreHost",
            "hive.hadoop2.metastorePort",
            "hive.hadoop2.databaseName",
            "hive.hadoop2.s3.awsAccessKey",
            "hive.hadoop2.s3.awsSecretKey",
            "hive.hadoop2.s3.writableBucket",
            "hive.hadoop2.s3.testDirectory",
    })
    @BeforeClass
    public void setup(String host, int port, String databaseName, String awsAccessKey, String awsSecretKey, String writableBucket, String testDirectory)
    {
        checkArgument(!isNullOrEmpty(host), "Expected non empty host");
        checkArgument(!isNullOrEmpty(databaseName), "Expected non empty databaseName");
        checkArgument(!isNullOrEmpty(awsAccessKey), "Expected non empty awsAccessKey");
        checkArgument(!isNullOrEmpty(awsSecretKey), "Expected non empty awsSecretKey");
        checkArgument(!isNullOrEmpty(writableBucket), "Expected non empty writableBucket");
        checkArgument(!isNullOrEmpty(testDirectory), "Expected non empty testDirectory");

        executor = newCachedThreadPool(daemonThreadsNamed("s3select-json-%s"));
        heartbeatService = newScheduledThreadPool(1);

        ConfigurationInitializer s3Config = new TrinoS3ConfigurationInitializer(new HiveS3Config()
                .setS3AwsAccessKey(awsAccessKey)
                .setS3AwsSecretKey(awsSecretKey));
        HdfsConfigurationInitializer initializer = new HdfsConfigurationInitializer(new HdfsConfig(), ImmutableSet.of(s3Config));
        HdfsConfiguration hdfsConfiguration = new DynamicHdfsConfiguration(initializer, ImmutableSet.of());

        config = new HiveConfig().setS3SelectPushdownEnabled(true);
        HivePartitionManager hivePartitionManager = new HivePartitionManager(config);

        hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, new HdfsConfig(), new NoHdfsAuthentication());
        locationService = new HiveLocationService(hdfsEnvironment);
        JsonCodec<PartitionUpdate> partitionUpdateCodec = JsonCodec.jsonCodec(PartitionUpdate.class);

        metastoreClient = new AbstractTestHiveFileSystem.TestingHiveMetastore(
                new BridgingHiveMetastore(
                        testingThriftHiveMetastoreBuilder()
                                .metastoreClient(HostAndPort.fromParts(host, port))
                                .hiveConfig(config)
                                .hdfsEnvironment(hdfsEnvironment)
                                .build()),
                new Path(format("s3a://%s/%s/", writableBucket, testDirectory)),
                hdfsEnvironment);
        metadataFactory = new HiveMetadataFactory(
                new CatalogName("hive"),
                config,
                new HiveMetastoreConfig(),
                HiveMetastoreFactory.ofInstance(metastoreClient),
                hdfsEnvironment,
                hivePartitionManager,
                newDirectExecutorService(),
                heartbeatService,
                TESTING_TYPE_MANAGER,
                NOOP_METADATA_PROVIDER,
                locationService,
                partitionUpdateCodec,
                new NodeVersion("test_version"),
                new NoneHiveRedirectionsProvider(),
                ImmutableSet.of(
                        new PartitionsSystemTableProvider(hivePartitionManager, TESTING_TYPE_MANAGER),
                        new PropertiesSystemTableProvider()),
                new DefaultHiveMaterializedViewMetadataFactory(),
                SqlStandardAccessControlMetadata::new,
                new FileSystemDirectoryLister(),
                new PartitionProjectionService(config, ImmutableMap.of(), new TestingTypeManager()));
        transactionManager = new HiveTransactionManager(metadataFactory);

        splitManager = new HiveSplitManager(
                transactionManager,
                hivePartitionManager,
                new NamenodeStats(),
                hdfsEnvironment,
                new BoundedExecutor(executor, config.getMaxSplitIteratorThreads()),
                new CounterStat(),
                config.getMaxOutstandingSplits(),
                config.getMaxOutstandingSplitsSize(),
                config.getMinPartitionBatchSize(),
                config.getMaxPartitionBatchSize(),
                config.getMaxInitialSplits(),
                config.getSplitLoaderConcurrency(),
                config.getMaxSplitsPerSecond(),
                config.getRecursiveDirWalkerEnabled(),
                TESTING_TYPE_MANAGER);

        pageSourceProvider = new HivePageSourceProvider(
                TESTING_TYPE_MANAGER,
                hdfsEnvironment,
                config,
                getDefaultHivePageSourceFactories(hdfsEnvironment, config),
                getDefaultHiveRecordCursorProviders(config, hdfsEnvironment),
                new GenericHiveRecordCursorProvider(hdfsEnvironment, config),
                Optional.empty());

        tableJson = new SchemaTableName(databaseName, "trino_s3select_test_external_fs_json");
    }

    @Test
    public void testGetRecordsJson()
            throws Exception
    {
        assertEqualsIgnoreOrder(
                readTable(tableJson, transactionManager, config, pageSourceProvider, splitManager),
                MaterializedResult.resultBuilder(newSession(config), BIGINT, BIGINT)
                        .row(2L, 4L).row(5L, 6L) // test_table.json
                        .row(7L, 23L).row(28L, 22L).row(13L, 10L) // test_table.json.gz
                        .row(1L, 19L).row(6L, 3L).row(24L, 22L).row(100L, 77L) // test_table.json.bz2
                        .build());
    }

    @Test
    public void testFilterRecordsJson()
            throws Exception
    {
        List<ColumnHandle> projectedColumns = ImmutableList.of(
                createBaseColumn("col_1", 0, HIVE_INT, BIGINT, REGULAR, Optional.empty()));

        assertEqualsIgnoreOrder(
                filterTable(tableJson, projectedColumns, transactionManager, config, pageSourceProvider, splitManager),
                MaterializedResult.resultBuilder(newSession(config), BIGINT)
                        .row(2L).row(5L) // test_table.json
                        .row(7L).row(28L).row(13L) // test_table.json.gz
                        .row(1L).row(6L).row(24L).row(100L) // test_table.json.bz2
                        .build());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
        if (heartbeatService != null) {
            heartbeatService.shutdownNow();
            heartbeatService = null;
        }
    }
}
