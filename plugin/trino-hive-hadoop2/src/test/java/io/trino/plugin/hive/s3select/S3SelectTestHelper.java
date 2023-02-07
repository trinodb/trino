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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.json.JsonCodec;
import io.airlift.stats.CounterStat;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.ConfigurationInitializer;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.AbstractTestHiveFileSystem.TestingHiveMetastore;
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
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TestingTypeManager;
import io.trino.testing.MaterializedResult;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.LongStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.plugin.hive.HiveFileSystemTestUtils.filterTable;
import static io.trino.plugin.hive.HiveFileSystemTestUtils.getSplitsCount;
import static io.trino.plugin.hive.HiveTestUtils.getDefaultHivePageSourceFactories;
import static io.trino.plugin.hive.HiveTestUtils.getDefaultHiveRecordCursorProviders;
import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.spi.connector.MetadataProvider.NOOP_METADATA_PROVIDER;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.util.Strings.isNullOrEmpty;

public class S3SelectTestHelper
{
    private HdfsEnvironment hdfsEnvironment;
    private LocationService locationService;
    private TestingHiveMetastore metastoreClient;
    private HiveMetadataFactory metadataFactory;
    private HiveTransactionManager transactionManager;
    private ConnectorSplitManager splitManager;
    private ConnectorPageSourceProvider pageSourceProvider;

    private ExecutorService executorService;
    private HiveConfig hiveConfig;
    private ScheduledExecutorService heartbeatService;

    public S3SelectTestHelper(String host,
                              int port,
                              String databaseName,
                              String awsAccessKey,
                              String awsSecretKey,
                              String writableBucket,
                              String testDirectory,
                              HiveConfig hiveConfig)
    {
        checkArgument(!isNullOrEmpty(host), "Expected non empty host");
        checkArgument(!isNullOrEmpty(databaseName), "Expected non empty databaseName");
        checkArgument(!isNullOrEmpty(awsAccessKey), "Expected non empty awsAccessKey");
        checkArgument(!isNullOrEmpty(awsSecretKey), "Expected non empty awsSecretKey");
        checkArgument(!isNullOrEmpty(writableBucket), "Expected non empty writableBucket");
        checkArgument(!isNullOrEmpty(testDirectory), "Expected non empty testDirectory");

        executorService = newCachedThreadPool(daemonThreadsNamed("s3select-tests-%s"));
        heartbeatService = newScheduledThreadPool(1);

        ConfigurationInitializer s3Config = new TrinoS3ConfigurationInitializer(new HiveS3Config()
                .setS3AwsAccessKey(awsAccessKey)
                .setS3AwsSecretKey(awsSecretKey));
        HdfsConfigurationInitializer initializer = new HdfsConfigurationInitializer(new HdfsConfig(), ImmutableSet.of(s3Config));
        HdfsConfiguration hdfsConfiguration = new DynamicHdfsConfiguration(initializer, ImmutableSet.of());

        this.hiveConfig = hiveConfig;
        HivePartitionManager hivePartitionManager = new HivePartitionManager(this.hiveConfig);

        hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, new HdfsConfig(), new NoHdfsAuthentication());
        locationService = new HiveLocationService(hdfsEnvironment);
        JsonCodec<PartitionUpdate> partitionUpdateCodec = JsonCodec.jsonCodec(PartitionUpdate.class);

        metastoreClient = new TestingHiveMetastore(
                new BridgingHiveMetastore(
                        testingThriftHiveMetastoreBuilder()
                                .metastoreClient(HostAndPort.fromParts(host, port))
                                .hiveConfig(this.hiveConfig)
                                .hdfsEnvironment(hdfsEnvironment)
                                .build()),
                new Path(format("s3a://%s/%s/", writableBucket, testDirectory)),
                hdfsEnvironment);
        metadataFactory = new HiveMetadataFactory(
                new CatalogName("hive"),
                this.hiveConfig,
                new HiveMetastoreConfig(),
                HiveMetastoreFactory.ofInstance(metastoreClient),
                new HdfsFileSystemFactory(hdfsEnvironment),
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
                new PartitionProjectionService(this.hiveConfig, ImmutableMap.of(), new TestingTypeManager()),
                true);
        transactionManager = new HiveTransactionManager(metadataFactory);

        splitManager = new HiveSplitManager(
                transactionManager,
                hivePartitionManager,
                new HdfsFileSystemFactory(hdfsEnvironment),
                new NamenodeStats(),
                hdfsEnvironment,
                new BoundedExecutor(executorService, this.hiveConfig.getMaxSplitIteratorThreads()),
                new CounterStat(),
                this.hiveConfig.getMaxOutstandingSplits(),
                this.hiveConfig.getMaxOutstandingSplitsSize(),
                this.hiveConfig.getMinPartitionBatchSize(),
                this.hiveConfig.getMaxPartitionBatchSize(),
                this.hiveConfig.getMaxInitialSplits(),
                this.hiveConfig.getSplitLoaderConcurrency(),
                this.hiveConfig.getMaxSplitsPerSecond(),
                this.hiveConfig.getRecursiveDirWalkerEnabled(),
                TESTING_TYPE_MANAGER,
                this.hiveConfig.getMaxPartitionsPerScan());

        pageSourceProvider = new HivePageSourceProvider(
                TESTING_TYPE_MANAGER,
                hdfsEnvironment,
                this.hiveConfig,
                getDefaultHivePageSourceFactories(hdfsEnvironment, this.hiveConfig),
                getDefaultHiveRecordCursorProviders(this.hiveConfig, hdfsEnvironment),
                new GenericHiveRecordCursorProvider(hdfsEnvironment, this.hiveConfig));
    }

    public S3SelectTestHelper(String host,
                              int port,
                              String databaseName,
                              String awsAccessKey,
                              String awsSecretKey,
                              String writableBucket,
                              String testDirectory)
    {
        this(host, port, databaseName, awsAccessKey, awsSecretKey, writableBucket, testDirectory, new HiveConfig().setS3SelectPushdownEnabled(true));
    }

    public HiveTransactionManager getTransactionManager()
    {
        return transactionManager;
    }

    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    public HiveConfig getHiveConfig()
    {
        return hiveConfig;
    }

    public void tearDown()
    {
        hdfsEnvironment = null;
        locationService = null;
        metastoreClient = null;
        metadataFactory = null;
        transactionManager = null;
        splitManager = null;
        pageSourceProvider = null;
        hiveConfig = null;
        if (executorService != null) {
            executorService.shutdownNow();
            executorService = null;
        }
        if (heartbeatService != null) {
            heartbeatService.shutdownNow();
            heartbeatService = null;
        }
    }

    int getTableSplitsCount(SchemaTableName table)
    {
        return getSplitsCount(
                table,
                getTransactionManager(),
                getHiveConfig(),
                getSplitManager());
    }

    MaterializedResult getFilteredTableResult(SchemaTableName table, ColumnHandle column)
    {
        try {
            return filterTable(
                    table,
                    List.of(column),
                    getTransactionManager(),
                    getHiveConfig(),
                    getPageSourceProvider(),
                    getSplitManager());
        }
        catch (IOException ignored) {
        }

        return null;
    }

    static MaterializedResult expectedResult(ConnectorSession session, int start, int end)
    {
        MaterializedResult.Builder builder = MaterializedResult.resultBuilder(session, BIGINT);
        LongStream.rangeClosed(start, end).forEach(builder::row);
        return builder.build();
    }

    static boolean isSplitCountInOpenInterval(int splitCount,
                                       int lowerBound,
                                       int upperBound)
    {
        // Split number may vary, the minimum number of splits being obtained with
        // the first split of maxInitialSplitSize and the rest of maxSplitSize
        return lowerBound < splitCount && splitCount < upperBound;
    }
}
