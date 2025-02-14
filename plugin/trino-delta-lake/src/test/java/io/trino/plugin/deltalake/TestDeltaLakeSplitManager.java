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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.cache.DefaultCachingHostAddressProvider;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableMetadataScheduler;
import io.trino.plugin.deltalake.metastore.file.DeltaLakeFileMetastoreTableOperationsProvider;
import io.trino.plugin.deltalake.statistics.CachingExtendedStatisticsAccess;
import io.trino.plugin.deltalake.statistics.ExtendedStatistics;
import io.trino.plugin.deltalake.statistics.MetaDirStatisticsAccess;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointSchemaManager;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointWriterManager;
import io.trino.plugin.deltalake.transactionlog.checkpoint.LastCheckpoint;
import io.trino.plugin.deltalake.transactionlog.writer.NoIsolationSynchronizer;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogSynchronizerManager;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogWriterFactory;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;
import io.trino.testing.TestingConnectorContext;
import io.trino.testing.TestingConnectorSession;
import io.trino.testing.TestingNodeManager;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.Math.clamp;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeSplitManager
{
    private static final String TABLE_PATH = "/path/to/a/table";
    private static final String FILE_PATH = "directory/file";
    private static final String FULL_PATH = TABLE_PATH + "/" + FILE_PATH;
    private static final MetadataEntry metadataEntry = new MetadataEntry(
            "id",
            "name",
            "description",
            new MetadataEntry.Format("provider", ImmutableMap.of()),
            "{\"type\":\"struct\",\"fields\":[{\"name\":\"val\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]}",
            ImmutableList.of(),
            ImmutableMap.of(),
            0);
    private static final DeltaLakeTableHandle tableHandle = new DeltaLakeTableHandle(
            "schema",
            "table",
            true,
            TABLE_PATH,
            metadataEntry,
            new ProtocolEntry(1, 2, Optional.empty(), Optional.empty()),
            TupleDomain.all(),
            TupleDomain.all(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            0,
            false);
    private final HiveTransactionHandle transactionHandle = new HiveTransactionHandle(true);

    @Test
    public void testAbsolutePathSplits()
            throws Exception
    {
        testAbsolutePathSplits("file://path/to/file", "file://path/to/file");
        testAbsolutePathSplits("abfs://ct@st.dfs.core.windows.net/path/to/file", "abfs://ct@st.dfs.core.windows.net/path/to/file");
        testAbsolutePathSplits("hdfs://path/to/file", "hdfs://path/to/file");
        testAbsolutePathSplits("s3://my-s3-bucket/path/to//file", "s3://my-s3-bucket/path/to//file");
        testAbsolutePathSplits("s3://my-s3-bucket/path/to//file/", "s3://my-s3-bucket/path/to//file/");
        testAbsolutePathSplits("gs://my-gcp-bucket/path/to/file", "gs://my-gcp-bucket/path/to/file");
        testAbsolutePathSplits("abfs://ct@st.dfs.core.windows.net/+ab+/a%25/a%2525/path/to/file", "abfs://ct@st.dfs.core.windows.net/+ab+/a%/a%25/path/to/file");
    }

    private void testAbsolutePathSplits(String absoluteRawEncodedFilePath, String absoluteDecodedParsedFilePath)
            throws Exception
    {
        long fileSize = 20_000;
        List<AddFileEntry> addFileEntries = ImmutableList.of(addFileEntryOfSize(absoluteRawEncodedFilePath, fileSize));
        DeltaLakeConfig deltaLakeConfig = new DeltaLakeConfig()
                .setMaxSplitSize(DataSize.ofBytes(5_000));
        double minimumAssignedSplitWeight = deltaLakeConfig.getMinimumAssignedSplitWeight();

        DeltaLakeSplitManager splitManager = setupSplitManager(addFileEntries, deltaLakeConfig);
        List<DeltaLakeSplit> splits = getSplits(splitManager, deltaLakeConfig);
        List<DeltaLakeSplit> expected = ImmutableList.of(
                makeSplit(absoluteDecodedParsedFilePath, 0, 5_000, fileSize, minimumAssignedSplitWeight),
                makeSplit(absoluteDecodedParsedFilePath, 5_000, 5_000, fileSize, minimumAssignedSplitWeight),
                makeSplit(absoluteDecodedParsedFilePath, 10_000, 5_000, fileSize, minimumAssignedSplitWeight),
                makeSplit(absoluteDecodedParsedFilePath, 15_000, 5_000, fileSize, minimumAssignedSplitWeight));

        assertThat(splits).isEqualTo(expected);
    }

    @Test
    public void testSplitSizes()
            throws ExecutionException, InterruptedException
    {
        long fileSize = 50_000;
        List<AddFileEntry> addFileEntries = ImmutableList.of(addFileEntryOfSize(FILE_PATH, fileSize));
        DeltaLakeConfig deltaLakeConfig = new DeltaLakeConfig()
                .setMaxSplitSize(DataSize.ofBytes(20_000));
        double minimumAssignedSplitWeight = deltaLakeConfig.getMinimumAssignedSplitWeight();

        DeltaLakeSplitManager splitManager = setupSplitManager(addFileEntries, deltaLakeConfig);
        List<DeltaLakeSplit> splits = getSplits(splitManager, deltaLakeConfig);

        List<DeltaLakeSplit> expected = ImmutableList.of(
                makeSplit(FULL_PATH, 0, 20_000, fileSize, minimumAssignedSplitWeight),
                makeSplit(FULL_PATH, 20_000, 20_000, fileSize, minimumAssignedSplitWeight),
                makeSplit(FULL_PATH, 40_000, 10_000, fileSize, minimumAssignedSplitWeight));

        assertThat(splits).isEqualTo(expected);
    }

    @Test
    public void testSplitsFromMultipleFiles()
            throws ExecutionException, InterruptedException
    {
        long firstFileSize = 1_000;
        long secondFileSize = 20_000;
        List<AddFileEntry> addFileEntries = ImmutableList.of(addFileEntryOfSize(FILE_PATH, firstFileSize), addFileEntryOfSize(FILE_PATH, secondFileSize));
        DeltaLakeConfig deltaLakeConfig = new DeltaLakeConfig()
                .setMaxSplitSize(DataSize.ofBytes(10_000));
        double minimumAssignedSplitWeight = deltaLakeConfig.getMinimumAssignedSplitWeight();

        DeltaLakeSplitManager splitManager = setupSplitManager(addFileEntries, deltaLakeConfig);

        List<DeltaLakeSplit> splits = getSplits(splitManager, deltaLakeConfig);
        List<DeltaLakeSplit> expected = ImmutableList.of(
                makeSplit(FULL_PATH, 0, 1_000, firstFileSize, minimumAssignedSplitWeight),
                makeSplit(FULL_PATH, 0, 10_000, secondFileSize, minimumAssignedSplitWeight),
                makeSplit(FULL_PATH, 10_000, 10_000, secondFileSize, minimumAssignedSplitWeight));
        assertThat(splits).isEqualTo(expected);
    }

    private DeltaLakeSplitManager setupSplitManager(List<AddFileEntry> addFileEntries, DeltaLakeConfig deltaLakeConfig)
    {
        TestingConnectorContext context = new TestingConnectorContext();
        TypeManager typeManager = context.getTypeManager();

        HdfsFileSystemFactory hdfsFileSystemFactory = new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS);
        TransactionLogAccess transactionLogAccess = new TransactionLogAccess(
                typeManager,
                new CheckpointSchemaManager(typeManager),
                deltaLakeConfig,
                new FileFormatDataSourceStats(),
                hdfsFileSystemFactory,
                new ParquetReaderConfig())
        {
            @Override
            public Stream<AddFileEntry> getActiveFiles(
                    ConnectorSession session,
                    TableSnapshot tableSnapshot,
                    MetadataEntry metadataEntry,
                    ProtocolEntry protocolEntry,
                    TupleDomain<DeltaLakeColumnHandle> partitionConstraint,
                    Set<DeltaLakeColumnHandle> projectedColumns)
            {
                return addFileEntries.stream();
            }
        };

        CheckpointWriterManager checkpointWriterManager = new CheckpointWriterManager(
                typeManager,
                new CheckpointSchemaManager(typeManager),
                hdfsFileSystemFactory,
                new NodeVersion("test_version"),
                transactionLogAccess,
                new FileFormatDataSourceStats(),
                JsonCodec.jsonCodec(LastCheckpoint.class));

        HiveMetastoreFactory hiveMetastoreFactory = HiveMetastoreFactory.ofInstance(createTestingFileHiveMetastore(new MemoryFileSystemFactory(), Location.of("memory:///")));
        DeltaLakeMetadataFactory metadataFactory = new DeltaLakeMetadataFactory(
                hiveMetastoreFactory,
                hdfsFileSystemFactory,
                transactionLogAccess,
                typeManager,
                DeltaLakeAccessControlMetadataFactory.DEFAULT,
                new DeltaLakeConfig(),
                JsonCodec.jsonCodec(DataFileInfo.class),
                JsonCodec.jsonCodec(DeltaLakeMergeResult.class),
                new TransactionLogWriterFactory(
                        new TransactionLogSynchronizerManager(ImmutableMap.of(), new NoIsolationSynchronizer(hdfsFileSystemFactory))),
                new TestingNodeManager(),
                checkpointWriterManager,
                new CachingExtendedStatisticsAccess(new MetaDirStatisticsAccess(HDFS_FILE_SYSTEM_FACTORY, new JsonCodecFactory().jsonCodec(ExtendedStatistics.class))),
                true,
                new NodeVersion("test_version"),
                new DeltaLakeTableMetadataScheduler(new TestingNodeManager(), TESTING_TYPE_MANAGER, new DeltaLakeFileMetastoreTableOperationsProvider(hiveMetastoreFactory), Integer.MAX_VALUE, new DeltaLakeConfig()),
                newDirectExecutorService());

        ConnectorSession session = testingConnectorSessionWithConfig(deltaLakeConfig);
        DeltaLakeTransactionManager deltaLakeTransactionManager = new DeltaLakeTransactionManager(metadataFactory);
        deltaLakeTransactionManager.begin(transactionHandle);
        deltaLakeTransactionManager.get(transactionHandle, session.getIdentity()).getSnapshot(session, tableHandle.getSchemaTableName(), TABLE_PATH, Optional.empty());
        return new DeltaLakeSplitManager(
                typeManager,
                transactionLogAccess,
                newDirectExecutorService(),
                deltaLakeConfig,
                HDFS_FILE_SYSTEM_FACTORY,
                deltaLakeTransactionManager,
                new DefaultCachingHostAddressProvider());
    }

    private AddFileEntry addFileEntryOfSize(String path, long fileSize)
    {
        return new AddFileEntry(path, ImmutableMap.of(), fileSize, 0, false, Optional.empty(), Optional.empty(), ImmutableMap.of(), Optional.empty());
    }

    private DeltaLakeSplit makeSplit(String path, long start, long splitSize, long fileSize, double minimumAssignedSplitWeight)
    {
        SplitWeight splitWeight = SplitWeight.fromProportion(clamp((double) fileSize / splitSize, minimumAssignedSplitWeight, 1.0));
        return new DeltaLakeSplit(path, start, splitSize, fileSize, Optional.empty(), 0, Optional.empty(), splitWeight, TupleDomain.all(), ImmutableMap.of());
    }

    private List<DeltaLakeSplit> getSplits(DeltaLakeSplitManager splitManager, DeltaLakeConfig deltaLakeConfig)
            throws ExecutionException, InterruptedException
    {
        ConnectorSplitSource splitSource = splitManager.getSplits(
                transactionHandle,
                testingConnectorSessionWithConfig(deltaLakeConfig),
                tableHandle,
                DynamicFilter.EMPTY,
                Constraint.alwaysTrue());
        ImmutableList.Builder<DeltaLakeSplit> splits = ImmutableList.builder();
        while (!splitSource.isFinished()) {
            List<ConnectorSplit> nextBatch = splitSource.getNextBatch(10).get().getSplits();
            splits.addAll(
                    nextBatch.stream()
                            .map(split -> (DeltaLakeSplit) split)
                            .collect(Collectors.toList()));
        }
        return splits.build();
    }

    private ConnectorSession testingConnectorSessionWithConfig(DeltaLakeConfig deltaLakeConfig)
    {
        DeltaLakeSessionProperties sessionProperties = new DeltaLakeSessionProperties(deltaLakeConfig, new ParquetReaderConfig(), new ParquetWriterConfig());
        return TestingConnectorSession.builder()
                .setPropertyMetadata(sessionProperties.getSessionProperties())
                .build();
    }
}
