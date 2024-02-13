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
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.filesystem.cache.DefaultCachingHostAddressProvider;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.plugin.base.TypeDeserializer;
import io.trino.plugin.deltalake.statistics.CachingExtendedStatisticsAccess;
import io.trino.plugin.deltalake.statistics.ExtendedStatistics;
import io.trino.plugin.deltalake.statistics.MetaDirStatisticsAccess;
import io.trino.plugin.deltalake.transactionlog.DeletionVectorEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointSchemaManager;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointWriterManager;
import io.trino.plugin.deltalake.transactionlog.checkpoint.LastCheckpoint;
import io.trino.plugin.deltalake.transactionlog.writer.NoIsolationSynchronizer;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogSynchronizerManager;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogWriterFactory;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.LocationAccessControl;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.UnimplementedHiveMetastore;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.SplitWeight;
import io.trino.spi.block.Block;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.block.TestingBlockJsonSerde;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.testing.TestingConnectorContext;
import io.trino.testing.TestingNodeManager;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.trino.plugin.deltalake.DeltaLakeAnalyzeProperties.AnalyzeMode.FULL_REFRESH;
import static io.trino.plugin.deltalake.DeltaLakeTableHandle.WriteType.UPDATE;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestDeltaLakeCacheIds
{
    private static ScheduledExecutorService executorService = newScheduledThreadPool(1);
    private DeltaLakeCacheMetadata metadata;
    private DeltaLakeSplitManager splitManager;

    @BeforeAll
    public void setup()
    {
        DeltaLakeConfig config = new DeltaLakeConfig();
        HdfsConfiguration hdfsConfiguration = (context, uri) -> new Configuration(false);
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, new HdfsConfig(), new NoHdfsAuthentication());
        TestingConnectorContext context = new TestingConnectorContext();
        TypeManager typeManager = context.getTypeManager();
        CheckpointSchemaManager checkpointSchemaManager = new CheckpointSchemaManager(typeManager);
        HdfsFileSystemFactory hdfsFileSystemFactory = new HdfsFileSystemFactory(hdfsEnvironment, HDFS_FILE_SYSTEM_STATS);

        FileFormatDataSourceStats fileFormatDataSourceStats = new FileFormatDataSourceStats();

        TransactionLogAccess transactionLogAccess = new TransactionLogAccess(
                typeManager,
                checkpointSchemaManager,
                config,
                fileFormatDataSourceStats,
                HDFS_FILE_SYSTEM_FACTORY,
                new ParquetReaderConfig());
        CheckpointWriterManager checkpointWriterManager = new CheckpointWriterManager(
                typeManager,
                new CheckpointSchemaManager(typeManager),
                hdfsFileSystemFactory,
                new NodeVersion("test_version"),
                transactionLogAccess,
                new FileFormatDataSourceStats(),
                JsonCodec.jsonCodec(LastCheckpoint.class));

        DeltaLakeMetadataFactory metadataFactory = new DeltaLakeMetadataFactory(
                HiveMetastoreFactory.ofInstance(new UnimplementedHiveMetastore()),
                hdfsFileSystemFactory,
                LocationAccessControl.ALLOW_ALL,
                transactionLogAccess,
                typeManager,
                DeltaLakeAccessControlMetadataFactory.DEFAULT,
                config,
                JsonCodec.jsonCodec(DataFileInfo.class),
                JsonCodec.jsonCodec(DeltaLakeMergeResult.class),
                new TransactionLogWriterFactory(
                        new TransactionLogSynchronizerManager(ImmutableMap.of(), new NoIsolationSynchronizer(hdfsFileSystemFactory))),
                new TestingNodeManager(),
                checkpointWriterManager,
                DeltaLakeRedirectionsProvider.NOOP,
                new CachingExtendedStatisticsAccess(new MetaDirStatisticsAccess(HDFS_FILE_SYSTEM_FACTORY, new JsonCodecFactory().jsonCodec(ExtendedStatistics.class))),
                true,
                new NodeVersion("test_version"));
        metadata = new DeltaLakeCacheMetadata(
                createJsonCodec(DeltaLakeCacheTableId.class),
                createJsonCodec(DeltaLakeColumnHandle.class));
        splitManager = new DeltaLakeSplitManager(
                typeManager,
                transactionLogAccess,
                newDirectExecutorService(),
                config,
                hdfsFileSystemFactory,
                createJsonCodec(DeltaLakeCacheSplitId.class),
                new DeltaLakeTransactionManager(metadataFactory),
                new DefaultCachingHostAddressProvider());
    }

    @AfterAll
    public void tearDown()
    {
        if (executorService != null) {
            executorService.shutdownNow();
            executorService = null;
        }
    }

    @Test
    public void testTableId()
    {
        DeltaLakeColumnHandle partitionColumn = new DeltaLakeColumnHandle("col1", BIGINT, OptionalInt.empty(), "base_col1", BIGINT, DeltaLakeColumnType.PARTITION_KEY, Optional.empty());
        String schema = "{\"fields\": [{\"name\": \"value\", \"metadata\": {}}]}";

        // table id for updating query is empty
        assertThat(metadata.getCacheTableId(createDeltaLakeTableHandle(
                createMetadataEntry("id", schema),
                ImmutableSet.of(partitionColumn),
                Optional.of(ImmutableList.of(partitionColumn)),
                Optional.of(UPDATE)))
        ).isEqualTo(Optional.empty());

        // `managed` shouldn't be part of table id
        assertThat(metadata.getCacheTableId(createDeltaLakeTableHandle("schema", "table", false, "location", 0)))
                .isEqualTo(metadata.getCacheTableId(createDeltaLakeTableHandle("schema", "table", true, "location", 0)));

        // `location` should be part of table id - it is part of split
        assertThat(metadata.getCacheTableId(createDeltaLakeTableHandle("schema", "table", false, "location", 0)))
                .isNotEqualTo(metadata.getCacheTableId(createDeltaLakeTableHandle("schema", "table", false, "location2", 0)));

        // enforced predicate shouldn't be part of table id
        assertThat(metadata.getCacheTableId(createDeltaLakeTableHandle(TupleDomain.withColumnDomains(ImmutableMap.of(partitionColumn, singleValue(INTEGER, 1L))), TupleDomain.all())))
                .isEqualTo(metadata.getCacheTableId(createDeltaLakeTableHandle(TupleDomain.withColumnDomains(ImmutableMap.of(partitionColumn, singleValue(INTEGER, 2L))), TupleDomain.all())));

        // metadataEntry should be part of table id
        assertThat(metadata.getCacheTableId(createDeltaLakeTableHandle(
                createMetadataEntry("id1", schema),
                ImmutableSet.of(),
                Optional.empty(),
                Optional.empty()))
        ).isNotEqualTo(metadata.getCacheTableId(createDeltaLakeTableHandle(createMetadataEntry("id2", schema), ImmutableSet.of(), Optional.empty(), Optional.empty())));

        // projectedColumns shouldn't be part of table id
        assertThat(metadata.getCacheTableId(createDeltaLakeTableHandle(createMetadataEntry("id", schema), ImmutableSet.of(partitionColumn), Optional.empty(), Optional.empty())))
                .isEqualTo(metadata.getCacheTableId(createDeltaLakeTableHandle(createMetadataEntry("id", schema), ImmutableSet.of(), Optional.empty(), Optional.empty())));

        // nonPartitionConstraint should not be part of table id
        assertThat(metadata.getCacheTableId(createDeltaLakeTableHandle(TupleDomain.all(), TupleDomain.withColumnDomains(ImmutableMap.of(partitionColumn, singleValue(BIGINT, 1L))))))
                .isEqualTo(metadata.getCacheTableId(createDeltaLakeTableHandle(TupleDomain.all(), TupleDomain.withColumnDomains(ImmutableMap.of(partitionColumn, singleValue(INTEGER, 2L))))));

        // readVersion predicate should not be part of table id
        assertThat(metadata.getCacheTableId(createDeltaLakeTableHandle("schema", "table", false, "location", 0)))
                .isEqualTo(metadata.getCacheTableId(createDeltaLakeTableHandle("schema", "table", false, "location", 1)));

        // schema should be part of table id
        assertThat(metadata.getCacheTableId(createDeltaLakeTableHandle("schema", "table", false, "location", 0)))
                .isNotEqualTo(metadata.getCacheTableId(createDeltaLakeTableHandle("schema2", "table", false, "location", 0)));

        // table should be part of table id
        assertThat(metadata.getCacheTableId(createDeltaLakeTableHandle("schema", "table", false, "location", 0)))
                .isNotEqualTo(metadata.getCacheTableId(createDeltaLakeTableHandle("schema", "table2", false, "location", 0)));

        // writing queries should result in empty cacheTableId
        assertThat(metadata.getCacheTableId(createDeltaLakeTableHandle(createMetadataEntry("id", schema), ImmutableSet.of(), Optional.of(ImmutableList.of(partitionColumn)), Optional.of(UPDATE))))
                .isEmpty();

        // analyze queries should result in empty cacheTableId
        assertThat(metadata.getCacheTableId(createDeltaLakeTableHandle(
                "schema",
                "table",
                false,
                "location",
                0,
                TupleDomain.all(),
                TupleDomain.all(),
                createMetadataEntry("id", schema),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new AnalyzeHandle(FULL_REFRESH, Optional.empty(), Optional.empty())))))
                .isEmpty();
    }

    @Test
    public void testSplitId()
    {
        // different path should make ids different
        assertThat(splitManager.getCacheSplitId(createDeltaLakeSplit("path", 10, 0, 1024, 10)))
                .isNotEqualTo(splitManager.getCacheSplitId(createDeltaLakeSplit("path2", 10, 0, 1024, 10)));

        // different length should make ids different
        assertThat(splitManager.getCacheSplitId(createDeltaLakeSplit("path", 10, 0, 1024, 10)))
                .isNotEqualTo(splitManager.getCacheSplitId(createDeltaLakeSplit("path", 11, 0, 1024, 10)));

        // different start position should make ids different
        assertThat(splitManager.getCacheSplitId(createDeltaLakeSplit("path", 10, 0, 1024, 10)))
                .isNotEqualTo(splitManager.getCacheSplitId(createDeltaLakeSplit("path", 10, 1, 1024, 10)));

        // different fileSize position should make ids different
        assertThat(splitManager.getCacheSplitId(createDeltaLakeSplit("path", 10, 0, 1024, 10)))
                .isNotEqualTo(splitManager.getCacheSplitId(createDeltaLakeSplit("path", 10, 0, 512, 10)));

        // different fileModification should make ids different
        assertThat(splitManager.getCacheSplitId(createDeltaLakeSplit("path", 10, 0, 1024, 10)))
                .isNotEqualTo(splitManager.getCacheSplitId(createDeltaLakeSplit("path", 10, 0, 1024, 20)));

        // different partitionKeys should make ids different
        assertThat(splitManager.getCacheSplitId(createDeltaLakeSplit(SplitWeight.standard(), TupleDomain.all(), ImmutableMap.of("partitionKet", Optional.empty()))))
                .isNotEqualTo(splitManager.getCacheSplitId(createDeltaLakeSplit(SplitWeight.standard(), TupleDomain.all(), ImmutableMap.of())));

        // different split weight should not make ids different
        assertThat(splitManager.getCacheSplitId(createDeltaLakeSplit(SplitWeight.fromProportion(0.1), TupleDomain.all(), ImmutableMap.of())))
                .isEqualTo(splitManager.getCacheSplitId(createDeltaLakeSplit(SplitWeight.fromProportion(0.11), TupleDomain.all(), ImmutableMap.of())));

        // different row count should make ids different
        assertThat(splitManager.getCacheSplitId(createDeltaLakeSplit(SplitWeight.fromProportion(0.1), TupleDomain.all(), ImmutableMap.of(), Optional.of(10L), Optional.empty())))
                .isNotEqualTo(splitManager.getCacheSplitId(createDeltaLakeSplit(SplitWeight.fromProportion(0.1), TupleDomain.all(), ImmutableMap.of(), Optional.of(11L), Optional.empty())));

        // different deletion vector should make ids different
        assertThat(splitManager.getCacheSplitId(createDeltaLakeSplit(SplitWeight.standard(), TupleDomain.all(), ImmutableMap.of(), Optional.of(10L), Optional.of(new DeletionVectorEntry("", "", OptionalInt.of(10), 0, 0L)))))
                .isNotEqualTo(splitManager.getCacheSplitId(createDeltaLakeSplit(SplitWeight.standard(), TupleDomain.all(), ImmutableMap.of(), Optional.of(10L), Optional.of(new DeletionVectorEntry("", "", OptionalInt.of(11), 0, 0L)))));
    }

    private static DeltaLakeSplit createDeltaLakeSplit(
            SplitWeight splitWeight,
            TupleDomain<DeltaLakeColumnHandle> statisticsPredicate,
            Map<String, Optional<String>> partitionKeys)
    {
        return new DeltaLakeSplit(
                "path",
                0,
                1024,
                1024,
                Optional.of(10L),
                10,
                Optional.empty(),
                splitWeight,
                statisticsPredicate,
                partitionKeys);
    }

    private static DeltaLakeSplit createDeltaLakeSplit(
            SplitWeight splitWeight,
            TupleDomain<DeltaLakeColumnHandle> statisticsPredicate,
            Map<String, Optional<String>> partitionKeys,
            Optional<Long> rowCount,
            Optional<DeletionVectorEntry> deletionVectorEntry)
    {
        return new DeltaLakeSplit(
                "path",
                0,
                1024,
                1024,
                rowCount,
                10,
                deletionVectorEntry,
                splitWeight,
                statisticsPredicate,
                partitionKeys);
    }

    private static DeltaLakeSplit createDeltaLakeSplit(
            String path,
            long length,
            long start,
            long fileSize,
            long fileModifiedTime)
    {
        return new DeltaLakeSplit(
                path,
                start,
                length,
                fileSize,
                Optional.empty(),
                fileModifiedTime,
                Optional.empty(),
                SplitWeight.standard(),
                TupleDomain.all(),
                ImmutableMap.of());
    }

    private static DeltaLakeTableHandle createDeltaLakeTableHandle(
            String schemaName,
            String tableName,
            boolean managed,
            String location,
            long readVersion)
    {
        return createDeltaLakeTableHandle(
                schemaName,
                tableName,
                managed,
                location,
                readVersion,
                TupleDomain.all(),
                TupleDomain.all(),
                createMetadataEntry("id", "{\"fields\": [{\"name\": \"value\", \"metadata\": {}}]}"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private static DeltaLakeTableHandle createDeltaLakeTableHandle(
            MetadataEntry entry,
            Set<DeltaLakeColumnHandle> projectedColumns,
            Optional<List<DeltaLakeColumnHandle>> updatedColumns,
            Optional<DeltaLakeTableHandle.WriteType> writeType)
    {
        return createDeltaLakeTableHandle(
                "schema",
                "table",
                true,
                "location",
                0,
                TupleDomain.all(),
                TupleDomain.all(),
                entry,
                writeType,
                Optional.of(projectedColumns),
                updatedColumns,
                Optional.empty());
    }

    private static DeltaLakeTableHandle createDeltaLakeTableHandle(
            TupleDomain<DeltaLakeColumnHandle> enforcedPartitionConstraint,
            TupleDomain<DeltaLakeColumnHandle> nonPartitionConstraint)
    {
        return createDeltaLakeTableHandle(
                "schema",
                "table",
                true,
                "location",
                0,
                enforcedPartitionConstraint,
                nonPartitionConstraint,
                createMetadataEntry("id", "{\"fields\": [{\"name\": \"value\", \"metadata\": {}}]}"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private static DeltaLakeTableHandle createDeltaLakeTableHandle(
            String schemaName,
            String tableName,
            boolean managed,
            String location,
            long readVersion,
            TupleDomain<DeltaLakeColumnHandle> enforcedPartitionConstraint,
            TupleDomain<DeltaLakeColumnHandle> nonPartitionConstraint,
            MetadataEntry metadataEntry,
            Optional<DeltaLakeTableHandle.WriteType> writeType,
            Optional<Set<DeltaLakeColumnHandle>> projectedColumns,
            Optional<List<DeltaLakeColumnHandle>> updatedColumns,
            Optional<AnalyzeHandle> analyzeHandle)
    {
        return new DeltaLakeTableHandle(
                schemaName,
                tableName,
                managed,
                location,
                metadataEntry,
                new ProtocolEntry(3, 7, Optional.empty(), Optional.empty()),
                enforcedPartitionConstraint,
                nonPartitionConstraint,
                writeType,
                projectedColumns,
                updatedColumns,
                updatedColumns,
                analyzeHandle,
                readVersion);
    }

    private static MetadataEntry createMetadataEntry(String id, String schema)
    {
        return new MetadataEntry(
                id,
                "name",
                "description",
                new MetadataEntry.Format("provider", ImmutableMap.of()),
                schema,
                ImmutableList.of(),
                ImmutableMap.of(),
                0);
    }

    private static <T> JsonCodec<T> createJsonCodec(Class<T> clazz)
    {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        TypeDeserializer typeDeserializer = new TypeDeserializer(new TestingTypeManager());
        objectMapperProvider.setJsonDeserializers(
                ImmutableMap.of(
                        Block.class, new TestingBlockJsonSerde.Deserializer(new TestingBlockEncodingSerde()),
                        Type.class, typeDeserializer));
        objectMapperProvider.setJsonSerializers(ImmutableMap.of(Block.class, new TestingBlockJsonSerde.Serializer(new TestingBlockEncodingSerde())));
        return new JsonCodecFactory(objectMapperProvider).jsonCodec(clazz);
    }
}
