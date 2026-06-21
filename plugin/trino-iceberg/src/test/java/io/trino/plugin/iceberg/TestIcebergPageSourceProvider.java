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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.filesystem.local.LocalInputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.operator.FlatHashStrategyCompiler;
import io.trino.operator.NullSafeHashCompiler;
import io.trino.parquet.DiskRange;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.cache.MemoryParquetFooterCache;
import io.trino.parquet.cache.ParquetFooterCache;
import io.trino.parquet.cache.ParquetFooterCacheKey;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.ChunkedInputStream;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.iceberg.delete.DeleteFile;
import io.trino.spi.BlocksHashFactory;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.testing.TestingConnectorSession;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.airlift.units.DataSize.Unit.BYTE;
import static io.trino.hdfs.HdfsTestUtils.HDFS_ENVIRONMENT;
import static io.trino.hdfs.HdfsTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTestUtils.writeParquetFile;
import static io.trino.plugin.iceberg.ColumnIdentity.TypeCategory.PRIMITIVE;
import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.plugin.iceberg.IcebergTestUtils.FILE_IO_FACTORY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

class TestIcebergPageSourceProvider
{
    private static final OrcReaderConfig ORC_READER_CONFIG = new OrcReaderConfig();
    private static final ParquetReaderConfig PARQUET_READER_CONFIG = new ParquetReaderConfig();

    @Test
    void testMemoryTrackingWithEqualityDeletes(@TempDir Path tempDir)
            throws IOException
    {
        // Define columns
        ColumnIdentity regionkeyIdentity = new ColumnIdentity(1, "regionkey", PRIMITIVE, ImmutableList.of());
        IcebergColumnHandle regionkeyHandle = IcebergColumnHandle.optional(regionkeyIdentity).columnType(BIGINT).build();
        ColumnIdentity nameIdentity = new ColumnIdentity(2, "name", PRIMITIVE, ImmutableList.of());
        IcebergColumnHandle nameHandle = IcebergColumnHandle.optional(nameIdentity).columnType(VARCHAR).build();

        Schema tableSchema = new Schema(
                optional(regionkeyIdentity.getId(), "regionkey", Types.LongType.get()),
                optional(nameIdentity.getId(), "name", Types.StringType.get()));

        // Write data file
        Path dataFilePath = tempDir.resolve("data.parquet");
        writeParquetFileToDisk(dataFilePath,
                ImmutableList.of(BIGINT, VARCHAR),
                ImmutableList.of("regionkey", "name"),
                new Page(
                        createBigintBlock(1L, 2L, 3L),
                        createVarcharBlock("AMERICA", "ASIA", "EUROPE")));

        // Write equality delete file (deletes regionkey=1)
        Path deleteFilePath = tempDir.resolve("delete.parquet");
        writeParquetFileToDisk(
                deleteFilePath,
                ImmutableList.of(BIGINT),
                ImmutableList.of("regionkey"),
                new Page(createBigintBlock(1L)));

        LocalInputFile dataInputFile = new LocalInputFile(dataFilePath.toFile());
        LocalInputFile deleteInputFile = new LocalInputFile(deleteFilePath.toFile());

        DeleteFile equalityDelete = new DeleteFile(
                FileContent.EQUALITY_DELETES,
                deleteInputFile.location().toString(),
                FileFormat.PARQUET,
                1, // recordCount
                deleteInputFile.length(),
                ImmutableList.of(regionkeyIdentity.getId()), // equalityFieldIds
                OptionalLong.empty(),
                OptionalLong.empty(),
                1L, // dataSequenceNumber
                OptionalLong.empty(),
                Optional.empty());

        IcebergPageSourceProvider provider = createPageSourceProvider();

        assertThat(provider.getMemoryUsage()).isEqualTo(0);

        TestingConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new IcebergSessionProperties(
                        new IcebergConfig(),
                        ORC_READER_CONFIG,
                        new OrcWriterConfig(),
                        PARQUET_READER_CONFIG,
                        new ParquetWriterConfig()).getSessionProperties())
                .build();

        // Create a page source with equality deletes - the delete predicate is lazily loaded
        try (ConnectorPageSource pageSource = provider.createPageSource(
                session,
                ImmutableList.of(regionkeyHandle, nameHandle),
                tableSchema,
                PartitionSpec.unpartitioned(),
                new PartitionData(new Object[] {}),
                ImmutableList.of(equalityDelete),
                DynamicFilter.EMPTY,
                TupleDomain.all(),
                TupleDomain.all(),
                dataInputFile.location().toString(),
                0,
                dataInputFile.length(),
                dataInputFile.length(),
                3, // fileRecordCount
                PARQUET,
                new IcebergTableCredentials(ImmutableMap.of(), ImmutableList.of()),
                OptionalLong.of(0), // dataSequenceNumber
                OptionalLong.empty(),
                Optional.empty(),
                newSimpleAggregatedMemoryContext())) {
            // Memory should still be 0 before reading any pages (lazy loading)
            assertThat(provider.getMemoryUsage()).isEqualTo(0);

            // Read pages to trigger lazy loading of equality deletes
            while (!pageSource.isFinished()) {
                pageSource.getNextSourcePage();
            }

            // After reading, the equality delete filter should be loaded and tracked in memory
            assertThat(provider.getMemoryUsage()).isGreaterThan(100);
        }
    }

    @Test
    void testParquetFooterCacheMissAndHit()
            throws IOException
    {
        Slice parquetBytes = writeParquetFile(
                ParquetWriterOptions.builder().build(),
                ImmutableList.of(BIGINT),
                ImmutableList.of("regionkey"),
                ImmutableList.of(new Page(createBigintBlock(1L, 2L, 3L))));
        ParquetFooterCacheKey key = new ParquetFooterCacheKey("file:///data.parquet", parquetBytes.length());
        TestingParquetFooterCache cache = new TestingParquetFooterCache();

        RecordingParquetDataSource missDataSource = new RecordingParquetDataSource(parquetBytes);
        ParquetMetadata missMetadata = MetadataReader.readFooter(missDataSource, PARQUET_READER_CONFIG.toParquetReaderOptions(), Optional.empty(), Optional.empty(), cache, key);

        assertThat(cache.gets).isEqualTo(1);
        assertThat(cache.puts).isEqualTo(1);
        assertThat(missDataSource.tailReadLengths).isNotEmpty();

        RecordingParquetDataSource hitDataSource = new RecordingParquetDataSource(parquetBytes);
        ParquetMetadata hitMetadata = MetadataReader.readFooter(hitDataSource, PARQUET_READER_CONFIG.toParquetReaderOptions(), Optional.empty(), Optional.empty(), cache, key);

        assertThat(cache.gets).isEqualTo(2);
        assertThat(cache.puts).isEqualTo(1);
        assertThat(hitDataSource.tailReadLengths).isEmpty();
        assertThat(hitMetadata.getBlocks()).hasSameSizeAs(missMetadata.getBlocks());
    }

    @Test
    void testCorruptParquetFooterCacheFallsBack()
            throws IOException
    {
        Slice parquetBytes = writeParquetFile(
                ParquetWriterOptions.builder().build(),
                ImmutableList.of(BIGINT),
                ImmutableList.of("regionkey"),
                ImmutableList.of(new Page(createBigintBlock(1L, 2L, 3L))));
        ParquetFooterCacheKey key = new ParquetFooterCacheKey("file:///data.parquet", parquetBytes.length());
        TestingParquetFooterCache cache = new TestingParquetFooterCache();
        cache.values.put(key, Slices.allocate(8));

        RecordingParquetDataSource dataSource = new RecordingParquetDataSource(parquetBytes);
        MetadataReader.readFooter(dataSource, PARQUET_READER_CONFIG.toParquetReaderOptions(), Optional.empty(), Optional.empty(), cache, key);

        assertThat(cache.invalidations).isEqualTo(1);
        assertThat(cache.puts).isEqualTo(1);
        assertThat(dataSource.tailReadLengths).isNotEmpty();
    }

    @Test
    void testMemoryParquetFooterCacheIsBoundedByBytes()
    {
        MemoryParquetFooterCache cache = new MemoryParquetFooterCache(DataSize.of(8, BYTE));
        ParquetFooterCacheKey oversizedKey = new ParquetFooterCacheKey("file:///large.parquet", 100);
        ParquetFooterCacheKey maxSizeKey = new ParquetFooterCacheKey("file:///max.parquet", 100);

        cache.put(oversizedKey, Slices.allocate(9));
        cache.put(maxSizeKey, Slices.allocate(8));

        assertThat(cache.get(oversizedKey)).isEmpty();
        assertThat(cache.get(maxSizeKey)).isPresent();
    }

    private static void writeParquetFileToDisk(Path path, List<Type> types, List<String> columnNames, Page page)
            throws IOException
    {
        Slice parquetBytes = writeParquetFile(ParquetWriterOptions.builder().build(), types, columnNames, ImmutableList.of(page));
        Files.write(path, parquetBytes.getBytes());
    }

    private static Block createBigintBlock(long... values)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(values.length);
        for (long value : values) {
            BIGINT.writeLong(builder, value);
        }
        return builder.build();
    }

    private static Block createVarcharBlock(String... values)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, values.length);
        for (String value : values) {
            VARCHAR.writeString(builder, value);
        }
        return builder.build();
    }

    private static IcebergPageSourceProvider createPageSourceProvider()
    {
        BlocksHashFactory blocksHashFactory = new FlatHashStrategyCompiler(new TypeOperators(), new NullSafeHashCompiler(new TypeOperators())).createBlocksHashFactory();
        return new IcebergPageSourceProvider(
                new DefaultIcebergFileSystemFactory(new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS)),
                FILE_IO_FACTORY,
                new FileFormatDataSourceStats(),
                ORC_READER_CONFIG.toOrcReaderOptions(),
                PARQUET_READER_CONFIG.toParquetReaderOptions(),
                TESTING_TYPE_MANAGER,
                ParquetFooterCache.noop(),
                Optional.of(blocksHashFactory));
    }

    private static class TestingParquetFooterCache
            implements ParquetFooterCache
    {
        private final Map<ParquetFooterCacheKey, Slice> values = new HashMap<>();
        private int gets;
        private int puts;
        private int invalidations;

        @Override
        public Optional<Slice> get(ParquetFooterCacheKey key)
        {
            gets++;
            return Optional.ofNullable(values.get(key));
        }

        @Override
        public void put(ParquetFooterCacheKey key, Slice footerBytes)
        {
            puts++;
            values.put(key, footerBytes.copy());
        }

        @Override
        public void invalidate(ParquetFooterCacheKey key)
        {
            invalidations++;
            values.remove(key);
        }
    }

    private static class RecordingParquetDataSource
            implements ParquetDataSource
    {
        private final ParquetDataSourceId id = new ParquetDataSourceId("recording");
        private final Slice input;
        private final List<Integer> tailReadLengths = new ArrayList<>();
        private long readBytes;

        public RecordingParquetDataSource(Slice input)
        {
            this.input = input;
        }

        @Override
        public ParquetDataSourceId getId()
        {
            return id;
        }

        @Override
        public long getReadBytes()
        {
            return readBytes;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public long getEstimatedSize()
        {
            return input.length();
        }

        @Override
        public Slice readTail(int length)
        {
            tailReadLengths.add(length);
            int readSize = toIntExact(min(input.length(), length));
            readBytes += readSize;
            return input.slice(input.length() - readSize, readSize);
        }

        @Override
        public Slice readFully(long position, int length)
        {
            readBytes += length;
            return input.slice(toIntExact(position), length);
        }

        @Override
        public <K> Map<K, ChunkedInputStream> planRead(ListMultimap<K, DiskRange> diskRanges, AggregatedMemoryContext memoryContext)
        {
            throw new UnsupportedOperationException();
        }
    }
}
