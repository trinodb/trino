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
import io.airlift.slice.Slice;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.filesystem.local.LocalInputFile;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.iceberg.delete.DeleteFile;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
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
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.parquet.ParquetTestUtils.writeParquetFile;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.plugin.iceberg.ColumnIdentity.TypeCategory.PRIMITIVE;
import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.plugin.iceberg.IcebergTestUtils.FILE_IO_FACTORY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
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
        writeParquetFileToDisk(deleteFilePath,
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
                PartitionData.toJson(new PartitionData(new Object[] {})),
                PARQUET,
                ImmutableMap.of(),
                0L, // dataSequenceNumber
                Optional.empty())) {
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
        return new IcebergPageSourceProvider(
                new DefaultIcebergFileSystemFactory(new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS)),
                FILE_IO_FACTORY,
                new FileFormatDataSourceStats(),
                ORC_READER_CONFIG.toOrcReaderOptions(),
                PARQUET_READER_CONFIG.toParquetReaderOptions(),
                TESTING_TYPE_MANAGER);
    }
}
