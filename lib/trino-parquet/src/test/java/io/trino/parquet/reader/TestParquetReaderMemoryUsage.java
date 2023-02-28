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
package io.trino.parquet.reader;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.Field;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.writer.ParquetSchemaConverter;
import io.trino.parquet.writer.ParquetWriter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.type.Type;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.MessageColumnIO;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTypeUtils.constructField;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnByName;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.util.Collections.nCopies;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.joda.time.DateTimeZone.UTC;

public class TestParquetReaderMemoryUsage
{
    @Test
    public void testColumnReaderMemoryUsage()
            throws IOException
    {
        // Write a file with 100 rows per row-group
        List<String> columnNames = ImmutableList.of("columnA", "columnB");
        List<Type> types = ImmutableList.of(INTEGER, BIGINT);

        ParquetDataSource dataSource = new TestingParquetDataSource(writeParquetFile(types, columnNames), new ParquetReaderOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        assertThat(parquetMetadata.getBlocks().size()).isGreaterThan(1);
        // Verify file has only non-dictionary encodings as dictionary memory usage is already tested in TestFlatColumnReader#testMemoryUsage
        parquetMetadata.getBlocks().forEach(block -> {
            block.getColumns()
                    .forEach(columnChunkMetaData -> assertThat(columnChunkMetaData.getEncodingStats().hasDictionaryEncodedPages()).isFalse());
            assertThat(block.getRowCount()).isEqualTo(100);
        });

        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        ParquetReader reader = createParquetReader(dataSource, parquetMetadata, memoryContext, types, columnNames);

        Page page = reader.nextPage();
        assertThat(page.getBlock(0)).isInstanceOf(LazyBlock.class);
        assertThat(memoryContext.getBytes()).isEqualTo(0);
        page.getBlock(0).getLoadedBlock();
        // Memory usage due to reading data and decoding parquet page of 1st block
        long initialMemoryUsage = memoryContext.getBytes();
        assertThat(initialMemoryUsage).isGreaterThan(0);

        // Memory usage due to decoding parquet page of 2nd block
        page.getBlock(1).getLoadedBlock();
        long currentMemoryUsage = memoryContext.getBytes();
        assertThat(currentMemoryUsage).isGreaterThan(initialMemoryUsage);

        // Memory usage does not change until next row group (1 page per row-group)
        long rowGroupRowCount = parquetMetadata.getBlocks().get(0).getRowCount();
        int rowsRead = page.getPositionCount();
        while (rowsRead < rowGroupRowCount) {
            rowsRead += reader.nextPage().getPositionCount();
            if (rowsRead <= rowGroupRowCount) {
                assertThat(memoryContext.getBytes()).isEqualTo(currentMemoryUsage);
            }
        }

        // New row-group should release memory from old column readers, while using some memory for data read
        reader.nextPage();
        assertThat(memoryContext.getBytes()).isBetween(1L, currentMemoryUsage - 1);

        reader.close();
        assertThat(memoryContext.getBytes()).isEqualTo(0);
    }

    private static Slice writeParquetFile(List<Type> types, List<String> columnNames)
            throws IOException
    {
        checkArgument(types.size() == columnNames.size());
        ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(types, columnNames, false, false);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ParquetWriter writer = new ParquetWriter(
                outputStream,
                schemaConverter.getMessageType(),
                schemaConverter.getPrimitiveTypes(),
                ParquetWriterOptions.builder()
                        .setMaxPageSize(DataSize.ofBytes(100))
                        .setMaxBlockSize(DataSize.ofBytes(1))
                        .build(),
                CompressionCodec.SNAPPY,
                "test-version",
                false,
                Optional.of(DateTimeZone.getDefault()),
                Optional.empty());

        for (io.trino.spi.Page inputPage : generateInputPages(types, 100, 5)) {
            checkArgument(types.size() == inputPage.getChannelCount());
            writer.write(inputPage);
        }
        writer.close();
        return Slices.wrappedBuffer(outputStream.toByteArray());
    }

    private static List<io.trino.spi.Page> generateInputPages(List<Type> types, int positionsPerPage, int pageCount)
    {
        ImmutableList.Builder<io.trino.spi.Page> pagesBuilder = ImmutableList.builder();
        for (int i = 0; i < pageCount; i++) {
            List<Block> blocks = types.stream()
                    .map(type -> generateBlock(type, positionsPerPage))
                    .collect(toImmutableList());
            pagesBuilder.add(new Page(blocks.toArray(Block[]::new)));
        }
        return pagesBuilder.build();
    }

    private static Block generateBlock(Type type, int positions)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, positions);
        for (int i = 0; i < positions; i++) {
            writeNativeValue(type, blockBuilder, (long) i);
        }
        return blockBuilder.build();
    }

    private static ParquetReader createParquetReader(
            ParquetDataSource input,
            ParquetMetadata parquetMetadata,
            AggregatedMemoryContext memoryContext,
            List<Type> types,
            List<String> columnNames)
            throws IOException
    {
        org.apache.parquet.hadoop.metadata.FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
        MessageColumnIO messageColumnIO = getColumnIO(fileMetaData.getSchema(), fileMetaData.getSchema());
        ImmutableList.Builder<Field> columnFields = ImmutableList.builder();
        for (int i = 0; i < types.size(); i++) {
            columnFields.add(constructField(
                    types.get(i),
                    lookupColumnByName(messageColumnIO, columnNames.get(i)))
                    .orElseThrow());
        }
        long nextStart = 0;
        ImmutableList.Builder<Long> blockStartsBuilder = ImmutableList.builder();
        for (BlockMetaData block : parquetMetadata.getBlocks()) {
            blockStartsBuilder.add(nextStart);
            nextStart += block.getRowCount();
        }
        List<Long> blockStarts = blockStartsBuilder.build();
        return new ParquetReader(
                Optional.ofNullable(fileMetaData.getCreatedBy()),
                columnFields.build(),
                parquetMetadata.getBlocks(),
                blockStarts,
                input,
                UTC,
                memoryContext,
                new ParquetReaderOptions(),
                exception -> {
                    throwIfUnchecked(exception);
                    return new RuntimeException(exception);
                },
                Optional.empty(),
                nCopies(blockStarts.size(), Optional.empty()),
                Optional.empty());
    }
}
