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
package io.trino.parquet.writer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.parquet.DataPage;
import io.trino.parquet.DiskRange;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.ChunkedInputStream;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.PageReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.TestingParquetDataSource;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.Util;
import org.apache.parquet.schema.PrimitiveType;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.operator.scalar.CharacterStringCasts.varcharToVarcharSaturatedFloorCast;
import static io.trino.parquet.BloomFilterStore.hasBloomFilter;
import static io.trino.parquet.ParquetCompressionUtils.decompress;
import static io.trino.parquet.ParquetTestUtils.createParquetReader;
import static io.trino.parquet.ParquetTestUtils.createParquetWriter;
import static io.trino.parquet.ParquetTestUtils.generateInputPages;
import static io.trino.parquet.ParquetTestUtils.writeParquetFile;
import static io.trino.parquet.writer.ParquetWriterOptions.DEFAULT_BLOOM_FILTER_FPP;
import static io.trino.parquet.writer.ParquetWriters.BLOOM_FILTER_EXPECTED_ENTRIES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.toIntExact;
import static java.util.stream.Collectors.toList;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;

public class TestParquetWriter
{
    @Test
    public void testCreatedByIsParsable()
            throws VersionParser.VersionParseException, IOException
    {
        String createdBy = ParquetWriter.formatCreatedBy("test-version");
        // createdBy must start with "parquet-mr" to make Apache Hive perform timezone conversion on INT96 timestamps correctly
        // when hive.parquet.timestamp.skip.conversion is set to true.
        // Apache Hive 3.2 and above enable hive.parquet.timestamp.skip.conversion by default
        assertThat(createdBy).startsWith("parquet-mr");
        VersionParser.ParsedVersion version = VersionParser.parse(createdBy);
        assertThat(version).isNotNull();
        assertThat(version.application).isEqualTo("parquet-mr-trino");
        assertThat(version.version).isEqualTo("test-version");
        assertThat(version.appBuildHash).isEqualTo("n/a");
    }

    @Test
    public void testWrittenPageSize()
            throws IOException
    {
        List<String> columnNames = ImmutableList.of("columnA", "columnB");
        List<Type> types = ImmutableList.of(INTEGER, BIGINT);

        // Write a file with many small input pages and parquet max page size of 20Kb
        ParquetDataSource dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder()
                                .setMaxPageSize(DataSize.ofBytes(20 * 1024))
                                .build(),
                        types,
                        columnNames,
                        generateInputPages(types, 100, 1000)),
                new ParquetReaderOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        assertThat(parquetMetadata.getBlocks()).hasSize(1);
        assertThat(parquetMetadata.getBlocks().get(0).rowCount()).isEqualTo(100 * 1000);

        ColumnChunkMetadata chunkMetaData = parquetMetadata.getBlocks().get(0).columns().get(0);
        DiskRange range = new DiskRange(chunkMetaData.getStartingPos(), chunkMetaData.getTotalSize());
        Map<Integer, ChunkedInputStream> chunkReader = dataSource.planRead(ImmutableListMultimap.of(0, range), newSimpleAggregatedMemoryContext());

        PageReader pageReader = PageReader.createPageReader(
                new ParquetDataSourceId("test"),
                chunkReader.get(0),
                chunkMetaData,
                new ColumnDescriptor(new String[] {"columna"}, new PrimitiveType(REQUIRED, INT32, "columna"), 0, 0),
                null,
                Optional.empty());

        pageReader.readDictionaryPage();
        assertThat(pageReader.hasNext()).isTrue();
        int pagesRead = 0;
        DataPage dataPage;
        while (pageReader.hasNext()) {
            dataPage = pageReader.readPage();
            pagesRead++;
            if (!pageReader.hasNext()) {
                break; // skip last page size validation
            }
            assertThat(dataPage.getValueCount()).isBetween(4500, 5500);
        }
        assertThat(pagesRead).isGreaterThan(10);
    }

    @Test
    public void testWrittenPageValueCount()
            throws IOException
    {
        List<String> columnNames = ImmutableList.of("columnA", "columnB");
        List<Type> types = ImmutableList.of(INTEGER, BIGINT);

        // Write a file with many small input pages and parquet max page value count of 1000
        ParquetDataSource dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder()
                                .setMaxPageValueCount(1000)
                                .setBatchSize(100)
                                .build(),
                        types,
                        columnNames,
                        generateInputPages(types, 100, 1000)),
                new ParquetReaderOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        assertThat(parquetMetadata.getBlocks()).hasSize(1);
        assertThat(parquetMetadata.getBlocks().get(0).rowCount()).isEqualTo(100 * 1000);

        ColumnChunkMetadata columnAMetaData = parquetMetadata.getBlocks().get(0).columns().get(0);
        ColumnChunkMetadata columnBMetaData = parquetMetadata.getBlocks().get(0).columns().get(1);
        Map<Integer, ChunkedInputStream> chunkReader = dataSource.planRead(
                ImmutableListMultimap.of(
                        0, new DiskRange(columnAMetaData.getStartingPos(), columnAMetaData.getTotalSize()),
                        1, new DiskRange(columnBMetaData.getStartingPos(), columnBMetaData.getTotalSize())),
                newSimpleAggregatedMemoryContext());

        PageReader pageReader = PageReader.createPageReader(
                new ParquetDataSourceId("test"),
                chunkReader.get(0),
                columnAMetaData,
                new ColumnDescriptor(new String[] {"columna"}, new PrimitiveType(REQUIRED, INT32, "columna"), 0, 0),
                null,
                Optional.empty());

        pageReader.readDictionaryPage();
        assertThat(pageReader.hasNext()).isTrue();
        int pagesRead = 0;
        DataPage dataPage;
        while (pageReader.hasNext()) {
            dataPage = pageReader.readPage();
            pagesRead++;
            assertThat(dataPage.getValueCount()).isEqualTo(1000);
        }
        assertThat(pagesRead).isEqualTo(100);

        pageReader = PageReader.createPageReader(
                new ParquetDataSourceId("test"),
                chunkReader.get(1),
                columnAMetaData,
                new ColumnDescriptor(new String[] {"columnb"}, new PrimitiveType(REQUIRED, INT64, "columnb"), 0, 0),
                null,
                Optional.empty());

        pageReader.readDictionaryPage();
        assertThat(pageReader.hasNext()).isTrue();
        pagesRead = 0;
        while (pageReader.hasNext()) {
            dataPage = pageReader.readPage();
            pagesRead++;
            assertThat(dataPage.getValueCount()).isEqualTo(1000);
        }
        assertThat(pagesRead).isEqualTo(100);
    }

    @Test
    public void testLargeStringTruncation()
            throws IOException
    {
        List<String> columnNames = ImmutableList.of("columnA", "columnB");
        List<Type> types = ImmutableList.of(VARCHAR, VARCHAR);

        Slice minA = Slices.utf8Slice("abc".repeat(300)); // within truncation threshold
        Block blockA = VARCHAR.createBlockBuilder(null, 2)
                .writeEntry(minA)
                .writeEntry(Slices.utf8Slice("y".repeat(3200))) // bigger than truncation threshold
                .build();

        String threeByteCodePoint = new String(Character.toChars(0x20AC));
        String maxCodePoint = new String(Character.toChars(Character.MAX_CODE_POINT));
        Slice minB = Slices.utf8Slice(threeByteCodePoint.repeat(300)); // truncation in middle of unicode bytes
        Block blockB = VARCHAR.createBlockBuilder(null, 2)
                .writeEntry(minB)
                // start with maxCodePoint to make it max value in stats
                // last character for truncation is maxCodePoint
                .writeEntry(Slices.utf8Slice(maxCodePoint + "d".repeat(1017) + maxCodePoint))
                .build();

        ParquetDataSource dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder().build(),
                        types,
                        columnNames,
                        ImmutableList.of(new Page(2, blockA, blockB))),
                new ParquetReaderOptions());

        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        BlockMetadata blockMetaData = getOnlyElement(parquetMetadata.getBlocks());

        ColumnChunkMetadata chunkMetaData = blockMetaData.columns().get(0);
        assertThat(chunkMetaData.getStatistics().getMinBytes()).isEqualTo(minA.getBytes());
        Slice truncatedMax = Slices.utf8Slice("y".repeat(1023) + "z");
        assertThat(chunkMetaData.getStatistics().getMaxBytes()).isEqualTo(truncatedMax.getBytes());

        chunkMetaData = blockMetaData.columns().get(1);
        Slice truncatedMin = varcharToVarcharSaturatedFloorCast(1024, minB);
        assertThat(chunkMetaData.getStatistics().getMinBytes()).isEqualTo(truncatedMin.getBytes());
        truncatedMax = Slices.utf8Slice(maxCodePoint + "d".repeat(1016) + "e");
        assertThat(chunkMetaData.getStatistics().getMaxBytes()).isEqualTo(truncatedMax.getBytes());
    }

    @Test
    public void testColumnReordering()
            throws IOException
    {
        List<String> columnNames = ImmutableList.of("columnA", "columnB", "columnC", "columnD");
        List<Type> types = ImmutableList.of(BIGINT, TINYINT, INTEGER, DecimalType.createDecimalType(12));

        // Write a file with many row groups
        ParquetDataSource dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder()
                                .setMaxBlockSize(DataSize.ofBytes(20 * 1024))
                                .build(),
                        types,
                        columnNames,
                        generateInputPages(types, 100, 100)),
                new ParquetReaderOptions());

        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        assertThat(parquetMetadata.getBlocks().size()).isGreaterThanOrEqualTo(10);
        for (BlockMetadata blockMetaData : parquetMetadata.getBlocks()) {
            // Verify that the columns are stored in the same order as the metadata
            List<Long> offsets = blockMetaData.columns().stream()
                    .map(ColumnChunkMetadata::getFirstDataPageOffset)
                    .collect(toImmutableList());
            assertThat(offsets).isSorted();
        }
    }

    @Test
    public void testWriterMemoryAccounting()
            throws IOException
    {
        List<String> columnNames = ImmutableList.of("columnA", "columnB");
        List<Type> types = ImmutableList.of(INTEGER, INTEGER);

        ParquetWriter writer = createParquetWriter(
                new ByteArrayOutputStream(),
                ParquetWriterOptions.builder()
                        .setMaxPageSize(DataSize.ofBytes(1024))
                        .build(),
                types,
                columnNames,
                CompressionCodec.SNAPPY);
        List<io.trino.spi.Page> inputPages = generateInputPages(types, 1000, 100);

        long previousRetainedBytes = 0;
        for (io.trino.spi.Page inputPage : inputPages) {
            checkArgument(types.size() == inputPage.getChannelCount());
            writer.write(inputPage);
            long currentRetainedBytes = writer.getRetainedBytes();
            assertThat(currentRetainedBytes).isGreaterThanOrEqualTo(previousRetainedBytes);
            previousRetainedBytes = currentRetainedBytes;
        }
        assertThat(previousRetainedBytes).isGreaterThanOrEqualTo(2 * Integer.BYTES * 1000 * 100);
        writer.close();
        assertThat(previousRetainedBytes - writer.getRetainedBytes()).isGreaterThanOrEqualTo(2 * Integer.BYTES * 1000 * 100);
    }

    @Test
    public void testDictionaryPageOffset()
            throws IOException
    {
        List<String> columnNames = ImmutableList.of("column");
        List<Type> types = ImmutableList.of(INTEGER);

        // Write a file with dictionary encoded data
        ParquetDataSource dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder().build(),
                        types,
                        columnNames,
                        generateInputPages(types, 100, 100)),
                new ParquetReaderOptions());

        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        assertThat(parquetMetadata.getBlocks().size()).isGreaterThanOrEqualTo(1);
        for (BlockMetadata blockMetaData : parquetMetadata.getBlocks()) {
            ColumnChunkMetadata chunkMetaData = getOnlyElement(blockMetaData.columns());
            assertThat(chunkMetaData.getDictionaryPageOffset()).isGreaterThan(0);
            int dictionaryPageSize = toIntExact(chunkMetaData.getFirstDataPageOffset() - chunkMetaData.getDictionaryPageOffset());
            assertThat(dictionaryPageSize).isGreaterThan(0);
            assertThat(chunkMetaData.getEncodingStats().hasDictionaryPages()).isTrue();
            assertThat(chunkMetaData.getEncodingStats().hasDictionaryEncodedPages()).isTrue();
            assertThat(chunkMetaData.getEncodingStats().hasNonDictionaryEncodedPages()).isFalse();
            assertThat(hasBloomFilter(chunkMetaData)).isFalse();

            // verify reading dictionary page
            SliceInput inputStream = dataSource.readFully(chunkMetaData.getStartingPos(), dictionaryPageSize).getInput();
            PageHeader pageHeader = Util.readPageHeader(inputStream);
            assertThat(pageHeader.getType()).isEqualTo(PageType.DICTIONARY_PAGE);
            assertThat(pageHeader.getDictionary_page_header().getNum_values()).isEqualTo(100);
            Slice compressedData = inputStream.readSlice(pageHeader.getCompressed_page_size());
            Slice uncompressedData = decompress(
                    new ParquetDataSourceId("test"),
                    chunkMetaData.getCodec().getParquetCompressionCodec(),
                    compressedData,
                    pageHeader.getUncompressed_page_size());
            int[] ids = new int[100];
            uncompressedData.getInts(0, ids, 0, 100);
            for (int i = 0; i < 100; i++) {
                assertThat(ids[i]).isEqualTo(i);
            }
        }
    }

    @Test
    void testRowGroupOffset()
            throws IOException
    {
        // Write a file with 100 rows per row-group
        List<String> columnNames = ImmutableList.of("columnA", "columnB");
        List<Type> types = ImmutableList.of(INTEGER, BIGINT);

        ParquetDataSource dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder()
                                .setMaxBlockSize(DataSize.ofBytes(1000))
                                .build(),
                        types,
                        columnNames,
                        generateInputPages(types, 100, 10)),
                new ParquetReaderOptions());

        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        List<BlockMetadata> blocks = parquetMetadata.getBlocks();
        assertThat(blocks.size()).isGreaterThan(1);

        List<RowGroup> rowGroups = parquetMetadata.getParquetMetadata().getRow_groups();
        assertThat(rowGroups.size()).isEqualTo(blocks.size());
        for (int rowGroupIndex = 0; rowGroupIndex < rowGroups.size(); rowGroupIndex++) {
            RowGroup rowGroup = rowGroups.get(rowGroupIndex);
            assertThat(rowGroup.isSetFile_offset()).isTrue();
            BlockMetadata blockMetadata = blocks.get(rowGroupIndex);
            assertThat(blockMetadata.getStartingPos()).isEqualTo(rowGroup.getFile_offset());
        }
    }

    @ParameterizedTest
    @MethodSource("testWriteBloomFiltersParams")
    public void testWriteBloomFilters(Type type, List<?> data)
            throws IOException
    {
        List<String> columnNames = ImmutableList.of("columnA", "columnB");
        List<Type> types = ImmutableList.of(type, type);
        ParquetDataSource dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder()
                                .setMaxBlockSize(DataSize.ofBytes(4 * 1024))
                                .setBloomFilterColumns(ImmutableSet.copyOf(columnNames))
                                .build(),
                        types,
                        columnNames,
                        generateInputPages(types, 100, data)),
                new ParquetReaderOptions());

        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        // Check that bloom filters are right after each other
        int bloomFilterSize = Integer.highestOneBit(BlockSplitBloomFilter.optimalNumOfBits(BLOOM_FILTER_EXPECTED_ENTRIES, DEFAULT_BLOOM_FILTER_FPP) / 8) << 1;
        for (BlockMetadata block : parquetMetadata.getBlocks()) {
            for (int i = 0; i < block.columns().size(); i++) {
                ColumnChunkMetadata chunkMetaData = block.columns().get(i);
                assertThat(hasBloomFilter(chunkMetaData)).isTrue();
                assertThat(chunkMetaData.getEncodingStats().hasDictionaryPages()).isFalse();
                assertThat(chunkMetaData.getEncodingStats().hasDictionaryEncodedPages()).isFalse();
                assertThat(chunkMetaData.getEncodingStats().hasNonDictionaryEncodedPages()).isTrue();

                if (i < block.columns().size() - 1) {
                    assertThat(chunkMetaData.getBloomFilterOffset() + bloomFilterSize + 17) // + 17 bytes for Bloom filter metadata
                            .isEqualTo(block.columns().get(i + 1).getBloomFilterOffset());
                }
            }
        }
        int rowGroupCount = parquetMetadata.getBlocks().size();
        assertThat(rowGroupCount).isGreaterThanOrEqualTo(2);

        TupleDomain<String> predicate = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        "columnA", Domain.singleValue(type, data.get(data.size() / 2))));
        try (ParquetReader reader = createParquetReader(dataSource, parquetMetadata, new ParquetReaderOptions().withBloomFilter(true), newSimpleAggregatedMemoryContext(), types, columnNames, predicate)) {
            Page page = reader.nextPage();
            int rowsRead = 0;
            while (page != null) {
                rowsRead += page.getPositionCount();
                page = reader.nextPage();
            }
            assertThat(rowsRead).isCloseTo(data.size() / rowGroupCount, Percentage.withPercentage(20));
        }

        try (ParquetReader reader = createParquetReader(dataSource, parquetMetadata, new ParquetReaderOptions().withBloomFilter(false), newSimpleAggregatedMemoryContext(), types, columnNames, predicate)) {
            Page page = reader.nextPage();
            int rowsRead = 0;
            while (page != null) {
                rowsRead += page.getPositionCount();
                page = reader.nextPage();
            }
            assertThat(rowsRead).isEqualTo(data.size());
        }
    }

    @Test
    void testBloomFilterWithDictionaryFallback()
            throws IOException
    {
        List<String> columnNames = ImmutableList.of("column");
        List<Type> types = ImmutableList.of(BIGINT);
        ParquetDataSource dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder()
                                .setMaxPageValueCount(200)
                                .setBloomFilterColumns(ImmutableSet.copyOf(columnNames))
                                .build(),
                        types,
                        columnNames,
                        ImmutableList.<io.trino.spi.Page>builder()
                                .addAll(generateInputPages(types, 10, 10))
                                // Max size of dictionary page is 1024 * 1024
                                .addAll(generateInputPages(types, 200, shuffle(new Random(42), (1024 * 1025) / Long.BYTES)))
                                .build()),
                new ParquetReaderOptions());

        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        BlockMetadata blockMetaData = getOnlyElement(parquetMetadata.getBlocks());
        ColumnChunkMetadata chunkMetaData = getOnlyElement(blockMetaData.columns());
        assertThat(chunkMetaData.getEncodingStats().hasDictionaryPages()).isTrue();
        assertThat(chunkMetaData.getEncodingStats().hasDictionaryEncodedPages()).isTrue();
        assertThat(chunkMetaData.getEncodingStats().hasNonDictionaryEncodedPages()).isTrue();
        assertThat(hasBloomFilter(chunkMetaData)).isTrue();
    }

    public static Stream<Arguments> testWriteBloomFiltersParams()
    {
        int size = 2000;
        Random random = new Random(42);
        return Stream.of(
                Arguments.of(INTEGER, shuffle(random, size)),
                Arguments.of(BIGINT, shuffle(random, size)),
                Arguments.of(REAL, shuffle(random, size).stream().map(x -> (long) floatToRawIntBits((float) x)).toList()),
                Arguments.of(DOUBLE, shuffle(random, size).stream().map(Long::doubleValue).toList()),
                Arguments.of(VARBINARY, shuffle(random, size).stream().map(i -> Slices.utf8Slice(i.toString())).toList()),
                Arguments.of(VARCHAR, shuffle(random, size).stream().map(i -> Slices.utf8Slice(i.toString())).toList()),
                // This should be UUID, but we can only read, not write UUID natively
                Arguments.of(VARBINARY, shuffle(random, size).stream().map(i -> javaUuidToTrinoUuid(new java.util.UUID(i, i))).toList()));
    }

    private static List<Long> shuffle(Random random, int size)
    {
        List<Long> shuffledData = LongStream.range(0, size).boxed().collect(toList());
        Collections.shuffle(shuffledData, random);
        return shuffledData;
    }
}
