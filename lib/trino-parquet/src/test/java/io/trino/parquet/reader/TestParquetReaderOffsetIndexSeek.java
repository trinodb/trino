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
import io.trino.parquet.DataPageV1;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.PageLocation;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndexBuilder;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Random;
import java.util.function.BiPredicate;

import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.ParquetEncoding.RLE;
import static io.trino.parquet.ParquetTestUtils.createParquetReader;
import static io.trino.parquet.ParquetTestUtils.writeParquetFile;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.Math.toIntExact;
import static org.apache.parquet.format.CompressionCodec.UNCOMPRESSED;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.apache.parquet.schema.LogicalTypeAnnotation.listType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestParquetReaderOffsetIndexSeek
{
    private static final int ROWS = 20_000;
    private static final int ROW_GROUP_ROWS = 6_000;
    private static final ArrayType ARRAY_TYPE = new ArrayType(BIGINT);
    private static final ArrayType NESTED_ARRAY_TYPE = new ArrayType(ARRAY_TYPE);
    private static final MapType MAP_TYPE = new MapType(BIGINT, BIGINT, new TypeOperators());
    private static final RowType ROW_TYPE = RowType.rowType(RowType.field("a", BIGINT), RowType.field("b", BIGINT));
    private static final List<Type> TYPES = ImmutableList.of(BIGINT, ARRAY_TYPE, NESTED_ARRAY_TYPE, MAP_TYPE, ROW_TYPE);
    private static final List<String> COLUMN_NAMES = ImmutableList.of("id", "values", "nested", "map", "row");

    @Test
    void testSkippingPagesOfRepeatedColumns()
            throws IOException
    {
        // only the nested columns are read, so that the pages skipped by the flat column reader are not counted
        Page expected = createExpectedPage();
        Page repeatedColumns = expected.getColumns(1, 2, 3, 4);
        List<Type> types = TYPES.subList(1, TYPES.size());
        List<String> columnNames = COLUMN_NAMES.subList(1, COLUMN_NAMES.size());
        int totalPages = read(createDataSource(expected), types, columnNames, repeatedColumns, (_, _) -> true).dataPageReadCount();
        ReadResult result = read(createDataSource(expected), types, columnNames, repeatedColumns, TestParquetReaderOffsetIndexSeek::isEndOfRowGroup);
        assertThat(result.readRows()).isGreaterThan(0);
        assertThat(result.dataPageReadCount()).isLessThan(totalPages);
    }

    @Test
    void testSkippedPagesAreNotRead()
            throws IOException
    {
        assertSkippedPagesAreNotRead(DataSize.of(48, KILOBYTE));
        // the offset indexes are not in the footer tail, so they are read separately
        assertSkippedPagesAreNotRead(DataSize.ofBytes(8));
    }

    private static void assertSkippedPagesAreNotRead(DataSize footerReadSize)
            throws IOException
    {
        // The buffer is smaller than the column chunks, so they are read in parts and the parts covered by skipped pages are not read.
        // The id column is read lazily too, so pages of a flat column are skipped as well.
        ParquetReaderOptions options = ParquetReaderOptions.builder()
                .withMaxReadBlockRowCount(50)
                .withMaxBufferSize(DataSize.of(4, KILOBYTE))
                .withFooterReadSize(footerReadSize)
                .build();
        Page expected = createExpectedPage();
        Page reordered = expected.getColumns(1, 2, 3, 4, 0);
        List<Type> types = ImmutableList.of(ARRAY_TYPE, NESTED_ARRAY_TYPE, MAP_TYPE, ROW_TYPE, BIGINT);
        List<String> columnNames = ImmutableList.of("values", "nested", "map", "row", "id");
        ReadResult all = read(createDataSource(expected, options), options, types, columnNames, reordered, (_, _) -> true);
        ReadResult endOfRowGroups = read(createDataSource(expected, options), options, types, columnNames, reordered, TestParquetReaderOffsetIndexSeek::isEndOfRowGroup);
        assertThat(endOfRowGroups.readRows()).isGreaterThan(0);
        // dictionary pages are always read
        assertThat(endOfRowGroups.readBytes()).isLessThan(all.readBytes() * 3 / 4);

        // The repeated columns would hide the savings of the flat column, so the bytes read for the id column alone are compared
        Page withId = expected.getColumns(1, 0);
        Page withoutId = expected.getColumns(1);
        List<Type> withIdTypes = ImmutableList.of(ARRAY_TYPE, BIGINT);
        List<String> withIdNames = ImmutableList.of("values", "id");
        List<Type> withoutIdTypes = ImmutableList.of(ARRAY_TYPE);
        List<String> withoutIdNames = ImmutableList.of("values");
        long idAll = read(createDataSource(expected, options), options, withIdTypes, withIdNames, withId, (_, _) -> true).readBytes()
                - read(createDataSource(expected, options), options, withoutIdTypes, withoutIdNames, withoutId, (_, _) -> true).readBytes();
        long idEndOfRowGroups = read(createDataSource(expected, options), options, withIdTypes, withIdNames, withId, TestParquetReaderOffsetIndexSeek::isEndOfRowGroup).readBytes()
                - read(createDataSource(expected, options), options, withoutIdTypes, withoutIdNames, withoutId, TestParquetReaderOffsetIndexSeek::isEndOfRowGroup).readBytes();
        assertThat(idEndOfRowGroups).isLessThan(idAll * 3 / 4);

        Random random = new Random(11);
        for (int iteration = 0; iteration < 20; iteration++) {
            double probability = random.nextDouble();
            long seed = random.nextLong();
            read(createDataSource(expected, options), options, types, columnNames, reordered, (row, column) -> new Random(seed + row * 31L + column).nextDouble() < probability);
        }
    }

    @Test
    void testOffsetIndexNotReadForSmallColumnChunks()
            throws IOException
    {
        // The offset indexes are not in the tail of the file read with the footer, and the column chunks are smaller than the buffer,
        // so the offset indexes are not read and the pages are decoded to skip them
        ParquetReaderOptions options = ParquetReaderOptions.builder()
                .withMaxReadBlockRowCount(50)
                .withFooterReadSize(DataSize.ofBytes(8))
                .build();
        Page expected = createExpectedPage();
        Page repeatedColumns = expected.getColumns(1, 2, 3, 4);
        List<Type> types = TYPES.subList(1, TYPES.size());
        List<String> columnNames = COLUMN_NAMES.subList(1, COLUMN_NAMES.size());
        Slice file = writeTrinoFile(expected);
        ParquetMetadata metadata = MetadataReader.readFooter(new TestingParquetDataSource(file, options), Optional.empty());
        assertThat(metadata.getBlocks())
                .flatMap(BlockMetadata::columns)
                .allSatisfy(column -> assertThat(column.getOffsetIndexReference()).isNotNull());

        List<long[]> reads = new ArrayList<>();
        ParquetDataSource dataSource = new TestingParquetDataSource(file, options)
        {
            @Override
            protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
                    throws IOException
            {
                reads.add(new long[] {position, bufferLength});
                super.readInternal(position, buffer, bufferOffset, bufferLength);
            }
        };
        int totalPages = read(createDataSource(expected, options), options, types, columnNames, repeatedColumns, (_, _) -> true).dataPageReadCount();
        ReadResult result = read(dataSource, options, types, columnNames, repeatedColumns, TestParquetReaderOffsetIndexSeek::isEndOfRowGroup);
        assertThat(result.readRows()).isGreaterThan(0);
        assertThat(result.dataPageReadCount()).isEqualTo(totalPages);
        // the footer is read with two requests and the data with a single one, as the file is smaller than the buffer
        assertThat(reads).hasSize(3);
    }

    @Test
    void testRandomSkipPatterns()
            throws IOException
    {
        Random random = new Random(42);
        for (int iteration = 0; iteration < 20; iteration++) {
            double probability = random.nextDouble();
            long seed = random.nextLong();
            read((row, column) -> new Random(seed + row * 31L + column).nextDouble() < probability);
        }
    }

    @Test
    void testFileWrittenByParquetJava(@TempDir Path directory)
            throws IOException
    {
        MessageType schema = Types.buildMessage()
                .required(INT64).named("id")
                .optionalGroup().as(listType())
                .repeatedGroup()
                .optional(INT64).named("element")
                .named("list")
                .named("values")
                .named("test");
        List<Type> types = ImmutableList.of(BIGINT, ARRAY_TYPE);
        List<String> columnNames = ImmutableList.of("id", "values");
        Path file = directory.resolve("values.parquet");
        ArrayBlockBuilder arrays = ARRAY_TYPE.createBlockBuilder(null, ROWS);
        BlockBuilder ids = BIGINT.createBlockBuilder(null, ROWS);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
                .withType(schema)
                .withWriteMode(OVERWRITE)
                .withRowGroupRowCountLimit(ROW_GROUP_ROWS)
                .withPageRowCountLimit(40)
                .build()) {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            for (long rowIndex = 0; rowIndex < ROWS; rowIndex++) {
                long row = rowIndex;
                Group group = factory.newGroup().append("id", row);
                BIGINT.writeLong(ids, row);
                if (row % 11 == 0) {
                    arrays.appendNull();
                }
                else {
                    Group list = group.addGroup("values");
                    long length = row % 7 == 0 ? 0 : row % 5 + 1;
                    for (long i = 0; i < length; i++) {
                        list.addGroup("list").append("element", row * 10 + i);
                    }
                    arrays.buildEntry(elements -> {
                        for (long i = 0; i < length; i++) {
                            BIGINT.writeLong(elements, row * 10 + i);
                        }
                    });
                }
                writer.write(group);
            }
        }
        Page expected = new Page(ids.build(), arrays.build());

        // only the repeated column is read, so that the pages skipped by the flat column reader are not counted
        List<Type> repeatedTypes = ImmutableList.of(ARRAY_TYPE);
        List<String> repeatedColumnNames = ImmutableList.of("values");
        Page repeatedColumns = expected.getColumns(1);
        int totalPages = readFile(file, repeatedTypes, repeatedColumnNames, repeatedColumns, (_, _) -> true).dataPageReadCount();
        assertThat(readFile(file, repeatedTypes, repeatedColumnNames, repeatedColumns, TestParquetReaderOffsetIndexSeek::isEndOfRowGroup).dataPageReadCount()).isLessThan(totalPages);
        Random random = new Random(7);
        for (int iteration = 0; iteration < 20; iteration++) {
            double probability = random.nextDouble();
            long seed = random.nextLong();
            readFile(file, types, columnNames, expected, (row, column) -> new Random(seed + row * 31L + column).nextDouble() < probability);
        }
    }

    // The columns are read only at the end of every row group, so the readers have to skip over most of its pages.
    // A row group whose columns are not read at all is skipped without reading any of its pages.
    private static boolean isEndOfRowGroup(int row, int column)
    {
        return row % ROW_GROUP_ROWS >= ROW_GROUP_ROWS - 500 || row >= ROWS - 500;
    }

    private static ReadResult readFile(Path file, List<Type> types, List<String> columnNames, Page expected, BiPredicate<Integer, Integer> readColumn)
            throws IOException
    {
        // the data source is closed together with the reader
        return read(new FileParquetDataSource(file.toFile(), ParquetReaderOptions.defaultOptions()), types, columnNames, expected, readColumn);
    }

    @Test
    void testOffsetIndexIsReadableByParquetJava(@TempDir Path directory)
            throws IOException
    {
        Path file = directory.resolve("trino.parquet");
        Files.write(file, writeTrinoFile(createExpectedPage()).getBytes());

        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            assertThat(reader.getRecordCount()).isEqualTo(ROWS);
            assertThat(reader.getRowGroups().size()).isGreaterThan(1);
            for (BlockMetaData block : reader.getRowGroups()) {
                for (ColumnChunkMetaData column : block.getColumns()) {
                    OffsetIndex offsetIndex = reader.readOffsetIndex(column);
                    assertThat(offsetIndex).isNotNull();
                    assertThat(offsetIndex.getPageCount()).isGreaterThan(1);
                    assertThat(offsetIndex.getOffset(0)).isEqualTo(column.getFirstDataPageOffset());
                    assertThat(offsetIndex.getFirstRowIndex(0)).isEqualTo(0);
                    for (int page = 1; page < offsetIndex.getPageCount(); page++) {
                        assertThat(offsetIndex.getFirstRowIndex(page)).isGreaterThan(offsetIndex.getFirstRowIndex(page - 1));
                        assertThat(offsetIndex.getOffset(page)).isEqualTo(offsetIndex.getOffset(page - 1) + offsetIndex.getCompressedPageSize(page - 1));
                    }
                    assertThat(offsetIndex.getLastRowIndex(offsetIndex.getPageCount() - 1, block.getRowCount())).isEqualTo(block.getRowCount() - 1);
                }
            }
        }
    }

    @Test
    void testNextPageRowCount()
    {
        OffsetIndexBuilder offsetIndex = OffsetIndexBuilder.getBuilder();
        offsetIndex.add(4, 100, 0);
        offsetIndex.add(104, 100, 10);
        offsetIndex.add(204, 100, 25);
        PageReader pageReader = createPageReader(3);
        pageReader.setOffsetIndexLoader(offsetIndex::build, 40);

        assertThat(pageReader.getNextPageRowCount()).hasValue(10);
        pageReader.skipNextPage();
        assertThat(pageReader.getNextPageRowCount()).hasValue(15);
        pageReader.skipNextPage();
        // the last page ends with the row group
        assertThat(pageReader.getNextPageRowCount()).hasValue(15);
        pageReader.skipNextPage();
        assertThat(pageReader.getNextPageRowCount()).isEmpty();
    }

    @Test
    void testNextPageRowCountWithoutOffsetIndex()
    {
        PageReader pageReader = createPageReader(1);
        assertThat(pageReader.getNextPageRowCount()).isEmpty();
        pageReader.setOffsetIndexLoader(() -> null, 10);
        assertThat(pageReader.getNextPageRowCount()).isEmpty();
    }

    @Test
    void testNextPageRowCountWithInvalidOffsetIndex()
    {
        OffsetIndexBuilder tooFewPages = OffsetIndexBuilder.getBuilder();
        tooFewPages.add(4, 100, 0);
        PageReader pageReader = createPageReader(2);
        pageReader.setOffsetIndexLoader(tooFewPages::build, 20);
        assertThat(pageReader.getNextPageRowCount()).hasValue(20);
        pageReader.skipNextPage();
        assertThatThrownBy(pageReader::getNextPageRowCount)
                .isInstanceOf(ParquetDecodingException.class)
                .hasMessageContaining("has 1 pages, but page 1 was found");

        OffsetIndexBuilder notIncreasing = OffsetIndexBuilder.getBuilder();
        notIncreasing.add(4, 100, 0);
        notIncreasing.add(104, 100, 0);
        pageReader = createPageReader(2);
        pageReader.setOffsetIndexLoader(notIncreasing::build, 20);
        assertThatThrownBy(pageReader::getNextPageRowCount)
                .isInstanceOf(ParquetDecodingException.class)
                .hasMessageContaining("invalid first row index");

        OffsetIndexBuilder notStartingAtFirstRow = OffsetIndexBuilder.getBuilder();
        notStartingAtFirstRow.add(4, 100, 5);
        notStartingAtFirstRow.add(104, 100, 100);
        pageReader = createPageReader(2);
        pageReader.setOffsetIndexLoader(notStartingAtFirstRow::build, 200);
        assertThatThrownBy(pageReader::getNextPageRowCount)
                .isInstanceOf(ParquetDecodingException.class)
                .hasMessageContaining("invalid first row index 5 of page 0");
    }

    @Test
    void testOffsetIndexNotMatchingColumnChunk()
            throws IOException
    {
        Slice file = writeTrinoFile(createExpectedPage());
        ColumnChunkMetadata chunk = MetadataReader.readFooter(new TestingParquetDataSource(file, ParquetReaderOptions.defaultOptions()), Optional.empty())
                .getBlocks().getFirst().columns().getFirst();
        long start = chunk.getStartingPos();
        long end = start + chunk.getTotalSize();

        assertThat(loadOffsetIndex(chunk, start, start + 10)).isNotNull();
        assertThat(loadOffsetIndex(chunk, start, end - 1)).isNotNull();
        // the pages have to be within the column chunk
        assertThat(loadOffsetIndex(chunk, start - 1, start + 10)).isNull();
        assertThat(loadOffsetIndex(chunk, start, end)).isNull();
        // and in increasing order
        assertThat(loadOffsetIndex(chunk, start + 10, start + 10)).isNull();
        assertThat(loadOffsetIndex(chunk, start + 10, start)).isNull();
    }

    private static OffsetIndex loadOffsetIndex(ColumnChunkMetadata chunk, long... pageOffsets)
            throws IOException
    {
        List<PageLocation> pageLocations = new ArrayList<>();
        for (int page = 0; page < pageOffsets.length; page++) {
            pageLocations.add(new PageLocation(pageOffsets[page], 1, page));
        }
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Util.writeOffsetIndex(new org.apache.parquet.format.OffsetIndex(pageLocations), output, null, null);
        return ParquetReader.loadOffsetIndex(new ByteArrayInputStream(output.toByteArray()), chunk);
    }

    private static PageReader createPageReader(int pageCount)
    {
        ImmutableList.Builder<io.trino.parquet.Page> pages = ImmutableList.builder();
        for (int i = 0; i < pageCount; i++) {
            pages.add(new DataPageV1(Slices.EMPTY_SLICE, 1, 0, OptionalLong.empty(), RLE, RLE, PLAIN, i));
        }
        PageReader pageReader = new PageReader(new ParquetDataSourceId("test"), UNCOMPRESSED, pages.build().iterator(), false, false, Optional.empty(), -1, -1);
        // no dictionary page
        assertThat(pageReader.readDictionaryPage()).isNull();
        return pageReader;
    }

    private static ReadResult read(BiPredicate<Integer, Integer> readColumn)
            throws IOException
    {
        Page expected = createExpectedPage();
        return read(createDataSource(expected), TYPES, COLUMN_NAMES, expected, readColumn);
    }

    private static ReadResult read(ParquetDataSource dataSource, List<Type> types, List<String> columnNames, Page expected, BiPredicate<Integer, Integer> readColumn)
            throws IOException
    {
        return read(dataSource, ParquetReaderOptions.builder().withMaxReadBlockRowCount(50).build(), types, columnNames, expected, readColumn);
    }

    private static ReadResult read(ParquetDataSource dataSource, ParquetReaderOptions options, List<Type> types, List<String> columnNames, Page expected, BiPredicate<Integer, Integer> readColumn)
            throws IOException
    {
        ParquetMetadata metadata = MetadataReader.readFooter(dataSource, options, Optional.empty(), Optional.empty());
        long readRows = 0;
        int dataPageReadCount;
        try (ParquetReader reader = createParquetReader(
                dataSource,
                metadata,
                options,
                newSimpleAggregatedMemoryContext(),
                types,
                columnNames,
                TupleDomain.all())) {
            // the id column is read in every batch to verify the position of the rows
            boolean hasIds = columnNames.getFirst().equals("id");
            int row = 0;
            for (SourcePage page = reader.nextPage(); page != null; page = reader.nextPage()) {
                if (hasIds) {
                    Block ids = page.getBlock(0);
                    for (int position = 0; position < page.getPositionCount(); position++) {
                        assertThat(BIGINT.getLong(ids, position)).isEqualTo(row + position);
                    }
                }
                for (int column = hasIds ? 1 : 0; column < types.size(); column++) {
                    if (readColumn.test(row, column)) {
                        Block actual = page.getBlock(column);
                        for (int position = 0; position < page.getPositionCount(); position++) {
                            assertThat(types.get(column).getObjectValue(actual, position))
                                    .isEqualTo(types.get(column).getObjectValue(expected.getBlock(column), row + position));
                        }
                        readRows += page.getPositionCount();
                    }
                }
                row += page.getPositionCount();
            }
            assertThat(row).isEqualTo(ROWS);
            dataPageReadCount = reader.getDataPageReadCount();
        }
        return new ReadResult(readRows, dataPageReadCount, dataSource.getReadBytes());
    }

    private static Page createExpectedPage()
    {
        BlockBuilder ids = BIGINT.createBlockBuilder(null, ROWS);
        ArrayBlockBuilder arrays = ARRAY_TYPE.createBlockBuilder(null, ROWS);
        ArrayBlockBuilder nestedArrays = NESTED_ARRAY_TYPE.createBlockBuilder(null, ROWS);
        MapBlockBuilder maps = MAP_TYPE.createBlockBuilder(null, ROWS);
        RowBlockBuilder rows = ROW_TYPE.createBlockBuilder(null, ROWS);
        for (long rowIndex = 0; rowIndex < ROWS; rowIndex++) {
            long row = rowIndex;
            BIGINT.writeLong(ids, row);

            // null, empty and non-empty arrays of various lengths
            if (row % 11 == 0) {
                arrays.appendNull();
            }
            else {
                long length = row % 7 == 0 ? 0 : row % 5 + 1;
                arrays.buildEntry(elements -> {
                    for (long i = 0; i < length; i++) {
                        BIGINT.writeLong(elements, row * 10 + i);
                    }
                });
            }

            if (row % 13 == 0) {
                nestedArrays.appendNull();
            }
            else {
                nestedArrays.buildEntry(elements -> {
                    for (long inner = 0; inner < row % 4; inner++) {
                        if (inner == 1 && row % 3 == 0) {
                            elements.appendNull();
                            continue;
                        }
                        long length = (row + inner) % 3;
                        ARRAY_TYPE.writeObject(elements, createBigintArray(row * 100 + inner * 10, length));
                    }
                });
            }

            if (row % 9 == 0) {
                maps.appendNull();
            }
            else {
                maps.buildEntry((keys, values) -> {
                    for (long i = 0; i < row % 4; i++) {
                        BIGINT.writeLong(keys, i);
                        BIGINT.writeLong(values, row + i);
                    }
                });
            }

            if (row % 6 == 0) {
                rows.appendNull();
            }
            else {
                rows.buildEntry(fields -> {
                    BIGINT.writeLong(fields.get(0), row);
                    if (row % 5 == 0) {
                        fields.get(1).appendNull();
                    }
                    else {
                        BIGINT.writeLong(fields.get(1), row * 2);
                    }
                });
            }
        }
        return new Page(ids.build(), arrays.build(), nestedArrays.build(), maps.build(), rows.build());
    }

    private static Block createBigintArray(long start, long length)
    {
        BlockBuilder builder = BIGINT.createBlockBuilder(null, toIntExact(length));
        for (long i = 0; i < length; i++) {
            BIGINT.writeLong(builder, start + i);
        }
        return builder.build();
    }

    private static ParquetDataSource createDataSource(Page page)
            throws IOException
    {
        return createDataSource(page, ParquetReaderOptions.defaultOptions());
    }

    private static ParquetDataSource createDataSource(Page page, ParquetReaderOptions options)
            throws IOException
    {
        return new TestingParquetDataSource(writeTrinoFile(page), options);
    }

    private static Slice writeTrinoFile(Page page)
            throws IOException
    {
        return writeParquetFile(
                ParquetWriterOptions.builder()
                        .setMaxPageValueCount(100)
                        .setBatchSize(10)
                        .setMaxRowGroupRowCount(ROW_GROUP_ROWS)
                        .build(),
                TYPES,
                COLUMN_NAMES,
                ImmutableList.of(page));
    }

    private record ReadResult(long readRows, int dataPageReadCount, long readBytes) {}
}
