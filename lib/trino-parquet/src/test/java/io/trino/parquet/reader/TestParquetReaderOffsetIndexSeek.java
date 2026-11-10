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
import io.trino.parquet.DataPageV1;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Random;
import java.util.function.BiPredicate;

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
    private static final ArrayType ARRAY_TYPE = new ArrayType(BIGINT);
    private static final ArrayType NESTED_ARRAY_TYPE = new ArrayType(ARRAY_TYPE);
    private static final MapType MAP_TYPE = new MapType(BIGINT, BIGINT, new TypeOperators());
    private static final List<Type> TYPES = ImmutableList.of(BIGINT, ARRAY_TYPE, NESTED_ARRAY_TYPE, MAP_TYPE);
    private static final List<String> COLUMN_NAMES = ImmutableList.of("id", "values", "nested", "map");

    @Test
    void testSkippingPagesOfRepeatedColumns()
            throws IOException
    {
        // read the repeated columns only in the last batches, so they have to skip over most of the file
        int totalPages = read((_, _) -> true).dataPageReadCount();
        ReadResult result = read((batch, _) -> batch >= 300);
        assertThat(result.readRows()).isGreaterThan(0);
        assertThat(result.dataPageReadCount()).isLessThan(totalPages);
    }

    @Test
    void testRandomSkipPatterns()
            throws IOException
    {
        Random random = new Random(42);
        for (int iteration = 0; iteration < 20; iteration++) {
            double probability = random.nextDouble();
            long seed = random.nextLong();
            read((batch, column) -> new Random(seed + batch * 31L + column).nextDouble() < probability);
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
                .withRowGroupRowCountLimit(6_000)
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

        int totalPages = readFile(file, types, columnNames, expected, (_, _) -> true).dataPageReadCount();
        assertThat(readFile(file, types, columnNames, expected, (batch, _) -> batch >= 300).dataPageReadCount()).isLessThan(totalPages);
        Random random = new Random(7);
        for (int iteration = 0; iteration < 20; iteration++) {
            double probability = random.nextDouble();
            long seed = random.nextLong();
            readFile(file, types, columnNames, expected, (batch, column) -> new Random(seed + batch * 31L + column).nextDouble() < probability);
        }
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
        ParquetMetadata metadata = MetadataReader.readFooter(dataSource, Optional.empty());
        long readRows = 0;
        int dataPageReadCount;
        try (ParquetReader reader = createParquetReader(
                dataSource,
                metadata,
                ParquetReaderOptions.builder().withMaxReadBlockRowCount(50).build(),
                newSimpleAggregatedMemoryContext(),
                types,
                columnNames,
                TupleDomain.all())) {
            int batch = 0;
            int row = 0;
            for (SourcePage page = reader.nextPage(); page != null; page = reader.nextPage()) {
                Block ids = page.getBlock(0);
                for (int position = 0; position < page.getPositionCount(); position++) {
                    assertThat(BIGINT.getLong(ids, position)).isEqualTo(row + position);
                }
                for (int column = 1; column < types.size(); column++) {
                    if (readColumn.test(batch, column)) {
                        Block actual = page.getBlock(column);
                        for (int position = 0; position < page.getPositionCount(); position++) {
                            assertThat(types.get(column).getObjectValue(actual, position))
                                    .isEqualTo(types.get(column).getObjectValue(expected.getBlock(column), row + position));
                        }
                        readRows += page.getPositionCount();
                    }
                }
                row += page.getPositionCount();
                batch++;
            }
            assertThat(row).isEqualTo(ROWS);
            dataPageReadCount = reader.getDataPageReadCount();
        }
        return new ReadResult(readRows, dataPageReadCount);
    }

    private static Page createExpectedPage()
    {
        BlockBuilder ids = BIGINT.createBlockBuilder(null, ROWS);
        ArrayBlockBuilder arrays = ARRAY_TYPE.createBlockBuilder(null, ROWS);
        ArrayBlockBuilder nestedArrays = NESTED_ARRAY_TYPE.createBlockBuilder(null, ROWS);
        MapBlockBuilder maps = MAP_TYPE.createBlockBuilder(null, ROWS);
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
        }
        return new Page(ids.build(), arrays.build(), nestedArrays.build(), maps.build());
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
        return new TestingParquetDataSource(writeTrinoFile(page), ParquetReaderOptions.defaultOptions());
    }

    private static Slice writeTrinoFile(Page page)
            throws IOException
    {
        return writeParquetFile(
                ParquetWriterOptions.builder()
                        .setMaxPageValueCount(100)
                        .setBatchSize(10)
                        .setMaxRowGroupRowCount(6_000)
                        .build(),
                TYPES,
                COLUMN_NAMES,
                ImmutableList.of(page));
    }

    private record ReadResult(long readRows, int dataPageReadCount) {}
}
