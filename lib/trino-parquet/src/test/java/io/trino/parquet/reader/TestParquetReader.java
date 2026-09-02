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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.io.Resources;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.Column;
import io.trino.parquet.DiskRange;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Count;
import io.trino.spi.metrics.Metric;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTestUtils.createParquetReader;
import static io.trino.parquet.ParquetTestUtils.generateInputPages;
import static io.trino.parquet.ParquetTestUtils.writeParquetFile;
import static io.trino.parquet.ParquetTypeUtils.constructField;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnByName;
import static io.trino.parquet.predicate.PredicateUtils.buildPredicate;
import static io.trino.parquet.predicate.PredicateUtils.getFilteredRowGroups;
import static io.trino.parquet.reader.ParquetReader.COLUMN_INDEX_ROWS_FILTERED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.joda.time.DateTimeZone.UTC;

public class TestParquetReader
{
    @Test
    public void testColumnReaderMemoryUsage()
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
                        generateInputPages(types, 100, 5)),
                ParquetReaderOptions.defaultOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        assertThat(parquetMetadata.getBlocks().size()).isGreaterThan(1);
        // Verify file has only non-dictionary encodings as dictionary memory usage is already tested in TestFlatColumnReader#testMemoryUsage
        parquetMetadata.getBlocks().forEach(block -> {
            block.columns()
                    .forEach(columnChunkMetaData -> assertThat(columnChunkMetaData.getEncodingStats().hasDictionaryEncodedPages()).isFalse());
            assertThat(block.rowCount()).isEqualTo(100);
        });

        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        ParquetReader reader = createParquetReader(dataSource, parquetMetadata, memoryContext, types, columnNames);

        SourcePage page = reader.nextPage();
        assertThat(memoryContext.getBytes()).isEqualTo(0);
        page.getBlock(0);
        // Memory usage due to reading data and decoding parquet page of 1st block
        long initialMemoryUsage = memoryContext.getBytes();
        assertThat(initialMemoryUsage).isGreaterThan(0);

        // Memory usage due to decoding parquet page of 2nd block
        page.getBlock(1);
        long currentMemoryUsage = memoryContext.getBytes();
        assertThat(currentMemoryUsage).isGreaterThan(initialMemoryUsage);

        // Memory usage does not change until next row group (1 page per row-group)
        long rowGroupRowCount = parquetMetadata.getBlocks().get(0).rowCount();
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

    @Test
    public void testEmptyRowRangesWithColumnIndex()
            throws URISyntaxException, IOException
    {
        List<String> columnNames = ImmutableList.of("l_shipdate", "l_commitdate");
        List<Type> types = ImmutableList.of(DATE, DATE);

        ParquetDataSource dataSource = new FileParquetDataSource(
                new File(Resources.getResource("lineitem_sorted_by_shipdate/data.parquet").toURI()),
                ParquetReaderOptions.defaultOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        assertThat(parquetMetadata.getBlocks()).hasSize(2);
        try (ParquetReader reader = createParquetReader(dataSource, parquetMetadata, ParquetReaderOptions.defaultOptions(), newSimpleAggregatedMemoryContext(), types, columnNames, sortedLineitemPageSkippingPredicate())) {
            SourcePage page = reader.nextPage();
            int rowsRead = 0;
            while (page != null) {
                rowsRead += page.getPositionCount();
                page = reader.nextPage();
            }
            assertThat(rowsRead).isEqualTo(2387);
            Map<String, Metric<?>> metrics = reader.getMetrics().getMetrics();
            assertThat(metrics).containsKey(COLUMN_INDEX_ROWS_FILTERED);
            // Column index should filter at least the first row group
            assertThat(((Count<?>) metrics.get(COLUMN_INDEX_ROWS_FILTERED)).getTotal())
                    .isGreaterThanOrEqualTo(parquetMetadata.getBlocks().get(0).rowCount());
        }
    }

    @Test
    public void testRowNumbersWithColumnIndex()
            throws URISyntaxException, IOException
    {
        List<String> columnNames = ImmutableList.of("l_shipdate", "l_commitdate", "l_comment");
        List<Type> types = ImmutableList.of(DATE, DATE, VARCHAR);
        ParquetReaderOptions options = ParquetReaderOptions.defaultOptions();
        File file = new File(Resources.getResource("lineitem_sorted_by_shipdate/data.parquet").toURI());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(new FileParquetDataSource(file, options), Optional.empty());

        List<Slice> comments = new ArrayList<>();
        try (ParquetReader reader = createParquetReader(new FileParquetDataSource(file, options), parquetMetadata, options, newSimpleAggregatedMemoryContext(), types, columnNames, TupleDomain.all())) {
            for (SourcePage page = reader.nextPage(); page != null; page = reader.nextPage()) {
                Block comment = page.getBlock(2);
                for (int position = 0; position < comment.getPositionCount(); position++) {
                    comments.add(VARCHAR.getSlice(comment, position));
                }
            }
        }

        try (ParquetReader reader = createParquetReaderWithRowNumbers(new FileParquetDataSource(file, options), parquetMetadata, options, types, columnNames, sortedLineitemPageSkippingPredicate())) {
            int rowsRead = 0;
            for (SourcePage page = reader.nextPage(); page != null; page = reader.nextPage()) {
                rowsRead += page.getPositionCount();
                // Drop the first position twice before loading the row number block
                for (int i = 0; i < 2 && page.getPositionCount() > 1; i++) {
                    int[] positions = IntStream.range(0, page.getPositionCount()).toArray();
                    page.selectPositions(positions, 1, page.getPositionCount() - 1);
                }
                Block comment = page.getBlock(2);
                Block rowNumber = page.getBlock(3);
                for (int position = 0; position < page.getPositionCount(); position++) {
                    assertThat(VARCHAR.getSlice(comment, position)).isEqualTo(comments.get(toIntExact(BIGINT.getLong(rowNumber, position))));
                }
            }
            assertThat(rowsRead).isEqualTo(2387);
            assertThat(reader.getMetrics().getMetrics()).containsKey(COLUMN_INDEX_ROWS_FILTERED);
        }
    }

    @Test
    public void testSelectPositionsOnLoadedBlocks()
            throws IOException
    {
        List<String> columnNames = ImmutableList.of("columna", "columnb");
        List<Type> types = ImmutableList.of(BIGINT, BIGINT);

        // columnA has row number values, columnB has row number * 10
        int rowCount = 100;
        BlockBuilder columnA = BIGINT.createFixedSizeBlockBuilder(rowCount);
        BlockBuilder columnB = BIGINT.createFixedSizeBlockBuilder(rowCount);
        for (int i = 0; i < rowCount; i++) {
            BIGINT.writeLong(columnA, i);
            BIGINT.writeLong(columnB, i * 10L);
        }
        ParquetDataSource dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder().build(),
                        types,
                        columnNames,
                        ImmutableList.of(new Page(rowCount, columnA.build(), columnB.build()))),
                ParquetReaderOptions.defaultOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        try (ParquetReader reader = createParquetReader(dataSource, parquetMetadata, newSimpleAggregatedMemoryContext(), types, columnNames)) {
            // batch sizes grow from 1, skip to a page with at least 8 positions
            long firstRow = 0;
            SourcePage page = reader.nextPage();
            while (page.getPositionCount() < 8) {
                firstRow += page.getPositionCount();
                page = reader.nextPage();
            }

            page.selectPositions(new int[] {1, 3, 5, 7}, 0, 4);
            assertThat(blockValues(page.getBlock(0))).containsExactly(firstRow + 1, firstRow + 3, firstRow + 5, firstRow + 7);

            // select again with positions relative to the previous selection
            page.selectPositions(new int[] {1, 2}, 0, 2);
            assertThat(page.getPositionCount()).isEqualTo(2);
            // columnA was loaded before the second selection, columnB is loaded after it
            assertThat(blockValues(page.getBlock(0))).containsExactly(firstRow + 3, firstRow + 5);
            assertThat(blockValues(page.getBlock(1))).containsExactly((firstRow + 3) * 10, (firstRow + 5) * 10);
        }
    }

    // The predicate and the sorted file are prepared so that page indexes result in non-overlapping row ranges
    // which eliminate the entire first row group while the second row group still has to be read
    private static TupleDomain<String> sortedLineitemPageSkippingPredicate()
    {
        return TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        "l_shipdate", Domain.multipleValues(DATE, ImmutableList.of(LocalDate.of(1993, 1, 1).toEpochDay(), LocalDate.of(1997, 1, 1).toEpochDay())),
                        "l_commitdate", Domain.create(ValueSet.ofRanges(Range.greaterThan(DATE, LocalDate.of(1995, 1, 1).toEpochDay())), false)));
    }

    private static ParquetReader createParquetReaderWithRowNumbers(
            ParquetDataSource dataSource,
            ParquetMetadata parquetMetadata,
            ParquetReaderOptions options,
            List<Type> types,
            List<String> columnNames,
            TupleDomain<String> predicate)
            throws IOException
    {
        MessageType fileSchema = parquetMetadata.getFileMetaData().getSchema();
        MessageColumnIO messageColumnIO = getColumnIO(fileSchema, fileSchema);
        ImmutableList.Builder<Column> columnFields = ImmutableList.builder();
        for (int i = 0; i < types.size(); i++) {
            columnFields.add(new Column(columnNames.get(i), constructField(types.get(i), lookupColumnByName(messageColumnIO, columnNames.get(i))).orElseThrow()));
        }
        Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, fileSchema);
        TupleDomain<ColumnDescriptor> parquetTupleDomain = predicate.transformKeys(columnName -> descriptorsByPath.get(ImmutableList.of(columnName)));
        TupleDomainParquetPredicate parquetPredicate = buildPredicate(fileSchema, parquetTupleDomain, descriptorsByPath, UTC);
        List<RowGroupInfo> rowGroups = getFilteredRowGroups(
                0,
                dataSource.getEstimatedSize(),
                dataSource,
                parquetMetadata,
                ImmutableList.of(parquetTupleDomain),
                ImmutableList.of(parquetPredicate),
                descriptorsByPath,
                UTC,
                1000,
                options);
        return new ParquetReader(
                Optional.empty(),
                columnFields.build(),
                true,
                rowGroups,
                dataSource,
                UTC,
                newSimpleAggregatedMemoryContext(),
                options,
                RuntimeException::new,
                Optional.of(parquetPredicate),
                Optional.empty(),
                Optional.empty());
    }

    private static List<Long> blockValues(Block block)
    {
        ImmutableList.Builder<Long> values = ImmutableList.builder();
        for (int position = 0; position < block.getPositionCount(); position++) {
            values.add(BIGINT.getLong(block, position));
        }
        return values.build();
    }

    @Test
    public void testBackwardsCompatibleRepeatedStringField()
            throws Exception
    {
        File parquetFile = new File(Resources.getResource("parquet_repeated_primitives/string/old-repeated-string.parquet").toURI());
        List<List<String>> expectedValues = ImmutableList.of(Arrays.asList("hello", "world"), Arrays.asList("good", "bye"), Arrays.asList("one", "two", "three"));
        testReadingOldParquetFiles(parquetFile, ImmutableList.of("myString"), new ArrayType(VARCHAR), expectedValues);
    }

    @Test
    public void testBackwardsCompatibleRepeatedIntegerField()
            throws Exception
    {
        File parquetFile = new File(Resources.getResource("parquet_repeated_primitives/int/old-repeated-int.parquet").toURI());
        List<List<Integer>> expectedValues = ImmutableList.of(Arrays.asList(1, 2, 3));
        testReadingOldParquetFiles(parquetFile, ImmutableList.of("repeatedInt"), new ArrayType(INTEGER), expectedValues);
    }

    @Test
    public void testBackwardsCompatibleRepeatedPrimitiveFieldDefinedAsPrimitive()
    {
        assertThatThrownBy(() -> {
            File parquetFile = new File(Resources.getResource("parquet_repeated_primitives/int/old-repeated-int.parquet").toURI());
            List<List<Integer>> expectedValues = ImmutableList.of(Arrays.asList(1, 2, 3));
            testReadingOldParquetFiles(parquetFile, ImmutableList.of("repeatedInt"), INTEGER, expectedValues);
        }).hasMessage("Unsupported Trino column type (integer) for Parquet column ([repeatedint] repeated int32 repeatedint)")
                .isInstanceOf(TrinoException.class);
    }

    @Test
    void testReadMetadataWithSplitOffset()
            throws IOException
    {
        // Write a file with 100 rows per row-group
        List<String> columnNames = ImmutableList.of("columna", "columnb");
        List<Type> types = ImmutableList.of(INTEGER, BIGINT);

        ParquetDataSource dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder()
                                .setMaxBlockSize(DataSize.ofBytes(1000))
                                .build(),
                        types,
                        columnNames,
                        generateInputPages(types, 100, 5)),
                ParquetReaderOptions.defaultOptions());

        // Read both columns, 1 row group
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        List<BlockMetadata> columnBlocks = parquetMetadata.getBlocks(0, 800);
        assertThat(columnBlocks.size()).isEqualTo(1);
        assertThat(columnBlocks.getFirst().columns().size()).isEqualTo(2);
        assertThat(columnBlocks.getFirst().rowCount()).isEqualTo(100);

        // Read both columns, half row groups
        parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        columnBlocks = parquetMetadata.getBlocks(0, 2500);
        assertThat(columnBlocks.stream().allMatch(block -> block.columns().size() == 2)).isTrue();
        assertThat(columnBlocks.stream().mapToLong(BlockMetadata::rowCount).sum()).isEqualTo(300);

        // Read both columns, all row groups
        parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        columnBlocks = parquetMetadata.getBlocks();
        assertThat(columnBlocks.stream().allMatch(block -> block.columns().size() == 2)).isTrue();
        assertThat(columnBlocks.stream().mapToLong(BlockMetadata::rowCount).sum()).isEqualTo(500);
    }

    @Test
    void testMaxFooterReadSize()
            throws IOException
    {
        // Write a file with 10 rows per row-group
        List<String> columnNames = ImmutableList.of("columna", "columnb");
        List<Type> types = ImmutableList.of(INTEGER, BIGINT);

        ParquetDataSource dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder()
                                .setMaxBlockSize(DataSize.ofBytes(10))
                                .build(),
                        types,
                        columnNames,
                        generateInputPages(types, 10, 50)),
                ParquetReaderOptions.defaultOptions());

        ParquetReaderOptions readerOptions = ParquetReaderOptions.builder()
                .withMaxFooterReadSize(DataSize.ofBytes(1000))
                .build();

        assertThatThrownBy(() -> MetadataReader.readFooter(dataSource, readerOptions, Optional.empty(), Optional.empty()))
                .hasMessageMatching(".* Parquet footer size .* exceeds maximum allowed size .*");
    }

    @Test
    void testFooterReadSize()
            throws IOException
    {
        List<String> columnNames = ImmutableList.of("columna", "columnb");
        List<Type> types = ImmutableList.of(INTEGER, BIGINT);

        RecordingParquetDataSource dataSource = new RecordingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder()
                                .setMaxBlockSize(DataSize.ofBytes(10))
                                .build(),
                        types,
                        columnNames,
                        generateInputPages(types, 10, 50)));

        ParquetReaderOptions readerOptions = ParquetReaderOptions.builder()
                .withFooterReadSize(DataSize.ofBytes(128))
                .build();

        MetadataReader.readFooter(dataSource, readerOptions, Optional.empty(), Optional.empty());

        assertThat(dataSource.getTailReadLengths()).startsWith(128);
    }

    @Test
    void testParseFooterFromSuppliedFooterBytes()
            throws IOException
    {
        List<String> columnNames = ImmutableList.of("columna", "columnb");
        List<Type> types = ImmutableList.of(INTEGER, BIGINT);
        Slice parquetFile = writeParquetFile(
                ParquetWriterOptions.builder()
                        .setMaxBlockSize(DataSize.ofBytes(10))
                        .build(),
                types,
                columnNames,
                generateInputPages(types, 10, 50));

        RecordingParquetDataSource source = new RecordingParquetDataSource(parquetFile);
        Slice footerBytes = MetadataReader.readFooterBytes(source, ParquetReaderOptions.defaultOptions());

        ParquetMetadata metadata = MetadataReader.parseFooter(new ParquetDataSourceId("test"), parquetFile.length(), footerBytes, Optional.empty(), Optional.empty());

        assertThat(metadata.getBlocks().stream().mapToLong(BlockMetadata::rowCount).sum()).isEqualTo(500);
    }

    private void testReadingOldParquetFiles(File file, List<String> columnNames, Type columnType, List<?> expectedValues)
            throws IOException
    {
        ParquetDataSource dataSource = new FileParquetDataSource(
                file,
                ParquetReaderOptions.defaultOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        try (ParquetReader reader = createParquetReader(dataSource, parquetMetadata, newSimpleAggregatedMemoryContext(), ImmutableList.of(columnType), columnNames)) {
            SourcePage page = reader.nextPage();
            Iterator<?> expected = expectedValues.iterator();
            while (page != null) {
                Block block = page.getBlock(0);
                for (int i = 0; i < block.getPositionCount(); i++) {
                    assertThat(columnType.getObjectValue(block, i)).isEqualTo(expected.next());
                }
                page = reader.nextPage();
            }
            assertThat(expected.hasNext())
                    .describedAs("Read fewer values than expected")
                    .isFalse();
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

        public List<Integer> getTailReadLengths()
        {
            return tailReadLengths;
        }
    }
}
