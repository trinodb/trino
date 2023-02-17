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
import io.airlift.slice.Slices;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.parquet.DataPage;
import io.trino.parquet.DataPageV2;
import io.trino.parquet.Page;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.reader.decoders.ValueDecoders;
import io.trino.parquet.reader.flat.FlatColumnReader;
import io.trino.spi.block.Block;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter;
import org.apache.parquet.column.values.fallback.FallbackValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.apache.parquet.schema.PrimitiveType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTypeUtils.getParquetEncoding;
import static io.trino.parquet.reader.FilteredRowRanges.RowRange;
import static io.trino.parquet.reader.TestingColumnReader.toTrinoDictionaryPage;
import static io.trino.parquet.reader.flat.IntColumnAdapter.INT_ADAPTER;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.DataProviders.cartesianProduct;
import static io.trino.testing.DataProviders.concat;
import static io.trino.testing.DataProviders.toDataProvider;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.column.Encoding.RLE_DICTIONARY;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.internal.filter2.columnindex.TestingRowRanges.toRowRange;
import static org.apache.parquet.internal.filter2.columnindex.TestingRowRanges.toRowRanges;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;

public class TestColumnReader
{
    private static final Random RANDOM = new Random(104729L);
    private static final PrimitiveType REQUIRED_TYPE = new PrimitiveType(REQUIRED, INT32, "");
    private static final PrimitiveType OPTIONAL_TYPE = new PrimitiveType(OPTIONAL, INT32, "");
    private static final PrimitiveField NULLABLE_FIELD = new PrimitiveField(INTEGER, false, new ColumnDescriptor(new String[] {}, OPTIONAL_TYPE, 0, 1), 0);
    private static final PrimitiveField FIELD = new PrimitiveField(INTEGER, true, new ColumnDescriptor(new String[] {}, REQUIRED_TYPE, 0, 0), 0);
    private static final LocalMemoryContext MEMORY_CONTEXT = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");

    @Test(dataProvider = "testRowRangesProvider")
    public void testReadFilteredPage(
            ColumnReaderInput columnReaderInput,
            BatchSkipper skipper,
            Optional<RowRanges> selectedRows,
            List<RowRange> pageRowRanges)
    {
        ColumnReaderProvider columnReaderProvider = columnReaderInput.columnReaderProvider();
        ColumnReader reader = columnReaderProvider.createColumnReader();
        NullPositionsProvider nullPositionsProvider = columnReaderInput.getPageNullPositionsProvider();
        List<TestingPage> testingPages = getTestingPages(nullPositionsProvider, pageRowRanges);
        reader.setPageReader(
                getPageReader(testingPages, columnReaderInput.dictionaryEncoding()),
                selectedRows.map(FilteredRowRanges::new));

        int rowCount = selectedRows.map(ranges -> toIntExact(ranges.rowCount()))
                .orElseGet(() -> pagesRowCount(testingPages));
        List<Long> valuesRead = new ArrayList<>(rowCount);
        List<Long> expectedValues = new ArrayList<>(rowCount);
        PrimitiveIterator.OfLong rowRangesIterator = selectedRows.map(RowRanges::iterator)
                .orElseGet(() -> LongStream.range(pageRowRanges.get(0).start(), rowCount).iterator());
        Set<Integer> required = getRequiredPositions(testingPages);

        int readCount = 0;
        int batchSize = 1;
        Supplier<Boolean> skipFunction = skipper.getFunction();
        while (readCount < rowCount) {
            reader.prepareNextRead(batchSize);
            if (skipFunction.get()) {
                // skip current batch to force a seek on next read
                for (int i = 0; i < batchSize; i++) {
                    rowRangesIterator.next();
                }
            }
            else {
                Block block = reader.readPrimitive().getBlock();
                assertThat(block.getPositionCount()).isEqualTo(batchSize);
                for (int i = 0; i < block.getPositionCount(); i++) {
                    long selectedRowNumber = rowRangesIterator.next();
                    if (required.contains(toIntExact(selectedRowNumber))) {
                        valuesRead.add((long) block.getInt(i, 0));
                        expectedValues.add(selectedRowNumber);
                    }
                    else {
                        assertThat(block.isNull(i)).isTrue();
                    }
                }
            }

            readCount += batchSize;
            batchSize = Math.min(Math.min(batchSize * 2, 512), rowCount - readCount);
        }
        assertThat(rowRangesIterator.hasNext()).isFalse();
        assertThat(valuesRead).isEqualTo(expectedValues);
    }

    @DataProvider
    public Object[][] testRowRangesProvider()
    {
        Object[][] columnReaders = Stream.of(ColumnReaderProvider.values())
                .flatMap(reader -> Arrays.stream(getColumnReaderInputs(reader)))
                .collect(toDataProvider());
        Object[][] batchSkippers = Stream.of(BatchSkipper.values())
                .collect(toDataProvider());
        Object[][] rowRanges = Stream.of(
                        Optional.empty(),
                        Optional.of(toRowRange(4096)),
                        Optional.of(toRowRange(956)),
                        Optional.of(toRowRanges(range(101, 900))),
                        Optional.of(toRowRanges(range(56, 89), range(120, 250), range(300, 455), range(600, 980), range(2345, 3140))))
                .collect(toDataProvider());
        Object[][] pageRowRanges = Stream.of(
                        ImmutableList.of(range(0, 4095)),
                        ImmutableList.of(range(0, 127), range(128, 4095)),
                        ImmutableList.of(range(0, 767), range(768, 4095)),
                        ImmutableList.of(range(0, 255), range(256, 511), range(512, 767), range(768, 4095)),
                        // Parquet pages with small size to simulate cases of FlatColumnReader#seek skipping over parquet pages
                        IntStream.rangeClosed(0, 4095 / 150).boxed()
                                .map(i -> {
                                    long start = i * 150;
                                    return range(start, Math.min(start + 149, 4095));
                                })
                                .collect(toImmutableList()))
                .collect(toDataProvider());
        Object[][] rangesWithNoPageSkipped = cartesianProduct(columnReaders, batchSkippers, rowRanges, pageRowRanges);
        Object[][] rangesWithPagesSkipped = cartesianProduct(
                columnReaders,
                batchSkippers,
                Stream.of(Optional.of(toRowRanges(range(56, 80), range(120, 200), range(350, 455), range(600, 940))))
                        .collect(toDataProvider()),
                Stream.of(ImmutableList.of(range(50, 100), range(120, 275), range(290, 455), range(590, 800), range(801, 1000)))
                        .collect(toDataProvider()));
        return concat(rangesWithNoPageSkipped, rangesWithPagesSkipped);
    }

    private enum ColumnReaderProvider
    {
        INT_PRIMITIVE_NO_NULLS(() -> new IntColumnReader(FIELD), FIELD),
        INT_PRIMITIVE_NULLABLE(() -> new IntColumnReader(NULLABLE_FIELD), NULLABLE_FIELD),
        INT_FLAT_NO_NULLS(() -> new FlatColumnReader<>(FIELD, ValueDecoders::getIntDecoder, INT_ADAPTER, MEMORY_CONTEXT), FIELD),
        INT_FLAT_NULLABLE(() -> new FlatColumnReader<>(NULLABLE_FIELD, ValueDecoders::getIntDecoder, INT_ADAPTER, MEMORY_CONTEXT), NULLABLE_FIELD);

        private final Supplier<ColumnReader> columnReader;
        private final PrimitiveField field;

        ColumnReaderProvider(Supplier<ColumnReader> columnReader, PrimitiveField field)
        {
            this.columnReader = requireNonNull(columnReader, "columnReader is null");
            this.field = requireNonNull(field, "field is null");
        }

        ColumnReader createColumnReader()
        {
            return columnReader.get();
        }

        public PrimitiveField getField()
        {
            return field;
        }
    }

    private enum BatchSkipper
    {
        NO_SEEK {
            @Override
            Supplier<Boolean> getFunction()
            {
                return () -> false;
            }
        },
        RANDOM_SEEK {
            @Override
            Supplier<Boolean> getFunction()
            {
                return RANDOM::nextBoolean;
            }
        },
        ALTERNATE_SEEK {
            @Override
            Supplier<Boolean> getFunction()
            {
                AtomicBoolean last = new AtomicBoolean();
                return () -> {
                    last.set(!last.get());
                    return last.get();
                };
            }
        };

        abstract Supplier<Boolean> getFunction();
    }

    private record ColumnReaderInput(ColumnReaderProvider columnReaderProvider, NullPositionsProvider nullPositionsProvider, DictionaryEncoding dictionaryEncoding)
    {
        private ColumnReaderInput(ColumnReaderProvider columnReaderProvider, NullPositionsProvider nullPositionsProvider, DictionaryEncoding dictionaryEncoding)
        {
            this.columnReaderProvider = requireNonNull(columnReaderProvider, "columnReader is null");
            this.nullPositionsProvider = requireNonNull(nullPositionsProvider, "nullPositions is null");
            this.dictionaryEncoding = dictionaryEncoding;
        }

        public NullPositionsProvider getPageNullPositionsProvider()
        {
            return nullPositionsProvider;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("columnReader", columnReaderProvider)
                    .add("nullPositions", nullPositionsProvider)
                    .add("dictionary", dictionaryEncoding)
                    .toString();
        }
    }

    private static ColumnReaderInput[] getColumnReaderInputs(ColumnReaderProvider columnReaderProvider)
    {
        boolean required = columnReaderProvider.getField().isRequired();
        Object[][] nullPositionsProviders;
        if (required) {
            nullPositionsProviders = Stream.of(NullPositionsProvider.ALL_NON_NULLS)
                    .collect(toDataProvider());
        }
        else {
            nullPositionsProviders = Stream.of(NullPositionsProvider.values())
                    .collect(toDataProvider());
        }
        Object[][] dictionaryEncoded = Arrays.stream(DictionaryEncoding.values())
                .collect(toDataProvider());
        return Arrays.stream(cartesianProduct(nullPositionsProviders, dictionaryEncoded))
                // Skip ALL_NULLS case for dictionary encoded data
                .filter(args -> args[0] != NullPositionsProvider.ALL_NULLS || args[1].equals(DictionaryEncoding.NONE))
                .map(args -> new ColumnReaderInput(columnReaderProvider, (NullPositionsProvider) args[0], (DictionaryEncoding) args[1]))
                .toArray(ColumnReaderInput[]::new);
    }

    private enum NullPositionsProvider
    {
        ALL_NON_NULLS {
            @Override
            boolean[] getRequiredPositions(int positionsCount)
            {
                boolean[] required = new boolean[positionsCount];
                Arrays.fill(required, true);
                return required;
            }
        },
        ALL_NULLS {
            @Override
            boolean[] getRequiredPositions(int positionsCount)
            {
                return new boolean[positionsCount];
            }
        },
        RANDOM {
            @Override
            boolean[] getRequiredPositions(int positionsCount)
            {
                boolean[] required = new boolean[positionsCount];
                Random r = new Random(toIntExact(104729L * positionsCount));
                for (int i = 0; i < positionsCount; i++) {
                    required[i] = r.nextBoolean();
                }
                return required;
            }
        },
        ALTERNATE_NULLS {
            @Override
            boolean[] getRequiredPositions(int positionsCount)
            {
                boolean[] required = new boolean[positionsCount];
                for (int i = 0; i < positionsCount; i++) {
                    if (i % 2 == 0) {
                        required[i] = true;
                    }
                }
                return required;
            }
        };

        abstract boolean[] getRequiredPositions(int positionsCount);
    }

    private enum DictionaryEncoding
    {
        NONE,
        ALL,
        MIXED
    }

    private static Set<Integer> getRequiredPositions(List<TestingPage> testingPages)
    {
        return testingPages.stream()
                .flatMap(positions -> {
                    boolean[] required = positions.getRequiredPositions();
                    return IntStream.range(0, required.length)
                            .filter(idx -> required[idx])
                            .map(idx -> idx + toIntExact(positions.getPageRowRange().start()))
                            .boxed();
                })
                .collect(toImmutableSet());
    }

    private static PageReader getPageReader(List<TestingPage> testingPages, DictionaryEncoding dictionaryEncoding)
    {
        ValuesWriter encoder;
        if (dictionaryEncoding == DictionaryEncoding.ALL) {
            encoder = new PlainIntegerDictionaryValuesWriter(Integer.MAX_VALUE, RLE_DICTIONARY, Encoding.PLAIN, HeapByteBufferAllocator.getInstance());
        }
        else if (dictionaryEncoding == DictionaryEncoding.MIXED) {
            int numDictionaryPages = 1;
            if (testingPages.size() > 1) {
                // Choose a point for fallback from dictionary to plain encoding randomly
                numDictionaryPages = RANDOM.nextInt(1, testingPages.size());
            }
            encoder = FallbackValuesWriter.of(
                    new TestingPlainIntegerDictionaryValuesWriter(numDictionaryPages, RLE_DICTIONARY, Encoding.PLAIN, HeapByteBufferAllocator.getInstance()),
                    new PlainValuesWriter(1000, 1000, HeapByteBufferAllocator.getInstance()));
        }
        else {
            encoder = new PlainValuesWriter(1000, 1000, HeapByteBufferAllocator.getInstance());
        }
        List<? extends Page> inputPages = createDataPages(testingPages, encoder);
        if (dictionaryEncoding != DictionaryEncoding.NONE) {
            inputPages = ImmutableList.<Page>builder().add(toTrinoDictionaryPage(encoder.toDictPageAndClose())).addAll(inputPages).build();
        }
        return new PageReader(
                UNCOMPRESSED,
                inputPages.iterator(),
                dictionaryEncoding == DictionaryEncoding.ALL || (dictionaryEncoding == DictionaryEncoding.MIXED && testingPages.size() == 1),
                false);
    }

    private static List<DataPage> createDataPages(List<TestingPage> testingPage, ValuesWriter encoder)
    {
        return testingPage.stream()
                .map(page -> createDataPage(page, encoder))
                .collect(toImmutableList());
    }

    private static DataPage createDataPage(TestingPage testingPage, ValuesWriter encoder)
    {
        int rowCount = testingPage.getRowCount();
        int[] values = new int[rowCount];
        int valueCount = getPageValues(testingPage, values);
        int nullCount = rowCount - valueCount;
        byte[] encodedBytes = encodePlainValues(encoder, values, valueCount);
        DataPage dataPage = new DataPageV2(
                rowCount,
                nullCount,
                rowCount,
                EMPTY_SLICE,
                Slices.wrappedBuffer(encodeDefinitionLevels(testingPage.getRequiredPositions())),
                getParquetEncoding(encoder.getEncoding()),
                Slices.wrappedBuffer(encodedBytes),
                rowCount * 4,
                OptionalLong.of(toIntExact(testingPage.getPageRowRange().start())),
                null,
                false);
        encoder.reset();
        return dataPage;
    }

    private static int getPageValues(TestingPage testingPage, int[] values)
    {
        RowRange pageRowRange = testingPage.getPageRowRange();
        int start = toIntExact(pageRowRange.start());
        int end = toIntExact(pageRowRange.end()) + 1;
        boolean[] required = testingPage.getRequiredPositions();
        int valueCount = 0;
        for (int i = start; i < end; i++) {
            values[valueCount] = i;
            valueCount += required[i - start] ? 1 : 0;
        }
        return valueCount;
    }

    private static byte[] encodeDefinitionLevels(boolean[] values)
    {
        RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(1, values.length, values.length, HeapByteBufferAllocator.getInstance());
        try {
            for (boolean value : values) {
                encoder.writeInt(value ? 1 : 0);
            }
            return encoder.toBytes().toByteArray();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static byte[] encodePlainValues(ValuesWriter encoder, int[] values, int valueCount)
    {
        try {
            for (int i = 0; i < valueCount; i++) {
                encoder.writeInteger(values[i]);
            }
            return encoder.getBytes().toByteArray();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static int pagesRowCount(List<TestingPage> pageRowRanges)
    {
        return pageRowRanges.stream()
                .mapToInt(TestingPage::getRowCount)
                .sum();
    }

    private static List<TestingPage> getTestingPages(NullPositionsProvider nullPositionsProvider, List<RowRange> pageRowRanges)
    {
        return pageRowRanges.stream()
                .map(rowRange -> new TestingPage(
                        rowRange,
                        nullPositionsProvider.getRequiredPositions(toIntExact(rowRange.end() + 1 - rowRange.start()))))
                .collect(toImmutableList());
    }

    private static class TestingPage
    {
        private final RowRange pageRowRange;
        private final boolean[] required;

        public TestingPage(RowRange pageRowRange, boolean[] required)
        {
            this.pageRowRange = pageRowRange;
            this.required = required;
        }

        public RowRange getPageRowRange()
        {
            return pageRowRange;
        }

        public boolean[] getRequiredPositions()
        {
            return required;
        }

        public int getRowCount()
        {
            return toIntExact(pageRowRange.end() + 1 - pageRowRange.start());
        }
    }

    private static class TestingPlainIntegerDictionaryValuesWriter
            extends PlainIntegerDictionaryValuesWriter
    {
        private final int dictionaryPagesBeforeFallback;
        private int pagesWritten;

        public TestingPlainIntegerDictionaryValuesWriter(int dictionaryPagesBeforeFallback, Encoding encodingForDataPage, Encoding encodingForDictionaryPage, ByteBufferAllocator allocator)
        {
            super(Integer.MAX_VALUE, encodingForDataPage, encodingForDictionaryPage, allocator);
            this.dictionaryPagesBeforeFallback = dictionaryPagesBeforeFallback;
        }

        @Override
        public boolean isCompressionSatisfying(long rawSize, long encodedSize)
        {
            return true;
        }

        @Override
        public void reset()
        {
            pagesWritten++;
            super.reset();
        }

        @Override
        public boolean shouldFallBack()
        {
            return pagesWritten >= dictionaryPagesBeforeFallback;
        }
    }

    private static RowRange range(long start, long end)
    {
        return new RowRange(start, end);
    }
}
