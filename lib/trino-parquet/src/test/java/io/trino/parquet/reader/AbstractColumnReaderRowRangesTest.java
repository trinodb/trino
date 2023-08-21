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
import io.trino.parquet.DataPage;
import io.trino.parquet.DataPageV2;
import io.trino.parquet.Page;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.reader.decoders.ValueDecoder;
import io.trino.parquet.reader.decoders.ValueDecoders;
import io.trino.spi.block.Block;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.booleans.BooleanList;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter;
import org.apache.parquet.column.values.fallback.FallbackValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.parquet.ParquetTestUtils.toTrinoDictionaryPage;
import static io.trino.parquet.ParquetTypeUtils.getParquetEncoding;
import static io.trino.parquet.reader.FilteredRowRanges.RowRange;
import static io.trino.parquet.reader.TestingRowRanges.toRowRange;
import static io.trino.parquet.reader.TestingRowRanges.toRowRanges;
import static io.trino.testing.DataProviders.cartesianProduct;
import static io.trino.testing.DataProviders.concat;
import static io.trino.testing.DataProviders.toDataProvider;
import static java.lang.Math.toIntExact;
import static org.apache.parquet.bytes.BytesUtils.getWidthFromMaxInt;
import static org.apache.parquet.column.Encoding.RLE_DICTIONARY;
import static org.apache.parquet.format.CompressionCodec.UNCOMPRESSED;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractColumnReaderRowRangesTest
{
    private static final Random RANDOM = new Random(104729L);

    @Test(dataProvider = "testRowRangesProvider")
    public void testReadFilteredPage(
            ColumnReaderInput columnReaderInput,
            BatchSkipper skipper,
            Optional<RowRanges> parquetRowRanges,
            List<RowRange> pageRowRanges,
            boolean rowsAcrossPages)
    {
        ColumnReaderProvider columnReaderProvider = columnReaderInput.columnReaderProvider();
        ColumnReader reader = columnReaderProvider.createColumnReader();
        int maxDef = columnReaderProvider.getField().getDefinitionLevel();
        boolean required = columnReaderProvider.getField().isRequired();
        List<TestingPage> testingPages = getTestingPages(
                columnReaderInput.repetitionLevelsProvider(),
                columnReaderInput.definitionLevelsProvider(),
                pageRowRanges,
                rowsAcrossPages);
        reader.setPageReader(
                getPageReader(testingPages, maxDef, required, columnReaderInput.dictionaryEncoding()),
                parquetRowRanges.map(FilteredRowRanges::new));

        int rowCount = parquetRowRanges.map(ranges -> toIntExact(ranges.rowCount()))
                .orElseGet(() -> pagesRowCount(testingPages));
        List<Long> valuesRead = new ArrayList<>(rowCount);
        List<Long> expectedValues = new ArrayList<>(rowCount);
        PrimitiveIterator.OfLong rowRangesIterator = parquetRowRanges.map(RowRanges::iterator)
                .orElseGet(() -> LongStream.range(pageRowRanges.get(0).start(), rowCount).iterator());
        Int2ObjectMap<BooleanList> requiredPositions = getRequiredPositions(testingPages, maxDef, required);

        int rowReadCount = 0;
        int batchSize = 1;
        Supplier<Boolean> skipFunction = skipper.getFunction();
        while (rowReadCount < rowCount) {
            reader.prepareNextRead(batchSize);
            if (skipFunction.get()) {
                // skip current batch to force a seek on next read
                for (int i = 0; i < batchSize; i++) {
                    rowRangesIterator.next();
                }
            }
            else {
                ColumnChunk chunk = reader.readPrimitive();
                Block block = chunk.getBlock();
                int blockIndex = 0;
                IntList rowValueCounts = getPerRowValueCounts(chunk, maxDef, required);
                assertThat(rowValueCounts.size()).isEqualTo(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    long selectedRowNumber = rowRangesIterator.next();

                    BooleanList expectedDefinitions = requiredPositions.get((int) selectedRowNumber);
                    assertThat(rowValueCounts.getInt(i)).isEqualTo(expectedDefinitions.size());
                    for (int j = 0; j < rowValueCounts.getInt(i); j++) {
                        if (expectedDefinitions.getBoolean(j)) {
                            assertThat(block.getInt(blockIndex++, 0)).isEqualTo(selectedRowNumber);
                        }
                        else {
                            assertThat(block.isNull(blockIndex++)).isTrue();
                        }
                    }
                }
            }

            rowReadCount += batchSize;
            batchSize = Math.min(Math.min(batchSize * 2, 512), rowCount - rowReadCount);
        }
        assertThat(rowRangesIterator.hasNext()).isFalse();
        assertThat(valuesRead).isEqualTo(expectedValues);
    }

    protected abstract ColumnReaderProvider[] getColumnReaderProviders();

    protected interface ColumnReaderProvider
    {
        ColumnReader createColumnReader();

        PrimitiveField getField();
    }

    protected static ValueDecoder.ValueDecodersProvider<int[]> getIntDecodersProvider(PrimitiveField field)
    {
        ValueDecoders valueDecoders = new ValueDecoders(field);
        return valueDecoders::getIntDecoder;
    }

    @DataProvider
    public Object[][] testRowRangesProvider()
    {
        Object[][] columnReaders = Stream.of(getColumnReaderProviders())
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
        Object[][] rowsAcrossPages = ImmutableList.of(true, false)
                .stream()
                .collect(toDataProvider());
        Object[][] rangesWithNoPageSkipped = cartesianProduct(columnReaders, batchSkippers, rowRanges, pageRowRanges, rowsAcrossPages);
        Object[][] rangesWithPagesSkipped = cartesianProduct(
                columnReaders,
                batchSkippers,
                Stream.of(Optional.of(toRowRanges(range(56, 80), range(120, 200), range(350, 455), range(600, 940))))
                        .collect(toDataProvider()),
                Stream.of(ImmutableList.of(range(50, 100), range(120, 275), range(290, 455), range(590, 800), range(801, 1000)))
                        .collect(toDataProvider()),
                Stream.of(false).collect(toDataProvider()));
        return concat(rangesWithNoPageSkipped, rangesWithPagesSkipped);
    }

    private static IntList getPerRowValueCounts(ColumnChunk chunk, int maxDef, boolean required)
    {
        IntList rows = new IntArrayList();
        int currentRowValueCount = -1;
        int[] repetitionLevels = chunk.getRepetitionLevels();
        int[] definitionLevels = chunk.getDefinitionLevels();
        int valueCount = repetitionLevels.length == 0 ? chunk.getBlock().getPositionCount() : repetitionLevels.length;
        verify(repetitionLevels.length == definitionLevels.length, "repetition and definition levels length not matching");

        for (int i = 0; i < valueCount; i++) {
            if (repetitionLevels.length == 0 || repetitionLevels[i] == 0) {
                if (currentRowValueCount != -1) {
                    rows.add(currentRowValueCount);
                }
                currentRowValueCount = 0;
            }

            if ((maxDef == 0 || (!required && maxDef == 1)) // flat
                    || (required && definitionLevels[i] == maxDef) // nested non-null
                    || (!required && definitionLevels[i] >= maxDef - 1)) { // nested nullable
                currentRowValueCount++;
            }
        }
        rows.add(currentRowValueCount);

        return rows;
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

    record ColumnReaderInput(
            ColumnReaderProvider columnReaderProvider,
            DefinitionLevelsProvider definitionLevelsProvider,
            RepetitionLevelsProvider repetitionLevelsProvider,
            DictionaryEncoding dictionaryEncoding)
    {
        @Override
        public String toString()
        {
            return toStringHelper("input")
                    .add("columnReader", columnReaderProvider)
                    .add("definitionLevels", definitionLevelsProvider)
                    .add("repetitionLevels", repetitionLevelsProvider)
                    .add("dictionaryEncoding", dictionaryEncoding)
                    .toString();
        }
    }

    private static ColumnReaderInput[] getColumnReaderInputs(ColumnReaderProvider columnReaderProvider)
    {
        Object[][] definitionLevelsProviders = Arrays.stream(DefinitionLevelsProvider.ofDefinitionLevel(
                columnReaderProvider.getField().getDefinitionLevel(),
                columnReaderProvider.getField().isRequired()))
                .collect(toDataProvider());
        PrimitiveField field = columnReaderProvider.getField();

        Object[][] repetitionLevelsProviders;
        if (field.getRepetitionLevel() == 0) {
            repetitionLevelsProviders = Stream.of(RepetitionLevelsProvider.ALL_SINGLE_VALUES)
                    .collect(toDataProvider());
        }
        else {
            repetitionLevelsProviders = Arrays.stream(RepetitionLevelsProvider.values())
                    .collect(toDataProvider());
        }

        Object[][] dictionaryEncodings = Arrays.stream(DictionaryEncoding.values())
                .collect(toDataProvider());

        return Arrays.stream(cartesianProduct(definitionLevelsProviders, repetitionLevelsProviders, dictionaryEncodings))
                // Skip ALL_NULLS case for dictionary encoded data
                .filter(args -> args[0] != DefinitionLevelsProvider.ALL_1 || args[2].equals(DictionaryEncoding.NONE))
                .map(args -> new ColumnReaderInput(columnReaderProvider, (DefinitionLevelsProvider) args[0], (RepetitionLevelsProvider) args[1], (DictionaryEncoding) args[2]))
                .toArray(ColumnReaderInput[]::new);
    }

    private enum DefinitionLevelsProvider
    {
        ALL_0 {
            @Override
            int[] getDefinitionLevels(int positionsCount)
            {
                return new int[positionsCount];
            }
        },
        ALL_1 {
            @Override
            int[] getDefinitionLevels(int positionsCount)
            {
                int[] definitionLevels = new int[positionsCount];
                Arrays.fill(definitionLevels, 1);
                return definitionLevels;
            }
        },
        ALL_2 {
            @Override
            int[] getDefinitionLevels(int positionsCount)
            {
                int[] definitionLevels = new int[positionsCount];
                Arrays.fill(definitionLevels, 2);
                return definitionLevels;
            }
        },
        RANDOM_0_1 {
            @Override
            int[] getDefinitionLevels(int positionsCount)
            {
                int[] definitionLevels = new int[positionsCount];
                Random r = new Random(104729L * positionsCount);
                for (int i = 0; i < positionsCount; i++) {
                    definitionLevels[i] = r.nextBoolean() ? 1 : 0;
                }
                return definitionLevels;
            }
        },
        RANDOM_1_2 {
            @Override
            int[] getDefinitionLevels(int positionsCount)
            {
                int[] definitionLevels = new int[positionsCount];
                Random r = new Random(104729L * positionsCount);
                for (int i = 0; i < positionsCount; i++) {
                    definitionLevels[i] = (r.nextBoolean() ? 1 : 0) + 1;
                }
                return definitionLevels;
            }
        },
        ALTERNATE_0_1 {
            @Override
            int[] getDefinitionLevels(int positionsCount)
            {
                int[] definitionLevels = new int[positionsCount];
                for (int i = 0; i < positionsCount; i++) {
                    definitionLevels[i] = i % 2;
                }
                return definitionLevels;
            }
        },
        ALTERNATE_1_2 {
            @Override
            int[] getDefinitionLevels(int positionsCount)
            {
                int[] definitionLevels = new int[positionsCount];
                for (int i = 0; i < positionsCount; i++) {
                    definitionLevels[i] = (i % 2) + 1;
                }
                return definitionLevels;
            }
        },
        RANDOM_0_2 {
            @Override
            int[] getDefinitionLevels(int positionsCount)
            {
                int[] definitionLevels = new int[positionsCount];
                Random r = new Random(104729L * positionsCount);
                for (int i = 0; i < positionsCount; i++) {
                    definitionLevels[i] = r.nextInt(3);
                }
                return definitionLevels;
            }
        },
        ALTERNATE_0_2 {
            @Override
            int[] getDefinitionLevels(int positionsCount)
            {
                int[] definitionLevels = new int[positionsCount];
                for (int i = 0; i < positionsCount; i++) {
                    definitionLevels[i] = i % 3;
                }
                return definitionLevels;
            }
        },
        /**/;

        static DefinitionLevelsProvider[] ofDefinitionLevel(int maxDef, boolean required)
        {
            if (required) {
                return switch (maxDef) {
                    case 0 -> new DefinitionLevelsProvider[] {ALL_0};
                    case 1 -> new DefinitionLevelsProvider[] {ALL_1, RANDOM_0_1, ALTERNATE_0_1};
                    case 2 -> new DefinitionLevelsProvider[] {ALL_2, RANDOM_1_2, ALTERNATE_1_2};
                    default -> throw new IllegalArgumentException("Wrong definition level: " + maxDef);
                };
            }
            return switch (maxDef) {
                case 0 -> new DefinitionLevelsProvider[] {};
                case 1 -> new DefinitionLevelsProvider[] {ALL_0, RANDOM_0_1, ALTERNATE_0_1};
                case 2 -> new DefinitionLevelsProvider[] {ALL_1, RANDOM_0_1, ALTERNATE_0_1, RANDOM_0_2, ALTERNATE_0_2};
                default -> throw new IllegalArgumentException("Wrong definition level: " + maxDef);
            };
        }

        abstract int[] getDefinitionLevels(int positionsCount);
    }

    private enum RepetitionLevelsProvider
    {
        ALL_SINGLE_VALUES {
            @Override
            int[] getRepetitionLevels(int positionCount, boolean startWithUnfinishedRow)
            {
                return new int[positionCount + 1];
            }
        },
        ALL_DOUBLES {
            @Override
            int[] getRepetitionLevels(int positionCount, boolean startWithUnfinishedRow)
            {
                // If rows are allowed between pages we always put cut in the middle of the row
                int[] repetition = new int[positionCount * 2 + (startWithUnfinishedRow ? 1 : 0)];
                for (int i = 0; i < positionCount - (startWithUnfinishedRow ? 0 : 1); i++) {
                    repetition[2 * i + (startWithUnfinishedRow ? 0 : 1)] = 1;
                }
                return repetition;
            }
        },
        RANDOM_2 {
            @Override
            int[] getRepetitionLevels(int positionCount, boolean startWithUnfinishedRow)
            {
                return randomRepetitions(positionCount, 2, startWithUnfinishedRow);
            }
        },
        RANDOM_5 {
            @Override
            int[] getRepetitionLevels(int positionCount, boolean startWithUnfinishedRow)
            {
                return randomRepetitions(positionCount, 5, startWithUnfinishedRow);
            }
        };

        private static int[] randomRepetitions(int positionCount, int frequency, boolean startWithUnfinishedRow)
        {
            Random random = new Random(positionCount * 131071L);
            IntList levels = new IntArrayList();
            if (startWithUnfinishedRow) {
                int valuesFromPreviousRow = random.nextInt(frequency);
                for (int i = 0; i < valuesFromPreviousRow; i++) {
                    levels.add(1);
                }
            }
            levels.add(0);
            for (int i = 0; i < positionCount; ) {
                if (random.nextInt(frequency) == 0) {
                    levels.add(0);
                    i++;
                }
                else {
                    levels.add(1);
                }
            }
            return levels.toIntArray();
        }

        abstract int[] getRepetitionLevels(int positionCount, boolean startWithUnfinishedRow);
    }

    private enum DictionaryEncoding
    {
        NONE,
        ALL,
        MIXED
    }

    /**
     * @return mapping from row index to nullability of the values for that row. e.g
     * 5 -&gt; (false, true, false) means that the row 5 consist of (null, some value, null)
     */
    private static Int2ObjectMap<BooleanList> getRequiredPositions(List<TestingPage> testingPages, int maxDef, boolean required)
    {
        Int2ObjectMap<BooleanList> result = new Int2ObjectOpenHashMap<>();
        BooleanList currentRow = null;
        int rowIndex = -1;
        for (TestingPage testingPage : testingPages) {
            int[] definitionLevels = testingPage.definitionLevels();
            int[] repetitionLevels = testingPage.repetitionLevels();
            boolean firstRowInPage = true;

            for (int i = 0; i < definitionLevels.length; i++) {
                if (repetitionLevels[i] == 0) {
                    if (currentRow != null) {
                        result.put(rowIndex++, currentRow);
                    }
                    currentRow = new BooleanArrayList();
                    if (firstRowInPage) {
                        rowIndex = toIntExact(testingPage.pageRowRange().start());
                        firstRowInPage = false;
                    }
                }
                if (definitionLevels[i] == maxDef) {
                    currentRow.add(true);
                }
                else if (!required && definitionLevels[i] == maxDef - 1) {
                    currentRow.add(false);
                }
            }
        }
        result.put(rowIndex, currentRow);

        return result;
    }

    private static PageReader getPageReader(List<TestingPage> testingPages, int maxDef, boolean required, DictionaryEncoding dictionaryEncoding)
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
        List<Page> inputPages = createDataPages(testingPages, encoder, maxDef, required);
        if (dictionaryEncoding != DictionaryEncoding.NONE) {
            inputPages = ImmutableList.<Page>builder().add(toTrinoDictionaryPage(encoder.toDictPageAndClose())).addAll(inputPages).build();
        }
        return new PageReader(
                UNCOMPRESSED,
                inputPages.iterator(),
                dictionaryEncoding == DictionaryEncoding.ALL || (dictionaryEncoding == DictionaryEncoding.MIXED && testingPages.size() == 1),
                false);
    }

    private static List<Page> createDataPages(List<TestingPage> testingPages, ValuesWriter encoder, int maxDef, boolean required)
    {
        ImmutableList.Builder<Page> dataPages = ImmutableList.builder();

        long endOfPreviousRange = -1;
        for (TestingPage testingPage : testingPages) {
            dataPages.add(createDataPage(testingPage, encoder, maxDef, required, endOfPreviousRange));
            endOfPreviousRange = testingPage.pageRowRange().end();
        }

        return dataPages.build();
    }

    private static DataPage createDataPage(TestingPage testingPage, ValuesWriter encoder, int maxDef, boolean required, long endOfPreviousRange)
    {
        int rowCount = testingPage.getRowCount();
        int positionCount = testingPage.repetitionLevels().length - 1;
        int[] values = new int[positionCount];
        int valueCount = getPageValues(testingPage, maxDef, values, required, endOfPreviousRange);

        byte[] encodedBytes = encodePlainValues(encoder, values, valueCount);
        DataPage dataPage = new DataPageV2(
                rowCount,
                -1,
                positionCount,
                Slices.wrappedBuffer(encodeLevels(testingPage.repetitionLevels(), 1)),
                Slices.wrappedBuffer(encodeLevels(testingPage.definitionLevels(), maxDef)),
                getParquetEncoding(encoder.getEncoding()),
                Slices.wrappedBuffer(encodedBytes),
                valueCount * 4,
                OptionalLong.of(toIntExact(testingPage.pageRowRange().start())),
                null,
                false);
        encoder.reset();
        return dataPage;
    }

    private static int getPageValues(TestingPage testingPage, int maxDef, int[] values, boolean required, long endOfPreviousRange)
    {
        RowRange pageRowRange = testingPage.pageRowRange();
        int start = toIntExact(pageRowRange.start());
        int end = toIntExact(pageRowRange.end()) + 1;
        int[] repetitionLevels = testingPage.repetitionLevels();
        int[] definitionLevels = testingPage.definitionLevels();
        int valueCount = 0;
        int existingValueCount = 0;
        int positionCount = 0;

        // Finish the row started in the previous page
        for (; repetitionLevels[positionCount] != 0; positionCount++) {
            values[existingValueCount] = (int) endOfPreviousRange;
            valueCount += definitionLevels[positionCount] >= (maxDef - (required ? 0 : 1)) ? 1 : 0; // Count values and nulls
            existingValueCount += definitionLevels[positionCount] == maxDef ? 1 : 0; // count only non-null values
        }

        for (int i = start; i < end; positionCount++) {
            values[existingValueCount] = i;
            valueCount += definitionLevels[positionCount] >= (maxDef - (required ? 0 : 1)) ? 1 : 0; // Count values and nulls
            existingValueCount += definitionLevels[positionCount] == maxDef ? 1 : 0; // count only non-null values
            if (repetitionLevels[positionCount + 1] == 0) {
                i++;
            }
        }
        return valueCount;
    }

    private static byte[] encodeLevels(int[] values, int maxLevel)
    {
        int bitWidth = getWidthFromMaxInt(maxLevel);
        RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(bitWidth, values.length, values.length, HeapByteBufferAllocator.getInstance());
        try {
            for (int value : values) {
                encoder.writeInt(value);
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

    private static List<TestingPage> getTestingPages(
            RepetitionLevelsProvider repetitionLevelsProvider,
            DefinitionLevelsProvider definitionLevelsProvider,
            List<RowRange> pageRowRanges,
            boolean rowsAcrossPages)
    {
        ImmutableList.Builder<TestingPage> testingPages = ImmutableList.builder();
        for (int i = 0; i < pageRowRanges.size(); i++) {
            RowRange rowRange = pageRowRanges.get(i);
            int rowCount = toIntExact(rowRange.end() + 1 - rowRange.start());
            int[] repetitions = repetitionLevelsProvider.getRepetitionLevels(rowCount, rowsAcrossPages && i != 0);
            int[] definitions = definitionLevelsProvider.getDefinitionLevels(repetitions.length - 1);
            testingPages.add(new TestingPage(rowRange, repetitions, definitions));
        }

        return testingPages.build();
    }

    record TestingPage(RowRange pageRowRange, int[] repetitionLevels, int[] definitionLevels)
    {
        int getRowCount()
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
