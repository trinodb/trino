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
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.RichColumnDescriptor;
import io.trino.spi.block.Block;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.apache.parquet.schema.PrimitiveType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.DataProviders.cartesianProduct;
import static io.trino.testing.DataProviders.concat;
import static io.trino.testing.DataProviders.toDataProvider;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.internal.filter2.columnindex.TestingRowRanges.RowRange;
import static org.apache.parquet.internal.filter2.columnindex.TestingRowRanges.range;
import static org.apache.parquet.internal.filter2.columnindex.TestingRowRanges.toRowRange;
import static org.apache.parquet.internal.filter2.columnindex.TestingRowRanges.toRowRanges;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;

public class TestColumnReader
{
    private static final PrimitiveType TYPE = new PrimitiveType(REQUIRED, INT32, "");
    private static final PrimitiveField NULLABLE_FIELD = new PrimitiveField(INTEGER, 0, 0, false, new RichColumnDescriptor(new ColumnDescriptor(new String[] {}, TYPE, 0, 0), TYPE), 0);
    private static final PrimitiveField FIELD = new PrimitiveField(INTEGER, 0, 0, true, new RichColumnDescriptor(new ColumnDescriptor(new String[] {}, TYPE, 0, 0), TYPE), 0);

    @Test(dataProvider = "testRowRangesProvider")
    public void testReadFilteredPage(
            ColumnReaderInput columnReaderInput,
            BatchSkipper skipper,
            Optional<RowRanges> rowRanges,
            List<RowRange> pageRowRanges)
    {
        PrimitiveColumnReader reader = columnReaderInput.createColumnReader();
        reader.setPageReader(
                new TestingPageReader(pageRowRanges),
                rowRanges.orElse(null));

        int rowCount = rowRanges.map(ranges -> toIntExact(ranges.rowCount()))
                .orElseGet(() -> pagesRowCount(pageRowRanges));
        List<Long> valuesRead = new ArrayList<>(rowCount);
        List<Long> expectedValues = new ArrayList<>(rowCount);
        PrimitiveIterator.OfLong rowRangesIterator = rowRanges.map(RowRanges::iterator)
                .orElseGet(() -> LongStream.range(pageRowRanges.get(0).getStart(), rowCount).iterator());

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
                Block block = reader.readPrimitive(columnReaderInput.getField()).getBlock();
                assertThat(block.getPositionCount()).isEqualTo(batchSize);
                for (int i = 0; i < block.getPositionCount(); i++) {
                    valuesRead.add((long) block.getInt(i, 0));
                    expectedValues.add(rowRangesIterator.next());
                }
            }

            readCount += batchSize;
            batchSize = Math.min(batchSize * 2, rowCount - readCount);
        }
        assertThat(rowRangesIterator.hasNext()).isFalse();
        assertThat(valuesRead).isEqualTo(expectedValues);
    }

    @DataProvider
    public Object[][] testRowRangesProvider()
    {
        Object[][] columnReaders = Stream.of(ColumnReaderInput.values())
                .collect(toDataProvider());
        Object[][] batchSkippers = Stream.of(BatchSkipper.values())
                .collect(toDataProvider());
        Object[][] rowRanges = Stream.of(
                        Optional.empty(),
                        Optional.of(toRowRange(1024)),
                        Optional.of(toRowRange(956)),
                        Optional.of(toRowRanges(range(101, 900))),
                        Optional.of(toRowRanges(range(56, 89), range(120, 250), range(300, 455), range(600, 980))))
                .collect(toDataProvider());
        Object[][] pageRowRanges = Stream.of(
                        ImmutableList.of(range(0, 1023)),
                        ImmutableList.of(range(0, 127), range(128, 1023)),
                        ImmutableList.of(range(0, 767), range(768, 1023)),
                        ImmutableList.of(range(0, 255), range(256, 511), range(512, 767), range(768, 1023)),
                        ImmutableList.of(range(0, 99), range(100, 199), range(200, 399), range(400, 599), range(600, 799), range(800, 999), range(1000, 1023)))
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

    private enum ColumnReaderInput
    {
        INT_PRIMITIVE_NO_NULLS(() -> new IntColumnReader(FIELD.getDescriptor()), FIELD),
        INT_PRIMITIVE_NULLABLE(() -> new IntColumnReader(NULLABLE_FIELD.getDescriptor()), NULLABLE_FIELD);

        private final Supplier<PrimitiveColumnReader> columnReader;
        private final PrimitiveField field;

        ColumnReaderInput(Supplier<PrimitiveColumnReader> columnReader, PrimitiveField field)
        {
            this.columnReader = requireNonNull(columnReader, "columnReader is null");
            this.field = requireNonNull(field, "field is null");
        }

        PrimitiveColumnReader createColumnReader()
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
                Random random = new Random(42);
                return random::nextBoolean;
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

    private static class TestingPageReader
            extends PageReader
    {
        private final Queue<RowRange> pageRowRanges;

        public TestingPageReader(List<RowRange> pageRowRanges)
        {
            super(
                    UNCOMPRESSED,
                    new LinkedList<>(),
                    null,
                    null,
                    pagesRowCount(pageRowRanges));
            this.pageRowRanges = new LinkedList<>(pageRowRanges);
        }

        @Override
        public DataPage readPage()
        {
            if (pageRowRanges.isEmpty()) {
                return null;
            }
            return createDataPage(pageRowRanges.remove());
        }

        private static DataPage createDataPage(RowRange pageRowRange)
        {
            int start = toIntExact(pageRowRange.getStart());
            int end = toIntExact(pageRowRange.getEnd()) + 1;
            int valueCount = end - start;
            int[] values = new int[valueCount];
            boolean[] required = new boolean[valueCount];
            for (int i = start; i < end; i++) {
                values[i - start] = i;
                required[i - start] = true;
            }
            return new DataPageV2(
                    valueCount,
                    0,
                    valueCount,
                    Slices.wrappedBuffer(),
                    Slices.wrappedBuffer(encodeDefinitionLevels(required)),
                    PLAIN,
                    Slices.wrappedBuffer(encodePlainValues(values)),
                    valueCount * 4,
                    OptionalLong.of(start),
                    new IntStatistics(),
                    false);
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

        private static byte[] encodePlainValues(int[] values)
        {
            PlainValuesWriter encoder = new PlainValuesWriter(values.length, values.length, HeapByteBufferAllocator.getInstance());
            try {
                for (int value : values) {
                    encoder.writeInteger(value);
                }
                return encoder.getBytes().toByteArray();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static int pagesRowCount(List<RowRange> pageRowRanges)
    {
        return pageRowRanges.stream()
                .mapToInt(range -> toIntExact(range.getEnd() - range.getStart() + 1))
                .sum();
    }
}
