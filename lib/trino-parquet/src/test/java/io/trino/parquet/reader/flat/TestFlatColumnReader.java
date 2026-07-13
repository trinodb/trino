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
package io.trino.parquet.reader.flat;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.parquet.DataPage;
import io.trino.parquet.DataPageV1;
import io.trino.parquet.DictionaryPage;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetEncoding;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.reader.AbstractColumnReaderTest;
import io.trino.parquet.reader.ColumnReader;
import io.trino.parquet.reader.ColumnReaderFactory;
import io.trino.parquet.reader.PageReader;
import io.trino.parquet.reader.TestingColumnReader.ColumnReaderFormat;
import io.trino.parquet.reader.TestingColumnReader.DataPageVersion;
import io.trino.spi.block.Block;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Random;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetEncoding.BIT_PACKED;
import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.ParquetEncoding.RLE;
import static io.trino.parquet.ParquetEncoding.RLE_DICTIONARY;
import static io.trino.parquet.reader.TestingColumnReader.DataPageVersion.V1;
import static io.trino.parquet.reader.TestingColumnReader.getDictionaryPage;
import static io.trino.parquet.reader.TestingValuesWriters.getValuesWriter;
import static io.trino.spi.type.IntegerType.INTEGER;
import static org.apache.parquet.format.CompressionCodec.UNCOMPRESSED;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.joda.time.DateTimeZone.UTC;

/**
 * The purpose of this class is to cover all code paths of FlatColumnReader with all supported data types.
 */
public class TestFlatColumnReader
        extends AbstractColumnReaderTest
{
    private static final PrimitiveType TYPE = new PrimitiveType(REQUIRED, INT32, "");
    private static final PrimitiveType OPTIONAL_TYPE = new PrimitiveType(OPTIONAL, INT32, "");
    private static final PrimitiveField NULLABLE_FIELD = new PrimitiveField(INTEGER, false, new ColumnDescriptor(new String[] {"test"}, TYPE, 0, 0), 0);
    private static final PrimitiveField OPTIONAL_FIELD = new PrimitiveField(INTEGER, false, new ColumnDescriptor(new String[] {"test"}, OPTIONAL_TYPE, 0, 1), 0);
    private static final PrimitiveField FIELD = new PrimitiveField(INTEGER, true, new ColumnDescriptor(new String[] {"test"}, TYPE, 0, 0), 0);

    @Override
    protected ColumnReader createColumnReader(PrimitiveField field)
    {
        ColumnReaderFactory columnReaderFactory = new ColumnReaderFactory(UTC, ParquetReaderOptions.defaultOptions());
        ColumnReader columnReader = columnReaderFactory.create(field, newSimpleAggregatedMemoryContext());
        assertThat(columnReader).isInstanceOf(FlatColumnReader.class);
        return columnReader;
    }

    @Test
    public void testReadPageV1BitPacked()
            throws IOException
    {
        FlatColumnReader<?> reader = (FlatColumnReader<?>) createColumnReader(NULLABLE_FIELD);
        reader.setPageReader(getSimplePageReaderMock(BIT_PACKED), Optional.empty());
        reader.prepareNextRead(1);

        assertThatThrownBy(reader::readNullable)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid definition level encoding: BIT_PACKED");
    }

    @Test
    public void testReadPageV1BitPackedNoNulls()
            throws IOException
    {
        FlatColumnReader<?> reader = (FlatColumnReader<?>) createColumnReader(FIELD);
        reader.setPageReader(getSimplePageReaderMock(BIT_PACKED), Optional.empty());
        reader.prepareNextRead(1);

        reader.readNonNull();
    }

    @Test
    public void testReadPageV1RleNoNulls()
            throws IOException
    {
        FlatColumnReader<?> reader = (FlatColumnReader<?>) createColumnReader(FIELD);
        assertThat(FIELD.isRequired()).isTrue();
        assertThat(FIELD.getDescriptor().getMaxDefinitionLevel()).isEqualTo(0);
        reader.setPageReader(getSimplePageReaderMock(RLE), Optional.empty());
        reader.prepareNextRead(1);

        Block block = reader.readNonNull().getBlock();
        assertThat(block.getPositionCount()).isEqualTo(1);
        assertThat(INTEGER.getInt(block, 0)).isEqualTo(42);
    }

    @Test
    public void testReadPageV1RleOnlyNulls()
            throws IOException
    {
        FlatColumnReader<?> reader = (FlatColumnReader<?>) createColumnReader(NULLABLE_FIELD);
        assertThat(NULLABLE_FIELD.isRequired()).isFalse();
        assertThat(NULLABLE_FIELD.getDescriptor().getMaxDefinitionLevel()).isEqualTo(0);
        reader.setPageReader(getNullOnlyPageReaderMock(), Optional.empty());
        reader.prepareNextRead(1);

        Block block = reader.readNullable().getBlock();
        assertThat(block.getPositionCount()).isEqualTo(1);
        assertThat(block.isNull(0)).isTrue();
    }

    @Test
    public void testReadSelectedPositions()
            throws IOException
    {
        FlatColumnReader<?> reader = (FlatColumnReader<?>) createColumnReader(FIELD);
        reader.setPageReader(getPlainPageReaderMock(
                        createPlainDataPage(0, 1, 2, 3, 4),
                        createPlainDataPage(5, 6, 7, 8, 9),
                        createPlainDataPage(10, 11, 12, 13, 14),
                        createPlainDataPage(15, 16)),
                Optional.empty());

        reader.prepareNextRead(15);
        Block selected = reader.readPrimitive(new int[] {99, 1, 2, 11, 14, 99}, 1, 4).getBlock();
        assertThat(intValues(selected)).containsExactly(1, 2, 11, 14);

        reader.prepareNextRead(2);
        assertThat(intValues(reader.readPrimitive().getBlock())).containsExactly(15, 16);
    }

    @ParameterizedTest
    @MethodSource("io.trino.parquet.reader.TestingColumnReader#readersWithPageVersions")
    public <T> void testReadSelectedPositionsAllTypes(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        PrimitiveField field = createField(format, true);
        ColumnReader reader = createColumnReader(field);
        ValuesWriter writer = format.getValuesWriter(version);

        T[] firstPageValues = format.write(writer, new Integer[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        DataPage firstPage = createDataPage(version, writer, field, new int[0], new int[10]);
        T[] secondPageValues = format.resetAndWrite(writer, new Integer[] {10, 11, 12, 13, 14, 15, 16, 17, 18, 19});
        DataPage secondPage = createDataPage(version, writer, field, new int[0], new int[10]);

        reader.setPageReader(getPageReaderMock(List.of(firstPage, secondPage), null), Optional.empty());
        reader.prepareNextRead(20);
        Block selected = reader.readPrimitive(new int[] {1, 2, 12, 19}, 0, 4).getBlock();

        format.assertBlock(firstPageValues, selected, 1, 0, 2);
        format.assertBlock(secondPageValues, selected, 2, 2, 1);
        format.assertBlock(secondPageValues, selected, 9, 3, 1);
    }

    @ParameterizedTest
    @MethodSource("io.trino.parquet.reader.TestingColumnReader#readersWithPageVersions")
    public <T> void testReadSelectedNullablePositionsAllTypes(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        PrimitiveField field = createField(format, false);
        ColumnReader reader = createColumnReader(field);
        ValuesWriter writer = format.getValuesWriter(version);
        T[] values = format.write(writer, new Integer[] {0, null, 2, 3, null, 5, 6, null, 8, 9});
        DataPage page = createNullableDataPage(version, writer, field, false, true, false, false, true, false, false, true, false, false);

        reader.setPageReader(getPageReaderMock(List.of(page), null), Optional.empty());
        reader.prepareNextRead(10);
        Block selected = reader.readPrimitive(new int[] {1, 2, 4, 7, 9}, 0, 5).getBlock();

        format.assertBlock(values, selected, 1, 0, 2);
        format.assertBlock(values, selected, 4, 2, 1);
        format.assertBlock(values, selected, 7, 3, 1);
        format.assertBlock(values, selected, 9, 4, 1);
    }

    @ParameterizedTest
    @MethodSource("io.trino.parquet.reader.TestingColumnReader#dictionaryReadersWithPageVersions")
    public <T> void testReadSelectedDictionaryPositionsAllTypes(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        PrimitiveField field = createField(format, true);
        ColumnReader reader = createColumnReader(field);
        DictionaryValuesWriter writer = format.getDictionaryWriter();
        T[] values = format.write(writer, new Integer[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        DataPage page = createDataPage(version, RLE_DICTIONARY, writer, field, new int[0], new int[10]);
        DictionaryPage dictionaryPage = getDictionaryPage(writer);

        reader.setPageReader(getPageReaderMock(List.of(page), dictionaryPage), Optional.empty());
        reader.prepareNextRead(10);
        Block selected = reader.readPrimitive(new int[] {1, 2, 7, 9}, 0, 4).getBlock();

        format.assertBlock(values, selected, 1, 0, 2);
        format.assertBlock(values, selected, 7, 2, 1);
        format.assertBlock(values, selected, 9, 3, 1);
    }

    @Test
    public void testReadEmptySelection()
            throws IOException
    {
        FlatColumnReader<?> reader = (FlatColumnReader<?>) createColumnReader(FIELD);
        reader.setPageReader(getPlainPageReaderMock(
                        createPlainDataPage(0, 1, 2, 3, 4),
                        createPlainDataPage(5, 6, 7, 8, 9),
                        createPlainDataPage(10, 11)),
                Optional.empty());

        reader.prepareNextRead(10);
        assertThat(reader.readPrimitive(new int[0], 0, 0).getBlock().getPositionCount()).isZero();

        reader.prepareNextRead(2);
        assertThat(intValues(reader.readPrimitive().getBlock())).containsExactly(10, 11);
    }

    @Test
    public void testReadSelectedNullablePositions()
            throws IOException
    {
        FlatColumnReader<?> reader = (FlatColumnReader<?>) createColumnReader(OPTIONAL_FIELD);
        reader.setPageReader(getPlainPageReaderMock(
                        createNullablePage(0, null, 2, 3, null),
                        createNullablePage(5, null, 7, null, 9)),
                Optional.empty());

        reader.prepareNextRead(10);
        Block selected = reader.readPrimitive(new int[] {1, 2, 3, 6, 8, 9}, 0, 6).getBlock();
        assertThat(nullableIntValues(selected)).containsExactly(null, 2, 3, null, null, 9);
    }

    @Test
    public void testReadSelectedDictionaryPositions()
            throws IOException
    {
        DictionaryValuesWriter writer = (DictionaryValuesWriter) getValuesWriter(RLE_DICTIONARY, INT32, OptionalInt.empty());
        for (int value : new int[] {10, 11, 10, 12, 11, 13, 10, 13}) {
            writer.writeInteger(value);
        }
        DataPage page = createDataPage(V1, RLE_DICTIONARY, writer, FIELD, new int[0], new int[8]);
        DictionaryPage dictionaryPage = getDictionaryPage(writer);

        FlatColumnReader<?> reader = (FlatColumnReader<?>) createColumnReader(FIELD);
        reader.setPageReader(getPageReaderMock(List.of(page), dictionaryPage), Optional.empty());
        reader.prepareNextRead(8);

        Block selected = reader.readPrimitive(new int[] {0, 2, 3, 6, 7}, 0, 5).getBlock();
        assertThat(intValues(selected)).containsExactly(10, 10, 12, 10, 13);
    }

    @Test
    public void testReadSelectedPositionsRandomized()
            throws IOException
    {
        int valueCount = 257;
        int[] values = new int[valueCount];
        for (int index = 0; index < valueCount; index++) {
            values[index] = index * 17;
        }

        Random random = new Random(918273645L);
        for (int iteration = 0; iteration < 50; iteration++) {
            int[] positions = random.ints(valueCount, 0, valueCount)
                    .distinct()
                    .sorted()
                    .limit(random.nextInt(valueCount + 1))
                    .toArray();

            FlatColumnReader<?> reader = (FlatColumnReader<?>) createColumnReader(FIELD);
            reader.setPageReader(getPlainPageReaderMock(
                            createPlainDataPage(Arrays.copyOfRange(values, 0, 64)),
                            createPlainDataPage(Arrays.copyOfRange(values, 64, 129)),
                            createPlainDataPage(Arrays.copyOfRange(values, 129, 200)),
                            createPlainDataPage(Arrays.copyOfRange(values, 200, valueCount))),
                    Optional.empty());
            reader.prepareNextRead(valueCount);

            Block selected = reader.readPrimitive(positions, 0, positions.length).getBlock();
            assertThat(intValues(selected)).containsExactly(Arrays.stream(positions)
                    .map(position -> values[position])
                    .boxed()
                    .toArray(Integer[]::new));
        }
    }

    private static PageReader getSimplePageReaderMock(ParquetEncoding encoding)
            throws IOException
    {
        ValuesWriter writer = getValuesWriter(PLAIN, INT32, OptionalInt.empty());
        writer.writeInteger(42);
        byte[] valueBytes = writer.getBytes().toByteArray();
        List<DataPage> pages = ImmutableList.of(new DataPageV1(
                Slices.wrappedBuffer(valueBytes),
                1,
                valueBytes.length,
                OptionalLong.empty(),
                encoding,
                encoding,
                PLAIN,
                0));
        return new PageReader(new ParquetDataSourceId("test"), UNCOMPRESSED, pages.iterator(), false, false, Optional.empty(), -1, -1);
    }

    private static DataPage createPlainDataPage(int... values)
            throws IOException
    {
        ValuesWriter writer = getValuesWriter(PLAIN, INT32, OptionalInt.empty());
        for (int value : values) {
            writer.writeInteger(value);
        }
        byte[] valueBytes = writer.getBytes().toByteArray();
        return new DataPageV1(
                Slices.wrappedBuffer(valueBytes),
                values.length,
                valueBytes.length,
                OptionalLong.empty(),
                RLE,
                RLE,
                PLAIN,
                0);
    }

    private DataPage createNullablePage(Integer... values)
            throws IOException
    {
        ValuesWriter writer = getValuesWriter(PLAIN, INT32, OptionalInt.empty());
        boolean[] isNull = new boolean[values.length];
        for (int index = 0; index < values.length; index++) {
            if (values[index] == null) {
                isNull[index] = true;
            }
            else {
                writer.writeInteger(values[index]);
            }
        }
        return createNullableDataPage(V1, writer, OPTIONAL_FIELD, isNull);
    }

    private static PageReader getPlainPageReaderMock(DataPage... pages)
    {
        return new PageReader(new ParquetDataSourceId("test"), UNCOMPRESSED, ImmutableList.copyOf(pages).iterator(), false, false, Optional.empty(), -1, -1);
    }

    private static List<Integer> intValues(Block block)
    {
        ImmutableList.Builder<Integer> values = ImmutableList.builder();
        for (int position = 0; position < block.getPositionCount(); position++) {
            values.add(INTEGER.getInt(block, position));
        }
        return values.build();
    }

    private static List<Integer> nullableIntValues(Block block)
    {
        List<Integer> values = new ArrayList<>(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            values.add(block.isNull(position) ? null : INTEGER.getInt(block, position));
        }
        return values;
    }

    private static PageReader getNullOnlyPageReaderMock()
            throws IOException
    {
        RunLengthBitPackingHybridValuesWriter nullsWriter = new RunLengthBitPackingHybridValuesWriter(1, 1, 1, HeapByteBufferAllocator.getInstance());
        nullsWriter.writeInteger(0);
        byte[] nullBytes = nullsWriter.getBytes().toByteArray();
        List<DataPage> pages = ImmutableList.of(new DataPageV1(
                Slices.wrappedBuffer(nullBytes),
                1,
                nullBytes.length,
                OptionalLong.empty(),
                RLE,
                RLE,
                PLAIN,
                0));
        return new PageReader(new ParquetDataSourceId("test"), UNCOMPRESSED, pages.iterator(), false, false, Optional.empty(), -1, -1);
    }
}
