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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.DataPage;
import io.trino.parquet.DictionaryPage;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.reader.TestingColumnReader.ColumnReaderFormat;
import io.trino.parquet.reader.TestingColumnReader.DataPageVersion;
import io.trino.spi.block.Block;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetEncoding.RLE_DICTIONARY;
import static io.trino.parquet.reader.NestedColumnReader.isSingleRleRun;
import static io.trino.parquet.reader.TestingColumnReader.getDictionaryPage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.UTC;

public class TestNestedColumnReader
        extends AbstractColumnReaderTest
{
    @Override
    protected ColumnReader createColumnReader(PrimitiveField field)
    {
        return createColumnReader(field, newSimpleAggregatedMemoryContext());
    }

    private static ColumnReader createColumnReader(PrimitiveField field, AggregatedMemoryContext memoryContext)
    {
        ColumnDescriptor descriptor = field.getDescriptor();
        PrimitiveField tesingField = new PrimitiveField(
                field.getType(),
                field.isRequired(),
                new ColumnDescriptor(
                        // Force nested readers
                        new String[] {"dummy", "dummy_2_to_skip_flat_readers"},
                        descriptor.getPrimitiveType(),
                        descriptor.getMaxRepetitionLevel(),
                        descriptor.getMaxDefinitionLevel()),
                field.getId());
        ColumnReaderFactory columnReaderFactory = new ColumnReaderFactory(UTC, ParquetReaderOptions.defaultOptions());
        ColumnReader columnReader = columnReaderFactory.create(tesingField, memoryContext);
        assertThat(columnReader).isInstanceOf(NestedColumnReader.class);
        return columnReader;
    }

    @Override
    @ParameterizedTest
    @MethodSource("io.trino.parquet.reader.TestingColumnReader#dictionaryReadersWithPageVersions")
    public <T> void testSingleValueDictionaryNullableWithNoNullsUsingColumnStats(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false);
        ColumnReader reader = createColumnReader(field);
        // Write data
        DictionaryValuesWriter dictionaryWriter = format.getDictionaryWriter();
        T[] values = format.write(dictionaryWriter, new Integer[] {1, 1});
        DataPage page = createNullableDataPage(version, RLE_DICTIONARY, dictionaryWriter, field, false, false);
        DictionaryPage dictionaryPage = getDictionaryPage(dictionaryWriter);
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page), dictionaryPage, true), Optional.empty());
        reader.prepareNextRead(2);
        Block actual = reader.readPrimitive().getBlock();
        format.assertBlock(values, actual);
    }

    @ParameterizedTest
    @MethodSource("io.trino.parquet.reader.TestingColumnReader#readersWithPageVersions")
    public <T> void testMultipleValuesPerPosition(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, true, 1, 0);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getValuesWriter(version);
        T[] values1 = format.write(writer, new Integer[] {1, 2});
        DataPage page1 = createDataPage(version, writer, field, new int[] {0, 1});
        T[] values2 = format.resetAndWrite(writer, new Integer[] {3, 4, 5});
        DataPage page2 = createDataPage(version, writer, field, new int[] {0, 1, 0});
        T[] values3 = format.resetAndWrite(writer, new Integer[] {6, 7, 8});
        DataPage page3 = createDataPage(version, writer, field, new int[] {0, 0, 1});
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page1, page2, page3), null), Optional.empty());
        Block actual1 = readBlock(reader, 1, 2); // Parquet/Trino page size the same
        Block actual2 = readBlock(reader, 1, 2); // Parquet page bigger than Trino
        Block actual3 = readBlock(reader, 3, 4); // Parquet page smaller than Trino

        format.assertBlock(values1, actual1);
        format.assertBlock(values2, actual2, 0, 0, 2);
        format.assertBlock(values2, actual3, 2, 0, 1);
        format.assertBlock(values3, actual3, 0, 1, 3);
    }

    @ParameterizedTest
    @MethodSource("io.trino.parquet.reader.TestingColumnReader#readersWithPageVersions")
    public <T> void testDefinitionLevelNullable(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false, 0, 2);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getValuesWriter(version);
        T[] values1 = format.write(writer, new Integer[] {null, 1});
        // First value is ignored, the second is null and the third is an actual value
        DataPage page1 = createDataPage(version, writer, field, new int[0], new int[] {0, 1, 2});
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page1), null), Optional.empty());
        Block actual1 = readBlock(reader, 3, 2);

        format.assertBlock(values1, actual1);
    }

    @ParameterizedTest
    @MethodSource("io.trino.parquet.reader.TestingColumnReader#readersWithPageVersions")
    public <T> void testDefinitionLevelNullableWithNoNulls(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false, 0, 2);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getValuesWriter(version);
        T[] values1 = format.write(writer, new Integer[] {1});
        // First two values are ignored, and the third is an actual value
        DataPage page1 = createDataPage(version, writer, field, new int[0], new int[] {0, 0, 2});
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page1), null), Optional.empty());
        Block actual1 = readBlock(reader, 3, 1);

        format.assertBlock(values1, actual1);
    }

    @Test
    public void testIsSingleRleRun()
    {
        // RLE run of 8096 values of level 3, as TestDefinitionLevelWriter writes it
        Slice levels = Slices.wrappedBuffer((byte) 0xC0, (byte) 0x7E, (byte) 3);
        assertThat(isSingleRleRun(levels, 3, 8096)).isTrue();
        assertThat(isSingleRleRun(levels, 3, 8097)).isFalse();
        assertThat(isSingleRleRun(levels, 2, 8096)).isFalse();
        assertThat(isSingleRleRun(levels.slice(0, 2), 3, 8096)).isFalse();
        // Bit-packed group of eight values of level 3
        assertThat(isSingleRleRun(Slices.wrappedBuffer((byte) 3, (byte) 0xFF, (byte) 0xFF), 3, 8)).isFalse();
        // Five byte header with bits above 32 set, the decoder drops them so the run covers a single value only
        assertThat(isSingleRleRun(Slices.wrappedBuffer((byte) 0x82, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x10, (byte) 2), 2, 2)).isFalse();
    }

    @ParameterizedTest
    @MethodSource("io.trino.parquet.reader.TestingColumnReader#readersWithPageVersions")
    public <T> void testDefinitionLevelNullableAllDefinedRunFollowedByNull(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false, 0, 2);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getValuesWriter(version);
        Integer[] data = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, null};
        T[] values = format.write(writer, data);
        // The levels start with an RLE run of the max definition level which does not cover the whole page
        int[] definition = new int[data.length];
        Arrays.fill(definition, 2);
        definition[data.length - 1] = 1;
        DataPage page = createDataPage(version, writer, field, new int[0], definition);
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page), null), Optional.empty());
        Block actual = readBlock(reader, data.length, data.length);

        format.assertBlock(values, actual);
    }

    @ParameterizedTest
    @MethodSource("io.trino.parquet.reader.TestingColumnReader#readersWithPageVersions")
    public <T> void testDefinitionLevelNullableAllDefinedPageFollowedByPageWithNulls(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false, 0, 2);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getValuesWriter(version);
        Integer[] data1 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        T[] values1 = format.write(writer, data1);
        int[] definition1 = new int[data1.length];
        Arrays.fill(definition1, 2);
        DataPage page1 = createDataPage(version, writer, field, new int[0], definition1);
        Integer[] data2 = {11, null, 13, null};
        T[] values2 = format.resetAndWrite(writer, data2);
        DataPage page2 = createDataPage(version, writer, field, new int[0], new int[] {2, 1, 2, 1});
        // Read and assert, the first batch spans both pages
        reader.setPageReader(getPageReaderMock(List.of(page1, page2), null), Optional.empty());
        Block actual1 = readBlock(reader, 12, 12);
        Block actual2 = readBlock(reader, 2, 2);

        format.assertBlock(values1, actual1, 0, 0, 10);
        format.assertBlock(values2, actual1, 0, 10, 2);
        format.assertBlock(values2, actual2, 2, 0, 2);
    }

    @ParameterizedTest
    @MethodSource("io.trino.parquet.reader.TestingColumnReader#readersWithPageVersions")
    public <T> void testRowSpanningPagesWithAllDefinedLevels(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false, 1, 2);
        ColumnReader reader = createColumnReader(field);
        // Write data, every definition level in both pages is the max one and the RLE runs cover the whole page
        ValuesWriter writer = format.getValuesWriter(version);
        T[] values1 = format.write(writer, new Integer[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        DataPage page1 = createDataPage(version, writer, field, new int[] {0, 1, 1, 1, 0, 1, 1, 1, 1, 1}, allDefined(10));
        T[] values2 = format.resetAndWrite(writer, new Integer[] {11, 12, 13, 14, 15, 16, 17, 18, 19, 20});
        DataPage page2 = createDataPage(version, writer, field, new int[] {1, 1, 0, 1, 1, 1, 0, 1, 1, 1}, allDefined(10));
        // Read and assert, the second row starts in the first page and ends in the second one
        reader.setPageReader(getPageReaderMock(List.of(page1, page2), null), Optional.empty());
        Block actual = readBlock(reader, 4, 20);

        format.assertBlock(values1, actual, 0, 0, 10);
        format.assertBlock(values2, actual, 0, 10, 10);
    }

    @ParameterizedTest
    @MethodSource("io.trino.parquet.reader.TestingColumnReader#readersWithPageVersions")
    public <T> void testSeekOverRowsWithAllDefinedLevels(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false, 1, 2);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getValuesWriter(version);
        T[] values1 = format.write(writer, new Integer[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        DataPage page1 = createDataPage(version, writer, field, new int[] {0, 1, 1, 1, 0, 1, 1, 1, 1, 1}, allDefined(10));
        T[] values2 = format.resetAndWrite(writer, new Integer[] {11, 12, 13, 14, 15, 16, 17, 18, 19, 20});
        DataPage page2 = createDataPage(version, writer, field, new int[] {1, 1, 0, 1, 1, 1, 0, 1, 1, 1}, allDefined(10));
        // Skip the first row and read the second one that continues on the second page
        reader.setPageReader(getPageReaderMock(List.of(page1, page2), null), Optional.empty());
        reader.prepareNextRead(1);
        Block actual1 = readBlock(reader, 1, 8);
        // Skip the third row and read the fourth one
        reader.prepareNextRead(1);
        Block actual2 = readBlock(reader, 1, 4);

        format.assertBlock(values1, actual1, 4, 0, 6);
        format.assertBlock(values2, actual1, 0, 6, 2);
        format.assertBlock(values2, actual2, 6, 0, 4);
    }

    @ParameterizedTest
    @MethodSource("io.trino.parquet.reader.TestingColumnReader#dictionaryReadersWithPageVersions")
    public <T> void testDictionaryWithAllDefinedLevels(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false, 0, 2);
        ColumnReader reader = createColumnReader(field);
        // Write data
        DictionaryValuesWriter dictionaryWriter = format.getDictionaryWriter();
        T[] values = format.write(dictionaryWriter, new Integer[] {1, 2, 1, 2, 1, 2, 1, 2, 1, 2});
        DataPage page = createDataPage(version, RLE_DICTIONARY, dictionaryWriter, field, new int[0], allDefined(10));
        DictionaryPage dictionaryPage = getDictionaryPage(dictionaryWriter);
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page), dictionaryPage, true), Optional.empty());
        Block actual = readBlock(reader, 10, 10);

        format.assertBlock(values, actual);
    }

    private static int[] allDefined(int count)
    {
        int[] definition = new int[count];
        Arrays.fill(definition, 2);
        return definition;
    }

    @ParameterizedTest
    @MethodSource("io.trino.parquet.reader.TestingColumnReader#readersWithPageVersions")
    public <T> void testDefinitionLevelNonNull(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, true, 0, 2);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getValuesWriter(version);
        T[] values1 = format.write(writer, new Integer[] {1});
        // First two values are ignored and the third is an actual value
        DataPage page1 = createDataPage(version, writer, field, new int[0], new int[] {0, 1, 2});
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page1), null), Optional.empty());
        Block actual1 = readBlock(reader, 3, 1);

        format.assertBlock(values1, actual1);
    }

    @ParameterizedTest
    @MethodSource("io.trino.parquet.reader.TestingColumnReader#dictionaryReadersWithPageVersions")
    public <T> void testMemoryUsageWithoutRepetitionLevels(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false, 0, 2);
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        ColumnReader reader = createColumnReader(field, memoryContext);
        // Write data
        DictionaryValuesWriter dictionaryWriter = format.getDictionaryWriter();
        format.write(dictionaryWriter, new Integer[] {1, 2, 3});
        DataPage page1 = createDataPage(version, RLE_DICTIONARY, dictionaryWriter, field, new int[0], new int[] {2, 2, 2});
        format.resetAndWrite(dictionaryWriter, new Integer[] {2, 4, 5, 3, 7});
        DataPage page2 = createDataPage(version, RLE_DICTIONARY, dictionaryWriter, field, new int[0], new int[] {2, 2, 2, 2, 2});
        format.resetAndWrite(dictionaryWriter, new Integer[] {1});
        DataPage page3 = createDataPage(version, RLE_DICTIONARY, dictionaryWriter, field, new int[0], new int[] {2});
        DictionaryPage dictionaryPage = getDictionaryPage(dictionaryWriter);
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page1, page2, page3), dictionaryPage), Optional.empty());
        readBlock(reader, 3, 3);
        long memoryUsage = memoryContext.getBytes();
        assertThat(memoryUsage).isGreaterThan(0);
        readBlock(reader, 5, 5);
        // Memory usage does not depend on the value count of the current page
        assertThat(memoryContext.getBytes()).isEqualTo(memoryUsage);
    }

    @ParameterizedTest
    @MethodSource("io.trino.parquet.reader.TestingColumnReader#dictionaryReadersWithPageVersions")
    public <T> void testDefinitionLevelNullableWithDictionaries(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false, 0, 2);
        ColumnReader reader = createColumnReader(field);
        // Write data
        DictionaryValuesWriter dictionaryWriter = format.getDictionaryWriter();
        T[] values1 = format.write(dictionaryWriter, new Integer[] {null, 1});
        // First value is ignored, the second is null and the third is an actual value
        DataPage page1 = createDataPage(version, RLE_DICTIONARY, dictionaryWriter, field, new int[0], new int[] {0, 1, 2});
        DictionaryPage dictionaryPage = getDictionaryPage(dictionaryWriter);
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page1), dictionaryPage), Optional.empty());
        Block actual1 = readBlock(reader, 3, 2);

        format.assertBlock(values1, actual1);
    }

    @ParameterizedTest
    @MethodSource("io.trino.parquet.reader.TestingColumnReader#dictionaryReadersWithPageVersions")
    public <T> void testDefinitionLevelNullableWithDictionariesAndNoNulls(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false, 0, 2);
        ColumnReader reader = createColumnReader(field);
        // Write data
        DictionaryValuesWriter dictionaryWriter = format.getDictionaryWriter();
        T[] values1 = format.write(dictionaryWriter, new Integer[] {1});
        // First two values are ignored, and the third is an actual value
        DataPage page1 = createDataPage(version, RLE_DICTIONARY, dictionaryWriter, field, new int[0], new int[] {0, 0, 2});
        DictionaryPage dictionaryPage = getDictionaryPage(dictionaryWriter);
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page1), dictionaryPage), Optional.empty());
        Block actual1 = readBlock(reader, 3, 1);

        format.assertBlock(values1, actual1);
    }

    @ParameterizedTest
    @MethodSource("io.trino.parquet.reader.TestingColumnReader#dictionaryReadersWithPageVersions")
    public <T> void testDefinitionLevelNonNullWithDictionaries(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, true, 0, 2);
        ColumnReader reader = createColumnReader(field);
        // Write data
        DictionaryValuesWriter dictionaryWriter = format.getDictionaryWriter();
        T[] values1 = format.write(dictionaryWriter, new Integer[] {1});
        // First two values are ignored and the third is an actual value
        DataPage page1 = createDataPage(version, RLE_DICTIONARY, dictionaryWriter, field, new int[0], new int[] {0, 1, 2});
        DictionaryPage dictionaryPage = getDictionaryPage(dictionaryWriter);
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page1), dictionaryPage), Optional.empty());
        Block actual1 = readBlock(reader, 3, 1);

        format.assertBlock(values1, actual1);
    }

    @ParameterizedTest
    @MethodSource("io.trino.parquet.reader.TestingColumnReader#dictionaryReadersWithPageVersions")
    public <T> void testSingleRowPerTwoPagesNonNull(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, true, 1, 0);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getValuesWriter(version);
        T[] values1 = format.write(writer, new Integer[] {1, 2});
        DataPage page1 = createDataPage(version, writer, field, new int[] {0, 1});
        T[] values2 = format.resetAndWrite(writer, new Integer[] {3, 4, 5});
        DataPage page2 = createDataPage(version, writer, field, new int[] {1, 1, 0});
        T[] values3 = format.resetAndWrite(writer, new Integer[] {6, 7, 8});
        DataPage page3 = createDataPage(version, writer, field, new int[] {1, 0, 1});
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page1, page2, page3), null), Optional.empty());
        Block actual1 = readBlock(reader, 3, 8);

        format.assertBlock(values1, actual1, 0, 0, 2);
        format.assertBlock(values2, actual1, 0, 2, 3);
        format.assertBlock(values3, actual1, 0, 5, 3);
    }

    @ParameterizedTest
    @MethodSource("io.trino.parquet.reader.TestingColumnReader#dictionaryReadersWithPageVersions")
    public <T> void testSingleRowPerTwoPagesNullable(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false, 1, 2);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getValuesWriter(version);
        T[] values1 = format.write(writer, new Integer[] {null});
        DataPage page1 = createDataPage(version, writer, field, new int[] {0, 1}, new int[] {0, 1});
        T[] values2 = format.resetAndWrite(writer, new Integer[] {1, null});
        DataPage page2 = createDataPage(version, writer, field, new int[] {1, 1, 0}, new int[] {2, 0, 1});
        T[] values3 = format.resetAndWrite(writer, new Integer[] {2, null});
        DataPage page3 = createDataPage(version, writer, field, new int[] {1, 0, 1}, new int[] {2, 0, 1});
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page1, page2, page3), null), Optional.empty());
        Block actual1 = readBlock(reader, 3, 5);

        format.assertBlock(values1, actual1, 0, 0, 1);
        format.assertBlock(values2, actual1, 0, 1, 2);
        format.assertBlock(values3, actual1, 0, 3, 2);
    }

    @ParameterizedTest
    @MethodSource("io.trino.parquet.reader.TestingColumnReader#dictionaryReadersWithPageVersions")
    public <T> void testSingleRowPerMultiplePagesNonNull(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, true, 1, 0);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getValuesWriter(version);
        T[] values1 = format.write(writer, new Integer[] {1, 2});
        DataPage page1 = createDataPage(version, writer, field, new int[] {0, 1});
        T[] values2 = format.resetAndWrite(writer, new Integer[] {3, 4, 5});
        DataPage page2 = createDataPage(version, writer, field, new int[] {1, 1, 1});
        T[] values3 = format.resetAndWrite(writer, new Integer[] {6, 7, 8});
        DataPage page3 = createDataPage(version, writer, field, new int[] {1, 0, 1});
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page1, page2, page3), null), Optional.empty());
        Block actual1 = readBlock(reader, 2, 8);

        format.assertBlock(values1, actual1, 0, 0, 2);
        format.assertBlock(values2, actual1, 0, 2, 3);
        format.assertBlock(values3, actual1, 0, 5, 3);
    }

    @ParameterizedTest
    @MethodSource("io.trino.parquet.reader.TestingColumnReader#dictionaryReadersWithPageVersions")
    public <T> void testSingleRowPerMultiplePagesNullable(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false, 1, 2);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getValuesWriter(version);
        T[] values1 = format.write(writer, new Integer[] {null});
        DataPage page1 = createDataPage(version, writer, field, new int[] {0, 1}, new int[] {0, 1});
        T[] values2 = format.resetAndWrite(writer, new Integer[] {1, null});
        DataPage page2 = createDataPage(version, writer, field, new int[] {1, 1, 1}, new int[] {2, 0, 1});
        T[] values3 = format.resetAndWrite(writer, new Integer[] {2, null});
        DataPage page3 = createDataPage(version, writer, field, new int[] {1, 0, 1}, new int[] {2, 0, 1});
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page1, page2, page3), null), Optional.empty());
        Block actual1 = readBlock(reader, 2, 5);

        format.assertBlock(values1, actual1, 0, 0, 1);
        format.assertBlock(values2, actual1, 0, 1, 2);
        format.assertBlock(values3, actual1, 0, 3, 2);
    }

    @ParameterizedTest
    @MethodSource("io.trino.parquet.reader.TestingColumnReader#dictionaryReadersWithPageVersions")
    public <T> void testSingleRowPerMultiplePagesNonNullSeek(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, true, 1, 0);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getValuesWriter(version);
        format.write(writer, new Integer[] {1, 2});
        DataPage page1 = createDataPage(version, writer, field, new int[] {0, 1});
        format.resetAndWrite(writer, new Integer[] {3, 4, 5});
        DataPage page2 = createDataPage(version, writer, field, new int[] {1, 1, 1});
        T[] values3 = format.resetAndWrite(writer, new Integer[] {6, 7, 8});
        DataPage page3 = createDataPage(version, writer, field, new int[] {1, 0, 1});
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page1, page2, page3), null), Optional.empty());
        reader.prepareNextRead(1); // Skip first value
        Block actual1 = readBlock(reader, 1, 2);

        format.assertBlock(values3, actual1, 1, 0, 2);
    }

    @ParameterizedTest
    @MethodSource("io.trino.parquet.reader.TestingColumnReader#dictionaryReadersWithPageVersions")
    public <T> void testSingleRowPerMultiplePagesNullableSeek(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false, 1, 2);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getValuesWriter(version);
        format.write(writer, new Integer[] {null});
        DataPage page1 = createDataPage(version, writer, field, new int[] {0, 1}, new int[] {0, 1});
        format.resetAndWrite(writer, new Integer[] {1, null});
        DataPage page2 = createDataPage(version, writer, field, new int[] {1, 1, 1}, new int[] {2, 0, 1});
        T[] values3 = format.resetAndWrite(writer, new Integer[] {2, null});
        DataPage page3 = createDataPage(version, writer, field, new int[] {1, 0, 1}, new int[] {2, 0, 1});
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page1, page2, page3), null), Optional.empty());
        reader.prepareNextRead(1); // Skip first value
        Block actual1 = readBlock(reader, 1, 1);

        format.assertBlock(values3, actual1, 1, 0, 1);
    }

    private static DataPage createDataPage(
            DataPageVersion version,
            ValuesWriter writer,
            PrimitiveField field,
            int[] repetition)
            throws IOException
    {
        return createDataPage(version, writer, field, repetition, new int[0]);
    }
}
