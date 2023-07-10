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

import io.trino.parquet.DataPage;
import io.trino.parquet.DictionaryPage;
import io.trino.parquet.ParquetEncoding;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.PrimitiveField;
import io.trino.spi.block.Block;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.ParquetEncoding.RLE_DICTIONARY;
import static io.trino.parquet.reader.TestingColumnReader.ColumnReaderFormat;
import static io.trino.parquet.reader.TestingColumnReader.DataPageVersion;
import static io.trino.parquet.reader.TestingColumnReader.getDictionaryPage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.UTC;

public class TestNestedColumnReader
        extends AbstractColumnReaderTest
{
    @Override
    protected ColumnReader createColumnReader(PrimitiveField field)
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
        ColumnReaderFactory columnReaderFactory = new ColumnReaderFactory(UTC, new ParquetReaderOptions().withBatchColumnReaders(true).withBatchNestedColumnReaders(true));
        ColumnReader columnReader = columnReaderFactory.create(tesingField, newSimpleAggregatedMemoryContext());
        assertThat(columnReader).isInstanceOf(NestedColumnReader.class);
        return columnReader;
    }

    @Override
    @Test(dataProvider = "dictionaryReadersWithPageVersions", dataProviderClass = TestingColumnReader.class)
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

    @Test(dataProvider = "readersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testMultipleValuesPerPosition(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, true, 1, 0);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getPlainWriter();
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

    @Test(dataProvider = "readersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testDefinitionLevelNullable(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false, 0, 2);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getPlainWriter();
        T[] values1 = format.write(writer, new Integer[] {null, 1});
        // First value is ignored, the second is null and the third is an actual value
        DataPage page1 = createDataPage(version, PLAIN, writer, field, new int[0], new int[] {0, 1, 2});
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page1), null), Optional.empty());
        Block actual1 = readBlock(reader, 3, 2);

        format.assertBlock(values1, actual1);
    }

    @Test(dataProvider = "readersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testDefinitionLevelNullableWithNoNulls(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false, 0, 2);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getPlainWriter();
        T[] values1 = format.write(writer, new Integer[] {1});
        // First two values are ignored, and the third is an actual value
        DataPage page1 = createDataPage(version, PLAIN, writer, field, new int[0], new int[] {0, 0, 2});
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page1), null), Optional.empty());
        Block actual1 = readBlock(reader, 3, 1);

        format.assertBlock(values1, actual1);
    }

    @Test(dataProvider = "readersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testDefinitionLevelNonNull(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, true, 0, 2);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getPlainWriter();
        T[] values1 = format.write(writer, new Integer[] {1});
        // First two values are ignored and the third is an actual value
        DataPage page1 = createDataPage(version, PLAIN, writer, field, new int[0], new int[] {0, 1, 2});
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page1), null), Optional.empty());
        Block actual1 = readBlock(reader, 3, 1);

        format.assertBlock(values1, actual1);
    }

    @Test(dataProvider = "dictionaryReadersWithPageVersions", dataProviderClass = TestingColumnReader.class)
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

    @Test(dataProvider = "dictionaryReadersWithPageVersions", dataProviderClass = TestingColumnReader.class)
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

    @Test(dataProvider = "dictionaryReadersWithPageVersions", dataProviderClass = TestingColumnReader.class)
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

    @Test(dataProvider = "dictionaryReadersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testSingleRowPerTwoPagesNonNull(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, true, 1, 0);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getPlainWriter();
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

    @Test(dataProvider = "dictionaryReadersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testSingleRowPerTwoPagesNullable(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false, 1, 2);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getPlainWriter();
        T[] values1 = format.write(writer, new Integer[] {null});
        DataPage page1 = createDataPage(version, PLAIN, writer, field, new int[] {0, 1}, new int[] {0, 1});
        T[] values2 = format.resetAndWrite(writer, new Integer[] {1, null});
        DataPage page2 = createDataPage(version, PLAIN, writer, field, new int[] {1, 1, 0}, new int[] {2, 0, 1});
        T[] values3 = format.resetAndWrite(writer, new Integer[] {2, null});
        DataPage page3 = createDataPage(version, PLAIN, writer, field, new int[] {1, 0, 1}, new int[] {2, 0, 1});
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page1, page2, page3), null), Optional.empty());
        Block actual1 = readBlock(reader, 3, 5);

        format.assertBlock(values1, actual1, 0, 0, 1);
        format.assertBlock(values2, actual1, 0, 1, 2);
        format.assertBlock(values3, actual1, 0, 3, 2);
    }

    @Test(dataProvider = "dictionaryReadersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testSingleRowPerMultiplePagesNonNull(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, true, 1, 0);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getPlainWriter();
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

    @Test(dataProvider = "dictionaryReadersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testSingleRowPerMultiplePagesNullable(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false, 1, 2);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getPlainWriter();
        T[] values1 = format.write(writer, new Integer[] {null});
        DataPage page1 = createDataPage(version, PLAIN, writer, field, new int[] {0, 1}, new int[] {0, 1});
        T[] values2 = format.resetAndWrite(writer, new Integer[] {1, null});
        DataPage page2 = createDataPage(version, PLAIN, writer, field, new int[] {1, 1, 1}, new int[] {2, 0, 1});
        T[] values3 = format.resetAndWrite(writer, new Integer[] {2, null});
        DataPage page3 = createDataPage(version, PLAIN, writer, field, new int[] {1, 0, 1}, new int[] {2, 0, 1});
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page1, page2, page3), null), Optional.empty());
        Block actual1 = readBlock(reader, 2, 5);

        format.assertBlock(values1, actual1, 0, 0, 1);
        format.assertBlock(values2, actual1, 0, 1, 2);
        format.assertBlock(values3, actual1, 0, 3, 2);
    }

    @Test(dataProvider = "dictionaryReadersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testSingleRowPerMultiplePagesNonNullSeek(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, true, 1, 0);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getPlainWriter();
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

    @Test(dataProvider = "dictionaryReadersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testSingleRowPerMultiplePagesNullableSeek(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false, 1, 2);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getPlainWriter();
        format.write(writer, new Integer[] {null});
        DataPage page1 = createDataPage(version, PLAIN, writer, field, new int[] {0, 1}, new int[] {0, 1});
        format.resetAndWrite(writer, new Integer[] {1, null});
        DataPage page2 = createDataPage(version, PLAIN, writer, field, new int[] {1, 1, 1}, new int[] {2, 0, 1});
        T[] values3 = format.resetAndWrite(writer, new Integer[] {2, null});
        DataPage page3 = createDataPage(version, PLAIN, writer, field, new int[] {1, 0, 1}, new int[] {2, 0, 1});
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
        return createDataPage(version, ParquetEncoding.PLAIN, writer, field, repetition, new int[0]);
    }
}
