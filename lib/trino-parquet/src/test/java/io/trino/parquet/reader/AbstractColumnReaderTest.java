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
import com.google.common.primitives.Bytes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.DataPage;
import io.trino.parquet.DataPageV1;
import io.trino.parquet.DataPageV2;
import io.trino.parquet.DictionaryPage;
import io.trino.parquet.Page;
import io.trino.parquet.ParquetEncoding;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.reader.TestingColumnReader.ColumnReaderFormat;
import io.trino.parquet.reader.TestingColumnReader.DataPageVersion;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.AbstractVariableWidthType;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.bitpacking.DevNullValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Types.PrimitiveBuilder;
import org.testng.annotations.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetEncoding.BIT_PACKED;
import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.ParquetEncoding.PLAIN_DICTIONARY;
import static io.trino.parquet.ParquetEncoding.RLE;
import static io.trino.parquet.ParquetEncoding.RLE_DICTIONARY;
import static io.trino.parquet.ParquetTypeUtils.getParquetEncoding;
import static io.trino.parquet.reader.TestingColumnReader.DataPageVersion.V1;
import static io.trino.parquet.reader.TestingColumnReader.getDictionaryPage;
import static org.apache.parquet.bytes.BytesUtils.getWidthFromMaxInt;
import static org.apache.parquet.format.CompressionCodec.UNCOMPRESSED;
import static org.apache.parquet.internal.filter2.columnindex.TestingRowRanges.toRowRange;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.UTC;

public abstract class AbstractColumnReaderTest
{
    protected abstract ColumnReader createColumnReader(PrimitiveField field);

    @Test(dataProvider = "dictionaryReadersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testSingleValueDictionary(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, true);
        ColumnReader reader = createColumnReader(field);
        // Write data
        DictionaryValuesWriter dictionaryWriter = format.getDictionaryWriter();
        T[] values = format.write(dictionaryWriter, new Integer[] {1, 1});
        DataPage page = createDataPage(version, RLE_DICTIONARY, dictionaryWriter, 2);
        DictionaryPage dictionaryPage = getDictionaryPage(dictionaryWriter);
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page), dictionaryPage), Optional.empty());
        reader.prepareNextRead(2);
        Block actual = reader.readPrimitive().getBlock();
        assertThat(actual.mayHaveNull()).isFalse();
        if (field.getType() instanceof AbstractVariableWidthType) {
            assertThat(actual).isInstanceOf(DictionaryBlock.class);
        }
        format.assertBlock(values, actual);
    }

    @Test(dataProvider = "dictionaryReadersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testSingleValueDictionaryNullable(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false);
        ColumnReader reader = createColumnReader(field);
        // Write data
        DictionaryValuesWriter dictionaryWriter = format.getDictionaryWriter();
        T[] values = format.write(dictionaryWriter, new Integer[] {1, null});
        DataPage page = createNullableDataPage(version, RLE_DICTIONARY, dictionaryWriter, field, false, true);
        DictionaryPage dictionaryPage = getDictionaryPage(dictionaryWriter);
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page), dictionaryPage), Optional.empty());
        reader.prepareNextRead(2);
        Block actual = reader.readPrimitive().getBlock();
        assertThat(actual.mayHaveNull()).isTrue();
        if (field.getType() instanceof AbstractVariableWidthType) {
            assertThat(actual).isInstanceOf(DictionaryBlock.class);
        }
        format.assertBlock(values, actual);
    }

    @Test(dataProvider = "dictionaryReadersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testSingleValueDictionaryNullableWithNoNulls(DataPageVersion version, ColumnReaderFormat<T> format)
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
        reader.setPageReader(getPageReaderMock(List.of(page), dictionaryPage), Optional.empty());
        reader.prepareNextRead(2);
        Block actual = reader.readPrimitive().getBlock();
        if (field.getType() instanceof AbstractVariableWidthType) {
            assertThat(actual).isInstanceOf(DictionaryBlock.class);
            assertThat(actual.mayHaveNull()).isTrue();
        }
        format.assertBlock(values, actual);
    }

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
        if (field.getType() instanceof AbstractVariableWidthType) {
            assertThat(actual).isInstanceOf(DictionaryBlock.class);
            assertThat(actual.mayHaveNull()).isFalse();
        }
        format.assertBlock(values, actual);
    }

    @Test(dataProvider = "dictionaryReadersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testSingleValueDictionaryNullableWithOnlyNulls(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false);
        ColumnReader reader = createColumnReader(field);
        // Write data
        DictionaryValuesWriter dictionaryWriter = format.getDictionaryWriter();
        // Write dummy value. Without it dictionary would end up null.
        format.write(dictionaryWriter, new Integer[] {42});
        T[] values = format.resetAndWrite(dictionaryWriter, new Integer[] {null, null});
        DataPage page = createNullableDataPage(version, RLE_DICTIONARY, dictionaryWriter, field, true, true);
        DictionaryPage dictionaryPage = getDictionaryPage(dictionaryWriter);
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page), dictionaryPage), Optional.empty());
        reader.prepareNextRead(2);
        Block actual = reader.readPrimitive().getBlock();
        assertThat(actual).isInstanceOf(RunLengthEncodedBlock.class);
        assertThat(actual.mayHaveNull()).isTrue();
        format.assertBlock(values, actual);
    }

    @Test(dataProvider = "dictionaryReadersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testDictionariesSharedBetweenPages(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        PrimitiveField field = createField(format, true);
        ColumnReader reader = createColumnReader(field);

        DictionaryValuesWriter dictionaryWriter = format.getDictionaryWriter();
        T[] values1 = format.write(dictionaryWriter, new Integer[] {42, 43});
        DataPage page1 = createDataPage(version, RLE_DICTIONARY, dictionaryWriter, 2);
        T[] values2 = format.resetAndWrite(dictionaryWriter, new Integer[] {42, 43});
        DataPage page2 = createDataPage(version, RLE_DICTIONARY, dictionaryWriter, 2);
        DictionaryPage dictionaryPage = getDictionaryPage(dictionaryWriter);

        reader.setPageReader(getPageReaderMock(List.of(page1, page2), dictionaryPage), Optional.empty());
        reader.prepareNextRead(2);
        Block block1 = reader.readPrimitive().getBlock();
        reader.prepareNextRead(2);
        Block block2 = reader.readPrimitive().getBlock();

        if (field.getType() instanceof AbstractVariableWidthType) {
            assertThat(block1).isInstanceOf(DictionaryBlock.class);
            assertThat(block2).isInstanceOf(DictionaryBlock.class);

            assertThat(((DictionaryBlock) block1).getDictionary())
                    .isEqualTo(((DictionaryBlock) block2).getDictionary());
        }
        format.assertBlock(values1, block1);
        format.assertBlock(values2, block2);
    }

    @Test(dataProvider = "readersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testReadNoNull(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, true);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getPlainWriter();
        T[] values1 = format.write(writer, new Integer[] {1});
        DataPage page1 = createDataPage(version, PLAIN, writer, 1);
        T[] values2 = format.resetAndWrite(writer, new Integer[] {2, 3});
        DataPage page2 = createDataPage(version, PLAIN, writer, 2);
        T[] values3 = format.resetAndWrite(writer, new Integer[] {4});
        DataPage page3 = createDataPage(version, PLAIN, writer, 1);
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page1, page2, page3), null), Optional.empty());
        Block actual1 = readBlock(reader, 1); // Parquet/Trino page size the same
        Block actual2 = readBlock(reader, 1); // Parquet page bigger than Trino
        Block actual3 = readBlock(reader, 2); // Parquet page smaller than Trino

        format.assertBlock(values1, actual1);
        format.assertBlock(values2, actual2, 0, 0, 1);
        format.assertBlock(values2, actual3, 1, 0, 1);
        format.assertBlock(values3, actual3, 0, 1, 1);
        assertThat(actual1.mayHaveNull()).isFalse();
        assertThat(actual2.mayHaveNull()).isFalse();
        assertThat(actual3.mayHaveNull()).isFalse();
    }

    @Test(dataProvider = "dictionaryReadersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testSingleValueDictionaryAndNonDictionaryInASingleChunk(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, true);
        ColumnReader reader = createColumnReader(field);
        // Write data
        DictionaryValuesWriter dictionaryWriter = format.getDictionaryWriter();
        ValuesWriter writer = format.getPlainWriter();
        T[] values1 = format.write(dictionaryWriter, new Integer[] {1, 1});
        DataPage page1 = createDataPage(version, RLE_DICTIONARY, dictionaryWriter, 2);
        T[] values2 = format.write(writer, new Integer[] {2});
        DataPage page2 = createDataPage(version, PLAIN, writer, 1);
        DictionaryPage dictionaryPage = getDictionaryPage(dictionaryWriter);
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page1, page2), dictionaryPage), Optional.empty());
        reader.prepareNextRead(3);
        Block actual = reader.readPrimitive().getBlock();
        assertThat(actual).isNotInstanceOf(DictionaryBlock.class);
        assertThat(actual.mayHaveNull()).isFalse();
        format.assertBlock(values1, actual, 0, 0, 2);
        format.assertBlock(values2, actual, 0, 2, 1);
    }

    @Test(dataProvider = "readersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testReadOnlyNulls(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getPlainWriter();
        T[] values1 = format.write(writer, new Integer[] {null});
        DataPage page1 = createNullableDataPage(version, PLAIN, writer, field, true);
        T[] values2 = format.write(writer, new Integer[] {null, null});
        DataPage page2 = createNullableDataPage(version, PLAIN, writer, field, true, true);
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page1, page2), null), Optional.empty());
        // Deliberate mismatch between Trino/Parquet page sizes
        Block actual1 = readBlock(reader, 2);
        Block actual2 = readBlock(reader, 1);

        assertThat(actual1.mayHaveNull()).isTrue();
        assertThat(actual2.mayHaveNull()).isTrue();

        format.assertBlock(values1, actual1, 0, 0, 1);
        format.assertBlock(values2, actual1, 0, 1, 1);
        format.assertBlock(values2, actual2, 1, 0, 1);
    }

    @Test(dataProvider = "readersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testReadNullable(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getPlainWriter();
        T[] values1 = format.write(writer, new Integer[] {null, 1});
        DataPage page1 = createNullableDataPage(version, PLAIN, writer, field, true, false);
        T[] values2 = format.resetAndWrite(writer, new Integer[] {null, 2, null});
        DataPage page2 = createNullableDataPage(version, PLAIN, writer, field, true, false, true);
        T[] values3 = format.resetAndWrite(writer, new Integer[] {3, null, 4});
        DataPage page3 = createNullableDataPage(version, PLAIN, writer, field, false, true, false);
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page1, page2, page3), null), Optional.empty());
        // Deliberate mismatch between Trino/Parquet page sizes
        Block actual1 = readBlock(reader, 2);
        Block actual2 = readBlock(reader, 2);
        Block actual3 = readBlock(reader, 4);

        assertThat(actual1.mayHaveNull()).isTrue();
        assertThat(actual2.mayHaveNull()).isTrue();
        assertThat(actual3.mayHaveNull()).isTrue();

        format.assertBlock(values1, actual1);
        format.assertBlock(values2, actual2, 0, 0, 2);
        format.assertBlock(values2, actual3, 2, 0, 1);
        format.assertBlock(values3, actual3, 0, 1, 3);
    }

    @Test(dataProvider = "dictionaryReadersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testReadNullableDictionary(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false);
        ColumnReader reader = createColumnReader(field);
        // Write data
        DictionaryValuesWriter dictionaryWriter = format.getDictionaryWriter();
        T[] values1 = format.write(dictionaryWriter, new Integer[] {0, 1, null});
        DataPage page1 = createNullableDataPage(version, RLE_DICTIONARY, dictionaryWriter, field, false, false, true);
        T[] values2 = format.resetAndWrite(dictionaryWriter, new Integer[] {2, 3, null});
        DataPage page2 = createNullableDataPage(version, RLE_DICTIONARY, dictionaryWriter, field, false, false, true);
        T[] values3 = format.resetAndWrite(dictionaryWriter, new Integer[] {3, null, 4});
        DataPage page3 = createNullableDataPage(version, RLE_DICTIONARY, dictionaryWriter, field, false, true, false);
        DictionaryPage dictionaryPage = getDictionaryPage(dictionaryWriter);
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page1, page2, page3), dictionaryPage), Optional.empty());
        // Deliberate mismatch between Trino/Parquet page sizes
        Block actual1 = readBlock(reader, 2);
        Block actual2 = readBlock(reader, 3);
        Block actual3 = readBlock(reader, 4);

        if (field.getType() instanceof AbstractVariableWidthType) {
            assertThat(actual1).isInstanceOf(DictionaryBlock.class);
            assertThat(actual2).isInstanceOf(DictionaryBlock.class);
            assertThat(actual3).isInstanceOf(DictionaryBlock.class);

            assertThat(((DictionaryBlock) actual1).getDictionary().mayHaveNull()).isTrue();
            assertThat(((DictionaryBlock) actual1).getDictionary())
                    .isEqualTo(((DictionaryBlock) actual2).getDictionary());

            assertThat(((DictionaryBlock) actual2).getDictionary())
                    .isEqualTo(((DictionaryBlock) actual3).getDictionary());
        }

        format.assertBlock(values1, actual1, 0, 0, 2);
        format.assertBlock(values1, actual2, 2, 0, 1);
        format.assertBlock(values2, actual2, 0, 1, 2);
        format.assertBlock(values2, actual3, 2, 0, 1);
        format.assertBlock(values3, actual3, 0, 1, 3);
    }

    @Test(dataProvider = "readersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testReadNullableWithNoNulls(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getPlainWriter();
        T[] values1 = format.write(writer, new Integer[] {1, 2});
        DataPage page1 = createNullableDataPage(version, PLAIN, writer, field, false, false);
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page1), null), Optional.empty());
        // Deliberate mismatch between Trino/Parquet page sizes
        Block actual1 = readBlock(reader, 2);

        assertThat(actual1.mayHaveNull()).isFalse();
        format.assertBlock(values1, actual1);
    }

    @Test(dataProvider = "readersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testReadNullableWithNoNullsUsingColumnStats(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getPlainWriter();
        T[] values1 = format.write(writer, new Integer[] {1, 2});
        DataPage page1 = createNullableDataPage(version, PLAIN, writer, field, false, false);
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page1), null, true), Optional.empty());
        // Deliberate mismatch between Trino/Parquet page sizes
        Block actual1 = readBlock(reader, 2);
        assertThat(actual1.mayHaveNull()).isFalse();
        format.assertBlock(values1, actual1);
    }

    @Test(dataProvider = "dictionaryReadersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testMixedDictionaryAndOrdinary(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, true);
        ColumnReader reader = createColumnReader(field);
        // Write data
        DictionaryValuesWriter dictionaryWriter = format.getDictionaryWriter();
        T[] values1 = format.write(dictionaryWriter, new Integer[] {1, 2, 3});
        DataPage page1 = createDataPage(version, RLE_DICTIONARY, dictionaryWriter, 3);
        ValuesWriter writer = format.getPlainWriter();
        T[] values2 = format.resetAndWrite(writer, new Integer[] {4, 5, 6});
        DataPage page2 = createDataPage(version, PLAIN, writer, 3);
        DictionaryPage dictionaryPage = getDictionaryPage(dictionaryWriter);
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page1, page2), dictionaryPage), Optional.empty());
        // Deliberate mismatch between Trino/Parquet page sizes
        Block actual1 = readBlock(reader, 2); // Only dictionary
        Block actual2 = readBlock(reader, 2); // Mixed
        Block actual3 = readBlock(reader, 2); // Only non-dictionary

        // When there is mix of dictionary and non-dictionary pages, we don't produce DictionaryBlock
        assertThat(actual1).isNotInstanceOf(DictionaryBlock.class);
        assertThat(actual2).isNotInstanceOf(DictionaryBlock.class);
        assertThat(actual3).isNotInstanceOf(DictionaryBlock.class);

        format.assertBlock(values1, actual1, 0, 0, 2);
        format.assertBlock(values1, actual2, 2, 0, 1);
        format.assertBlock(values2, actual2, 0, 1, 1);
        format.assertBlock(values2, actual3, 1, 0, 2);
    }

    @Test(dataProvider = "dictionaryReadersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testLargeDictionaryWithSmallValuesCount(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, true);
        ColumnReader reader = createColumnReader(field);
        // Write data
        DictionaryValuesWriter dictionaryWriter = format.getDictionaryWriter();
        format.write(dictionaryWriter, new Integer[] {1, 2, 3, 4, 5, 6, 7});
        createDataPage(version, RLE_DICTIONARY, dictionaryWriter, 3);
        T[] values2 = format.resetAndWrite(dictionaryWriter, new Integer[] {4, 5, 6});
        DataPage dataPage = createDataPage(version, RLE_DICTIONARY, dictionaryWriter, 3, OptionalLong.of(0));
        DictionaryPage dictionaryPage = getDictionaryPage(dictionaryWriter);
        assertThat(dictionaryPage.getDictionarySize()).isGreaterThan(dataPage.getValueCount());
        // Read and assert
        PageReader pageReader = getPageReaderMock(List.of(dataPage), dictionaryPage);
        assertThat(pageReader.hasOnlyDictionaryEncodedPages()).isTrue();
        reader.setPageReader(pageReader, Optional.of(new FilteredRowRanges(toRowRange(3))));
        // Deliberate mismatch between Trino/Parquet page sizes
        Block actual = readBlock(reader, 3);

        // When we're reading fewer values than the dictionary size, we don't produce DictionaryBlock
        assertThat(actual).isNotInstanceOf(DictionaryBlock.class);

        format.assertBlock(values2, actual);
    }

    @Test(dataProvider = "readersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testOnlyNullParquetPage(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, false);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getPlainWriter();
        T[] values1 = format.write(writer, new Integer[] {1});
        DataPage page1 = createNullableDataPage(version, PLAIN, writer, field, false);
        T[] values2 = format.resetAndWrite(writer, new Integer[] {null});
        DataPage page2 = createNullableDataPage(version, PLAIN, writer, field, true);
        T[] values3 = format.resetAndWrite(writer, new Integer[] {2});
        DataPage page3 = createNullableDataPage(version, PLAIN, writer, field, false);
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page1, page2, page3), null), Optional.empty());
        // Deliberate mismatch between Trino/Parquet page sizes
        Block actual = readBlock(reader, 3);
        assertThat(actual.mayHaveNull()).isTrue();

        format.assertBlock(values1, actual, 0, 0, 1);
        format.assertBlock(values2, actual, 0, 1, 1);
        format.assertBlock(values3, actual, 0, 2, 1);
    }

    @Test(dataProvider = "dictionaryReadersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testSkip(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, true);
        ColumnReader reader = createColumnReader(field);
        // Write data
        ValuesWriter writer = format.getPlainWriter();
        DictionaryValuesWriter dictionaryWriter = format.getDictionaryWriter();
        T[] values1 = format.write(writer, new Integer[] {1, 2, 3});
        DataPage page1 = createDataPage(version, PLAIN, writer, 3);
        T[] values2 = format.resetAndWrite(dictionaryWriter, new Integer[] {4, 5, 6});
        DataPage page2 = createDataPage(version, RLE_DICTIONARY, dictionaryWriter, 3);
        DictionaryPage dictionaryPage = getDictionaryPage(dictionaryWriter);
        // Read and assert
        reader.setPageReader(getPageReaderMock(List.of(page1, page2), dictionaryPage), Optional.empty());
        reader.prepareNextRead(1); // skip
        Block actual = readBlock(reader, 2);
        reader.prepareNextRead(1); // skip
        Block actual2 = readBlock(reader, 2);

        format.assertBlock(values1, actual, 1, 0, 2);
        format.assertBlock(values2, actual2, 1, 0, 2);
    }

    @Test(dataProvider = "dictionaryReadersWithPageVersions", dataProviderClass = TestingColumnReader.class)
    public <T> void testMemoryUsage(DataPageVersion version, ColumnReaderFormat<T> format)
            throws IOException
    {
        // Create reader
        PrimitiveField field = createField(format, true);
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        ColumnReader reader = ColumnReaderFactory.create(field, UTC, memoryContext, new ParquetReaderOptions().withBatchColumnReaders(true));
        // Write data
        DictionaryValuesWriter dictionaryWriter = format.getDictionaryWriter();
        format.write(dictionaryWriter, new Integer[] {1, 2, 3});
        DataPage page1 = createDataPage(version, RLE_DICTIONARY, dictionaryWriter, 3);
        format.resetAndWrite(dictionaryWriter, new Integer[] {2, 4, 5, 3, 7});
        DataPage page2 = createDataPage(version, RLE_DICTIONARY, dictionaryWriter, 5);
        DictionaryPage dictionaryPage = getDictionaryPage(dictionaryWriter);
        // Read and assert
        assertThat(memoryContext.getBytes()).isEqualTo(0);
        reader.setPageReader(getPageReaderMock(List.of(page1, page2), dictionaryPage), Optional.empty());
        assertThat(memoryContext.getBytes()).isEqualTo(0);
        readBlock(reader, 3);
        long memoryUsage = memoryContext.getBytes();
        assertThat(memoryUsage).isGreaterThan(0);
        readBlock(reader, 4);
        // For uncompressed pages, only the dictionary needs to be accounted for
        // That's why memory usage does not change on reading the 2nd page
        assertThat(memoryContext.getBytes()).isEqualTo(memoryUsage);
    }

    private static Block readBlock(ColumnReader reader, int valueCount)
    {
        return readBlock(reader, valueCount, valueCount);
    }

    protected static Block readBlock(ColumnReader reader, int rowCount, int valueCount)
    {
        reader.prepareNextRead(rowCount);
        Block block = reader.readPrimitive().getBlock();
        assertThat(block.getPositionCount()).isEqualTo(valueCount);
        return block;
    }

    protected static PrimitiveField createField(ColumnReaderFormat<?> format, boolean required)
    {
        return createField(format, required, 0, required ? 0 : 1);
    }

    protected static PrimitiveField createField(
            ColumnReaderFormat<?> format,
            boolean required,
            int maxRep,
            int maxDef)
    {
        PrimitiveBuilder<PrimitiveType> builder;
        if (required) {
            builder = Types.required(format.getTypeName());
        }
        else {
            builder = Types.optional(format.getTypeName());
        }
        builder = builder.length(format.getTypeLengthInBytes());
        if (format.getLogicalTypeAnnotation() != null) {
            builder = builder.as(format.getLogicalTypeAnnotation());
        }
        PrimitiveType primitiveType = builder.named("dummy");
        return new PrimitiveField(
                format.getTrinoType(),
                required,
                new ColumnDescriptor(
                        maxRep == 0 ? new String[] {"dummy"} : new String[] {"dummy", "dummy_nested"},
                        primitiveType,
                        maxRep,
                        maxDef),
                0);
    }

    protected DataPage createNullableDataPage(DataPageVersion version, ParquetEncoding encoding, ValuesWriter writer, PrimitiveField field, boolean... isNull)
            throws IOException
    {
        int[] definitionLevels = new int[isNull.length];
        if (!field.isRequired()) {
            for (int i = 0; i < isNull.length; i++) {
                definitionLevels[i] = isNull[i] ? 0 : 1;
            }
        }
        return createDataPage(version, encoding, writer, field, new int[0], definitionLevels);
    }

    protected static DataPage createDataPage(
            DataPageVersion version,
            ParquetEncoding encoding,
            ValuesWriter writer,
            PrimitiveField field,
            int[] repetition,
            int[] definition)
            throws IOException
    {
        int valueCount = Math.max(repetition.length, definition.length);
        ValuesWriter repetitionWriter = getLevelsWriter(field.getRepetitionLevel(), valueCount);
        ValuesWriter definitionWriter = getLevelsWriter(field.getDefinitionLevel(), valueCount);

        int maxDef = field.getDefinitionLevel();
        int nullCount = 0;
        for (int level : definition) {
            definitionWriter.writeInteger(level);
            if (field.isRequired()) {
                nullCount += level == maxDef ? 1 : 0;
            }
            else {
                nullCount += level == maxDef - 1 ? 1 : 0;
            }
        }
        for (int level : repetition) {
            repetitionWriter.writeInteger(level);
        }
        byte[] repetitionBytes = repetitionWriter.getBytes().toByteArray();
        byte[] definitionBytes = definitionWriter.getBytes().toByteArray();
        byte[] valueBytes = writer.getBytes().toByteArray();

        if (version == V1) {
            Slice slice = Slices.wrappedBuffer(Bytes.concat(repetitionBytes, definitionBytes, valueBytes));
            return new DataPageV1(
                    slice,
                    valueCount,
                    slice.length(),
                    OptionalLong.empty(),
                    getParquetEncoding(repetitionWriter.getEncoding()),
                    getParquetEncoding(definitionWriter.getEncoding()),
                    encoding);
        }
        return new DataPageV2(
                valueCount,
                nullCount,
                valueCount,
                repetitionBytes.length == 0 ? EMPTY_SLICE : Slices.wrappedBuffer(repetitionBytes, Integer.BYTES, repetitionBytes.length - Integer.BYTES),
                definitionBytes.length == 0 ? EMPTY_SLICE : Slices.wrappedBuffer(definitionBytes, Integer.BYTES, definitionBytes.length - Integer.BYTES),
                encoding,
                Slices.wrappedBuffer(valueBytes),
                definitionBytes.length + repetitionBytes.length + valueBytes.length,
                OptionalLong.empty(),
                null,
                false);
    }

    protected static PageReader getPageReaderMock(List<DataPage> dataPages, @Nullable DictionaryPage dictionaryPage)
    {
        return getPageReaderMock(dataPages, dictionaryPage, false);
    }

    protected static PageReader getPageReaderMock(List<DataPage> dataPages, @Nullable DictionaryPage dictionaryPage, boolean hasNoNulls)
    {
        ImmutableList.Builder<Page> pagesBuilder = ImmutableList.builder();
        if (dictionaryPage != null) {
            pagesBuilder.add(dictionaryPage);
        }
        return new PageReader(
                UNCOMPRESSED,
                pagesBuilder.addAll(dataPages).build().iterator(),
                dataPages.stream()
                        .map(page -> {
                            if (page instanceof DataPageV1 pageV1) {
                                return pageV1.getValueEncoding();
                            }
                            return ((DataPageV2) page).getDataEncoding();
                        })
                        .allMatch(encoding -> encoding == PLAIN_DICTIONARY || encoding == RLE_DICTIONARY),
                hasNoNulls);
    }

    private DataPage createDataPage(DataPageVersion version, ParquetEncoding encoding, ValuesWriter writer, int valueCount)
            throws IOException
    {
        return createDataPage(version, encoding, writer, valueCount, OptionalLong.empty());
    }

    private DataPage createDataPage(DataPageVersion version, ParquetEncoding encoding, ValuesWriter writer, int valueCount, OptionalLong firstRowIndex)
            throws IOException
    {
        Slice slice = Slices.wrappedBuffer(writer.getBytes().toByteArray());
        if (version == V1) {
            return new DataPageV1(slice, valueCount, slice.length(), firstRowIndex, RLE, BIT_PACKED, encoding);
        }
        return new DataPageV2(
                valueCount,
                0,
                valueCount,
                EMPTY_SLICE,
                EMPTY_SLICE,
                encoding,
                slice,
                slice.length(),
                firstRowIndex,
                null,
                false);
    }

    private static ValuesWriter getLevelsWriter(int maxLevel, int valueCount)
    {
        if (maxLevel == 0) {
            return new DevNullValuesWriter();
        }
        return new RunLengthBitPackingHybridValuesWriter(
                getWidthFromMaxInt(maxLevel),
                valueCount + 1,
                valueCount + 1,
                HeapByteBufferAllocator.getInstance());
    }
}
