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
import com.google.common.collect.ImmutableSet;
import io.airlift.compress.snappy.SnappyCompressor;
import io.airlift.compress.snappy.SnappyRawCompressor;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.DataPage;
import io.trino.parquet.DataPageV1;
import io.trino.parquet.DataPageV2;
import io.trino.parquet.DictionaryPage;
import io.trino.parquet.ParquetEncoding;
import io.trino.parquet.ParquetTypeUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.parquet.reader.TestPageReader.DataPageType.V1;
import static io.trino.parquet.reader.TestPageReader.DataPageType.V2;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.format.CompressionCodec.SNAPPY;
import static org.apache.parquet.format.CompressionCodec.UNCOMPRESSED;
import static org.apache.parquet.format.PageType.DATA_PAGE_V2;
import static org.apache.parquet.format.PageType.DICTIONARY_PAGE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestPageReader
{
    private static final byte[] DATA_PAGE = new byte[] {1, 2, 3};

    @Test(dataProvider = "pageParameters")
    public void singlePage(CompressionCodec compressionCodec, DataPageType dataPageType)
            throws Exception
    {
        int valueCount = 10;
        byte[] compressedDataPage = dataPageType.compress(compressionCodec, DATA_PAGE);

        PageHeader pageHeader = new PageHeader(dataPageType.pageType(), DATA_PAGE.length, compressedDataPage.length);
        dataPageType.setDataPageHeader(pageHeader, valueCount);

        ByteArrayOutputStream out = new ByteArrayOutputStream(20);
        Util.writePageHeader(pageHeader, out);
        int headerSize = out.size();
        out.write(compressedDataPage);
        byte[] bytes = out.toByteArray();

        // single slice
        assertSinglePage(compressionCodec, valueCount, pageHeader, compressedDataPage, ImmutableList.of(
                Slices.wrappedBuffer(bytes)));

        // pageHeader split across two slices
        assertSinglePage(compressionCodec, valueCount, pageHeader, compressedDataPage, ImmutableList.of(
                Slices.wrappedBuffer(Arrays.copyOf(bytes, 8)),
                Slices.wrappedBuffer(Arrays.copyOfRange(bytes, 8, bytes.length))));

        // pageHeader split across many slices
        int secondHeaderChunkOffset = 5;
        int thirdHeaderChunkOffset = 11;
        assertSinglePage(compressionCodec, valueCount, pageHeader, compressedDataPage, ImmutableList.of(
                Slices.wrappedBuffer(Arrays.copyOf(bytes, secondHeaderChunkOffset)),
                Slices.wrappedBuffer(Arrays.copyOfRange(bytes, secondHeaderChunkOffset, thirdHeaderChunkOffset)),
                Slices.wrappedBuffer(Arrays.copyOfRange(bytes, thirdHeaderChunkOffset, bytes.length))));

        // page data split across two slices
        assertSinglePage(compressionCodec, valueCount, pageHeader, compressedDataPage, ImmutableList.of(
                Slices.wrappedBuffer(Arrays.copyOf(bytes, headerSize + 1)),
                Slices.wrappedBuffer(Arrays.copyOfRange(bytes, headerSize + 1, bytes.length))));

        // page data split across many slices
        assertSinglePage(compressionCodec, valueCount, pageHeader, compressedDataPage, ImmutableList.of(
                Slices.wrappedBuffer(Arrays.copyOf(bytes, headerSize + 1)),
                Slices.wrappedBuffer(Arrays.copyOfRange(bytes, headerSize + 1, headerSize + 2)),
                Slices.wrappedBuffer(Arrays.copyOfRange(bytes, headerSize + 2, bytes.length))));
    }

    @Test(dataProvider = "pageParameters")
    public void manyPages(CompressionCodec compressionCodec, DataPageType dataPageType)
            throws Exception
    {
        int totalValueCount = 30;
        byte[] compressedDataPage = dataPageType.compress(compressionCodec, DATA_PAGE);

        PageHeader pageHeader = new PageHeader(dataPageType.pageType(), DATA_PAGE.length, compressedDataPage.length);
        dataPageType.setDataPageHeader(pageHeader, 10);

        ByteArrayOutputStream out = new ByteArrayOutputStream(100);
        Util.writePageHeader(pageHeader, out);
        int headerSize = out.size();
        out.write(compressedDataPage);
        // second page
        Util.writePageHeader(pageHeader, out);
        out.write(compressedDataPage);
        // third page
        Util.writePageHeader(pageHeader, out);
        out.write(compressedDataPage);
        byte[] bytes = out.toByteArray();

        // single slice
        assertPages(compressionCodec, totalValueCount, 3, pageHeader, compressedDataPage, ImmutableList.of(
                Slices.wrappedBuffer(bytes)));

        // each page in its own split
        int pageSize = headerSize + compressedDataPage.length;
        assertPages(compressionCodec, totalValueCount, 3, pageHeader, compressedDataPage, ImmutableList.of(
                Slices.wrappedBuffer(Arrays.copyOf(bytes, pageSize)),
                Slices.wrappedBuffer(Arrays.copyOfRange(bytes, pageSize, pageSize * 2)),
                Slices.wrappedBuffer(Arrays.copyOfRange(bytes, pageSize * 2, bytes.length))));

        // page start in the middle of the slice
        assertPages(compressionCodec, totalValueCount, 3, pageHeader, compressedDataPage, ImmutableList.of(
                Slices.wrappedBuffer(Arrays.copyOf(bytes, pageSize - 2)),
                Slices.wrappedBuffer(Arrays.copyOfRange(bytes, pageSize - 2, pageSize * 2)),
                Slices.wrappedBuffer(Arrays.copyOfRange(bytes, pageSize * 2, bytes.length))));
    }

    @Test(dataProvider = "pageParameters")
    public void dictionaryPage(CompressionCodec compressionCodec, DataPageType dataPageType)
            throws Exception
    {
        byte[] dictionaryPage = {4};
        byte[] compressedDictionaryPage = TestPageReader.compress(compressionCodec, dictionaryPage, 0, dictionaryPage.length);
        PageHeader dictionaryPageHeader = new PageHeader(DICTIONARY_PAGE, dictionaryPage.length, compressedDictionaryPage.length);
        dictionaryPageHeader.setDictionary_page_header(new DictionaryPageHeader(3, Encoding.PLAIN));
        int totalValueCount = 30;
        byte[] compressedDataPage = dataPageType.compress(compressionCodec, DATA_PAGE);

        PageHeader pageHeader = new PageHeader(dataPageType.pageType(), DATA_PAGE.length, compressedDataPage.length);
        dataPageType.setDataPageHeader(pageHeader, 10);

        ByteArrayOutputStream out = new ByteArrayOutputStream(100);
        Util.writePageHeader(dictionaryPageHeader, out);
        int dictionaryHeaderSize = out.size();
        out.write(compressedDictionaryPage);
        int dictionaryPageSize = out.size();

        Util.writePageHeader(pageHeader, out);
        out.write(compressedDataPage);
        // second page
        Util.writePageHeader(pageHeader, out);
        out.write(compressedDataPage);
        // third page
        Util.writePageHeader(pageHeader, out);
        out.write(compressedDataPage);
        byte[] bytes = out.toByteArray();

        PageReader pageReader = createPageReader(totalValueCount, compressionCodec, true, ImmutableList.of(Slices.wrappedBuffer(bytes)));
        DictionaryPage uncompressedDictionaryPage = pageReader.readDictionaryPage();
        assertThat(uncompressedDictionaryPage.getDictionarySize()).isEqualTo(dictionaryPageHeader.getDictionary_page_header().getNum_values());
        assertEncodingEquals(uncompressedDictionaryPage.getEncoding(), dictionaryPageHeader.getDictionary_page_header().getEncoding());
        assertThat(uncompressedDictionaryPage.getSlice()).isEqualTo(Slices.wrappedBuffer(dictionaryPage));

        // single slice
        assertPages(compressionCodec, totalValueCount, 3, pageHeader, compressedDataPage, true, ImmutableList.of(Slices.wrappedBuffer(bytes)));

        // only dictionary
        pageReader = createPageReader(0, compressionCodec, true, ImmutableList.of(Slices.wrappedBuffer(Arrays.copyOf(bytes, dictionaryPageSize))));
        assertThatThrownBy(pageReader::readDictionaryPage)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("No more data left to read");

        // multiple slices dictionary
        assertPages(compressionCodec, totalValueCount, 3, pageHeader, compressedDataPage, true, ImmutableList.of(
                Slices.wrappedBuffer(Arrays.copyOfRange(bytes, 0, dictionaryHeaderSize - 1)),
                Slices.wrappedBuffer(Arrays.copyOfRange(bytes, dictionaryHeaderSize - 1, dictionaryPageSize - 1)),
                Slices.wrappedBuffer(Arrays.copyOfRange(bytes, dictionaryPageSize - 1, dictionaryPageSize + 1)),
                Slices.wrappedBuffer(Arrays.copyOfRange(bytes, dictionaryPageSize + 1, bytes.length))));
    }

    @Test
    public void dictionaryPageNotFirst()
            throws Exception
    {
        byte[] dictionaryPage = {4};
        CompressionCodec compressionCodec = UNCOMPRESSED;
        byte[] compressedDictionaryPage = TestPageReader.compress(compressionCodec, dictionaryPage, 0, dictionaryPage.length);
        PageHeader dictionaryPageHeader = new PageHeader(DICTIONARY_PAGE, dictionaryPage.length, compressedDictionaryPage.length);
        dictionaryPageHeader.setDictionary_page_header(new DictionaryPageHeader(3, Encoding.PLAIN));
        DataPageType dataPageType = V2;
        byte[] compressedDataPage = DATA_PAGE;

        PageHeader pageHeader = new PageHeader(dataPageType.pageType(), DATA_PAGE.length, compressedDataPage.length);
        int valueCount = 10;
        dataPageType.setDataPageHeader(pageHeader, valueCount);

        ByteArrayOutputStream out = new ByteArrayOutputStream(100);
        Util.writePageHeader(pageHeader, out);
        out.write(compressedDataPage);

        Util.writePageHeader(dictionaryPageHeader, out);
        out.write(compressedDictionaryPage);
        // write another page so that we have something to read after the first
        Util.writePageHeader(pageHeader, out);
        out.write(compressedDataPage);
        byte[] bytes = out.toByteArray();

        int totalValueCount = valueCount * 2;

        // There is a dictionary, but it's there as the second page
        PageReader pageReader = createPageReader(totalValueCount, compressionCodec, true, ImmutableList.of(Slices.wrappedBuffer(bytes)));
        assertThat(pageReader.readDictionaryPage()).isNull();
        assertThat(pageReader.readPage()).isNotNull();
        assertThatThrownBy(pageReader::readPage)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("has a dictionary page after the first position");
    }

    @Test
    public void unusedDictionaryPage()
            throws Exception
    {
        // A parquet file produced by Impala was found to have an empty dictionary
        // which is not used in the encoding of data pages in the column
        CompressionCodec compressionCodec = UNCOMPRESSED;
        byte[] compressedDictionaryPage = TestPageReader.compress(compressionCodec, new byte[0], 0, 0);
        PageHeader dictionaryPageHeader = new PageHeader(DICTIONARY_PAGE, 0, compressedDictionaryPage.length);
        dictionaryPageHeader.setDictionary_page_header(new DictionaryPageHeader(0, Encoding.PLAIN));
        ByteArrayOutputStream out = new ByteArrayOutputStream(100);
        Util.writePageHeader(dictionaryPageHeader, out);
        out.write(compressedDictionaryPage);

        DataPageType dataPageType = V2;
        byte[] compressedDataPage = DATA_PAGE;

        PageHeader pageHeader = new PageHeader(dataPageType.pageType(), DATA_PAGE.length, compressedDataPage.length);
        int valueCount = 10;
        dataPageType.setDataPageHeader(pageHeader, valueCount);

        Util.writePageHeader(pageHeader, out);
        out.write(compressedDataPage);
        byte[] bytes = out.toByteArray();

        // There is a dictionary, but it's there as the second page
        PageReader pageReader = createPageReader(valueCount, compressionCodec, true, ImmutableList.of(Slices.wrappedBuffer(bytes)));
        assertThat(pageReader.readDictionaryPage()).isNotNull();
        assertThat(pageReader.readPage()).isNotNull();
        assertThat(pageReader.readPage()).isNull();
    }

    private static void assertSinglePage(CompressionCodec compressionCodec, int valueCount, PageHeader pageHeader, byte[] compressedDataPage, List<Slice> slices)
            throws IOException
    {
        assertPages(compressionCodec, valueCount, 1, pageHeader, compressedDataPage, slices);
    }

    private static void assertPages(CompressionCodec compressionCodec, int valueCount, int pageCount, PageHeader pageHeader, byte[] compressedDataPage, List<Slice> slices)
            throws IOException
    {
        assertPages(compressionCodec, valueCount, pageCount, pageHeader, compressedDataPage, false, slices);
    }

    private static void assertPages(
            CompressionCodec compressionCodec,
            int valueCount,
            int pageCount,
            PageHeader pageHeader,
            byte[] compressedDataPage,
            boolean hasDictionary,
            List<Slice> slices)
            throws IOException
    {
        PageReader pageReader = createPageReader(valueCount, compressionCodec, hasDictionary, slices);
        DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
        assertEquals(dictionaryPage != null, hasDictionary);

        for (int i = 0; i < pageCount; i++) {
            assertTrue(pageReader.hasNext());
            DataPage decompressedPage = pageReader.readPage();
            assertNotNull(decompressedPage);
            assertDataPageEquals(pageHeader, DATA_PAGE, compressedDataPage, decompressedPage);
        }
        assertFalse(pageReader.hasNext());
        assertNull(pageReader.readPage());
    }

    @DataProvider
    public Object[][] pageParameters()
    {
        return new Object[][] {{UNCOMPRESSED, V1}, {SNAPPY, V1}, {UNCOMPRESSED, V2}, {SNAPPY, V2}};
    }

    public enum DataPageType
    {
        V1(PageType.DATA_PAGE) {
            @Override
            public void setDataPageHeader(PageHeader pageHeader, int valueCount)
            {
                pageHeader.setData_page_header(new DataPageHeader(valueCount, Encoding.PLAIN, Encoding.PLAIN, Encoding.PLAIN));
            }

            @Override
            public byte[] compress(CompressionCodec compressionCodec, byte[] dataPage)
            {
                return TestPageReader.compress(compressionCodec, dataPage, 0, dataPage.length);
            }
        },
        V2(DATA_PAGE_V2) {
            @Override
            public void setDataPageHeader(PageHeader pageHeader, int valueCount)
            {
                pageHeader.setData_page_header_v2(new DataPageHeaderV2(valueCount, 0, valueCount, Encoding.PLAIN, 1, 1));
            }

            @Override
            public byte[] compress(CompressionCodec compressionCodec, byte[] dataPage)
            {
                // compress only the date, copy definition and repetition levels uncompressed
                byte[] compressedData = TestPageReader.compress(compressionCodec, dataPage, 2, dataPage.length - 2);
                Slice slice = Slices.allocate(2 + compressedData.length);
                slice.setBytes(0, dataPage, 0, 2);
                slice.setBytes(2, compressedData);
                return slice.byteArray();
            }
        };

        private final PageType pageType;

        DataPageType(PageType pageType)
        {
            this.pageType = requireNonNull(pageType, "pageType is null");
        }

        public abstract void setDataPageHeader(PageHeader pageHeader, int valueCount);

        public PageType pageType()
        {
            return pageType;
        }

        public abstract byte[] compress(CompressionCodec compressionCodec, byte[] dataPage);
    }

    private static byte[] compress(CompressionCodec compressionCodec, byte[] bytes, int offset, int length)
    {
        if (compressionCodec == UNCOMPRESSED) {
            return Arrays.copyOfRange(bytes, offset, offset + length);
        }
        if (compressionCodec == SNAPPY) {
            byte[] out = new byte[SnappyRawCompressor.maxCompressedLength(length)];
            int compressedSize = new SnappyCompressor().compress(bytes, offset, length, out, 0, out.length);
            return Arrays.copyOf(out, compressedSize);
        }
        throw new IllegalArgumentException("unsupported compression code " + compressionCodec);
    }

    private static PageReader createPageReader(int valueCount, CompressionCodec compressionCodec, boolean hasDictionary, List<Slice> slices)
            throws IOException
    {
        EncodingStats.Builder encodingStats = new EncodingStats.Builder();
        if (hasDictionary) {
            encodingStats.addDictEncoding(PLAIN);
        }
        ColumnChunkMetaData columnChunkMetaData = ColumnChunkMetaData.get(
                ColumnPath.get(""),
                INT32,
                CompressionCodecName.fromParquet(compressionCodec),
                encodingStats.build(),
                ImmutableSet.of(),
                Statistics.createStats(Types.optional(INT32).named("fake_type")),
                0,
                0,
                valueCount,
                0,
                0);
        return PageReader.createPageReader(
                new ChunkedInputStream(slices.stream().map(TestingChunkReader::new).collect(toImmutableList())),
                columnChunkMetaData,
                new ColumnDescriptor(new String[] {}, new PrimitiveType(REQUIRED, INT32, ""), 0, 0),
                null,
                Optional.empty());
    }

    private static void assertDataPageEquals(PageHeader pageHeader, byte[] dataPage, byte[] compressedDataPage, DataPage decompressedPage)
    {
        assertThat(decompressedPage.getUncompressedSize()).isEqualTo(pageHeader.getUncompressed_page_size());
        assertThat(pageHeader.getCompressed_page_size()).isEqualTo(compressedDataPage.length);
        assertThat(decompressedPage.getFirstRowIndex()).isEmpty();
        if (PageType.DATA_PAGE.equals(pageHeader.getType())) {
            assertDataPageV1(dataPage, pageHeader, decompressedPage);
        }
        if (DATA_PAGE_V2.equals(pageHeader.getType())) {
            assertDataPageV2(dataPage, pageHeader, decompressedPage);
        }
    }

    private static void assertDataPageV1(byte[] dataPage, PageHeader pageHeader, DataPage decompressedPage)
    {
        DataPageHeader dataPageHeader = pageHeader.getData_page_header();
        assertThat(decompressedPage.getValueCount()).isEqualTo(dataPageHeader.getNum_values());
        assertThat(decompressedPage).isInstanceOf(DataPageV1.class);
        DataPageV1 decompressedV1Page = (DataPageV1) decompressedPage;
        assertThat(decompressedV1Page.getSlice()).isEqualTo(Slices.wrappedBuffer(dataPage));
        assertEncodingEquals(decompressedV1Page.getValueEncoding(), dataPageHeader.getEncoding());
        assertEncodingEquals(decompressedV1Page.getDefinitionLevelEncoding(), dataPageHeader.getDefinition_level_encoding());
        assertEncodingEquals(decompressedV1Page.getRepetitionLevelEncoding(), dataPageHeader.getRepetition_level_encoding());
    }

    private static void assertDataPageV2(byte[] dataPage, PageHeader pageHeader, DataPage decompressedPage)
    {
        DataPageHeaderV2 dataPageHeader = pageHeader.getData_page_header_v2();
        assertThat(decompressedPage.getValueCount()).isEqualTo(dataPageHeader.getNum_values());
        assertThat(decompressedPage).isInstanceOf(DataPageV2.class);
        DataPageV2 decompressedV2Page = (DataPageV2) decompressedPage;
        int dataOffset = dataPageHeader.getDefinition_levels_byte_length() + dataPageHeader.getRepetition_levels_byte_length();
        assertThat(decompressedV2Page.getSlice()).isEqualTo(Slices.wrappedBuffer(dataPage).slice(dataOffset, dataPage.length - dataOffset));
        assertEncodingEquals(decompressedV2Page.getDataEncoding(), dataPageHeader.getEncoding());

        assertThat(decompressedV2Page.getRepetitionLevels()).isEqualTo(Slices.wrappedBuffer(dataPage).slice(0, 1));
        assertThat(decompressedV2Page.getDefinitionLevels()).isEqualTo(Slices.wrappedBuffer(dataPage).slice(1, 1));
    }

    private static void assertEncodingEquals(ParquetEncoding parquetEncoding, Encoding encoding)
    {
        assertThat(parquetEncoding).isEqualTo(ParquetTypeUtils.getParquetEncoding(org.apache.parquet.column.Encoding.valueOf(encoding.name())));
    }
}
