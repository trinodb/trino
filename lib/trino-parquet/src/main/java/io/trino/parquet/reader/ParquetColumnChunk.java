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

import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slice;
import io.trino.parquet.DataPage;
import io.trino.parquet.DataPageV1;
import io.trino.parquet.DataPageV2;
import io.trino.parquet.DictionaryPage;
import io.trino.parquet.ParquetCorruptionException;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Verify.verify;
import static io.trino.parquet.ParquetTypeUtils.getParquetEncoding;
import static java.util.Objects.requireNonNull;

public class ParquetColumnChunk
{
    private final Optional<String> fileCreatedBy;
    private final ColumnChunkDescriptor descriptor;
    private final List<Slice> slices;
    private int sliceIndex;
    private BasicSliceInput input;
    private final OffsetIndex offsetIndex;

    public ParquetColumnChunk(
            Optional<String> fileCreatedBy,
            ColumnChunkDescriptor descriptor,
            List<Slice> slices,
            OffsetIndex offsetIndex)
    {
        this.fileCreatedBy = requireNonNull(fileCreatedBy, "fileCreatedBy is null");
        this.descriptor = descriptor;
        this.slices = slices;
        this.sliceIndex = 0;
        this.input = slices.get(0).getInput();
        this.offsetIndex = offsetIndex;
    }

    private void advanceIfNecessary()
    {
        if (input.available() == 0) {
            sliceIndex++;
            if (sliceIndex < slices.size()) {
                input = slices.get(sliceIndex).getInput();
            }
        }
    }

    protected PageHeader readPageHeader()
            throws IOException
    {
        verify(input.available() > 0, "Reached end of input unexpectedly");
        PageHeader pageHeader = Util.readPageHeader(input);
        advanceIfNecessary();
        return pageHeader;
    }

    public PageReader readAllPages()
            throws IOException
    {
        LinkedList<DataPage> pages = new LinkedList<>();
        DictionaryPage dictionaryPage = null;
        long valueCount = 0;
        int dataPageCount = 0;
        while (hasMorePages(valueCount, dataPageCount)) {
            PageHeader pageHeader = readPageHeader();
            int uncompressedPageSize = pageHeader.getUncompressed_page_size();
            int compressedPageSize = pageHeader.getCompressed_page_size();
            OptionalLong firstRowIndex;
            switch (pageHeader.type) {
                case DICTIONARY_PAGE:
                    if (dictionaryPage != null) {
                        throw new ParquetCorruptionException("%s has more than one dictionary page in column chunk", descriptor.getColumnDescriptor());
                    }
                    dictionaryPage = readDictionaryPage(pageHeader, uncompressedPageSize, compressedPageSize);
                    break;
                case DATA_PAGE:
                    firstRowIndex = PageReader.getFirstRowIndex(dataPageCount, offsetIndex);
                    valueCount += readDataPageV1(pageHeader, uncompressedPageSize, compressedPageSize, pages, firstRowIndex);
                    ++dataPageCount;
                    break;
                case DATA_PAGE_V2:
                    firstRowIndex = PageReader.getFirstRowIndex(dataPageCount, offsetIndex);
                    valueCount += readDataPageV2(pageHeader, uncompressedPageSize, compressedPageSize, pages, firstRowIndex);
                    ++dataPageCount;
                    break;
                default:
                    input.skip(compressedPageSize);
                    advanceIfNecessary();
                    break;
            }
        }
        return new PageReader(descriptor.getColumnChunkMetaData().getCodec(), pages, dictionaryPage, offsetIndex, valueCount);
    }

    private boolean hasMorePages(long valuesCountReadSoFar, int dataPageCountReadSoFar)
    {
        if (offsetIndex == null) {
            return valuesCountReadSoFar < descriptor.getColumnChunkMetaData().getValueCount();
        }
        else {
            return dataPageCountReadSoFar < offsetIndex.getPageCount();
        }
    }

    private Slice getSlice(int size)
    {
        Slice slice = input.readSlice(size);
        advanceIfNecessary();
        return slice;
    }

    private DictionaryPage readDictionaryPage(PageHeader pageHeader, int uncompressedPageSize, int compressedPageSize)
    {
        DictionaryPageHeader dicHeader = pageHeader.getDictionary_page_header();
        return new DictionaryPage(
                getSlice(compressedPageSize),
                uncompressedPageSize,
                dicHeader.getNum_values(),
                getParquetEncoding(Encoding.valueOf(dicHeader.getEncoding().name())));
    }

    private long readDataPageV1(
            PageHeader pageHeader,
            int uncompressedPageSize,
            int compressedPageSize,
            List<DataPage> pages,
            OptionalLong firstRowIndex)
    {
        DataPageHeader dataHeaderV1 = pageHeader.getData_page_header();
        pages.add(new DataPageV1(
                getSlice(compressedPageSize),
                dataHeaderV1.getNum_values(),
                uncompressedPageSize,
                firstRowIndex,
                getParquetEncoding(Encoding.valueOf(dataHeaderV1.getRepetition_level_encoding().name())),
                getParquetEncoding(Encoding.valueOf(dataHeaderV1.getDefinition_level_encoding().name())),
                getParquetEncoding(Encoding.valueOf(dataHeaderV1.getEncoding().name()))));
        return dataHeaderV1.getNum_values();
    }

    private long readDataPageV2(
            PageHeader pageHeader,
            int uncompressedPageSize,
            int compressedPageSize,
            List<DataPage> pages,
            OptionalLong firstRowIndex)
    {
        DataPageHeaderV2 dataHeaderV2 = pageHeader.getData_page_header_v2();
        int dataSize = compressedPageSize - dataHeaderV2.getRepetition_levels_byte_length() - dataHeaderV2.getDefinition_levels_byte_length();
        pages.add(new DataPageV2(
                dataHeaderV2.getNum_rows(),
                dataHeaderV2.getNum_nulls(),
                dataHeaderV2.getNum_values(),
                getSlice(dataHeaderV2.getRepetition_levels_byte_length()),
                getSlice(dataHeaderV2.getDefinition_levels_byte_length()),
                getParquetEncoding(Encoding.valueOf(dataHeaderV2.getEncoding().name())),
                getSlice(dataSize),
                uncompressedPageSize,
                firstRowIndex,
                MetadataReader.readStats(
                        fileCreatedBy,
                        Optional.ofNullable(dataHeaderV2.getStatistics()),
                        descriptor.getColumnDescriptor().getPrimitiveType()),
                dataHeaderV2.isIs_compressed()));
        return dataHeaderV2.getNum_values();
    }
}
