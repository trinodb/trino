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

import io.trino.parquet.DataPageV1;
import io.trino.parquet.DataPageV2;
import io.trino.parquet.DictionaryPage;
import io.trino.parquet.Page;
import io.trino.parquet.ParquetCorruptionException;
import jakarta.annotation.Nullable;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.parquet.ParquetTypeUtils.getParquetEncoding;
import static java.util.Objects.requireNonNull;

public final class ParquetColumnChunkIterator
        implements Iterator<Page>
{
    private final Optional<String> fileCreatedBy;
    private final ColumnDescriptor descriptor;
    private final ColumnChunkMetaData metadata;
    private final ChunkedInputStream input;
    private final OffsetIndex offsetIndex;

    private long valueCount;
    private int dataPageCount;

    public ParquetColumnChunkIterator(
            Optional<String> fileCreatedBy,
            ColumnDescriptor descriptor,
            ColumnChunkMetaData metadata,
            ChunkedInputStream input,
            @Nullable OffsetIndex offsetIndex)
    {
        this.fileCreatedBy = requireNonNull(fileCreatedBy, "fileCreatedBy is null");
        this.descriptor = requireNonNull(descriptor, "descriptor is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.input = requireNonNull(input, "input is null");
        this.offsetIndex = offsetIndex;
    }

    @Override
    public boolean hasNext()
    {
        return hasMorePages(valueCount, dataPageCount);
    }

    @Override
    public Page next()
    {
        checkState(hasNext(), "No more data left to read in column (%s), metadata (%s), valueCount %s, dataPageCount %s", descriptor, metadata, valueCount, dataPageCount);

        try {
            PageHeader pageHeader = readPageHeader();
            int uncompressedPageSize = pageHeader.getUncompressed_page_size();
            int compressedPageSize = pageHeader.getCompressed_page_size();
            Page result = null;
            switch (pageHeader.type) {
                case DICTIONARY_PAGE:
                    if (dataPageCount != 0) {
                        throw new ParquetCorruptionException("Column (%s) has a dictionary page after the first position in column chunk", descriptor);
                    }
                    result = readDictionaryPage(pageHeader, pageHeader.getUncompressed_page_size(), pageHeader.getCompressed_page_size());
                    break;
                case DATA_PAGE:
                    result = readDataPageV1(pageHeader, uncompressedPageSize, compressedPageSize, getFirstRowIndex(dataPageCount, offsetIndex));
                    ++dataPageCount;
                    break;
                case DATA_PAGE_V2:
                    result = readDataPageV2(pageHeader, uncompressedPageSize, compressedPageSize, getFirstRowIndex(dataPageCount, offsetIndex));
                    ++dataPageCount;
                    break;
                default:
                    input.skip(compressedPageSize);
                    break;
            }
            return result;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private PageHeader readPageHeader()
            throws IOException
    {
        return Util.readPageHeader(input);
    }

    private boolean hasMorePages(long valuesCountReadSoFar, int dataPageCountReadSoFar)
    {
        if (offsetIndex == null) {
            return valuesCountReadSoFar < metadata.getValueCount();
        }
        return dataPageCountReadSoFar < offsetIndex.getPageCount();
    }

    private DictionaryPage readDictionaryPage(PageHeader pageHeader, int uncompressedPageSize, int compressedPageSize)
            throws IOException
    {
        DictionaryPageHeader dicHeader = pageHeader.getDictionary_page_header();
        return new DictionaryPage(
                input.getSlice(compressedPageSize),
                uncompressedPageSize,
                dicHeader.getNum_values(),
                getParquetEncoding(Encoding.valueOf(dicHeader.getEncoding().name())));
    }

    private DataPageV1 readDataPageV1(
            PageHeader pageHeader,
            int uncompressedPageSize,
            int compressedPageSize,
            OptionalLong firstRowIndex)
            throws IOException
    {
        DataPageHeader dataHeaderV1 = pageHeader.getData_page_header();
        valueCount += dataHeaderV1.getNum_values();
        return new DataPageV1(
                input.getSlice(compressedPageSize),
                dataHeaderV1.getNum_values(),
                uncompressedPageSize,
                firstRowIndex,
                getParquetEncoding(Encoding.valueOf(dataHeaderV1.getRepetition_level_encoding().name())),
                getParquetEncoding(Encoding.valueOf(dataHeaderV1.getDefinition_level_encoding().name())),
                getParquetEncoding(Encoding.valueOf(dataHeaderV1.getEncoding().name())));
    }

    private DataPageV2 readDataPageV2(
            PageHeader pageHeader,
            int uncompressedPageSize,
            int compressedPageSize,
            OptionalLong firstRowIndex)
            throws IOException
    {
        DataPageHeaderV2 dataHeaderV2 = pageHeader.getData_page_header_v2();
        int dataSize = compressedPageSize - dataHeaderV2.getRepetition_levels_byte_length() - dataHeaderV2.getDefinition_levels_byte_length();
        valueCount += dataHeaderV2.getNum_values();
        return new DataPageV2(
                dataHeaderV2.getNum_rows(),
                dataHeaderV2.getNum_nulls(),
                dataHeaderV2.getNum_values(),
                input.getSlice(dataHeaderV2.getRepetition_levels_byte_length()),
                input.getSlice(dataHeaderV2.getDefinition_levels_byte_length()),
                getParquetEncoding(Encoding.valueOf(dataHeaderV2.getEncoding().name())),
                input.getSlice(dataSize),
                uncompressedPageSize,
                firstRowIndex,
                MetadataReader.readStats(
                        fileCreatedBy,
                        Optional.ofNullable(dataHeaderV2.getStatistics()),
                        descriptor.getPrimitiveType()),
                dataHeaderV2.isIs_compressed());
    }

    private static OptionalLong getFirstRowIndex(int pageIndex, OffsetIndex offsetIndex)
    {
        return offsetIndex == null ? OptionalLong.empty() : OptionalLong.of(offsetIndex.getFirstRowIndex(pageIndex));
    }
}
