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
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.crypto.AesCipherUtils;
import io.trino.parquet.crypto.ColumnDecryptionContext;
import io.trino.parquet.crypto.ModuleType;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import jakarta.annotation.Nullable;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.format.BlockCipher.Decryptor;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.parquet.ParquetTypeUtils.getParquetEncoding;
import static java.util.Objects.requireNonNull;

public final class ParquetColumnChunkIterator
        implements Iterator<Page>
{
    private final ParquetDataSourceId dataSourceId;
    private final Optional<String> fileCreatedBy;
    private final ColumnDescriptor descriptor;
    private final ColumnChunkMetadata metadata;
    private final ChunkedInputStream input;
    private final OffsetIndex offsetIndex;
    private final Optional<ColumnDecryptionContext> decryptionContext;
    private final long maxPageReadSizeInBytes;

    private long valueCount;
    private int dataPageCount;

    private byte[] dataPageHeaderAad;
    private boolean dictionaryWasRead;

    public ParquetColumnChunkIterator(
            ParquetDataSourceId dataSourceId,
            Optional<String> fileCreatedBy,
            ColumnDescriptor descriptor,
            ColumnChunkMetadata metadata,
            ChunkedInputStream input,
            @Nullable OffsetIndex offsetIndex,
            Optional<ColumnDecryptionContext> decryptionContext,
            long maxPageReadSizeInBytes)
    {
        this.dataSourceId = requireNonNull(dataSourceId, "dataSourceId is null");
        this.fileCreatedBy = requireNonNull(fileCreatedBy, "fileCreatedBy is null");
        this.descriptor = requireNonNull(descriptor, "descriptor is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.input = requireNonNull(input, "input is null");
        this.offsetIndex = offsetIndex;
        this.decryptionContext = requireNonNull(decryptionContext, "decryptionContext is null");
        checkArgument(maxPageReadSizeInBytes > 0, "maxPageReadSizeInBytes must be positive");
        this.maxPageReadSizeInBytes = maxPageReadSizeInBytes;
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
            byte[] pageHeaderAAD = null;
            if (decryptionContext.isPresent()) {
                if (!dictionaryWasRead && metadata.hasDictionaryPage()) {
                    pageHeaderAAD = getDictionaryPageHeaderAAD();
                }
                else {
                    pageHeaderAAD = getPageHeaderAAD();
                }
            }
            PageHeader pageHeader = readPageHeader(decryptionContext.map(ColumnDecryptionContext::metadataDecryptor).orElse(null), pageHeaderAAD);
            int uncompressedPageSize = pageHeader.getUncompressed_page_size();
            int compressedPageSize = pageHeader.getCompressed_page_size();

            if (uncompressedPageSize > maxPageReadSizeInBytes) {
                throw new ParquetCorruptionException(
                        dataSourceId,
                        "Parquet page size %d bytes exceeds maximum allowed size %d bytes for column %s",
                        uncompressedPageSize,
                        maxPageReadSizeInBytes,
                        descriptor);
            }

            Page result = null;
            switch (pageHeader.type) {
                case DICTIONARY_PAGE:
                    if (dataPageCount != 0) {
                        throw new ParquetCorruptionException(dataSourceId, "Column (%s) has a dictionary page after the first position in column chunk", descriptor);
                    }
                    result = readDictionaryPage(pageHeader, pageHeader.getUncompressed_page_size(), pageHeader.getCompressed_page_size());
                    dictionaryWasRead = true;
                    break;
                case DATA_PAGE:
                    result = readDataPageV1(pageHeader, uncompressedPageSize, compressedPageSize, getFirstRowIndex(dataPageCount, offsetIndex), dataPageCount);
                    ++dataPageCount;
                    break;
                case DATA_PAGE_V2:
                    result = readDataPageV2(pageHeader, uncompressedPageSize, compressedPageSize, getFirstRowIndex(dataPageCount, offsetIndex), dataPageCount);
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

    private byte[] getPageHeaderAAD()
    {
        checkState(decryptionContext.isPresent());

        if (dataPageHeaderAad != null) {
            AesCipherUtils.quickUpdatePageAAD(dataPageHeaderAad, dataPageCount);
            return dataPageHeaderAad;
        }

        dataPageHeaderAad = AesCipherUtils.createModuleAAD(
                decryptionContext.get().fileAad(),
                ModuleType.DataPageHeader,
                metadata.getRowGroupOrdinal(),
                metadata.getColumnOrdinal(),
                dataPageCount);
        return dataPageHeaderAad;
    }

    private byte[] getDictionaryPageHeaderAAD()
    {
        checkState(decryptionContext.isPresent());
        return AesCipherUtils.createModuleAAD(
                decryptionContext.get().fileAad(),
                ModuleType.DictionaryPageHeader,
                metadata.getRowGroupOrdinal(),
                metadata.getColumnOrdinal(),
                -1);
    }

    private PageHeader readPageHeader(Decryptor headerBlockDecryptor, byte[] pageHeaderAAD)
            throws IOException
    {
        return Util.readPageHeader(input, headerBlockDecryptor, pageHeaderAAD);
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
            OptionalLong firstRowIndex,
            int pageIndex)
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
                getParquetEncoding(Encoding.valueOf(dataHeaderV1.getEncoding().name())),
                pageIndex);
    }

    private DataPageV2 readDataPageV2(
            PageHeader pageHeader,
            int uncompressedPageSize,
            int compressedPageSize,
            OptionalLong firstRowIndex,
            int pageIndex)
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
                dataHeaderV2.isIs_compressed(),
                pageIndex);
    }

    private static OptionalLong getFirstRowIndex(int pageIndex, OffsetIndex offsetIndex)
    {
        return offsetIndex == null ? OptionalLong.empty() : OptionalLong.of(offsetIndex.getFirstRowIndex(pageIndex));
    }
}
