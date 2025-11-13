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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import io.airlift.slice.Slice;
import io.trino.parquet.DataPage;
import io.trino.parquet.DataPageV1;
import io.trino.parquet.DataPageV2;
import io.trino.parquet.DictionaryPage;
import io.trino.parquet.Page;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.crypto.AesCipherUtils;
import io.trino.parquet.crypto.ColumnDecryptionContext;
import io.trino.parquet.crypto.FileDecryptionContext;
import io.trino.parquet.crypto.ModuleType;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import jakarta.annotation.Nullable;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.parquet.ParquetCompressionUtils.decompress;
import static io.trino.parquet.ParquetReaderUtils.isOnlyDictionaryEncodingPages;
import static java.util.Objects.requireNonNull;

public final class PageReader
{
    private final ParquetDataSourceId dataSourceId;
    private final CompressionCodec codec;
    private final boolean hasOnlyDictionaryEncodedPages;
    private final boolean hasNoNulls;
    private final PeekingIterator<Page> compressedPages;
    private final Optional<BlockCipher.Decryptor> blockDecryptor;

    private boolean dictionaryAlreadyRead;
    private int dataPageReadCount;
    @Nullable
    private byte[] dataPageAad;
    @Nullable
    private byte[] dictionaryPageAad;

    public static PageReader createPageReader(
            ParquetDataSourceId dataSourceId,
            ChunkedInputStream columnChunk,
            ColumnChunkMetadata metadata,
            ColumnDescriptor columnDescriptor,
            @Nullable OffsetIndex offsetIndex,
            Optional<String> fileCreatedBy,
            Optional<FileDecryptionContext> decryptionContext,
            long maxPageSizeInBytes)
    {
        // Parquet schema may specify a column definition as OPTIONAL even though there are no nulls in the actual data.
        // Row-group column statistics can be used to identify such cases and switch to faster non-nullable read
        // paths in FlatColumnReader.
        Statistics<?> columnStatistics = metadata.getStatistics();
        boolean hasNoNulls = columnStatistics != null && columnStatistics.getNumNulls() == 0;
        boolean hasOnlyDictionaryEncodedPages = isOnlyDictionaryEncodingPages(metadata);
        Optional<ColumnDecryptionContext> columnDecryptionContext = decryptionContext.flatMap(context -> context.getColumnDecryptionContext(ColumnPath.get(columnDescriptor.getPath())));
        ParquetColumnChunkIterator compressedPages = new ParquetColumnChunkIterator(
                dataSourceId,
                fileCreatedBy,
                columnDescriptor,
                metadata,
                columnChunk,
                offsetIndex,
                columnDecryptionContext,
                maxPageSizeInBytes);

        return new PageReader(
                dataSourceId,
                metadata.getCodec().getParquetCompressionCodec(),
                compressedPages,
                hasOnlyDictionaryEncodedPages,
                hasNoNulls,
                columnDecryptionContext,
                metadata.getRowGroupOrdinal(),
                metadata.getColumnOrdinal());
    }

    @VisibleForTesting
    public PageReader(
            ParquetDataSourceId dataSourceId,
            CompressionCodec codec,
            Iterator<? extends Page> compressedPages,
            boolean hasOnlyDictionaryEncodedPages,
            boolean hasNoNulls,
            Optional<ColumnDecryptionContext> decryptionContext,
            int rowGroupOrdinal,
            int columnOrdinal)
    {
        this.dataSourceId = requireNonNull(dataSourceId, "dataSourceId is null");
        this.codec = codec;
        this.compressedPages = Iterators.peekingIterator(compressedPages);
        this.hasOnlyDictionaryEncodedPages = hasOnlyDictionaryEncodedPages;
        this.hasNoNulls = hasNoNulls;
        this.blockDecryptor = decryptionContext.map(ColumnDecryptionContext::dataDecryptor);
        if (blockDecryptor.isPresent()) {
            dataPageAad = AesCipherUtils.createModuleAAD(decryptionContext.get().fileAad(), ModuleType.DataPage, rowGroupOrdinal, columnOrdinal, 0);
            dictionaryPageAad = AesCipherUtils.createModuleAAD(decryptionContext.get().fileAad(), ModuleType.DictionaryPage, rowGroupOrdinal, columnOrdinal, -1);
        }
    }

    public boolean hasNoNulls()
    {
        return hasNoNulls;
    }

    public boolean hasOnlyDictionaryEncodedPages()
    {
        return hasOnlyDictionaryEncodedPages;
    }

    public DataPage readPage()
    {
        if (!compressedPages.hasNext()) {
            return null;
        }
        Page compressedPage = compressedPages.next();
        checkState(compressedPage instanceof DataPage, "Found page %s instead of a DataPage", compressedPage);
        dataPageReadCount++;
        try {
            if (blockDecryptor.isPresent()) {
                AesCipherUtils.quickUpdatePageAAD(dataPageAad, ((DataPage) compressedPage).getPageIndex());
            }
            Slice slice = decryptSliceIfNeeded(compressedPage.getSlice(), dataPageAad);
            if (compressedPage instanceof DataPageV1 dataPageV1) {
                return new DataPageV1(
                        !arePagesCompressed() ? slice : decompress(dataSourceId, codec, slice, dataPageV1.getUncompressedSize()),
                        dataPageV1.getValueCount(),
                        dataPageV1.getUncompressedSize(),
                        dataPageV1.getFirstRowIndex(),
                        dataPageV1.getRepetitionLevelEncoding(),
                        dataPageV1.getDefinitionLevelEncoding(),
                        dataPageV1.getValueEncoding(),
                        dataPageV1.getPageIndex());
            }
            DataPageV2 dataPageV2 = (DataPageV2) compressedPage;
            if (!dataPageV2.isCompressed()) {
                return dataPageV2;
            }
            int uncompressedSize = dataPageV2.getUncompressedSize()
                    - dataPageV2.getDefinitionLevels().length()
                    - dataPageV2.getRepetitionLevels().length();
            return new DataPageV2(
                    dataPageV2.getRowCount(),
                    dataPageV2.getNullCount(),
                    dataPageV2.getValueCount(),
                    dataPageV2.getRepetitionLevels(),
                    dataPageV2.getDefinitionLevels(),
                    dataPageV2.getDataEncoding(),
                    decompress(dataSourceId, codec, slice, uncompressedSize),
                    dataPageV2.getUncompressedSize(),
                    dataPageV2.getFirstRowIndex(),
                    dataPageV2.getStatistics(),
                    false,
                    dataPageV2.getPageIndex());
        }
        catch (IOException e) {
            throw new RuntimeException("Could not decompress page", e);
        }
    }

    public DictionaryPage readDictionaryPage()
    {
        checkState(!dictionaryAlreadyRead, "Dictionary was already read");
        checkState(dataPageReadCount == 0, "Dictionary has to be read first but " + dataPageReadCount + " was read already");
        dictionaryAlreadyRead = true;
        if (!(compressedPages.peek() instanceof DictionaryPage)) {
            return null;
        }
        try {
            DictionaryPage compressedDictionaryPage = (DictionaryPage) compressedPages.next();
            Slice slice = decryptSliceIfNeeded(compressedDictionaryPage.getSlice(), dictionaryPageAad);
            return new DictionaryPage(
                    decompress(dataSourceId, codec, slice, compressedDictionaryPage.getUncompressedSize()),
                    compressedDictionaryPage.getDictionarySize(),
                    compressedDictionaryPage.getEncoding());
        }
        catch (IOException e) {
            throw new RuntimeException("Error reading dictionary page", e);
        }
    }

    public boolean hasNext()
    {
        return compressedPages.hasNext();
    }

    public DataPage getNextPage()
    {
        verifyDictionaryPageRead();

        return (DataPage) compressedPages.peek();
    }

    public void skipNextPage()
    {
        verifyDictionaryPageRead();
        compressedPages.next();
    }

    public boolean arePagesCompressed()
    {
        return codec != CompressionCodec.UNCOMPRESSED;
    }

    private void verifyDictionaryPageRead()
    {
        checkArgument(dictionaryAlreadyRead, "Dictionary has to be read first");
    }

    private Slice decryptSliceIfNeeded(Slice slice, byte[] aad)
            throws IOException
    {
        if (blockDecryptor.isEmpty()) {
            return slice;
        }
        byte[] plainText = blockDecryptor.get().decrypt(slice.getBytes(), aad);
        return wrappedBuffer(plainText);
    }
}
