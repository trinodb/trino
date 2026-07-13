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
import com.google.common.collect.ImmutableList;
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
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
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
    private final Iterator<? extends Page> compressedPages;
    private final ArrayDeque<BufferedPage> bufferedPages = new ArrayDeque<>();
    private final Optional<BlockCipher.Decryptor> blockDecryptor;

    private boolean dictionaryAlreadyRead;
    private int dataPageReadCount;
    private long bufferedPageBytes;
    private long bufferedPageRetainedBytes;
    private long currentPageRetainedBytes;
    private boolean currentPageOwned;
    @Nullable
    private Page currentPage;
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
        this.compressedPages = requireNonNull(compressedPages, "compressedPages is null");
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
        if (!hasNext()) {
            return null;
        }
        Page compressedPage = nextCompressedPage();
        try {
            checkState(compressedPage instanceof DataPage, "Found page %s instead of a DataPage", compressedPage);
            dataPageReadCount++;
            if (blockDecryptor.isPresent()) {
                AesCipherUtils.quickUpdatePageAAD(dataPageAad, ((DataPage) compressedPage).getPageIndex());
            }
            Slice slice = decryptSliceIfNeeded(compressedPage.getSlice(), dataPageAad);
            if (compressedPage instanceof DataPageV1 dataPageV1) {
                PageData pageData = getPageData(slice, dataPageV1.getUncompressedSize(), arePagesCompressed());
                DataPage page = new DataPageV1(
                        pageData.data(),
                        dataPageV1.getValueCount(),
                        dataPageV1.getUncompressedSize(),
                        dataPageV1.getFirstRowIndex(),
                        dataPageV1.getRepetitionLevelEncoding(),
                        dataPageV1.getDefinitionLevelEncoding(),
                        dataPageV1.getValueEncoding(),
                        dataPageV1.getPageIndex());
                replaceCurrentPage(page, pageData.retainedBytes(), pageData.owned());
                return page;
            }
            DataPageV2 dataPageV2 = (DataPageV2) compressedPage;
            if (!dataPageV2.isCompressed() && blockDecryptor.isEmpty()) {
                return dataPageV2;
            }
            int uncompressedDataSize = dataPageV2.getUncompressedSize()
                    - dataPageV2.getDefinitionLevels().length()
                    - dataPageV2.getRepetitionLevels().length();
            PageData pageData = getPageData(slice, uncompressedDataSize, dataPageV2.isCompressed());
            Slice repetitionLevels = copySlice(dataPageV2.getRepetitionLevels());
            Slice definitionLevels = copySlice(dataPageV2.getDefinitionLevels());
            DataPage page = new DataPageV2(
                    dataPageV2.getRowCount(),
                    dataPageV2.getNullCount(),
                    dataPageV2.getValueCount(),
                    repetitionLevels,
                    definitionLevels,
                    dataPageV2.getDataEncoding(),
                    pageData.data(),
                    dataPageV2.getUncompressedSize(),
                    dataPageV2.getFirstRowIndex(),
                    dataPageV2.getStatistics(),
                    false,
                    dataPageV2.getPageIndex());
            replaceCurrentPage(
                    page,
                    repetitionLevels.length() + definitionLevels.length() + pageData.retainedBytes(),
                    true);
            return page;
        }
        catch (IOException e) {
            releaseCurrentPage();
            throw new RuntimeException("Could not decompress page", e);
        }
        catch (RuntimeException | Error e) {
            releaseCurrentPage();
            throw e;
        }
    }

    public DictionaryPage readDictionaryPage()
    {
        checkState(!dictionaryAlreadyRead, "Dictionary was already read");
        checkState(dataPageReadCount == 0, "Dictionary has to be read first but %s was read already", dataPageReadCount);
        dictionaryAlreadyRead = true;
        if (!(peekCompressedPage() instanceof DictionaryPage)) {
            return null;
        }
        try {
            DictionaryPage compressedDictionaryPage = (DictionaryPage) nextCompressedPage();
            Slice slice = decryptSliceIfNeeded(compressedDictionaryPage.getSlice(), dictionaryPageAad);
            PageData pageData = getPageData(slice, compressedDictionaryPage.getUncompressedSize(), arePagesCompressed());
            DictionaryPage dictionaryPage = new DictionaryPage(
                    pageData.data(),
                    compressedDictionaryPage.getDictionarySize(),
                    compressedDictionaryPage.getEncoding());
            replaceCurrentPage(dictionaryPage, pageData.retainedBytes(), pageData.owned());
            return dictionaryPage;
        }
        catch (IOException e) {
            releaseCurrentPage();
            throw new RuntimeException("Error reading dictionary page", e);
        }
        catch (RuntimeException | Error e) {
            releaseCurrentPage();
            throw e;
        }
    }

    public boolean hasNext()
    {
        return !bufferedPages.isEmpty() || compressedPages.hasNext();
    }

    public DataPage getNextPage()
    {
        verifyDictionaryPageRead();

        return (DataPage) peekCompressedPage();
    }

    public void skipNextPage()
    {
        verifyDictionaryPageRead();
        nextCompressedPage();
        releaseCurrentPage();
    }

    public List<DataPage> getNextDataPages(int valueCount, int maxPageCount, long maxBufferedBytes)
    {
        verifyDictionaryPageRead();
        checkArgument(valueCount >= 0, "valueCount is negative");
        checkArgument(maxPageCount > 0, "maxPageCount must be positive");
        checkArgument(maxBufferedBytes > 0, "maxBufferedBytes must be positive");

        int bufferedValueCount = 0;
        int pageCount = 0;
        for (BufferedPage bufferedPage : bufferedPages) {
            Page page = bufferedPage.page();
            checkState(page instanceof DataPage, "Found page %s instead of a DataPage", page);
            DataPage dataPage = (DataPage) page;
            pageCount++;
            bufferedValueCount += dataPage.getValueCount();
            if (bufferedValueCount >= valueCount || pageCount >= maxPageCount) {
                return getBufferedDataPages(valueCount, maxPageCount);
            }
        }

        while (bufferedValueCount < valueCount && pageCount < maxPageCount && bufferedPageBytes < maxBufferedBytes && canAdvanceInput() && compressedPages.hasNext()) {
            ownLastBufferedPage();
            Page page = compressedPages.next();
            checkState(page instanceof DataPage, "Found page %s instead of a DataPage", page);
            addBufferedPage(page);
            DataPage dataPage = (DataPage) page;
            bufferedValueCount += dataPage.getValueCount();
            pageCount++;
        }
        return getBufferedDataPages(valueCount, maxPageCount);
    }

    public long getRetainedPageBytes()
    {
        return bufferedPageRetainedBytes + currentPageRetainedBytes;
    }

    @VisibleForTesting
    public int getDataPageReadCount()
    {
        return dataPageReadCount;
    }

    private Page peekCompressedPage()
    {
        if (bufferedPages.isEmpty()) {
            addBufferedPage(compressedPages.next());
        }
        return bufferedPages.peekFirst().page();
    }

    private Page nextCompressedPage()
    {
        releaseCurrentPage();
        if (!bufferedPages.isEmpty()) {
            BufferedPage bufferedPage = bufferedPages.removeFirst();
            bufferedPageBytes -= getPageBytes(bufferedPage.page());
            bufferedPageRetainedBytes -= bufferedPage.retainedBytes();
            currentPage = bufferedPage.page();
            currentPageRetainedBytes = bufferedPage.retainedBytes();
            currentPageOwned = bufferedPage.owned();
            return currentPage;
        }
        currentPage = compressedPages.next();
        currentPageOwned = false;
        return currentPage;
    }

    public void releaseCurrentPage()
    {
        currentPage = null;
        currentPageRetainedBytes = 0;
        currentPageOwned = false;
    }

    public void close()
    {
        releaseCurrentPage();
        bufferedPages.clear();
        bufferedPageBytes = 0;
        bufferedPageRetainedBytes = 0;
    }

    private void replaceCurrentPage(Page page, long retainedBytes, boolean owned)
    {
        currentPage = requireNonNull(page, "page is null");
        currentPageRetainedBytes = retainedBytes;
        currentPageOwned = owned;
    }

    private void addBufferedPage(Page page)
    {
        BufferedPage bufferedPage = new BufferedPage(page, 0, false);
        bufferedPages.addLast(bufferedPage);
        bufferedPageBytes += getPageBytes(page);
    }

    private void ownLastBufferedPage()
    {
        if (!bufferedPages.isEmpty()) {
            BufferedPage bufferedPage = bufferedPages.removeLast();
            if (!bufferedPage.owned()) {
                BufferedPage ownedPage = copyPage(bufferedPage.page());
                bufferedPageRetainedBytes += ownedPage.retainedBytes();
                bufferedPage = ownedPage;
            }
            bufferedPages.addLast(bufferedPage);
        }
    }

    private boolean canAdvanceInput()
    {
        return currentPage == null || currentPageOwned;
    }

    private List<DataPage> getBufferedDataPages(int valueCount, int maxPageCount)
    {
        ImmutableList.Builder<DataPage> pages = ImmutableList.builder();
        int bufferedValueCount = 0;
        int pageCount = 0;
        for (BufferedPage bufferedPage : bufferedPages) {
            Page page = bufferedPage.page();
            checkState(page instanceof DataPage, "Found page %s instead of a DataPage", page);
            DataPage dataPage = (DataPage) page;
            pages.add(dataPage);
            bufferedValueCount += dataPage.getValueCount();
            pageCount++;
            if (bufferedValueCount >= valueCount || pageCount >= maxPageCount) {
                break;
            }
        }
        return pages.build();
    }

    private static Slice copySlice(Slice slice)
    {
        return wrappedBuffer(slice.getBytes());
    }

    private PageData getPageData(Slice data, int uncompressedSize, boolean compressed)
            throws IOException
    {
        if (compressed) {
            Slice uncompressed = arePagesCompressed()
                    ? decompress(dataSourceId, codec, data, uncompressedSize)
                    : copySlice(data);
            return new PageData(uncompressed, uncompressed.length(), true);
        }
        if (blockDecryptor.isPresent()) {
            return new PageData(data, data.length(), true);
        }
        return new PageData(data, currentPageRetainedBytes, currentPageOwned);
    }

    private static BufferedPage copyPage(Page page)
    {
        if (page instanceof DataPageV1 dataPageV1) {
            Slice data = copySlice(dataPageV1.getSlice());
            return new BufferedPage(
                    new DataPageV1(
                            data,
                            dataPageV1.getValueCount(),
                            dataPageV1.getUncompressedSize(),
                            dataPageV1.getFirstRowIndex(),
                            dataPageV1.getRepetitionLevelEncoding(),
                            dataPageV1.getDefinitionLevelEncoding(),
                            dataPageV1.getValueEncoding(),
                            dataPageV1.getPageIndex()),
                    data.length(),
                    true);
        }
        if (page instanceof DataPageV2 dataPageV2) {
            int repetitionLevelsLength = dataPageV2.getRepetitionLevels().length();
            int definitionLevelsLength = dataPageV2.getDefinitionLevels().length();
            int dataLength = dataPageV2.getSlice().length();
            byte[] bytes = new byte[repetitionLevelsLength + definitionLevelsLength + dataLength];
            dataPageV2.getRepetitionLevels().getBytes(0, bytes, 0, repetitionLevelsLength);
            dataPageV2.getDefinitionLevels().getBytes(0, bytes, repetitionLevelsLength, definitionLevelsLength);
            dataPageV2.getSlice().getBytes(0, bytes, repetitionLevelsLength + definitionLevelsLength, dataLength);
            Slice data = wrappedBuffer(bytes);
            return new BufferedPage(
                    new DataPageV2(
                            dataPageV2.getRowCount(),
                            dataPageV2.getNullCount(),
                            dataPageV2.getValueCount(),
                            data.slice(0, repetitionLevelsLength),
                            data.slice(repetitionLevelsLength, definitionLevelsLength),
                            dataPageV2.getDataEncoding(),
                            data.slice(repetitionLevelsLength + definitionLevelsLength, dataLength),
                            dataPageV2.getUncompressedSize(),
                            dataPageV2.getFirstRowIndex(),
                            dataPageV2.getStatistics(),
                            dataPageV2.isCompressed(),
                            dataPageV2.getPageIndex()),
                    bytes.length,
                    true);
        }
        if (page instanceof DictionaryPage dictionaryPage) {
            Slice data = copySlice(dictionaryPage.getSlice());
            return new BufferedPage(
                    new DictionaryPage(
                            data,
                            dictionaryPage.getUncompressedSize(),
                            dictionaryPage.getDictionarySize(),
                            dictionaryPage.getEncoding()),
                    data.length(),
                    true);
        }
        throw new IllegalArgumentException("Unsupported page: " + page);
    }

    private static long getPageBytes(Page page)
    {
        if (page instanceof DataPageV2 dataPageV2) {
            return (long) dataPageV2.getRepetitionLevels().length()
                    + dataPageV2.getDefinitionLevels().length()
                    + dataPageV2.getSlice().length();
        }
        return page.getSlice().length();
    }

    private record BufferedPage(Page page, long retainedBytes, boolean owned)
    {
        private BufferedPage
        {
            requireNonNull(page, "page is null");
            checkArgument(retainedBytes >= 0, "retainedBytes is negative");
            checkArgument(owned || retainedBytes == 0, "borrowed page has retained bytes");
        }
    }

    private record PageData(Slice data, long retainedBytes, boolean owned)
    {
        private PageData
        {
            requireNonNull(data, "data is null");
            checkArgument(retainedBytes >= 0, "retainedBytes is negative");
            checkArgument(owned || retainedBytes == 0, "borrowed data has retained bytes");
        }
    }

    private boolean arePagesCompressed()
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
