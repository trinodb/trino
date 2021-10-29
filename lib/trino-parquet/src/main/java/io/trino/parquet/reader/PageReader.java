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
import io.trino.parquet.DataPageV1;
import io.trino.parquet.DataPageV2;
import io.trino.parquet.DictionaryPage;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;

import java.io.IOException;
import java.util.LinkedList;
import java.util.OptionalLong;

import static io.trino.parquet.ParquetCompressionUtils.decompress;

class PageReader
{
    private final CompressionCodecName codec;
    private final long valueCount;
    private final LinkedList<DataPage> compressedPages;
    private final DictionaryPage compressedDictionaryPage;
    private final OffsetIndex offsetIndex;
    private int pageIndex;

    /**
     * @param compressedPages This parameter will be mutated destructively as {@link DataPage} entries are removed as part of {@link #readPage()}. The caller
     * should not retain a reference to this list after passing it in as a constructor argument.
     */
    public PageReader(CompressionCodecName codec,
                      LinkedList<DataPage> compressedPages,
                      DictionaryPage compressedDictionaryPage,
                      OffsetIndex offsetIndex,
                      long valueCount)
    {
        this.codec = codec;
        this.compressedPages = compressedPages;
        this.compressedDictionaryPage = compressedDictionaryPage;
        this.valueCount = valueCount;
        this.offsetIndex = offsetIndex;
        this.pageIndex = 0;
    }

    public long getTotalValueCount()
    {
        return valueCount;
    }

    public DataPage readPage()
    {
        if (compressedPages.isEmpty()) {
            return null;
        }
        DataPage compressedPage = compressedPages.removeFirst();
        try {
            OptionalLong firstRowIndex = getFirstRowIndex(pageIndex, offsetIndex);
            pageIndex++;
            if (compressedPage instanceof DataPageV1) {
                DataPageV1 dataPageV1 = (DataPageV1) compressedPage;
                return new DataPageV1(
                        decompress(codec, dataPageV1.getSlice(), dataPageV1.getUncompressedSize()),
                        dataPageV1.getValueCount(),
                        dataPageV1.getUncompressedSize(),
                        firstRowIndex,
                        dataPageV1.getRepetitionLevelEncoding(),
                        dataPageV1.getDefinitionLevelEncoding(),
                        dataPageV1.getValueEncoding());
            }
            else {
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
                        decompress(codec, dataPageV2.getSlice(), uncompressedSize),
                        dataPageV2.getUncompressedSize(),
                        firstRowIndex,
                        dataPageV2.getStatistics(),
                        false);
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Could not decompress page", e);
        }
    }

    public DictionaryPage readDictionaryPage()
    {
        if (compressedDictionaryPage == null) {
            return null;
        }
        try {
            return new DictionaryPage(
                    decompress(codec, compressedDictionaryPage.getSlice(), compressedDictionaryPage.getUncompressedSize()),
                    compressedDictionaryPage.getDictionarySize(),
                    compressedDictionaryPage.getEncoding());
        }
        catch (IOException e) {
            throw new RuntimeException("Error reading dictionary page", e);
        }
    }

    public static OptionalLong getFirstRowIndex(int pageIndex, OffsetIndex offsetIndex)
    {
        return offsetIndex == null ? OptionalLong.empty() : OptionalLong.of(offsetIndex.getFirstRowIndex(pageIndex));
    }
}
