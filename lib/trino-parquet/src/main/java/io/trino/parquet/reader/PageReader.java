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

import java.io.IOException;
import java.util.LinkedList;

import static io.trino.parquet.ParquetCompressionUtils.decompress;

public final class PageReader
{
    private final CompressionCodecName codec;
    private final long valueCount;
    private final boolean hasNoNulls;
    private final LinkedList<DataPage> compressedPages;
    private final DictionaryPage compressedDictionaryPage;

    /**
     * @param compressedPages This parameter will be mutated destructively as {@link DataPage} entries are removed as part of {@link #readPage()}. The caller
     * should not retain a reference to this list after passing it in as a constructor argument.
     */
    public PageReader(CompressionCodecName codec,
                      LinkedList<DataPage> compressedPages,
                      DictionaryPage compressedDictionaryPage,
                      long valueCount,
                      boolean hasNoNulls)
    {
        this.codec = codec;
        this.compressedPages = compressedPages;
        this.compressedDictionaryPage = compressedDictionaryPage;
        this.valueCount = valueCount;
        this.hasNoNulls = hasNoNulls;
    }

    public long getTotalValueCount()
    {
        return valueCount;
    }

    public boolean hasNoNulls()
    {
        return hasNoNulls;
    }

    public DataPage readPage()
    {
        if (compressedPages.isEmpty()) {
            return null;
        }
        DataPage compressedPage = compressedPages.removeFirst();
        try {
            if (compressedPage instanceof DataPageV1) {
                DataPageV1 dataPageV1 = (DataPageV1) compressedPage;
                return new DataPageV1(
                        decompress(codec, dataPageV1.getSlice(), dataPageV1.getUncompressedSize()),
                        dataPageV1.getValueCount(),
                        dataPageV1.getUncompressedSize(),
                        dataPageV1.getFirstRowIndex(),
                        dataPageV1.getRepetitionLevelEncoding(),
                        dataPageV1.getDefinitionLevelEncoding(),
                        dataPageV1.getValueEncoding());
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
                    decompress(codec, dataPageV2.getSlice(), uncompressedSize),
                    dataPageV2.getUncompressedSize(),
                    dataPageV2.getFirstRowIndex(),
                    dataPageV2.getStatistics(),
                    false);
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

    public DataPage getNextPage()
    {
        return compressedPages.getFirst();
    }

    public boolean hasNext()
    {
        return !compressedPages.isEmpty();
    }

    public void skipNextPage()
    {
        compressedPages.removeFirst();
    }
}
