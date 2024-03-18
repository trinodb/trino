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
package io.trino.plugin.varada.storage.write;

import io.trino.plugin.varada.dictionary.DictionaryWarmInfo;
import io.trino.plugin.varada.dictionary.WriteDictionary;
import io.trino.plugin.varada.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.storage.juffers.WriteJuffersWarmUpElement;
import io.trino.plugin.varada.storage.lucene.LuceneIndexer;
import io.trino.plugin.varada.storage.write.appenders.BlockAppender;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

final class StorageWriterContext
{
    private final AtomicReference<Object> isCleanupDone;
    private final WarmupElementStats warmupElementStats;
    private boolean weClosed;
    private final WarmupElementWriteMetadata warmupElementWriteMetadata;
    private int recordBufferSize;
    private int recordBufferPos;
    private WarmUpElement.Builder warmupElementBuilder;
    private final WriteJuffersWarmUpElement writeJuffersWarmUpElement;
    private final DictionaryWarmInfo dictionaryWarmInfo;
    private final long weCookie;
    private final BlockAppender blockAppender;
    private boolean weSuccess;
    private final Optional<WriteDictionary> writeDictionary;
    private final Optional<LuceneIndexer> luceneIndexer;

    StorageWriterContext(
            WarmupElementWriteMetadata warmupElementWriteMetadata,
            WarmUpElement.Builder warmupElementBuilder,
            WriteJuffersWarmUpElement writeJuffersWarmUpElement,
            DictionaryWarmInfo dictionaryWarmInfo,
            long weCookie,
            BlockAppender blockAppender,
            boolean weSuccess,
            Optional<WriteDictionary> writeDictionary,
            Optional<LuceneIndexer> luceneIndexer)
    {
        this.warmupElementWriteMetadata = warmupElementWriteMetadata;
        this.recordBufferSize = 0;
        this.recordBufferPos = 0;
        this.warmupElementBuilder = warmupElementBuilder;
        this.writeJuffersWarmUpElement = writeJuffersWarmUpElement;
        this.dictionaryWarmInfo = dictionaryWarmInfo;
        this.weCookie = weCookie;
        this.blockAppender = blockAppender;
        this.weSuccess = weSuccess;
        this.writeDictionary = writeDictionary;
        this.luceneIndexer = luceneIndexer;
        this.isCleanupDone = new AtomicReference<>(null);
        this.warmupElementStats = new WarmupElementStats(0, null, null);
    }

    int getRecordBufferPos()
    {
        return recordBufferPos;
    }

    void setFailed()
    {
        weSuccess = false;
    }

    void setWeClosed()
    {
        weClosed = true;
    }

    DictionaryWarmInfo getDictionaryWarmInfo()
    {
        return dictionaryWarmInfo;
    }

    public long getWeCookie()
    {
        return weCookie;
    }

    int getRecordBufferSize()
    {
        return recordBufferSize;
    }

    boolean isWeClosed()
    {
        return weClosed;
    }

    WarmUpElement.Builder getWarmupElementBuilder()
    {
        return warmupElementBuilder;
    }

    WriteJuffersWarmUpElement getWriteJuffersWarmUpElement()
    {
        return writeJuffersWarmUpElement;
    }

    Optional<LuceneIndexer> getLuceneIndexer()
    {
        return luceneIndexer;
    }

    boolean weSuccess()
    {
        return weSuccess;
    }

    void setWarmupElementBuilder(WarmUpElement.Builder warmupElementBuilder)
    {
        this.warmupElementBuilder = warmupElementBuilder;
    }

    void resetRecords()
    {
        this.recordBufferSize = 0;
        this.recordBufferPos = 0;
    }

    void resetRecordBufferPos()
    {
        recordBufferPos = 0;
    }

    void incRecordBufferPos(int nCurrentRows)
    {
        recordBufferPos += nCurrentRows;
    }

    boolean isRecordBufferFull()
    {
        return (recordBufferSize > 0) && (recordBufferPos >= recordBufferSize);
    }

    int getRemainingBufferSize()
    {
        return recordBufferSize - recordBufferPos;
    }

    void setRecordBufferSize(int newSize)
    {
        this.recordBufferSize = newSize;
    }

    WarmupElementWriteMetadata getWarmupElementWriteMetadata()
    {
        return warmupElementWriteMetadata;
    }

    BlockAppender getBlockAppender()
    {
        return blockAppender;
    }

    public Optional<WriteDictionary> getWriteDictionary()
    {
        return writeDictionary;
    }

    WarmupElementStats getWarmupElementStats()
    {
        return warmupElementStats;
    }

    AtomicReference<Object> getIsCleanupDone()
    {
        return isCleanupDone;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StorageWriterContext that = (StorageWriterContext) o;
        return weClosed == that.weClosed &&
                recordBufferSize == that.recordBufferSize &&
                recordBufferPos == that.recordBufferPos &&
                weSuccess == that.weSuccess &&
                weCookie == that.weCookie &&
                Objects.equals(warmupElementStats, that.warmupElementStats) &&
                Objects.equals(warmupElementWriteMetadata, that.warmupElementWriteMetadata) &&
                Objects.equals(warmupElementBuilder, that.warmupElementBuilder) &&
                Objects.equals(writeJuffersWarmUpElement, that.writeJuffersWarmUpElement) &&
                Objects.equals(dictionaryWarmInfo, that.dictionaryWarmInfo) &&
                Objects.equals(blockAppender, that.blockAppender) &&
                Objects.equals(writeDictionary, that.writeDictionary) &&
                Objects.equals(luceneIndexer, that.luceneIndexer);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                warmupElementStats,
                weClosed,
                warmupElementWriteMetadata,
                recordBufferSize,
                recordBufferPos,
                warmupElementBuilder,
                writeJuffersWarmUpElement,
                dictionaryWarmInfo,
                weCookie,
                blockAppender,
                weSuccess,
                writeDictionary,
                luceneIndexer);
    }

    @Override
    public String toString()
    {
        return "StorageWriterParams{" +
                "isCleanupDone=" + isCleanupDone +
                ", warmupElementStats=" + warmupElementStats +
                ", weClosed=" + weClosed +
                ", warmupElementWriteMetadata=" + warmupElementWriteMetadata +
                ", recordBufferSize=" + recordBufferSize +
                ", recordBufferPos=" + recordBufferPos +
                ", warmupElementBuilder=" + warmupElementBuilder +
                ", writeJuffersWarmUpElement=" + writeJuffersWarmUpElement +
                ", dictionaryWarmInfo=" + dictionaryWarmInfo +
                ", weCookie=" + weCookie +
                ", blockAppender=" + blockAppender +
                ", weSuccess=" + weSuccess +
                ", writeDictionary=" + writeDictionary +
                ", luceneIndexer=" + luceneIndexer +
                '}';
    }
}
