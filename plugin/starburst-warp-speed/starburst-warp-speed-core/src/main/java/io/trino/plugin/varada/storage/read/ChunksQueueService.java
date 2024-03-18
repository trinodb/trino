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
package io.trino.plugin.varada.storage.read;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.storage.engine.StorageEngine;

@Singleton
public class ChunksQueueService
{
    private static final Logger logger = Logger.get(ChunksQueueService.class);

    private final StorageEngine storageEngine;

    @Inject
    public ChunksQueueService(StorageEngine storageEngine)
    {
        this.storageEngine = storageEngine;
    }

    int getChunkIndexForMatch(ChunksQueue chunksQueue)
    {
        return chunksQueue.getTotalNumChunks();
    }

    void updateChunkRangeAfterMatch(ChunksQueue chunksQueue, int endChunkIndex, int numMatchedChunks, short[] matchedChunksIndexes, int[] matchBitmapResetPoints)
    {
        chunksQueue.add(endChunkIndex, numMatchedChunks, matchedChunksIndexes, matchBitmapResetPoints);
    }

    // return true is completely finished, false otherwise
    boolean updateChunkRangeFullScan(ChunksQueue chunksQueue, int numChunks, int numChunksInRange)
    {
        if (isCompletelyFinished(chunksQueue, numChunks)) {
            return true;
        }
        int startChunkIndex = chunksQueue.getTotalNumChunks();
        chunksQueue.add(startChunkIndex, Math.min(startChunkIndex + numChunksInRange, numChunks));
        return false;
    }

    boolean isChunkRangeCompleted(ChunksQueue chunksQueue)
    {
        return chunksQueue.isEmpty();
    }

    boolean storeRestoreRequired(ChunksQueue chunksQueue)
    {
        return !isChunkRangeCompleted(chunksQueue);
    }

    boolean isCompletelyFinished(ChunksQueue chunksQueue, int numChunks)
    {
        return isChunkRangeCompleted(chunksQueue) && (chunksQueue.getTotalNumChunks() >= numChunks);
    }

    // returns if buffer if exhausted and we need to stop collecting
    boolean prepareNextChunk(ChunksQueue chunksQueue,
            boolean chunkPrepared,
            int collectTxId,
            int rowsLimit,
            int numCollectedRows,
            int[] outResultType)
    {
        boolean bufferIsFull = false;
        if (!chunkPrepared) {
            int chunkIndex = chunksQueue.getCurrent();
            int bitmapResetPoint = chunksQueue.getCurrentResetPoint();
            logger.debug("collectFromStorage process match chunkIndex %d bitmapResetPoint %d rowsLimit %d numCollectedRows %d",
                    chunkIndex, bitmapResetPoint, rowsLimit, numCollectedRows);
            int ret = storageEngine.processMatchResult(collectTxId,
                    chunksQueue.getCurrent(),
                    chunksQueue.getCurrentResetPoint(),
                    rowsLimit - numCollectedRows,
                    outResultType);
            if (ret == -1) {
                throw new RuntimeException(String.format("prepareNextChunk failed unexpectedly collectTxId %d chunkIx %d resetPoint %d numCollectedRows %d rowsLimit %d",
                        collectTxId, chunksQueue.getCurrent(), chunksQueue.getCurrentResetPoint(), numCollectedRows, rowsLimit));
            }
            bufferIsFull = (ret > 0);
        }
        return bufferIsFull;
    }
}
