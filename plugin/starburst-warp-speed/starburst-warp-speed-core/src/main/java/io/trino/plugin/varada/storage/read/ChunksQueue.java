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

import io.trino.spi.TrinoException;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.varada.VaradaErrorCode.VARADA_UNRECOVERABLE_MATCH_FAILED;

class ChunksQueue
{
    private final int allSet;
    private final Deque<MatchChunkResult> chunksToCollect;
    private int totalNumChunks; // in case queue is empty we return the total number of chunks

    ChunksQueue(int maxChunks, int pageSize)
    {
        this.allSet = pageSize * Byte.SIZE; // constant used for setting full bitmaps in full scan case
        this.chunksToCollect = new ArrayDeque<>(maxChunks);
    }

    // add more chunks to collect and update total number of chunks
    void add(int totalNumChunks, int numMatchedChunks, short[] matchedChunksIndexes, int[] matchBitmapResetPoints)
    {
        for (int i = 0; i < numMatchedChunks; i++) {
            if (matchBitmapResetPoints[i] < 0) {
                throw new TrinoException(VARADA_UNRECOVERABLE_MATCH_FAILED, "match got to error state at chunk " + matchedChunksIndexes[i]);
            }
            chunksToCollect.add(new MatchChunkResult((int) matchedChunksIndexes[i], matchBitmapResetPoints[i]));
        }
        this.totalNumChunks = totalNumChunks;
    }

    // add a range of chunks to collect and set total number of chunks as the end of the range
    void add(int startChunkIndex, int endChunkIndex)
    {
        for (int chunkIndex = startChunkIndex; chunkIndex < endChunkIndex; chunkIndex++) {
            chunksToCollect.add(new MatchChunkResult(chunkIndex, allSet));
        }
        this.totalNumChunks = endChunkIndex;
    }

    // get the current chunk to collect
    int getCurrent()
    {
        return chunksToCollect.getFirst().chunkIndex();
    }

    // get the current chunk match bitmap reset point
    int getCurrentResetPoint()
    {
        return chunksToCollect.getFirst().bitmapResetPoint();
    }

    Optional<int[]> getChunkIndexesWithBitmap()
    {
        if (isEmpty()) {
            return Optional.empty();
        }
        Iterator<MatchChunkResult> itr = chunksToCollect.iterator();
        List<Integer> chunksRemaining = new ArrayList<>(chunksToCollect.size());
        while (itr.hasNext()) {
            MatchChunkResult matchChunkResult = itr.next();
            // value larger than allSet means the reset point is not valid and the bitmap is used
            if (matchChunkResult.bitmapResetPoint() > allSet) {
                chunksRemaining.add(matchChunkResult.chunkIndex());
            }
        }
        return (chunksRemaining.size() > 0) ? Optional.of(chunksRemaining.stream().mapToInt(x -> x).toArray()) : Optional.empty();
    }

    // get total number of chunks
    int getTotalNumChunks()
    {
        return totalNumChunks;
    }

    // advance to the next chunk to collect
    void currentCompleted()
    {
        chunksToCollect.poll();
    }

    // are there chunks to collect
    boolean isEmpty()
    {
        return chunksToCollect.isEmpty();
    }
}
