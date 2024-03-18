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

import it.unimi.dsi.fastutil.longs.LongArrayList;

class RangeData
{
    private final long rowsBuffId;
    private int numChunkRowsCollected;
    private int lastChunkIndex;
    private final LongArrayList lowerInclusive;
    private final LongArrayList upperExclusive;

    public RangeData(long rowsBuffId)
    {
        this.rowsBuffId = rowsBuffId;
        // ranges are gathered only if needed (mixed query)
        this.numChunkRowsCollected = 0;
        this.lastChunkIndex = 0;
        this.upperExclusive = new LongArrayList();
        this.lowerInclusive = new LongArrayList();
    }

    public long getRowsBuffId()
    {
        return rowsBuffId;
    }

    public int getLastChunkIndex()
    {
        return lastChunkIndex;
    }

    public int getNumChunkRowsCollected()
    {
        return numChunkRowsCollected;
    }

    public void incNumChunkRowsCollected(int numRows)
    {
        numChunkRowsCollected += numRows;
    }

    public void setLastChunkIndex(int chunkIndex)
    {
        lastChunkIndex = chunkIndex;
    }

    public void resetNumChunkRowsCollected()
    {
        numChunkRowsCollected = 0;
    }

    public void addLowerInclusive(long value)
    {
        this.lowerInclusive.add(value);
    }

    public long removeLowerInclusive(int value)
    {
        return this.lowerInclusive.removeLong(value);
    }

    public long removeUpperExclusive(int value)
    {
        return this.upperExclusive.removeLong(value);
    }

    public void addUpperExclusive(int value)
    {
        this.upperExclusive.add(value);
    }

    public void clearUpperExclusive()
    {
        this.upperExclusive.clear();
    }

    public void clearLowerInclusive()
    {
        this.lowerInclusive.clear();
    }

    public long[] getLowerInclusiveAsArray()
    {
        return this.lowerInclusive.toLongArray();
    }

    public long[] getUpperExclusiveAsArray()
    {
        return this.upperExclusive.toLongArray();
    }

    public int getLowerInclusiveSize()
    {
        return this.lowerInclusive.size();
    }

    public int getUpperExclusiveSize()
    {
        return this.upperExclusive.size();
    }

    public long getUpperExclusiveValue(int index)
    {
        return this.upperExclusive.getLong(index);
    }
}
