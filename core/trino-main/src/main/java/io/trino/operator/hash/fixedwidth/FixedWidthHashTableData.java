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
package io.trino.operator.hash.fixedwidth;

import io.trino.operator.hash.HashTableData;

import static io.trino.operator.hash.HashTableDataGroupByHash.calculateMaxFill;

public abstract class FixedWidthHashTableData
        implements HashTableData
{
    private final int hashCapacity;
    private final int maxFill;

    protected final int hashChannelsCount;
    protected final int mask;
    protected final int valuesLength;
    protected final FixedWidthGroupByHashTableEntries entries;
    protected final int entrySize;

    protected int hashTableSize;
    protected long hashCollisions;

    public FixedWidthHashTableData(int hashChannelsCount, int hashCapacity, int valuesLength, long hashCollisions, FixedWidthGroupByHashTableEntries entries)
    {
        this.hashChannelsCount = hashChannelsCount;
        this.hashCapacity = hashCapacity;
        this.valuesLength = valuesLength;
        this.hashCollisions = hashCollisions;
        this.maxFill = calculateMaxFill(hashCapacity);
        this.mask = hashCapacity - 1;
        this.entries = entries;
        this.entrySize = entries.getEntrySize();
    }

    public int getHashTableSize()
    {
        return hashTableSize;
    }

    @Override
    public boolean needRehash()
    {
        return hashTableSize >= maxFill;
    }

    public int maxFill()
    {
        return maxFill;
    }

    @Override
    public FixedWidthGroupByHashTableEntries entries()
    {
        return entries;
    }

    @Override
    public void markCollision()
    {
        hashCollisions++;
    }

    public long getHashCollisions()
    {
        return hashCollisions;
    }

    public long getRawHash(int groupId)
    {
        return entries.getHash(getPosition(groupId));
    }

    public int getCapacity()
    {
        return hashCapacity;
    }
}
