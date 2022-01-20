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

import java.util.Arrays;

import static io.airlift.slice.SizeOf.sizeOf;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;

public class SingleTableHashTableData
        extends FixedWidthHashTableData
{
    private final int[] groupToHashPosition;

    public SingleTableHashTableData(int hashChannelsCount, int hashCapacity, int valuesLength)
    {
        this(hashChannelsCount, hashCapacity, valuesLength, 0, new int[0]);
    }

    public SingleTableHashTableData(int hashChannelsCount, int hashCapacity, int valuesLength, long hashCollisions, int[] groupToHashPosition)
    {
        super(hashChannelsCount, hashCapacity, valuesLength, hashCollisions, FixedWidthGroupByHashTableEntries.allocate(hashCapacity, hashChannelsCount, valuesLength));
        this.groupToHashPosition = groupToHashPosition.length >= maxFill() ? groupToHashPosition : Arrays.copyOf(groupToHashPosition, maxFill());
    }

    private void copyFrom(SingleTableHashTableData other)
    {
        FixedWidthGroupByHashTableEntries otherHashTable = other.entries;
        FixedWidthGroupByHashTableEntries thisHashTable = this.entries;
        int entrySize = entries.getEntrySize();
        for (int i = 0; i <= otherHashTable.capacity() - entrySize; i += entrySize) {
            if (otherHashTable.getGroupId(i) != -1) {
                int hashPosition = getHashPosition(otherHashTable.getHash(i));
                // look for an empty slot or a slot containing this key
                while (thisHashTable.getGroupId(hashPosition) != -1) {
                    hashPosition = hashPosition + entrySize;
                    if (hashPosition >= thisHashTable.capacity()) {
                        hashPosition = 0;
                    }
                    hashCollisions++;
                }
                thisHashTable.copyEntryFrom(otherHashTable, i, hashPosition);
                groupToHashPosition[otherHashTable.getGroupId(i)] = hashPosition;
            }
        }
        hashTableSize += other.getHashTableSize();
    }

    @Override
    public int getPosition(int groupId)
    {
        return groupToHashPosition[groupId];
    }

    @Override
    public int getHashPosition(long rawHash)
    {
        return (int) (murmurHash3(rawHash) & mask) * entries.getEntrySize();
    }

    @Override
    public int getGroupId(int hashPosition)
    {
        return entries.getGroupId(hashPosition);
    }

    @Override
    public int addNewGroupId(int hashPosition)
    {
        int groupId = hashTableSize++;
        groupToHashPosition[groupId] = hashPosition;
        return groupId;
    }

    @Override
    public int nextPosition(int hashPosition)
    {
        hashPosition = hashPosition + entrySize;
        if (hashPosition >= entries.capacity()) {
            hashPosition = 0;
        }
        return hashPosition;
    }

    public SingleTableHashTableData resize(int newCapacity)
    {
        SingleTableHashTableData newHashTableData = new SingleTableHashTableData(
                hashChannelsCount,
                newCapacity,
                valuesLength, hashCollisions,
                groupToHashPosition);

        newHashTableData.copyFrom(this);
        return newHashTableData;
    }

    @Override
    public int entriesPosition(int hashPosition, int groupId)
    {
        return hashPosition;
    }

    public long getEstimatedSize()
    {
        return entries.getEstimatedSize() + sizeOf(groupToHashPosition);
    }
}
