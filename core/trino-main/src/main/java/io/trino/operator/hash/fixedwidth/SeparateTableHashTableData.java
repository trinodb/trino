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
import static io.trino.operator.hash.HashTableDataGroupByHash.calculateMaxFill;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;

public class SeparateTableHashTableData
        extends FixedWidthHashTableData
{
    // hash table that keeps group ids
    private final int[] hashTable;

    public SeparateTableHashTableData(int hashChannelsCount, int hashCapacity, int valuesLength)
    {
        this(hashChannelsCount, hashCapacity, valuesLength, 0);
    }

    public SeparateTableHashTableData(int hashChannelsCount, int hashCapacity, int valuesLength, long hashCollisions)
    {
        super(hashChannelsCount, hashCapacity, valuesLength, hashCollisions, FixedWidthGroupByHashTableEntries.allocate(calculateMaxFill(hashCapacity), hashChannelsCount, valuesLength));
        this.hashTable = new int[hashCapacity];
        Arrays.fill(hashTable, -1);
    }

    private void copyFrom(SeparateTableHashTableData other)
    {
        FixedWidthGroupByHashTableEntries otherEntries = other.entries();
        FixedWidthGroupByHashTableEntries thisEntries = this.entries();
        int[] otherHashTable = other.hashTable;
        int[] thisHashTable = this.hashTable;
        for (int i = 0; i < otherHashTable.length; i++) {
            int groupId = otherHashTable[i];
            if (groupId != -1) {
                int entriesPosition = groupId * otherEntries.getEntrySize();
                int hashPosition = getHashPosition(otherEntries.getHash(entriesPosition));
                // look for an empty slot or a slot containing this key
                while (thisHashTable[hashPosition] != -1) {
                    hashPosition = nextPosition(hashPosition);
                    hashCollisions++;
                }
                thisHashTable[hashPosition] = groupId;
            }
        }
        // TODO lysy: we dont need to copy this (can chain arrays like in BigArray). Not sure if it's better though (we need to pay for BigInt indirection on every access).
        thisEntries.getBuffer().copyFrom(otherEntries.getBuffer(), 0, 0, other.getHashTableSize() * entrySize);

        hashTableSize += other.getHashTableSize();
    }

    @Override
    public int getPosition(int groupId)
    {
        return groupId * entrySize;
    }

    @Override
    public int getHashPosition(long rawHash)
    {
        return (int) (murmurHash3(rawHash) & mask);
    }

    @Override
    public int getGroupId(int hashPosition)
    {
        return hashTable[hashPosition];
    }

    @Override
    public int addNewGroupId(int hashPosition)
    {
        int groupId = hashTableSize++;
        hashTable[hashPosition] = groupId;
        return groupId;
    }

    @Override
    public int nextPosition(int hashPosition)
    {
        return (hashPosition + 1) & mask;
    }

    public SeparateTableHashTableData resize(int newCapacity)
    {
        SeparateTableHashTableData newHashTableData = new SeparateTableHashTableData(
                hashChannelsCount,
                newCapacity,
                valuesLength,
                getHashCollisions());

        newHashTableData.copyFrom(this);
        return newHashTableData;
    }

    @Override
    public int entriesPosition(int hashPosition, int groupId)
    {
        return getPosition(groupId);
    }

    public long getEstimatedSize()
    {
        return entries.getEstimatedSize() + sizeOf(hashTable);
    }
}
