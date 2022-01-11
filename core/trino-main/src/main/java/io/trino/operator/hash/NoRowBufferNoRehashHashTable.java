package io.trino.operator.hash;

import io.trino.operator.HashGenerator;
import io.trino.operator.hash.fastbb.FastByteBuffer;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.sizeOf;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;

// does not copy to row Buffer in putIfAbsent
class NoRowBufferNoRehashHashTable
        implements NoRehashHashTable
{
    private final HashGenerator hashGenerator;
    private final int hashChannelsCount;
    private final int hashCapacity;
    private final int maxFill;
    private final int mask;
    private final int[] groupToHashPosition;

    private int hashTableSize;
    private long hashCollisions;
    private final HashTableRowFormat rowFormat;
    private final GroupByHashTableEntries entries;
    private final GroupByHashTableEntries valuesTable;

    public NoRowBufferNoRehashHashTable(
            HashGenerator hashGenerator,
            HashTableRowFormat rowFormat,
            int hashChannelsCount,
            int hashCapacity,
            FastByteBuffer overflow,
            GroupByHashTableEntries valuesTable,
            int[] groupToHashPosition,
            long hashCollisions)
    {
        this.hashGenerator = hashGenerator;
        this.rowFormat = rowFormat;
        this.hashChannelsCount = hashChannelsCount;
        this.hashCapacity = hashCapacity;
        this.groupToHashPosition = groupToHashPosition;

        this.maxFill = MultiChannelGroupByHashRowWise.calculateMaxFill(hashCapacity);
        checkArgument(maxFill <= valuesTable.maxEntryCount());
        this.mask = hashCapacity - 1;
        this.entries = rowFormat.allocateHashTableEntries(hashChannelsCount, hashCapacity, overflow);
        this.valuesTable = valuesTable;
        this.hashCollisions = hashCollisions;
    }

    public int putIfAbsent(int position, Page page)
    {
        long rawHash = hashGenerator.hashPosition(position, page);
        int hashPosition = getHashPosition(rawHash);
        // look for an empty slot or a slot containing this key
        while (true) {
            int current = entries.getGroupId(hashPosition);

            if (current == -1) {
                // empty slot found
                int groupId = hashTableSize++;
                rowFormat.putEntry(entries, hashPosition, groupId, page, position, rawHash);
                valuesTable.copyEntryFrom(entries, hashPosition, groupId * valuesTable.getEntrySize());
                groupToHashPosition[groupId] = hashPosition;
                return groupId;
            }

            if (rowFormat.keyEquals(entries, hashPosition, page, position, rawHash)) {
                return current;
            }

            hashPosition = hashPosition + entries.getEntrySize();
            if (hashPosition >= entries.capacity()) {
                hashPosition = 0;
            }
            hashCollisions++;
        }
    }

    @Override
    public boolean contains(int position, Page page)
    {
        long rawHash = hashGenerator.hashPosition(position, page);
        return contains(position, page, rawHash);
    }

    @Override
    public boolean contains(int position, Page page, long rawHash)
    {
        int hashPosition = getHashPosition(rawHash);
        // look for an empty slot or a slot containing this key
        while (entries.getGroupId(hashPosition) != -1) {
            if (rowFormat.keyEquals(entries, hashPosition, page, position, rawHash)) {
                // found an existing slot for this key
                return true;
            }
            hashPosition = hashPosition + entries.getEntrySize();
            if (hashPosition >= entries.capacity()) {
                hashPosition = 0;
            }
        }

        return false;
    }

    public int getSize()
    {
        return hashTableSize;
    }

    public boolean needRehash()
    {
        return hashTableSize >= maxFill;
    }

    @Override
    public int maxFill()
    {
        return maxFill;
    }

    @Override
    public NoRehashHashTable resize(int newCapacity)
    {
        GroupByHashTableEntries newValuesTable = valuesTable.extend(MultiChannelGroupByHashRowWise.calculateMaxFill(newCapacity));

        NoRowBufferNoRehashHashTable newHashTable = new NoRowBufferNoRehashHashTable(
                hashGenerator,
                rowFormat,
                hashChannelsCount,
                newCapacity,
                entries.takeOverflow(),
                newValuesTable,
                Arrays.copyOf(groupToHashPosition, newCapacity),
                hashCollisions);

        newHashTable.copyFrom(this);
        return newHashTable;
    }

    public int getCapacity()
    {
        return hashCapacity;
    }

    public long getEstimatedSize()
    {
        return entries.getEstimatedSize() + +valuesTable.getEstimatedSize() + sizeOf(groupToHashPosition);
    }

    public void copyFrom(NoRehashHashTable table)
    {
        NoRowBufferNoRehashHashTable other = (NoRowBufferNoRehashHashTable) table;
        GroupByHashTableEntries otherHashTable = other.entries;
        GroupByHashTableEntries thisHashTable = this.entries;
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
                // we can just copy data because overflow is reused
                thisHashTable.copyEntryFrom(otherHashTable, i, hashPosition);
                groupToHashPosition[otherHashTable.getGroupId(i)] = hashPosition;
            }
        }
        hashTableSize += other.getSize();
    }

    public void appendValuesTo(int hashPosition, PageBuilder pageBuilder, int outputChannelOffset, boolean outputHash)
    {
        rowFormat.appendValuesTo(valuesTable, hashPosition * valuesTable.getEntrySize(), pageBuilder, outputChannelOffset, outputHash);
    }

    @Override
    public long getRawHash(int groupId)
    {
        return valuesTable.getHash(groupId * valuesTable.getEntrySize());
    }

    private int getHashPosition(long rawHash)
    {
        return (int) (murmurHash3(rawHash) & mask) * entries.getEntrySize();
    }

    public long getHashCollisions()
    {
        return hashCollisions;
    }

    public int isNull(int hashPosition, int index)
    {
        return entries.isNull(hashPosition, index);
    }
}
