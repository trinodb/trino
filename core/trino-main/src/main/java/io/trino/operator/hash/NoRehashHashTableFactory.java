package io.trino.operator.hash;

import io.trino.operator.HashGenerator;
import io.trino.operator.hash.fastbb.FastByteBuffer;
import io.trino.spi.type.Type;

import java.util.List;

import static it.unimi.dsi.fastutil.HashCommon.arraySize;

public class NoRehashHashTableFactory
{
    private final int hashChannelsCount;
    private final HashTableRowFormat rowFormat;
    private final HashGenerator hashGenerator;

    public NoRehashHashTableFactory(
            List<? extends Type> hashTypes,
            HashTableRowFormat rowFormat,
            HashGenerator hashGenerator)
    {
        this.hashChannelsCount = hashTypes.size();
        this.rowFormat = rowFormat;
        this.hashGenerator = hashGenerator;
    }

    public NoRehashHashTable create(int expectedSize)
    {
        int hashCapacity = arraySize(expectedSize, MultiChannelGroupByHashRowWise.FILL_RATIO);
        return create(
                hashCapacity,
                FastByteBuffer.allocate(128 * 1024),
                new int[hashCapacity],
                rowFormat.allocateHashTableEntries(hashChannelsCount, MultiChannelGroupByHashRowWise.calculateMaxFill(hashCapacity), FastByteBuffer.allocate(128 * 1024)),
                0);
    }

    public NoRehashHashTable create(
            int totalEntryCount,
            FastByteBuffer overflow,
            int[] groupToHashPosition,
            GroupByHashTableEntries valuesTable,
            long hashCollisions)
    {
        return new NoRowBufferNoRehashHashTable(
                hashGenerator,
                rowFormat,
                hashChannelsCount,
                totalEntryCount,
                overflow,
                valuesTable,
                groupToHashPosition,
                hashCollisions);
    }
}
