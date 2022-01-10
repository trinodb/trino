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
package io.trino.operator.hash;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.trino.operator.GroupByHash;
import io.trino.operator.GroupByIdBlock;
import io.trino.operator.HashGenerator;
import io.trino.operator.InterpretedHashGenerator;
import io.trino.operator.PrecomputedHashGenerator;
import io.trino.operator.UpdateMemory;
import io.trino.operator.Work;
import io.trino.operator.aggregation.builder.InMemoryHashAggregationBuilder;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.util.HashCollisionsEstimator.estimateNumberOfHashCollisions;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class MultiChannelGroupByHashInlineFastBBAllTypes
        implements GroupByHash
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MultiChannelGroupByHashInlineFastBBAllTypes.class).instanceSize();

    private static final float FILL_RATIO = 0.75f;

    private final Optional<Integer> inputHashChannel;
    private final HashTableFactory hashTableFactory;
    private final int[] hashChannels;
    private final int[] hashChannelsWithHash;

    // the hash table with value + groupId entries.
    // external groupId is the position/index in this table and artificial groupId concatenated.
    private NoRehashHashTable hashTable;

    private double expectedHashCollisions;

    // reserve enough memory before rehash
    private final UpdateMemory updateMemory;
    private long preallocatedMemoryInBytes;
    private long currentPageSizeInBytes;

    private final List<Type> types;

    public MultiChannelGroupByHashInlineFastBBAllTypes(
            IsolatedRowExtractorFactory isolatedRowExtractorFactory,
            List<? extends Type> hashTypes,
            int[] hashChannels,
            Optional<Integer> inputHashChannel,
            int expectedSize,
            UpdateMemory updateMemory,
            BlockTypeOperators blockTypeOperators,
            int maxVarWidthBufferSize)
    {
        checkArgument(expectedSize > 0, "expectedSize must be greater than zero");

        checkArgument(hashChannels.length > 0, "hashChannels.length must be at least 1");
        this.hashChannels = hashChannels;
        requireNonNull(inputHashChannel, "inputHashChannel is null");

        if (inputHashChannel.isPresent()) {
            int channel = inputHashChannel.get();
            int mappedChannel = -1;
            for (int i = 0; i < hashChannels.length; i++) {
                if (hashChannels[i] == channel) {
                    mappedChannel = i;
                    break;
                }
            }
            if (mappedChannel == -1) {
                this.inputHashChannel = Optional.of(hashChannels.length);
                this.hashChannelsWithHash = Arrays.copyOf(hashChannels, hashChannels.length + 1);
                // map inputHashChannel to last column
                this.hashChannelsWithHash[hashChannels.length] = channel;
            }
            else {
                // inputHashChannel is one of the hashChannels
                this.inputHashChannel = inputHashChannel;
                this.hashChannelsWithHash = hashChannels;
            }
        }
        else {
            this.inputHashChannel = Optional.empty();
            this.hashChannelsWithHash = hashChannels;
        }

        this.hashTableFactory = new HashTableFactory(
                isolatedRowExtractorFactory.create(hashTypes, maxVarWidthBufferSize),
                hashTypes,
                this.inputHashChannel,
                blockTypeOperators);
        this.hashTable = hashTableFactory.create(expectedSize);

        // This interface is used for actively reserving memory (push model) for rehash.
        // The caller can also query memory usage on this object (pull model)
        this.updateMemory = requireNonNull(updateMemory, "updateMemory is null");
        this.types = inputHashChannel.isPresent() ? ImmutableList.copyOf(Iterables.concat(hashTypes, ImmutableList.of(BIGINT))) : ImmutableList.copyOf(hashTypes);
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                hashTable.getEstimatedSize() +
                preallocatedMemoryInBytes;
    }

    @Override
    public long getHashCollisions()
    {
        return hashTable.getHashCollisions();
    }

    @Override
    public double getExpectedHashCollisions()
    {
        return expectedHashCollisions + estimateNumberOfHashCollisions(getGroupCount(), hashTable.getCapacity());
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public int getGroupCount()
    {
        return hashTable.getSize();
    }

    @Override
    public GroupCursor consecutiveGroups()
    {
        return new GroupCursor()
        {
            private int currentGroupId = -1;

            @Override
            public boolean hasNext()
            {
                return currentGroupId + 1 < hashTable.hashTableSize();
            }

            @Override
            public void next()
            {
                currentGroupId++;
            }

            @Override
            public void appendValuesTo(PageBuilder pageBuilder, int outputChannelOffset)
            {
                MultiChannelGroupByHashInlineFastBBAllTypes.this.appendValuesTo(hashTable, hashTable.groupToHashPosition(currentGroupId), pageBuilder, outputChannelOffset);
            }

            @Override
            public int getGroupId()
            {
                return currentGroupId;
            }

            @Override
            public void evaluatePage(PageBuilder pageBuilder, List<InMemoryHashAggregationBuilder.Aggregator> aggregators)
            {
                List<Type> types = getTypes();
                while (!pageBuilder.isFull() && hasNext()) {
                    next();

                    appendValuesTo(pageBuilder, 0);

                    pageBuilder.declarePosition();
                    for (int i = 0; i < aggregators.size(); i++) {
                        InMemoryHashAggregationBuilder.Aggregator aggregator = aggregators.get(i);
                        BlockBuilder output = pageBuilder.getBlockBuilder(types.size() + i);
                        aggregator.evaluate(getGroupId(), output);
                    }
                }
            }
        };
    }

    @Override
    public GroupCursor hashSortedGroups()
    {
        throw new UnsupportedOperationException();
    }

    public void appendValuesTo(NoRehashHashTable hashTable, int hashPosition, PageBuilder pageBuilder, int outputChannelOffset)
    {
        hashTable.appendValuesTo(hashPosition, pageBuilder, outputChannelOffset, inputHashChannel.isPresent());
    }

    @Override
    public Work<?> addPage(Page page)
    {
        currentPageSizeInBytes = page.getRetainedSizeInBytes();

        return new AddPageWork(page.getColumns(hashChannelsWithHash));
    }

    @Override
    public Work<GroupByIdBlock> getGroupIds(Page page)
    {
        currentPageSizeInBytes = page.getRetainedSizeInBytes();
        return new GetGroupIdsWork(page.getColumns(hashChannelsWithHash));
    }

    @Override
    public boolean contains(int position, Page page, int[] hashChannels)
    {
        throw new UnsupportedOperationException();
    }

    @VisibleForTesting
    @Override
    public int getCapacity()
    {
        return hashTable.getCapacity();
    }

    private boolean tryRehash()
    {
        return tryRehash(hashTable.getCapacity() * 2L);
    }

    private boolean tryRehash(long newCapacityLong)
    {
        if (newCapacityLong > Integer.MAX_VALUE) {
            throw new TrinoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }
        int newCapacity = toIntExact(newCapacityLong);
//        System.out.println("rehash: old: " + hashCapacity + " new: " + newCapacity);
        // An estimate of how much extra memory is needed before we can go ahead and expand the hash table.
        // This includes the new capacity for values, groupIds, and valuesByGroupId as well as the size of the current page
        preallocatedMemoryInBytes = (newCapacity - hashTable.getCapacity()) * (long) (Long.BYTES + Integer.BYTES) + (calculateMaxFill(newCapacity) - hashTable.maxFill()) * Long.BYTES + currentPageSizeInBytes;
        if (!updateMemory.update()) {
            // reserved memory but has exceeded the limit
            return false;
        }
        preallocatedMemoryInBytes = 0;

        expectedHashCollisions += estimateNumberOfHashCollisions(getGroupCount(), hashTable.getCapacity());

//        GroupByHashTableEntries valuesTable = hashTable.valuesTable();
//        GroupByHashTableEntries newValuesTable = valuesTable.extend(valuesTable.capacity() * 2);

        NoRehashHashTable newHashTable = hashTableFactory.create(
                newCapacity,
                hashTable.entries().takeOverflow(),
                Arrays.copyOf(hashTable.groupToHashPosition(), newCapacity),
                null,
                hashTable.getHashCollisions());

        newHashTable.copyFrom(hashTable);

        hashTable.close();
        hashTable = newHashTable;
        return true;
    }

    private static int calculateMaxFill(int hashSize)
    {
        checkArgument(hashSize > 0, "hashSize must be greater than 0");
        int maxFill = (int) Math.ceil(hashSize * FILL_RATIO);
        if (maxFill == hashSize) {
            maxFill--;
        }
        checkArgument(hashSize > maxFill, "hashSize must be larger than maxFill");
        return maxFill;
    }

    private class AddPageWork
            implements Work<Void>
    {
        private final Page page;
        private int lastPosition;

        private AddPageWork(Page page)
        {
            this.page = requireNonNull(page, "page is null");
        }

        @Override
        public boolean process()
        {
            int positionCount = page.getPositionCount();
            checkState(lastPosition < positionCount, "position count out of bound");
            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (hashTable.needRehash() && !tryRehash()) {
                return false;
            }

            NoRehashHashTable hashTable = MultiChannelGroupByHashInlineFastBBAllTypes.this.hashTable;
            while (lastPosition < positionCount && !hashTable.needRehash()) {
                hashTable.putIfAbsent(lastPosition, page);
                lastPosition++;
                if (hashTable.needRehash()) {
                    tryRehash();
                    hashTable = MultiChannelGroupByHashInlineFastBBAllTypes.this.hashTable;
                }
            }
            return lastPosition == positionCount;
        }

        @Override
        public Void getResult()
        {
            throw new UnsupportedOperationException();
        }
    }

    private class GetGroupIdsWork
            implements Work<GroupByIdBlock>
    {
        private final BlockBuilder blockBuilder;
        private final Page page;
        private boolean finished;
        private int lastPosition;

        private GetGroupIdsWork(Page page)
        {
            this.page = requireNonNull(page, "page is null");
            // we know the exact size required for the block
            this.blockBuilder = BIGINT.createFixedSizeBlockBuilder(page.getPositionCount());
        }

        @Override
        public boolean process()
        {
            int positionCount = page.getPositionCount();
            checkState(lastPosition < positionCount, "position count out of bound");
            checkState(!finished);

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (hashTable.needRehash() && !tryRehash()) {
                return false;
            }

            NoRehashHashTable hashTable = MultiChannelGroupByHashInlineFastBBAllTypes.this.hashTable;
            while (lastPosition < positionCount && !hashTable.needRehash()) {
                // output the group id for this row
                int groupId = hashTable.putIfAbsent(lastPosition, page);
                BIGINT.writeLong(blockBuilder, groupId);
                lastPosition++;
                if (hashTable.needRehash()) {
                    tryRehash();
                    hashTable = MultiChannelGroupByHashInlineFastBBAllTypes.this.hashTable;
                }
            }
            return lastPosition == positionCount;
        }

        @Override
        public GroupByIdBlock getResult()
        {
            checkState(lastPosition == page.getPositionCount(), "process has not yet finished");
            checkState(!finished, "result has produced");
            finished = true;
            return new GroupByIdBlock(getGroupCount(), blockBuilder.build());
        }
    }

    static class HashTableFactory
    {
        private final int dataValuesLength;
        private final int hashChannelsCount;
        private final RowExtractor rowExtractor;
        private final HashGenerator hashGenerator;

        public HashTableFactory(
                RowExtractor rowExtractor,
                List<? extends Type> hashTypes,
                Optional<Integer> inputHashChannel,
                BlockTypeOperators blockTypeOperators)
        {
            this.hashChannelsCount = hashTypes.size();
            this.rowExtractor = rowExtractor;
            this.dataValuesLength = rowExtractor.mainBufferValuesLength();
            this.hashGenerator = inputHashChannel.isPresent() ?
                    new PrecomputedHashGenerator(inputHashChannel.get()) :
                    InterpretedHashGenerator.createPositionalWithTypes(ImmutableList.copyOf(hashTypes), blockTypeOperators);
        }

        public NoRehashHashTable create(int expectedSize)
        {
            int hashCapacity = arraySize(expectedSize, MultiChannelGroupByHashInlineFastBBAllTypes.FILL_RATIO);
            return create(
                    hashCapacity,
                    FastByteBuffer.allocate(128 * 1024),
                    new int[hashCapacity],
//                    rowExtractor.allocateHashTableEntries(hashChannelsCount, expectedSize, FastByteBuffer.allocate(128 * 1024), dataValuesLength),
                    null,
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
                    rowExtractor,
                    hashChannelsCount,
                    totalEntryCount,
                    overflow,
                    valuesTable,
                    groupToHashPosition,
                    hashCollisions,
                    dataValuesLength);
        }
    }

    // does not copy to rowBuffer in putIfAbsent
    static class NoRowBufferNoRehashHashTable
            implements NoRehashHashTable
    {
        private final HashGenerator hashGenerator;
        private final int hashCapacity;
        private final int maxFill;
        private final int mask;
        private final int[] groupToHashPosition;

        private int hashTableSize;
        private long hashCollisions;
        private final RowExtractor rowExtractor;
        private final GroupByHashTableEntries entries;
        private final GroupByHashTableEntries valuesTable;

        public NoRowBufferNoRehashHashTable(
                HashGenerator hashGenerator,
                RowExtractor rowExtractor,
                int hashChannelsCount,
                int hashCapacity,
                FastByteBuffer overflow,
                GroupByHashTableEntries valuesTable,
                int[] groupToHashPosition,
                long hashCollisions,
                int dataValuesLength)
        {
            this.hashGenerator = hashGenerator;
            this.rowExtractor = rowExtractor;
            this.hashCapacity = hashCapacity;
            this.groupToHashPosition = groupToHashPosition;

            this.maxFill = calculateMaxFill(hashCapacity);
            this.mask = hashCapacity - 1;
            this.entries = rowExtractor.allocateHashTableEntries(hashChannelsCount, hashCapacity, overflow, dataValuesLength);
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
                    rowExtractor.putEntry(entries, hashPosition, groupId, page, position, rawHash);
//                    valuesTable.copyEntryFrom(entries, hashPosition, groupId * valuesTable.getEntrySize());
                    groupToHashPosition[groupId] = hashPosition;
//                System.out.println(hashPosition + ": v=" + value + ", " + hashTableSize);
                    return groupId;
                }

                if (rowExtractor.keyEquals(entries, hashPosition, page, position, rawHash)) {
                    return current;
                }

                hashPosition = hashPosition + entries.getEntrySize();
                if (hashPosition >= entries.capacity()) {
                    hashPosition = 0;
                }
                hashCollisions++;
            }
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

        public int getCapacity()
        {
            return hashCapacity;
        }

        public long getEstimatedSize()
        {
            return entries.getEstimatedSize() + sizeOf(groupToHashPosition);
        }

        @Override
        public void close()
        {
            entries.close();
        }

        public void copyFrom(NoRehashHashTable other)
        {
            GroupByHashTableEntries otherHashTable = other.entries();
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
            rowExtractor.appendValuesTo(entries, hashPosition, pageBuilder, outputChannelOffset, outputHash);
        }

        private int getHashPosition(long rawHash)
        {
            return (int) (murmurHash3(rawHash) & mask) * entries.getEntrySize();
        }

        public long getHash(int startPosition)
        {
            return entries.getHash(startPosition + Integer.BYTES);
        }

        public long getHashCollisions()
        {
            return hashCollisions;
        }

        public int isNull(int hashPosition, int index)
        {
            return entries.isNull(hashPosition, index);
        }

        @Override
        public GroupByHashTableEntries entries()
        {
            return entries;
        }

        @Override
        public GroupByHashTableEntries valuesTable()
        {
            return valuesTable;
        }

        @Override
        public int hashTableSize()
        {
            return hashTableSize;
        }

        @Override
        public int groupToHashPosition(int groupId)
        {
            return groupToHashPosition[groupId];
        }

        @Override
        public int[] groupToHashPosition()
        {
            return groupToHashPosition;
        }
    }

    static class RowBufferNoRehashHashTable
            implements NoRehashHashTable
    {
        private final int hashCapacity;
        private final int maxFill;
        private final int mask;
        private final int[] groupToHashPosition;

        private int hashTableSize;
        private long hashCollisions;
        private final RowExtractor rowExtractor;
        private final GroupByHashTableEntries rowBuffer;
        private final GroupByHashTableEntries entries;

        public RowBufferNoRehashHashTable(
                RowExtractor rowExtractor,
                int hashChannelsCount,
                int hashCapacity,
                FastByteBuffer overflow,
                int[] groupToHashPosition,
                long hashCollisions,
                int dataValuesLength)
        {
            this.rowExtractor = rowExtractor;
            this.rowBuffer = rowExtractor.allocateRowBuffer(hashChannelsCount, dataValuesLength);
            this.hashCapacity = hashCapacity;
            this.groupToHashPosition = groupToHashPosition;

            this.maxFill = calculateMaxFill(hashCapacity);
            this.mask = hashCapacity - 1;
            this.entries = rowExtractor.allocateHashTableEntries(hashChannelsCount, hashCapacity, overflow, dataValuesLength);
            this.hashCollisions = hashCollisions;
        }

        @Override
        public int putIfAbsent(int position, Page page)
        {
            rowExtractor.copyToEntriesTable(page, position, rowBuffer, 0);

            int hashPosition = getHashPosition(rowBuffer, 0);
            // look for an empty slot or a slot containing this key
            while (true) {
                int current = entries.getGroupId(hashPosition);

                if (current == -1) {
                    // empty slot found
                    int groupId = hashTableSize++;
                    entries.putEntry(hashPosition, groupId, rowBuffer);
                    groupToHashPosition[groupId] = hashPosition;
//                System.out.println(hashPosition + ": v=" + value + ", " + hashTableSize);
                    return groupId;
                }

                if (entries.keyEquals(hashPosition, rowBuffer, 0)) {
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
        public int getCapacity()
        {
            return hashCapacity;
        }

        @Override
        public long getEstimatedSize()
        {
            return entries.getEstimatedSize() + sizeOf(groupToHashPosition);
        }

        @Override
        public void close()
        {
            try {
                entries.close();
            }
            finally {
                rowBuffer.close();
            }
        }

        @Override
        public void copyFrom(NoRehashHashTable other)
        {
            GroupByHashTableEntries otherHashTable = other.entries();
            GroupByHashTableEntries thisHashTable = this.entries;
            int entrySize = entries.getEntrySize();
            for (int i = 0; i <= otherHashTable.capacity() - entrySize; i += entrySize) {
                if (otherHashTable.getGroupId(i) != -1) {
                    int hashPosition = getHashPosition(otherHashTable, i);
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

        @Override
        public void appendValuesTo(int hashPosition, PageBuilder pageBuilder, int outputChannelOffset, boolean outputHash)
        {
            rowExtractor.appendValuesTo(entries, hashPosition, pageBuilder, outputChannelOffset, outputHash);
        }

        private int getHashPosition(GroupByHashTableEntries row, int position)
        {
            long hash = murmurHash3(row.getHash(position));

            return (int) (hash & mask) * entries.getEntrySize();
        }

        @Override
        public long getHash(int startPosition)
        {
            return entries.getHash(startPosition + Integer.BYTES);
        }

        @Override
        public long getHashCollisions()
        {
            return hashCollisions;
        }

        @Override
        public int isNull(int hashPosition, int index)
        {
            return entries.isNull(hashPosition, index);
        }

        @Override
        public GroupByHashTableEntries entries()
        {
            return entries;
        }

        @Override
        public GroupByHashTableEntries valuesTable()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int hashTableSize()
        {
            return hashTableSize;
        }

        @Override
        public int groupToHashPosition(int groupId)
        {
            return groupToHashPosition[groupId];
        }

        @Override
        public int[] groupToHashPosition()
        {
            return groupToHashPosition;
        }
    }

    @Override
    public void close()
    {
        hashTable.close();
    }
}
