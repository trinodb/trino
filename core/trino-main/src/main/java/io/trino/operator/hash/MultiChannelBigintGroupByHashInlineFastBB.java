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
import io.trino.operator.GroupByHash;
import io.trino.operator.GroupByIdBlock;
import io.trino.operator.UpdateMemory;
import io.trino.operator.Work;
import io.trino.operator.aggregation.builder.InMemoryHashAggregationBuilder;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.HashCommon;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.Collections;
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

public class MultiChannelBigintGroupByHashInlineFastBB
        implements GroupByHash
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MultiChannelBigintGroupByHashInlineFastBB.class).instanceSize();

    private static final float FILL_RATIO = 0.75f;

    private final int[] hashChannels;
    private final Optional<Integer> inputHashChannel;

    // the hash table with value + groupId entries.
    // external groupId is the position/index in this table and artificial groupId concatenated.
    private NoRehashHashTable hashTable;

    private double expectedHashCollisions;

    // reserve enough memory before rehash
    private final UpdateMemory updateMemory;
    private long preallocatedMemoryInBytes;
    private long currentPageSizeInBytes;

    private final List<Type> types;

    public MultiChannelBigintGroupByHashInlineFastBB(
            int[] hashChannels,
            Optional<Integer> inputHashChannel,
            int expectedSize,
            UpdateMemory updateMemory)
    {
        checkArgument(expectedSize > 0, "expectedSize must be greater than zero");

        this.hashChannels = requireNonNull(hashChannels, "hashChannels is null");
        checkArgument(hashChannels.length > 0, "hashChannels.length must be at least 1");
        this.inputHashChannel = requireNonNull(inputHashChannel, "inputHashChannel is null");
        this.hashTable = NoRehashHashTable.create(hashChannels.length, expectedSize);

        // This interface is used for actively reserving memory (push model) for rehash.
        // The caller can also query memory usage on this object (pull model)
        this.updateMemory = requireNonNull(updateMemory, "updateMemory is null");
        this.types = Collections.nCopies(hashChannels.length + (inputHashChannel.isPresent() ? 1 : 0), BIGINT);
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
                return currentGroupId + 1 < hashTable.hashTableSize;
            }

            @Override
            public void next()
            {
                currentGroupId++;
            }

            @Override
            public void appendValuesTo(PageBuilder pageBuilder, int outputChannelOffset)
            {
                MultiChannelBigintGroupByHashInlineFastBB.this.appendValuesTo(hashTable, hashTable.groupToHashPosition[currentGroupId], pageBuilder, outputChannelOffset);
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

    public void appendValuesTo(int hashPosition, PageBuilder pageBuilder, int outputChannelOffset)
    {
        appendValuesTo(hashTable, hashPosition, pageBuilder, outputChannelOffset);
    }

    public void appendValuesTo(NoRehashHashTable hashTable, int hashPosition, PageBuilder pageBuilder, int outputChannelOffset)
    {
        for (int i = 0; i < hashChannels.length; i++) {
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset);
            if (hashTable.isNull(hashPosition, i) == 1) {
                blockBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(blockBuilder, hashTable.getValue(hashPosition, i));
            }
            outputChannelOffset++;
        }

        if (inputHashChannel.isPresent()) {
            BlockBuilder hashBlockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset);
            BIGINT.writeLong(hashBlockBuilder, hashTable.getHash(hashPosition + 1));
        }
    }

    @Override
    public Work<?> addPage(Page page)
    {
        currentPageSizeInBytes = page.getRetainedSizeInBytes();

        return new AddPageWork(getBlocks(page));
    }

    private Block[] getBlocks(Page page)
    {
        Block[] blocks = new Block[hashChannels.length];
        for (int i = 0; i < hashChannels.length; i++) {
            Block block = page.getBlock(hashChannels[i]);
            if (block instanceof RunLengthEncodedBlock) {
                throw new UnsupportedOperationException();
            }
            if (block instanceof DictionaryBlock) {
                throw new UnsupportedOperationException();
            }
            blocks[i] = block;
        }
        return blocks;
    }

    @Override
    public Work<GroupByIdBlock> getGroupIds(Page page)
    {
        currentPageSizeInBytes = page.getRetainedSizeInBytes();
        return new GetGroupIdsWork(getBlocks(page));
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

    private boolean valueEqualsBuffer(int hashPosition, FastByteBuffer hashTable, int hashChannelsLength, FastByteBuffer valuesBuffer, int valuesBufferIsNullOffset, int isNullOffset)
    {
        return hashTable.subArrayEquals(valuesBuffer, hashPosition + Integer.BYTES, 0, valuesBufferIsNullOffset);
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
        preallocatedMemoryInBytes = (newCapacity - hashTable.getCapacity()) * (long) (Long.BYTES + Integer.BYTES) + (calculateMaxFill(newCapacity) - hashTable.maxFill) * Long.BYTES + currentPageSizeInBytes;
        if (!updateMemory.update()) {
            // reserved memory but has exceeded the limit
            return false;
        }
        preallocatedMemoryInBytes = 0;

        expectedHashCollisions += estimateNumberOfHashCollisions(getGroupCount(), hashTable.getCapacity());

        NoRehashHashTable newHashTable = new NoRehashHashTable(
                hashChannels.length,
                newCapacity,
                Arrays.copyOf(hashTable.groupToHashPosition, newCapacity),
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
        private final Block[] blocks;
        private int lastPosition;

        private AddPageWork(Block[] blocks)
        {
            this.blocks = requireNonNull(blocks, "blocks is null");
        }

        @Override
        public boolean process()
        {
            int positionCount = blocks[0].getPositionCount();
            checkState(lastPosition < positionCount, "position count out of bound");
            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (hashTable.needRehash() && !tryRehash()) {
                return false;
            }

            NoRehashHashTable hashTable = MultiChannelBigintGroupByHashInlineFastBB.this.hashTable;
            while (lastPosition < positionCount && !hashTable.needRehash()) {
                hashTable.putIfAbsent(lastPosition, blocks);
                lastPosition++;
                if (hashTable.needRehash()) {
                    tryRehash();
                    hashTable = MultiChannelBigintGroupByHashInlineFastBB.this.hashTable;
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
        private final Block[] blocks;
        private boolean finished;
        private int lastPosition;

        private GetGroupIdsWork(Block[] blocks)
        {
            this.blocks = requireNonNull(blocks, "blocks is null");
            // we know the exact size required for the block
            this.blockBuilder = BIGINT.createFixedSizeBlockBuilder(blocks[0].getPositionCount());
        }

        @Override
        public boolean process()
        {
            int positionCount = blocks[0].getPositionCount();
            checkState(lastPosition < positionCount, "position count out of bound");
            checkState(!finished);

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (hashTable.needRehash() && !tryRehash()) {
                return false;
            }

            NoRehashHashTable hashTable = MultiChannelBigintGroupByHashInlineFastBB.this.hashTable;
            while (lastPosition < positionCount && !hashTable.needRehash()) {
                // output the group id for this row
                BIGINT.writeLong(blockBuilder, hashTable.putIfAbsent(lastPosition, blocks));
                lastPosition++;
                if (hashTable.needRehash()) {
                    tryRehash();
                    hashTable = MultiChannelBigintGroupByHashInlineFastBB.this.hashTable;
                }
            }
            return lastPosition == positionCount;
        }

        @Override
        public GroupByIdBlock getResult()
        {
            checkState(lastPosition == blocks[0].getPositionCount(), "process has not yet finished");
            checkState(!finished, "result has produced");
            finished = true;
            return new GroupByIdBlock(getGroupCount(), blockBuilder.build());
        }
    }

    static class NoRehashHashTable
            implements AutoCloseable
    {
        private final int hashChannelsCount;
        private final int hashCapacity;
        private final int maxFill;
        private final int mask;
        private final FastByteBuffer hashTable;
        private final int[] groupToHashPosition;

        private final int entrySize;
        private final int isNullOffset;

        private final FastByteBuffer valuesBuffer;
        private final int valuesBufferIsNullOffset;

        private int hashTableSize;
        private long hashCollisions;

        public static NoRehashHashTable create(int hashChannelsCount, int expectedSize)
        {
            int hashCapacity = arraySize(expectedSize, FILL_RATIO);
            return new NoRehashHashTable(
                    hashChannelsCount,
                    hashCapacity,
                    new int[hashCapacity],
                    0);
        }

        public NoRehashHashTable(
                int hashChannelsCount,
                int hashCapacity,
                int[] groupToHashPosition,
                long hashCollisions)
        {
            this.valuesBuffer = FastByteBuffer.allocate(hashChannelsCount * Long.BYTES + hashChannelsCount);
            this.hashCapacity = hashCapacity;
            this.groupToHashPosition = groupToHashPosition;

            this.hashChannelsCount = hashChannelsCount;
            this.valuesBufferIsNullOffset = hashChannelsCount * Long.BYTES;

            this.entrySize = valuesBuffer.capacity() + Integer.BYTES;
            this.maxFill = calculateMaxFill(hashCapacity);
            this.mask = hashCapacity - 1;
            this.hashTable = FastByteBuffer.allocate(entrySize * hashCapacity);
            this.hashCollisions = hashCollisions;
            for (int i = 0; i <= hashTable.capacity() - entrySize; i += entrySize) {
                this.hashTable.putInt(i, -1);
            }
            this.isNullOffset = Integer.BYTES + (hashChannelsCount * Long.BYTES);
        }

        private int putIfAbsent(int position, Block[] blocks)
        {
            for (int i = 0; i < blocks.length; i++) {
                byte isNull = (byte) (blocks[i].isNull(position) ? 1 : 0);
                valuesBuffer.putLong(i * Long.BYTES, BIGINT.getLong(blocks[i], position) & (isNull - 1)); // isNull -1 makes 0 to all 1s mask and 1 to all 0s mask
                valuesBuffer.put(valuesBufferIsNullOffset + i, isNull);
            }

            int hashPosition = getHashPosition(valuesBuffer, 0);
            // look for an empty slot or a slot containing this key
            while (true) {
                int current = hashTable.getInt(hashPosition);

                if (current == -1) {
                    // empty slot found
                    int groupId = hashTableSize++;
                    hashTable.putInt(hashPosition, groupId);
                    hashTable.copyFrom(valuesBuffer, 0, hashPosition + Integer.BYTES, valuesBuffer.capacity());
                    groupToHashPosition[groupId] = hashPosition;
//                System.out.println(hashPosition + ": v=" + value + ", " + hashTableSize);
                    return groupId;
                }
                if (valueEqualsBuffer(hashPosition, hashTable)) {
                    return current;
                }

                hashPosition = hashPosition + entrySize;
                if (hashPosition >= hashTable.capacity()) {
                    hashPosition = 0;
                }
                hashCollisions++;
            }
        }

        public int getSize()
        {
            return hashTableSize;
        }

        private boolean needRehash()
        {
            return hashTableSize >= maxFill;
        }

        public int getCapacity()
        {
            return hashCapacity;
        }

        private boolean valueEqualsBuffer(int hashPosition, FastByteBuffer hashTable)
        {
            return hashTable.subArrayEquals(valuesBuffer, hashPosition + Integer.BYTES, 0, valuesBufferIsNullOffset);
        }

        public long getEstimatedSize()
        {
            return hashTable.capacity() + sizeOf(groupToHashPosition);
        }

        private byte isNull(int hashPosition, int i)
        {
            return hashTable.get(hashPosition + isNullOffset + i);
        }

        @Override
        public void close()
        {
            try {
                hashTable.close();
            }
            finally {
                valuesBuffer.close();
            }
        }

        public void copyFrom(NoRehashHashTable other)
        {
            FastByteBuffer otherHashTable = other.hashTable;
            FastByteBuffer thisHashTable = this.hashTable;
            for (int i = 0; i <= otherHashTable.capacity() - entrySize; i += entrySize) {
                if (otherHashTable.getInt(i) != -1) {
                    int hashPosition = getHashPosition(otherHashTable, i + Integer.BYTES);
                    // look for an empty slot or a slot containing this key
                    while (thisHashTable.getInt(hashPosition) != -1) {
                        hashPosition = hashPosition + entrySize;
                        if (hashPosition >= thisHashTable.capacity()) {
                            hashPosition = 0;
                        }
                        hashCollisions++;
                    }
                    thisHashTable.copyFrom(otherHashTable, i, hashPosition, entrySize);
                }
            }
            hashTableSize += other.getSize();
        }

        public long getValue(int hashPosition, int index)
        {
            return hashTable.getLong(hashPosition + Integer.BYTES + index * Long.BYTES);
        }

        private int getHashPosition(FastByteBuffer values, int startPosition)
        {
            long hash = getHash(values, startPosition);

            return (int) (hash & mask) * entrySize;
        }

        public long getHash(int startPosition)
        {
            return getHash(hashTable, startPosition);
        }

        private long getHash(FastByteBuffer values, int startPosition)
        {
            long result = 1;
            for (int i = startPosition; i <= startPosition + (hashChannelsCount - 1) * Long.BYTES; i += Long.BYTES) {
                long element = values.getLong(i);
                long elementHash = HashCommon.mix(element);
                result = 31 * result + elementHash;
            }

            return murmurHash3(result);
        }

        public long getHashCollisions()
        {
            return hashCollisions;
        }
    }

    @Override
    public void close()
    {
        hashTable.close();
    }
}
