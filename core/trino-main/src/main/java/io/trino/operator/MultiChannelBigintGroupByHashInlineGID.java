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
package io.trino.operator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.trino.operator.aggregation.builder.InMemoryHashAggregationBuilder;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.AbstractLongType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
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
import static io.trino.type.TypeUtils.NULL_HASH_CODE;
import static io.trino.util.HashCollisionsEstimator.estimateNumberOfHashCollisions;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class MultiChannelBigintGroupByHashInlineGID
        implements GroupByHash
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MultiChannelBigintGroupByHashInlineGID.class).instanceSize();

    private static final float FILL_RATIO = 0.75f;
    private static final List<Type> TYPES = ImmutableList.of(BIGINT);
    private static final List<Type> TYPES_WITH_RAW_HASH = ImmutableList.of(BIGINT, BIGINT);

    private final int[] hashChannels;
    private final Optional<Integer> inputHashChannel;

    private int hashCapacity;
    private int maxFill;
    private int mask;

    // the hash table with value + groupId entries.
    // external groupId is the position/index in this table and artificial groupId concatenated.
    private long[] hashTable;
    private int hashTableSize;
    private int[] groupToHashPosition;

    private long hashCollisions;
    private double expectedHashCollisions;

    // reserve enough memory before rehash
    private final UpdateMemory updateMemory;
    private long preallocatedMemoryInBytes;
    private long currentPageSizeInBytes;

    private final long[] valuesBuffer;
    private boolean[] isNullBuffer;
    private final int entrySize;
    private final List<Type> types;

    public MultiChannelBigintGroupByHashInlineGID(
            int[] hashChannels,
            Optional<Integer> inputHashChannel,
            int expectedSize,
            UpdateMemory updateMemory)
    {
        checkArgument(expectedSize > 0, "expectedSize must be greater than zero");

        this.hashChannels = requireNonNull(hashChannels, "hashChannels is null");
        checkArgument(hashChannels.length > 0, "hashChannels.length must be at least 1");
        this.inputHashChannel = requireNonNull(inputHashChannel, "inputHashChannel is null");
        hashCapacity = arraySize(expectedSize, FILL_RATIO);
        int isNullBufferSize = (int) Math.ceil(((double) hashChannels.length) / 64);
        this.valuesBuffer = new long[hashChannels.length + isNullBufferSize];
        this.isNullBuffer = new boolean[hashChannels.length];

        this.entrySize = valuesBuffer.length + 1;
        maxFill = calculateMaxFill(hashCapacity);
        mask = hashCapacity - 1;
        hashTable = new long[entrySize * hashCapacity];
        Arrays.fill(hashTable, -1);
        groupToHashPosition = new int[hashCapacity];

        // This interface is used for actively reserving memory (push model) for rehash.
        // The caller can also query memory usage on this object (pull model)
        this.updateMemory = requireNonNull(updateMemory, "updateMemory is null");
        this.types = Collections.nCopies(hashChannels.length + (inputHashChannel.isPresent() ? 1 : 0), BIGINT);
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                sizeOf(hashTable) +
                sizeOf(groupToHashPosition) +
                preallocatedMemoryInBytes;
    }

    @Override
    public long getHashCollisions()
    {
        return hashCollisions;
    }

    @Override
    public double getExpectedHashCollisions()
    {
        return expectedHashCollisions + estimateNumberOfHashCollisions(getGroupCount(), hashCapacity);
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public int getGroupCount()
    {
        return hashTableSize;
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
                return currentGroupId + 1 < hashTableSize;
            }

            @Override
            public void next()
            {
                currentGroupId++;
            }

            @Override
            public void appendValuesTo(PageBuilder pageBuilder, int outputChannelOffset)
            {
                MultiChannelBigintGroupByHashInlineGID.this.appendValuesTo(hashTable, groupToHashPosition[currentGroupId], pageBuilder, outputChannelOffset);
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

    public void appendValuesTo(long[] hashTable, int hashPosition, PageBuilder pageBuilder, int outputChannelOffset)
    {
        for (int i = 0; i < hashChannels.length; i++) {
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset);
            if (isNull(hashPosition, i)) {
                blockBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(blockBuilder, hashTable[hashPosition + 1 + i]);
            }
            outputChannelOffset++;
        }

        if (inputHashChannel.isPresent()) {
            BlockBuilder hashBlockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset);
            BIGINT.writeLong(hashBlockBuilder, getHash(hashTable, hashPosition + 1, hashChannels.length));
        }
    }

    private boolean isNull(int hashPosition, int i)
    {
        int wordIndex = i >> 6;
        return (hashTable[hashPosition + 1 + hashChannels.length + wordIndex] & (1L << i)) != 0;
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
            Block block = page.getBlock(i);
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

    public long getRawHash(int groupId)
    {
        return BigintType.hash(hashTable[groupId]);
    }

    @VisibleForTesting
    @Override
    public int getCapacity()
    {
        return hashCapacity;
    }

    private int putIfAbsent(int position, Block[] blocks)
    {
        for (int i = 0; i < blocks.length; i++) {
            boolean isNull = blocks[i].isNull(position);
            isNullBuffer[i] = isNull;
            valuesBuffer[i] = BIGINT.getLong(blocks[i], position) * (isNull ? 0 : 1);
            int wordIndex = i + i >> 6;
            valuesBuffer[wordIndex] |= (isNull ? 1L : 0) << i;
        }

        int hashPosition = getHashPosition(valuesBuffer, 0, hashChannels.length, mask);
        // look for an empty slot or a slot containing this key
        while (true) {
            long current = hashTable[hashPosition];

            if (current == -1) {
                // empty slot found
                int groupId = hashTableSize++;
                hashTable[hashPosition] = groupId;
                System.arraycopy(valuesBuffer, 0, hashTable, hashPosition + 1, valuesBuffer.length);
                groupToHashPosition[groupId] = hashPosition;
                if (needRehash()) {
                    tryRehash();
                }
//                System.out.println(hashPosition + ": v=" + value + ", " + hashTableSize);
                return groupId;
            }
            if (valueEqualsBuffer(hashPosition)) {
                return (int) hashTable[hashPosition];
            }

            hashPosition = hashPosition + entrySize;
            if (hashPosition >= hashTable.length) {
                hashPosition = 0;
            }
            hashCollisions++;
        }
    }

    private boolean valueEqualsBuffer(int hashPosition)
    {
        for (int i = 0; i < hashChannels.length; i++) {
            if (!(isNullBuffer[i] == isNull(hashPosition, i))) {
                // null and not null
                return false;
            }
            if (isNullBuffer[i]) {
                // both null
                return true;
            }
            // both not null
            if (!Arrays.equals(
                    hashTable, hashPosition + 1, hashPosition + 1 + hashChannels.length,
                    valuesBuffer, 0, hashChannels.length)) {
                return false;
            }
        }
        return true;
    }

    private boolean tryRehash()
    {
        return tryRehash(hashCapacity * 2L);
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
        preallocatedMemoryInBytes = (newCapacity - hashCapacity) * (long) (Long.BYTES + Integer.BYTES) + (calculateMaxFill(newCapacity) - maxFill) * Long.BYTES + currentPageSizeInBytes;
        if (!updateMemory.update()) {
            // reserved memory but has exceeded the limit
            return false;
        }
        preallocatedMemoryInBytes = 0;

        expectedHashCollisions += estimateNumberOfHashCollisions(getGroupCount(), hashCapacity);

        long[] newHashTable = new long[entrySize * newCapacity];
        Arrays.fill(newHashTable, -1);
        int newMask = newCapacity - 1;
        for (int i = 0; i + entrySize <= hashTable.length; i += entrySize) {
            long groupId = hashTable[i];
            int hashPosition = getHashPosition(hashTable, i, hashChannels.length, newMask);
            // look for an empty slot or a slot containing this key
            while (newHashTable[hashPosition] != -1) {
                hashPosition = hashPosition + entrySize;
                if (hashPosition >= newHashTable.length) {
                    hashPosition = 0;
                }
                hashCollisions++;
            }
            newHashTable[hashPosition] = groupId;
            System.arraycopy(hashTable, i, newHashTable, hashPosition, entrySize);
//            System.out.println("rehash " + i + " -> " + hashPosition + ": v=" + value + ", " + groupId);
        }

        this.mask = newMask;
        hashCapacity = newCapacity;
        hashTable = newHashTable;
        maxFill = calculateMaxFill(hashCapacity);
        groupToHashPosition = Arrays.copyOf(groupToHashPosition, hashCapacity);
//        System.out.println("new mask: " + mask
//                + " hashCapacity: "
//                + hashCapacity +
//                " hashTable.length: " + hashTable.length);

        return true;
    }

    private boolean needRehash()
    {
        return hashTableSize >= maxFill;
    }

    private int getHashPosition(long[] values, int startPosition, int length, int mask)
    {
        long hash = getHash(values, startPosition, length);

        return (int) (hash & mask) * entrySize;
    }

    private long getHash(long[] values, int startPosition, int length)
    {
        int result = 1;
        for (int i = startPosition; i < startPosition + length; i++) {
            long element = values[i];
            int elementHash = (int) (element ^ (element >>> 32));
            result = 31 * result + elementHash;
        }

        return murmurHash3(result);
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
            if (needRehash() && !tryRehash()) {
                return false;
            }

            // putIfAbsent will rehash automatically if rehash is needed, unless there isn't enough memory to do so.
            // Therefore needRehash will not generally return true even if we have just crossed the capacity boundary.
            while (lastPosition < positionCount && !needRehash()) {
                // get the group for the current row
                putIfAbsent(lastPosition, blocks);
                lastPosition++;
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
            if (needRehash() && !tryRehash()) {
                return false;
            }

            // putIfAbsent will rehash automatically if rehash is needed, unless there isn't enough memory to do so.
            // Therefore needRehash will not generally return true even if we have just crossed the capacity boundary.
            while (lastPosition < positionCount && !needRehash()) {
                // output the group id for this row
                BIGINT.writeLong(blockBuilder, putIfAbsent(lastPosition, blocks));
                lastPosition++;
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
}
