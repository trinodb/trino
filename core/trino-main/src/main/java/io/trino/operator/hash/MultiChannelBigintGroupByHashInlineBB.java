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
import org.openjdk.jol.info.ClassLayout;

import java.nio.ByteBuffer;
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

public class MultiChannelBigintGroupByHashInlineBB
        implements GroupByHash
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MultiChannelBigintGroupByHashInlineBB.class).instanceSize();

    private static final float FILL_RATIO = 0.75f;

    private final int[] hashChannels;
    private final Optional<Integer> inputHashChannel;

    private int hashCapacity;
    private int maxFill;
    private int mask;

    // the hash table with value + groupId entries.
    // external groupId is the position/index in this table and artificial groupId concatenated.
    private ByteBuffer hashTable;
    private int hashTableSize;
    private int[] groupToHashPosition;

    private long hashCollisions;
    private double expectedHashCollisions;

    private final int entrySize;
    private final ByteBuffer valuesBuffer;
    private final int valuesBufferIsNullOffset;
    private final int isNullOffset;

    // reserve enough memory before rehash
    private final UpdateMemory updateMemory;
    private long preallocatedMemoryInBytes;
    private long currentPageSizeInBytes;

    private final List<Type> types;

    public MultiChannelBigintGroupByHashInlineBB(
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
        this.valuesBuffer = ByteBuffer.allocate(hashChannels.length * Long.BYTES + hashChannels.length);

        this.entrySize = valuesBuffer.capacity() + Integer.BYTES;
        maxFill = calculateMaxFill(hashCapacity);
        mask = hashCapacity - 1;
        hashTable = ByteBuffer.allocate(entrySize * hashCapacity);
        for (int i = 0; i <= hashTable.capacity() - entrySize; i += entrySize) {
            hashTable.putInt(i, -1);
        }
        groupToHashPosition = new int[hashCapacity];
        valuesBufferIsNullOffset = hashChannels.length * Long.BYTES;
        isNullOffset = Integer.BYTES + (hashChannels.length * Long.BYTES);
        // This interface is used for actively reserving memory (push model) for rehash.
        // The caller can also query memory usage on this object (pull model)
        this.updateMemory = requireNonNull(updateMemory, "updateMemory is null");
        this.types = Collections.nCopies(hashChannels.length + (inputHashChannel.isPresent() ? 1 : 0), BIGINT);
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                hashTable.capacity() +
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
                MultiChannelBigintGroupByHashInlineBB.this.appendValuesTo(hashTable, groupToHashPosition[currentGroupId], pageBuilder, outputChannelOffset);
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

    public void appendValuesTo(ByteBuffer hashTable, int hashPosition, PageBuilder pageBuilder, int outputChannelOffset)
    {
        for (int i = 0; i < hashChannels.length; i++) {
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset);
            if (isNull(hashPosition, i) == 1) {
                blockBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(blockBuilder, hashTable.getLong(hashPosition + Integer.BYTES + i * Long.BYTES));
            }
            outputChannelOffset++;
        }

        if (inputHashChannel.isPresent()) {
            BlockBuilder hashBlockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset);
            BIGINT.writeLong(hashBlockBuilder, getHash(hashTable, hashPosition + 1, hashChannels.length));
        }
    }

    private byte isNull(int hashPosition, int i)
    {
        return hashTable.get(hashPosition + isNullOffset + i);
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
        return hashCapacity;
    }

    private int putIfAbsent(int position, Block[] blocks)
    {
        for (int i = 0; i < blocks.length; i++) {
            byte isNull = (byte) (blocks[i].isNull(position) ? 1 : 0);
            valuesBuffer.putLong(i * Long.BYTES, BIGINT.getLong(blocks[i], position) & (isNull - 1)); // isNull -1 makes 0 to all 1s mask and 1 to all 0s mask
            valuesBuffer.put(valuesBufferIsNullOffset + i, isNull);
        }

        int hashPosition = getHashPosition(valuesBuffer, 0, hashChannels.length, mask);
        // look for an empty slot or a slot containing this key
        while (true) {
            int current = hashTable.getInt(hashPosition);

            if (current == -1) {
                // empty slot found
                int groupId = hashTableSize++;
                hashTable.putInt(hashPosition, groupId);
                System.arraycopy(valuesBuffer.array(), 0, hashTable.array(), hashPosition + Integer.BYTES, valuesBuffer.capacity());
                groupToHashPosition[groupId] = hashPosition;
                if (needRehash()) {
                    tryRehash();
                }
//                System.out.println(hashPosition + ": v=" + value + ", " + hashTableSize);
                return groupId;
            }
            if (valueEqualsBuffer(hashPosition)) {
                return hashTable.getInt(hashPosition);
            }

            hashPosition = hashPosition + entrySize;
            if (hashPosition >= hashTable.capacity()) {
                hashPosition = 0;
            }
            hashCollisions++;
        }
    }

    private boolean valueEqualsBuffer(int hashPosition)
    {
        for (int i = 0; i < hashChannels.length; i++) {
            byte bufferIsNull = valuesBuffer.get(valuesBufferIsNullOffset + i);
            if (bufferIsNull != isNull(hashPosition, i)) {
                // null and not null
                return false;
            }
            if (bufferIsNull == 1) {
                // both null
                return true;
            }
            // both not null
            if (!Arrays.equals(
                    hashTable.array(), hashPosition + Integer.BYTES, hashPosition + isNullOffset,
                    valuesBuffer.array(), 0, valuesBufferIsNullOffset)) {
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

        ByteBuffer newHashTable = ByteBuffer.allocate(entrySize * newCapacity);
        for (int i = 0; i <= newHashTable.capacity() - entrySize; i += entrySize) {
            newHashTable.putInt(i, -1);
        }
        int newMask = newCapacity - 1;
        for (int i = 0; i <= hashTable.capacity() - entrySize; i += entrySize) {
            int hashPosition = getHashPosition(hashTable, i, hashChannels.length, newMask);
            // look for an empty slot or a slot containing this key
            while (newHashTable.getInt(hashPosition) != -1) {
                hashPosition = hashPosition + entrySize;
                if (hashPosition >= newHashTable.capacity()) {
                    hashPosition = 0;
                }
                hashCollisions++;
            }
            System.arraycopy(
                    hashTable.array(),
                    i,
                    newHashTable.array(),
                    hashPosition,
                    entrySize);
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

    private int getHashPosition(ByteBuffer values, int startPosition, int length, int mask)
    {
        long hash = getHash(values, startPosition, length);

        return (int) (hash & mask) * entrySize;
    }

    private long getHash(ByteBuffer values, int startPosition, int length)
    {
        int result = 1;
        for (int i = startPosition; i <= startPosition + (length - 1) * Long.BYTES; i += Long.BYTES) {
            long element = values.getLong(i);
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
