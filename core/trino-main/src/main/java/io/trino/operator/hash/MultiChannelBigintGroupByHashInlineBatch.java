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
import static io.trino.util.HashCollisionsEstimator.estimateNumberOfHashCollisions;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class MultiChannelBigintGroupByHashInlineBatch
        implements GroupByHash
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MultiChannelBigintGroupByHashInlineBatch.class).instanceSize();

    private static final float FILL_RATIO = 0.75f;

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

    private final long[][] valuesBuffer;
    private boolean[][] isNullBuffer;
    private final int entrySize;
    private final List<Type> types;
    private final int batchSize;
    private final int[] hashPositions;
    private final int[] current;

    public MultiChannelBigintGroupByHashInlineBatch(
            int[] hashChannels,
            Optional<Integer> inputHashChannel,
            int expectedSize,
            UpdateMemory updateMemory)
    {
        this(hashChannels, inputHashChannel, expectedSize, updateMemory, 32);
    }

    public MultiChannelBigintGroupByHashInlineBatch(
            int[] hashChannels,
            Optional<Integer> inputHashChannel,
            int expectedSize,
            UpdateMemory updateMemory,
            int batchSize)
    {
        checkArgument(expectedSize > 0, "expectedSize must be greater than zero");
        this.batchSize = batchSize;
        this.hashPositions = new int[batchSize];
        this.current = new int[batchSize];
        this.hashChannels = requireNonNull(hashChannels, "hashChannels is null");
        checkArgument(hashChannels.length > 0, "hashChannels.length must be at least 1");
        this.inputHashChannel = requireNonNull(inputHashChannel, "inputHashChannel is null");
        hashCapacity = arraySize(expectedSize, FILL_RATIO);
        int isNullBufferSize = (int) Math.ceil(((double) hashChannels.length) / 64);
        this.valuesBuffer = new long[batchSize][hashChannels.length + isNullBufferSize];
        this.isNullBuffer = new boolean[batchSize][hashChannels.length];

        this.entrySize = valuesBuffer[0].length + 1;
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
                MultiChannelBigintGroupByHashInlineBatch.this.appendValuesTo(hashTable, groupToHashPosition[currentGroupId], pageBuilder, outputChannelOffset);
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

    private void putIfAbsent(int[] positionsToProcess, int positionToProcessCount, Block[] blocks, int[] outGroupIds)
    {
        copyValues(positionsToProcess, positionToProcessCount, blocks);

        getHashPositions(positionToProcessCount);

        do {
            positionToProcessCount = processIteration(outGroupIds, positionsToProcess, positionToProcessCount);
        }
        while (positionToProcessCount > 0);
    }

    private int processIteration(int[] outGroupIds, int[] positionsToProcess, int positionToProcessCount)
    {
        getCurrent(positionToProcessCount);

        int remainingToProcess = 0;
        for (int i = 0; i < positionToProcessCount; i++) {
            int hashPosition = hashPositions[i];
            int currentPosition = positionsToProcess[i];
            if (current[i] == -1) {
                if (hashTable[hashPosition] == -1) {
                    // empty slot found
                    int groupId = hashTableSize++;
                    hashTable[hashPosition] = groupId;
                    System.arraycopy(valuesBuffer[i], 0, hashTable, hashPosition + 1, valuesBuffer[i].length);
                    groupToHashPosition[groupId] = hashPosition;

                    outGroupIds[currentPosition] = groupId;
                    continue;
                }
                // mid-batch conflict
                current[i] = (int) hashTable[hashPosition];
            }
            if (valueEqualsBuffer(i, hashPosition)) {
                outGroupIds[currentPosition] = current[i];
                continue;
            }

            hashPosition = hashPosition + entrySize;
            if (hashPosition >= hashTable.length) {
                hashPosition = 0;
            }
            positionsToProcess[remainingToProcess] = currentPosition;
            swapBuffers(i, remainingToProcess);
            hashPositions[remainingToProcess++] = hashPosition;

            hashCollisions++;
        }
        return remainingToProcess;
    }

    private void swapBuffers(int first, int second)
    {
        long[] tempValues = valuesBuffer[first];
        boolean[] tempIsNull = isNullBuffer[first];
        valuesBuffer[first] = valuesBuffer[second];
        isNullBuffer[first] = isNullBuffer[second];
        valuesBuffer[second] = tempValues;
        isNullBuffer[second] = tempIsNull;
    }

    private void getCurrent(int positionCount)
    {
        for (int positionIndex = 0; positionIndex < positionCount; positionIndex++) {
            current[positionIndex] = (int) hashTable[hashPositions[positionIndex]];
        }
    }

    private void getHashPositions(int positionCount)
    {
        for (int positionIndex = 0; positionIndex < positionCount; positionIndex++) {
            int hashPosition = getHashPosition(valuesBuffer[positionIndex], 0, hashChannels.length, mask);
            hashPositions[positionIndex] = hashPosition;
        }
    }

    private void copyValues(int[] positions, int positionCount, Block[] blocks)
    {
        // clear bitset
        for (int buffer = 0; buffer < valuesBuffer.length; buffer++) {
            long[] buff = valuesBuffer[buffer];
            for (int i = hashChannels.length; i < buff.length; i++) {
                buff[i] = 0;
            }
        }

        for (int i = 0; i < blocks.length; i++) {
            for (int positionIndex = 0; positionIndex < positionCount; positionIndex++) {
                int position = positions[positionIndex];
                boolean isNull = blocks[i].isNull(position);
                isNullBuffer[positionIndex][i] = isNull;
                valuesBuffer[positionIndex][i] = BIGINT.getLong(blocks[i], position) * (isNull ? 0 : 1);
                int wordIndex = i + i >> 6;
                valuesBuffer[positionIndex][hashChannels.length + wordIndex] |= (isNull ? 1L : 0) << i;
            }
        }
    }

    private boolean valueEqualsBuffer(int valuesIndex, int hashPosition)
    {
        int hashTablePosition = hashPosition + 1;
        long[] values = valuesBuffer[valuesIndex];
        for (int i = 0; i < values.length; i++) {
            if (hashTable[hashTablePosition++] != values[i]) {
                return false;
            }
        }
        return true;
//        return Arrays.equals(
//                hashTable, hashPosition + 1, hashPosition + 1 + valuesBuffer[valuesIndex].length,
//                valuesBuffer[valuesIndex], 0, valuesBuffer[valuesIndex].length);
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
        for (int i = 0; i <= newHashTable.length - entrySize; i += entrySize) {
            newHashTable[i] = -1;
        }

        int newMask = newCapacity - 1;
        int rehashBatchSize = 256;
        int[] positionsToProcess = new int[rehashBatchSize];
        int[] hashPositions = new int[rehashBatchSize];
        int[] current = new int[rehashBatchSize];
        int positionToProcessCount = 0;
        for (int oldHashTablePosition = 0; oldHashTablePosition <= hashTable.length - entrySize; oldHashTablePosition += (entrySize * rehashBatchSize)) {
            for (int i = 0; i < Math.min(rehashBatchSize, hashTable.length - oldHashTablePosition); i++) {
                if (hashTable[oldHashTablePosition + (i * entrySize)] != -1) {
                    positionsToProcess[positionToProcessCount++] = oldHashTablePosition + i * entrySize;
                }
            }
            for (int i = 0; i < positionToProcessCount; i++) {
                hashPositions[i] = getHashPosition(hashTable, positionsToProcess[i], hashChannels.length, newMask);
            }

            do {
                for (int i = 0; i < positionToProcessCount; i++) {
                    current[i] = (int) newHashTable[hashPositions[i]];
                }

                int remainingToProcess = 0;
                for (int i = 0; i < positionToProcessCount; i++) {
                    int hashPosition = hashPositions[i];
                    int currentPosition = positionsToProcess[i];
                    if (current[i] == -1 && newHashTable[hashPosition] == -1) {
                        System.arraycopy(hashTable, currentPosition, newHashTable, hashPosition, entrySize);
                        groupToHashPosition[(int) hashTable[currentPosition]] = hashPosition;
                        continue;
                    }
                    hashPosition = hashPosition + entrySize;
                    if (hashPosition >= newHashTable.length) {
                        hashPosition = 0;
                    }
                    positionsToProcess[remainingToProcess] = currentPosition;
                    hashPositions[remainingToProcess++] = hashPosition;
                    hashCollisions++;
                }
                positionToProcessCount = remainingToProcess;
            }
            while (positionToProcessCount > 0);
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

    private boolean needRehash(int newGroups)
    {
        return hashTableSize + newGroups >= maxFill;
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

            int[] groupIds = new int[positionCount];
            int[] positions = new int[batchSize];

            for (int batchStart = lastPosition; batchStart < positionCount; batchStart += batchSize) {
                if (needRehash(batchSize) && !tryRehash()) {
                    return false;
                }
                int currentBatchSize = Math.min(batchSize, positionCount - batchStart);
                for (int batchOffset = 0; batchOffset < currentBatchSize; batchOffset++) {
                    positions[batchOffset] = batchStart + batchOffset;
                }
                putIfAbsent(positions, currentBatchSize, blocks, groupIds);
                lastPosition += currentBatchSize;
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

            int[] groupIds = new int[positionCount];
            int[] positions = new int[batchSize];

            for (int batchStart = lastPosition; batchStart < positionCount; batchStart += batchSize) {
                if (needRehash(batchSize) && !tryRehash()) {
                    return false;
                }
                int currentBatchSize = Math.min(batchSize, positionCount - batchStart);
                for (int batchOffset = 0; batchOffset < currentBatchSize; batchOffset++) {
                    positions[batchOffset] = batchStart + batchOffset;
                }
                putIfAbsent(positions, currentBatchSize, blocks, groupIds);

                for (int batchOffset = 0; batchOffset < currentBatchSize; batchOffset++) {
                    BIGINT.writeLong(blockBuilder, groupIds[batchStart + batchOffset]);
                }

                lastPosition += currentBatchSize;
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
