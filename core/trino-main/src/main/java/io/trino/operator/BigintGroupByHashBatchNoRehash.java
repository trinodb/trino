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
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.AbstractLongType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

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
import static it.unimi.dsi.fastutil.HashCommon.nextPowerOfTwo;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class BigintGroupByHashBatchNoRehash
        implements GroupByHash
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BigintGroupByHashBatchNoRehash.class).instanceSize();

    private static final float FILL_RATIO = 0.75f;
    private static final List<Type> TYPES = ImmutableList.of(BIGINT);
    private static final List<Type> TYPES_WITH_RAW_HASH = ImmutableList.of(BIGINT, BIGINT);

    private final int hashChannel;
    private final boolean outputRawHash;

    private int hashCapacity;
    private int maxFill;
    private int mask;

    // the hash table with values. groupId is the position/index in this table.
    private long[] hashTable;
    private int hashTableSize;
    private boolean containsNullKey;

    // groupId for the null value
    private static final int nullGroupId = -1;

    private long hashCollisions;
    private double expectedHashCollisions;

    // reserve enough memory before rehash
    private final UpdateMemory updateMemory;
    private long preallocatedMemoryInBytes;
    private long currentPageSizeInBytes;
    private boolean containsNull;

    public BigintGroupByHashBatchNoRehash(int hashChannel, boolean outputRawHash, int expectedSize, UpdateMemory updateMemory)
    {
        checkArgument(hashChannel >= 0, "hashChannel must be at least zero");
        checkArgument(expectedSize > 0, "expectedSize must be greater than zero");

        this.hashChannel = hashChannel;
        this.outputRawHash = outputRawHash;

        hashCapacity = arraySize(expectedSize, FILL_RATIO);

        maxFill = calculateMaxFill(hashCapacity);
        mask = hashCapacity - 1;
        hashTable = new long[hashCapacity + 1]; // + 1 is for value 0 if present

        // This interface is used for actively reserving memory (push model) for rehash.
        // The caller can also query memory usage on this object (pull model)
        this.updateMemory = requireNonNull(updateMemory, "updateMemory is null");
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                sizeOf(hashTable) +
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
        return outputRawHash ? TYPES_WITH_RAW_HASH : TYPES;
    }

    @Override
    public int getGroupCount()
    {
        return hashTableSize;
    }

    public void appendValuesTo(int groupId, PageBuilder pageBuilder, int outputChannelOffset)
    {
        checkArgument(groupId >= 0 || groupId == nullGroupId, "groupId is negative");
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset);
        if (groupId == nullGroupId) {
            blockBuilder.appendNull();
        }
        else {
            BIGINT.writeLong(blockBuilder, hashTable[groupId]);
        }

        if (outputRawHash) {
            BlockBuilder hashBlockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset + 1);
            if (groupId == nullGroupId) {
                BIGINT.writeLong(hashBlockBuilder, NULL_HASH_CODE);
            }
            else {
                BIGINT.writeLong(hashBlockBuilder, AbstractLongType.hash(hashTable[groupId]));
            }
        }
    }

    @Override
    public Work<?> addPage(Page page)
    {
        currentPageSizeInBytes = page.getRetainedSizeInBytes();
        Block block = page.getBlock(hashChannel);
        if (block instanceof RunLengthEncodedBlock) {
            throw new UnsupportedOperationException();
        }
        if (block instanceof DictionaryBlock) {
            throw new UnsupportedOperationException();
        }

        return new AddPageWork(block);
    }

    @Override
    public Work<GroupByIdBlock> getGroupIds(Page page)
    {
        currentPageSizeInBytes = page.getRetainedSizeInBytes();
        Block block = page.getBlock(hashChannel);
        if (block instanceof RunLengthEncodedBlock) {
            throw new UnsupportedOperationException();
        }
        if (block instanceof DictionaryBlock) {
            throw new UnsupportedOperationException();
        }

        return new GetGroupIdsWork(page.getBlock(hashChannel));
    }

    @Override
    public boolean contains(int position, Page page, int[] hashChannels)
    {
        Block block = page.getBlock(hashChannel);
        if (block.isNull(position)) {
            return containsNull;
        }

        long value = BIGINT.getLong(block, position);
        if (value == 0) {
            return containsNullKey;
        }
        int hashPosition = getHashPosition(value, mask);

        // look for an empty slot or a slot containing this key
        while (true) {
            long current = hashTable[hashPosition];
            if (current == 0) {
                return false;
            }
            if (value == current) {
                return true;
            }

            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
        }
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

    private static final int batchSize = 64;

    private void putIfAbsent(int startPosition, int length, Block block, long[] outGroupIds)
    {
        checkArgument(!block.mayHaveNull()); // TODO lysy: handle nulls

        tryEnsureCapacity(length);

        int endPosition = startPosition + length;

        long[] currentValues = new long[batchSize];
        long[] valuesBatch = new long[batchSize];
        int[] toProcess = new int[batchSize];
        int[] positions = new int[batchSize];

        for (int batchStart = startPosition; batchStart < startPosition + length; batchStart += batchSize) {
            processBatch(batchStart, Math.min(batchSize, endPosition - batchStart), block, outGroupIds,
                    currentValues, valuesBatch, toProcess, positions);
        }
    }

    private void processBatch(int startPosition, int length, Block block, long[] outGroupIds,
            long[] currentValues, long[] valuesBatch, int[] toProcess, int[] positions)
    {
        for (int i = 0; i < length; i++) {
            valuesBatch[i] = BIGINT.getLong(block, startPosition + i);
        }

        getPositions(valuesBatch, length, positions);

        // look for an empty slot or a slot containing this key
        for (int i = 0; i < length; i++) {
            toProcess[i] = startPosition + i;
        }

        processPositions(currentValues, outGroupIds, valuesBatch, positions, toProcess, length);
    }

    private void processPositions(long[] currentValues, long[] outGroupIds, long[] valuesBatch, int[] positions, int[] toProcess, int toProcessCount)
    {
        do {
            int remainingProcess = 0;
            remainingProcess = processIteration(currentValues, outGroupIds, valuesBatch, positions, toProcess, toProcessCount, remainingProcess);
            toProcessCount = remainingProcess;
        }
        while (toProcessCount > 0);
    }

    private int processIteration(long[] currentValues, long[] outGroupIds, long[] valuesBatch, int[] positions, int[] toProcess, int toProcessCount, int remainingProcess)
    {
        long[] hashTable = this.hashTable;
        fetchCurrentValues(currentValues, positions, toProcessCount, hashTable);

        for (int i = 0; i < toProcessCount; i++) {
            int current = toProcess[i];

            int position = positions[i];
            long value = valuesBatch[i];
            if (value == 0) {
                outGroupIds[current] = hashCapacity;
                hashTableSize += containsNullKey ? 0 : 1;
                containsNullKey = true;
            }
            else if (currentValues[i] == 0) {
                // empty slot found
                hashTable[position] = value;
                outGroupIds[current] = position;
                hashTableSize++;
            }
            else if (value != currentValues[i]) {
                remainingProcess = hashCollision(positions, toProcess, remainingProcess, current, i, position);
            }
        }
        return remainingProcess;
    }

    private void fetchCurrentValues(long[] currentValues, int[] positions, int toProcessCount, long[] hashTable)
    {
        for (int i = 0; i < toProcessCount; i++) {
            currentValues[i] = hashTable[positions[i]];
        }
    }

    private int hashCollision(int[] positions, int[] toProcess, int remainingProcess, int current, int i, int position)
    {
        positions[i] = (position + 1) & mask;
        hashCollisions++;
        toProcess[remainingProcess] = current;
        remainingProcess++;
        return remainingProcess;
    }

    private void getPositions(long[] valuesBatch, int length, int[] positions)
    {
        for (int i = 0; i < length; i++) {
            positions[i] = (int) (murmurHash3(valuesBatch[i]) & mask);
        }
    }

    private boolean tryRehash()
    {
        long newCapacityLong = hashCapacity * 2L;
        return tryRehash(newCapacityLong);
    }

    private boolean tryEnsureCapacity(long totalNewCapacity)
    {
        if (hashCapacity < totalNewCapacity) {
            return tryRehash(nextPowerOfTwo(totalNewCapacity));
        }
        return true;
    }

    private boolean tryRehash(long newCapacityLong)
    {
        checkArgument(hashTableSize == 0, "rehash not supported %s", hashTableSize);
        if (newCapacityLong > Integer.MAX_VALUE) {
            throw new TrinoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }
        int newCapacity = toIntExact(newCapacityLong);

        // An estimate of how much extra memory is needed before we can go ahead and expand the hash table.
        // This includes the new capacity for values, groupIds, and valuesByGroupId as well as the size of the current page
        preallocatedMemoryInBytes = (newCapacity - hashCapacity) * (long) (Long.BYTES + Integer.BYTES) + (calculateMaxFill(newCapacity) - maxFill) * Long.BYTES + currentPageSizeInBytes;
        if (!updateMemory.update()) {
            // reserved memory but has exceeded the limit
            return false;
        }
        preallocatedMemoryInBytes = 0;

        expectedHashCollisions += estimateNumberOfHashCollisions(getGroupCount(), hashCapacity);

        mask = newCapacity - 1;
        hashCapacity = newCapacity;
        maxFill = calculateMaxFill(hashCapacity);
        hashTable = new long[hashCapacity + 1];

        return true;
    }

    private boolean needRehash()
    {
        return hashTableSize >= maxFill;
    }

    private static int getHashPosition(long rawHash, int mask)
    {
        return (int) (murmurHash3(rawHash) & mask);
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
        private final Block block;

        private int lastPosition;

        public AddPageWork(Block block)
        {
            this.block = requireNonNull(block, "block is null");
        }

        @Override
        public boolean process()
        {
            int positionCount = block.getPositionCount();
            checkState(lastPosition < positionCount, "position count out of bound");

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (needRehash() && !tryRehash()) {
                return false;
            }

            putIfAbsent(0, positionCount, block, new long[positionCount]);
            lastPosition = positionCount;
            return true;
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
        private final Block block;
        long[] outGroupIds;

        private boolean finished;
        private int lastPosition;

        public GetGroupIdsWork(Block block)
        {
            this.block = requireNonNull(block, "block is null");
            this.outGroupIds = new long[block.getPositionCount()];
        }

        @Override
        public boolean process()
        {
            int positionCount = block.getPositionCount();
            checkState(lastPosition < positionCount, "position count out of bound");
            checkState(!finished);

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (needRehash() && !tryRehash()) {
                return false;
            }

            putIfAbsent(0, positionCount, block, outGroupIds);
            lastPosition = positionCount;
            return true;
        }

        @Override
        public GroupByIdBlock getResult()
        {
            checkState(lastPosition == block.getPositionCount(), "process has not yet finished");
            checkState(!finished, "result has produced");
            finished = true;
            return new GroupByIdBlock(getGroupCount(), new LongArrayBlock(block.getPositionCount(), Optional.empty(), outGroupIds));
        }
    }
}
