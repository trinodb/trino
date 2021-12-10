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

import java.util.Arrays;
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

public class BigintGroupByHashBatch
        implements GroupByHash
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BigintGroupByHashBatch.class).instanceSize();

    private static final float FILL_RATIO = 0.75f;
    private static final List<Type> TYPES = ImmutableList.of(BIGINT);
    private static final List<Type> TYPES_WITH_RAW_HASH = ImmutableList.of(BIGINT, BIGINT);

    private final int hashChannel;
    private final boolean outputRawHash;

    private int hashCapacity;
    private int maxFill;
    private int mask;

    // the hash table from values to groupIds
    private long[] values;
    private int[] groupIds;

    // groupId for the null value
    private int nullGroupId = -1;

    // reverse index from the groupId back to the value
    private long[] valuesByGroupId;

    private int nextGroupId;
    private long hashCollisions;
    private double expectedHashCollisions;

    // reserve enough memory before rehash
    private final UpdateMemory updateMemory;
    private long preallocatedMemoryInBytes;
    private long currentPageSizeInBytes;

    public BigintGroupByHashBatch(int hashChannel, boolean outputRawHash, int expectedSize, UpdateMemory updateMemory)
    {
        checkArgument(hashChannel >= 0, "hashChannel must be at least zero");
        checkArgument(expectedSize > 0, "expectedSize must be greater than zero");

        this.hashChannel = hashChannel;
        this.outputRawHash = outputRawHash;

        hashCapacity = arraySize(expectedSize, FILL_RATIO);

        maxFill = calculateMaxFill(hashCapacity);
        mask = hashCapacity - 1;
        values = new long[hashCapacity];
        groupIds = new int[hashCapacity];
        Arrays.fill(groupIds, -1);

        valuesByGroupId = new long[maxFill];

        // This interface is used for actively reserving memory (push model) for rehash.
        // The caller can also query memory usage on this object (pull model)
        this.updateMemory = requireNonNull(updateMemory, "updateMemory is null");
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                sizeOf(groupIds) +
                sizeOf(values) +
                sizeOf(valuesByGroupId) +
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
        return nextGroupId;
    }

    public void appendValuesTo(int groupId, PageBuilder pageBuilder, int outputChannelOffset)
    {
        checkArgument(groupId >= 0, "groupId is negative");
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset);
        if (groupId == nullGroupId) {
            blockBuilder.appendNull();
        }
        else {
            BIGINT.writeLong(blockBuilder, valuesByGroupId[groupId]);
        }

        if (outputRawHash) {
            BlockBuilder hashBlockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset + 1);
            if (groupId == nullGroupId) {
                BIGINT.writeLong(hashBlockBuilder, NULL_HASH_CODE);
            }
            else {
                BIGINT.writeLong(hashBlockBuilder, AbstractLongType.hash(valuesByGroupId[groupId]));
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
            return nullGroupId >= 0;
        }

        long value = BIGINT.getLong(block, position);
        int hashPosition = getHashPosition(value, mask);

        // look for an empty slot or a slot containing this key
        while (true) {
            int groupId = groupIds[hashPosition];
            if (groupId == -1) {
                return false;
            }
            if (value == values[hashPosition]) {
                return true;
            }

            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
        }
    }

    public long getRawHash(int groupId)
    {
        return BigintType.hash(valuesByGroupId[groupId]);
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

        tryEnsureCapacity(3_000_000);

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

        getPositions(valuesBatch, positions);

        // look for an empty slot or a slot containing this key
        for (int i = 0; i < length; i++) {
            toProcess[i] = startPosition + i;
        }

        getInitialGroupsStandard(outGroupIds, valuesBatch, positions, toProcess, length);

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
        long[] values = this.values;
        int[] groupIds = this.groupIds;
        fetchCurrentValues(currentValues, positions, toProcessCount, values);

        for (int i = 0; i < toProcessCount; i++) {
            int current = toProcess[i];

            int position = positions[i];
            long value = valuesBatch[i];

            if (value != currentValues[i]) {
                remainingProcess = hashCollision(outGroupIds, valuesBatch, positions, toProcess, remainingProcess, current, i, position, groupIds);
            }
        }
        return remainingProcess;
    }

    private void fetchCurrentValues(long[] currentValues, int[] positions, int toProcessCount, long[] values)
    {
        for (int i = 0; i < toProcessCount; i++) {
            currentValues[i] = values[positions[i]];
        }
    }

    private int hashCollision(long[] outGroupIds, long[] valuesBatch, int[] positions, int[] toProcess, int remainingProcess, int current, int i, int position, int[] groupIds)
    {
        position = (position + 1) & mask;
        positions[i] = position;
        hashCollisions++;
        outGroupIds[current] = groupIds[position];
        if (outGroupIds[current] == -1) {
//            outGroupIds[current] = addNewGroup(position, valuesBatch[i]);
            outGroupIds[current] = 0;
        }
        else {
            toProcess[remainingProcess] = current;
            remainingProcess++;
        }
        return remainingProcess & 1;
    }

    private void getInitialGroupsUnroll(long[] outGroupIds, long[] valuesBatch, int[] positions, int[] toProcess, int toProcessCount)
    {
        int[] groupIds = this.groupIds;

        int i = 0;
        for (; i + 3 < toProcessCount; i += 4) {
            int current0 = toProcess[i];
            int position0 = positions[i];
            int current1 = toProcess[i + 1];
            int position1 = positions[i + 1];
            int current2 = toProcess[i + 2];
            int position2 = positions[i + 2];
            int current3 = toProcess[i + 3];
            int position3 = positions[i + 3];
            outGroupIds[current0] = groupIds[position0];
            outGroupIds[current1] = groupIds[position1];
            outGroupIds[current2] = groupIds[position2];
            outGroupIds[current3] = groupIds[position3];

            if (outGroupIds[current0] == -1) {
                outGroupIds[current0] = addNewGroup(position0, valuesBatch[i]);
            }
            if (outGroupIds[current1] == -1) {
                outGroupIds[current1] = addNewGroup(position1, valuesBatch[i + 1]);
            }
            if (outGroupIds[current2] == -1) {
                outGroupIds[current2] = addNewGroup(position2, valuesBatch[i + 2]);
            }
            if (outGroupIds[current3] == -1) {
                outGroupIds[current3] = addNewGroup(position3, valuesBatch[i + 3]);
            }
        }

//        Verify.verify(i == toProcessCount, "%s != %s", i, toProcessCount);
        for (; i < toProcessCount; i++) {
            int current = toProcess[i];
            int position = positions[i];
            outGroupIds[current] = groupIds[position];
            if (outGroupIds[current] == -1) {
                outGroupIds[current] = addNewGroup(position, valuesBatch[i]);
            }
        }
    }

    private void getInitialGroupsStandard(long[] outGroupIds, long[] valuesBatch, int[] positions, int[] toProcess, int toProcessCount)
    {
        int[] groupIds = this.groupIds;

        for (int i = 0; i < toProcessCount; i++) {
            int current = toProcess[i];
            int position = positions[i];
            outGroupIds[current] = groupIds[position];
            if (outGroupIds[current] == -1) {
                outGroupIds[current] = addNewGroup(position, valuesBatch[i]);
            }
        }
    }

    private void getPositions(long[] valuesBatch, int[] positions)
    {
        for (int i = 0; i < valuesBatch.length; i++) {
            positions[i] = (int) (murmurHash3(valuesBatch[i]) & mask);
        }
    }

    private int addNewGroup(int hashPosition, long value)
    {
        // record group id in hash
        int groupId = nextGroupId++;

        values[hashPosition] = value;
        valuesByGroupId[groupId] = value;
        groupIds[hashPosition] = groupId;

        // increase capacity, if necessary
        if (needRehash()) {
            tryRehash();
        }
        return groupId;
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

        int newMask = newCapacity - 1;
        long[] newValues = new long[newCapacity];

        int[] newGroupIds = new int[newCapacity];
        Arrays.fill(newGroupIds, -1);

        for (int groupId = 0; groupId < nextGroupId; groupId++) {
            if (groupId == nullGroupId) {
                continue;
            }
            long value = valuesByGroupId[groupId];

            // find an empty slot for the address
            int hashPosition = getHashPosition(value, newMask);
            while (newGroupIds[hashPosition] != -1) {
                hashPosition = (hashPosition + 1) & newMask;
                hashCollisions++;
            }

            // record the mapping
            newValues[hashPosition] = value;
            newGroupIds[hashPosition] = groupId;
        }

        mask = newMask;
        hashCapacity = newCapacity;
        maxFill = calculateMaxFill(hashCapacity);
        values = newValues;
        groupIds = newGroupIds;

        valuesByGroupId = Arrays.copyOf(valuesByGroupId, maxFill);
        return true;
    }

    private boolean needRehash()
    {
        return nextGroupId >= maxFill;
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
            return new GroupByIdBlock(nextGroupId, new LongArrayBlock(block.getPositionCount(), Optional.empty(), outGroupIds));
        }
    }
}
