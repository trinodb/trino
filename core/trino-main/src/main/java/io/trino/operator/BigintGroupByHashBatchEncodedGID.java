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
import static it.unimi.dsi.fastutil.Arrays.quickSort;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static it.unimi.dsi.fastutil.HashCommon.nextPowerOfTwo;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class BigintGroupByHashBatchEncodedGID
        implements GroupByHash
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BigintGroupByHashBatchEncodedGID.class).instanceSize();

    private static final float FILL_RATIO = 0.75f;
    private static final List<Type> TYPES = ImmutableList.of(BIGINT);
    private static final List<Type> TYPES_WITH_RAW_HASH = ImmutableList.of(BIGINT, BIGINT);

    private final int hashChannel;
    private final boolean outputRawHash;

    private int hashCapacity;
    private int maxFill;
    private int mask;

    // the hash table with value + groupId entries.
    // external groupId is the position/index in this table and artificial groupId concatenated.
    private long[] hashTable;
    private int hashTableSize;
    private long[] groupToValue;

    // groupId for the null value
    private int nullGroupId = -1;
    // group id for the value 0
    private int zeroKeyGroupId = -1;

    //    private DictionaryLookBack dictionaryLookBack;
    private long hashCollisions;
    private double expectedHashCollisions;

    // reserve enough memory before rehash
    private final UpdateMemory updateMemory;
    private long preallocatedMemoryInBytes;
    private long currentPageSizeInBytes;

    public BigintGroupByHashBatchEncodedGID(int hashChannel, boolean outputRawHash, int expectedSize, UpdateMemory updateMemory)
    {
        checkArgument(hashChannel >= 0, "hashChannel must be at least zero");
        checkArgument(expectedSize > 0, "expectedSize must be greater than zero");

        this.hashChannel = hashChannel;
        this.outputRawHash = outputRawHash;

        hashCapacity = arraySize(expectedSize, FILL_RATIO);

        maxFill = calculateMaxFill(hashCapacity);
        mask = hashCapacity - 1;
        hashTable = new long[2 * (hashCapacity + 1)]; // + 1 is for value 0 if present
        groupToValue = new long[hashCapacity];

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
                BigintGroupByHashBatchEncodedGID.this.appendValuesTo(groupToValue[currentGroupId], currentGroupId, pageBuilder, outputChannelOffset);
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
        long[] packedHashTable = new long[hashTableSize * 2];
        int packedIndex = 0;
        for (int i = 0; i < hashTable.length; i += 2) {
            if (hashTable[i] != 0) {
                packedHashTable[packedIndex] = hashTable[i];
                packedHashTable[packedIndex + 1] = hashTable[i + 1];
                packedIndex++;
            }
        }
        if (nullGroupId > 0) {
            packedHashTable[packedHashTable.length - 1] = nullGroupId;
        }
        quickSort(
                0,
                hashTableSize,
                (left, right) -> Long.compare(
                        BigintType.hash(packedHashTable[left * 2]),
                        BigintType.hash(packedHashTable[right * 2])),
                (a, b) -> {
                    long tempValue = packedHashTable[a];
                    long tempGroupId = packedHashTable[a + 1];
                    packedHashTable[a] = packedHashTable[b];
                    packedHashTable[a + 1] = packedHashTable[b + 1];
                    packedHashTable[b] = tempValue;
                    packedHashTable[b + 1] = tempGroupId;
                });
        return new GroupCursor()
        {
            int i = -1;

            @Override
            public boolean hasNext()
            {
                return i + 1 < packedHashTable.length;
            }

            @Override
            public void next()
            {
                i++;
            }

            @Override
            public void appendValuesTo(PageBuilder pageBuilder, int outputChannelOffset)
            {
                BigintGroupByHashBatchEncodedGID.this.appendValuesTo(packedHashTable[i], (int) packedHashTable[i + 1], pageBuilder, outputChannelOffset);
            }

            @Override
            public int getGroupId()
            {
                return (int) packedHashTable[i + 1];
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

    public void appendValuesTo(int hashPosition, int groupId, PageBuilder pageBuilder, int outputChannelOffset)
    {
        appendValuesTo(groupId == nullGroupId ? -1 : hashTable[hashPosition], groupId, pageBuilder, outputChannelOffset);
    }

    public void appendValuesTo(long value, int groupId, PageBuilder pageBuilder, int outputChannelOffset)
    {
        checkArgument(groupId >= 0 || groupId == nullGroupId, "groupId is negative");
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset);
        if (groupId == nullGroupId) {
            blockBuilder.appendNull();
        }
        else {
            BIGINT.writeLong(blockBuilder, value);
        }

        if (outputRawHash) {
            BlockBuilder hashBlockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset + 1);
            if (groupId == nullGroupId) {
                BIGINT.writeLong(hashBlockBuilder, NULL_HASH_CODE);
            }
            else {
                BIGINT.writeLong(hashBlockBuilder, AbstractLongType.hash(value));
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
        if (value == 0) {
            return zeroKeyGroupId >= 0;
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

            hashPosition = hashPosition + 2;
            if (hashPosition >= hashCapacity * 2) {
                hashPosition = 0;
            }
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

        int endPosition = startPosition + length;

        long[] currentValues = new long[batchSize * 2];
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

        processPositions(block, currentValues, outGroupIds, valuesBatch, positions, toProcess, length);
    }

    private void processPositions(Block block, long[] currentValues, long[] outGroupIds, long[] valuesBatch, int[] positions, int[] toProcess, int toProcessCount)
    {
        do {
            int remainingProcess = 0;
            remainingProcess = processIteration(block, currentValues, outGroupIds, valuesBatch, positions, toProcess, toProcessCount, remainingProcess);
            toProcessCount = remainingProcess;
        }
        while (toProcessCount > 0);
    }

    private int processIteration(Block block, long[] currentValues, long[] outGroupIds, long[] valuesBatch, int[] positions, int[] toProcess, int toProcessCount, int remainingProcess)
    {
        long[] hashTable = this.hashTable;
        long[] groupToValue = this.groupToValue;
        fetchCurrentValues(currentValues, positions, toProcessCount, hashTable);
        // 64
        for (int i = 0; i < toProcessCount; i++) {
            int positionToProcess = toProcess[i];

            if (block.isNull(positionToProcess)) {
                nullValue(outGroupIds, positionToProcess);
            }
            else {
                int hashPosition = positions[i];
                long valueToProcess = valuesBatch[i];
                if (valueToProcess == 0) {
                    zeroValue(outGroupIds, hashTable, positionToProcess);
                }
                else {
                    long currentValue = hashTable[hashPosition];
                    if (currentValue == 0) {

                        emptySlotFound(outGroupIds, hashTable, groupToValue, positionToProcess, hashPosition, valueToProcess);

//                System.out.println(hashPosition + ": v=" + value + ", " + hashTableSize);
                    }
                    else {
                        if (valueToProcess != currentValue) {
                            remainingProcess = hashCollision(positions, toProcess, remainingProcess, positionToProcess, i, hashPosition, valueToProcess, valuesBatch);
                        }
                        else {
                            outGroupIds[positionToProcess] = hashTable[hashPosition + 1];
                        }
                    }
                }
            }
        }
        return remainingProcess;
    }

    private void nullValue(long[] outGroupIds, int positionToProcess)
    {
        if (nullGroupId < 0) {
            nullGroupId = hashTableSize++;
        }
        outGroupIds[positionToProcess] = nullGroupId;
    }

    private void zeroValue(long[] outGroupIds, long[] hashTable, int positionToProcess)
    {
        if (zeroKeyGroupId < 0) {
            zeroKeyGroupId = hashTableSize++;
            hashTable[hashTable.length - 1] = zeroKeyGroupId;
        }
        outGroupIds[positionToProcess] = zeroKeyGroupId;
    }

    private void emptySlotFound(long[] outGroupIds, long[] hashTable, long[] groupToValue, int positionToProcess, int hashPosition, long valueToProcess)
    {
        // empty slot found
        hashTable[hashPosition] = valueToProcess;
        int groupId = hashTableSize++;
        hashTable[hashPosition + 1] = groupId;

        groupToValue[groupId] = valueToProcess;
        outGroupIds[positionToProcess] = groupId;
        if (needRehash()) {
            tryRehash();
        }
    }

    private void fetchCurrentValues(long[] currentValues, int[] positions, int toProcessCount, long[] hashTable)
    {
        for (int i = 0; i < toProcessCount; i++) {
            int position = positions[i];
            currentValues[i] = hashTable[position];
            currentValues[i + 1] = hashTable[position + 1];
        }
    }

    private int hashCollision(int[] positions, int[] toProcess, int remainingProcess, int current, int i, int position, long valueTOProcess, long[] valuesBatch)
    {
        position = position + 2;
        if (position >= hashCapacity * 2) {
            position = 0;
        }
        positions[remainingProcess] = position;
        toProcess[remainingProcess] = current;
        valuesBatch[remainingProcess] = valueTOProcess;
        hashCollisions++;
        remainingProcess++;
        return remainingProcess;
    }

    private void getPositions(long[] valuesBatch, int length, int[] positions)
    {
        for (int i = 0; i < length; i++) {
            positions[i] = getHashPosition(valuesBatch[i], mask);
        }
    }

    private boolean tryRehash()
    {
        return tryRehash(hashCapacity * 2L);
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

        long[] newHashTable = new long[2 * newCapacity + 1];
        int newMask = newCapacity - 1;
        for (int i = 0; i < hashCapacity * 2; i += 2) {
            long value = hashTable[i];
            long groupId = hashTable[i + 1];
            if (groupId == nullGroupId) {
                continue;
            }
            int hashPosition = getHashPosition(value, newMask);
            // look for an empty slot or a slot containing this key
            while (newHashTable[hashPosition] != 0) {
                hashPosition = hashPosition + 2;
                if (hashPosition >= newCapacity * 2) {
                    hashPosition = 0;
                }
//                hashCollisions++;
            }
            newHashTable[hashPosition] = value;
            newHashTable[hashPosition + 1] = groupId;
//            System.out.println("rehash " + i + " -> " + hashPosition + ": v=" + value + ", " + groupId);
        }
        if (zeroKeyGroupId > 0) {
            newHashTable[newHashTable.length - 1] = zeroKeyGroupId;
        }

        this.mask = newMask;
        hashCapacity = newCapacity;
        hashTable = newHashTable;
        maxFill = calculateMaxFill(hashCapacity);
        groupToValue = Arrays.copyOf(groupToValue, hashCapacity);
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

    private static int getHashPosition(long rawHash, int mask)
    {
        return (int) (murmurHash3(rawHash) & mask) * 2;
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

            // TODO lysy: handle memory not available
            putIfAbsent(0, positionCount, block, new long[positionCount]);
            lastPosition = positionCount;
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

            // TODO lysy: handle memory not available
            putIfAbsent(0, positionCount, block, outGroupIds);
            lastPosition = positionCount;
            return lastPosition == positionCount;
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
