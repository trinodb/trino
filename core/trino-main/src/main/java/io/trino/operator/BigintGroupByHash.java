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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.type.TypeUtils.NULL_HASH_CODE;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class BigintGroupByHash
        implements GroupByHash
{
    private static final int INSTANCE_SIZE = instanceSize(BigintGroupByHash.class);
    private static final int BATCH_SIZE = 1024;

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
    private DictionaryLookBack dictionaryLookBack;

    // reserve enough memory before rehash
    private final UpdateMemory updateMemory;
    private long preallocatedMemoryInBytes;
    private long currentPageSizeInBytes;

    public BigintGroupByHash(int hashChannel, boolean outputRawHash, int expectedSize, UpdateMemory updateMemory)
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
    public List<Type> getTypes()
    {
        return outputRawHash ? TYPES_WITH_RAW_HASH : TYPES;
    }

    @Override
    public int getGroupCount()
    {
        return nextGroupId;
    }

    @Override
    public void appendValuesTo(int groupId, PageBuilder pageBuilder)
    {
        checkArgument(groupId >= 0, "groupId is negative");
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
        if (groupId == nullGroupId) {
            blockBuilder.appendNull();
        }
        else {
            BIGINT.writeLong(blockBuilder, valuesByGroupId[groupId]);
        }

        if (outputRawHash) {
            BlockBuilder hashBlockBuilder = pageBuilder.getBlockBuilder(1);
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
            return new AddRunLengthEncodedPageWork((RunLengthEncodedBlock) block);
        }
        if (block instanceof DictionaryBlock) {
            return new AddDictionaryPageWork((DictionaryBlock) block);
        }

        return new AddPageWork(block);
    }

    @Override
    public Work<GroupByIdBlock> getGroupIds(Page page)
    {
        currentPageSizeInBytes = page.getRetainedSizeInBytes();
        Block block = page.getBlock(hashChannel);
        if (block instanceof RunLengthEncodedBlock) {
            return new GetRunLengthEncodedGroupIdsWork((RunLengthEncodedBlock) block);
        }
        if (block instanceof DictionaryBlock) {
            return new GetDictionaryGroupIdsWork((DictionaryBlock) block);
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

    @Override
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

    private int putIfAbsent(int position, Block block)
    {
        if (block.isNull(position)) {
            if (nullGroupId < 0) {
                // set null group id
                nullGroupId = nextGroupId++;
            }

            return nullGroupId;
        }

        long value = BIGINT.getLong(block, position);
        int hashPosition = getHashPosition(value, mask);

        // look for an empty slot or a slot containing this key
        while (true) {
            int groupId = groupIds[hashPosition];
            if (groupId == -1) {
                break;
            }

            if (value == values[hashPosition]) {
                return groupId;
            }

            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
        }

        return addNewGroup(hashPosition, value);
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
        if (newCapacityLong > Integer.MAX_VALUE) {
            throw new TrinoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }
        int newCapacity = toIntExact(newCapacityLong);

        // An estimate of how much extra memory is needed before we can go ahead and expand the hash table.
        // This includes the new capacity for values, groupIds, and valuesByGroupId as well as the size of the current page
        preallocatedMemoryInBytes = newCapacity * (long) (Long.BYTES + Integer.BYTES) + ((long) calculateMaxFill(newCapacity)) * Long.BYTES + currentPageSizeInBytes;
        if (!updateMemory.update()) {
            // reserved memory but has exceeded the limit
            return false;
        }

        int newMask = newCapacity - 1;
        long[] newValues = new long[newCapacity];
        int[] newGroupIds = new int[newCapacity];
        Arrays.fill(newGroupIds, -1);

        for (int i = 0; i < values.length; i++) {
            int groupId = groupIds[i];

            if (groupId != -1) {
                long value = values[i];
                int hashPosition = getHashPosition(value, newMask);

                // find an empty slot for the address
                while (newGroupIds[hashPosition] != -1) {
                    hashPosition = (hashPosition + 1) & newMask;
                }

                // record the mapping
                newValues[hashPosition] = value;
                newGroupIds[hashPosition] = groupId;
            }
        }

        mask = newMask;
        hashCapacity = newCapacity;
        maxFill = calculateMaxFill(hashCapacity);
        values = newValues;
        groupIds = newGroupIds;

        this.valuesByGroupId = Arrays.copyOf(valuesByGroupId, maxFill);

        preallocatedMemoryInBytes = 0;
        // release temporary memory reservation
        updateMemory.update();
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

    private void updateDictionaryLookBack(Block dictionary)
    {
        if (dictionaryLookBack == null || dictionaryLookBack.getDictionary() != dictionary) {
            dictionaryLookBack = new DictionaryLookBack(dictionary);
        }
    }

    private int registerGroupId(Block dictionary, int positionInDictionary)
    {
        if (dictionaryLookBack.isProcessed(positionInDictionary)) {
            return dictionaryLookBack.getGroupId(positionInDictionary);
        }

        int groupId = putIfAbsent(positionInDictionary, dictionary);
        dictionaryLookBack.setProcessed(positionInDictionary, groupId);
        return groupId;
    }

    @VisibleForTesting
    class AddPageWork
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
            checkState(lastPosition <= positionCount, "position count out of bound");
            int remainingPositions = positionCount - lastPosition;

            while (remainingPositions != 0) {
                int batchSize = min(remainingPositions, BATCH_SIZE);
                if (!ensureHashTableSize(batchSize)) {
                    return false;
                }

                for (int i = lastPosition; i < lastPosition + batchSize; i++) {
                    putIfAbsent(i, block);
                }

                lastPosition += batchSize;
                remainingPositions -= batchSize;
            }
            verify(lastPosition == positionCount);
            return true;
        }

        @Override
        public Void getResult()
        {
            throw new UnsupportedOperationException();
        }
    }

    @VisibleForTesting
    class AddDictionaryPageWork
            implements Work<Void>
    {
        private final Block dictionary;
        private final DictionaryBlock block;

        private int lastPosition;

        public AddDictionaryPageWork(DictionaryBlock block)
        {
            this.block = requireNonNull(block, "block is null");
            this.dictionary = block.getDictionary();
            updateDictionaryLookBack(dictionary);
        }

        @Override
        public boolean process()
        {
            int positionCount = block.getPositionCount();
            checkState(lastPosition <= positionCount, "position count out of bound");

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (needRehash() && !tryRehash()) {
                return false;
            }

            // putIfAbsent will rehash automatically if rehash is needed, unless there isn't enough memory to do so.
            // Therefore needRehash will not generally return true even if we have just crossed the capacity boundary.
            while (lastPosition < positionCount && !needRehash()) {
                int positionInDictionary = block.getId(lastPosition);
                registerGroupId(dictionary, positionInDictionary);
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

    @VisibleForTesting
    class AddRunLengthEncodedPageWork
            implements Work<Void>
    {
        private final RunLengthEncodedBlock block;

        private boolean finished;

        public AddRunLengthEncodedPageWork(RunLengthEncodedBlock block)
        {
            this.block = requireNonNull(block, "block is null");
        }

        @Override
        public boolean process()
        {
            checkState(!finished);
            if (block.getPositionCount() == 0) {
                finished = true;
                return true;
            }

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (needRehash() && !tryRehash()) {
                return false;
            }

            // Only needs to process the first row since it is Run Length Encoded
            putIfAbsent(0, block.getValue());
            finished = true;

            return true;
        }

        @Override
        public Void getResult()
        {
            throw new UnsupportedOperationException();
        }
    }

    @VisibleForTesting
    class GetGroupIdsWork
            implements Work<GroupByIdBlock>
    {
        private final long[] groupIds;
        private final Block block;

        private boolean finished;
        private int lastPosition;

        public GetGroupIdsWork(Block block)
        {
            this.block = requireNonNull(block, "block is null");
            this.groupIds = new long[block.getPositionCount()];
        }

        @Override
        public boolean process()
        {
            int positionCount = block.getPositionCount();
            checkState(lastPosition <= positionCount, "position count out of bound");
            checkState(!finished);

            int remainingPositions = positionCount - lastPosition;

            while (remainingPositions != 0) {
                int batchSize = min(remainingPositions, BATCH_SIZE);
                if (!ensureHashTableSize(batchSize)) {
                    return false;
                }

                for (int i = lastPosition; i < lastPosition + batchSize; i++) {
                    // output the group id for this row
                    groupIds[i] = putIfAbsent(i, block);
                }

                lastPosition += batchSize;
                remainingPositions -= batchSize;
            }
            verify(lastPosition == positionCount);
            return true;
        }

        @Override
        public GroupByIdBlock getResult()
        {
            checkState(lastPosition == block.getPositionCount(), "process has not yet finished");
            checkState(!finished, "result has produced");
            finished = true;
            return new GroupByIdBlock(nextGroupId, new LongArrayBlock(block.getPositionCount(), Optional.empty(), groupIds));
        }
    }

    @VisibleForTesting
    class GetDictionaryGroupIdsWork
            implements Work<GroupByIdBlock>
    {
        private final long[] groupIds;
        private final Block dictionary;
        private final DictionaryBlock block;

        private boolean finished;
        private int lastPosition;

        public GetDictionaryGroupIdsWork(DictionaryBlock block)
        {
            this.block = requireNonNull(block, "block is null");
            this.dictionary = block.getDictionary();
            updateDictionaryLookBack(dictionary);

            this.groupIds = new long[block.getPositionCount()];
        }

        @Override
        public boolean process()
        {
            int positionCount = block.getPositionCount();
            checkState(lastPosition <= positionCount, "position count out of bound");
            checkState(!finished);

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (needRehash() && !tryRehash()) {
                return false;
            }

            // putIfAbsent will rehash automatically if rehash is needed, unless there isn't enough memory to do so.
            // Therefore needRehash will not generally return true even if we have just crossed the capacity boundary.
            while (lastPosition < positionCount && !needRehash()) {
                int positionInDictionary = block.getId(lastPosition);
                int groupId = registerGroupId(dictionary, positionInDictionary);
                groupIds[lastPosition] = groupId;
                lastPosition++;
            }
            return lastPosition == positionCount;
        }

        @Override
        public GroupByIdBlock getResult()
        {
            checkState(lastPosition == block.getPositionCount(), "process has not yet finished");
            checkState(!finished, "result has produced");
            finished = true;
            return new GroupByIdBlock(nextGroupId, new LongArrayBlock(block.getPositionCount(), Optional.empty(), groupIds));
        }
    }

    @VisibleForTesting
    class GetRunLengthEncodedGroupIdsWork
            implements Work<GroupByIdBlock>
    {
        private final RunLengthEncodedBlock block;

        int groupId = -1;
        private boolean processFinished;
        private boolean resultProduced;

        public GetRunLengthEncodedGroupIdsWork(RunLengthEncodedBlock block)
        {
            this.block = requireNonNull(block, "block is null");
        }

        @Override
        public boolean process()
        {
            checkState(!processFinished);
            if (block.getPositionCount() == 0) {
                processFinished = true;
                return true;
            }

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (needRehash() && !tryRehash()) {
                return false;
            }

            // Only needs to process the first row since it is Run Length Encoded
            groupId = putIfAbsent(0, block.getValue());
            processFinished = true;
            return true;
        }

        @Override
        public GroupByIdBlock getResult()
        {
            checkState(processFinished);
            checkState(!resultProduced);
            resultProduced = true;

            return new GroupByIdBlock(
                    nextGroupId,
                    RunLengthEncodedBlock.create(
                            BIGINT.createFixedSizeBlockBuilder(1).writeLong(groupId).build(),
                            block.getPositionCount()));
        }
    }

    private boolean ensureHashTableSize(int batchSize)
    {
        int positionCountUntilRehash = maxFill - nextGroupId;
        while (positionCountUntilRehash < batchSize) {
            if (!tryRehash()) {
                return false;
            }
            positionCountUntilRehash = maxFill - nextGroupId;
        }
        return true;
    }

    private static final class DictionaryLookBack
    {
        private final Block dictionary;
        private final int[] processed;

        public DictionaryLookBack(Block dictionary)
        {
            this.dictionary = dictionary;
            this.processed = new int[dictionary.getPositionCount()];
            Arrays.fill(processed, -1);
        }

        public Block getDictionary()
        {
            return dictionary;
        }

        public int getGroupId(int position)
        {
            return processed[position];
        }

        public boolean isProcessed(int position)
        {
            return processed[position] != -1;
        }

        public void setProcessed(int position, int groupId)
        {
            processed[position] = groupId;
        }
    }
}
