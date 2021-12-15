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
import io.trino.array.LongBigArray;
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
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
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

public class BigintGroupByHashInlineGIDBigArray
        implements GroupByHash
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BigintGroupByHashInlineGIDBigArray.class).instanceSize();

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
    private LongBigArray hashTable;
    private int hashTableSize;
    private LongBigArray groupToValue;

    // groupId for the null value
    private int nullGroupId = -1;
    // group id for the value 0
    private int zeroKeyGroupId = -1;

    private DictionaryLookBack dictionaryLookBack;
    private long hashCollisions;
    private double expectedHashCollisions;

    // reserve enough memory before rehash
    private final UpdateMemory updateMemory;
    private long preallocatedMemoryInBytes;
    private long currentPageSizeInBytes;
    private int hashTableLength;

    public BigintGroupByHashInlineGIDBigArray(int hashChannel, boolean outputRawHash, int expectedSize, UpdateMemory updateMemory)
    {
        checkArgument(hashChannel >= 0, "hashChannel must be at least zero");
        checkArgument(expectedSize > 0, "expectedSize must be greater than zero");

        this.hashChannel = hashChannel;
        this.outputRawHash = outputRawHash;

        hashCapacity = arraySize(expectedSize, FILL_RATIO);

        maxFill = calculateMaxFill(hashCapacity);
        mask = hashCapacity - 1;
        hashTable = new LongBigArray();
        hashTableLength = toIntExact(2L * (hashCapacity + 1));
        hashTable.ensureCapacity(hashTableLength); // + 1 is for value 0 if present
        groupToValue = new LongBigArray();
        groupToValue.ensureCapacity(hashCapacity);

        // This interface is used for actively reserving memory (push model) for rehash.
        // The caller can also query memory usage on this object (pull model)
        this.updateMemory = requireNonNull(updateMemory, "updateMemory is null");
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                hashTable.sizeOf() +
                groupToValue.sizeOf() +
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
                BigintGroupByHashInlineGIDBigArray.this.appendValuesTo(groupToValue.get(currentGroupId), currentGroupId, pageBuilder, outputChannelOffset);
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
        LongBigArray packedHashTable = new LongBigArray();
        long packedLength = hashTableSize * 2L;
        packedHashTable.ensureCapacity(packedLength);
        int packedIndex = 0;
        for (int i = 0; i < hashTableLength; i += 2) {
            if (hashTable.get(i) != 0) {
                packedHashTable.set(packedIndex, hashTable.get(i));
                packedHashTable.set(packedIndex, hashTable.get(i + 1));
                packedIndex++;
            }
        }
        if (nullGroupId > 0) {
            packedHashTable.set(packedLength - 1, nullGroupId);
        }
        quickSort(
                0,
                hashTableSize,
                (left, right) -> Long.compare(
                        BigintType.hash(packedHashTable.get(left * 2L)),
                        BigintType.hash(packedHashTable.get(right * 2L))),
                (a, b) -> {
                    long tempValue = packedHashTable.get(a);
                    long tempGroupId = packedHashTable.get(a + 1);
                    packedHashTable.set(a, packedHashTable.get(b));
                    packedHashTable.set(a + 1, packedHashTable.get(b + 1));
                    packedHashTable.set(b, tempValue);
                    packedHashTable.set(b + 1, tempGroupId);
                });
        return new GroupCursor()
        {
            int i = -1;

            @Override
            public boolean hasNext()
            {
                return i + 1 < packedLength;
            }

            @Override
            public void next()
            {
                i++;
            }

            @Override
            public void appendValuesTo(PageBuilder pageBuilder, int outputChannelOffset)
            {
                BigintGroupByHashInlineGIDBigArray.this.appendValuesTo(packedHashTable.get(i), (int) packedHashTable.get(i + 1), pageBuilder, outputChannelOffset);
            }

            @Override
            public int getGroupId()
            {
                return (int) packedHashTable.get(i + 1);
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
        appendValuesTo(groupId == nullGroupId ? -1 : hashTable.get(hashPosition), groupId, pageBuilder, outputChannelOffset);
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
        if (value == 0) {
            return zeroKeyGroupId >= 0;
        }
        int hashPosition = getHashPosition(value, mask);

        // look for an empty slot or a slot containing this key
        while (true) {
            long current = hashTable.get(hashPosition);
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
                nullGroupId = hashTableSize++;
            }
            return nullGroupId;
        }

        long value = BIGINT.getLong(block, position);
        if (value == 0) {
            if (zeroKeyGroupId < 0) {
                zeroKeyGroupId = hashTableSize++;
                hashTable.set(hashTableLength - 1, zeroKeyGroupId);
            }
            return zeroKeyGroupId;
        }

        int hashPosition = getHashPosition(value, mask);
        // look for an empty slot or a slot containing this key
        while (true) {
            long current = hashTable.get(hashPosition);

            if (current == 0) {
                // empty slot found
                hashTable.set(hashPosition, value);
                int groupId = hashTableSize++;
                hashTable.set(hashPosition + 1, groupId);
                groupToValue.set(groupId, value);
                if (needRehash()) {
                    tryRehash();
                }
//                System.out.println(hashPosition + ": v=" + value + ", " + hashTableSize);
                return groupId;
            }
            if (value == current) {
                return (int) hashTable.get(hashPosition + 1);
            }

            hashPosition = hashPosition + 2;
            if (hashPosition >= hashCapacity * 2) {
                hashPosition = 0;
            }
            hashCollisions++;
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

        LongBigArray newHashTable = new LongBigArray();
        int newHashTableLength = 2 * newCapacity + 1;
        newHashTable.ensureCapacity(newHashTableLength);
        int newMask = newCapacity - 1;
        for (int i = 0; i < hashCapacity * 2; i += 2) {
            long value = hashTable.get(i);
            long groupId = hashTable.get(i + 1);
            if (groupId == nullGroupId) {
                continue;
            }
            int hashPosition = getHashPosition(value, newMask);
            // look for an empty slot or a slot containing this key
            while (newHashTable.get(hashPosition) != 0) {
                hashPosition = hashPosition + 2;
                if (hashPosition >= newCapacity * 2) {
                    hashPosition = 0;
                }
                hashCollisions++;
            }
            newHashTable.set(hashPosition, value);
            newHashTable.set(hashPosition + 1, groupId);
//            System.out.println("rehash " + i + " -> " + hashPosition + ": v=" + value + ", " + groupId);
        }
        if (zeroKeyGroupId > 0) {
            newHashTable.set(newHashTableLength - 1, zeroKeyGroupId);
        }

        this.mask = newMask;
        hashCapacity = newCapacity;
        hashTable = newHashTable;
        hashTableLength = newHashTableLength;
        maxFill = calculateMaxFill(hashCapacity);

        groupToValue.ensureCapacity(hashCapacity);
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

            // putIfAbsent will rehash automatically if rehash is needed, unless there isn't enough memory to do so.
            // Therefore needRehash will not generally return true even if we have just crossed the capacity boundary.
            while (lastPosition < positionCount && !needRehash()) {
                // get the group for the current row
                putIfAbsent(lastPosition, block);
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

    private class AddDictionaryPageWork
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
            checkState(lastPosition < positionCount, "position count out of bound");

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

    private class AddRunLengthEncodedPageWork
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

    private class GetGroupIdsWork
            implements Work<GroupByIdBlock>
    {
        private final BlockBuilder blockBuilder;
        private final Block block;

        private boolean finished;
        private int lastPosition;

        public GetGroupIdsWork(Block block)
        {
            this.block = requireNonNull(block, "block is null");
            // we know the exact size required for the block
            this.blockBuilder = BIGINT.createFixedSizeBlockBuilder(block.getPositionCount());
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

            // putIfAbsent will rehash automatically if rehash is needed, unless there isn't enough memory to do so.
            // Therefore needRehash will not generally return true even if we have just crossed the capacity boundary.
            while (lastPosition < positionCount && !needRehash()) {
                // output the group id for this row
                BIGINT.writeLong(blockBuilder, putIfAbsent(lastPosition, block));
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
            return new GroupByIdBlock(getGroupCount(), blockBuilder.build());
        }
    }

    private class GetDictionaryGroupIdsWork
            implements Work<GroupByIdBlock>
    {
        private final BlockBuilder blockBuilder;
        private final Block dictionary;
        private final DictionaryBlock block;

        private boolean finished;
        private int lastPosition;

        public GetDictionaryGroupIdsWork(DictionaryBlock block)
        {
            this.block = requireNonNull(block, "block is null");
            this.dictionary = block.getDictionary();
            updateDictionaryLookBack(dictionary);

            // we know the exact size required for the block
            this.blockBuilder = BIGINT.createFixedSizeBlockBuilder(block.getPositionCount());
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

            // putIfAbsent will rehash automatically if rehash is needed, unless there isn't enough memory to do so.
            // Therefore needRehash will not generally return true even if we have just crossed the capacity boundary.
            while (lastPosition < positionCount && !needRehash()) {
                int positionInDictionary = block.getId(lastPosition);
                int groupId = registerGroupId(dictionary, positionInDictionary);
                BIGINT.writeLong(blockBuilder, groupId);
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
            return new GroupByIdBlock(getGroupCount(), blockBuilder.build());
        }
    }

    private class GetRunLengthEncodedGroupIdsWork
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
                    getGroupCount(),
                    new RunLengthEncodedBlock(
                            BIGINT.createFixedSizeBlockBuilder(1).writeLong(groupId).build(),
                            block.getPositionCount()));
        }
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
