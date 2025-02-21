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

import com.google.common.primitives.Ints;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.SizeOf.sizeOfObjectArray;
import static io.trino.operator.FlatHash.sumExact;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static java.lang.Math.addExact;
import static java.lang.Math.clamp;
import static java.lang.Math.max;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Objects.checkIndex;
import static java.util.Objects.requireNonNull;

public final class MinimalFlatHash
{
    private static final double DEFAULT_LOAD_FACTOR = 15.0 / 16;

    private static int computeCapacity(int maxSize, double loadFactor)
    {
        int capacity = (int) (maxSize / loadFactor);
        return max(toIntExact(1L << (64 - Long.numberOfLeadingZeros(capacity - 1))), 16);
    }

    private static int calculateMaxFill(int capacity)
    {
        return toIntExact(capacity * 15L / 16);
    }

    private static final int RECORDS_PER_GROUP_SHIFT = 10;
    private static final int RECORDS_PER_GROUP = 1 << RECORDS_PER_GROUP_SHIFT;
    private static final int RECORDS_PER_GROUP_MASK = RECORDS_PER_GROUP - 1;

    private static final int VECTOR_LENGTH = Long.BYTES;
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, LITTLE_ENDIAN);

    private final MinimalFlatHashStrategy flatHashStrategy;
    private final MinimalVariableWidthData variableWidthData;
    private final UpdateMemory checkMemoryReservation;

    private byte[] control;
    private int[] groupIdsByHash;
    private byte[][] fixedSizeRecords;

    private final int fixedRecordSize;
    private final int variableWidthOffset;
    private final int fixedValueOffset;

    private int capacity;
    private int mask;
    private int nextGroupId;
    private int maxFill;

    public MinimalFlatHash(MinimalFlatHashStrategy flatHashStrategy, int expectedSize, UpdateMemory checkMemoryReservation)
    {
        this.flatHashStrategy = requireNonNull(flatHashStrategy, "flatHashStrategy is null");
        this.checkMemoryReservation = requireNonNull(checkMemoryReservation, "checkMemoryReservation is null");
        boolean hasVariableData = flatHashStrategy.isAnyVariableWidth();
        this.variableWidthData = hasVariableData ? new MinimalVariableWidthData() : null;

        this.capacity = max(VECTOR_LENGTH, computeCapacity(expectedSize, DEFAULT_LOAD_FACTOR));
        this.mask = capacity - 1;
        this.control = new byte[capacity + VECTOR_LENGTH];
        this.groupIdsByHash = new int[capacity];
        Arrays.fill(groupIdsByHash, -1);
        this.maxFill = calculateMaxFill(capacity);

        int groupsRequired = recordGroupsRequiredForCapacity(capacity);
        this.fixedSizeRecords = new byte[groupsRequired][];

        // the record is laid out as follows:
        // 1. raw hash (long)
        // 2. optional variable width pointer (int chunkIndex, int chunkOffset)
        // 3. fixed data for each type
        this.variableWidthOffset = Long.BYTES;
        this.fixedValueOffset = variableWidthOffset + (hasVariableData ? MinimalVariableWidthData.POINTER_SIZE : 0);
        this.fixedRecordSize = fixedValueOffset + flatHashStrategy.getTotalFlatFixedLength();
    }

    public int size()
    {
        return nextGroupId;
    }

    public int getCapacity()
    {
        return capacity;
    }

    public long hashPosition(int groupId)
    {
        checkArgument(groupId < nextGroupId, "groupId out of range");
        int recordOffset = getFixedRecordOffset(groupId);
        byte[] records = getFixedSizeRecords(groupId);
        return (long) LONG_HANDLE.get(records, recordOffset);
    }

    public void appendTo(int groupId, BlockBuilder[] blockBuilders)
    {
        checkArgument(groupId < nextGroupId, "groupId out of range");

        byte[] fixedSizeRecords = getFixedSizeRecords(groupId);
        int recordOffset = getFixedRecordOffset(groupId);

        byte[] variableWidthChunk = null;
        int variableChunkOffset = 0;
        if (variableWidthData != null) {
            variableWidthChunk = variableWidthData.getChunk(fixedSizeRecords, recordOffset + variableWidthOffset);
            variableChunkOffset = MinimalVariableWidthData.getChunkOffset(fixedSizeRecords, recordOffset + variableWidthOffset);
        }

        flatHashStrategy.readFlat(
                fixedSizeRecords,
                recordOffset + fixedValueOffset,
                variableWidthChunk,
                variableChunkOffset,
                blockBuilders);
    }

    public boolean contains(Block[] blocks, int position)
    {
        return contains(blocks, position, flatHashStrategy.hash(blocks, position));
    }

    public boolean contains(Block[] blocks, int position, long hash)
    {
        return getIndex(blocks, position, hash) >= 0;
    }

    public void computeHashes(Block[] blocks, long[] hashes, int offset, int length)
    {
        flatHashStrategy.hashBlocksBatched(blocks, hashes, offset, length);
    }

    public int putIfAbsent(Block[] blocks, int position)
    {
        return putIfAbsent(blocks, position, flatHashStrategy.hash(blocks, position));
    }

    public int putIfAbsent(Block[] blocks, int position, long hash)
    {
        int index = getIndex(blocks, position, hash);
        if (index >= 0) {
            int groupId = groupIdsByHash[index];
            if (groupId < 0) {
                throw new IllegalStateException("groupId out of range");
            }
            return groupId;
        }

        index = -index - 1;
        int groupId = addNewGroup(index, blocks, position, hash);
        if (nextGroupId >= maxFill) {
            rehash(0);
        }
        return groupId;
    }

    private int getIndex(Block[] blocks, int position, long hash)
    {
        byte hashPrefix = (byte) (hash & 0x7F | 0x80);
        int bucket = bucket((int) hash >> 7);

        int step = 1;
        long repeated = repeat(hashPrefix);

        while (true) {
            final long controlVector = (long) LONG_HANDLE.get(control, bucket);

            int matchIndex = matchInVector(blocks, position, hash, bucket, repeated, controlVector);
            if (matchIndex >= 0) {
                return matchIndex;
            }

            int emptyIndex = findEmptyInVector(controlVector, bucket);
            if (emptyIndex >= 0) {
                return -emptyIndex - 1;
            }

            bucket = bucket(bucket + step);
            step += VECTOR_LENGTH;
        }
    }

    private int matchInVector(Block[] blocks, int position, long hash, int vectorStartBucket, long repeated, long controlVector)
    {
        long controlMatches = match(controlVector, repeated);
        while (controlMatches != 0) {
            int index = bucket(vectorStartBucket + (Long.numberOfTrailingZeros(controlMatches) >>> 3));
            int groupId = groupIdsByHash[index];
            if (valueIdentical(groupId, blocks, position, hash)) {
                return index;
            }

            controlMatches = controlMatches & (controlMatches - 1);
        }
        return -1;
    }

    private int findEmptyInVector(long vector, int vectorStartBucket)
    {
        long controlMatches = match(vector, 0x00_00_00_00_00_00_00_00L);
        if (controlMatches == 0) {
            return -1;
        }
        int slot = Long.numberOfTrailingZeros(controlMatches) >>> 3;
        return bucket(vectorStartBucket + slot);
    }

    private int addNewGroup(int index, Block[] blocks, int position, long hash)
    {
        setControl(index, (byte) (hash & 0x7F | 0x80));
        int groupId = nextGroupId++;
        groupIdsByHash[index] = groupId;
        int fixedRecordOffset = getFixedRecordOffset(groupId);
        if (fixedRecordOffset == 0) {
            // new record batch start, populate the record batch fields
            fixedSizeRecords[recordGroupIndexForGroupId(groupId)] = new byte[multiplyExact(capacity, fixedRecordSize)];
        }
        byte[] fixedSizeRecords = getFixedSizeRecords(groupId);
        LONG_HANDLE.set(fixedSizeRecords, fixedRecordOffset, hash);

        byte[] variableWidthChunk = null;
        int variableWidthChunkOffset = 0;
        if (variableWidthData != null) {
            int variableWidthSize = flatHashStrategy.getTotalVariableWidth(blocks, position);
            variableWidthChunk = variableWidthData.allocate(fixedSizeRecords, fixedRecordOffset + variableWidthOffset, variableWidthSize);
            variableWidthChunkOffset = MinimalVariableWidthData.getChunkOffset(fixedSizeRecords, fixedRecordOffset + variableWidthOffset);
        }

        flatHashStrategy.writeFlat(
                blocks,
                position,
                fixedSizeRecords,
                fixedRecordOffset + fixedValueOffset,
                variableWidthChunk,
                variableWidthChunkOffset);

        return groupId;
    }

    private void setControl(int index, byte hashPrefix)
    {
        control[index] = hashPrefix;
        if (index < VECTOR_LENGTH) {
            control[index + capacity] = hashPrefix;
        }
    }

    public boolean ensureAvailableCapacity(int batchSize)
    {
        long requiredMaxFill = nextGroupId + batchSize;
        if (requiredMaxFill >= maxFill) {
            long minimumRequiredCapacity = (requiredMaxFill + 1) * 16 / 15;
            return tryRehash(toIntExact(minimumRequiredCapacity));
        }
        return true;
    }

    private boolean tryRehash(int minimumRequiredCapacity)
    {
        int newCapacity = computeNewCapacity(minimumRequiredCapacity);

        // TODO: compute new hash table size before this check
        if (!checkMemoryReservation.update()) {
            return false;
        }

        rehash(minimumRequiredCapacity);
        return true;
    }

    private void rehash(int minimumRequiredCapacity)
    {
        capacity = computeNewCapacity(minimumRequiredCapacity);
        maxFill = calculateMaxFill(capacity);
        mask = capacity - 1;

        // Resize the record groups top level array to accommodate the new record groups
        fixedSizeRecords = Arrays.copyOf(fixedSizeRecords, recordGroupsRequiredForCapacity(capacity));

        // Construct the new hash table
        control = new byte[capacity + VECTOR_LENGTH];
        groupIdsByHash = new int[capacity];
        Arrays.fill(groupIdsByHash, -1);

        for (int groupId = 0; groupId < nextGroupId; groupId++) {
            byte[] fixedSizeRecords = getFixedSizeRecords(groupId);
            int fixedRecordOffset = getFixedRecordOffset(groupId);
            long hash = (long) LONG_HANDLE.get(fixedSizeRecords, fixedRecordOffset);

            byte hashPrefix = (byte) (hash & 0x7F | 0x80);
            int bucket = bucket((int) (hash >> 7));

            // getIndex is not used here because values in a rehash are always distinct
            int step = 1;
            while (true) {
                final long controlVector = (long) LONG_HANDLE.get(control, bucket);
                // values are already distinct, so just find the first empty slot
                int emptyIndex = findEmptyInVector(controlVector, bucket);
                if (emptyIndex >= 0) {
                    setControl(emptyIndex, hashPrefix);
                    if (groupIdsByHash[emptyIndex] != -1) {
                        throw new IllegalStateException("groupId mapping already exists at index");
                    }
                    groupIdsByHash[emptyIndex] = groupId;
                    break;
                }
                bucket = bucket(bucket + step);
                step += VECTOR_LENGTH;
            }
        }

        // TODO: memory usage calculations
        checkMemoryReservation.update();
    }

    private int bucket(int hash)
    {
        return hash & mask;
    }

    private static int recordGroupIndexForGroupId(int groupId)
    {
        return groupId >> RECORDS_PER_GROUP_SHIFT;
    }

    private byte[] getFixedSizeRecords(int groupId)
    {
        return fixedSizeRecords[recordGroupIndexForGroupId(groupId >> RECORDS_PER_GROUP_SHIFT)];
    }

    private int getFixedRecordOffset(int groupId)
    {
        return (groupId & RECORDS_PER_GROUP_MASK) * fixedRecordSize;
    }

    private boolean valueIdentical(int groupId, Block[] rightBlocks, int rightPosition, long rightHash)
    {
        checkArgument(groupId >= 0, "groupId is negative");
        byte[] fixedSizeRecords = getFixedSizeRecords(groupId);
        int fixedRecordsOffset = getFixedRecordOffset(groupId);

        if (rightHash != (long) LONG_HANDLE.get(fixedSizeRecords, fixedRecordsOffset)) {
            return false;
        }

        byte[] variableWidthChunk = null;
        int variableWidthChunkOffset = 0;
        if (variableWidthData != null) {
            variableWidthChunk = variableWidthData.getChunk(fixedSizeRecords, fixedRecordsOffset + variableWidthOffset);
            variableWidthChunkOffset = MinimalVariableWidthData.getChunkOffset(fixedSizeRecords, fixedRecordsOffset + variableWidthOffset);
        }

        return flatHashStrategy.valueIdentical(
                fixedSizeRecords,
                fixedRecordsOffset + fixedValueOffset,
                variableWidthChunk,
                variableWidthChunkOffset,
                rightBlocks,
                rightPosition);
    }

    private int computeNewCapacity(int minimumRequiredCapacity)
    {
        checkArgument(minimumRequiredCapacity >= 0, "minimumRequiredCapacity must be positive");
        long newCapacityLong = capacity * 2L;
        while (newCapacityLong < minimumRequiredCapacity) {
            newCapacityLong = multiplyExact(newCapacityLong, 2);
        }
        if (newCapacityLong > Integer.MAX_VALUE) {
            throw new TrinoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }
        return toIntExact(newCapacityLong);
    }

    private static long repeat(byte value)
    {
        return ((value & 0xFF) * 0x01_01_01_01_01_01_01_01L);
    }

    private static long match(long vector, long repeatedValue)
    {
        // HD 6-1
        long comparison = vector ^ repeatedValue;
        return (comparison - 0x01_01_01_01_01_01_01_01L) & ~comparison & 0x80_80_80_80_80_80_80_80L;
    }

    private static int recordGroupsRequiredForCapacity(int capacity)
    {
        checkArgument(capacity > 0, "capacity must be positive");
        return (capacity + 1) >> RECORDS_PER_GROUP_SHIFT;
    }

    private static int estimateNewVariableWidthCapacity(int previousUsedCapacity)
    {
        return (previousUsedCapacity / (RECORDS_PER_GROUP - 1)) * RECORDS_PER_GROUP;
    }

    private static int computeNewVariableWidthCapacity(int currentCapacity, int minimumRequiredCapacity)
    {
        checkArgument(minimumRequiredCapacity >= 0, "minimumRequiredCapacity must be positive");
        long newCapacityLong = currentCapacity * 2L;
        while (newCapacityLong < minimumRequiredCapacity) {
            newCapacityLong *= 2;
        }
        return toIntExact(newCapacityLong);
    }

    /**
     * Duplicated from {@link VariableWidthData}, except with {@link VariableWidthData#free} and the associated
     * length argument from the pointer removed since it takes extra space unnecessarily
     */
    private static final class MinimalVariableWidthData
    {
        private static final int INSTANCE_SIZE = instanceSize(MinimalVariableWidthData.class);

        public static final int MIN_CHUNK_SIZE = 1024;
        public static final int MAX_CHUNK_SIZE = 8 * 1024 * 1024;

        public static final int POINTER_SIZE = Integer.BYTES + Integer.BYTES;

        private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
        public static final byte[] EMPTY_CHUNK = new byte[0];

        private final List<byte[]> chunks = new ArrayList<>();
        private int openChunkOffset;

        private long chunksRetainedSizeInBytes;

        private long allocatedBytes;
        private long freeBytes;

        public MinimalVariableWidthData() {}

        public long getRetainedSizeBytes()
        {
            return sumExact(
                    INSTANCE_SIZE,
                    chunksRetainedSizeInBytes,
                    sizeOfObjectArray(chunks.size()));
        }

        public List<byte[]> getAllChunks()
        {
            return chunks;
        }

        public long getAllocatedBytes()
        {
            return allocatedBytes;
        }

        public long getFreeBytes()
        {
            return freeBytes;
        }

        public byte[] allocate(byte[] pointer, int pointerOffset, int size)
        {
            if (size == 0) {
                writePointer(pointer, pointerOffset, 0, 0);
                return EMPTY_CHUNK;
            }

            byte[] openChunk = chunks.isEmpty() ? EMPTY_CHUNK : chunks.getLast();
            if (openChunk.length - openChunkOffset < size) {
                // record unused space as free bytes
                freeBytes += (openChunk.length - openChunkOffset);

                // allocate enough space for 32 values of the current size, or double the current chunk size, whichever is larger
                int newSize = Ints.saturatedCast(max(size * 32L, openChunk.length * 2L));
                // constrain to be between min and max chunk size
                newSize = clamp(newSize, MIN_CHUNK_SIZE, MAX_CHUNK_SIZE);
                // jumbo rows get a separate allocation
                newSize = max(newSize, size);
                openChunk = new byte[newSize];
                chunks.add(openChunk);
                allocatedBytes += newSize;
                chunksRetainedSizeInBytes = addExact(chunksRetainedSizeInBytes, sizeOf(openChunk));
                openChunkOffset = 0;
            }

            writePointer(
                    pointer,
                    pointerOffset,
                    chunks.size() - 1,
                    openChunkOffset);
            openChunkOffset += size;
            return openChunk;
        }

        public byte[] getChunk(byte[] pointer, int pointerOffset)
        {
            int chunkIndex = getChunkIndex(pointer, pointerOffset);
            if (chunks.isEmpty()) {
                verify(chunkIndex == 0);
                return EMPTY_CHUNK;
            }
            checkIndex(chunkIndex, chunks.size());
            return chunks.get(chunkIndex);
        }

        private static int getChunkIndex(byte[] pointer, int pointerOffset)
        {
            return (int) INT_HANDLE.get(pointer, pointerOffset);
        }

        public static int getChunkOffset(byte[] pointer, int pointerOffset)
        {
            return (int) INT_HANDLE.get(pointer, pointerOffset + Integer.BYTES);
        }

        public static void writePointer(byte[] pointer, int pointerOffset, int chunkIndex, int chunkOffset)
        {
            INT_HANDLE.set(pointer, pointerOffset, chunkIndex);
            INT_HANDLE.set(pointer, pointerOffset + Integer.BYTES, chunkOffset);
        }
    }
}
