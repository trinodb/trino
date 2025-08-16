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

import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.operator.AppendOnlyVariableWidthData.getChunkOffset;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static java.lang.Math.addExact;
import static java.lang.Math.max;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Objects.requireNonNull;

public final class FlatHash
{
    private static final int INSTANCE_SIZE = instanceSize(FlatHash.class);

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

    private final FlatHashStrategy flatHashStrategy;
    private final AppendOnlyVariableWidthData variableWidthData;
    private final UpdateMemory checkMemoryReservation;

    private final boolean cacheHashValue;
    private final int fixedRecordSize;
    private final int variableWidthOffset;
    private final int fixedValueOffset;

    private byte[] control;
    private int[] groupIdsByHash;
    private byte[][] fixedSizeRecords;

    private long fixedRecordGroupsRetainedSize;
    private long temporaryRehashRetainedSize;
    private int capacity;
    private int mask;
    private int nextGroupId;
    private int maxFill;

    public FlatHash(FlatHashStrategy flatHashStrategy, GroupByHashMode hashMode, int expectedSize, UpdateMemory checkMemoryReservation)
    {
        this.flatHashStrategy = requireNonNull(flatHashStrategy, "flatHashStrategy is null");
        this.checkMemoryReservation = requireNonNull(checkMemoryReservation, "checkMemoryReservation is null");
        boolean hasVariableData = flatHashStrategy.isAnyVariableWidth();
        this.variableWidthData = hasVariableData ? new AppendOnlyVariableWidthData() : null;
        requireNonNull(hashMode, "hashMode is null");
        this.cacheHashValue = hashMode.isHashCached();

        // the record is laid out as follows:
        // 1. optional raw hash (long)
        // 2. optional variable width pointer (int chunkIndex, int chunkOffset)
        // 3. fixed data for each type
        this.variableWidthOffset = cacheHashValue ? Long.BYTES : 0;
        this.fixedValueOffset = variableWidthOffset + (hasVariableData ? AppendOnlyVariableWidthData.POINTER_SIZE : 0);
        this.fixedRecordSize = fixedValueOffset + flatHashStrategy.getTotalFlatFixedLength();

        this.capacity = max(VECTOR_LENGTH, computeCapacity(expectedSize, DEFAULT_LOAD_FACTOR));
        this.mask = capacity - 1;
        this.maxFill = calculateMaxFill(capacity);

        int groupsRequired = recordGroupsRequiredForCapacity(capacity);
        this.control = new byte[capacity + VECTOR_LENGTH];
        this.groupIdsByHash = new int[capacity];
        Arrays.fill(groupIdsByHash, -1);
        this.fixedSizeRecords = new byte[groupsRequired][];
    }

    public FlatHash(FlatHash other)
    {
        this.flatHashStrategy = other.flatHashStrategy;
        this.checkMemoryReservation = other.checkMemoryReservation;
        this.variableWidthData = other.variableWidthData == null ? null : new AppendOnlyVariableWidthData(other.variableWidthData);
        this.cacheHashValue = other.cacheHashValue;
        this.fixedRecordSize = other.fixedRecordSize;
        this.variableWidthOffset = other.variableWidthOffset;
        this.fixedValueOffset = other.fixedValueOffset;
        this.fixedRecordGroupsRetainedSize = other.fixedRecordGroupsRetainedSize;
        this.capacity = other.capacity;
        this.mask = other.mask;
        this.nextGroupId = other.nextGroupId;
        this.maxFill = other.maxFill;
        this.control = other.control == null ? null : Arrays.copyOf(other.control, other.control.length);
        this.groupIdsByHash = other.groupIdsByHash == null ? null : Arrays.copyOf(other.groupIdsByHash, other.groupIdsByHash.length);
        this.fixedSizeRecords = Arrays.stream(other.fixedSizeRecords)
                .map(fixedSizeRecords -> fixedSizeRecords == null ? null : Arrays.copyOf(fixedSizeRecords, fixedSizeRecords.length))
                .toArray(byte[][]::new);
    }

    public long getEstimatedSize()
    {
        return sumExact(
                INSTANCE_SIZE,
                fixedRecordGroupsRetainedSize,
                temporaryRehashRetainedSize,
                sizeOf(control),
                sizeOf(groupIdsByHash),
                sizeOf(fixedSizeRecords),
                variableWidthData == null ? 0 : variableWidthData.getRetainedSizeBytes());
    }

    public int size()
    {
        return nextGroupId;
    }

    public int getCapacity()
    {
        return capacity;
    }

    /**
     * Releases memory associated with the hash table which is no longer necessary to produce output. Subsequent
     * calls to insert new elements are rejected, and calls to {@link FlatHash#appendTo(int, BlockBuilder[])} will
     * incrementally release memory associated with prior groupId values assuming that the caller will only call into
     * the method to produce output in a sequential fashion.
     */
    public void startReleasingOutput()
    {
        checkState(!isReleasingOutput(), "already releasing output");
        control = null;
        groupIdsByHash = null;
    }

    private boolean isReleasingOutput()
    {
        return control == null;
    }

    public long hashPosition(int groupId)
    {
        if (groupId < 0 || groupId >= nextGroupId) {
            throw new IllegalArgumentException("groupId out of range: " + groupId);
        }
        byte[] fixedSizeRecords = getFixedSizeRecords(groupId);
        int fixedRecordOffset = getFixedRecordOffset(groupId);
        checkState(!isReleasingOutput() || fixedSizeRecords != null, "groupId already released");
        if (cacheHashValue) {
            return (long) LONG_HANDLE.get(fixedSizeRecords, fixedRecordOffset);
        }
        byte[] variableWidthChunk = null;
        int variableChunkOffset = 0;
        if (variableWidthData != null) {
            variableWidthChunk = variableWidthData.getChunk(fixedSizeRecords, fixedRecordOffset + variableWidthOffset);
            variableChunkOffset = getChunkOffset(fixedSizeRecords, fixedRecordOffset + variableWidthOffset);
        }
        try {
            return flatHashStrategy.hash(fixedSizeRecords, fixedRecordOffset + fixedValueOffset, variableWidthChunk, variableChunkOffset);
        }
        catch (Throwable throwable) {
            throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    public void appendTo(int groupId, BlockBuilder[] blockBuilders)
    {
        checkArgument(groupId < nextGroupId, "groupId out of range");

        int recordGroupIndex = recordGroupIndexForGroupId(groupId);
        byte[] fixedSizeRecords = this.fixedSizeRecords[recordGroupIndex];
        int recordOffset = getFixedRecordOffset(groupId);

        byte[] variableWidthChunk = null;
        int variableChunkOffset = 0;
        if (variableWidthData != null) {
            variableWidthChunk = variableWidthData.getChunk(fixedSizeRecords, recordOffset + variableWidthOffset);
            variableChunkOffset = getChunkOffset(fixedSizeRecords, recordOffset + variableWidthOffset);
        }

        flatHashStrategy.readFlat(
                fixedSizeRecords,
                recordOffset + fixedValueOffset,
                variableWidthChunk,
                variableChunkOffset,
                blockBuilders);

        // Release memory from the previous fixed size records batch
        if (isReleasingOutput() && recordOffset == 0 && recordGroupIndex > 0) {
            byte[] releasedRecords = this.fixedSizeRecords[recordGroupIndex - 1];
            this.fixedSizeRecords[recordGroupIndex - 1] = null;
            if (releasedRecords == null) {
                throw new IllegalStateException("already released previous record batch");
            }
            fixedRecordGroupsRetainedSize -= sizeOf(releasedRecords);
            if (variableWidthData != null) {
                variableWidthData.freeChunksBefore(fixedSizeRecords, recordOffset + variableWidthOffset);
            }
        }
    }

    public void computeHashes(Block[] blocks, long[] hashes, int offset, int length)
    {
        flatHashStrategy.hashBlocksBatched(blocks, hashes, offset, length);
    }

    public int putIfAbsent(Block[] blocks, int position)
    {
        long hash = flatHashStrategy.hash(blocks, position);
        return putIfAbsent(blocks, position, hash);
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
        checkState(!isReleasingOutput(), "already releasing output");
        byte hashPrefix = (byte) (hash & 0x7F | 0x80);
        int bucket = bucket((int) (hash >> 7));

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
        int recordGroupIndex = recordGroupIndexForGroupId(groupId);
        int fixedRecordOffset = getFixedRecordOffset(groupId);
        byte[] fixedSizeRecords = this.fixedSizeRecords[recordGroupIndex];
        if (fixedRecordOffset == 0) {
            if (fixedSizeRecords != null) {
                throw new IllegalStateException("fixedSizeRecords already exists");
            }
            // new record batch start, populate the record batch fields
            fixedSizeRecords = new byte[multiplyExact(RECORDS_PER_GROUP, fixedRecordSize)];
            this.fixedSizeRecords[recordGroupIndex] = fixedSizeRecords;
            fixedRecordGroupsRetainedSize = addExact(fixedRecordGroupsRetainedSize, sizeOf(fixedSizeRecords));
        }

        if (cacheHashValue) {
            LONG_HANDLE.set(fixedSizeRecords, fixedRecordOffset, hash);
        }

        byte[] variableWidthChunk = null;
        int variableWidthChunkOffset = 0;
        if (variableWidthData != null) {
            int variableWidthSize = flatHashStrategy.getTotalVariableWidth(blocks, position);
            variableWidthChunk = variableWidthData.allocate(fixedSizeRecords, fixedRecordOffset + variableWidthOffset, variableWidthSize);
            variableWidthChunkOffset = getChunkOffset(fixedSizeRecords, fixedRecordOffset + variableWidthOffset);
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
        checkState(!isReleasingOutput(), "already releasing output");
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
        temporaryRehashRetainedSize = multiplyExact((long) newCapacity, Integer.BYTES + Byte.BYTES);
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
            long hash = hashPosition(groupId);

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

        // release temporary memory reservation
        temporaryRehashRetainedSize = 0;
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
        return fixedSizeRecords[recordGroupIndexForGroupId(groupId)];
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

        if (cacheHashValue && rightHash != (long) LONG_HANDLE.get(fixedSizeRecords, fixedRecordsOffset)) {
            return false;
        }

        byte[] variableWidthChunk = null;
        int variableWidthChunkOffset = 0;
        if (variableWidthData != null) {
            variableWidthChunk = variableWidthData.getChunk(fixedSizeRecords, fixedRecordsOffset + variableWidthOffset);
            variableWidthChunkOffset = getChunkOffset(fixedSizeRecords, fixedRecordsOffset + variableWidthOffset);
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
        return max(1, (capacity + 1) >> RECORDS_PER_GROUP_SHIFT);
    }

    public static long sumExact(long... values)
    {
        long result = 0;
        for (long value : values) {
            result = addExact(result, value);
        }
        return result;
    }

    public FlatHash copy()
    {
        return new FlatHash(this);
    }
}
