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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.SizeOf.sizeOfByteArray;
import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static io.trino.operator.VariableWidthData.EMPTY_CHUNK;
import static io.trino.operator.VariableWidthData.POINTER_SIZE;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.Math.max;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

public final class FlatHash
{
    private static final int INSTANCE_SIZE = instanceSize(FlatHash.class);

    private static final double DEFAULT_LOAD_FACTOR = 15.0 / 16;

    private static int computeCapacity(int maxSize, double loadFactor)
    {
        int capacity = (int) (maxSize / loadFactor);
        return max(toIntExact(1L << (64 - Long.numberOfLeadingZeros(capacity - 1))), 16);
    }

    private static final int RECORDS_PER_GROUP_SHIFT = 10;
    private static final int RECORDS_PER_GROUP = 1 << RECORDS_PER_GROUP_SHIFT;
    private static final int RECORDS_PER_GROUP_MASK = RECORDS_PER_GROUP - 1;

    private static final int VECTOR_LENGTH = Long.BYTES;
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, LITTLE_ENDIAN);
    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, LITTLE_ENDIAN);

    private final FlatHashStrategy flatHashStrategy;
    private final boolean hasPrecomputedHash;

    private final int recordSize;
    private final int recordGroupIdOffset;
    private final int recordHashOffset;
    private final int recordValueOffset;

    private int capacity;
    private int mask;

    private byte[] control;
    private byte[][] recordGroups;
    private final VariableWidthData variableWidthData;

    // position of each group in the hash table
    private int[] groupRecordIndex;

    // reserve enough memory before rehash
    private final UpdateMemory checkMemoryReservation;
    private long rehashMemoryReservation;

    private int nextGroupId;
    private int maxFill;

    public FlatHash(FlatHashStrategy flatHashStrategy, boolean hasPrecomputedHash, int expectedSize, UpdateMemory checkMemoryReservation)
    {
        this.flatHashStrategy = flatHashStrategy;
        this.hasPrecomputedHash = hasPrecomputedHash;
        this.checkMemoryReservation = checkMemoryReservation;

        capacity = max(VECTOR_LENGTH, computeCapacity(expectedSize, DEFAULT_LOAD_FACTOR));
        maxFill = toIntExact(capacity * 15L / 16);
        mask = capacity - 1;
        control = new byte[capacity + VECTOR_LENGTH];

        groupRecordIndex = new int[maxFill];

        // the record is laid out as follows:
        // 1. optional variable width pointer
        // 2. groupId (int)
        // 3. fixed data for each type
        boolean variableWidth = flatHashStrategy.isAnyVariableWidth();
        variableWidthData = variableWidth ? new VariableWidthData() : null;
        recordGroupIdOffset = (variableWidth ? POINTER_SIZE : 0);
        recordHashOffset = recordGroupIdOffset + Integer.BYTES;
        recordValueOffset = recordHashOffset + (hasPrecomputedHash ? Long.BYTES : 0);
        recordSize = recordValueOffset + flatHashStrategy.getTotalFlatFixedLength();
        recordGroups = createRecordGroups(capacity, recordSize);
    }

    private static byte[][] createRecordGroups(int capacity, int recordSize)
    {
        if (capacity < RECORDS_PER_GROUP) {
            return new byte[][] {new byte[multiplyExact(capacity, recordSize)]};
        }

        byte[][] groups = new byte[(capacity + 1) >> RECORDS_PER_GROUP_SHIFT][];
        for (int i = 0; i < groups.length; i++) {
            groups[i] = new byte[multiplyExact(RECORDS_PER_GROUP, recordSize)];
        }
        return groups;
    }

    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                sizeOf(control) +
                (sizeOf(recordGroups[0]) * recordGroups.length) +
                (variableWidthData == null ? 0 : variableWidthData.getRetainedSizeBytes()) +
                sizeOf(groupRecordIndex) +
                rehashMemoryReservation;
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
        // for spilling
        checkArgument(groupId < nextGroupId, "groupId out of range");

        int index = groupRecordIndex[groupId];
        byte[] records = getRecords(index);
        if (hasPrecomputedHash) {
            return (long) LONG_HANDLE.get(records, getRecordOffset(index) + recordHashOffset);
        }
        else {
            return valueHashCode(records, index);
        }
    }

    public void appendTo(int groupId, BlockBuilder[] blockBuilders)
    {
        checkArgument(groupId < nextGroupId, "groupId out of range");
        int index = groupRecordIndex[groupId];
        byte[] records = getRecords(index);
        int recordOffset = getRecordOffset(index);

        byte[] variableWidthChunk = EMPTY_CHUNK;
        int variableWidthOffset = 0;
        if (variableWidthData != null) {
            variableWidthChunk = variableWidthData.getChunk(records, recordOffset);
            variableWidthOffset = VariableWidthData.getChunkOffset(records, recordOffset);
        }

        flatHashStrategy.readFlat(records, recordOffset + recordValueOffset, variableWidthChunk, variableWidthOffset, blockBuilders);
        if (hasPrecomputedHash) {
            BIGINT.writeLong(blockBuilders[blockBuilders.length - 1], (long) LONG_HANDLE.get(records, recordOffset + recordHashOffset));
        }
    }

    public boolean contains(Block[] blocks, int position)
    {
        return contains(blocks, position, flatHashStrategy.hash(blocks, position));
    }

    public boolean contains(Block[] blocks, int position, long hash)
    {
        return getIndex(blocks, position, hash) >= 0;
    }

    public int putIfAbsent(Block[] blocks, int position)
    {
        long hash;
        if (hasPrecomputedHash) {
            hash = BIGINT.getLong(blocks[blocks.length - 1], position);
        }
        else {
            hash = flatHashStrategy.hash(blocks, position);
        }

        int index = getIndex(blocks, position, hash);
        if (index >= 0) {
            return (int) INT_HANDLE.get(getRecords(index), getRecordOffset(index) + recordGroupIdOffset);
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
            if (valueNotDistinctFrom(index, blocks, position, hash)) {
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

        byte[] records = getRecords(index);
        int recordOffset = getRecordOffset(index);

        int groupId = nextGroupId++;
        INT_HANDLE.set(records, recordOffset + recordGroupIdOffset, groupId);
        groupRecordIndex[groupId] = index;

        if (hasPrecomputedHash) {
            LONG_HANDLE.set(records, recordOffset + recordHashOffset, hash);
        }

        byte[] variableWidthChunk = EMPTY_CHUNK;
        int variableWidthChunkOffset = 0;
        if (variableWidthData != null) {
            int variableWidthSize = flatHashStrategy.getTotalVariableWidth(blocks, position);
            variableWidthChunk = variableWidthData.allocate(records, recordOffset, variableWidthSize);
            variableWidthChunkOffset = VariableWidthData.getChunkOffset(records, recordOffset);
        }
        flatHashStrategy.writeFlat(blocks, position, records, recordOffset + recordValueOffset, variableWidthChunk, variableWidthChunkOffset);
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
        long newCapacityLong = capacity * 2L;
        while (newCapacityLong < minimumRequiredCapacity) {
            newCapacityLong *= 2;
        }
        if (newCapacityLong > Integer.MAX_VALUE) {
            throw new TrinoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }
        int newCapacity = toIntExact(newCapacityLong);

        // the entire newCapacity is reserved since during the rehash both the old and new hash table are in retained
        long tempControlBytes = sizeOfByteArray(newCapacity + VECTOR_LENGTH);
        long tempRecordsBytes = sizeOfByteArray(newCapacity * recordSize);
        long tempGroupRecordIndexBytes = sizeOfIntArray(newCapacity);
        long tempRehashBytes = tempControlBytes + tempRecordsBytes + tempGroupRecordIndexBytes;
        rehashMemoryReservation = tempRehashBytes;
        if (!checkMemoryReservation.update()) {
            return false;
        }

        rehash(minimumRequiredCapacity);
        return true;
    }

    private void rehash(int minimumRequiredCapacity)
    {
        int oldCapacity = capacity;
        byte[] oldControl = control;
        byte[][] oldRecordGroups = recordGroups;

        long newCapacityLong = capacity * 2L;
        while (newCapacityLong < minimumRequiredCapacity) {
            newCapacityLong *= 2;
        }
        if (newCapacityLong > Integer.MAX_VALUE) {
            throw new TrinoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }

        capacity = (int) newCapacityLong;
        maxFill = toIntExact(capacity * 15L / 16);
        mask = capacity - 1;

        control = new byte[capacity + VECTOR_LENGTH];
        recordGroups = createRecordGroups(capacity, recordSize);
        groupRecordIndex = new int[maxFill];

        for (int oldIndex = 0; oldIndex < oldCapacity; oldIndex++) {
            if (oldControl[oldIndex] == 0) {
                continue;
            }

            byte[] oldRecords = oldRecordGroups[oldIndex >> RECORDS_PER_GROUP_SHIFT];
            long hash;
            if (hasPrecomputedHash) {
                hash = (long) LONG_HANDLE.get(oldRecords, getRecordOffset(oldIndex) + recordHashOffset);
            }
            else {
                hash = valueHashCode(oldRecords, oldIndex);
            }

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

                    byte[] records = getRecords(emptyIndex);
                    int recordOffset = getRecordOffset(emptyIndex);
                    int oldRecordOffset = getRecordOffset(oldIndex);
                    System.arraycopy(oldRecords, oldRecordOffset, records, recordOffset, recordSize);

                    int groupId = (int) INT_HANDLE.get(records, recordOffset + recordGroupIdOffset);
                    groupRecordIndex[groupId] = emptyIndex;

                    break;
                }

                bucket = bucket(bucket + step);
                step += VECTOR_LENGTH;
            }
        }

        // release temporary memory reservation
        rehashMemoryReservation = 0;
        checkMemoryReservation.update();
    }

    private int bucket(int hash)
    {
        return hash & mask;
    }

    private byte[] getRecords(int index)
    {
        return recordGroups[index >> RECORDS_PER_GROUP_SHIFT];
    }

    private int getRecordOffset(int index)
    {
        return (index & RECORDS_PER_GROUP_MASK) * recordSize;
    }

    private long valueHashCode(byte[] records, int index)
    {
        int recordOffset = getRecordOffset(index);

        try {
            byte[] variableWidthChunk = EMPTY_CHUNK;
            if (variableWidthData != null) {
                variableWidthChunk = variableWidthData.getChunk(records, recordOffset);
            }

            return flatHashStrategy.hash(records, recordOffset + recordValueOffset, variableWidthChunk);
        }
        catch (Throwable throwable) {
            throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    private boolean valueNotDistinctFrom(int leftIndex, Block[] rightBlocks, int rightPosition, long rightHash)
    {
        byte[] leftRecords = getRecords(leftIndex);
        int leftRecordOffset = getRecordOffset(leftIndex);

        if (hasPrecomputedHash) {
            long leftHash = (long) LONG_HANDLE.get(leftRecords, leftRecordOffset + recordHashOffset);
            if (leftHash != rightHash) {
                return false;
            }
        }

        byte[] leftVariableWidthChunk = EMPTY_CHUNK;
        if (variableWidthData != null) {
            leftVariableWidthChunk = variableWidthData.getChunk(leftRecords, leftRecordOffset);
        }

        return flatHashStrategy.valueNotDistinctFrom(
                leftRecords,
                leftRecordOffset + recordValueOffset,
                leftVariableWidthChunk,
                rightBlocks,
                rightPosition);
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

    public int getPhysicalPosition(int groupId)
    {
        return groupRecordIndex[groupId];
    }
}
