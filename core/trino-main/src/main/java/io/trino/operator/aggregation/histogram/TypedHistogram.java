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
package io.trino.operator.aggregation.histogram;

import com.google.common.base.Throwables;
import com.google.common.primitives.Ints;
import io.trino.operator.VariableWidthData;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.operator.VariableWidthData.EMPTY_CHUNK;
import static io.trino.operator.VariableWidthData.POINTER_SIZE;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.Math.multiplyExact;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Objects.checkIndex;
import static java.util.Objects.requireNonNull;

public final class TypedHistogram
{
    private static final int INSTANCE_SIZE = instanceSize(TypedHistogram.class);

    // See jdk.internal.util.ArraysSupport#SOFT_MAX_ARRAY_LENGTH for an explanation
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    // Hash table capacity must be a power of 2 and at least VECTOR_LENGTH
    private static final int INITIAL_CAPACITY = 16;

    private static int calculateMaxFill(int capacity)
    {
        // The hash table uses a load factory of 15/16
        return (capacity / 16) * 15;
    }

    private static final long HASH_COMBINE_PRIME = 4999L;

    private static final int RECORDS_PER_GROUP_SHIFT = 10;
    private static final int RECORDS_PER_GROUP = 1 << RECORDS_PER_GROUP_SHIFT;
    private static final int RECORDS_PER_GROUP_MASK = RECORDS_PER_GROUP - 1;

    private static final int VECTOR_LENGTH = Long.BYTES;
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, LITTLE_ENDIAN);
    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, LITTLE_ENDIAN);

    private final Type type;
    private final MethodHandle readFlat;
    private final MethodHandle writeFlat;
    private final MethodHandle hashFlat;
    private final MethodHandle distinctFlatBlock;
    private final MethodHandle hashBlock;

    private final int recordSize;
    private final int recordGroupIdOffset;
    private final int recordNextIndexOffset;
    private final int recordCountOffset;
    private final int recordValueOffset;

    private int capacity;
    private int mask;

    private byte[] control;
    private byte[][] recordGroups;
    private final VariableWidthData variableWidthData;

    // head position of each group in the hash table
    @Nullable
    private int[] groupRecordIndex;

    private int size;
    private int maxFill;

    public TypedHistogram(
            Type type,
            MethodHandle readFlat,
            MethodHandle writeFlat,
            MethodHandle hashFlat,
            MethodHandle distinctFlatBlock,
            MethodHandle hashBlock,
            boolean grouped)
    {
        this.type = requireNonNull(type, "type is null");

        this.readFlat = requireNonNull(readFlat, "readFlat is null");
        this.writeFlat = requireNonNull(writeFlat, "writeFlat is null");
        this.hashFlat = requireNonNull(hashFlat, "hashFlat is null");
        this.distinctFlatBlock = requireNonNull(distinctFlatBlock, "distinctFlatBlock is null");
        this.hashBlock = requireNonNull(hashBlock, "hashBlock is null");

        capacity = INITIAL_CAPACITY;
        maxFill = calculateMaxFill(capacity);
        mask = capacity - 1;
        control = new byte[capacity + VECTOR_LENGTH];

        groupRecordIndex = grouped ? new int[0] : null;

        boolean variableWidth = type.isFlatVariableWidth();
        variableWidthData = variableWidth ? new VariableWidthData() : null;
        if (grouped) {
            recordGroupIdOffset = (variableWidth ? POINTER_SIZE : 0);
            recordNextIndexOffset = recordGroupIdOffset + Integer.BYTES;
            recordCountOffset = recordNextIndexOffset + Integer.BYTES;
        }
        else {
            // use MIN_VALUE so that when it is added to the record offset we get a negative value, and thus an ArrayIndexOutOfBoundsException
            recordGroupIdOffset = Integer.MIN_VALUE;
            recordNextIndexOffset = Integer.MIN_VALUE;
            recordCountOffset = (variableWidth ? POINTER_SIZE : 0);
        }
        recordValueOffset = recordCountOffset + Long.BYTES;
        recordSize = recordValueOffset + type.getFlatFixedSize();
        recordGroups = createRecordGroups(capacity, recordSize);
    }

    private static byte[][] createRecordGroups(int capacity, int recordSize)
    {
        if (capacity < RECORDS_PER_GROUP) {
            return new byte[][]{new byte[multiplyExact(capacity, recordSize)]};
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
                (groupRecordIndex == null ? 0 : sizeOf(groupRecordIndex));
    }

    public void setMaxGroupId(int maxGroupId)
    {
        checkState(groupRecordIndex != null, "grouping is not enabled");

        int requiredSize = maxGroupId + 1;
        checkIndex(requiredSize, MAX_ARRAY_SIZE);

        int currentSize = groupRecordIndex.length;
        if (requiredSize > currentSize) {
            groupRecordIndex = Arrays.copyOf(groupRecordIndex, Ints.constrainToRange(requiredSize * 2, 1024, MAX_ARRAY_SIZE));
            Arrays.fill(groupRecordIndex, currentSize, groupRecordIndex.length, -1);
        }
    }

    public int size()
    {
        return size;
    }

    public void serialize(int groupId, MapBlockBuilder out)
    {
        if (size == 0) {
            out.appendNull();
            return;
        }

        if (groupRecordIndex == null) {
            checkArgument(groupId == 0, "groupId must be zero when grouping is not enabled");

            // if there is only one group, serialize the entire histogram
            out.buildEntry((keyBuilder, valueBuilder) -> {
                for (int i = 0; i < capacity; i++) {
                    if (control[i] != 0) {
                        byte[] records = getRecords(i);
                        int recordOffset = getRecordOffset(i);
                        serializeEntry(keyBuilder, valueBuilder, records, recordOffset);
                    }
                }
            });
            return;
        }

        int index = groupRecordIndex[groupId];
        if (index == -1) {
            out.appendNull();
            return;
        }

        // follow the linked list of records for this group
        out.buildEntry((keyBuilder, valueBuilder) -> {
            int nextIndex = index;
            while (nextIndex >= 0) {
                byte[] records = getRecords(nextIndex);
                int recordOffset = getRecordOffset(nextIndex);

                serializeEntry(keyBuilder, valueBuilder, records, recordOffset);

                nextIndex = (int) INT_HANDLE.get(records, recordOffset + recordNextIndexOffset);
            }
        });
    }

    private void serializeEntry(BlockBuilder keyBuilder, BlockBuilder valueBuilder, byte[] records, int recordOffset)
    {
        byte[] variableWidthChunk = EMPTY_CHUNK;
        if (variableWidthData != null) {
            variableWidthChunk = variableWidthData.getChunk(records, recordOffset);
        }

        try {
            readFlat.invokeExact(records, recordOffset + recordValueOffset, variableWidthChunk, keyBuilder);
        }
        catch (Throwable throwable) {
            Throwables.throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
        BIGINT.writeLong(valueBuilder, (long) LONG_HANDLE.get(records, recordOffset + recordCountOffset));
    }

    public void add(int groupId, Block block, int position, long count)
    {
        checkArgument(!block.isNull(position), "value must not be null");
        checkArgument(groupId == 0 || groupRecordIndex != null, "groupId must be zero when grouping is not enabled");

        long hash = valueHashCode(groupId, block, position);

        byte hashPrefix = (byte) (hash & 0x7F | 0x80);
        int bucket = bucket((int) (hash >> 7));

        int step = 1;
        long repeated = repeat(hashPrefix);

        while (true) {
            final long controlVector = (long) LONG_HANDLE.get(control, bucket);

            int matchBucket = matchInVector(groupId, block, position, bucket, repeated, controlVector);
            if (matchBucket >= 0) {
                addCount(matchBucket, count);
                return;
            }

            int emptyIndex = findEmptyInVector(controlVector, bucket);
            if (emptyIndex >= 0) {
                insert(emptyIndex, groupId, block, position, count, hashPrefix);
                size++;

                if (size >= maxFill) {
                    rehash();
                }
                return;
            }

            bucket = bucket(bucket + step);
            step += VECTOR_LENGTH;
        }
    }

    private int matchInVector(int groupId, Block block, int position, int vectorStartBucket, long repeated, long controlVector)
    {
        long controlMatches = match(controlVector, repeated);
        while (controlMatches != 0) {
            int bucket = bucket(vectorStartBucket + (Long.numberOfTrailingZeros(controlMatches) >>> 3));
            if (valueNotDistinctFrom(bucket, block, position, groupId)) {
                return bucket;
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

    private void addCount(int index, long increment)
    {
        byte[] records = getRecords(index);
        int countOffset = getRecordOffset(index) + recordCountOffset;
        LONG_HANDLE.set(records, countOffset, (long) LONG_HANDLE.get(records, countOffset) + increment);
    }

    private void insert(int index, int groupId, Block block, int position, long count, byte hashPrefix)
    {
        setControl(index, hashPrefix);

        byte[] records = getRecords(index);
        int recordOffset = getRecordOffset(index);

        if (groupRecordIndex != null) {
            // write groupId
            INT_HANDLE.set(records, recordOffset + recordGroupIdOffset, groupId);

            // update linked list pointers
            int nextRecordIndex = groupRecordIndex[groupId];
            groupRecordIndex[groupId] = index;
            INT_HANDLE.set(records, recordOffset + recordNextIndexOffset, nextRecordIndex);
        }

        // write count
        LONG_HANDLE.set(records, recordOffset + recordCountOffset, count);

        // write value
        byte[] variableWidthChunk = EMPTY_CHUNK;
        int variableWidthChunkOffset = 0;
        if (variableWidthData != null) {
            int variableWidthLength = type.getFlatVariableWidthSize(block, position);
            variableWidthChunk = variableWidthData.allocate(records, recordOffset, variableWidthLength);
            variableWidthChunkOffset = VariableWidthData.getChunkOffset(records, recordOffset);
        }

        try {
            writeFlat.invokeExact(block, position, records, recordOffset + recordValueOffset, variableWidthChunk, variableWidthChunkOffset);
        }
        catch (Throwable throwable) {
            Throwables.throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    private void setControl(int index, byte hashPrefix)
    {
        control[index] = hashPrefix;
        if (index < VECTOR_LENGTH) {
            control[index + capacity] = hashPrefix;
        }
    }

    private void rehash()
    {
        int oldCapacity = capacity;
        byte[] oldControl = control;
        byte[][] oldRecordGroups = recordGroups;

        long newCapacityLong = capacity * 2L;
        if (newCapacityLong > MAX_ARRAY_SIZE) {
            throw new TrinoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }

        capacity = (int) newCapacityLong;
        maxFill = calculateMaxFill(capacity);
        mask = capacity - 1;

        control = new byte[capacity + VECTOR_LENGTH];
        recordGroups = createRecordGroups(capacity, recordSize);

        if (groupRecordIndex != null) {
            // reset the groupRecordIndex as it will be rebuilt during the rehash
            Arrays.fill(groupRecordIndex, -1);
        }

        for (int oldIndex = 0; oldIndex < oldCapacity; oldIndex++) {
            if (oldControl[oldIndex] != 0) {
                byte[] oldRecords = oldRecordGroups[oldIndex >> RECORDS_PER_GROUP_SHIFT];
                int oldRecordOffset = getRecordOffset(oldIndex);

                int groupId = 0;
                if (groupRecordIndex != null) {
                    groupId = (int) INT_HANDLE.get(oldRecords, oldRecordOffset + recordGroupIdOffset);
                }

                long hash = valueHashCode(groupId, oldRecords, oldIndex);
                byte hashPrefix = (byte) (hash & 0x7F | 0x80);
                int bucket = bucket((int) (hash >> 7));

                int step = 1;
                while (true) {
                    final long controlVector = (long) LONG_HANDLE.get(control, bucket);
                    // values are already distinct, so just find the first empty slot
                    int emptyIndex = findEmptyInVector(controlVector, bucket);
                    if (emptyIndex >= 0) {
                        setControl(emptyIndex, hashPrefix);

                        // copy full record including groupId and count
                        byte[] records = getRecords(emptyIndex);
                        int recordOffset = getRecordOffset(emptyIndex);
                        System.arraycopy(oldRecords, oldRecordOffset, records, recordOffset, recordSize);

                        if (groupRecordIndex != null) {
                            // update linked list pointer to reflect the positions in the new hash
                            INT_HANDLE.set(records, recordOffset + recordNextIndexOffset, groupRecordIndex[groupId]);
                            groupRecordIndex[groupId] = emptyIndex;
                        }

                        break;
                    }

                    bucket = bucket(bucket + step);
                    step += VECTOR_LENGTH;
                }
            }
        }
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

    private long valueHashCode(int groupId, byte[] records, int index)
    {
        int recordOffset = getRecordOffset(index);

        try {
            byte[] variableWidthChunk = EMPTY_CHUNK;
            if (variableWidthData != null) {
                variableWidthChunk = variableWidthData.getChunk(records, recordOffset);
            }

            long valueHash = (long) hashFlat.invokeExact(
                    records,
                    recordOffset + recordValueOffset,
                    variableWidthChunk);
            return groupId * HASH_COMBINE_PRIME + valueHash;
        }
        catch (Throwable throwable) {
            Throwables.throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    private long valueHashCode(int groupId, Block right, int rightPosition)
    {
        try {
            long valueHash = (long) hashBlock.invokeExact(right, rightPosition);
            return groupId * HASH_COMBINE_PRIME + valueHash;
        }
        catch (Throwable throwable) {
            Throwables.throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    private boolean valueNotDistinctFrom(int leftPosition, Block right, int rightPosition, int rightGroupId)
    {
        byte[] leftRecords = getRecords(leftPosition);
        int leftRecordOffset = getRecordOffset(leftPosition);

        if (groupRecordIndex != null) {
            long leftGroupId = (int) INT_HANDLE.get(leftRecords, leftRecordOffset + recordGroupIdOffset);
            if (leftGroupId != rightGroupId) {
                return false;
            }
        }

        byte[] leftVariableWidthChunk = EMPTY_CHUNK;
        if (variableWidthData != null) {
            leftVariableWidthChunk = variableWidthData.getChunk(leftRecords, leftRecordOffset);
        }

        try {
            return !(boolean) distinctFlatBlock.invokeExact(
                    leftRecords,
                    leftRecordOffset + recordValueOffset,
                    leftVariableWidthChunk,
                    right,
                    rightPosition);
        }
        catch (Throwable throwable) {
            Throwables.throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
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
}
