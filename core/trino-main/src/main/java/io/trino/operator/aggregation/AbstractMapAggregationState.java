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
package io.trino.operator.aggregation;

import com.google.common.base.Throwables;
import io.trino.operator.VariableWidthData;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.ValueBlock;
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
import static java.lang.Math.clamp;
import static java.lang.Math.multiplyExact;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Objects.checkIndex;
import static java.util.Objects.requireNonNull;

public abstract class AbstractMapAggregationState
        implements MapAggregationState
{
    private static final int INSTANCE_SIZE = instanceSize(AbstractMapAggregationState.class);

    // See java.util.ArrayList for an explanation
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

    private final Type keyType;
    private final MethodHandle keyReadFlat;
    private final MethodHandle keyWriteFlat;
    private final MethodHandle keyHashFlat;
    private final MethodHandle keyDistinctFlatBlock;
    private final MethodHandle keyHashBlock;

    private final Type valueType;
    private final MethodHandle valueReadFlat;
    private final MethodHandle valueWriteFlat;

    private final int recordSize;
    private final int recordGroupIdOffset;
    private final int recordNextIndexOffset;
    private final int recordKeyOffset;
    private final int recordValueNullOffset;
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

    public AbstractMapAggregationState(
            Type keyType,
            MethodHandle keyReadFlat,
            MethodHandle keyWriteFlat,
            MethodHandle hashFlat,
            MethodHandle distinctFlatBlock,
            MethodHandle keyHashBlock,
            Type valueType,
            MethodHandle valueReadFlat,
            MethodHandle valueWriteFlat,
            boolean grouped)
    {
        this.keyType = requireNonNull(keyType, "keyType is null");

        this.keyReadFlat = requireNonNull(keyReadFlat, "keyReadFlat is null");
        this.keyWriteFlat = requireNonNull(keyWriteFlat, "keyWriteFlat is null");
        this.keyHashFlat = requireNonNull(hashFlat, "hashFlat is null");
        this.keyDistinctFlatBlock = requireNonNull(distinctFlatBlock, "distinctFlatBlock is null");
        this.keyHashBlock = requireNonNull(keyHashBlock, "keyHashBlock is null");

        this.valueType = requireNonNull(valueType, "valueType is null");
        this.valueReadFlat = requireNonNull(valueReadFlat, "valueReadFlat is null");
        this.valueWriteFlat = requireNonNull(valueWriteFlat, "valueWriteFlat is null");

        capacity = INITIAL_CAPACITY;
        maxFill = calculateMaxFill(capacity);
        mask = capacity - 1;
        control = new byte[capacity + VECTOR_LENGTH];

        groupRecordIndex = grouped ? new int[0] : null;

        boolean variableWidth = keyType.isFlatVariableWidth() || valueType.isFlatVariableWidth();
        variableWidthData = variableWidth ? new VariableWidthData() : null;
        if (grouped) {
            recordGroupIdOffset = variableWidth ? POINTER_SIZE : 0;
            recordNextIndexOffset = recordGroupIdOffset + Integer.BYTES;
            recordKeyOffset = recordNextIndexOffset + Integer.BYTES;
        }
        else {
            // use MIN_VALUE so that when it is added to the record offset we get a negative value, and thus an ArrayIndexOutOfBoundsException
            recordGroupIdOffset = Integer.MIN_VALUE;
            recordNextIndexOffset = Integer.MIN_VALUE;
            recordKeyOffset = variableWidth ? POINTER_SIZE : 0;
        }
        recordValueNullOffset = recordKeyOffset + keyType.getFlatFixedSize();
        recordValueOffset = recordValueNullOffset + 1;
        recordSize = recordValueOffset + valueType.getFlatFixedSize();
        recordGroups = createRecordGroups(capacity, recordSize);
    }

    public AbstractMapAggregationState(AbstractMapAggregationState state)
    {
        this.keyType = state.keyType;
        this.keyReadFlat = state.keyReadFlat;
        this.keyWriteFlat = state.keyWriteFlat;
        this.keyHashFlat = state.keyHashFlat;
        this.keyDistinctFlatBlock = state.keyDistinctFlatBlock;
        this.keyHashBlock = state.keyHashBlock;

        this.valueType = state.valueType;
        this.valueReadFlat = state.valueReadFlat;
        this.valueWriteFlat = state.valueWriteFlat;

        this.recordSize = state.recordSize;
        this.recordGroupIdOffset = state.recordGroupIdOffset;
        this.recordNextIndexOffset = state.recordNextIndexOffset;
        this.recordKeyOffset = state.recordKeyOffset;
        this.recordValueNullOffset = state.recordValueNullOffset;
        this.recordValueOffset = state.recordValueOffset;

        this.capacity = state.capacity;
        this.mask = state.mask;
        this.control = Arrays.copyOf(state.control, state.control.length);

        this.recordGroups = Arrays.stream(state.recordGroups)
                .map(records -> Arrays.copyOf(records, records.length))
                .toArray(byte[][]::new);
        this.variableWidthData = state.variableWidthData == null ? null : new VariableWidthData(state.variableWidthData);
        this.groupRecordIndex = state.groupRecordIndex == null ? null : Arrays.copyOf(state.groupRecordIndex, state.groupRecordIndex.length);

        this.size = state.size;
        this.maxFill = state.maxFill;
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

    @Override
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
            groupRecordIndex = Arrays.copyOf(groupRecordIndex, clamp(requiredSize * 2L, 1024, MAX_ARRAY_SIZE));
            Arrays.fill(groupRecordIndex, currentSize, groupRecordIndex.length, -1);
        }
    }

    protected void serialize(int groupId, MapBlockBuilder out)
    {
        if (size == 0) {
            out.appendNull();
            return;
        }

        if (groupRecordIndex == null) {
            checkArgument(groupId == 0, "groupId must be zero when grouping is not enabled");

            // if not grouped, serialize the entire histogram
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
            keyReadFlat.invokeExact(records, recordOffset + recordKeyOffset, variableWidthChunk, keyBuilder);
            if (records[recordOffset + recordValueNullOffset] != 0) {
                valueBuilder.appendNull();
            }
            else {
                valueReadFlat.invokeExact(records, recordOffset + recordValueOffset, variableWidthChunk, valueBuilder);
            }
        }
        catch (Throwable throwable) {
            Throwables.throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    protected void add(int groupId, ValueBlock keyBlock, int keyPosition, ValueBlock valueBlock, int valuePosition)
    {
        checkArgument(!keyBlock.isNull(keyPosition), "key must not be null");
        checkArgument(groupId == 0 || groupRecordIndex != null, "groupId must be zero when grouping is not enabled");

        long hash = keyHashCode(groupId, keyBlock, keyPosition);

        byte hashPrefix = (byte) (hash & 0x7F | 0x80);
        int bucket = bucket((int) (hash >> 7));

        int step = 1;
        long repeated = repeat(hashPrefix);

        while (true) {
            final long controlVector = (long) LONG_HANDLE.get(control, bucket);

            int matchBucket = matchInVector(groupId, keyBlock, keyPosition, bucket, repeated, controlVector);
            if (matchBucket >= 0) {
                return;
            }

            int emptyIndex = findEmptyInVector(controlVector, bucket);
            if (emptyIndex >= 0) {
                insert(emptyIndex, groupId, keyBlock, keyPosition, valueBlock, valuePosition, hashPrefix);
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

    private int matchInVector(int groupId, ValueBlock block, int position, int vectorStartBucket, long repeated, long controlVector)
    {
        long controlMatches = match(controlVector, repeated);
        while (controlMatches != 0) {
            int bucket = bucket(vectorStartBucket + (Long.numberOfTrailingZeros(controlMatches) >>> 3));
            if (keyNotDistinctFrom(bucket, block, position, groupId)) {
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

    private void insert(int index, int groupId, ValueBlock keyBlock, int keyPosition, ValueBlock valueBlock, int valuePosition, byte hashPrefix)
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

        int keyVariableWidthSize = 0;
        byte[] variableWidthChunk = EMPTY_CHUNK;
        int variableWidthChunkOffset = 0;
        if (variableWidthData != null) {
            keyVariableWidthSize = keyType.getFlatVariableWidthSize(keyBlock, keyPosition);
            int valueVariableWidthSize = valueBlock.isNull(valuePosition) ? 0 : valueType.getFlatVariableWidthSize(valueBlock, valuePosition);
            variableWidthChunk = variableWidthData.allocate(records, recordOffset, keyVariableWidthSize + valueVariableWidthSize);
            variableWidthChunkOffset = VariableWidthData.getChunkOffset(records, recordOffset);
        }

        try {
            keyWriteFlat.invokeExact(keyBlock, keyPosition, records, recordOffset + recordKeyOffset, variableWidthChunk, variableWidthChunkOffset);
            if (valueBlock.isNull(valuePosition)) {
                records[recordOffset + recordValueNullOffset] = 1;
            }
            else {
                valueWriteFlat.invokeExact(valueBlock, valuePosition, records, recordOffset + recordValueOffset, variableWidthChunk, variableWidthChunkOffset + keyVariableWidthSize);
            }
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

                long hash = keyHashCode(groupId, oldRecords, oldIndex);
                byte hashPrefix = (byte) (hash & 0x7F | 0x80);
                int bucket = bucket((int) (hash >> 7));

                int step = 1;
                while (true) {
                    final long controlVector = (long) LONG_HANDLE.get(control, bucket);
                    // values are already distinct, so just find the first empty slot
                    int emptyIndex = findEmptyInVector(controlVector, bucket);
                    if (emptyIndex >= 0) {
                        setControl(emptyIndex, hashPrefix);

                        // copy full record including groupId
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

    private long keyHashCode(int groupId, byte[] records, int index)
    {
        int recordOffset = getRecordOffset(index);

        try {
            byte[] variableWidthChunk = EMPTY_CHUNK;
            if (variableWidthData != null) {
                variableWidthChunk = variableWidthData.getChunk(records, recordOffset);
            }

            long valueHash = (long) keyHashFlat.invokeExact(
                    records,
                    recordOffset + recordKeyOffset,
                    variableWidthChunk);
            return groupId * HASH_COMBINE_PRIME + valueHash;
        }
        catch (Throwable throwable) {
            Throwables.throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    private long keyHashCode(int groupId, ValueBlock right, int rightPosition)
    {
        try {
            long valueHash = (long) keyHashBlock.invokeExact(right, rightPosition);
            return groupId * HASH_COMBINE_PRIME + valueHash;
        }
        catch (Throwable throwable) {
            Throwables.throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    private boolean keyNotDistinctFrom(int leftPosition, ValueBlock right, int rightPosition, int rightGroupId)
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
            return !(boolean) keyDistinctFlatBlock.invokeExact(
                    leftRecords,
                    leftRecordOffset + recordKeyOffset,
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
        return (value & 0xFF) * 0x01_01_01_01_01_01_01_01L;
    }

    private static long match(long vector, long repeatedValue)
    {
        // HD 6-1
        long comparison = vector ^ repeatedValue;
        return (comparison - 0x01_01_01_01_01_01_01_01L) & ~comparison & 0x80_80_80_80_80_80_80_80L;
    }
}
