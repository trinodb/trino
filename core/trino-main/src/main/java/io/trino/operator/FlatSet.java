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

import com.google.common.base.Throwables;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.operator.VariableWidthData.EMPTY_CHUNK;
import static io.trino.operator.VariableWidthData.POINTER_SIZE;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static java.lang.Math.multiplyExact;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Objects.requireNonNull;

final class FlatSet
{
    private static final int INSTANCE_SIZE = instanceSize(FlatSet.class);

    // See jdk.internal.util.ArraysSupport#SOFT_MAX_ARRAY_LENGTH for an explanation
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    // Hash table capacity must be a power of two and at least VECTOR_LENGTH
    private static final int INITIAL_CAPACITY = 16;

    private static final int RECORDS_PER_GROUP_SHIFT = 10;
    private static final int RECORDS_PER_GROUP = 1 << RECORDS_PER_GROUP_SHIFT;
    private static final int RECORDS_PER_GROUP_MASK = RECORDS_PER_GROUP - 1;

    private static final int VECTOR_LENGTH = Long.BYTES;
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, LITTLE_ENDIAN);

    private final Type type;
    private final MethodHandle writeFlat;
    private final MethodHandle hashFlat;
    private final MethodHandle distinctFlatBlock;
    private final MethodHandle hashBlock;

    private final int recordSize;
    private final int recordValueOffset;

    private boolean hasNull;

    private int capacity;
    private int mask;

    private byte[] control;
    private byte[][] recordGroups;
    private final VariableWidthData variableWidthData;

    private int size;
    private int maxFill;

    public FlatSet(
            Type type,
            MethodHandle writeFlat,
            MethodHandle hashFlat,
            MethodHandle distinctFlatBlock,
            MethodHandle hashBlock)
    {
        this.type = requireNonNull(type, "type is null");

        this.writeFlat = requireNonNull(writeFlat, "writeFlat is null");
        this.hashFlat = requireNonNull(hashFlat, "hashFlat is null");
        this.distinctFlatBlock = requireNonNull(distinctFlatBlock, "distinctFlatBlock is null");
        this.hashBlock = requireNonNull(hashBlock, "hashBlock is null");

        capacity = INITIAL_CAPACITY;
        maxFill = calculateMaxFill(capacity);
        mask = capacity - 1;
        control = new byte[capacity + VECTOR_LENGTH];

        boolean variableWidth = type.isFlatVariableWidth();
        variableWidthData = variableWidth ? new VariableWidthData() : null;

        recordValueOffset = (variableWidth ? POINTER_SIZE : 0);
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
                (variableWidthData == null ? 0 : variableWidthData.getRetainedSizeBytes());
    }

    public int size()
    {
        return size + (hasNull ? 1 : 0);
    }

    public boolean containsNull()
    {
        return hasNull;
    }

    public boolean contains(Block block, int position)
    {
        if (block.isNull(position)) {
            return hasNull;
        }
        return getIndex(block, position, valueHashCode(block, position)) >= 0;
    }

    public boolean contains(Block block, int position, long hash)
    {
        if (block.isNull(position)) {
            return hasNull;
        }
        return getIndex(block, position, hash) >= 0;
    }

    public void add(Block block, int position)
    {
        if (block.isNull(position)) {
            hasNull = true;
            return;
        }
        addNonNull(block, position, valueHashCode(block, position));
    }

    public void add(Block block, int position, long hash)
    {
        if (block.isNull(position)) {
            hasNull = true;
            return;
        }
        addNonNull(block, position, hash);
    }

    private void addNonNull(Block block, int position, long hash)
    {
        int index = getIndex(block, position, hash);
        if (index >= 0) {
            return;
        }

        index = -index - 1;
        insert(index, block, position, hash);
        size++;
        if (size >= maxFill) {
            rehash();
        }
    }

    private int getIndex(Block block, int position, long hash)
    {
        byte hashPrefix = (byte) (hash & 0x7F | 0x80);
        int bucket = bucket((int) (hash >> 7));

        int step = 1;
        long repeated = repeat(hashPrefix);

        while (true) {
            final long controlVector = (long) LONG_HANDLE.get(control, bucket);

            int matchIndex = matchInVector(block, position, bucket, repeated, controlVector);
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

    private int matchInVector(Block block, int position, int vectorStartBucket, long repeated, long controlVector)
    {
        long controlMatches = match(controlVector, repeated);
        while (controlMatches != 0) {
            int bucket = bucket(vectorStartBucket + (Long.numberOfTrailingZeros(controlMatches) >>> 3));
            if (valueNotDistinctFrom(bucket, block, position)) {
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

    private void insert(int index, Block block, int position, long hash)
    {
        setControl(index, (byte) (hash & 0x7F | 0x80));

        byte[] records = getRecords(index);
        int recordOffset = getRecordOffset(index);

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

        for (int oldIndex = 0; oldIndex < oldCapacity; oldIndex++) {
            if (oldControl[oldIndex] != 0) {
                byte[] oldRecords = oldRecordGroups[oldIndex >> RECORDS_PER_GROUP_SHIFT];
                int oldRecordOffset = getRecordOffset(oldIndex);

                long hash = valueHashCode(oldRecords, oldIndex);
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

    private long valueHashCode(byte[] records, int index)
    {
        int recordOffset = getRecordOffset(index);

        try {
            byte[] variableWidthChunk = EMPTY_CHUNK;
            if (variableWidthData != null) {
                variableWidthChunk = variableWidthData.getChunk(records, recordOffset);
            }

            return (long) hashFlat.invokeExact(
                    records,
                    recordOffset + recordValueOffset,
                    variableWidthChunk);
        }
        catch (Throwable throwable) {
            Throwables.throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    private long valueHashCode(Block right, int rightPosition)
    {
        try {
            return (long) hashBlock.invokeExact(right, rightPosition);
        }
        catch (Throwable throwable) {
            Throwables.throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    private boolean valueNotDistinctFrom(int leftPosition, Block right, int rightPosition)
    {
        byte[] leftRecords = getRecords(leftPosition);
        int leftRecordOffset = getRecordOffset(leftPosition);

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

    private static int calculateMaxFill(int capacity)
    {
        // The hash table uses a load factory of 15/16
        return (capacity / 16) * 15;
    }
}
