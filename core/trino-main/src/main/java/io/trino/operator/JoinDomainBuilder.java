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
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeUtils;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.operator.VariableWidthData.EMPTY_CHUNK;
import static io.trino.operator.VariableWidthData.POINTER_SIZE;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FLAT;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FLAT_RETURN;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TypeUtils.isFloatingPointNaN;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static java.lang.Math.multiplyExact;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Objects.requireNonNull;

public class JoinDomainBuilder
{
    private static final int INSTANCE_SIZE = instanceSize(JoinDomainBuilder.class);

    private static final int DEFAULT_DISTINCT_HASH_CAPACITY = 64;

    private static final int VECTOR_LENGTH = Long.BYTES;
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, LITTLE_ENDIAN);

    private final Type type;

    private final int maxDistinctValues;
    private final long maxFilterSizeInBytes;
    private final Runnable notifyStateChange;

    private final MethodHandle readFlat;
    private final MethodHandle writeFlat;

    private final MethodHandle hashFlat;
    private final MethodHandle hashBlock;

    private final MethodHandle distinctFlatFlat;
    private final MethodHandle distinctFlatBlock;

    private final MethodHandle compareFlatFlat;
    private final MethodHandle compareBlockBlock;

    private final int distinctRecordSize;
    private final int distinctRecordValueOffset;

    private int distinctCapacity;
    private int distinctMask;

    private byte[] distinctControl;
    private byte[] distinctRecords;
    private VariableWidthData distinctVariableWidthData;

    private int distinctSize;
    private int distinctMaxFill;

    private Block minValue;
    private Block maxValue;

    private boolean collectDistinctValues = true;
    private boolean collectMinMax;

    private long retainedSizeInBytes = INSTANCE_SIZE;

    public JoinDomainBuilder(
            Type type,
            int maxDistinctValues,
            DataSize maxFilterSize,
            boolean minMaxEnabled,
            Runnable notifyStateChange,
            TypeOperators typeOperators)
    {
        this.type = requireNonNull(type, "type is null");

        this.maxDistinctValues = maxDistinctValues;
        this.maxFilterSizeInBytes = maxFilterSize.toBytes();
        this.notifyStateChange = requireNonNull(notifyStateChange, "notifyStateChange is null");

        // Skipping DOUBLE and REAL in collectMinMaxValues to avoid dealing with NaN values
        this.collectMinMax = minMaxEnabled && type.isOrderable() && type != DOUBLE && type != REAL;

        MethodHandle readOperator = typeOperators.getReadValueOperator(type, simpleConvention(NULLABLE_RETURN, FLAT));
        readOperator = readOperator.asType(readOperator.type().changeReturnType(Object.class));
        this.readFlat = readOperator;
        this.writeFlat = typeOperators.getReadValueOperator(type, simpleConvention(FLAT_RETURN, BLOCK_POSITION_NOT_NULL));

        this.hashFlat = typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, FLAT));
        this.hashBlock = typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL));
        this.distinctFlatFlat = typeOperators.getDistinctFromOperator(type, simpleConvention(FAIL_ON_NULL, FLAT, FLAT));
        this.distinctFlatBlock = typeOperators.getDistinctFromOperator(type, simpleConvention(FAIL_ON_NULL, FLAT, BLOCK_POSITION_NOT_NULL));
        if (collectMinMax) {
            this.compareFlatFlat = typeOperators.getComparisonUnorderedLastOperator(type, simpleConvention(FAIL_ON_NULL, FLAT, FLAT));
            this.compareBlockBlock = typeOperators.getComparisonUnorderedLastOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL, BLOCK_POSITION_NOT_NULL));
        }
        else {
            this.compareFlatFlat = null;
            this.compareBlockBlock = null;
        }

        distinctCapacity = DEFAULT_DISTINCT_HASH_CAPACITY;
        distinctMaxFill = (distinctCapacity / 16) * 15;
        distinctMask = distinctCapacity - 1;
        distinctControl = new byte[distinctCapacity + VECTOR_LENGTH];

        boolean variableWidth = type.isFlatVariableWidth();
        distinctVariableWidthData = variableWidth ? new VariableWidthData() : null;
        distinctRecordValueOffset = (variableWidth ? POINTER_SIZE : 0);
        distinctRecordSize = distinctRecordValueOffset + type.getFlatFixedSize();
        distinctRecords = new byte[multiplyExact(distinctCapacity, distinctRecordSize)];

        retainedSizeInBytes += sizeOf(distinctControl) + sizeOf(distinctRecords);
    }

    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes + (distinctVariableWidthData == null ? 0 : distinctVariableWidthData.getRetainedSizeBytes());
    }

    public boolean isCollecting()
    {
        return collectMinMax || collectDistinctValues;
    }

    public void add(Block block)
    {
        if (collectDistinctValues) {
            for (int position = 0; position < block.getPositionCount(); ++position) {
                add(block, position);
            }

            // if the distinct size is too large, fall back to min max, and drop the distinct values
            if (distinctSize > maxDistinctValues || getRetainedSizeInBytes() > maxFilterSizeInBytes) {
                retainedSizeInBytes = INSTANCE_SIZE;
                if (collectMinMax) {
                    int minIndex = -1;
                    int maxIndex = -1;
                    for (int index = 0; index < distinctCapacity; index++) {
                        if (distinctControl[index] != 0) {
                            if (minIndex == -1) {
                                minIndex = index;
                                maxIndex = index;
                                continue;
                            }

                            if (valueCompare(index, minIndex) < 0) {
                                minIndex = index;
                            }
                            else if (valueCompare(index, maxIndex) > 0) {
                                maxIndex = index;
                            }
                        }
                    }
                    if (minIndex != -1) {
                        minValue = readValueToBlock(minIndex);
                        maxValue = readValueToBlock(maxIndex);
                        retainedSizeInBytes += minValue.getRetainedSizeInBytes() + maxValue.getRetainedSizeInBytes();
                    }
                }
                else {
                    notifyStateChange.run();
                }

                collectDistinctValues = false;
                distinctCapacity = 0;
                distinctControl = null;
                distinctRecords = null;
                distinctVariableWidthData = null;
                distinctSize = 0;
                distinctMaxFill = 0;
            }
        }
        else if (collectMinMax) {
            int minValuePosition = -1;
            int maxValuePosition = -1;

            for (int position = 0; position < block.getPositionCount(); ++position) {
                if (block.isNull(position)) {
                    continue;
                }
                if (minValuePosition == -1) {
                    // First non-null value
                    minValuePosition = position;
                    maxValuePosition = position;
                    continue;
                }
                if (valueCompare(block, position, block, minValuePosition) < 0) {
                    minValuePosition = position;
                }
                else if (valueCompare(block, position, block, maxValuePosition) > 0) {
                    maxValuePosition = position;
                }
            }

            if (minValuePosition == -1) {
                // all block values are nulls
                return;
            }

            if (minValue == null) {
                minValue = block.getSingleValueBlock(minValuePosition);
                maxValue = block.getSingleValueBlock(maxValuePosition);
                return;
            }
            if (valueCompare(block, minValuePosition, minValue, 0) < 0) {
                retainedSizeInBytes -= minValue.getRetainedSizeInBytes();
                minValue = block.getSingleValueBlock(minValuePosition);
                retainedSizeInBytes += minValue.getRetainedSizeInBytes();
            }
            if (valueCompare(block, maxValuePosition, maxValue, 0) > 0) {
                retainedSizeInBytes -= maxValue.getRetainedSizeInBytes();
                maxValue = block.getSingleValueBlock(maxValuePosition);
                retainedSizeInBytes += maxValue.getRetainedSizeInBytes();
            }
        }
    }

    public void disableMinMax()
    {
        collectMinMax = false;
        if (minValue != null) {
            retainedSizeInBytes -= minValue.getRetainedSizeInBytes();
            minValue = null;
        }
        if (maxValue != null) {
            retainedSizeInBytes -= maxValue.getRetainedSizeInBytes();
            maxValue = null;
        }
    }

    public Domain build()
    {
        if (collectDistinctValues) {
            ImmutableList.Builder<Object> values = ImmutableList.builder();
            for (int i = 0; i < distinctCapacity; i++) {
                if (distinctControl[i] != 0) {
                    Object value = readValueToObject(i);
                    // join doesn't match rows with NaN values.
                    if (!isFloatingPointNaN(type, value)) {
                        values.add(value);
                    }
                }
            }
            // Inner and right join doesn't match rows with null key column values.
            return Domain.create(ValueSet.copyOf(type, values.build()), false);
        }
        if (collectMinMax) {
            if (minValue == null) {
                // all values were null
                return Domain.none(type);
            }
            Object min = readNativeValue(type, minValue, 0);
            Object max = readNativeValue(type, maxValue, 0);
            return Domain.create(ValueSet.ofRanges(range(type, min, true, max, true)), false);
        }
        return Domain.all(type);
    }

    private void add(Block block, int position)
    {
        // Inner and right join doesn't match rows with null key column values.
        if (block.isNull(position)) {
            return;
        }

        long hash = valueHashCode(block, position);

        byte hashPrefix = getHashPrefix(hash);
        int hashBucket = getHashBucket(hash);

        int step = 1;
        long repeated = repeat(hashPrefix);

        while (true) {
            final long controlVector = getControlVector(hashBucket);

            int matchBucket = matchInVector(block, position, hashBucket, repeated, controlVector);
            if (matchBucket >= 0) {
                return;
            }

            int emptyIndex = findEmptyInVector(controlVector, hashBucket);
            if (emptyIndex >= 0) {
                insert(emptyIndex, block, position, hashPrefix);
                distinctSize++;

                if (distinctSize >= distinctMaxFill) {
                    rehash();
                }
                return;
            }

            hashBucket = bucket(hashBucket + step);
            step += VECTOR_LENGTH;
        }
    }

    private int matchInVector(byte[] otherValues, VariableWidthData otherVariableWidthData, int position, int vectorStartBucket, long repeated, long controlVector)
    {
        long controlMatches = match(controlVector, repeated);
        while (controlMatches != 0) {
            int slot = Long.numberOfTrailingZeros(controlMatches) >>> 3;
            int bucket = bucket(vectorStartBucket + slot);
            if (valueNotDistinctFrom(bucket, otherValues, otherVariableWidthData, position)) {
                return bucket;
            }

            controlMatches = controlMatches & (controlMatches - 1);
        }
        return -1;
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

    private void insert(int index, Block block, int position, byte hashPrefix)
    {
        setControl(index, hashPrefix);

        int recordOffset = getRecordOffset(index);

        byte[] variableWidthChunk = EMPTY_CHUNK;
        int variableWidthChunkOffset = 0;
        if (distinctVariableWidthData != null) {
            int variableWidthLength = type.getFlatVariableWidthSize(block, position);
            variableWidthChunk = distinctVariableWidthData.allocate(distinctRecords, recordOffset, variableWidthLength);
            variableWidthChunkOffset = VariableWidthData.getChunkOffset(distinctRecords, recordOffset);
        }

        try {
            writeFlat.invokeExact(
                    block,
                    position,
                    distinctRecords,
                    recordOffset + distinctRecordValueOffset,
                    variableWidthChunk,
                    variableWidthChunkOffset);
        }
        catch (Throwable throwable) {
            Throwables.throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    private void setControl(int index, byte hashPrefix)
    {
        distinctControl[index] = hashPrefix;
        if (index < VECTOR_LENGTH) {
            distinctControl[index + distinctCapacity] = hashPrefix;
        }
    }

    private void rehash()
    {
        int oldCapacity = distinctCapacity;
        byte[] oldControl = distinctControl;
        byte[] oldRecords = distinctRecords;

        long newCapacityLong = distinctCapacity * 2L;
        if (newCapacityLong > Integer.MAX_VALUE) {
            throw new TrinoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }

        distinctSize = 0;
        distinctCapacity = (int) newCapacityLong;
        distinctMaxFill = (distinctCapacity / 16) * 15;
        distinctMask = distinctCapacity - 1;

        distinctControl = new byte[distinctCapacity + VECTOR_LENGTH];
        distinctRecords = new byte[multiplyExact(distinctCapacity, distinctRecordSize)];

        retainedSizeInBytes = retainedSizeInBytes - sizeOf(oldControl) - sizeOf(oldRecords) + sizeOf(distinctControl) + sizeOf(distinctRecords);

        for (int oldIndex = 0; oldIndex < oldCapacity; oldIndex++) {
            if (oldControl[oldIndex] != 0) {
                long hash = valueHashCode(oldRecords, oldIndex);

                byte hashPrefix = getHashPrefix(hash);
                int bucket = getHashBucket(hash);

                int step = 1;
                long repeated = repeat(hashPrefix);

                while (true) {
                    long controlVector = getControlVector(bucket);

                    int matchIndex = matchInVector(oldRecords, distinctVariableWidthData, oldIndex, bucket, repeated, controlVector);
                    if (matchIndex >= 0) {
                        break;
                    }

                    int emptyIndex = findEmptyInVector(controlVector, bucket);
                    if (emptyIndex >= 0) {
                        setControl(emptyIndex, hashPrefix);

                        System.arraycopy(
                                oldRecords,
                                getRecordOffset(oldIndex),
                                distinctRecords,
                                getRecordOffset(emptyIndex),
                                distinctRecordSize);
                        // variable width data does not need to be copied, since rehash only moves the fixed records

                        distinctSize++;
                        break;
                    }

                    bucket = bucket(bucket + step);
                    step += VECTOR_LENGTH;
                }
            }
        }
    }

    private long getControlVector(int bucket)
    {
        return (long) LONG_HANDLE.get(distinctControl, bucket);
    }

    private int getHashBucket(long hash)
    {
        return bucket((int) (hash >> 7));
    }

    private static byte getHashPrefix(long hash)
    {
        return (byte) (hash & 0x7F | 0x80);
    }

    private int bucket(int hash)
    {
        return hash & distinctMask;
    }

    private int getRecordOffset(int bucket)
    {
        return bucket * distinctRecordSize;
    }

    private Object readValueToObject(int position)
    {
        int recordOffset = getRecordOffset(position);

        try {
            byte[] variableWidthChunk = EMPTY_CHUNK;
            if (distinctVariableWidthData != null) {
                variableWidthChunk = distinctVariableWidthData.getChunk(distinctRecords, recordOffset);
            }

            return (Object) readFlat.invokeExact(
                    distinctRecords,
                    recordOffset + distinctRecordValueOffset,
                    variableWidthChunk);
        }
        catch (Throwable throwable) {
            Throwables.throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    private Block readValueToBlock(int position)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
        TypeUtils.writeNativeValue(type, blockBuilder, readValueToObject(position));
        return blockBuilder.build();
    }

    private long valueHashCode(byte[] values, int position)
    {
        int recordOffset = getRecordOffset(position);

        try {
            byte[] variableWidthChunk = EMPTY_CHUNK;
            if (distinctVariableWidthData != null) {
                variableWidthChunk = distinctVariableWidthData.getChunk(values, recordOffset);
            }

            return (long) hashFlat.invokeExact(
                    values,
                    recordOffset + distinctRecordValueOffset,
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
        byte[] leftFixedRecordChunk = distinctRecords;
        int leftRecordOffset = getRecordOffset(leftPosition);
        byte[] leftVariableWidthChunk = EMPTY_CHUNK;
        if (distinctVariableWidthData != null) {
            leftVariableWidthChunk = distinctVariableWidthData.getChunk(leftFixedRecordChunk, leftRecordOffset);
        }

        try {
            return !(boolean) distinctFlatBlock.invokeExact(
                    leftFixedRecordChunk,
                    leftRecordOffset + distinctRecordValueOffset,
                    leftVariableWidthChunk,
                    right,
                    rightPosition);
        }
        catch (Throwable throwable) {
            Throwables.throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    private boolean valueNotDistinctFrom(int leftPosition, byte[] rightValues, VariableWidthData rightVariableWidthData, int rightPosition)
    {
        byte[] leftFixedRecordChunk = distinctRecords;
        int leftRecordOffset = getRecordOffset(leftPosition);
        byte[] leftVariableWidthChunk = EMPTY_CHUNK;
        if (distinctVariableWidthData != null) {
            leftVariableWidthChunk = distinctVariableWidthData.getChunk(leftFixedRecordChunk, leftRecordOffset);
        }

        byte[] rightFixedRecordChunk = rightValues;
        int rightRecordOffset = getRecordOffset(rightPosition);
        byte[] rightVariableWidthChunk = EMPTY_CHUNK;
        if (rightVariableWidthData != null) {
            rightVariableWidthChunk = rightVariableWidthData.getChunk(rightFixedRecordChunk, rightRecordOffset);
        }

        try {
            return !(boolean) distinctFlatFlat.invokeExact(
                    leftFixedRecordChunk,
                    leftRecordOffset + distinctRecordValueOffset,
                    leftVariableWidthChunk,
                    rightFixedRecordChunk,
                    rightRecordOffset + distinctRecordValueOffset,
                    rightVariableWidthChunk);
        }
        catch (Throwable throwable) {
            Throwables.throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    private int valueCompare(Block left, int leftPosition, Block right, int rightPosition)
    {
        try {
            return (int) (long) compareBlockBlock.invokeExact(
                    left,
                    leftPosition,
                    right,
                    rightPosition);
        }
        catch (Throwable throwable) {
            Throwables.throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    private int valueCompare(int leftPosition, int rightPosition)
    {
        int leftRecordOffset = getRecordOffset(leftPosition);
        int rightRecordOffset = getRecordOffset(rightPosition);

        byte[] leftVariableWidthChunk = EMPTY_CHUNK;
        byte[] rightVariableWidthChunk = EMPTY_CHUNK;
        if (distinctVariableWidthData != null) {
            leftVariableWidthChunk = distinctVariableWidthData.getChunk(distinctRecords, leftRecordOffset);
            rightVariableWidthChunk = distinctVariableWidthData.getChunk(distinctRecords, rightRecordOffset);
        }

        try {
            return (int) (long) compareFlatFlat.invokeExact(
                    distinctRecords,
                    leftRecordOffset + distinctRecordValueOffset,
                    leftVariableWidthChunk,
                    distinctRecords,
                    rightRecordOffset + distinctRecordValueOffset,
                    rightVariableWidthChunk);
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
