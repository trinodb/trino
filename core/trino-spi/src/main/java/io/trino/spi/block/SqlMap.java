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

package io.trino.spi.block;

import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.trino.spi.TrinoException;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.ObjLongConsumer;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.block.BlockUtil.checkReadablePosition;
import static io.trino.spi.block.MapHashTables.HASH_MULTIPLIER;
import static io.trino.spi.block.MapHashTables.computePosition;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SqlMap
        implements Block
{
    private static final int INSTANCE_SIZE = instanceSize(SqlMap.class);

    private final MapType mapType;
    private final Block rawKeyBlock;
    private final Block rawValueBlock;
    private final HashTableSupplier hashTablesSupplier;
    private final int offset;
    private final int positionCount;    // The number of keys in this single map * 2

    public SqlMap(MapType mapType, Block rawKeyBlock, Block rawValueBlock, HashTableSupplier hashTablesSupplier, int offset, int positionCount)
    {
        this.mapType = mapType;
        this.rawKeyBlock = rawKeyBlock;
        this.rawValueBlock = rawValueBlock;
        this.hashTablesSupplier = hashTablesSupplier;
        this.offset = offset;
        this.positionCount = positionCount;
    }

    @Override
    public final List<Block> getChildren()
    {
        return List.of(rawKeyBlock, rawValueBlock);
    }

    public Type getMapType()
    {
        return mapType;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public OptionalInt fixedSizeInBytesPerPosition()
    {
        return OptionalInt.empty();
    }

    @Override
    public long getSizeInBytes()
    {
        return rawKeyBlock.getRegionSizeInBytes(offset / 2, positionCount / 2) +
                rawValueBlock.getRegionSizeInBytes(offset / 2, positionCount / 2) +
                sizeOfIntArray(positionCount / 2 * HASH_MULTIPLIER);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + rawKeyBlock.getRetainedSizeInBytes() +
                rawValueBlock.getRetainedSizeInBytes() +
                hashTablesSupplier.tryGetHashTable().map(SizeOf::sizeOf).orElse(0L);
    }

    @Override
    public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
    {
        consumer.accept(rawKeyBlock, rawKeyBlock.getRetainedSizeInBytes());
        consumer.accept(rawValueBlock, rawValueBlock.getRetainedSizeInBytes());
        consumer.accept(this, INSTANCE_SIZE);
    }

    @Override
    public String getEncodingName()
    {
        return SqlMapBlockEncoding.NAME;
    }

    int getOffset()
    {
        return offset;
    }

    Block getRawKeyBlock()
    {
        return rawKeyBlock;
    }

    Block getRawValueBlock()
    {
        return rawValueBlock;
    }

    @Override
    public Block copyWithAppendedNull()
    {
        throw new UnsupportedOperationException("SqlMap does not support newBlockWithAppendedNull()");
    }

    @Override
    public String toString()
    {
        return format("SqlMap{positionCount=%d}", getPositionCount());
    }

    @Override
    public boolean isLoaded()
    {
        return rawKeyBlock.isLoaded() && rawValueBlock.isLoaded();
    }

    @Override
    public Block getLoadedBlock()
    {
        if (rawKeyBlock != rawKeyBlock.getLoadedBlock()) {
            // keyBlock has to be loaded since MapBlock constructs hash table eagerly.
            throw new IllegalStateException();
        }

        Block loadedValueBlock = rawValueBlock.getLoadedBlock();
        if (loadedValueBlock == rawValueBlock) {
            return this;
        }
        return new SqlMap(mapType, rawKeyBlock, loadedValueBlock, hashTablesSupplier, offset, positionCount);
    }

    private int getAbsolutePosition(int position)
    {
        checkReadablePosition(this, position);
        return position + offset;
    }

    @Override
    public boolean isNull(int position)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            if (rawKeyBlock.isNull(position / 2)) {
                throw new IllegalStateException("Map key is null");
            }
            return false;
        }
        return rawValueBlock.isNull(position / 2);
    }

    @Override
    public byte getByte(int position, int offset)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return rawKeyBlock.getByte(position / 2, offset);
        }
        return rawValueBlock.getByte(position / 2, offset);
    }

    @Override
    public short getShort(int position, int offset)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return rawKeyBlock.getShort(position / 2, offset);
        }
        return rawValueBlock.getShort(position / 2, offset);
    }

    @Override
    public int getInt(int position, int offset)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return rawKeyBlock.getInt(position / 2, offset);
        }
        return rawValueBlock.getInt(position / 2, offset);
    }

    @Override
    public long getLong(int position, int offset)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return rawKeyBlock.getLong(position / 2, offset);
        }
        return rawValueBlock.getLong(position / 2, offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return rawKeyBlock.getSlice(position / 2, offset, length);
        }
        return rawValueBlock.getSlice(position / 2, offset, length);
    }

    @Override
    public void writeSliceTo(int position, int offset, int length, SliceOutput output)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            rawKeyBlock.writeSliceTo(position / 2, offset, length, output);
        }
        else {
            rawValueBlock.writeSliceTo(position / 2, offset, length, output);
        }
    }

    @Override
    public int getSliceLength(int position)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return rawKeyBlock.getSliceLength(position / 2);
        }
        return rawValueBlock.getSliceLength(position / 2);
    }

    @Override
    public int compareTo(int position, int offset, int length, Block otherBlock, int otherPosition, int otherOffset, int otherLength)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return rawKeyBlock.compareTo(position / 2, offset, length, otherBlock, otherPosition, otherOffset, otherLength);
        }
        return rawValueBlock.compareTo(position / 2, offset, length, otherBlock, otherPosition, otherOffset, otherLength);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return rawKeyBlock.bytesEqual(position / 2, offset, otherSlice, otherOffset, length);
        }
        return rawValueBlock.bytesEqual(position / 2, offset, otherSlice, otherOffset, length);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return rawKeyBlock.bytesCompare(position / 2, offset, length, otherSlice, otherOffset, otherLength);
        }
        return rawValueBlock.bytesCompare(position / 2, offset, length, otherSlice, otherOffset, otherLength);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return rawKeyBlock.equals(position / 2, offset, otherBlock, otherPosition, otherOffset, length);
        }
        return rawValueBlock.equals(position / 2, offset, otherBlock, otherPosition, otherOffset, length);
    }

    @Override
    public long hash(int position, int offset, int length)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return rawKeyBlock.hash(position / 2, offset, length);
        }
        return rawValueBlock.hash(position / 2, offset, length);
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return rawKeyBlock.getObject(position / 2, clazz);
        }
        return rawValueBlock.getObject(position / 2, clazz);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return rawKeyBlock.getSingleValueBlock(position / 2);
        }
        return rawValueBlock.getSingleValueBlock(position / 2);
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        position = getAbsolutePosition(position);
        if (position % 2 == 0) {
            return rawKeyBlock.getEstimatedDataSizeForStats(position / 2);
        }
        return rawValueBlock.getEstimatedDataSizeForStats(position / 2);
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions, int selectedPositionsCount)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        throw new UnsupportedOperationException();
    }

    public Optional<int[]> tryGetHashTable()
    {
        return hashTablesSupplier.tryGetHashTable();
    }

    /**
     * @return position of the value under {@code nativeValue} key. -1 when key is not found.
     */
    public int seekKey(Object nativeValue)
    {
        if (positionCount == 0) {
            return -1;
        }

        if (nativeValue == null) {
            throw new TrinoException(NOT_SUPPORTED, "map key cannot be null or contain nulls");
        }

        int[] hashTable = hashTablesSupplier.getHashTables();

        long hashCode;
        try {
            hashCode = (long) mapType.getKeyNativeHashCode().invoke(nativeValue);
        }
        catch (Throwable throwable) {
            throw handleThrowable(throwable);
        }

        int hashTableOffset = offset / 2 * HASH_MULTIPLIER;
        int hashTableSize = positionCount / 2 * HASH_MULTIPLIER;
        int position = computePosition(hashCode, hashTableSize);
        while (true) {
            int keyPosition = hashTable[hashTableOffset + position];
            if (keyPosition == -1) {
                return -1;
            }

            int rawKeyPosition = offset / 2 + keyPosition;
            checkKeyNotNull(rawKeyBlock, rawKeyPosition);

            Boolean match;
            try {
                // assuming maps with indeterminate keys are not supported.
                // the left and right values are never null because the above call check for null before the insertion
                match = (Boolean) mapType.getKeyBlockNativeEqual().invoke(rawKeyBlock, rawKeyPosition, nativeValue);
            }
            catch (Throwable throwable) {
                throw handleThrowable(throwable);
            }
            checkNotIndeterminate(match);
            if (match) {
                return keyPosition * 2 + 1;
            }
            position++;
            if (position == hashTableSize) {
                position = 0;
            }
        }
    }

    public int seekKey(MethodHandle keyEqualOperator, MethodHandle keyHashOperator, Block targetKeyBlock, int targetKeyPosition)
    {
        if (positionCount == 0) {
            return -1;
        }

        int[] hashTable = hashTablesSupplier.getHashTables();

        checkKeyNotNull(targetKeyBlock, targetKeyPosition);
        long hashCode;
        try {
            hashCode = (long) keyHashOperator.invoke(targetKeyBlock, targetKeyPosition);
        }
        catch (Throwable throwable) {
            throw handleThrowable(throwable);
        }

        int hashTableOffset = offset / 2 * HASH_MULTIPLIER;
        int hashTableSize = positionCount / 2 * HASH_MULTIPLIER;
        int position = computePosition(hashCode, hashTableSize);
        while (true) {
            int keyPosition = hashTable[hashTableOffset + position];
            if (keyPosition == -1) {
                return -1;
            }

            int rawKeyPosition = offset / 2 + keyPosition;
            checkKeyNotNull(rawKeyBlock, rawKeyPosition);

            Boolean match;
            try {
                // assuming maps with indeterminate keys are not supported
                match = (Boolean) keyEqualOperator.invoke(rawKeyBlock, rawKeyPosition, targetKeyBlock, targetKeyPosition);
            }
            catch (Throwable throwable) {
                throw handleThrowable(throwable);
            }
            checkNotIndeterminate(match);
            if (match) {
                return keyPosition * 2 + 1;
            }
            position++;
            if (position == hashTableSize) {
                position = 0;
            }
        }
    }

    // The next 5 seekKeyExact functions are the same as seekKey
    // except MethodHandle.invoke is replaced with invokeExact.

    public int seekKeyExact(long nativeValue)
    {
        if (positionCount == 0) {
            return -1;
        }

        int[] hashTable = hashTablesSupplier.getHashTables();

        long hashCode;
        try {
            hashCode = (long) mapType.getKeyNativeHashCode().invokeExact(nativeValue);
        }
        catch (Throwable throwable) {
            throw handleThrowable(throwable);
        }

        int hashTableOffset = offset / 2 * HASH_MULTIPLIER;
        int hashTableSize = positionCount / 2 * HASH_MULTIPLIER;
        int position = computePosition(hashCode, hashTableSize);
        while (true) {
            int keyPosition = hashTable[hashTableOffset + position];
            if (keyPosition == -1) {
                return -1;
            }

            int rawKeyPosition = offset / 2 + keyPosition;
            checkKeyNotNull(rawKeyBlock, rawKeyPosition);

            Boolean match;
            try {
                // assuming maps with indeterminate keys are not supported
                match = (Boolean) mapType.getKeyBlockNativeEqual().invokeExact(rawKeyBlock, rawKeyPosition, nativeValue);
            }
            catch (Throwable throwable) {
                throw handleThrowable(throwable);
            }
            checkNotIndeterminate(match);
            if (match) {
                return keyPosition * 2 + 1;
            }
            position++;
            if (position == hashTableSize) {
                position = 0;
            }
        }
    }

    public int seekKeyExact(boolean nativeValue)
    {
        if (positionCount == 0) {
            return -1;
        }

        int[] hashTable = hashTablesSupplier.getHashTables();

        long hashCode;
        try {
            hashCode = (long) mapType.getKeyNativeHashCode().invokeExact(nativeValue);
        }
        catch (Throwable throwable) {
            throw handleThrowable(throwable);
        }

        int hashTableOffset = offset / 2 * HASH_MULTIPLIER;
        int hashTableSize = positionCount / 2 * HASH_MULTIPLIER;
        int position = computePosition(hashCode, hashTableSize);
        while (true) {
            int keyPosition = hashTable[hashTableOffset + position];
            if (keyPosition == -1) {
                return -1;
            }

            int rawKeyPosition = offset / 2 + keyPosition;
            checkKeyNotNull(rawKeyBlock, rawKeyPosition);

            Boolean match;
            try {
                // assuming maps with indeterminate keys are not supported
                match = (Boolean) mapType.getKeyBlockNativeEqual().invokeExact(rawKeyBlock, rawKeyPosition, nativeValue);
            }
            catch (Throwable throwable) {
                throw handleThrowable(throwable);
            }
            checkNotIndeterminate(match);
            if (match) {
                return keyPosition * 2 + 1;
            }
            position++;
            if (position == hashTableSize) {
                position = 0;
            }
        }
    }

    public int seekKeyExact(double nativeValue)
    {
        if (positionCount == 0) {
            return -1;
        }

        int[] hashTable = hashTablesSupplier.getHashTables();

        long hashCode;
        try {
            hashCode = (long) mapType.getKeyNativeHashCode().invokeExact(nativeValue);
        }
        catch (Throwable throwable) {
            throw handleThrowable(throwable);
        }

        int hashTableOffset = offset / 2 * HASH_MULTIPLIER;
        int hashTableSize = positionCount / 2 * HASH_MULTIPLIER;
        int position = computePosition(hashCode, hashTableSize);
        while (true) {
            int keyPosition = hashTable[hashTableOffset + position];
            if (keyPosition == -1) {
                return -1;
            }

            int rawKeyPosition = offset / 2 + keyPosition;
            checkKeyNotNull(rawKeyBlock, rawKeyPosition);

            Boolean match;
            try {
                // assuming maps with indeterminate keys are not supported
                match = (Boolean) mapType.getKeyBlockNativeEqual().invokeExact(rawKeyBlock, rawKeyPosition, nativeValue);
            }
            catch (Throwable throwable) {
                throw handleThrowable(throwable);
            }
            checkNotIndeterminate(match);
            if (match) {
                return keyPosition * 2 + 1;
            }
            position++;
            if (position == hashTableSize) {
                position = 0;
            }
        }
    }

    public int seekKeyExact(Object nativeValue)
    {
        if (positionCount == 0) {
            return -1;
        }

        if (nativeValue == null) {
            throw new TrinoException(NOT_SUPPORTED, "map key cannot be null or contain nulls");
        }

        int[] hashTable = hashTablesSupplier.getHashTables();

        long hashCode;
        try {
            hashCode = (long) mapType.getKeyNativeHashCode().invokeExact(nativeValue);
        }
        catch (Throwable throwable) {
            throw handleThrowable(throwable);
        }

        int hashTableOffset = offset / 2 * HASH_MULTIPLIER;
        int hashTableSize = positionCount / 2 * HASH_MULTIPLIER;
        int position = computePosition(hashCode, hashTableSize);
        while (true) {
            int keyPosition = hashTable[hashTableOffset + position];
            if (keyPosition == -1) {
                return -1;
            }

            int rawKeyPosition = offset / 2 + keyPosition;
            checkKeyNotNull(rawKeyBlock, rawKeyPosition);

            Boolean match;
            try {
                // assuming maps with indeterminate keys are not supported
                match = (Boolean) mapType.getKeyBlockNativeEqual().invokeExact(rawKeyBlock, rawKeyPosition, nativeValue);
            }
            catch (Throwable throwable) {
                throw handleThrowable(throwable);
            }
            checkNotIndeterminate(match);
            if (match) {
                return keyPosition * 2 + 1;
            }
            position++;
            if (position == hashTableSize) {
                position = 0;
            }
        }
    }

    private static RuntimeException handleThrowable(Throwable throwable)
    {
        if (throwable instanceof Error) {
            throw (Error) throwable;
        }
        if (throwable instanceof TrinoException) {
            throw (TrinoException) throwable;
        }
        throw new TrinoException(GENERIC_INTERNAL_ERROR, throwable);
    }

    private static void checkKeyNotNull(Block keyBlock, int positionCount)
    {
        if (keyBlock.isNull(positionCount)) {
            throw new TrinoException(NOT_SUPPORTED, "map key cannot be null or contain nulls");
        }
    }

    private static void checkNotIndeterminate(Boolean equalResult)
    {
        if (equalResult == null) {
            throw new TrinoException(NOT_SUPPORTED, "map key cannot be null or contain nulls");
        }
    }

    static class HashTableSupplier
    {
        private MapBlock mapBlock;
        private int[] hashTables;

        public HashTableSupplier(MapBlock mapBlock)
        {
            hashTables = mapBlock.getHashTables().tryGet().orElse(null);
            if (hashTables == null) {
                this.mapBlock = mapBlock;
            }
        }

        public HashTableSupplier(int[] hashTables)
        {
            this.hashTables = requireNonNull(hashTables, "hashTables is null");
        }

        public Optional<int[]> tryGetHashTable()
        {
            if (hashTables == null) {
                hashTables = mapBlock.getHashTables().tryGet().orElse(null);
            }
            return Optional.ofNullable(hashTables);
        }

        public int[] getHashTables()
        {
            if (hashTables == null) {
                mapBlock.ensureHashTableLoaded();
                hashTables = mapBlock.getHashTables().get();
            }
            return hashTables;
        }
    }
}
