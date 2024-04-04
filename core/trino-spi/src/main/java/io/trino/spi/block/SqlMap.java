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
import io.trino.spi.TrinoException;
import io.trino.spi.block.MapHashTables.HashBuildMode;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;
import java.util.Optional;
import java.util.function.ObjLongConsumer;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.block.MapHashTables.HASH_MULTIPLIER;
import static io.trino.spi.block.MapHashTables.computePosition;
import static io.trino.spi.block.MapHashTables.createSingleTable;
import static java.lang.String.format;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

public class SqlMap
{
    private static final int INSTANCE_SIZE = instanceSize(SqlMap.class);

    private final MapType mapType;
    private final Block rawKeyBlock;
    private final Block rawValueBlock;
    private final HashTableSupplier hashTablesSupplier;
    private final int offset;
    private final int size;

    public SqlMap(MapType mapType, HashBuildMode mode, Block keyBlock, Block valueBlock)
    {
        this.mapType = requireNonNull(mapType, "mapType is null");
        if (keyBlock.getPositionCount() != valueBlock.getPositionCount()) {
            throw new IllegalArgumentException(format("Key and value blocks have different size: %s %s", keyBlock.getPositionCount(), valueBlock.getPositionCount()));
        }
        this.rawKeyBlock = keyBlock;
        this.rawValueBlock = valueBlock;
        this.offset = 0;
        this.size = keyBlock.getPositionCount();

        this.hashTablesSupplier = new HashTableSupplier(createSingleTable(mapType, mode, keyBlock).get());
    }

    SqlMap(MapType mapType, Block rawKeyBlock, Block rawValueBlock, HashTableSupplier hashTablesSupplier, int offset, int size)
    {
        this.mapType = requireNonNull(mapType, "mapType is null");
        this.rawKeyBlock = requireNonNull(rawKeyBlock, "rawKeyBlock is null");
        this.rawValueBlock = requireNonNull(rawValueBlock, "rawValueBlock is null");
        this.hashTablesSupplier = requireNonNull(hashTablesSupplier, "hashTablesSupplier is null");

        checkFromIndexSize(offset, size, rawKeyBlock.getPositionCount());
        checkFromIndexSize(offset, size, rawValueBlock.getPositionCount());
        this.offset = offset;
        this.size = size;
    }

    public Type getMapType()
    {
        return mapType;
    }

    public int getSize()
    {
        return size;
    }

    public int getRawOffset()
    {
        return offset;
    }

    public Block getRawKeyBlock()
    {
        return rawKeyBlock;
    }

    public Block getRawValueBlock()
    {
        return rawValueBlock;
    }

    public int getUnderlyingKeyPosition(int position)
    {
        return rawKeyBlock.getUnderlyingValuePosition(offset + position);
    }

    public ValueBlock getUnderlyingKeyBlock()
    {
        return rawKeyBlock.getUnderlyingValueBlock();
    }

    public int getUnderlyingValuePosition(int position)
    {
        return rawValueBlock.getUnderlyingValuePosition(offset + position);
    }

    public ValueBlock getUnderlyingValueBlock()
    {
        return rawValueBlock.getUnderlyingValueBlock();
    }

    @Override
    public String toString()
    {
        return format("SqlMap{size=%d}", size);
    }

    /**
     * @return position of the value under {@code nativeValue} key. -1 when key is not found.
     */
    public int seekKey(Object nativeValue)
    {
        if (size == 0) {
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

        int hashTableOffset = offset * HASH_MULTIPLIER;
        int hashTableSize = size * HASH_MULTIPLIER;
        int position = computePosition(hashCode, hashTableSize);
        while (true) {
            int keyPosition = hashTable[hashTableOffset + position];
            if (keyPosition == -1) {
                return -1;
            }

            int rawKeyPosition = offset + keyPosition;
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
                return keyPosition;
            }
            position++;
            if (position == hashTableSize) {
                position = 0;
            }
        }
    }

    public int seekKey(MethodHandle keyEqualOperator, MethodHandle keyHashOperator, Block targetKeyBlock, int targetKeyPosition)
    {
        if (size == 0) {
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

        int hashTableOffset = offset * HASH_MULTIPLIER;
        int hashTableSize = size * HASH_MULTIPLIER;
        int position = computePosition(hashCode, hashTableSize);
        while (true) {
            int keyPosition = hashTable[hashTableOffset + position];
            if (keyPosition == -1) {
                return -1;
            }

            int rawKeyPosition = offset + keyPosition;
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
                return keyPosition;
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
        if (size == 0) {
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

        int hashTableOffset = offset * HASH_MULTIPLIER;
        int hashTableSize = size * HASH_MULTIPLIER;
        int position = computePosition(hashCode, hashTableSize);
        while (true) {
            int keyPosition = hashTable[hashTableOffset + position];
            if (keyPosition == -1) {
                return -1;
            }

            int rawKeyPosition = offset + keyPosition;
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
                return keyPosition;
            }
            position++;
            if (position == hashTableSize) {
                position = 0;
            }
        }
    }

    public int seekKeyExact(boolean nativeValue)
    {
        if (size == 0) {
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

        int hashTableOffset = offset * HASH_MULTIPLIER;
        int hashTableSize = size * HASH_MULTIPLIER;
        int position = computePosition(hashCode, hashTableSize);
        while (true) {
            int keyPosition = hashTable[hashTableOffset + position];
            if (keyPosition == -1) {
                return -1;
            }

            int rawKeyPosition = offset + keyPosition;
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
                return keyPosition;
            }
            position++;
            if (position == hashTableSize) {
                position = 0;
            }
        }
    }

    public int seekKeyExact(double nativeValue)
    {
        if (size == 0) {
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

        int hashTableOffset = offset * HASH_MULTIPLIER;
        int hashTableSize = size * HASH_MULTIPLIER;
        int position = computePosition(hashCode, hashTableSize);
        while (true) {
            int keyPosition = hashTable[hashTableOffset + position];
            if (keyPosition == -1) {
                return -1;
            }

            int rawKeyPosition = offset + keyPosition;
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
                return keyPosition;
            }
            position++;
            if (position == hashTableSize) {
                position = 0;
            }
        }
    }

    public int seekKeyExact(Object nativeValue)
    {
        if (size == 0) {
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

        int hashTableOffset = offset * HASH_MULTIPLIER;
        int hashTableSize = size * HASH_MULTIPLIER;
        int position = computePosition(hashCode, hashTableSize);
        while (true) {
            int keyPosition = hashTable[hashTableOffset + position];
            if (keyPosition == -1) {
                return -1;
            }

            int rawKeyPosition = offset + keyPosition;
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
                return keyPosition;
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

    public long getSizeInBytes()
    {
        return rawKeyBlock.getRegionSizeInBytes(offset, size) +
                rawValueBlock.getRegionSizeInBytes(offset, size) +
                sizeOfIntArray(size * HASH_MULTIPLIER);
    }

    public long getRetainedSizeInBytes()
    {
        long size = INSTANCE_SIZE +
                rawKeyBlock.getRetainedSizeInBytes() +
                rawValueBlock.getRetainedSizeInBytes() +
                hashTablesSupplier.tryGetHashTable().map(SizeOf::sizeOf).orElse(0L);
        return size;
    }

    public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
    {
        consumer.accept(this, INSTANCE_SIZE);
        consumer.accept(rawKeyBlock, rawKeyBlock.getRetainedSizeInBytes());
        consumer.accept(rawValueBlock, rawValueBlock.getRetainedSizeInBytes());
        hashTablesSupplier.tryGetHashTable().ifPresent(hashTables -> consumer.accept(hashTables, SizeOf.sizeOf(hashTables)));
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
