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

import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import java.lang.invoke.MethodHandle;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.ObjLongConsumer;

import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.block.MapHashTables.HASH_MULTIPLIER;
import static io.trino.spi.block.MapHashTables.computePosition;
import static java.lang.String.format;

public class SingleMapBlock
        extends AbstractSingleMapBlock
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleMapBlock.class).instanceSize();

    private final int offset;
    private final int positionCount;    // The number of keys in this single map * 2
    private final AbstractMapBlock mapBlock;

    SingleMapBlock(int offset, int positionCount, AbstractMapBlock mapBlock)
    {
        this.offset = offset;
        this.positionCount = positionCount;
        this.mapBlock = mapBlock;
    }

    public Type getMapType()
    {
        return mapBlock.getMapType();
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
        return mapBlock.getRawKeyBlock().getRegionSizeInBytes(offset / 2, positionCount / 2) +
                mapBlock.getRawValueBlock().getRegionSizeInBytes(offset / 2, positionCount / 2) +
                sizeOfIntArray(positionCount / 2 * HASH_MULTIPLIER);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + mapBlock.getRetainedSizeInBytes();
    }

    @Override
    public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
    {
        consumer.accept(mapBlock.getRawKeyBlock(), mapBlock.getRawKeyBlock().getRetainedSizeInBytes());
        consumer.accept(mapBlock.getRawValueBlock(), mapBlock.getRawValueBlock().getRetainedSizeInBytes());
        consumer.accept(mapBlock.getHashTables(), mapBlock.getHashTables().getRetainedSizeInBytes());
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public String getEncodingName()
    {
        return SingleMapBlockEncoding.NAME;
    }

    @Override
    public int getOffset()
    {
        return offset;
    }

    @Override
    Block getRawKeyBlock()
    {
        return mapBlock.getRawKeyBlock();
    }

    @Override
    Block getRawValueBlock()
    {
        return mapBlock.getRawValueBlock();
    }

    @Override
    public String toString()
    {
        return format("SingleMapBlock{positionCount=%d}", getPositionCount());
    }

    @Override
    public boolean isLoaded()
    {
        return mapBlock.getRawKeyBlock().isLoaded() && mapBlock.getRawValueBlock().isLoaded();
    }

    @Override
    public Block getLoadedBlock()
    {
        if (mapBlock.getRawKeyBlock() != mapBlock.getRawKeyBlock().getLoadedBlock()) {
            // keyBlock has to be loaded since MapBlock constructs hash table eagerly.
            throw new IllegalStateException();
        }

        Block loadedValueBlock = mapBlock.getRawValueBlock().getLoadedBlock();
        if (loadedValueBlock == mapBlock.getRawValueBlock()) {
            return this;
        }
        return new SingleMapBlock(
                offset,
                positionCount,
                mapBlock);
    }

    public Optional<int[]> tryGetHashTable()
    {
        return mapBlock.getHashTables().tryGet();
    }

    /**
     * @return position of the value under {@code nativeValue} key. -1 when key is not found.
     */
    public int seekKey(Object nativeValue)
    {
        if (positionCount == 0) {
            return -1;
        }

        mapBlock.ensureHashTableLoaded();
        int[] hashTable = mapBlock.getHashTables().get();

        long hashCode;
        try {
            hashCode = (long) mapBlock.getMapType().getKeyNativeHashCode().invoke(nativeValue);
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
            Boolean match;
            try {
                // assuming maps with indeterminate keys are not supported
                match = (Boolean) mapBlock.getMapType().getKeyBlockNativeEqual().invoke(mapBlock.getRawKeyBlock(), offset / 2 + keyPosition, nativeValue);
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

        mapBlock.ensureHashTableLoaded();
        int[] hashTable = mapBlock.getHashTables().get();

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
            Boolean match;
            try {
                // assuming maps with indeterminate keys are not supported
                match = (Boolean) keyEqualOperator.invoke(mapBlock.getRawKeyBlock(), offset / 2 + keyPosition, targetKeyBlock, targetKeyPosition);
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

        mapBlock.ensureHashTableLoaded();
        int[] hashTable = mapBlock.getHashTables().get();

        long hashCode;
        try {
            hashCode = (long) mapBlock.getMapType().getKeyNativeHashCode().invokeExact(nativeValue);
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
            Boolean match;
            try {
                // assuming maps with indeterminate keys are not supported
                match = (Boolean) mapBlock.getMapType().getKeyBlockNativeEqual().invokeExact(mapBlock.getRawKeyBlock(), offset / 2 + keyPosition, nativeValue);
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

        mapBlock.ensureHashTableLoaded();
        int[] hashTable = mapBlock.getHashTables().get();

        long hashCode;
        try {
            hashCode = (long) mapBlock.getMapType().getKeyNativeHashCode().invokeExact(nativeValue);
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
            Boolean match;
            try {
                // assuming maps with indeterminate keys are not supported
                match = (Boolean) mapBlock.getMapType().getKeyBlockNativeEqual().invokeExact(mapBlock.getRawKeyBlock(), offset / 2 + keyPosition, nativeValue);
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

        mapBlock.ensureHashTableLoaded();
        int[] hashTable = mapBlock.getHashTables().get();

        long hashCode;
        try {
            hashCode = (long) mapBlock.getMapType().getKeyNativeHashCode().invokeExact(nativeValue);
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
            Boolean match;
            try {
                // assuming maps with indeterminate keys are not supported
                match = (Boolean) mapBlock.getMapType().getKeyBlockNativeEqual().invokeExact(mapBlock.getRawKeyBlock(), offset / 2 + keyPosition, nativeValue);
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

        mapBlock.ensureHashTableLoaded();
        int[] hashTable = mapBlock.getHashTables().get();

        long hashCode;
        try {
            hashCode = (long) mapBlock.getMapType().getKeyNativeHashCode().invokeExact(nativeValue);
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
            Boolean match;
            try {
                // assuming maps with indeterminate keys are not supported
                match = (Boolean) mapBlock.getMapType().getKeyBlockNativeEqual().invokeExact(mapBlock.getRawKeyBlock(), offset / 2 + keyPosition, nativeValue);
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

    private static void checkNotIndeterminate(Boolean equalResult)
    {
        if (equalResult == null) {
            throw new TrinoException(NOT_SUPPORTED, "map key cannot be null or contain nulls");
        }
    }
}
