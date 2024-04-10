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

import com.google.errorprone.annotations.ThreadSafe;
import io.trino.spi.TrinoException;
import io.trino.spi.type.MapType;
import jakarta.annotation.Nullable;

import java.util.Arrays;
import java.util.Optional;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;

@ThreadSafe
public final class MapHashTables
{
    public static final int INSTANCE_SIZE = instanceSize(MapHashTables.class);

    // inverse of the hash fill ratio must be integer
    static final int HASH_MULTIPLIER = 2;

    public enum HashBuildMode
    {
        DUPLICATE_NOT_CHECKED, STRICT_EQUALS, STRICT_NOT_DISTINCT_FROM
    }

    private final MapType mapType;
    private final HashBuildMode mode;
    private final int hashTableCount;

    @SuppressWarnings("VolatileArrayField")
    @Nullable
    private volatile int[] hashTables;

    static MapHashTables createSingleTable(MapType mapType, HashBuildMode mode, Block keyBlock)
    {
        int[] hashTables = new int[keyBlock.getPositionCount() * HASH_MULTIPLIER];
        Arrays.fill(hashTables, -1);
        buildHashTable(mode, mapType, keyBlock, 0, keyBlock.getPositionCount(), hashTables);
        return new MapHashTables(mapType, mode, 1, Optional.of(hashTables));
    }

    static MapHashTables create(HashBuildMode mode, MapType mapType, int hashTableCount, Block keyBlock, int[] offsets, @Nullable boolean[] mapIsNull)
    {
        MapHashTables hashTables = new MapHashTables(mapType, mode, hashTableCount, Optional.empty());
        hashTables.buildAllHashTables(keyBlock, offsets, mapIsNull);
        return hashTables;
    }

    MapHashTables(MapType mapType, HashBuildMode mode, int hashTableCount, Optional<int[]> hashTables)
    {
        this.mapType = mapType;
        this.mode = mode;
        this.hashTableCount = hashTableCount;
        this.hashTables = hashTables.orElse(null);
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(hashTables);
    }

    /**
     * Returns the raw hash tables, which must not be modified.
     *
     * @throws IllegalStateException if the hash tables have not been built
     */
    int[] get()
    {
        if (hashTables == null) {
            throw new IllegalStateException("hashTables are not built");
        }
        return hashTables;
    }

    /**
     * Returns the raw hash tables if they have been built.  The raw hash tables must not be modified.
     */
    Optional<int[]> tryGet()
    {
        return Optional.ofNullable(hashTables);
    }

    void buildAllHashTablesIfNecessary(Block rawKeyBlock, int[] offsets, @Nullable boolean[] mapIsNull)
    {
        // this is double-checked locking
        if (hashTables == null) {
            buildAllHashTables(rawKeyBlock, offsets, mapIsNull);
        }
    }

    private synchronized void buildAllHashTables(Block rawKeyBlock, int[] offsets, @Nullable boolean[] mapIsNull)
    {
        if (hashTables != null) {
            return;
        }

        int[] hashTables = new int[rawKeyBlock.getPositionCount() * HASH_MULTIPLIER];
        Arrays.fill(hashTables, -1);

        for (int i = 0; i < hashTableCount; i++) {
            int keyOffset = offsets[i];
            int keyCount = offsets[i + 1] - keyOffset;
            if (keyCount < 0) {
                throw new IllegalArgumentException(format("Offset is not monotonically ascending. offsets[%s]=%s, offsets[%s]=%s", i, offsets[i], i + 1, offsets[i + 1]));
            }
            if (mapIsNull != null && mapIsNull[i] && keyCount != 0) {
                throw new IllegalArgumentException("A null map must have zero entries");
            }
            buildHashTable(mode, mapType, rawKeyBlock, keyOffset, keyCount, hashTables);
        }
        this.hashTables = hashTables;
    }

    private static void buildHashTable(HashBuildMode mode, MapType mapType, Block rawKeyBlock, int keyOffset, int keyCount, int[] hashTables)
    {
        switch (mode) {
            case DUPLICATE_NOT_CHECKED -> buildHashTableDuplicateNotChecked(mapType, rawKeyBlock, keyOffset, keyCount, hashTables);
            case STRICT_EQUALS -> buildHashTableStrict(mapType, rawKeyBlock, keyOffset, keyCount, hashTables);
            case STRICT_NOT_DISTINCT_FROM -> buildDistinctHashTableStrict(mapType, rawKeyBlock, keyOffset, keyCount, hashTables);
        }
    }

    private static void buildHashTableDuplicateNotChecked(MapType mapType, Block keyBlock, int keyOffset, int keyCount, int[] hashTables)
    {
        int hashTableOffset = keyOffset * HASH_MULTIPLIER;
        int hashTableSize = keyCount * HASH_MULTIPLIER;
        for (int i = 0; i < keyCount; i++) {
            int hash = getHashPosition(mapType, keyBlock, keyOffset + i, hashTableSize);
            while (true) {
                if (hashTables[hashTableOffset + hash] == -1) {
                    hashTables[hashTableOffset + hash] = i;
                    break;
                }
                hash++;
                if (hash == hashTableSize) {
                    hash = 0;
                }
            }
        }
    }

    /**
     * This method checks whether {@code keyBlock} has duplicated entries (in the specified range)
     */
    private static void buildHashTableStrict(MapType mapType, Block keyBlock, int keyOffset, int keyCount, int[] hashTables)
    {
        int hashTableOffset = keyOffset * HASH_MULTIPLIER;
        int hashTableSize = keyCount * HASH_MULTIPLIER;

        for (int i = 0; i < keyCount; i++) {
            // this throws if the position is null
            int hash = getHashPosition(mapType, keyBlock, keyOffset + i, hashTableSize);
            while (true) {
                if (hashTables[hashTableOffset + hash] == -1) {
                    hashTables[hashTableOffset + hash] = i;
                    break;
                }

                Boolean isDuplicateKey;
                try {
                    // assuming maps with indeterminate keys are not supported,
                    // the left and right values are never null because the above call check for null before the insertion
                    isDuplicateKey = (Boolean) mapType.getKeyBlockEqual().invokeExact(keyBlock, keyOffset + i, keyBlock, keyOffset + hashTables[hashTableOffset + hash]);
                }
                catch (RuntimeException e) {
                    throw e;
                }
                catch (Throwable throwable) {
                    throw new RuntimeException(throwable);
                }

                if (isDuplicateKey == null) {
                    throw new TrinoException(NOT_SUPPORTED, "map key cannot be null or contain nulls");
                }

                if (isDuplicateKey) {
                    throw new DuplicateMapKeyException(keyBlock, keyOffset + i);
                }

                hash++;
                if (hash == hashTableSize) {
                    hash = 0;
                }
            }
        }
    }

    /**
     * This method checks whether {@code keyBlock} has duplicates based on type NOT DISTINCT FROM.
     */
    private static void buildDistinctHashTableStrict(MapType mapType, Block keyBlock, int keyOffset, int keyCount, int[] hashTables)
    {
        int hashTableOffset = keyOffset * HASH_MULTIPLIER;
        int hashTableSize = keyCount * HASH_MULTIPLIER;

        for (int i = 0; i < keyCount; i++) {
            int hash = getHashPosition(mapType, keyBlock, keyOffset + i, hashTableSize);
            while (true) {
                if (hashTables[hashTableOffset + hash] == -1) {
                    hashTables[hashTableOffset + hash] = i;
                    break;
                }

                boolean isDuplicateKey;
                try {
                    // assuming maps with indeterminate keys are not supported
                    isDuplicateKey = (boolean) mapType.getKeyBlockNotDistinctFrom().invokeExact(keyBlock, keyOffset + i, keyBlock, keyOffset + hashTables[hashTableOffset + hash]);
                }
                catch (RuntimeException e) {
                    throw e;
                }
                catch (Throwable throwable) {
                    throw new RuntimeException(throwable);
                }

                if (isDuplicateKey) {
                    throw new DuplicateMapKeyException(keyBlock, keyOffset + i);
                }

                hash++;
                if (hash == hashTableSize) {
                    hash = 0;
                }
            }
        }
    }

    private static int getHashPosition(MapType mapType, Block keyBlock, int position, int hashTableSize)
    {
        if (keyBlock.isNull(position)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "map key cannot be null");
        }

        long hashCode;
        try {
            hashCode = (long) mapType.getKeyBlockHashCode().invokeExact(keyBlock, position);
        }
        catch (RuntimeException e) {
            throw e;
        }
        catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }

        return computePosition(hashCode, hashTableSize);
    }

    // This function reduces the 64 bit hashcode to [0, hashTableSize) uniformly. It first reduces the hashcode to 32 bit
    // integer x then normalize it to x / 2^32 * hashSize to reduce the range of x from [0, 2^32) to [0, hashTableSize)
    static int computePosition(long hashcode, int hashTableSize)
    {
        return (int) ((Integer.toUnsignedLong(Long.hashCode(hashcode)) * hashTableSize) >> 32);
    }
}
