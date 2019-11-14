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
package io.prestosql.spi.block;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.MapType;

import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;

final class MapHashTables
{
    private MapHashTables() {}

    /**
     * This method assumes that {@code keyBlock} has no duplicated entries (in the specified range)
     */
    static void buildHashTable(Block keyBlock, int keyOffset, int keyCount, MapType mapType, int[] outputHashTable, int hashTableOffset, int hashTableSize)
    {
        for (int i = 0; i < keyCount; i++) {
            int hash = getHashPosition(mapType, keyBlock, keyOffset + i, hashTableSize);
            while (true) {
                if (outputHashTable[hashTableOffset + hash] == -1) {
                    outputHashTable[hashTableOffset + hash] = i;
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
    static void buildHashTableStrict(
            Block keyBlock,
            int keyOffset,
            int keyCount,
            MapType mapType,
            int[] outputHashTable,
            int hashTableOffset,
            int hashTableSize)
            throws DuplicateMapKeyException
    {
        for (int i = 0; i < keyCount; i++) {
            int hash = getHashPosition(mapType, keyBlock, keyOffset + i, hashTableSize);
            while (true) {
                if (outputHashTable[hashTableOffset + hash] == -1) {
                    outputHashTable[hashTableOffset + hash] = i;
                    break;
                }

                Boolean isDuplicateKey;
                try {
                    // assuming maps with indeterminate keys are not supported
                    isDuplicateKey = (Boolean) mapType.getKeyBlockEquals().invokeExact(keyBlock, keyOffset + i, keyBlock, keyOffset + outputHashTable[hashTableOffset + hash]);
                }
                catch (RuntimeException e) {
                    throw e;
                }
                catch (Throwable throwable) {
                    throw new RuntimeException(throwable);
                }

                if (isDuplicateKey == null) {
                    throw new PrestoException(NOT_SUPPORTED, "map key cannot be null or contain nulls");
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
            throw new IllegalArgumentException("map keys cannot be null");
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
