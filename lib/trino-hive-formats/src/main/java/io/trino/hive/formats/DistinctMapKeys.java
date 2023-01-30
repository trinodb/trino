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
package io.trino.hive.formats;

import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.DuplicateMapKeyException;
import io.trino.spi.type.MapType;

import java.util.Arrays;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public class DistinctMapKeys
{
    private static final int HASH_MULTIPLIER = 2;

    private final MapType mapType;
    private final boolean userLastEntry;

    private boolean[] distinctBuffer = new boolean[0];
    private int[] hashTableBuffer = new int[0];

    public DistinctMapKeys(MapType mapType, boolean userLastEntry)
    {
        this.mapType = mapType;
        this.userLastEntry = userLastEntry;
    }

    /**
     * Determines which keys are distinct and thus should be copied through to the final results.
     *
     * @throws TrinoException if there is an indeterminate key
     */
    // NOTE: this is a fork of the logic in MapHashTables
    public boolean[] selectDistinctKeys(Block keyBlock)
            throws DuplicateMapKeyException
    {
        int keyCount = keyBlock.getPositionCount();
        int hashTableSize = keyCount * HASH_MULTIPLIER;

        if (distinctBuffer.length < keyCount) {
            int bufferSize = calculateBufferSize(keyCount);
            distinctBuffer = new boolean[bufferSize];
            hashTableBuffer = new int[bufferSize];
        }
        boolean[] distinct = distinctBuffer;
        Arrays.fill(distinct, false);
        int[] hashTable = hashTableBuffer;
        Arrays.fill(hashTable, -1);

        int hashTableOffset = 0;
        for (int i = 0; i < keyCount; i++) {
            // Nulls are not marked as distinct and thus are ignored
            if (keyBlock.isNull(i)) {
                continue;
            }
            int hash = getHashPosition(keyBlock, i, hashTableSize);
            while (true) {
                if (hashTable[hashTableOffset + hash] == -1) {
                    hashTable[hashTableOffset + hash] = i;
                    distinct[i] = true;
                    break;
                }

                Boolean isDuplicateKey;
                try {
                    // assuming maps with indeterminate keys are not supported
                    isDuplicateKey = (Boolean) mapType.getKeyBlockEqual().invokeExact(keyBlock, i, keyBlock, hashTable[hashTableOffset + hash]);
                }
                catch (RuntimeException e) {
                    throw e;
                }
                catch (Throwable throwable) {
                    throw new RuntimeException(throwable);
                }

                // this will only happen if there is an indeterminate key
                if (isDuplicateKey == null) {
                    throw new TrinoException(NOT_SUPPORTED, "map key cannot be null or contain nulls");
                }

                // duplicate keys are ignored
                if (isDuplicateKey) {
                    if (userLastEntry) {
                        int duplicateIndex = hashTable[hashTableOffset + hash];
                        distinct[duplicateIndex] = false;

                        hashTable[hashTableOffset + hash] = i;
                        distinct[i] = true;
                    }
                    break;
                }

                hash++;
                if (hash == hashTableSize) {
                    hash = 0;
                }
            }
        }
        return distinct;
    }

    private int getHashPosition(Block keyBlock, int position, int hashTableSize)
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

    // This function reduces the 64 bit hashcode to [0, hashTableSize) uniformly. It first reduces the hashcode to 32-bit
    // integer x then normalize it to x / 2^32 * hashSize to reduce the range of x from [0, 2^32) to [0, hashTableSize)
    private static int computePosition(long hashcode, int hashTableSize)
    {
        return (int) ((Integer.toUnsignedLong(Long.hashCode(hashcode)) * hashTableSize) >> 32);
    }

    private static int calculateBufferSize(int value)
    {
        if (value < 128) {
            return 128;
        }

        int highestOneBit = Integer.highestOneBit(value);
        if (value == highestOneBit) {
            return value;
        }
        return highestOneBit << 1;
    }
}
