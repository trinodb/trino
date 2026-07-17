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
package io.trino.parquet.reader.flat;

import io.trino.spi.block.Block;

import java.util.List;

import static io.trino.spi.block.Bitmap.allocateWords;
import static io.trino.spi.block.Bitmap.clear;
import static io.trino.spi.block.Bitmap.countTransitions;
import static io.trino.spi.block.Bitmap.getBits;

public interface ColumnAdapter<BufferType>
{
    /**
     * Temporary buffer used for null unpacking
     */
    default BufferType createTemporaryBuffer(int currentOffset, int size, BufferType buffer)
    {
        // Null unpacking may read one sentinel value after the last non-null value when filling trailing nulls.
        return createBuffer(size + 1);
    }

    BufferType createBuffer(int size);

    void copyValue(BufferType source, int sourceIndex, BufferType destination, int destinationIndex);

    void copyValues(BufferType source, int sourceIndex, BufferType destination, int destinationIndex, int length);

    /// Creates a block from values and a validity bitmap using the [io.trino.spi.block.Bitmap] encoding.
    Block createNullableBlock(long[] valueIsValid, BufferType values);

    /// Creates a dictionary block with one trailing null entry. The validity bitmap uses the
    /// [io.trino.spi.block.Bitmap] encoding.
    default Block createNullableDictionaryBlock(BufferType dictionary, int nonNullsCount)
    {
        long[] valueIsValid = allocateWords(nonNullsCount + 1, true);
        clear(valueIsValid, 0, nonNullsCount);
        return createNullableBlock(valueIsValid, dictionary);
    }

    Block createNonNullBlock(BufferType values);

    /// Expands packed non-null values into a destination shaped by a validity bitmap using the
    /// [io.trino.spi.block.Bitmap] encoding.
    default void unpackNullValues(BufferType source, BufferType destination, long[] valueIsValid, int destOffset, int nonNullCount, int totalValuesCount)
    {
        // Benchmarking shows scalar copying is faster once a validity word contains this many short runs.
        int scalarCopyTransitionThreshold = 12;
        int srcOffset = 0;
        int endOffset = destOffset + totalValuesCount;
        while (srcOffset < nonNullCount && destOffset < endOffset) {
            int bitsInWord = Math.min(Long.SIZE, endOffset - destOffset);
            long validBits = getBits(valueIsValid, 0, destOffset, bitsInWord);
            if (validBits == 0) {
                destOffset += bitsInWord;
                continue;
            }
            if (Long.bitCount(validBits) == bitsInWord) {
                copyValues(source, srcOffset, destination, destOffset, bitsInWord);
                srcOffset += bitsInWord;
                destOffset += bitsInWord;
                continue;
            }
            if (countTransitions(validBits, bitsInWord) >= scalarCopyTransitionThreshold) {
                int endOffsetInWord = destOffset + bitsInWord;
                while (destOffset < endOffsetInWord) {
                    copyValue(source, srcOffset, destination, destOffset);
                    srcOffset += (int) (validBits & 1);
                    destOffset++;
                    validBits >>>= 1;
                }
                continue;
            }

            int offsetInWord = 0;
            while (offsetInWord < bitsInWord) {
                int nullCount = Math.min(Long.numberOfTrailingZeros(validBits), bitsInWord - offsetInWord);
                destOffset += nullCount;
                offsetInWord += nullCount;
                validBits >>>= nullCount;
                if (offsetInWord == bitsInWord) {
                    break;
                }

                int validCount = Math.min(Long.numberOfTrailingZeros(~validBits), bitsInWord - offsetInWord);
                if (validCount == 1) {
                    // Avoid copyValues overhead for singleton valid runs.
                    copyValue(source, srcOffset, destination, destOffset);
                }
                else {
                    copyValues(source, srcOffset, destination, destOffset, validCount);
                }
                srcOffset += validCount;
                destOffset += validCount;
                offsetInWord += validCount;
                validBits >>>= validCount;
            }
        }
    }

    void decodeDictionaryIds(BufferType values, int offset, int length, int[] ids, BufferType dictionary);

    long getSizeInBytes(BufferType values);

    BufferType merge(List<BufferType> buffers);
}
