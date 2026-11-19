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
package io.trino.parquet.writer;

import io.airlift.slice.Slice;
import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;

import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Math.clamp;
import static org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter.UPPER_BOUND_BYTES;

final class FoldingBloomFilter
{
    private static final int BLOCK_BYTES = 32;

    private final byte[] bitset;
    private final BlockSplitBloomFilter filter;
    private final double targetFpp;

    FoldingBloomFilter(int maxBytes, double targetFpp)
    {
        bitset = new byte[Integer.highestOneBit(clamp(maxBytes, BLOCK_BYTES, UPPER_BOUND_BYTES))];
        filter = new BlockSplitBloomFilter(bitset);
        this.targetFpp = targetFpp;
    }

    BlockSplitBloomFilter filter()
    {
        return filter;
    }

    BlockSplitBloomFilter fold()
    {
        byte[] current = bitset;
        while (current.length > BLOCK_BYTES) {
            Slice source = wrappedBuffer(current);
            byte[] candidate = new byte[current.length / 2];
            Slice destination = wrappedBuffer(candidate);
            double sumFpp = 0;
            for (int block = 0; block < candidate.length; block += BLOCK_BYTES) {
                // Halving the block count drops the low bit of the Parquet block index.
                // Each pair of adjacent blocks therefore maps to one output block.
                double blockFpp = 1;
                for (int word = 0; word < BLOCK_BYTES; word += Integer.BYTES) {
                    int bits = source.getInt(2 * block + word) | source.getInt(2 * block + BLOCK_BYTES + word);
                    destination.setInt(block + word, bits);
                    // A probe must hit a set bit in each of the eight words.
                    blockFpp *= Integer.bitCount(bits) / (double) Integer.SIZE;
                }
                sumFpp += blockFpp;
            }
            int blockCount = candidate.length / BLOCK_BYTES;
            if (sumFpp > targetFpp * blockCount) {
                break;
            }
            current = candidate;
        }
        return current == bitset ? filter : new BlockSplitBloomFilter(current);
    }
}
