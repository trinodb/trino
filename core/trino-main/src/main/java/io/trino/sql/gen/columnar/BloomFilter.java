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
package io.trino.sql.gen.columnar;

import io.airlift.slice.Slice;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.spi.block.Block;

public interface BloomFilter
{
    long hash(long value);

    long hash(double value);

    long hash(Slice value);

    long hash(Object value);

    void insert(long hashCode);

    boolean contains(Block block, int position);

    abstract class AbstractBloomFilter
            implements BloomFilter
    {
        private final long[] bloom;
        private final int bloomSizeMask;
        private final boolean containsNull;

        /**
         * A Bloom filter for a set of Slice values.
         * This is approx 2X faster than the Bloom filter implementations in ORC and parquet because
         * it uses single hash function and uses that to set 3 bits within a 64 bit word.
         * The memory footprint is up to (4 * values.size()) bytes, which is much smaller than maintaining a hash set of strings.
         *
         * @param containsNull whether null values are contained by the filter
         * @param expectedValuesCount expected number of distinct values
         */
        public AbstractBloomFilter(boolean containsNull, int expectedValuesCount)
        {
            this.containsNull = containsNull;
            int bloomSize = getBloomFilterSize(expectedValuesCount);
            bloom = new long[bloomSize];
            bloomSizeMask = bloomSize - 1;
        }

        @UsedByGeneratedCode
        public boolean containsNull()
        {
            return containsNull;
        }

        @Override
        public void insert(long hashCode)
        {
            // Set 3 bits in a 64 bit word
            bloom[bloomIndex(hashCode)] |= bloomMask(hashCode);
        }

        @Override
        public long hash(long value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long hash(double value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long hash(Slice value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long hash(Object value)
        {
            throw new UnsupportedOperationException();
        }

        protected boolean contains(long hashCode)
        {
            long mask = bloomMask(hashCode);
            return mask == (bloom[bloomIndex(hashCode)] & mask);
        }

        private int bloomIndex(long hashCode)
        {
            // Lower 21 bits are not used by bloomMask
            // These are enough for the maximum size array that will be used here
            return (int) (hashCode & bloomSizeMask);
        }

        private static long bloomMask(long hashCode)
        {
            // returned mask sets 3 bits based on portions of given hash
            // Extract 38th to 43rd bits
            return (1L << ((hashCode >> 21) & 63))
                    // Extract 32nd to 37th bits
                    | (1L << ((hashCode >> 27) & 63))
                    // Extract 26th to 31st bits
                    | (1L << ((hashCode >> 33) & 63));
        }

        private static int getBloomFilterSize(int valuesCount)
        {
            // Linear hash table size is the highest power of two less than or equal to number of values * 4. This means that the
            // table is under half full, e.g. 127 elements gets 256 slots.
            int hashTableSize = Integer.highestOneBit(valuesCount * 4);
            // We will allocate 8 bits in the bloom filter for every slot in a comparable hash table.
            // The bloomSize is a count of longs, hence / 8.
            return Math.max(1, hashTableSize / 8);
        }
    }
}
