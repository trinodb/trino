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
package io.trino.sql.planner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.DoNotCall;
import io.airlift.slice.XxHash64;

import java.util.Arrays;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A Bloom filter for a set of long values.
 * This is approx 2X faster than the Bloom filter implementations in ORC and parquet because
 * it uses single hash function and uses that to set 3 bits within a 64 bit word.
 * The memory footprint is up to (4 * values.size()) bytes, which is much smaller than maintaining a hash set of longs.
 */
public final class LongBloomFilter
{
    private static final int EXPECTED_NDV = 1_000_000;

    private final long[] bloom;
    private final int bloomSizeMask;
    private int minDistinctHashes;

    public LongBloomFilter()
    {
        int bloomSize = getBloomFilterSize(EXPECTED_NDV);
        bloom = new long[bloomSize];
        bloomSizeMask = bloomSize - 1;
    }

    private LongBloomFilter(long[] bloom, int minDistinctHashes)
    {
        this.bloom = bloom;
        this.bloomSizeMask = bloom.length - 1;
        this.minDistinctHashes = minDistinctHashes;
    }

    @JsonCreator
    @DoNotCall // For JSON deserialization only
    public static LongBloomFilter fromJson(
            @JsonProperty("bloom") long[] bloom,
            @JsonProperty("minDistinctHashes") int minDistinctHashes)
    {
        return new LongBloomFilter(bloom, minDistinctHashes);
    }

    @JsonProperty
    @DoNotCall
    public final long[] getBloom()
    {
        return bloom;
    }

    @JsonProperty
    public int getMinDistinctHashes()
    {
        return minDistinctHashes;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LongBloomFilter that = (LongBloomFilter) o;
        return bloomSizeMask == that.bloomSizeMask && Arrays.equals(bloom, that.bloom);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(Arrays.hashCode(bloom), bloomSizeMask);
    }

    public boolean isEmpty()
    {
        return minDistinctHashes == 0;
    }

    public long getSizeInBytes()
    {
        return (long) bloom.length * Long.BYTES;
    }

    public void insert(long value)
    {
        long hashCode = XxHash64.hash(value);
        long mask = bloomMask(hashCode);
        int index = bloomIndex(hashCode);
        if (mask == (bloom[index] & mask)) {
            return;
        }
        // Set 3 bits in a 64 bit word
        bloom[index] |= mask;
        minDistinctHashes++;
    }

    public boolean contains(long value)
    {
        long hashCode = XxHash64.hash(value);
        long mask = bloomMask(hashCode);
        return mask == (bloom[bloomIndex(hashCode)] & mask);
    }

    public void merge(LongBloomFilter other)
    {
        checkArgument(bloom.length == other.bloom.length, "Bloom filters must have the same size");
        minDistinctHashes += other.getMinDistinctHashes();
        for (int i = 0; i < bloom.length; i++) {
            bloom[i] |= other.bloom[i];
        }
    }

    public void intersect(LongBloomFilter other)
    {
        checkArgument(bloom.length == other.bloom.length, "Bloom filters must have the same size");
        minDistinctHashes = Math.min(minDistinctHashes, other.getMinDistinctHashes());
        for (int i = 0; i < bloom.length; i++) {
            bloom[i] &= other.bloom[i];
        }
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
