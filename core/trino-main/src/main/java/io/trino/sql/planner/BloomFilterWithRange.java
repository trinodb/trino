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
import io.airlift.units.DataSize;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.spi.block.Block;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class BloomFilterWithRange
{
    private final LongBloomFilter bloomFilter;
    private final ValueSet range;
    private final Type type;
    private final boolean nullAllowed;

    public BloomFilterWithRange(LongBloomFilter bloomFilter, Range range, Type type, boolean nullAllowed)
    {
        this.bloomFilter = requireNonNull(bloomFilter, "bloomFilter is null");
        this.range = ValueSet.ofRanges(range);
        this.type = requireNonNull(type, "type is null");
        this.nullAllowed = nullAllowed;
        checkArgument(type.isOrderable(), "Type must be orderable");
        checkArgument(type.getJavaType() == long.class, "Type must be of long class");
    }

    public boolean isNone()
    {
        return bloomFilter.isEmpty() && !nullAllowed;
    }

    @UsedByGeneratedCode
    public boolean test(Block block, int position)
    {
        if (block.isNull(position)) {
            return nullAllowed;
        }
        long value = type.getLong(block, position);
        return bloomFilter.contains(value);
    }

    public long getRetainedSizeInBytes()
    {
        return bloomFilter.getSizeInBytes() + range.getRetainedSizeInBytes();
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BloomFilterWithRange that = (BloomFilterWithRange) o;
        return nullAllowed == that.nullAllowed
                && Objects.equals(type, that.type)
                && Objects.equals(range, that.range)
                && Objects.equals(bloomFilter, that.bloomFilter);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bloomFilter, range, type, nullAllowed);
    }

    @Override
    public String toString()
    {
        return format(
                "[%s Bloom filter approx cardinality: %s, range %s, size: %s ]",
                nullAllowed ? " NULL," : "",
                bloomFilter.getMinDistinctHashes(),
                getSpan(),
                DataSize.succinctBytes(bloomFilter.getSizeInBytes()));
    }

    @JsonCreator
    @DoNotCall // For JSON deserialization only
    public static BloomFilterWithRange fromJson(
            @JsonProperty("bloomFilter") LongBloomFilter bloomFilter,
            @JsonProperty("range") ValueSet range,
            @JsonProperty("type") Type type,
            @JsonProperty("nullAllowed") boolean nullAllowed)
    {
        return new BloomFilterWithRange(bloomFilter, range.getRanges().getSpan(), type, nullAllowed);
    }

    @JsonProperty
    public LongBloomFilter bloomFilter()
    {
        return bloomFilter;
    }

    @JsonProperty
    public ValueSet range()
    {
        return range;
    }

    public Range getSpan()
    {
        return range.getRanges().getSpan();
    }

    @JsonProperty
    public Type type()
    {
        return type;
    }

    @JsonProperty
    public boolean nullAllowed()
    {
        return nullAllowed;
    }

    public static BloomFilterWithRange union(List<BloomFilterWithRange> filters)
    {
        if (filters.isEmpty()) {
            throw new IllegalArgumentException("bitmaps cannot be empty for union");
        }
        if (filters.size() == 1) {
            return filters.getFirst();
        }
        LongBloomFilter result = filters.getFirst().bloomFilter();
        ValueSet valueSet = filters.getFirst().range();
        for (int index = 1; index < filters.size(); index++) {
            BloomFilterWithRange filter = filters.get(index);
            result.merge(filter.bloomFilter());
            valueSet = valueSet.union(filter.range());
        }
        return new BloomFilterWithRange(
                result,
                valueSet.getRanges().getSpan(),
                filters.getFirst().type(),
                filters.stream().anyMatch(BloomFilterWithRange::nullAllowed));
    }

    public static Optional<BloomFilterWithRange> fromDomain(Domain domain)
    {
        if (domain.getType().getJavaType() != long.class || !domain.isNullableDiscreteSet()) {
            return Optional.empty();
        }
        List<Object> values = domain.getNullableDiscreteSet().getNonNullValues();
        LongBloomFilter bloomFilter = new LongBloomFilter();
        for (Object value : values) {
            bloomFilter.insert((long) value);
        }
        return Optional.of(new BloomFilterWithRange(bloomFilter, domain.getValues().getRanges().getSpan(), domain.getType(), domain.isNullAllowed()));
    }

    public static class LongBloomFilter
    {
        private static final int EXPECTED_NDV = 1_000_000;

        private final long[] bloom;
        private final int bloomSizeMask;
        private int minDistinctHashes;

        /**
         * A Bloom filter for a set of long values.
         * This is approx 2X faster than the Bloom filter implementations in ORC and parquet because
         * it uses single hash function and uses that to set 3 bits within a 64 bit word.
         * The memory footprint is up to (4 * values.size()) bytes, which is much smaller than maintaining a hash set of longs.
         */
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
        public long[] getBloom()
        {
            return bloom;
        }

        @JsonProperty
        public int getMinDistinctHashes()
        {
            return minDistinctHashes;
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
}
