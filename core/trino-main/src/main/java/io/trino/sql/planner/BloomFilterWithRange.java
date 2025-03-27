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
import io.airlift.units.DataSize;
import io.trino.spi.block.Block;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public record BloomFilterWithRange(LongBloomFilter bloomFilter, ValueSet ranges, Type type, boolean nullAllowed)
{
    public BloomFilterWithRange(LongBloomFilter bloomFilter, ValueSet ranges, Type type, boolean nullAllowed)
    {
        this.bloomFilter = requireNonNull(bloomFilter, "bloomFilter is null");
        this.ranges = requireNonNull(ranges, "ranges is null");
        this.type = requireNonNull(type, "type is null");
        this.nullAllowed = nullAllowed;
        checkArgument(type.isOrderable(), "Type %s must be orderable", type);
        checkArgument(type.getJavaType() == long.class, "Type %s must be of long class", type);
    }

    public boolean isNone()
    {
        return bloomFilter.isEmpty() && !nullAllowed;
    }

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
        return bloomFilter.getSizeInBytes() + ranges.getRetainedSizeInBytes();
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
                && Objects.equals(ranges, that.ranges)
                && Objects.equals(bloomFilter, that.bloomFilter);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bloomFilter, ranges, type, nullAllowed);
    }

    @Override
    public String toString()
    {
        return format(
                "[%s Bloom filter approx cardinality: %s, %s, size: %s ]",
                nullAllowed ? " NULL," : "",
                bloomFilter.getMinDistinctHashes(),
                ranges.toString(),
                DataSize.succinctBytes(bloomFilter.getSizeInBytes()));
    }

    @JsonCreator
    @DoNotCall // For JSON deserialization only
    public static BloomFilterWithRange fromJson(
            @JsonProperty("bloomFilter") LongBloomFilter bloomFilter,
            @JsonProperty("ranges") ValueSet ranges,
            @JsonProperty("type") Type type,
            @JsonProperty("nullAllowed") boolean nullAllowed)
    {
        return new BloomFilterWithRange(bloomFilter, ranges, type, nullAllowed);
    }

    @Override
    @JsonProperty
    public LongBloomFilter bloomFilter()
    {
        return bloomFilter;
    }

    @Override
    @JsonProperty
    public ValueSet ranges()
    {
        return ranges;
    }

    @Override
    @JsonProperty
    public Type type()
    {
        return type;
    }

    @Override
    @JsonProperty
    public boolean nullAllowed()
    {
        return nullAllowed;
    }

    public static BloomFilterWithRange union(List<BloomFilterWithRange> filters)
    {
        if (filters.isEmpty()) {
            throw new IllegalArgumentException("bloom filters cannot be empty for union");
        }
        if (filters.size() == 1) {
            return filters.getFirst();
        }
        LongBloomFilter result = filters.getFirst().bloomFilter();
        ValueSet ranges = filters.getFirst().ranges();
        for (int index = 1; index < filters.size(); index++) {
            BloomFilterWithRange filter = filters.get(index);
            result.merge(filter.bloomFilter());
            ranges = ranges.union(filter.ranges());
        }
        return new BloomFilterWithRange(
                result,
                ranges,
                filters.getFirst().type(),
                filters.stream().anyMatch(BloomFilterWithRange::nullAllowed));
    }
}
