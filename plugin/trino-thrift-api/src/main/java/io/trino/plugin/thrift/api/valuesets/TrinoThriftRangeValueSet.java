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
package io.trino.plugin.thrift.api.valuesets;

import io.airlift.drift.annotations.ThriftConstructor;
import io.airlift.drift.annotations.ThriftEnum;
import io.airlift.drift.annotations.ThriftEnumValue;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;
import io.trino.plugin.thrift.api.TrinoThriftBlock;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.drift.annotations.ThriftField.Requiredness.OPTIONAL;
import static io.trino.plugin.thrift.api.TrinoThriftBlock.fromNativeValue;
import static java.util.Objects.requireNonNull;

/**
 * A set containing zero or more Ranges of the same type over a continuous space of possible values.
 * Ranges are coalesced into the most compact representation of non-overlapping Ranges.
 * This structure is used with comparable and orderable types like bigint, integer, double, varchar, etc.
 */
@ThriftStruct
public final class TrinoThriftRangeValueSet
{
    private final List<TrinoThriftRange> ranges;

    @ThriftConstructor
    public TrinoThriftRangeValueSet(@ThriftField(name = "ranges") List<TrinoThriftRange> ranges)
    {
        this.ranges = requireNonNull(ranges, "ranges is null");
    }

    @ThriftField(1)
    public List<TrinoThriftRange> getRanges()
    {
        return ranges;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TrinoThriftRangeValueSet other = (TrinoThriftRangeValueSet) obj;
        return Objects.equals(this.ranges, other.ranges);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(ranges);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("numberOfRanges", ranges.size())
                .toString();
    }

    public static TrinoThriftRangeValueSet fromSortedRangeSet(SortedRangeSet valueSet)
    {
        List<TrinoThriftRange> ranges = valueSet.getOrderedRanges().stream()
                .map(TrinoThriftRange::fromRange)
                .collect(toImmutableList());
        return new TrinoThriftRangeValueSet(ranges);
    }

    @ThriftEnum
    public enum TrinoThriftBound
    {
        BELOW(1),   // lower than the value, but infinitesimally close to the value
        EXACTLY(2), // exactly the value
        ABOVE(3);   // higher than the value, but infinitesimally close to the value

        private final int value;

        TrinoThriftBound(int value)
        {
            this.value = value;
        }

        @ThriftEnumValue
        public int getValue()
        {
            return value;
        }
    }

    /**
     * LOWER UNBOUNDED is specified with an empty value and an ABOVE bound
     * UPPER UNBOUNDED is specified with an empty value and a BELOW bound
     */
    @ThriftStruct
    public static final class TrinoThriftMarker
    {
        private final TrinoThriftBlock value;
        private final TrinoThriftBound bound;

        @ThriftConstructor
        public TrinoThriftMarker(@Nullable TrinoThriftBlock value, TrinoThriftBound bound)
        {
            checkArgument(value == null || value.numberOfRecords() == 1, "value must contain exactly one record when present");
            this.value = value;
            this.bound = requireNonNull(bound, "bound is null");
        }

        @Nullable
        @ThriftField(value = 1, requiredness = OPTIONAL)
        public TrinoThriftBlock getValue()
        {
            return value;
        }

        @ThriftField(2)
        public TrinoThriftBound getBound()
        {
            return bound;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            TrinoThriftMarker other = (TrinoThriftMarker) obj;
            return Objects.equals(this.value, other.value) &&
                    this.bound == other.bound;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(value, bound);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("value", value)
                    .add("bound", bound)
                    .toString();
        }
    }

    @ThriftStruct
    public static final class TrinoThriftRange
    {
        private final TrinoThriftMarker low;
        private final TrinoThriftMarker high;

        @ThriftConstructor
        public TrinoThriftRange(TrinoThriftMarker low, TrinoThriftMarker high)
        {
            this.low = requireNonNull(low, "low is null");
            this.high = requireNonNull(high, "high is null");
        }

        @ThriftField(1)
        public TrinoThriftMarker getLow()
        {
            return low;
        }

        @ThriftField(2)
        public TrinoThriftMarker getHigh()
        {
            return high;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            TrinoThriftRange other = (TrinoThriftRange) obj;
            return Objects.equals(this.low, other.low) &&
                    Objects.equals(this.high, other.high);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(low, high);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("low", low)
                    .add("high", high)
                    .toString();
        }

        public static TrinoThriftRange fromRange(Range range)
        {
            TrinoThriftMarker low = new TrinoThriftMarker(
                    range.getLowValue()
                            .map(trinoNativeValue -> fromNativeValue(trinoNativeValue, range.getType()))
                            .orElse(null),
                    range.isLowInclusive() ? TrinoThriftBound.EXACTLY : TrinoThriftBound.ABOVE);

            TrinoThriftMarker high = new TrinoThriftMarker(
                    range.getHighValue()
                            .map(trinoNativeValue -> fromNativeValue(trinoNativeValue, range.getType()))
                            .orElse(null),
                    range.isHighInclusive() ? TrinoThriftBound.EXACTLY : TrinoThriftBound.BELOW);

            return new TrinoThriftRange(low, high);
        }
    }
}
