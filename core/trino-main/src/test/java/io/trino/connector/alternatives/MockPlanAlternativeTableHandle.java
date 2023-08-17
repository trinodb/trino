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
package io.trino.connector.alternatives;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeUtils;

import java.util.List;
import java.util.function.BiPredicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TypeUtils.isFloatingPointNaN;
import static java.util.Objects.requireNonNull;

public record MockPlanAlternativeTableHandle(ConnectorTableHandle delegate, ColumnHandle filterColumn, FilterDefinition filterDefinition)
        implements ConnectorTableHandle
{
    @JsonCreator
    public MockPlanAlternativeTableHandle(
            @JsonProperty ConnectorTableHandle delegate,
            @JsonProperty ColumnHandle filterColumn,
            @JsonProperty FilterDefinition filterDefinition)
    {
        this.delegate = requireNonNull(delegate, "delegate is null") instanceof MockPlanAlternativeTableHandle handle ? handle.delegate() : delegate;
        this.filterColumn = requireNonNull(filterColumn, "filterColumn is null");
        this.filterDefinition = requireNonNull(filterDefinition, "filterDefinition is null");
    }

    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            property = "@type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = IsNull.class, name = "isNull"),
            @JsonSubTypes.Type(value = VarcharIn.class, name = "varcharIn"),
            @JsonSubTypes.Type(value = BigintIn.class, name = "bigintIn"),
            @JsonSubTypes.Type(value = IntegerIn.class, name = "integerIn"),
            @JsonSubTypes.Type(value = Ranges.class, name = "ranges")})
    public sealed interface FilterDefinition
            permits IsNull, VarcharIn, BigintIn, IntegerIn, Ranges
    {
        BiPredicate<Block, Integer> asPredicate(ConnectorSession session);
    }

    public record IsNull()
            implements FilterDefinition
    {
        @Override
        public BiPredicate<Block, Integer> asPredicate(ConnectorSession session)
        {
            return Block::isNull;
        }
    }

    public record VarcharIn(Block values, List<String> discreteSet)
            implements FilterDefinition
    {
        public VarcharIn(Type type, List<Object> discreteSet)
        {
            this(toBlock(type, discreteSet), discreteSet.stream().map(value -> ((Slice) value).toStringUtf8()).collect(toImmutableList()));
        }

        @Override
        public BiPredicate<Block, Integer> asPredicate(ConnectorSession session)
        {
            return (block, position) -> {
                for (int i = 0; i < values.getPositionCount(); i++) {
                    int leftLength = block.getSliceLength(position);
                    int rightLength = values.getSliceLength(i);
                    if (leftLength == rightLength && block.getSlice(position, 0, leftLength).equals(values.getSlice(i, 0, rightLength))) {
                        return true;
                    }
                }
                return false;
            };
        }

        @Override
        public String toString()
        {
            return "VarcharIn(" + discreteSet + ")";
        }
    }

    public record BigintIn(long[] values)
            implements FilterDefinition
    {
        public BigintIn(List<Object> discreteSet)
        {
            this(discreteSet.stream().mapToLong(value -> (Long) value).toArray());
        }

        @Override
        public BiPredicate<Block, Integer> asPredicate(ConnectorSession session)
        {
            return (block, position) -> {
                long value = block.getLong(position, 0);
                for (int i = 0; i < values.length; i++) {
                    if (value == values[i]) {
                        return true;
                    }
                }
                return false;
            };
        }
    }

    public record IntegerIn(int[] values)
            implements FilterDefinition
    {
        public IntegerIn(List<Object> discreteSet)
        {
            this(discreteSet.stream().mapToInt(value -> ((Long) value).intValue()).toArray());
        }

        @Override
        public BiPredicate<Block, Integer> asPredicate(ConnectorSession session)
        {
            return (block, position) -> {
                int value = INTEGER.getInt(block, position);
                for (int i = 0; i < values.length; i++) {
                    if (value == values[i]) {
                        return true;
                    }
                }
                return false;
            };
        }
    }

    public record Ranges(SortedRangeSet ranges, boolean isNullAllowed)
            implements FilterDefinition
    {
        public Ranges(SortedRangeSet ranges, boolean isNullAllowed)
        {
            this.ranges = requireNonNull(ranges, "ranges is null");
            this.isNullAllowed = isNullAllowed;
        }

        @Override
        public BiPredicate<Block, Integer> asPredicate(ConnectorSession session)
        {
            return (block, position) -> {
                Object value = TypeUtils.readNativeValue(ranges.getType(), block, position);
                if (value == null) {
                    return isNullAllowed;
                }
                if (isFloatingPointNaN(ranges.getType(), value)) {
                    return false;
                }
                Range valueRange = Range.equal(ranges.getType(), value);
                for (Range range : ranges.getOrderedRanges()) {
                    if (range.contains(valueRange)) {
                        return true;
                    }
                }
                return false;
            };
        }
    }

    private static Block toBlock(Type type, List<Object> discreteSet)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, discreteSet.size());
        for (Object value : discreteSet) {
            TypeUtils.writeNativeValue(type, blockBuilder, value);
        }
        return blockBuilder.build();
    }
}
