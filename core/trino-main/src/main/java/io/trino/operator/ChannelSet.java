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
package io.trino.operator;

import com.google.common.base.Throwables;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import jakarta.annotation.Nullable;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FLAT;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FLAT_RETURN;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static java.util.Objects.requireNonNull;

public class ChannelSet
{
    private final FlatSet set;
    @Nullable
    private final Block values;
    @Nullable
    private final MethodHandle equalOperator;
    @Nullable
    private final MethodHandle indeterminateOperator;
    private final boolean hasIndeterminate;

    private ChannelSet(
            FlatSet set,
            @Nullable Block values,
            @Nullable MethodHandle equalOperator,
            @Nullable MethodHandle indeterminateOperator,
            boolean hasIndeterminate)
    {
        this.set = set;
        this.values = values;
        this.equalOperator = equalOperator;
        this.indeterminateOperator = indeterminateOperator;
        this.hasIndeterminate = hasIndeterminate;
    }

    public long getEstimatedSizeInBytes()
    {
        return set.getEstimatedSize() + (values == null ? 0 : values.getRetainedSizeInBytes());
    }

    public int size()
    {
        return set.size();
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    /**
     * SQL {@code IN} membership for a non-null probe value.
     * Returns {@code true}, {@code false}, or {@code null} (unknown).
     */
    public Boolean containsSql(Block valueBlock, int position)
    {
        boolean identicalMatch = set.contains(valueBlock, position);
        if (equalOperator == null) {
            if (identicalMatch) {
                return true;
            }
            return set.containsNull() ? null : false;
        }

        // Hash lookup uses IS NOT DISTINCT FROM. That is the wrong operator for IN:
        // ROW(1, 2) IS NOT DISTINCT FROM ROW(1, null) is false, while SQL equality is unknown.
        // A determinate identical match is also an equality match.
        boolean probeIndeterminate = isIndeterminate(valueBlock, position);
        if (identicalMatch) {
            return probeIndeterminate ? null : true;
        }

        // Determinate probe vs a determinate-only set: IDENTICAL miss is also an EQUAL miss.
        // Skip the scan so extra cost stays on sets that actually contain null fields.
        if (!probeIndeterminate && !hasIndeterminate) {
            return set.containsNull() ? null : false;
        }

        boolean unknown = set.containsNull();
        Block members = requireNonNull(values, "values is null");
        for (int i = 0; i < members.getPositionCount(); i++) {
            Boolean equal = equal(valueBlock, position, members, i);
            if (Boolean.TRUE.equals(equal)) {
                return true;
            }
            if (equal == null) {
                unknown = true;
            }
        }
        return unknown ? null : false;
    }

    private boolean isIndeterminate(Block block, int position)
    {
        try {
            return (boolean) indeterminateOperator.invokeExact(block, position);
        }
        catch (Throwable throwable) {
            Throwables.throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    private Boolean equal(Block left, int leftPosition, Block right, int rightPosition)
    {
        try {
            return (Boolean) equalOperator.invokeExact(left, leftPosition, right, rightPosition);
        }
        catch (Throwable throwable) {
            Throwables.throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    public static class ChannelSetBuilder
    {
        private final LocalMemoryContext memoryContext;
        private final FlatSet set;
        @Nullable
        private final BlockBuilder valuesBuilder;
        @Nullable
        private final MethodHandle equalOperator;
        @Nullable
        private final MethodHandle indeterminateOperator;
        private boolean hasIndeterminate;

        public ChannelSetBuilder(Type type, TypeOperators typeOperators, LocalMemoryContext memoryContext)
        {
            requireNonNull(type, "type is null");
            set = new FlatSet(
                    type,
                    typeOperators.getReadValueOperator(type, simpleConvention(FLAT_RETURN, BLOCK_POSITION_NOT_NULL)),
                    typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, FLAT)),
                    typeOperators.getIdenticalOperator(type, simpleConvention(FAIL_ON_NULL, FLAT, BLOCK_POSITION_NOT_NULL)),
                    typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL)));
            this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
            this.memoryContext.setBytes(set.getEstimatedSize());

            if (usesSqlThreeValuedIn(type)) {
                // Retained even when every member is determinate: an indeterminate probe
                // still has to EQUAL-scan those members (ROW(1, null) IN (ROW(1, 2))).
                valuesBuilder = type.createBlockBuilder(null, 16);
                equalOperator = typeOperators.getEqualOperator(type, simpleConvention(NULLABLE_RETURN, BLOCK_POSITION_NOT_NULL, BLOCK_POSITION_NOT_NULL));
                indeterminateOperator = typeOperators.getIndeterminateOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL));
            }
            else {
                valuesBuilder = null;
                equalOperator = null;
                indeterminateOperator = null;
            }
        }

        public ChannelSet build()
        {
            Block values = valuesBuilder == null ? null : valuesBuilder.build();
            return new ChannelSet(set, values, equalOperator, indeterminateOperator, hasIndeterminate);
        }

        public void addAll(Block valueBlock)
        {
            if (valueBlock.getPositionCount() == 0) {
                return;
            }

            if (valueBlock instanceof RunLengthEncodedBlock rleBlock) {
                add(rleBlock.getValue(), 0);
            }
            else {
                for (int position = 0; position < valueBlock.getPositionCount(); position++) {
                    add(valueBlock, position);
                }
            }

            memoryContext.setBytes(retainedSize());
        }

        private void add(Block valueBlock, int position)
        {
            if (set.add(valueBlock, position) && valuesBuilder != null && !valueBlock.isNull(position)) {
                valuesBuilder.append(valueBlock.getUnderlyingValueBlock(), valueBlock.getUnderlyingValuePosition(position));
                if (isIndeterminate(valueBlock, position)) {
                    hasIndeterminate = true;
                }
            }
        }

        private boolean isIndeterminate(Block block, int position)
        {
            try {
                return (boolean) indeterminateOperator.invokeExact(block, position);
            }
            catch (Throwable throwable) {
                Throwables.throwIfUnchecked(throwable);
                throw new RuntimeException(throwable);
            }
        }

        private long retainedSize()
        {
            return set.getEstimatedSize() + (valuesBuilder == null ? 0 : valuesBuilder.getRetainedSizeInBytes());
        }

        private static boolean usesSqlThreeValuedIn(Type type)
        {
            // Non-null primitive values are determinate. Structural values can contain null fields,
            // so hash-lookup with IS NOT DISTINCT FROM is not equivalent to SQL IN.
            return type instanceof RowType || type instanceof ArrayType || type instanceof MapType;
        }
    }
}
