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

import io.trino.memory.context.LocalMemoryContext;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperatorDeclaration;
import io.trino.spi.type.TypeOperators;
import jakarta.annotation.Nullable;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FLAT;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FLAT_RETURN;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static java.util.Objects.requireNonNull;

/// The build side of an `IN` subquery. Membership follows SQL semantics, where `v IN S` is
/// `v = ANY S`: true when `v = s` is true for some `s`, unknown when it is unknown for some `s`
/// (a null value, or a row or array whose comparison involves a null element), and false
/// otherwise.
public class ChannelSet
{
    private final FlatSet set;
    private final Optional<NestedNulls> nestedNulls;

    private ChannelSet(FlatSet set, Optional<NestedNulls> nestedNulls)
    {
        this.set = set;
        this.nestedNulls = nestedNulls;
    }

    public long getEstimatedSizeInBytes()
    {
        return set.getEstimatedSize() + nestedNulls.map(NestedNulls::getRetainedSizeInBytes).orElse(0L);
    }

    public int size()
    {
        return set.size();
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    public boolean containsNull()
    {
        return set.containsNull();
    }

    public boolean contains(Block valueBlock, int position)
    {
        return set.contains(valueBlock, position);
    }

    public boolean contains(Block valueBlock, int position, long rawHash)
    {
        return set.contains(valueBlock, position, rawHash);
    }

    /// The result of `value IN set` for the value at the position: true, false, or null for unknown.
    @Nullable
    public Boolean in(Block valueBlock, int position)
    {
        if (valueBlock.isNull(position)) {
            return isEmpty() ? false : null;
        }
        if (nestedNulls.isEmpty()) {
            if (set.contains(valueBlock, position)) {
                return true;
            }
            return set.containsNull() ? null : false;
        }
        NestedNulls nested = nestedNulls.get();
        boolean determinate = nested.isDeterminate(valueBlock, position);
        // For a value without null elements, equality coincides with identity, so the hash lookup
        // finds the equal values. Only a stored value with null elements can compare as unknown.
        if (determinate && set.contains(valueBlock, position)) {
            return true;
        }
        Block candidates = determinate ? nested.indeterminateValues() : nested.allValues();
        if (nested.anyUnknown(valueBlock, position, candidates)) {
            return null;
        }
        return set.containsNull() ? null : false;
    }

    public static class ChannelSetBuilder
    {
        private final LocalMemoryContext memoryContext;
        private final FlatSet set;
        private final Optional<NestedNullsBuilder> nestedNulls;

        public ChannelSetBuilder(Type type, TypeOperators typeOperators, LocalMemoryContext memoryContext)
        {
            set = new FlatSet(
                    type,
                    typeOperators.getReadValueOperator(type, simpleConvention(FLAT_RETURN, BLOCK_POSITION_NOT_NULL)),
                    typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, FLAT)),
                    typeOperators.getIdenticalOperator(type, simpleConvention(FAIL_ON_NULL, FLAT, BLOCK_POSITION_NOT_NULL)),
                    typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL)));
            nestedNulls = equalityMayBeUnknown(type, typeOperators)
                    ? Optional.of(new NestedNullsBuilder(type, typeOperators.getEqualOperator(type, simpleConvention(NULLABLE_RETURN, BLOCK_POSITION_NOT_NULL, BLOCK_POSITION_NOT_NULL))))
                    : Optional.empty();
            this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
            this.memoryContext.setBytes(set.getEstimatedSize());
        }

        public ChannelSet build()
        {
            return new ChannelSet(set, nestedNulls.map(NestedNullsBuilder::build));
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

            memoryContext.setBytes(set.getEstimatedSize() + nestedNulls.map(NestedNullsBuilder::getRetainedSizeInBytes).orElse(0L));
        }

        private void add(Block valueBlock, int position)
        {
            if (nestedNulls.isPresent() && !valueBlock.isNull(position) && !set.contains(valueBlock, position)) {
                nestedNulls.get().add(valueBlock, position);
            }
            set.add(valueBlock, position);
        }

        /// Whether `=` can be unknown for non-null values of the type, as for rows and arrays with null
        /// elements.
        private static boolean equalityMayBeUnknown(Type type, TypeOperators typeOperators)
        {
            TypeOperatorDeclaration declaration = type.getTypeOperatorDeclaration(typeOperators);
            return declaration == null || declaration.getEqualOperators().stream()
                    .anyMatch(method -> method.getCallingConvention().getReturnConvention() == NULLABLE_RETURN);
        }
    }

    /// The distinct non-null build values, kept for types whose equality can be unknown.
    private record NestedNulls(MethodHandle equal, Block allValues, Block indeterminateValues)
    {
        boolean isDeterminate(Block block, int position)
        {
            return isDeterminateValue(equal, block, position);
        }

        boolean anyUnknown(Block block, int position, Block candidates)
        {
            for (int candidate = 0; candidate < candidates.getPositionCount(); candidate++) {
                if (invokeEqual(equal, block, position, candidates, candidate) == null) {
                    return true;
                }
            }
            return false;
        }

        long getRetainedSizeInBytes()
        {
            return allValues.getRetainedSizeInBytes() + indeterminateValues.getRetainedSizeInBytes();
        }
    }

    private static class NestedNullsBuilder
    {
        private final MethodHandle equal;
        private final BlockBuilder allValues;
        private final BlockBuilder indeterminateValues;

        NestedNullsBuilder(Type type, MethodHandle equal)
        {
            this.equal = requireNonNull(equal, "equal is null");
            this.allValues = type.createBlockBuilder(null, 16);
            this.indeterminateValues = type.createBlockBuilder(null, 16);
        }

        void add(Block block, int position)
        {
            allValues.append(block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(position));
            if (!isDeterminateValue(equal, block, position)) {
                indeterminateValues.append(block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(position));
            }
        }

        long getRetainedSizeInBytes()
        {
            return allValues.getRetainedSizeInBytes() + indeterminateValues.getRetainedSizeInBytes();
        }

        NestedNulls build()
        {
            return new NestedNulls(equal, allValues.build(), indeterminateValues.build());
        }
    }

    /// A value is determinate when it has no null elements, i.e. comparing it with itself is not
    /// unknown. (It may still be unequal to itself, as a row containing NaN is; such a value keeps the
    /// identity semantics of the hash set, as NaN does at the top level.)
    private static boolean isDeterminateValue(MethodHandle equal, Block block, int position)
    {
        return invokeEqual(equal, block, position, block, position) != null;
    }

    @Nullable
    private static Boolean invokeEqual(MethodHandle equal, Block left, int leftPosition, Block right, int rightPosition)
    {
        try {
            return (Boolean) equal.invokeExact(left, leftPosition, right, rightPosition);
        }
        catch (Throwable throwable) {
            throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }
}
