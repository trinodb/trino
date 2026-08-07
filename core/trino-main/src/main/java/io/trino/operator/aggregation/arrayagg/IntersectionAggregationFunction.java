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
package io.trino.operator.aggregation.arrayagg;

import io.trino.operator.scalar.MultisetMultiplicities;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlMultiset;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Convention;
import io.trino.spi.function.Description;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;
import io.trino.type.BlockTypeOperators.BlockPositionIsIdentical;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.function.OperatorType.IDENTICAL;

/// Combines the input multisets into a single multiset whose multiplicity for each value is the
/// minimum of its multiplicities across the inputs. A running multiset is kept in the accumulator
/// and each input is folded in with the minimum-multiplicity intersection used by
/// `MULTISET INTERSECT ALL`.
@AggregationFunction("intersection")
@Description("Combines the input multisets, taking the minimum element multiplicities")
public final class IntersectionAggregationFunction
{
    private IntersectionAggregationFunction() {}

    @InputFunction
    @TypeParameter("E")
    public static void input(
            @OperatorDependency(
                    operator = IDENTICAL,
                    argumentTypes = {"E", "E"},
                    convention = @Convention(arguments = {BLOCK_POSITION, BLOCK_POSITION}, result = FAIL_ON_NULL))
            BlockPositionIsIdentical elementIdentical,
            @OperatorDependency(
                    operator = HASH_CODE,
                    argumentTypes = "E",
                    convention = @Convention(arguments = BLOCK_POSITION, result = FAIL_ON_NULL))
            BlockPositionHashCode elementHashCode,
            @TypeParameter("E") Type elementType,
            @AggregationState("E") MultisetAggregationState state,
            @SqlType("multiset(E)") SqlMultiset multiset)
    {
        // the accumulator stores the elements as a plain block
        Block elements = multiset.getElementBlock();
        Block running = state.get();
        if (running == null) {
            state.set(copy(elements, elementType));
        }
        else {
            state.set(intersect(running, elements, elementIdentical, elementHashCode, elementType));
        }
    }

    @CombineFunction
    public static void combine(
            @OperatorDependency(
                    operator = IDENTICAL,
                    argumentTypes = {"E", "E"},
                    convention = @Convention(arguments = {BLOCK_POSITION, BLOCK_POSITION}, result = FAIL_ON_NULL))
            BlockPositionIsIdentical elementIdentical,
            @OperatorDependency(
                    operator = HASH_CODE,
                    argumentTypes = "E",
                    convention = @Convention(arguments = BLOCK_POSITION, result = FAIL_ON_NULL))
            BlockPositionHashCode elementHashCode,
            @TypeParameter("E") Type elementType,
            @AggregationState("E") MultisetAggregationState state,
            @AggregationState("E") MultisetAggregationState otherState)
    {
        Block other = otherState.get();
        if (other == null) {
            return;
        }
        Block running = state.get();
        if (running == null) {
            state.set(copy(other, elementType));
        }
        else {
            state.set(intersect(running, other, elementIdentical, elementHashCode, elementType));
        }
    }

    @OutputFunction("multiset(E)")
    public static void output(
            @AggregationState("E") MultisetAggregationState state,
            BlockBuilder out)
    {
        Block multiset = state.get();
        if (multiset == null) {
            out.appendNull();
        }
        else {
            ((ArrayBlockBuilder) out).buildEntry(elementBuilder -> appendElements(elementBuilder, multiset));
        }
    }

    private static Block copy(Block elements, Type elementType)
    {
        BlockBuilder blockBuilder = elementType.createBlockBuilder(null, elements.getPositionCount());
        appendElements(blockBuilder, elements);
        return blockBuilder.build();
    }

    private static Block intersect(Block left, Block right, BlockPositionIsIdentical elementIdentical, BlockPositionHashCode elementHashCode, Type elementType)
    {
        // emit each left element that can be paired with an available identical right occurrence, which
        // keeps the minimum multiplicity per value. A multiplicity map of the right operand makes the
        // pairing a single hashed lookup, so this is O(n + m) rather than the O(n * m) of a pairwise scan.
        MultisetMultiplicities rightCounts = MultisetMultiplicities.of(elementIdentical, elementHashCode, right);
        BlockBuilder blockBuilder = elementType.createBlockBuilder(null, Math.min(left.getPositionCount(), right.getPositionCount()));
        for (int i = 0; i < left.getPositionCount(); i++) {
            if (rightCounts.consume(left, i)) {
                blockBuilder.append(left.getUnderlyingValueBlock(), left.getUnderlyingValuePosition(i));
            }
        }
        return blockBuilder.build();
    }

    private static void appendElements(BlockBuilder elementBuilder, Block elements)
    {
        ValueBlock values = elements.getUnderlyingValueBlock();
        for (int i = 0; i < elements.getPositionCount(); i++) {
            elementBuilder.append(values, elements.getUnderlyingValuePosition(i));
        }
    }
}
