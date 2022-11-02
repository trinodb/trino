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
package io.trino.operator.aggregation.minmaxn;

import io.trino.operator.aggregation.minmaxn.MinMaxNStateFactory.SingleMinMaxNState;
import io.trino.spi.block.Block;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.Convention;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;
import java.util.function.Function;
import java.util.function.LongFunction;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.util.Failures.checkCondition;
import static java.lang.Math.toIntExact;

public class MaxNStateFactory
        implements AccumulatorStateFactory<MaxNState>
{
    private static final long MAX_NUMBER_OF_VALUES = 10_000;
    private final LongFunction<TypedHeap> heapFactory;
    private final Function<Block, TypedHeap> deserializer;

    public MaxNStateFactory(
            @OperatorDependency(
                    operator = OperatorType.COMPARISON_UNORDERED_FIRST,
                    argumentTypes = {"T", "T"},
                    convention = @Convention(arguments = {BLOCK_POSITION, BLOCK_POSITION}, result = FAIL_ON_NULL))
                    MethodHandle compare,
            @TypeParameter("T") Type elementType)
    {
        heapFactory = n -> {
            checkCondition(n > 0, INVALID_FUNCTION_ARGUMENT, "second argument of max_n must be positive");
            checkCondition(
                    n <= MAX_NUMBER_OF_VALUES,
                    INVALID_FUNCTION_ARGUMENT,
                    "second argument of max_n must be less than or equal to %s; found %s",
                    MAX_NUMBER_OF_VALUES,
                    n);
            return new TypedHeap(false, compare, elementType, toIntExact(n));
        };
        deserializer = rowBlock -> TypedHeap.deserialize(false, compare, elementType, rowBlock);
    }

    @Override
    public MaxNState createSingleState()
    {
        return new SingleMaxNState(heapFactory, deserializer);
    }

    @Override
    public MaxNState createGroupedState()
    {
        return new GroupedMaxNState(heapFactory, deserializer);
    }

    private static class GroupedMaxNState
            extends MinMaxNStateFactory.GroupedMinMaxNState
            implements MaxNState
    {
        public GroupedMaxNState(LongFunction<TypedHeap> heapFactory, Function<Block, TypedHeap> deserializer)
        {
            super(heapFactory, deserializer);
        }
    }

    private static class SingleMaxNState
            extends SingleMinMaxNState
            implements MaxNState
    {
        public SingleMaxNState(LongFunction<TypedHeap> heapFactory, Function<Block, TypedHeap> deserializer)
        {
            super(heapFactory, deserializer);
        }

        public SingleMaxNState(SingleMaxNState state)
        {
            super(state);
        }

        @Override
        public AccumulatorState copy()
        {
            return new SingleMaxNState(this);
        }
    }
}
