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
package io.trino.operator.aggregation.minmaxbyn;

import io.trino.operator.aggregation.minmaxbyn.MinMaxByNStateFactory.GroupedMinMaxByNState;
import io.trino.operator.aggregation.minmaxbyn.MinMaxByNStateFactory.SingleMinMaxByNState;
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

public class MinByNStateFactory
        implements AccumulatorStateFactory<MinByNState>
{
    private static final long MAX_NUMBER_OF_VALUES = 10_000;
    private final LongFunction<TypedKeyValueHeap> heapFactory;
    private final Function<Block, TypedKeyValueHeap> deserializer;

    public MinByNStateFactory(
            @OperatorDependency(
                    operator = OperatorType.COMPARISON_UNORDERED_LAST,
                    argumentTypes = {"K", "K"},
                    convention = @Convention(arguments = {BLOCK_POSITION, BLOCK_POSITION}, result = FAIL_ON_NULL))
                    MethodHandle compare,
            @TypeParameter("K") Type keyType,
            @TypeParameter("V") Type valueType)
    {
        heapFactory = n -> {
            checkCondition(n > 0, INVALID_FUNCTION_ARGUMENT, "third argument of min_by must be a positive integer");
            checkCondition(
                    n <= MAX_NUMBER_OF_VALUES,
                    INVALID_FUNCTION_ARGUMENT,
                    "third argument of min_by must be less than or equal to %s; found %s",
                    MAX_NUMBER_OF_VALUES,
                    n);
            return new TypedKeyValueHeap(true, compare, keyType, valueType, toIntExact(n));
        };
        deserializer = rowBlock -> TypedKeyValueHeap.deserialize(true, compare, keyType, valueType, rowBlock);
    }

    @Override
    public MinByNState createSingleState()
    {
        return new SingleMinByNState(heapFactory, deserializer);
    }

    @Override
    public MinByNState createGroupedState()
    {
        return new GroupedMinByNState(heapFactory, deserializer);
    }

    private static class GroupedMinByNState
            extends GroupedMinMaxByNState
            implements MinByNState
    {
        public GroupedMinByNState(LongFunction<TypedKeyValueHeap> heapFactory, Function<Block, TypedKeyValueHeap> deserializer)
        {
            super(heapFactory, deserializer);
        }
    }

    private static class SingleMinByNState
            extends SingleMinMaxByNState
            implements MinByNState
    {
        public SingleMinByNState(LongFunction<TypedKeyValueHeap> heapFactory, Function<Block, TypedKeyValueHeap> deserializer)
        {
            super(heapFactory, deserializer);
        }

        public SingleMinByNState(SingleMinByNState state)
        {
            super(state);
        }

        @Override
        public AccumulatorState copy()
        {
            return new SingleMinByNState(this);
        }
    }
}
