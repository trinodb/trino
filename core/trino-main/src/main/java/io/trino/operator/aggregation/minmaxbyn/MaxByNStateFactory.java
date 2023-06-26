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
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.Convention;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;
import java.util.function.LongFunction;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FLAT;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.BLOCK_BUILDER;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FLAT_RETURN;
import static io.trino.util.Failures.checkCondition;
import static java.lang.Math.toIntExact;

public class MaxByNStateFactory
        implements AccumulatorStateFactory<MaxByNState>
{
    private static final long MAX_NUMBER_OF_VALUES = 10_000;
    private final LongFunction<TypedKeyValueHeap> heapFactory;

    public MaxByNStateFactory(
            @OperatorDependency(
                    operator = OperatorType.READ_VALUE,
                    argumentTypes = "K",
                    convention = @Convention(arguments = FLAT, result = BLOCK_BUILDER))
                    MethodHandle keyReadFlat,
            @OperatorDependency(
                    operator = OperatorType.READ_VALUE,
                    argumentTypes = "K",
                    convention = @Convention(arguments = BLOCK_POSITION_NOT_NULL, result = FLAT_RETURN))
                    MethodHandle keyWriteFlat,
            @OperatorDependency(
                    operator = OperatorType.READ_VALUE,
                    argumentTypes = "V",
                    convention = @Convention(arguments = FLAT, result = BLOCK_BUILDER))
                    MethodHandle valueReadFlat,
            @OperatorDependency(
                    operator = OperatorType.READ_VALUE,
                    argumentTypes = "V",
                    convention = @Convention(arguments = BLOCK_POSITION_NOT_NULL, result = FLAT_RETURN))
                    MethodHandle valueWriteFlat,
            @OperatorDependency(
                    operator = OperatorType.COMPARISON_UNORDERED_FIRST,
                    argumentTypes = {"K", "K"},
                    convention = @Convention(arguments = {FLAT, FLAT}, result = FAIL_ON_NULL))
                    MethodHandle compareFlatFlat,
            @OperatorDependency(
                    operator = OperatorType.COMPARISON_UNORDERED_FIRST,
                    argumentTypes = {"K", "K"},
                    convention = @Convention(arguments = {FLAT, BLOCK_POSITION_NOT_NULL}, result = FAIL_ON_NULL))
                    MethodHandle compareFlatBlock,
            @TypeParameter("K") Type keyType,
            @TypeParameter("V") Type valueType)
    {
        heapFactory = n -> {
            checkCondition(n > 0, INVALID_FUNCTION_ARGUMENT, "third argument of max_by must be a positive integer");
            checkCondition(
                    n <= MAX_NUMBER_OF_VALUES,
                    INVALID_FUNCTION_ARGUMENT,
                    "third argument of max_by must be less than or equal to %s; found %s",
                    MAX_NUMBER_OF_VALUES,
                    n);
            return new TypedKeyValueHeap(
                    false,
                    keyReadFlat,
                    keyWriteFlat,
                    valueReadFlat,
                    valueWriteFlat,
                    compareFlatFlat,
                    compareFlatBlock,
                    keyType,
                    valueType,
                    toIntExact(n));
        };
    }

    @Override
    public MaxByNState createSingleState()
    {
        return new SingleMaxByNState(heapFactory);
    }

    @Override
    public MaxByNState createGroupedState()
    {
        return new GroupedMaxByNState(heapFactory);
    }

    private static class GroupedMaxByNState
            extends GroupedMinMaxByNState
            implements MaxByNState
    {
        public GroupedMaxByNState(LongFunction<TypedKeyValueHeap> heapFactory)
        {
            super(heapFactory);
        }
    }

    private static class SingleMaxByNState
            extends SingleMinMaxByNState
            implements MaxByNState
    {
        public SingleMaxByNState(LongFunction<TypedKeyValueHeap> heapFactory)
        {
            super(heapFactory);
        }

        public SingleMaxByNState(SingleMaxByNState state)
        {
            super(state);
        }

        @Override
        public AccumulatorState copy()
        {
            return new SingleMaxByNState(this);
        }
    }
}
