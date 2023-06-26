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

public class MinNStateFactory
        implements AccumulatorStateFactory<MinNState>
{
    private static final long MAX_NUMBER_OF_VALUES = 10_000;
    private final LongFunction<TypedHeap> heapFactory;

    public MinNStateFactory(
            @OperatorDependency(
                    operator = OperatorType.READ_VALUE,
                    argumentTypes = "T",
                    convention = @Convention(arguments = FLAT, result = BLOCK_BUILDER))
                    MethodHandle readFlat,
            @OperatorDependency(
                    operator = OperatorType.READ_VALUE,
                    argumentTypes = "T",
                    convention = @Convention(arguments = BLOCK_POSITION_NOT_NULL, result = FLAT_RETURN))
                    MethodHandle writeFlat,
            @OperatorDependency(
                    operator = OperatorType.COMPARISON_UNORDERED_LAST,
                    argumentTypes = {"T", "T"},
                    convention = @Convention(arguments = {FLAT, FLAT}, result = FAIL_ON_NULL))
                    MethodHandle compareFlatFlat,
            @OperatorDependency(
                    operator = OperatorType.COMPARISON_UNORDERED_LAST,
                    argumentTypes = {"T", "T"},
                    convention = @Convention(arguments = {FLAT, BLOCK_POSITION_NOT_NULL}, result = FAIL_ON_NULL))
                    MethodHandle compareFlatBlock,
            @TypeParameter("T") Type elementType)
    {
        heapFactory = n -> {
            checkCondition(n > 0, INVALID_FUNCTION_ARGUMENT, "second argument of min_n must be positive");
            checkCondition(
                    n <= MAX_NUMBER_OF_VALUES,
                    INVALID_FUNCTION_ARGUMENT,
                    "second argument of min_n must be less than or equal to %s; found %s",
                    MAX_NUMBER_OF_VALUES,
                    n);

            return new TypedHeap(true, readFlat, writeFlat, compareFlatFlat, compareFlatBlock, elementType, toIntExact(n));
        };
    }

    @Override
    public MinNState createSingleState()
    {
        return new SingleMinNState(heapFactory);
    }

    @Override
    public MinNState createGroupedState()
    {
        return new GroupedMinNState(heapFactory);
    }

    private static class GroupedMinNState
            extends MinMaxNStateFactory.GroupedMinMaxNState
            implements MinNState
    {
        public GroupedMinNState(LongFunction<TypedHeap> heapFactory)
        {
            super(heapFactory);
        }
    }

    private static class SingleMinNState
            extends SingleMinMaxNState
            implements MinNState
    {
        public SingleMinNState(LongFunction<TypedHeap> heapFactory)
        {
            super(heapFactory);
        }

        public SingleMinNState(SingleMinNState state)
        {
            super(state);
        }

        @Override
        public AccumulatorState copy()
        {
            return new SingleMinNState(this);
        }
    }
}
