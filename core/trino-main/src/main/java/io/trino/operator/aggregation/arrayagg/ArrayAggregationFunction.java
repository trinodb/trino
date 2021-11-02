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

import com.google.common.collect.ImmutableList;
import io.trino.metadata.AggregationFunctionMetadata;
import io.trino.metadata.BoundSignature;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.FunctionNullability;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlAggregationFunction;
import io.trino.operator.aggregation.AggregationMetadata;
import io.trino.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.trino.metadata.FunctionKind.AGGREGATE;
import static io.trino.metadata.Signature.typeVariable;
import static io.trino.spi.type.TypeSignature.arrayType;
import static io.trino.util.Reflection.methodHandle;

public class ArrayAggregationFunction
        extends SqlAggregationFunction
{
    public static final ArrayAggregationFunction ARRAY_AGG = new ArrayAggregationFunction();
    private static final String NAME = "array_agg";
    private static final MethodHandle INPUT_FUNCTION = methodHandle(ArrayAggregationFunction.class, "input", Type.class, ArrayAggregationState.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(ArrayAggregationFunction.class, "combine", Type.class, ArrayAggregationState.class, ArrayAggregationState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(ArrayAggregationFunction.class, "output", Type.class, ArrayAggregationState.class, BlockBuilder.class);

    private ArrayAggregationFunction()
    {
        super(
                new FunctionMetadata(
                        new Signature(
                                NAME,
                                ImmutableList.of(typeVariable("T")),
                                ImmutableList.of(),
                                arrayType(new TypeSignature("T")),
                                ImmutableList.of(new TypeSignature("T")),
                                false),
                        new FunctionNullability(true, ImmutableList.of(true)),
                        false,
                        true,
                        "return an array of values",
                        AGGREGATE),
                new AggregationFunctionMetadata(
                        true,
                        arrayType(new TypeSignature("T"))));
    }

    @Override
    public AggregationMetadata specialize(BoundSignature boundSignature)
    {
        Type type = boundSignature.getArgumentTypes().get(0);
        ArrayAggregationStateSerializer stateSerializer = new ArrayAggregationStateSerializer(type);
        ArrayAggregationStateFactory stateFactory = new ArrayAggregationStateFactory(type);

        MethodHandle inputFunction = INPUT_FUNCTION.bindTo(type);
        MethodHandle combineFunction = COMBINE_FUNCTION.bindTo(type);
        MethodHandle outputFunction = OUTPUT_FUNCTION.bindTo(type);

        return new AggregationMetadata(
                inputFunction,
                Optional.empty(),
                Optional.of(combineFunction),
                outputFunction,
                ImmutableList.of(new AccumulatorStateDescriptor<>(
                        ArrayAggregationState.class,
                        stateSerializer,
                        stateFactory)));
    }

    public static void input(Type type, ArrayAggregationState state, Block value, int position)
    {
        state.add(value, position);
    }

    public static void combine(Type type, ArrayAggregationState state, ArrayAggregationState otherState)
    {
        state.merge(otherState);
    }

    public static void output(Type elementType, ArrayAggregationState state, BlockBuilder out)
    {
        if (state.isEmpty()) {
            out.appendNull();
        }
        else {
            BlockBuilder entryBuilder = out.beginBlockEntry();
            state.forEach((block, position) -> elementType.appendTo(block, position, entryBuilder));
            out.closeEntry();
        }
    }
}
