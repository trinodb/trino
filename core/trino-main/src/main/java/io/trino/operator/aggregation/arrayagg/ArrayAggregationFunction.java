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
import io.airlift.bytecode.DynamicClassLoader;
import io.trino.metadata.AggregationFunctionMetadata;
import io.trino.metadata.FunctionArgumentDefinition;
import io.trino.metadata.FunctionBinding;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlAggregationFunction;
import io.trino.operator.aggregation.AccumulatorCompiler;
import io.trino.operator.aggregation.AggregationMetadata;
import io.trino.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata;
import io.trino.operator.aggregation.GenericAccumulatorFactoryBinder;
import io.trino.operator.aggregation.InternalAggregationFunction;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metadata.FunctionKind.AGGREGATE;
import static io.trino.metadata.Signature.typeVariable;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static io.trino.operator.aggregation.AggregationUtils.generateAggregationName;
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
                        true,
                        ImmutableList.of(new FunctionArgumentDefinition(true)),
                        false,
                        true,
                        "return an array of values",
                        AGGREGATE),
                new AggregationFunctionMetadata(
                        true,
                        arrayType(new TypeSignature("T"))));
    }

    @Override
    public InternalAggregationFunction specialize(FunctionBinding functionBinding)
    {
        Type type = functionBinding.getTypeVariable("T");
        return generateAggregation(type);
    }

    private static InternalAggregationFunction generateAggregation(Type type)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(ArrayAggregationFunction.class.getClassLoader());

        AccumulatorStateSerializer<?> stateSerializer = new ArrayAggregationStateSerializer(type);
        AccumulatorStateFactory<?> stateFactory = new ArrayAggregationStateFactory(type);

        List<Type> inputTypes = ImmutableList.of(type);
        Type outputType = new ArrayType(type);
        Type intermediateType = stateSerializer.getSerializedType();
        List<ParameterMetadata> inputParameterMetadata = createInputParameterMetadata(type);

        MethodHandle inputFunction = INPUT_FUNCTION.bindTo(type);
        MethodHandle combineFunction = COMBINE_FUNCTION.bindTo(type);
        MethodHandle outputFunction = OUTPUT_FUNCTION.bindTo(type);
        Class<? extends AccumulatorState> stateInterface = ArrayAggregationState.class;

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, type.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                inputParameterMetadata,
                inputFunction,
                Optional.empty(),
                combineFunction,
                outputFunction,
                ImmutableList.of(new AccumulatorStateDescriptor(
                        stateInterface,
                        stateSerializer,
                        stateFactory)),
                outputType);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, ImmutableList.of(intermediateType), outputType, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type value)
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(NULLABLE_BLOCK_INPUT_CHANNEL, value), new ParameterMetadata(BLOCK_INDEX));
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
