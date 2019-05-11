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
package io.prestosql.operator.aggregation.arrayagg;

import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.DynamicClassLoader;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionMetadata;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlAggregationFunction;
import io.prestosql.operator.aggregation.AccumulatorCompiler;
import io.prestosql.operator.aggregation.AggregationMetadata;
import io.prestosql.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata;
import io.prestosql.operator.aggregation.GenericAccumulatorFactoryBinder;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AccumulatorState;
import io.prestosql.spi.function.AccumulatorStateFactory;
import io.prestosql.spi.function.AccumulatorStateSerializer;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.metadata.FunctionKind.AGGREGATE;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static io.prestosql.operator.aggregation.AggregationUtils.generateAggregationName;
import static io.prestosql.util.Reflection.methodHandle;
import static java.util.Objects.requireNonNull;

public class ArrayAggregationFunction
        extends SqlAggregationFunction
{
    private static final String NAME = "array_agg";
    private static final MethodHandle INPUT_FUNCTION = methodHandle(ArrayAggregationFunction.class, "input", Type.class, ArrayAggregationState.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(ArrayAggregationFunction.class, "combine", Type.class, ArrayAggregationState.class, ArrayAggregationState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(ArrayAggregationFunction.class, "output", Type.class, ArrayAggregationState.class, BlockBuilder.class);

    private final ArrayAggGroupImplementation groupMode;

    public ArrayAggregationFunction(ArrayAggGroupImplementation groupMode)
    {
        super(new FunctionMetadata(
                new Signature(
                        NAME,
                        AGGREGATE,
                        ImmutableList.of(typeVariable("T")),
                        ImmutableList.of(),
                        TypeSignature.arrayType(new TypeSignature("T")),
                        ImmutableList.of(new TypeSignature("T")),
                        false),
                false,
                true,
                "return an array of values"));
        this.groupMode = requireNonNull(groupMode, "groupMode is null");
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, Metadata metadata)
    {
        Type type = boundVariables.getTypeVariable("T");
        return generateAggregation(type, groupMode);
    }

    private static InternalAggregationFunction generateAggregation(Type type, ArrayAggGroupImplementation groupMode)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(ArrayAggregationFunction.class.getClassLoader());

        AccumulatorStateSerializer<?> stateSerializer = new ArrayAggregationStateSerializer(type);
        AccumulatorStateFactory<?> stateFactory = new ArrayAggregationStateFactory(type, groupMode);

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
        return new InternalAggregationFunction(NAME, inputTypes, ImmutableList.of(intermediateType), outputType, true, true, factory);
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
