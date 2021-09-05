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
package io.trino.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.DynamicClassLoader;
import io.trino.metadata.AggregationFunctionMetadata;
import io.trino.metadata.FunctionArgumentDefinition;
import io.trino.metadata.FunctionBinding;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlAggregationFunction;
import io.trino.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.trino.operator.aggregation.state.BlockPositionState;
import io.trino.operator.aggregation.state.BlockPositionStateSerializer;
import io.trino.operator.aggregation.state.GenericBooleanState;
import io.trino.operator.aggregation.state.GenericBooleanStateSerializer;
import io.trino.operator.aggregation.state.GenericDoubleState;
import io.trino.operator.aggregation.state.GenericDoubleStateSerializer;
import io.trino.operator.aggregation.state.GenericLongState;
import io.trino.operator.aggregation.state.GenericLongStateSerializer;
import io.trino.operator.aggregation.state.StateCompiler;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metadata.FunctionKind.AGGREGATE;
import static io.trino.metadata.Signature.typeVariable;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static io.trino.operator.aggregation.AggregationUtils.generateAggregationName;
import static io.trino.util.Reflection.methodHandle;

public class ArbitraryAggregationFunction
        extends SqlAggregationFunction
{
    public static final ArbitraryAggregationFunction ARBITRARY_AGGREGATION = new ArbitraryAggregationFunction();
    private static final String NAME = "arbitrary";

    private static final MethodHandle LONG_INPUT_FUNCTION = methodHandle(ArbitraryAggregationFunction.class, "input", Type.class, GenericLongState.class, Block.class, int.class);
    private static final MethodHandle DOUBLE_INPUT_FUNCTION = methodHandle(ArbitraryAggregationFunction.class, "input", Type.class, GenericDoubleState.class, Block.class, int.class);
    private static final MethodHandle BOOLEAN_INPUT_FUNCTION = methodHandle(ArbitraryAggregationFunction.class, "input", Type.class, GenericBooleanState.class, Block.class, int.class);
    private static final MethodHandle BLOCK_POSITION_INPUT_FUNCTION = methodHandle(ArbitraryAggregationFunction.class, "input", Type.class, BlockPositionState.class, Block.class, int.class);

    private static final MethodHandle LONG_OUTPUT_FUNCTION = methodHandle(GenericLongState.class, "write", Type.class, GenericLongState.class, BlockBuilder.class);
    private static final MethodHandle DOUBLE_OUTPUT_FUNCTION = methodHandle(GenericDoubleState.class, "write", Type.class, GenericDoubleState.class, BlockBuilder.class);
    private static final MethodHandle BOOLEAN_OUTPUT_FUNCTION = methodHandle(GenericBooleanState.class, "write", Type.class, GenericBooleanState.class, BlockBuilder.class);
    private static final MethodHandle BLOCK_POSITION_OUTPUT_FUNCTION = methodHandle(BlockPositionState.class, "write", Type.class, BlockPositionState.class, BlockBuilder.class);

    private static final MethodHandle LONG_COMBINE_FUNCTION = methodHandle(ArbitraryAggregationFunction.class, "combine", GenericLongState.class, GenericLongState.class);
    private static final MethodHandle DOUBLE_COMBINE_FUNCTION = methodHandle(ArbitraryAggregationFunction.class, "combine", GenericDoubleState.class, GenericDoubleState.class);
    private static final MethodHandle BOOLEAN_COMBINE_FUNCTION = methodHandle(ArbitraryAggregationFunction.class, "combine", GenericBooleanState.class, GenericBooleanState.class);
    private static final MethodHandle BLOCK_POSITION_COMBINE_FUNCTION = methodHandle(ArbitraryAggregationFunction.class, "combine", BlockPositionState.class, BlockPositionState.class);

    protected ArbitraryAggregationFunction()
    {
        super(
                new FunctionMetadata(
                        new Signature(
                                NAME,
                                ImmutableList.of(typeVariable("T")),
                                ImmutableList.of(),
                                new TypeSignature("T"),
                                ImmutableList.of(new TypeSignature("T")),
                                false),
                        true,
                        ImmutableList.of(new FunctionArgumentDefinition(false)),
                        false,
                        true,
                        "Return an arbitrary non-null input value",
                        AGGREGATE),
                new AggregationFunctionMetadata(
                        false,
                        new TypeSignature("T")));
    }

    @Override
    public InternalAggregationFunction specialize(FunctionBinding functionBinding)
    {
        Type valueType = functionBinding.getTypeVariable("T");
        return generateAggregation(valueType);
    }

    private static InternalAggregationFunction generateAggregation(Type type)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(ArbitraryAggregationFunction.class.getClassLoader());

        List<Type> inputTypes = ImmutableList.of(type);

        MethodHandle inputFunction;
        MethodHandle combineFunction;
        MethodHandle outputFunction;
        Class<? extends AccumulatorState> stateInterface;
        AccumulatorStateSerializer<?> stateSerializer;

        if (type.getJavaType() == long.class) {
            stateInterface = GenericLongState.class;
            stateSerializer = new GenericLongStateSerializer(type);
            inputFunction = LONG_INPUT_FUNCTION;
            combineFunction = LONG_COMBINE_FUNCTION;
            outputFunction = LONG_OUTPUT_FUNCTION;
        }
        else if (type.getJavaType() == double.class) {
            stateInterface = GenericDoubleState.class;
            stateSerializer = new GenericDoubleStateSerializer(type);
            inputFunction = DOUBLE_INPUT_FUNCTION;
            combineFunction = DOUBLE_COMBINE_FUNCTION;
            outputFunction = DOUBLE_OUTPUT_FUNCTION;
        }
        else if (type.getJavaType() == boolean.class) {
            stateInterface = GenericBooleanState.class;
            stateSerializer = new GenericBooleanStateSerializer(type);
            inputFunction = BOOLEAN_INPUT_FUNCTION;
            combineFunction = BOOLEAN_COMBINE_FUNCTION;
            outputFunction = BOOLEAN_OUTPUT_FUNCTION;
        }
        else {
            //  native container type is Slice or Block
            stateInterface = BlockPositionState.class;
            stateSerializer = new BlockPositionStateSerializer(type);
            inputFunction = BLOCK_POSITION_INPUT_FUNCTION;
            combineFunction = BLOCK_POSITION_COMBINE_FUNCTION;
            outputFunction = BLOCK_POSITION_OUTPUT_FUNCTION;
        }
        inputFunction = inputFunction.bindTo(type);

        Type intermediateType = stateSerializer.getSerializedType();
        List<ParameterMetadata> inputParameterMetadata = createInputParameterMetadata(type);
        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, type.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                inputParameterMetadata,
                inputFunction,
                Optional.empty(),
                combineFunction,
                outputFunction.bindTo(type),
                ImmutableList.of(new AccumulatorStateDescriptor(
                        stateInterface,
                        stateSerializer,
                        StateCompiler.generateStateFactory(stateInterface, classLoader))),
                type);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, ImmutableList.of(intermediateType), type, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type value)
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(BLOCK_INPUT_CHANNEL, value), new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(Type type, GenericDoubleState state, Block block, int position)
    {
        if (!state.isNull()) {
            return;
        }
        state.setNull(false);
        state.setDouble(type.getDouble(block, position));
    }

    public static void input(Type type, GenericLongState state, Block block, int position)
    {
        if (!state.isNull()) {
            return;
        }
        state.setNull(false);
        state.setLong(type.getLong(block, position));
    }

    public static void input(Type type, GenericBooleanState state, Block block, int position)
    {
        if (!state.isNull()) {
            return;
        }
        state.setNull(false);
        state.setBoolean(type.getBoolean(block, position));
    }

    public static void input(Type type, BlockPositionState state, Block block, int position)
    {
        if (state.getBlock() != null) {
            return;
        }
        state.setBlock(block);
        state.setPosition(position);
    }

    public static void combine(GenericLongState state, GenericLongState otherState)
    {
        if (!state.isNull()) {
            return;
        }
        state.setNull(false);
        state.setLong(otherState.getLong());
    }

    public static void combine(GenericDoubleState state, GenericDoubleState otherState)
    {
        if (!state.isNull()) {
            return;
        }
        state.setNull(false);
        state.setDouble(otherState.getDouble());
    }

    public static void combine(GenericBooleanState state, GenericBooleanState otherState)
    {
        if (!state.isNull()) {
            return;
        }
        state.setNull(false);
        state.setBoolean(otherState.getBoolean());
    }

    public static void combine(BlockPositionState state, BlockPositionState otherState)
    {
        if (state.getBlock() != null) {
            return;
        }
        state.setBlock(otherState.getBlock());
        state.setPosition(otherState.getPosition());
    }
}
