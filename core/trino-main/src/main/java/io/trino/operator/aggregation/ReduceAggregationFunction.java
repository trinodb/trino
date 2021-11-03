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
import io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata;
import io.trino.operator.aggregation.state.GenericBooleanState;
import io.trino.operator.aggregation.state.GenericBooleanStateSerializer;
import io.trino.operator.aggregation.state.GenericDoubleState;
import io.trino.operator.aggregation.state.GenericDoubleStateSerializer;
import io.trino.operator.aggregation.state.GenericLongState;
import io.trino.operator.aggregation.state.GenericLongStateSerializer;
import io.trino.operator.aggregation.state.StateCompiler;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.gen.lambda.BinaryFunctionInterface;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static io.trino.metadata.FunctionKind.AGGREGATE;
import static io.trino.metadata.Signature.typeVariable;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static io.trino.operator.aggregation.AggregationUtils.generateAggregationName;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.TypeSignature.functionType;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.String.format;

public class ReduceAggregationFunction
        extends SqlAggregationFunction
{
    public static final ReduceAggregationFunction REDUCE_AGG = new ReduceAggregationFunction();
    private static final String NAME = "reduce_agg";

    private static final MethodHandle LONG_STATE_INPUT_FUNCTION = methodHandle(ReduceAggregationFunction.class, "input", GenericLongState.class, Object.class, long.class, BinaryFunctionInterface.class, BinaryFunctionInterface.class);
    private static final MethodHandle DOUBLE_STATE_INPUT_FUNCTION = methodHandle(ReduceAggregationFunction.class, "input", GenericDoubleState.class, Object.class, double.class, BinaryFunctionInterface.class, BinaryFunctionInterface.class);
    private static final MethodHandle BOOLEAN_STATE_INPUT_FUNCTION = methodHandle(ReduceAggregationFunction.class, "input", GenericBooleanState.class, Object.class, boolean.class, BinaryFunctionInterface.class, BinaryFunctionInterface.class);

    private static final MethodHandle LONG_STATE_COMBINE_FUNCTION = methodHandle(ReduceAggregationFunction.class, "combine", GenericLongState.class, GenericLongState.class, BinaryFunctionInterface.class, BinaryFunctionInterface.class);
    private static final MethodHandle DOUBLE_STATE_COMBINE_FUNCTION = methodHandle(ReduceAggregationFunction.class, "combine", GenericDoubleState.class, GenericDoubleState.class, BinaryFunctionInterface.class, BinaryFunctionInterface.class);
    private static final MethodHandle BOOLEAN_STATE_COMBINE_FUNCTION = methodHandle(ReduceAggregationFunction.class, "combine", GenericBooleanState.class, GenericBooleanState.class, BinaryFunctionInterface.class, BinaryFunctionInterface.class);

    private static final MethodHandle LONG_STATE_OUTPUT_FUNCTION = methodHandle(GenericLongState.class, "write", Type.class, GenericLongState.class, BlockBuilder.class);
    private static final MethodHandle DOUBLE_STATE_OUTPUT_FUNCTION = methodHandle(GenericDoubleState.class, "write", Type.class, GenericDoubleState.class, BlockBuilder.class);
    private static final MethodHandle BOOLEAN_STATE_OUTPUT_FUNCTION = methodHandle(GenericBooleanState.class, "write", Type.class, GenericBooleanState.class, BlockBuilder.class);

    public ReduceAggregationFunction()
    {
        super(
                new FunctionMetadata(
                        new Signature(
                                NAME,
                                ImmutableList.of(typeVariable("T"), typeVariable("S")),
                                ImmutableList.of(),
                                new TypeSignature("S"),
                                ImmutableList.of(
                                        new TypeSignature("T"),
                                        new TypeSignature("S"),
                                        functionType(new TypeSignature("S"), new TypeSignature("T"), new TypeSignature("S")),
                                        functionType(new TypeSignature("S"), new TypeSignature("S"), new TypeSignature("S"))),
                                false),
                        true,
                        ImmutableList.of(
                                new FunctionArgumentDefinition(false),
                                new FunctionArgumentDefinition(false),
                                new FunctionArgumentDefinition(false),
                                new FunctionArgumentDefinition(false)),
                        false,
                        true,
                        "Reduce input elements into a single value",
                        AGGREGATE),
                new AggregationFunctionMetadata(
                        false,
                        new TypeSignature("S")));
    }

    @Override
    public InternalAggregationFunction specialize(FunctionBinding functionBinding)
    {
        Type inputType = functionBinding.getTypeVariable("T");
        Type stateType = functionBinding.getTypeVariable("S");
        return generateAggregation(inputType, stateType);
    }

    private InternalAggregationFunction generateAggregation(Type inputType, Type stateType)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(ReduceAggregationFunction.class.getClassLoader());

        MethodHandle inputMethodHandle;
        MethodHandle combineMethodHandle;
        MethodHandle outputMethodHandle;
        AccumulatorStateDescriptor stateDescriptor;

        if (stateType.getJavaType() == long.class) {
            inputMethodHandle = LONG_STATE_INPUT_FUNCTION;
            combineMethodHandle = LONG_STATE_COMBINE_FUNCTION;
            outputMethodHandle = LONG_STATE_OUTPUT_FUNCTION.bindTo(stateType);
            stateDescriptor = new AccumulatorStateDescriptor(
                    GenericLongState.class,
                    new GenericLongStateSerializer(stateType),
                    StateCompiler.generateStateFactory(GenericLongState.class, classLoader));
        }
        else if (stateType.getJavaType() == double.class) {
            inputMethodHandle = DOUBLE_STATE_INPUT_FUNCTION;
            combineMethodHandle = DOUBLE_STATE_COMBINE_FUNCTION;
            outputMethodHandle = DOUBLE_STATE_OUTPUT_FUNCTION.bindTo(stateType);
            stateDescriptor = new AccumulatorStateDescriptor(
                    GenericDoubleState.class,
                    new GenericDoubleStateSerializer(stateType),
                    StateCompiler.generateStateFactory(GenericDoubleState.class, classLoader));
        }
        else if (stateType.getJavaType() == boolean.class) {
            inputMethodHandle = BOOLEAN_STATE_INPUT_FUNCTION;
            combineMethodHandle = BOOLEAN_STATE_COMBINE_FUNCTION;
            outputMethodHandle = BOOLEAN_STATE_OUTPUT_FUNCTION.bindTo(stateType);
            stateDescriptor = new AccumulatorStateDescriptor(
                    GenericBooleanState.class,
                    new GenericBooleanStateSerializer(stateType),
                    StateCompiler.generateStateFactory(GenericBooleanState.class, classLoader));
        }
        else {
            // State with Slice or Block as native container type is intentionally not supported yet,
            // as it may result in excessive JVM memory usage of remembered set.
            // See JDK-8017163.
            throw new TrinoException(NOT_SUPPORTED, format("State type not supported for %s: %s", NAME, stateType.getDisplayName()));
        }

        String name = getFunctionMetadata().getSignature().getName();
        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(name, inputType.getTypeSignature(), ImmutableList.of(inputType.getTypeSignature())),
                createInputParameterMetadata(inputType, stateType),
                inputMethodHandle.asType(
                        inputMethodHandle.type()
                                .changeParameterType(1, inputType.getJavaType())),
                Optional.empty(),
                combineMethodHandle,
                outputMethodHandle,
                ImmutableList.of(stateDescriptor),
                inputType,
                ImmutableList.of(BinaryFunctionInterface.class, BinaryFunctionInterface.class));

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(
                name,
                ImmutableList.of(inputType),
                ImmutableList.of(stateType),
                stateType,
                factory,
                ImmutableList.of(BinaryFunctionInterface.class, BinaryFunctionInterface.class));
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type inputType, Type stateType)
    {
        return ImmutableList.of(
                new ParameterMetadata(STATE),
                new ParameterMetadata(INPUT_CHANNEL, inputType),
                new ParameterMetadata(INPUT_CHANNEL, stateType));
    }

    public static void input(GenericLongState state, Object value, long initialStateValue, BinaryFunctionInterface inputFunction, BinaryFunctionInterface combineFunction)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setLong(initialStateValue);
        }
        state.setLong((long) inputFunction.apply(state.getLong(), value));
    }

    public static void input(GenericDoubleState state, Object value, double initialStateValue, BinaryFunctionInterface inputFunction, BinaryFunctionInterface combineFunction)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setDouble(initialStateValue);
        }
        state.setDouble((double) inputFunction.apply(state.getDouble(), value));
    }

    public static void input(GenericBooleanState state, Object value, boolean initialStateValue, BinaryFunctionInterface inputFunction, BinaryFunctionInterface combineFunction)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setBoolean(initialStateValue);
        }
        state.setBoolean((boolean) inputFunction.apply(state.getBoolean(), value));
    }

    public static void combine(GenericLongState state, GenericLongState otherState, BinaryFunctionInterface inputFunction, BinaryFunctionInterface combineFunction)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setLong(otherState.getLong());
            return;
        }
        state.setLong((long) combineFunction.apply(state.getLong(), otherState.getLong()));
    }

    public static void combine(GenericDoubleState state, GenericDoubleState otherState, BinaryFunctionInterface inputFunction, BinaryFunctionInterface combineFunction)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setDouble(otherState.getDouble());
            return;
        }
        state.setDouble((double) combineFunction.apply(state.getDouble(), otherState.getDouble()));
    }

    public static void combine(GenericBooleanState state, GenericBooleanState otherState, BinaryFunctionInterface inputFunction, BinaryFunctionInterface combineFunction)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setBoolean(otherState.getBoolean());
            return;
        }
        state.setBoolean((boolean) combineFunction.apply(state.getBoolean(), otherState.getBoolean()));
    }
}
