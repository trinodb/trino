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
import io.trino.metadata.SqlAggregationFunction;
import io.trino.operator.aggregation.state.GenericBooleanState;
import io.trino.operator.aggregation.state.GenericBooleanStateSerializer;
import io.trino.operator.aggregation.state.GenericDoubleState;
import io.trino.operator.aggregation.state.GenericDoubleStateSerializer;
import io.trino.operator.aggregation.state.GenericLongState;
import io.trino.operator.aggregation.state.GenericLongStateSerializer;
import io.trino.operator.aggregation.state.StateCompiler;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunctionMetadata;
import io.trino.spi.function.AggregationImplementation;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.gen.lambda.BinaryFunctionInterface;

import java.lang.invoke.MethodHandle;

import static io.trino.operator.aggregation.AggregationFunctionAdapter.AggregationParameterKind.INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.AggregationParameterKind.STATE;
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
                FunctionMetadata.aggregateBuilder()
                        .signature(Signature.builder()
                                .name(NAME)
                                .typeVariable("T")
                                .typeVariable("S")
                                .returnType(new TypeSignature("S"))
                                .argumentType(new TypeSignature("T"))
                                .argumentType(new TypeSignature("S"))
                                .argumentType(functionType(new TypeSignature("S"), new TypeSignature("T"), new TypeSignature("S")))
                                .argumentType(functionType(new TypeSignature("S"), new TypeSignature("S"), new TypeSignature("S")))
                                .build())
                        .description("Reduce input elements into a single value")
                        .build(),
                AggregationFunctionMetadata.builder()
                        .intermediateType(new TypeSignature("S"))
                        .build());
    }

    @Override
    public AggregationImplementation specialize(BoundSignature boundSignature)
    {
        Type inputType = boundSignature.getArgumentTypes().get(0);
        Type stateType = boundSignature.getArgumentTypes().get(1);

        if (stateType.getJavaType() == long.class) {
            return AggregationImplementation.builder()
                    .inputFunction(normalizeInputMethod(boundSignature, inputType, LONG_STATE_INPUT_FUNCTION))
                    .combineFunction(LONG_STATE_COMBINE_FUNCTION)
                    .outputFunction(LONG_STATE_OUTPUT_FUNCTION.bindTo(stateType))
                    .accumulatorStateDescriptor(
                            GenericLongState.class,
                            new GenericLongStateSerializer(stateType),
                            StateCompiler.generateStateFactory(GenericLongState.class))
                    .lambdaInterfaces(BinaryFunctionInterface.class, BinaryFunctionInterface.class)
                    .build();
        }
        if (stateType.getJavaType() == double.class) {
            return AggregationImplementation.builder()
                    .inputFunction(normalizeInputMethod(boundSignature, inputType, DOUBLE_STATE_INPUT_FUNCTION))
                    .combineFunction(DOUBLE_STATE_COMBINE_FUNCTION)
                    .outputFunction(DOUBLE_STATE_OUTPUT_FUNCTION.bindTo(stateType))
                    .accumulatorStateDescriptor(
                            GenericDoubleState.class,
                            new GenericDoubleStateSerializer(stateType),
                            StateCompiler.generateStateFactory(GenericDoubleState.class))
                    .lambdaInterfaces(BinaryFunctionInterface.class, BinaryFunctionInterface.class)
                    .build();
        }
        if (stateType.getJavaType() == boolean.class) {
            return AggregationImplementation.builder()
                    .inputFunction(normalizeInputMethod(boundSignature, inputType, BOOLEAN_STATE_INPUT_FUNCTION))
                    .combineFunction(BOOLEAN_STATE_COMBINE_FUNCTION)
                    .outputFunction(BOOLEAN_STATE_OUTPUT_FUNCTION.bindTo(stateType))
                    .accumulatorStateDescriptor(
                            GenericBooleanState.class,
                            new GenericBooleanStateSerializer(stateType),
                            StateCompiler.generateStateFactory(GenericBooleanState.class))
                    .lambdaInterfaces(BinaryFunctionInterface.class, BinaryFunctionInterface.class)
                    .build();
        }
        // State with Slice or Block as native container type is intentionally not supported yet,
        // as it may result in excessive JVM memory usage of remembered set.
        // See JDK-8017163.
        throw new TrinoException(NOT_SUPPORTED, format("State type not supported for %s: %s", NAME, stateType.getDisplayName()));
    }

    private static MethodHandle normalizeInputMethod(BoundSignature boundSignature, Type inputType, MethodHandle inputMethodHandle)
    {
        inputMethodHandle = inputMethodHandle.asType(inputMethodHandle.type().changeParameterType(1, inputType.getJavaType()));
        inputMethodHandle = AggregationFunctionAdapter.normalizeInputMethod(inputMethodHandle, boundSignature, ImmutableList.of(STATE, INPUT_CHANNEL, INPUT_CHANNEL), 2);
        return inputMethodHandle;
    }

    public static void input(GenericLongState state, Object value, long initialStateValue, BinaryFunctionInterface inputFunction, BinaryFunctionInterface combineFunction)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setValue(initialStateValue);
        }
        state.setValue((long) inputFunction.apply(state.getValue(), value));
    }

    public static void input(GenericDoubleState state, Object value, double initialStateValue, BinaryFunctionInterface inputFunction, BinaryFunctionInterface combineFunction)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setValue(initialStateValue);
        }
        state.setValue((double) inputFunction.apply(state.getValue(), value));
    }

    public static void input(GenericBooleanState state, Object value, boolean initialStateValue, BinaryFunctionInterface inputFunction, BinaryFunctionInterface combineFunction)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setValue(initialStateValue);
        }
        state.setValue((boolean) inputFunction.apply(state.getValue(), value));
    }

    public static void combine(GenericLongState state, GenericLongState otherState, BinaryFunctionInterface inputFunction, BinaryFunctionInterface combineFunction)
    {
        if (state.isNull()) {
            state.set(otherState);
            return;
        }
        state.setValue((long) combineFunction.apply(state.getValue(), otherState.getValue()));
    }

    public static void combine(GenericDoubleState state, GenericDoubleState otherState, BinaryFunctionInterface inputFunction, BinaryFunctionInterface combineFunction)
    {
        if (state.isNull()) {
            state.set(otherState);
            return;
        }
        state.setValue((double) combineFunction.apply(state.getValue(), otherState.getValue()));
    }

    public static void combine(GenericBooleanState state, GenericBooleanState otherState, BinaryFunctionInterface inputFunction, BinaryFunctionInterface combineFunction)
    {
        if (state.isNull()) {
            state.set(otherState);
            return;
        }
        state.setValue((boolean) combineFunction.apply(state.getValue(), otherState.getValue()));
    }
}
