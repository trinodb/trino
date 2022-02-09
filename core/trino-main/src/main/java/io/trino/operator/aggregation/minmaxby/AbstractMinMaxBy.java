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
package io.trino.operator.aggregation.minmaxby;

import com.google.common.collect.ImmutableList;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.AggregationFunctionMetadata;
import io.trino.metadata.BoundSignature;
import io.trino.metadata.FunctionDependencies;
import io.trino.metadata.FunctionDependencyDeclaration;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.FunctionNullability;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlAggregationFunction;
import io.trino.operator.aggregation.AggregationMetadata;
import io.trino.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.trino.operator.aggregation.state.BlockPositionState;
import io.trino.operator.aggregation.state.BlockPositionStateSerializer;
import io.trino.operator.aggregation.state.NullableBooleanState;
import io.trino.operator.aggregation.state.NullableBooleanStateSerializer;
import io.trino.operator.aggregation.state.NullableDoubleState;
import io.trino.operator.aggregation.state.NullableDoubleStateSerializer;
import io.trino.operator.aggregation.state.NullableLongState;
import io.trino.operator.aggregation.state.NullableLongStateSerializer;
import io.trino.operator.aggregation.state.NullableState;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.util.MinMaxCompare;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.Optional;

import static io.trino.metadata.FunctionKind.AGGREGATE;
import static io.trino.metadata.Signature.orderableTypeParameter;
import static io.trino.metadata.Signature.typeVariable;
import static io.trino.operator.aggregation.state.StateCompiler.generateStateFactory;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.util.MinMaxCompare.getMinMaxCompareFunctionDependencies;
import static io.trino.util.MinMaxCompare.getMinMaxCompareOperatorType;
import static java.lang.invoke.MethodHandles.explicitCastArguments;
import static java.lang.invoke.MethodHandles.insertArguments;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodType.methodType;

public abstract class AbstractMinMaxBy
        extends SqlAggregationFunction
{
    private final boolean min;

    protected AbstractMinMaxBy(boolean min, String description)
    {
        super(
                new FunctionMetadata(
                        new Signature(
                                (min ? "min" : "max") + "_by",
                                ImmutableList.of(orderableTypeParameter("K"), typeVariable("V")),
                                ImmutableList.of(),
                                new TypeSignature("V"),
                                ImmutableList.of(new TypeSignature("V"), new TypeSignature("K")),
                                false),
                        new FunctionNullability(true, ImmutableList.of(true, false)),
                        false,
                        true,
                        description,
                        AGGREGATE),
                new AggregationFunctionMetadata(
                        false,
                        new TypeSignature("K"),
                        new TypeSignature("V")));
        this.min = min;
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies()
    {
        return getMinMaxCompareFunctionDependencies(new TypeSignature("K"), min);
    }

    @Override
    public AggregationMetadata specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        try {
            Type keyType = boundSignature.getArgumentType(1);
            Type valueType = boundSignature.getArgumentType(0);

            MethodHandle inputMethod = generateInput(keyType, valueType, functionDependencies);
            MethodHandle combineMethod = generateCombine(keyType, valueType, functionDependencies);
            MethodHandle outputMethod = generateOutput(keyType, valueType);

            return new AggregationMetadata(
                    inputMethod,
                    Optional.empty(),
                    Optional.of(combineMethod),
                    outputMethod,
                    ImmutableList.of(
                            getAccumulatorStateDescriptor(keyType),
                            getAccumulatorStateDescriptor(valueType)));
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static AccumulatorStateDescriptor<? extends AccumulatorState> getAccumulatorStateDescriptor(Type type)
    {
        Class<? extends AccumulatorState> stateClass = getStateClass(type);
        if (stateClass.equals(BlockPositionState.class)) {
            return new AccumulatorStateDescriptor<>(
                    BlockPositionState.class,
                    new BlockPositionStateSerializer(type),
                    generateStateFactory(BlockPositionState.class));
        }
        return getAccumulatorStateDescriptor(stateClass, type);
    }

    private static <T extends AccumulatorState> AccumulatorStateDescriptor<T> getAccumulatorStateDescriptor(Class<T> stateClass, Type type)
    {
        return new AccumulatorStateDescriptor<>(
                stateClass,
                getStateSerializer(stateClass, type),
                generateStateFactory(stateClass));
    }

    private MethodHandle generateInput(Type keyType, Type valueType, FunctionDependencies functionDependencies)
            throws ReflectiveOperationException
    {
        MethodHandle input = lookup().findStatic(
                AbstractMinMaxBy.class,
                "input",
                methodType(void.class,
                        MethodHandle.class,
                        MethodHandle.class,
                        MethodHandle.class,
                        NullableState.class,
                        NullableState.class,
                        Block.class,
                        Block.class,
                        int.class));

        Class<? extends AccumulatorState> keyState = getStateClass(keyType);
        Class<? extends AccumulatorState> valueState = getStateClass(valueType);

        MethodHandle compareStateBlockPosition = generateCompareStateBlockPosition(keyType, functionDependencies, keyState);
        MethodHandle setKeyState = getSetStateValue(keyType, keyState);
        MethodHandle setValueState = getSetStateValue(valueType, valueState);
        input = insertArguments(input, 0, compareStateBlockPosition, setKeyState, setValueState);
        return explicitCastArguments(input, methodType(void.class, keyState, valueState, Block.class, Block.class, int.class));
    }

    private static void input(
            MethodHandle compareStateBlockPosition,
            MethodHandle setKeyState,
            MethodHandle setValueState,
            NullableState keyState,
            NullableState valueState,
            Block value,
            Block key,
            int position)
            throws Throwable
    {
        if (keyState.isNull() || (boolean) compareStateBlockPosition.invoke(key, position, keyState)) {
            setKeyState.invoke(keyState, key, position);
            setValueState.invoke(valueState, value, position);
        }
    }

    private MethodHandle generateCombine(Type keyType, Type valueType, FunctionDependencies functionDependencies)
            throws ReflectiveOperationException
    {
        MethodHandle combine = lookup().findStatic(
                AbstractMinMaxBy.class,
                "combine",
                methodType(void.class,
                        MethodHandle.class,
                        MethodHandle.class,
                        MethodHandle.class,
                        NullableState.class,
                        NullableState.class,
                        NullableState.class,
                        NullableState.class));

        Class<? extends AccumulatorState> keyState = getStateClass(keyType);
        Class<? extends AccumulatorState> valueState = getStateClass(valueType);

        MethodHandle compareStateBlockPosition = generateCompareStateState(keyType, functionDependencies, keyState);
        MethodHandle setKeyState = lookup().findVirtual(keyState, "set", methodType(void.class, keyState));
        MethodHandle setValueState = lookup().findVirtual(valueState, "set", methodType(void.class, valueState));
        combine = insertArguments(combine, 0, compareStateBlockPosition, setKeyState, setValueState);
        return explicitCastArguments(combine, methodType(void.class, keyState, valueState, keyState, valueState));
    }

    private static void combine(
            MethodHandle compareStateState,
            MethodHandle setKeyState,
            MethodHandle setValueState,
            NullableState keyState,
            NullableState valueState,
            NullableState otherKeyState,
            NullableState otherValueState)
            throws Throwable
    {
        if (otherKeyState.isNull()) {
            return;
        }
        if (keyState.isNull() || (boolean) compareStateState.invoke(otherKeyState, keyState)) {
            setKeyState.invoke(keyState, otherKeyState);
            setValueState.invoke(valueState, otherValueState);
        }
    }

    private static MethodHandle generateOutput(Type keyType, Type valueType)
            throws ReflectiveOperationException
    {
        Class<? extends AccumulatorState> keyState = getStateClass(keyType);
        Class<? extends AccumulatorState> valueState = getStateClass(valueType);
        MethodHandle writeState = lookup().findStatic(AbstractMinMaxBy.class, "writeState", methodType(void.class, Type.class, valueState, BlockBuilder.class))
                .bindTo(valueType);
        MethodHandle output = lookup().findStatic(
                AbstractMinMaxBy.class,
                "output",
                methodType(void.class, MethodHandle.class, NullableState.class, NullableState.class, BlockBuilder.class));
        output = output.bindTo(writeState);
        return explicitCastArguments(output, methodType(void.class, keyState, valueState, BlockBuilder.class));
    }

    private static void output(
            MethodHandle valueWriter,
            NullableState keyState,
            NullableState valueState,
            BlockBuilder blockBuilder)
            throws Throwable
    {
        if (keyState.isNull() || valueState.isNull()) {
            blockBuilder.appendNull();
            return;
        }
        valueWriter.invoke(valueState, blockBuilder);
    }

    @UsedByGeneratedCode
    private static void writeState(Type type, NullableLongState state, BlockBuilder output)
    {
        type.writeLong(output, state.getValue());
    }

    @UsedByGeneratedCode
    private static void writeState(Type type, NullableDoubleState state, BlockBuilder output)
    {
        type.writeDouble(output, state.getValue());
    }

    @UsedByGeneratedCode
    private static void writeState(Type type, NullableBooleanState state, BlockBuilder output)
    {
        type.writeBoolean(output, state.getValue());
    }

    @UsedByGeneratedCode
    private static void writeState(Type type, BlockPositionState state, BlockBuilder output)
    {
        type.appendTo(state.getBlock(), state.getPosition(), output);
    }

    private static Class<? extends AccumulatorState> getStateClass(Type type)
    {
        if (type.getJavaType().equals(long.class)) {
            return NullableLongState.class;
        }
        if (type.getJavaType().equals(double.class)) {
            return NullableDoubleState.class;
        }
        if (type.getJavaType().equals(boolean.class)) {
            return NullableBooleanState.class;
        }
        return BlockPositionState.class;
    }

    @SuppressWarnings("unchecked")
    private static <T extends AccumulatorState> AccumulatorStateSerializer<T> getStateSerializer(Class<T> state, Type type)
    {
        if (NullableLongState.class.equals(state)) {
            return (AccumulatorStateSerializer<T>) new NullableLongStateSerializer(type);
        }
        if (NullableDoubleState.class.equals(state)) {
            return (AccumulatorStateSerializer<T>) new NullableDoubleStateSerializer(type);
        }
        if (NullableBooleanState.class.equals(state)) {
            return (AccumulatorStateSerializer<T>) new NullableBooleanStateSerializer(type);
        }
        if (BlockPositionState.class.equals(state)) {
            return (AccumulatorStateSerializer<T>) new BlockPositionStateSerializer(type);
        }
        throw new IllegalArgumentException("Unsupported state class: " + state);
    }

    private static MethodHandle getSetStateValue(Type type, Class<?> stateClass)
            throws ReflectiveOperationException
    {
        if (stateClass.equals(BlockPositionState.class)) {
            return lookup().findStatic(AbstractMinMaxBy.class, "setStateValue", methodType(void.class, BlockPositionState.class, Block.class, int.class));
        }
        return lookup().findStatic(AbstractMinMaxBy.class, "setStateValue", methodType(void.class, Type.class, stateClass, Block.class, int.class))
                .bindTo(type);
    }

    @UsedByGeneratedCode
    private static void setStateValue(BlockPositionState state, Block block, int position)
    {
        state.setBlock(block);
        state.setPosition(position);
    }

    @UsedByGeneratedCode
    private static void setStateValue(Type valueType, NullableLongState state, Block block, int position)
    {
        if (block.isNull(position)) {
            state.setNull(true);
        }
        else {
            state.setNull(false);
            state.setValue(valueType.getLong(block, position));
        }
    }

    @UsedByGeneratedCode
    private static void setStateValue(Type valueType, NullableDoubleState state, Block block, int position)
    {
        if (block.isNull(position)) {
            state.setNull(true);
        }
        else {
            state.setNull(false);
            state.setValue(valueType.getDouble(block, position));
        }
    }

    @UsedByGeneratedCode
    private static void setStateValue(Type valueType, NullableBooleanState state, Block block, int position)
    {
        if (block.isNull(position)) {
            state.setNull(true);
        }
        else {
            state.setNull(false);
            state.setValue(valueType.getBoolean(block, position));
        }
    }

    private MethodHandle generateCompareStateBlockPosition(Type type, FunctionDependencies functionDependencies, Class<?> state)
            throws ReflectiveOperationException
    {
        if (state.equals(BlockPositionState.class)) {
            MethodHandle comparisonMethod = lookup().findStatic(AbstractMinMaxBy.class, "compareStateBlockPosition", methodType(long.class, MethodHandle.class, Block.class, int.class, BlockPositionState.class))
                    .bindTo(functionDependencies.getOperatorInvoker(getMinMaxCompareOperatorType(min), ImmutableList.of(type, type), simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION)).getMethodHandle());
            return MinMaxCompare.comparisonToMinMaxResult(min, comparisonMethod);
        }
        MethodHandle minMaxMethod = MinMaxCompare.getMinMaxCompare(functionDependencies, type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, NEVER_NULL), min);
        MethodHandle stateGetValue = lookup().findVirtual(state, "getValue", methodType(type.getJavaType()));
        return MethodHandles.filterArguments(minMaxMethod, 2, stateGetValue);
    }

    private static long compareStateBlockPosition(MethodHandle blockPositionBlockPositionOperator, Block left, int leftPosition, BlockPositionState state)
            throws Throwable
    {
        return (long) blockPositionBlockPositionOperator.invokeExact(left, leftPosition, state.getBlock(), state.getPosition());
    }

    private MethodHandle generateCompareStateState(Type type, FunctionDependencies functionDependencies, Class<?> state)
            throws ReflectiveOperationException
    {
        if (state.equals(BlockPositionState.class)) {
            MethodHandle comparisonMethod = lookup().findStatic(AbstractMinMaxBy.class, "compareStateState", methodType(long.class, MethodHandle.class, BlockPositionState.class, BlockPositionState.class))
                    .bindTo(functionDependencies.getOperatorInvoker(getMinMaxCompareOperatorType(min), ImmutableList.of(type, type), simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION)).getMethodHandle());
            return MinMaxCompare.comparisonToMinMaxResult(min, comparisonMethod);
        }
        MethodHandle maxMaxMethod = MinMaxCompare.getMinMaxCompare(functionDependencies, type, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL), min);
        MethodHandle stateGetValue = lookup().findVirtual(state, "getValue", methodType(type.getJavaType()));
        return MethodHandles.filterArguments(maxMaxMethod, 0, stateGetValue, stateGetValue);
    }

    private static long compareStateState(MethodHandle blockPositionBlockPositionOperator, BlockPositionState state, BlockPositionState otherState)
            throws Throwable
    {
        return (long) blockPositionBlockPositionOperator.invokeExact(state.getBlock(), state.getPosition(), otherState.getBlock(), otherState.getPosition());
    }
}
