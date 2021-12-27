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
import io.trino.operator.aggregation.TypedKeyValueHeap;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.util.MinMaxCompare;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.trino.metadata.FunctionKind.AGGREGATE;
import static io.trino.metadata.Signature.orderableTypeParameter;
import static io.trino.metadata.Signature.typeVariable;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.AggregationParameterKind.BLOCK_INDEX;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.AggregationParameterKind.BLOCK_INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.AggregationParameterKind.INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.AggregationParameterKind.NULLABLE_BLOCK_INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.AggregationParameterKind.STATE;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.normalizeInputMethod;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeSignature.arrayType;
import static io.trino.util.Failures.checkCondition;
import static io.trino.util.MinMaxCompare.getMinMaxCompare;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.Math.toIntExact;

public abstract class AbstractMinMaxByNAggregationFunction
        extends SqlAggregationFunction
{
    private static final MethodHandle INPUT_FUNCTION = methodHandle(AbstractMinMaxByNAggregationFunction.class, "input", MethodHandle.class, Type.class, Type.class, MinMaxByNState.class, Block.class, Block.class, long.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(AbstractMinMaxByNAggregationFunction.class, "combine", MinMaxByNState.class, MinMaxByNState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(AbstractMinMaxByNAggregationFunction.class, "output", ArrayType.class, MinMaxByNState.class, BlockBuilder.class);
    private static final long MAX_NUMBER_OF_VALUES = 10_000;

    private final boolean min;

    protected AbstractMinMaxByNAggregationFunction(String name, boolean min, String description)
    {
        super(
                new FunctionMetadata(
                        new Signature(
                                name,
                                ImmutableList.of(typeVariable("V"), orderableTypeParameter("K")),
                                ImmutableList.of(),
                                arrayType(new TypeSignature("V")),
                                ImmutableList.of(new TypeSignature("V"), new TypeSignature("K"), BIGINT.getTypeSignature()),
                                false),
                        new FunctionNullability(true, ImmutableList.of(true, false, false)),
                        false,
                        true,
                        description,
                        AGGREGATE),
                new AggregationFunctionMetadata(
                        false,
                        BIGINT.getTypeSignature(),
                        arrayType(new TypeSignature("K")),
                        arrayType(new TypeSignature("V"))));
        this.min = min;
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies(BoundSignature boundSignature)
    {
        return MinMaxCompare.getMinMaxCompareFunctionDependencies(boundSignature.getArgumentTypes().get(1).getTypeSignature(), min);
    }

    @Override
    public AggregationMetadata specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        Type keyType = boundSignature.getArgumentTypes().get(1);
        Type valueType = boundSignature.getArgumentTypes().get(0);
        MethodHandle keyComparisonMethod = getMinMaxCompare(functionDependencies, keyType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION), min);

        MinMaxByNStateSerializer stateSerializer = new MinMaxByNStateSerializer(keyComparisonMethod, keyType, valueType);
        ArrayType outputType = new ArrayType(valueType);

        MethodHandle inputFunction = INPUT_FUNCTION.bindTo(keyComparisonMethod).bindTo(valueType).bindTo(keyType);
        inputFunction = normalizeInputMethod(inputFunction, boundSignature, STATE, NULLABLE_BLOCK_INPUT_CHANNEL, BLOCK_INPUT_CHANNEL, INPUT_CHANNEL, BLOCK_INDEX);

        return new AggregationMetadata(
                inputFunction,
                Optional.empty(),
                Optional.of(COMBINE_FUNCTION),
                OUTPUT_FUNCTION.bindTo(outputType),
                ImmutableList.of(new AccumulatorStateDescriptor<>(
                        MinMaxByNState.class,
                        stateSerializer,
                        new MinMaxByNStateFactory())));
    }

    public static void input(MethodHandle keyComparisonMethod, Type valueType, Type keyType, MinMaxByNState state, Block value, Block key, long n, int blockIndex)
    {
        TypedKeyValueHeap heap = state.getTypedKeyValueHeap();
        if (heap == null) {
            if (n <= 0) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "third argument of max_by/min_by must be a positive integer");
            }
            checkCondition(n <= MAX_NUMBER_OF_VALUES, INVALID_FUNCTION_ARGUMENT, "third argument of max_by/min_by must be less than or equal to %s; found %s", MAX_NUMBER_OF_VALUES, n);
            heap = new TypedKeyValueHeap(keyComparisonMethod, keyType, valueType, toIntExact(n));
            state.setTypedKeyValueHeap(heap);
        }

        long startSize = heap.getEstimatedSize();
        if (!key.isNull(blockIndex)) {
            heap.add(key, value, blockIndex);
        }
        state.addMemoryUsage(heap.getEstimatedSize() - startSize);
    }

    public static void combine(MinMaxByNState state, MinMaxByNState otherState)
    {
        TypedKeyValueHeap otherHeap = otherState.getTypedKeyValueHeap();
        if (otherHeap == null) {
            return;
        }
        TypedKeyValueHeap heap = state.getTypedKeyValueHeap();
        if (heap == null) {
            state.setTypedKeyValueHeap(otherHeap);
            return;
        }
        long startSize = heap.getEstimatedSize();
        heap.addAll(otherHeap);
        state.addMemoryUsage(heap.getEstimatedSize() - startSize);
    }

    public static void output(ArrayType outputType, MinMaxByNState state, BlockBuilder out)
    {
        TypedKeyValueHeap heap = state.getTypedKeyValueHeap();
        if (heap == null || heap.isEmpty()) {
            out.appendNull();
            return;
        }

        Type elementType = outputType.getElementType();

        BlockBuilder arrayBlockBuilder = out.beginBlockEntry();
        BlockBuilder reversedBlockBuilder = elementType.createBlockBuilder(null, heap.getCapacity());
        long startSize = heap.getEstimatedSize();
        heap.popAll(reversedBlockBuilder);
        state.addMemoryUsage(heap.getEstimatedSize() - startSize);

        for (int i = reversedBlockBuilder.getPositionCount() - 1; i >= 0; i--) {
            elementType.appendTo(reversedBlockBuilder, i, arrayBlockBuilder);
        }
        out.closeEntry();
    }
}
