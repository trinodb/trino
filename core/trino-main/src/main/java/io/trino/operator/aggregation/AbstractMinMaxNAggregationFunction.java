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
import io.trino.metadata.FunctionDependencies;
import io.trino.metadata.FunctionDependencyDeclaration;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlAggregationFunction;
import io.trino.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.trino.operator.aggregation.state.MinMaxNState;
import io.trino.operator.aggregation.state.MinMaxNStateFactory;
import io.trino.operator.aggregation.state.MinMaxNStateSerializer;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.util.MinMaxCompare;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metadata.FunctionKind.AGGREGATE;
import static io.trino.metadata.Signature.orderableTypeParameter;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static io.trino.operator.aggregation.AggregationUtils.generateAggregationName;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.util.Failures.checkCondition;
import static io.trino.util.MinMaxCompare.getMinMaxCompare;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.Math.toIntExact;

public abstract class AbstractMinMaxNAggregationFunction
        extends SqlAggregationFunction
{
    private static final MethodHandle INPUT_FUNCTION = methodHandle(AbstractMinMaxNAggregationFunction.class, "input", MethodHandle.class, Type.class, MinMaxNState.class, Block.class, long.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(AbstractMinMaxNAggregationFunction.class, "combine", MinMaxNState.class, MinMaxNState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(AbstractMinMaxNAggregationFunction.class, "output", ArrayType.class, MinMaxNState.class, BlockBuilder.class);
    private static final long MAX_NUMBER_OF_VALUES = 10_000;

    private final boolean min;

    protected AbstractMinMaxNAggregationFunction(String name, boolean min, String description)
    {
        super(
                new FunctionMetadata(
                        new Signature(
                                name,
                                ImmutableList.of(orderableTypeParameter("E")),
                                ImmutableList.of(),
                                TypeSignature.arrayType(new TypeSignature("E")),
                                ImmutableList.of(new TypeSignature("E"), BIGINT.getTypeSignature()),
                                false),
                        true,
                        ImmutableList.of(
                                new FunctionArgumentDefinition(false),
                                new FunctionArgumentDefinition(false)),
                        false,
                        true,
                        description,
                        AGGREGATE),
                new AggregationFunctionMetadata(
                        false,
                        BIGINT.getTypeSignature(),
                        TypeSignature.arrayType(new TypeSignature("E"))));
        this.min = min;
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies(FunctionBinding functionBinding)
    {
        return MinMaxCompare.getMinMaxCompareFunctionDependencies(functionBinding.getTypeVariable("E").getTypeSignature(), min);
    }

    @Override
    public InternalAggregationFunction specialize(FunctionBinding functionBinding, FunctionDependencies functionDependencies)
    {
        Type type = functionBinding.getTypeVariable("E");
        MethodHandle compare = getMinMaxCompare(functionDependencies, type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION), min);
        return generateAggregation(compare, type);
    }

    protected InternalAggregationFunction generateAggregation(MethodHandle compare, Type type)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(AbstractMinMaxNAggregationFunction.class.getClassLoader());

        List<Type> inputTypes = ImmutableList.of(type, BIGINT);
        MinMaxNStateSerializer stateSerializer = new MinMaxNStateSerializer(compare, type);
        Type intermediateType = stateSerializer.getSerializedType();
        ArrayType outputType = new ArrayType(type);

        List<ParameterMetadata> inputParameterMetadata = ImmutableList.of(
                new ParameterMetadata(STATE),
                new ParameterMetadata(BLOCK_INPUT_CHANNEL, type),
                new ParameterMetadata(INPUT_CHANNEL, BIGINT),
                new ParameterMetadata(BLOCK_INDEX));

        String name = getFunctionMetadata().getSignature().getName();
        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(name, type.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                inputParameterMetadata,
                INPUT_FUNCTION.bindTo(compare).bindTo(type),
                Optional.empty(),
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION.bindTo(outputType),
                ImmutableList.of(new AccumulatorStateDescriptor(
                        MinMaxNState.class,
                        stateSerializer,
                        new MinMaxNStateFactory())),
                outputType);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(name, inputTypes, ImmutableList.of(intermediateType), outputType, factory);
    }

    public static void input(MethodHandle compare, Type type, MinMaxNState state, Block block, long n, int blockIndex)
    {
        TypedHeap heap = state.getTypedHeap();
        if (heap == null) {
            if (n <= 0) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "second argument of max_n/min_n must be positive");
            }
            checkCondition(n <= MAX_NUMBER_OF_VALUES, INVALID_FUNCTION_ARGUMENT, "second argument of max_n/min_n must be less than or equal to %s; found %s", MAX_NUMBER_OF_VALUES, n);
            heap = new TypedHeap(compare, type, toIntExact(n));
            state.setTypedHeap(heap);
        }
        long startSize = heap.getEstimatedSize();
        heap.add(block, blockIndex);
        state.addMemoryUsage(heap.getEstimatedSize() - startSize);
    }

    public static void combine(MinMaxNState state, MinMaxNState otherState)
    {
        TypedHeap otherHeap = otherState.getTypedHeap();
        if (otherHeap == null) {
            return;
        }
        TypedHeap heap = state.getTypedHeap();
        if (heap == null) {
            state.setTypedHeap(otherHeap);
            return;
        }
        long startSize = heap.getEstimatedSize();
        heap.addAll(otherHeap);
        state.addMemoryUsage(heap.getEstimatedSize() - startSize);
    }

    public static void output(ArrayType outputType, MinMaxNState state, BlockBuilder out)
    {
        TypedHeap heap = state.getTypedHeap();
        if (heap == null || heap.isEmpty()) {
            out.appendNull();
            return;
        }

        Type elementType = outputType.getElementType();

        BlockBuilder reversedBlockBuilder = elementType.createBlockBuilder(null, heap.getCapacity());
        long startSize = heap.getEstimatedSize();
        heap.popAll(reversedBlockBuilder);
        state.addMemoryUsage(heap.getEstimatedSize() - startSize);

        BlockBuilder arrayBlockBuilder = out.beginBlockEntry();
        for (int i = reversedBlockBuilder.getPositionCount() - 1; i >= 0; i--) {
            elementType.appendTo(reversedBlockBuilder, i, arrayBlockBuilder);
        }
        out.closeEntry();
    }
}
