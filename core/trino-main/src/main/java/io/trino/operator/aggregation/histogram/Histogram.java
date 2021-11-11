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
package io.trino.operator.aggregation.histogram;

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
import io.trino.operator.aggregation.GenericAccumulatorFactoryBinder;
import io.trino.operator.aggregation.InternalAggregationFunction;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.type.BlockTypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionEqual;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metadata.FunctionKind.AGGREGATE;
import static io.trino.metadata.Signature.comparableTypeParameter;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static io.trino.operator.aggregation.AggregationUtils.generateAggregationName;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.util.Reflection.methodHandle;
import static java.util.Objects.requireNonNull;

public class Histogram
        extends SqlAggregationFunction
{
    public static final String NAME = "histogram";
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(Histogram.class, "output", Type.class, HistogramState.class, BlockBuilder.class);
    private static final MethodHandle INPUT_FUNCTION = methodHandle(Histogram.class, "input", Type.class, HistogramState.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(Histogram.class, "combine", HistogramState.class, HistogramState.class);

    public static final int EXPECTED_SIZE_FOR_HASHING = 10;
    private final BlockTypeOperators blockTypeOperators;

    public Histogram(BlockTypeOperators blockTypeOperators)
    {
        super(
                new FunctionMetadata(
                        new Signature(
                                NAME,
                                ImmutableList.of(comparableTypeParameter("K")),
                                ImmutableList.of(),
                                mapType(new TypeSignature("K"), BIGINT.getTypeSignature()),
                                ImmutableList.of(new TypeSignature("K")),
                                false),
                        true,
                        ImmutableList.of(new FunctionArgumentDefinition(false)),
                        false,
                        true,
                        "Count the number of times each value occurs",
                        AGGREGATE),
                new AggregationFunctionMetadata(
                        false,
                        mapType(new TypeSignature("K"), BIGINT.getTypeSignature())));
        this.blockTypeOperators = blockTypeOperators;
    }

    @Override
    public InternalAggregationFunction specialize(FunctionBinding functionBinding)
    {
        Type keyType = functionBinding.getTypeVariable("K");
        BlockPositionEqual keyEqual = blockTypeOperators.getEqualOperator(keyType);
        BlockPositionHashCode keyHashCode = blockTypeOperators.getHashCodeOperator(keyType);
        Type outputType = functionBinding.getBoundSignature().getReturnType();
        return generateAggregation(NAME, keyType, keyEqual, keyHashCode, outputType);
    }

    private static InternalAggregationFunction generateAggregation(
            String functionName,
            Type keyType,
            BlockPositionEqual keyEqual,
            BlockPositionHashCode keyHashCode,
            Type outputType)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(Histogram.class.getClassLoader());
        List<Type> inputTypes = ImmutableList.of(keyType);
        HistogramStateSerializer stateSerializer = new HistogramStateSerializer(outputType);
        Type intermediateType = stateSerializer.getSerializedType();
        MethodHandle inputFunction = INPUT_FUNCTION.bindTo(keyType);
        MethodHandle outputFunction = OUTPUT_FUNCTION.bindTo(outputType);

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(functionName, outputType.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(keyType),
                inputFunction,
                Optional.empty(),
                COMBINE_FUNCTION,
                outputFunction,
                ImmutableList.of(new AccumulatorStateDescriptor(
                        HistogramState.class,
                        stateSerializer,
                        new HistogramStateFactory(keyType, keyEqual, keyHashCode, EXPECTED_SIZE_FOR_HASHING))),
                outputType);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(functionName, inputTypes, ImmutableList.of(intermediateType), outputType, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type keyType)
    {
        return ImmutableList.of(new ParameterMetadata(STATE),
                new ParameterMetadata(BLOCK_INPUT_CHANNEL, keyType),
                new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(Type type, HistogramState state, Block key, int position)
    {
        TypedHistogram typedHistogram = state.get();
        long startSize = typedHistogram.getEstimatedSize();
        typedHistogram.add(position, key, 1L);
        state.addMemoryUsage(typedHistogram.getEstimatedSize() - startSize);
    }

    public static void combine(HistogramState state, HistogramState otherState)
    {
        // NOTE: state = current merged state; otherState = scratchState (new data to be added)
        // for grouped histograms and single histograms, we have a single histogram object. In neither case, can otherState.get() return null.
        // Semantically, a histogram object will be returned even if the group is empty.
        // In that case, the histogram object will represent an empty histogram until we call add() on
        // it.
        requireNonNull(otherState.get(), "scratch state should always be non-null");
        TypedHistogram typedHistogram = state.get();
        long startSize = typedHistogram.getEstimatedSize();
        typedHistogram.addAll(otherState.get());
        state.addMemoryUsage(typedHistogram.getEstimatedSize() - startSize);
    }

    public static void output(Type type, HistogramState state, BlockBuilder out)
    {
        TypedHistogram typedHistogram = state.get();
        typedHistogram.serialize(out);
    }
}
