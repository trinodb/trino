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
import io.airlift.stats.QuantileDigest;
import io.trino.metadata.AggregationFunctionMetadata;
import io.trino.metadata.BoundSignature;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.FunctionNullability;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlAggregationFunction;
import io.trino.operator.aggregation.state.QuantileDigestState;
import io.trino.operator.aggregation.state.QuantileDigestStateFactory;
import io.trino.operator.aggregation.state.QuantileDigestStateSerializer;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.type.QuantileDigestType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.metadata.FunctionKind.AGGREGATE;
import static io.trino.metadata.Signature.comparableTypeParameter;
import static io.trino.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import static io.trino.spi.type.StandardTypes.QDIGEST;
import static io.trino.spi.type.TypeSignature.parametricType;
import static io.trino.util.MoreMath.nearlyEqual;
import static io.trino.util.Reflection.methodHandle;

@AggregationFunction("merge")
public final class MergeQuantileDigestFunction
        extends SqlAggregationFunction
{
    public static final MergeQuantileDigestFunction MERGE = new MergeQuantileDigestFunction();
    public static final String NAME = "merge";
    private static final MethodHandle INPUT_FUNCTION = methodHandle(MergeQuantileDigestFunction.class, "input", Type.class, QuantileDigestState.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(MergeQuantileDigestFunction.class, "combine", QuantileDigestState.class, QuantileDigestState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(MergeQuantileDigestFunction.class, "output", QuantileDigestStateSerializer.class, QuantileDigestState.class, BlockBuilder.class);
    private static final double COMPARISON_EPSILON = 1E-6;

    public MergeQuantileDigestFunction()
    {
        super(
                new FunctionMetadata(
                        new Signature(
                                NAME,
                                ImmutableList.of(comparableTypeParameter("T")),
                                ImmutableList.of(),
                                parametricType("qdigest", new TypeSignature("T")),
                                ImmutableList.of(parametricType("qdigest", new TypeSignature("T"))),
                                false),
                        new FunctionNullability(true, ImmutableList.of(false)),
                        false,
                        true,
                        "Merges the input quantile digests into a single quantile digest",
                        AGGREGATE),
                new AggregationFunctionMetadata(
                        true,
                        parametricType(QDIGEST, new TypeSignature("T"))));
    }

    @Override
    public AggregationMetadata specialize(BoundSignature boundSignature)
    {
        QuantileDigestType outputType = (QuantileDigestType) boundSignature.getReturnType();
        Type valueType = outputType.getValueType();
        QuantileDigestStateSerializer stateSerializer = new QuantileDigestStateSerializer(valueType);

        return new AggregationMetadata(
                INPUT_FUNCTION.bindTo(outputType),
                Optional.empty(),
                Optional.of(COMBINE_FUNCTION),
                OUTPUT_FUNCTION.bindTo(stateSerializer),
                ImmutableList.of(new AccumulatorStateDescriptor<>(
                        QuantileDigestState.class,
                        stateSerializer,
                        new QuantileDigestStateFactory())));
    }

    @InputFunction
    public static void input(Type type, QuantileDigestState state, Block value, int index)
    {
        merge(state, new QuantileDigest(type.getSlice(value, index)));
    }

    @CombineFunction
    public static void combine(QuantileDigestState state, QuantileDigestState otherState)
    {
        merge(state, otherState.getQuantileDigest());
    }

    private static void merge(QuantileDigestState state, QuantileDigest input)
    {
        if (input == null) {
            return;
        }
        QuantileDigest previous = state.getQuantileDigest();
        if (previous == null) {
            state.setQuantileDigest(input);
            state.addMemoryUsage(input.estimatedInMemorySizeInBytes());
        }
        else {
            checkArgument(nearlyEqual(previous.getMaxError(), input.getMaxError(), COMPARISON_EPSILON),
                    "Cannot merge qdigests with different accuracies (%s vs. %s)", state.getQuantileDigest().getMaxError(), input.getMaxError());
            checkArgument(nearlyEqual(previous.getAlpha(), input.getAlpha(), COMPARISON_EPSILON),
                    "Cannot merge qdigests with different alpha values (%s vs. %s)", state.getQuantileDigest().getAlpha(), input.getAlpha());
            state.addMemoryUsage(-previous.estimatedInMemorySizeInBytes());
            previous.merge(input);
            state.addMemoryUsage(previous.estimatedInMemorySizeInBytes());
        }
    }

    public static void output(QuantileDigestStateSerializer serializer, QuantileDigestState state, BlockBuilder out)
    {
        serializer.serialize(state, out);
    }
}
