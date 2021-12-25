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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.trino.metadata.AggregationFunctionMetadata;
import io.trino.metadata.BoundSignature;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.FunctionNullability;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlAggregationFunction;
import io.trino.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.trino.operator.aggregation.state.NullableLongState;
import io.trino.operator.aggregation.state.StateCompiler;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.TypeSignature;
import io.trino.type.BlockTypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionXxHash64;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.airlift.slice.Slices.wrappedLongArray;
import static io.trino.metadata.FunctionKind.AGGREGATE;
import static io.trino.metadata.Signature.comparableTypeParameter;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.util.Reflection.methodHandle;
import static java.util.Objects.requireNonNull;

public class ChecksumAggregationFunction
        extends SqlAggregationFunction
{
    @VisibleForTesting
    public static final long PRIME64 = 0x9E3779B185EBCA87L;
    private static final String NAME = "checksum";
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(ChecksumAggregationFunction.class, "output", NullableLongState.class, BlockBuilder.class);
    private static final MethodHandle INPUT_FUNCTION = methodHandle(ChecksumAggregationFunction.class, "input", BlockPositionXxHash64.class, NullableLongState.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(ChecksumAggregationFunction.class, "combine", NullableLongState.class, NullableLongState.class);

    private final BlockTypeOperators blockTypeOperators;

    public ChecksumAggregationFunction(BlockTypeOperators blockTypeOperators)
    {
        super(
                new FunctionMetadata(
                        new Signature(
                                NAME,
                                ImmutableList.of(comparableTypeParameter("T")),
                                ImmutableList.of(),
                                VARBINARY.getTypeSignature(),
                                ImmutableList.of(new TypeSignature("T")),
                                false),
                        new FunctionNullability(true, ImmutableList.of(true)),
                        false,
                        true,
                        "Checksum of the given values",
                        AGGREGATE),
                new AggregationFunctionMetadata(
                        false,
                        BIGINT.getTypeSignature()));
        this.blockTypeOperators = requireNonNull(blockTypeOperators, "blockTypeOperators is null");
    }

    @Override
    public AggregationMetadata specialize(BoundSignature boundSignature)
    {
        BlockPositionXxHash64 xxHash64Operator = blockTypeOperators.getXxHash64Operator(boundSignature.getArgumentTypes().get(0));
        AccumulatorStateSerializer<NullableLongState> stateSerializer = StateCompiler.generateStateSerializer(NullableLongState.class);
        return new AggregationMetadata(
                INPUT_FUNCTION.bindTo(xxHash64Operator),
                Optional.empty(),
                Optional.of(COMBINE_FUNCTION),
                OUTPUT_FUNCTION,
                ImmutableList.of(new AccumulatorStateDescriptor<>(
                        NullableLongState.class,
                        stateSerializer,
                        StateCompiler.generateStateFactory(NullableLongState.class))));
    }

    public static void input(BlockPositionXxHash64 xxHash64Operator, NullableLongState state, Block block, int position)
    {
        state.setNull(false);
        if (block.isNull(position)) {
            state.setValue(state.getValue() + PRIME64);
        }
        else {
            state.setValue(state.getValue() + xxHash64Operator.xxHash64(block, position) * PRIME64);
        }
    }

    public static void combine(NullableLongState state, NullableLongState otherState)
    {
        state.setNull(state.isNull() && otherState.isNull());
        state.setValue(state.getValue() + otherState.getValue());
    }

    public static void output(NullableLongState state, BlockBuilder out)
    {
        if (state.isNull()) {
            out.appendNull();
        }
        else {
            VARBINARY.writeSlice(out, wrappedLongArray(state.getValue()));
        }
    }
}
