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
import io.airlift.bytecode.DynamicClassLoader;
import io.trino.metadata.AggregationFunctionMetadata;
import io.trino.metadata.FunctionArgumentDefinition;
import io.trino.metadata.FunctionBinding;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlAggregationFunction;
import io.trino.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.trino.operator.aggregation.state.NullableLongState;
import io.trino.operator.aggregation.state.StateCompiler;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.type.BlockTypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionXxHash64;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static io.airlift.slice.Slices.wrappedLongArray;
import static io.trino.metadata.FunctionKind.AGGREGATE;
import static io.trino.metadata.Signature.comparableTypeParameter;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static io.trino.operator.aggregation.AggregationUtils.generateAggregationName;
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
                        true,
                        ImmutableList.of(new FunctionArgumentDefinition(true)),
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
    public InternalAggregationFunction specialize(FunctionBinding functionBinding)
    {
        Type valueType = functionBinding.getTypeVariable("T");
        BlockPositionXxHash64 xxHash64Operator = blockTypeOperators.getXxHash64Operator(valueType);
        return generateAggregation(valueType, xxHash64Operator);
    }

    private static InternalAggregationFunction generateAggregation(Type type, BlockPositionXxHash64 xxHash64Operator)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(ChecksumAggregationFunction.class.getClassLoader());

        AccumulatorStateSerializer<NullableLongState> stateSerializer = StateCompiler.generateStateSerializer(NullableLongState.class, classLoader);
        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, type.getTypeSignature(), ImmutableList.of(type.getTypeSignature())),
                createInputParameterMetadata(type),
                INPUT_FUNCTION.bindTo(xxHash64Operator),
                Optional.empty(),
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                ImmutableList.of(new AccumulatorStateDescriptor(
                        NullableLongState.class,
                        stateSerializer,
                        StateCompiler.generateStateFactory(NullableLongState.class, classLoader))),
                VARBINARY);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, ImmutableList.of(type), ImmutableList.of(stateSerializer.getSerializedType()), VARBINARY, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type type)
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(NULLABLE_BLOCK_INPUT_CHANNEL, type), new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(BlockPositionXxHash64 xxHash64Operator, NullableLongState state, Block block, int position)
    {
        state.setNull(false);
        if (block.isNull(position)) {
            state.setLong(state.getLong() + PRIME64);
        }
        else {
            state.setLong(state.getLong() + xxHash64Operator.xxHash64(block, position) * PRIME64);
        }
    }

    public static void combine(NullableLongState state, NullableLongState otherState)
    {
        state.setNull(state.isNull() && otherState.isNull());
        state.setLong(state.getLong() + otherState.getLong());
    }

    public static void output(NullableLongState state, BlockBuilder out)
    {
        if (state.isNull()) {
            out.appendNull();
        }
        else {
            VARBINARY.writeSlice(out, wrappedLongArray(state.getLong()));
        }
    }
}
