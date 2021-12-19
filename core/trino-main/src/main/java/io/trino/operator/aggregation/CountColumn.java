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
import io.trino.metadata.AggregationFunctionMetadata;
import io.trino.metadata.BoundSignature;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.FunctionNullability;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlAggregationFunction;
import io.trino.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.trino.operator.aggregation.state.LongState;
import io.trino.operator.aggregation.state.StateCompiler;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.trino.metadata.FunctionKind.AGGREGATE;
import static io.trino.metadata.Signature.typeVariable;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.util.Reflection.methodHandle;

public class CountColumn
        extends SqlAggregationFunction
{
    public static final CountColumn COUNT_COLUMN = new CountColumn();
    private static final String NAME = "count";
    private static final MethodHandle INPUT_FUNCTION = methodHandle(CountColumn.class, "input", LongState.class, Block.class, int.class);
    private static final MethodHandle REMOVE_INPUT_FUNCTION = methodHandle(CountColumn.class, "removeInput", LongState.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(CountColumn.class, "combine", LongState.class, LongState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(CountColumn.class, "output", LongState.class, BlockBuilder.class);

    public CountColumn()
    {
        super(
                new FunctionMetadata(
                        new Signature(
                                NAME,
                                ImmutableList.of(typeVariable("T")),
                                ImmutableList.of(),
                                BIGINT.getTypeSignature(),
                                ImmutableList.of(new TypeSignature("T")),
                                false),
                        new FunctionNullability(true, ImmutableList.of(false)),
                        false,
                        true,
                        "Counts the non-null values",
                        AGGREGATE),
                new AggregationFunctionMetadata(
                        false,
                        BIGINT.getTypeSignature()));
    }

    @Override
    public AggregationMetadata specialize(BoundSignature boundSignature)
    {
        AccumulatorStateSerializer<LongState> stateSerializer = StateCompiler.generateStateSerializer(LongState.class);
        AccumulatorStateFactory<LongState> stateFactory = StateCompiler.generateStateFactory(LongState.class);

        return new AggregationMetadata(
                INPUT_FUNCTION,
                Optional.of(REMOVE_INPUT_FUNCTION),
                Optional.of(COMBINE_FUNCTION),
                OUTPUT_FUNCTION,
                ImmutableList.of(new AccumulatorStateDescriptor<>(
                        LongState.class,
                        stateSerializer,
                        stateFactory)));
    }

    public static void input(LongState state, Block block, int index)
    {
        state.setValue(state.getValue() + 1);
    }

    public static void removeInput(LongState state, Block block, int index)
    {
        state.setValue(state.getValue() - 1);
    }

    public static void combine(LongState state, LongState otherState)
    {
        state.setValue(state.getValue() + otherState.getValue());
    }

    public static void output(LongState state, BlockBuilder out)
    {
        BIGINT.writeLong(out, state.getValue());
    }
}
