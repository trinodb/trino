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
import io.trino.operator.aggregation.state.DoubleState;
import io.trino.operator.aggregation.state.LongState;
import io.trino.operator.aggregation.state.StateCompiler;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static io.trino.metadata.FunctionKind.AGGREGATE;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static io.trino.operator.aggregation.AggregationUtils.generateAggregationName;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.intBitsToFloat;

public class RealAverageAggregation
        extends SqlAggregationFunction
{
    public static final RealAverageAggregation REAL_AVERAGE_AGGREGATION = new RealAverageAggregation();
    private static final String NAME = "avg";

    private static final MethodHandle INPUT_FUNCTION = methodHandle(RealAverageAggregation.class, "input", LongState.class, DoubleState.class, long.class);
    private static final MethodHandle REMOVE_INPUT_FUNCTION = methodHandle(RealAverageAggregation.class, "removeInput", LongState.class, DoubleState.class, long.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(RealAverageAggregation.class, "combine", LongState.class, DoubleState.class, LongState.class, DoubleState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(RealAverageAggregation.class, "output", LongState.class, DoubleState.class, BlockBuilder.class);

    protected RealAverageAggregation()
    {
        super(
                new FunctionMetadata(
                        new Signature(
                                NAME,
                                ImmutableList.of(),
                                ImmutableList.of(),
                                REAL.getTypeSignature(),
                                ImmutableList.of(REAL.getTypeSignature()),
                                false),
                        true,
                        ImmutableList.of(new FunctionArgumentDefinition(false)),
                        false,
                        true,
                        "Returns the average value of the argument",
                        AGGREGATE),
                new AggregationFunctionMetadata(
                        false,
                        BIGINT.getTypeSignature(),
                        DOUBLE.getTypeSignature()));
    }

    @Override
    public InternalAggregationFunction specialize(FunctionBinding functionBinding)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(AverageAggregations.class.getClassLoader());
        Class<? extends AccumulatorState> longStateInterface = LongState.class;
        Class<? extends AccumulatorState> doubleStateInterface = DoubleState.class;
        AccumulatorStateSerializer<?> longStateSerializer = StateCompiler.generateStateSerializer(longStateInterface, classLoader);
        AccumulatorStateSerializer<?> doubleStateSerializer = StateCompiler.generateStateSerializer(doubleStateInterface, classLoader);

        AggregationMetadata aggregationMetadata = new AggregationMetadata(
                generateAggregationName(NAME, REAL.getTypeSignature(), ImmutableList.of(REAL.getTypeSignature())),
                ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(STATE), new ParameterMetadata(INPUT_CHANNEL, REAL)),
                INPUT_FUNCTION,
                Optional.of(REMOVE_INPUT_FUNCTION),
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                ImmutableList.of(
                        new AccumulatorStateDescriptor(
                                longStateInterface,
                                longStateSerializer,
                                StateCompiler.generateStateFactory(longStateInterface, classLoader)),
                        new AccumulatorStateDescriptor(
                                doubleStateInterface,
                                doubleStateSerializer,
                                StateCompiler.generateStateFactory(doubleStateInterface, classLoader))),
                REAL);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(aggregationMetadata, classLoader);
        return new InternalAggregationFunction(
                NAME,
                ImmutableList.of(REAL),
                ImmutableList.of(
                        longStateSerializer.getSerializedType(),
                        doubleStateSerializer.getSerializedType()),
                REAL,
                factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type value)
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(BLOCK_INPUT_CHANNEL, value), new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(LongState count, DoubleState sum, long value)
    {
        count.setLong(count.getLong() + 1);
        sum.setDouble(sum.getDouble() + intBitsToFloat((int) value));
    }

    public static void removeInput(LongState count, DoubleState sum, long value)
    {
        count.setLong(count.getLong() - 1);
        sum.setDouble(sum.getDouble() - intBitsToFloat((int) value));
    }

    public static void combine(LongState count, DoubleState sum, LongState otherCount, DoubleState otherSum)
    {
        count.setLong(count.getLong() + otherCount.getLong());
        sum.setDouble(sum.getDouble() + otherSum.getDouble());
    }

    public static void output(LongState count, DoubleState sum, BlockBuilder out)
    {
        if (count.getLong() == 0) {
            out.appendNull();
        }
        else {
            REAL.writeLong(out, floatToIntBits((float) (sum.getDouble() / count.getLong())));
        }
    }
}
