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
import io.trino.operator.aggregation.state.DoubleState;
import io.trino.operator.aggregation.state.LongState;
import io.trino.operator.aggregation.state.StateCompiler;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.trino.metadata.FunctionKind.AGGREGATE;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.AggregationParameterKind.INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.AggregationParameterKind.STATE;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.normalizeInputMethod;
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
                        new FunctionNullability(true, ImmutableList.of(false)),
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
    public AggregationMetadata specialize(BoundSignature boundSignature)
    {
        Class<LongState> longStateInterface = LongState.class;
        Class<DoubleState> doubleStateInterface = DoubleState.class;
        AccumulatorStateSerializer<LongState> longStateSerializer = StateCompiler.generateStateSerializer(longStateInterface);
        AccumulatorStateSerializer<DoubleState> doubleStateSerializer = StateCompiler.generateStateSerializer(doubleStateInterface);

        MethodHandle inputFunction = normalizeInputMethod(INPUT_FUNCTION, boundSignature, STATE, STATE, INPUT_CHANNEL);
        MethodHandle removeFunction = normalizeInputMethod(REMOVE_INPUT_FUNCTION, boundSignature, STATE, STATE, INPUT_CHANNEL);

        return new AggregationMetadata(
                inputFunction,
                Optional.of(removeFunction),
                Optional.of(COMBINE_FUNCTION),
                OUTPUT_FUNCTION,
                ImmutableList.of(
                        new AccumulatorStateDescriptor<>(
                                longStateInterface,
                                longStateSerializer,
                                StateCompiler.generateStateFactory(longStateInterface)),
                        new AccumulatorStateDescriptor<>(
                                doubleStateInterface,
                                doubleStateSerializer,
                                StateCompiler.generateStateFactory(doubleStateInterface))));
    }

    public static void input(LongState count, DoubleState sum, long value)
    {
        count.setValue(count.getValue() + 1);
        sum.setValue(sum.getValue() + intBitsToFloat((int) value));
    }

    public static void removeInput(LongState count, DoubleState sum, long value)
    {
        count.setValue(count.getValue() - 1);
        sum.setValue(sum.getValue() - intBitsToFloat((int) value));
    }

    public static void combine(LongState count, DoubleState sum, LongState otherCount, DoubleState otherSum)
    {
        count.setValue(count.getValue() + otherCount.getValue());
        sum.setValue(sum.getValue() + otherSum.getValue());
    }

    public static void output(LongState count, DoubleState sum, BlockBuilder out)
    {
        if (count.getValue() == 0) {
            out.appendNull();
        }
        else {
            REAL.writeLong(out, floatToIntBits((float) (sum.getValue() / count.getValue())));
        }
    }
}
