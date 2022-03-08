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
import io.trino.operator.aggregation.state.Int128State;
import io.trino.operator.aggregation.state.Int128StateFactory;
import io.trino.operator.aggregation.state.Int128StateSerializer;
import io.trino.operator.aggregation.state.LongState;
import io.trino.operator.aggregation.state.NullableLongState;
import io.trino.operator.aggregation.state.StateCompiler;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.trino.metadata.FunctionKind.AGGREGATE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.Decimals.overflows;
import static io.trino.spi.type.Decimals.writeShortDecimal;
import static io.trino.spi.type.Int128Math.addWithOverflow;
import static io.trino.spi.type.Int128Math.divideRoundUp;
import static io.trino.spi.type.TypeSignatureParameter.typeVariable;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.util.Reflection.methodHandle;
import static java.math.BigDecimal.ROUND_HALF_UP;

public class DecimalAverageAggregation
        extends SqlAggregationFunction
{
    public static final DecimalAverageAggregation DECIMAL_AVERAGE_AGGREGATION = new DecimalAverageAggregation();

    private static final String NAME = "avg";
    private static final MethodHandle SHORT_DECIMAL_INPUT_FUNCTION = methodHandle(DecimalAverageAggregation.class, "inputShortDecimal", Int128State.class, LongState.class, NullableLongState.class, Block.class, int.class);
    private static final MethodHandle LONG_DECIMAL_INPUT_FUNCTION = methodHandle(DecimalAverageAggregation.class, "inputLongDecimal", Int128State.class, LongState.class, NullableLongState.class, Block.class, int.class);

    private static final MethodHandle SHORT_DECIMAL_OUTPUT_FUNCTION = methodHandle(DecimalAverageAggregation.class, "outputShortDecimal", DecimalType.class, Int128State.class, LongState.class, NullableLongState.class, BlockBuilder.class);
    private static final MethodHandle LONG_DECIMAL_OUTPUT_FUNCTION = methodHandle(DecimalAverageAggregation.class, "outputLongDecimal", DecimalType.class, Int128State.class, LongState.class, NullableLongState.class, BlockBuilder.class);

    private static final MethodHandle COMBINE_FUNCTION = methodHandle(DecimalAverageAggregation.class, "combine", Int128State.class, LongState.class, NullableLongState.class, Int128State.class, LongState.class, NullableLongState.class);

    private static final BigInteger TWO = new BigInteger("2");
    private static final BigInteger OVERFLOW_MULTIPLIER = TWO.pow(128);

    public DecimalAverageAggregation()
    {
        super(
                new FunctionMetadata(
                        new Signature(
                                NAME,
                                new TypeSignature("decimal", typeVariable("p"), typeVariable("s")),
                                ImmutableList.of(new TypeSignature("decimal", typeVariable("p"), typeVariable("s")))),
                        new FunctionNullability(true, ImmutableList.of(false)),
                        false,
                        true,
                        "Calculates the average value",
                        AGGREGATE),
                new AggregationFunctionMetadata(
                        false,
                        DecimalType.createDecimalType(Decimals.MAX_SHORT_PRECISION + 1).getTypeSignature(),
                        BIGINT.getTypeSignature(),
                        BIGINT.getTypeSignature()));
    }

    @Override
    public AggregationMetadata specialize(BoundSignature boundSignature)
    {
        Type type = getOnlyElement(boundSignature.getArgumentTypes());
        checkArgument(type instanceof DecimalType, "type must be Decimal");
        MethodHandle inputFunction;
        MethodHandle outputFunction;
        Class<Int128State> stateInterface = Int128State.class;
        Int128StateSerializer stateSerializer = new Int128StateSerializer();

        if (((DecimalType) type).isShort()) {
            inputFunction = SHORT_DECIMAL_INPUT_FUNCTION;
            outputFunction = SHORT_DECIMAL_OUTPUT_FUNCTION;
        }
        else {
            inputFunction = LONG_DECIMAL_INPUT_FUNCTION;
            outputFunction = LONG_DECIMAL_OUTPUT_FUNCTION;
        }
        outputFunction = outputFunction.bindTo(type);

        return new AggregationMetadata(
                inputFunction,
                Optional.empty(),
                Optional.of(COMBINE_FUNCTION),
                outputFunction,
                ImmutableList.of(new AccumulatorStateDescriptor<>(
                        stateInterface,
                        stateSerializer,
                        new Int128StateFactory()),
                        new AccumulatorStateDescriptor<>(
                                LongState.class,
                                StateCompiler.generateStateSerializer(LongState.class),
                                StateCompiler.generateStateFactory(LongState.class)),
                        new AccumulatorStateDescriptor<>(
                                NullableLongState.class,
                                StateCompiler.generateStateSerializer(NullableLongState.class),
                                StateCompiler.generateStateFactory(NullableLongState.class))));
    }

    public static void inputShortDecimal(Int128State decimalState, LongState counterState, NullableLongState overflowState, Block block, int position)
    {
        long[] decimal = decimalState.getArray();
        int decimalOffset = decimalState.getArrayOffset();

        decimalState.setNotNull(true);
        counterState.setValue(counterState.getValue() + 1);

        long rightLow = block.getLong(position, 0);
        long rightHigh = rightLow >> 63;

        long overflow = addWithOverflow(
                decimal[decimalOffset],
                decimal[decimalOffset + 1],
                rightHigh,
                rightLow,
                decimal,
                decimalOffset);

        overflowState.setNull(overflow == 0);
        overflowState.setValue(overflowState.getValue() + overflow);
    }

    public static void inputLongDecimal(Int128State decimalState, LongState counterState, NullableLongState overflowState, Block block, int position)
    {
        long[] decimal = decimalState.getArray();
        int decimalOffset = decimalState.getArrayOffset();

        counterState.setValue(counterState.getValue() + 1);
        decimalState.setNotNull(true);

        long rightHigh = block.getLong(position, 0);
        long rightLow = block.getLong(position, SIZE_OF_LONG);

        long overflow = addWithOverflow(
                decimal[decimalOffset],
                decimal[decimalOffset + 1],
                rightHigh,
                rightLow,
                decimal,
                decimalOffset);

        overflowState.setNull(overflow == 0);
        overflowState.setValue(overflowState.getValue() + overflow);
    }

    public static void combine(Int128State decimalState, LongState counterState, NullableLongState overflowState, Int128State otherDecimalState, LongState otherCounterState, NullableLongState otherOverflowState)
    {
        long[] decimal = decimalState.getArray();
        int decimalOffset = decimalState.getArrayOffset();
        long[] otherDecimal = otherDecimalState.getArray();
        int otherDecimalOffset = otherDecimalState.getArrayOffset();

        long overflow = addWithOverflow(
                    decimal[decimalOffset],
                    decimal[decimalOffset + 1],
                    otherDecimal[otherDecimalOffset],
                    otherDecimal[otherDecimalOffset + 1],
                    decimal,
                    decimalOffset);
        decimalState.setNotNull(decimalState.isNotNull() | otherDecimalState.isNotNull());
        counterState.setValue(counterState.getValue() + otherCounterState.getValue());
        overflowState.setValue(overflowState.getValue() + overflow + otherOverflowState.getValue());
        overflowState.setNull(overflowState.isNull() & otherOverflowState.isNull() & overflow == 0);
    }

    public static void outputShortDecimal(DecimalType type, Int128State decimalState, LongState counterState, NullableLongState overflowState, BlockBuilder out)
    {
        if (counterState.getValue() == 0) {
            out.appendNull();
        }
        else {
            writeShortDecimal(out, average(decimalState, counterState, overflowState, type).toLongExact());
        }
    }

    public static void outputLongDecimal(DecimalType type, Int128State decimalState, LongState counterState, NullableLongState overflowState, BlockBuilder out)
    {
        if (counterState.getValue() == 0) {
            out.appendNull();
        }
        else {
            type.writeObject(out, average(decimalState, counterState, overflowState, type));
        }
    }

    @VisibleForTesting
    public static Int128 average(Int128State decimalState, LongState counterState, NullableLongState overflowState, DecimalType type)
    {
        long[] decimal = decimalState.getArray();
        int decimalOffset = decimalState.getArrayOffset();

        long overflow = overflowState.getValue();
        if (overflow != 0) {
            BigDecimal sum = new BigDecimal(Int128.valueOf(decimal[decimalOffset], decimal[decimalOffset + 1]).toBigInteger(), type.getScale());
            sum = sum.add(new BigDecimal(OVERFLOW_MULTIPLIER.multiply(BigInteger.valueOf(overflow))));

            BigDecimal count = BigDecimal.valueOf(counterState.getValue());
            return Decimals.encodeScaledValue(sum.divide(count, type.getScale(), ROUND_HALF_UP), type.getScale());
        }

        Int128 result = divideRoundUp(decimal[decimalOffset], decimal[decimalOffset + 1], 0, 0, counterState.getValue(), 0);
        if (overflows(result)) {
            throw new ArithmeticException("Decimal overflow");
        }
        return result;
    }
}
