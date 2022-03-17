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
import io.trino.operator.aggregation.state.Int128State;
import io.trino.operator.aggregation.state.Int128StateFactory;
import io.trino.operator.aggregation.state.Int128StateSerializer;
import io.trino.operator.aggregation.state.NullableLongState;
import io.trino.operator.aggregation.state.StateCompiler;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.trino.metadata.FunctionKind.AGGREGATE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.Int128Math.addWithOverflow;
import static io.trino.spi.type.TypeSignatureParameter.numericParameter;
import static io.trino.spi.type.TypeSignatureParameter.typeVariable;
import static io.trino.util.Reflection.methodHandle;

public class DecimalSumAggregation
        extends SqlAggregationFunction
{
    public static final DecimalSumAggregation DECIMAL_SUM_AGGREGATION = new DecimalSumAggregation();
    private static final String NAME = "sum";
    private static final MethodHandle SHORT_DECIMAL_INPUT_FUNCTION = methodHandle(DecimalSumAggregation.class, "inputShortDecimal", Int128State.class, NullableLongState.class, Block.class, int.class);
    private static final MethodHandle LONG_DECIMAL_INPUT_FUNCTION = methodHandle(DecimalSumAggregation.class, "inputLongDecimal", Int128State.class, NullableLongState.class, Block.class, int.class);
    private static final MethodHandle LONG_DECIMAL_OUTPUT_FUNCTION = methodHandle(DecimalSumAggregation.class, "outputLongDecimal", Int128State.class, NullableLongState.class, BlockBuilder.class);
    private static final MethodHandle IS_NULL_FUNCTION = methodHandle(DecimalAverageAggregation.class, "isNull", Int128State.class);

    private static final MethodHandle COMBINE_FUNCTION = methodHandle(DecimalSumAggregation.class, "combine", Int128State.class, NullableLongState.class, Int128State.class, NullableLongState.class);

    public DecimalSumAggregation()
    {
        super(
                new FunctionMetadata(
                        new Signature(
                                NAME,
                                new TypeSignature("decimal", numericParameter(38), typeVariable("s")),
                                ImmutableList.of(new TypeSignature("decimal", typeVariable("p"), typeVariable("s")))),
                        new FunctionNullability(true, ImmutableList.of(false)),
                        false,
                        true,
                        "Calculates the sum over the input values",
                        AGGREGATE),
                new AggregationFunctionMetadata(
                        false,
                        DecimalType.createDecimalType(Decimals.MAX_SHORT_PRECISION + 1).getTypeSignature(),
                        BIGINT.getTypeSignature()));
    }

    @Override
    public AggregationMetadata specialize(BoundSignature boundSignature)
    {
        Type inputType = getOnlyElement(boundSignature.getArgumentTypes());
        checkArgument(inputType instanceof DecimalType, "type must be Decimal");
        MethodHandle inputFunction;

        if (((DecimalType) inputType).isShort()) {
            inputFunction = SHORT_DECIMAL_INPUT_FUNCTION;
        }
        else {
            inputFunction = LONG_DECIMAL_INPUT_FUNCTION;
        }

        return new AggregationMetadata(
                inputFunction,
                Optional.empty(),
                Optional.of(COMBINE_FUNCTION),
                LONG_DECIMAL_OUTPUT_FUNCTION,
                ImmutableList.of(
                        new AccumulatorStateDescriptor<>(
                                Int128State.class,
                                new Int128StateSerializer(),
                                new Int128StateFactory()),
                        new AccumulatorStateDescriptor<>(
                                NullableLongState.class,
                            StateCompiler.generateStateSerializer(NullableLongState.class),
                            StateCompiler.generateStateFactory(NullableLongState.class))), ImmutableList.of(), Optional.of(IS_NULL_FUNCTION));
    }

    public static void inputShortDecimal(Int128State decimalState, NullableLongState overflowState, Block block, int position)
    {
        long[] decimal = decimalState.getArray();
        int decimalOffset = decimalState.getArrayOffset();

        decimalState.setNotNull(true);
        long rightLow = block.getLong(position, 0);
        long rightHigh = rightLow >> 63;

        long overflow = addWithOverflow(
                decimal[decimalOffset],
                decimal[decimalOffset + 1],
                rightHigh,
                rightLow,
                decimal,
                decimalOffset);
        overflow = Math.addExact(overflow, overflowState.getValue());
        overflowState.setValue(overflow);
        overflowState.setNull(overflow == 0);
    }

    public static void inputLongDecimal(Int128State decimalState, NullableLongState overflowState, Block block, int position)
    {
        long[] decimal = decimalState.getArray();
        int decimalOffset = decimalState.getArrayOffset();

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
        overflow += overflowState.getValue();
        overflowState.setValue(overflow);
        overflowState.setNull(overflow == 0);
    }

    public static void combine(Int128State decimalState, NullableLongState overflowState, Int128State otherDecimalState, NullableLongState otherOverflowState)
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
        decimalState.setNotNull(true);
        // zalozenie, ze LHS nawet jak jest null to jest ok
        int isOverflowNull = overflowState.isNull() ? 0 : 1;
        overflow = overflowState.getValue() + Math.addExact(overflow, otherOverflowState.getValue() * isOverflowNull);
        overflowState.setValue(overflow);
        overflowState.setNull(overflow == 0);
    }

    public static boolean isNull(Int128State decimalState)
    {
        return !decimalState.isNotNull();
    }

    public static void outputLongDecimal(Int128State decimalState, NullableLongState overflowState, BlockBuilder out)
    {
        if (decimalState.isNotNull()) {
            if (overflowState.getValue() != 0) {
                throw new ArithmeticException("Decimal overflow");
            }

            long[] decimal = decimalState.getArray();
            int decimalOffset = decimalState.getArrayOffset();

            long rawHigh = decimal[decimalOffset];
            long rawLow = decimal[decimalOffset + 1];

            Decimals.throwIfOverflows(rawHigh, rawLow);
            out.writeLong(rawHigh);
            out.writeLong(rawLow);
            out.closeEntry();
        }
        else {
            out.appendNull();
        }
    }
}
