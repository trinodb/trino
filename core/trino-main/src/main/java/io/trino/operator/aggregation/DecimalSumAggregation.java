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
import io.trino.operator.aggregation.state.LongDecimalWithOverflowState;
import io.trino.operator.aggregation.state.LongDecimalWithOverflowStateFactory;
import io.trino.operator.aggregation.state.LongDecimalWithOverflowStateSerializer;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.trino.metadata.FunctionKind.AGGREGATE;
import static io.trino.operator.aggregation.AggregationMetadata.AggregationParameterKind.BLOCK_INDEX;
import static io.trino.operator.aggregation.AggregationMetadata.AggregationParameterKind.BLOCK_INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationMetadata.AggregationParameterKind.STATE;
import static io.trino.spi.type.TypeSignatureParameter.numericParameter;
import static io.trino.spi.type.TypeSignatureParameter.typeVariable;
import static io.trino.spi.type.UnscaledDecimal128Arithmetic.SIGN_LONG_MASK;
import static io.trino.spi.type.UnscaledDecimal128Arithmetic.addWithOverflow;
import static io.trino.spi.type.UnscaledDecimal128Arithmetic.throwIfOverflows;
import static io.trino.spi.type.UnscaledDecimal128Arithmetic.throwOverflowException;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.util.Reflection.methodHandle;

public class DecimalSumAggregation
        extends SqlAggregationFunction
{
    public static final DecimalSumAggregation DECIMAL_SUM_AGGREGATION = new DecimalSumAggregation();
    private static final String NAME = "sum";
    private static final MethodHandle SHORT_DECIMAL_INPUT_FUNCTION = methodHandle(DecimalSumAggregation.class, "inputShortDecimal", LongDecimalWithOverflowState.class, Block.class, int.class);
    private static final MethodHandle LONG_DECIMAL_INPUT_FUNCTION = methodHandle(DecimalSumAggregation.class, "inputLongDecimal", LongDecimalWithOverflowState.class, Block.class, int.class);

    private static final MethodHandle LONG_DECIMAL_OUTPUT_FUNCTION = methodHandle(DecimalSumAggregation.class, "outputLongDecimal", LongDecimalWithOverflowState.class, BlockBuilder.class);

    private static final MethodHandle COMBINE_FUNCTION = methodHandle(DecimalSumAggregation.class, "combine", LongDecimalWithOverflowState.class, LongDecimalWithOverflowState.class);

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
                        VARBINARY.getTypeSignature()));
    }

    @Override
    public AggregationMetadata specialize(BoundSignature boundSignature)
    {
        Type inputType = getOnlyElement(boundSignature.getArgumentTypes());
        return generateAggregation(inputType);
    }

    private static AggregationMetadata generateAggregation(Type inputType)
    {
        checkArgument(inputType instanceof DecimalType, "type must be Decimal");
        MethodHandle inputFunction;
        Class<LongDecimalWithOverflowState> stateInterface = LongDecimalWithOverflowState.class;
        LongDecimalWithOverflowStateSerializer stateSerializer = new LongDecimalWithOverflowStateSerializer();

        if (((DecimalType) inputType).isShort()) {
            inputFunction = SHORT_DECIMAL_INPUT_FUNCTION;
        }
        else {
            inputFunction = LONG_DECIMAL_INPUT_FUNCTION;
        }

        return new AggregationMetadata(
                ImmutableList.of(STATE, BLOCK_INPUT_CHANNEL, BLOCK_INDEX),
                inputFunction,
                Optional.empty(),
                COMBINE_FUNCTION,
                LONG_DECIMAL_OUTPUT_FUNCTION,
                ImmutableList.of(new AccumulatorStateDescriptor<>(
                        stateInterface,
                        stateSerializer,
                        new LongDecimalWithOverflowStateFactory())));
    }

    public static void inputShortDecimal(LongDecimalWithOverflowState state, Block block, int position)
    {
        state.setNotNull();

        long[] decimal = state.getDecimalArray();
        int offset = state.getDecimalArrayOffset();

        long rightLow = block.getLong(position, 0);
        long rightHigh = 0;
        if (rightLow < 0) {
            rightLow = -rightLow;
            rightHigh = SIGN_LONG_MASK;
        }

        long overflow = addWithOverflow(
                decimal[offset],
                decimal[offset + 1],
                rightLow,
                rightHigh,
                decimal,
                offset);
        state.addOverflow(overflow);
    }

    public static void inputLongDecimal(LongDecimalWithOverflowState state, Block block, int position)
    {
        state.setNotNull();

        long[] decimal = state.getDecimalArray();
        int offset = state.getDecimalArrayOffset();

        long overflow = addWithOverflow(
                decimal[offset],
                decimal[offset + 1],
                block.getLong(position, 0),
                block.getLong(position, SIZE_OF_LONG),
                decimal,
                offset);
        state.addOverflow(overflow);
    }

    public static void combine(LongDecimalWithOverflowState state, LongDecimalWithOverflowState otherState)
    {
        long overflow = otherState.getOverflow();

        long[] decimal = state.getDecimalArray();
        int offset = state.getDecimalArrayOffset();

        long[] otherDecimal = otherState.getDecimalArray();
        int otherOffset = otherState.getDecimalArrayOffset();

        if (state.isNotNull()) {
            overflow += addWithOverflow(
                    decimal[offset],
                    decimal[offset + 1],
                    otherDecimal[otherOffset],
                    otherDecimal[otherOffset + 1],
                    decimal,
                    offset);
        }
        else {
            state.setNotNull();
            decimal[offset] = otherDecimal[otherOffset];
            decimal[offset + 1] = otherDecimal[otherOffset + 1];
        }

        state.addOverflow(overflow);
    }

    public static void outputLongDecimal(LongDecimalWithOverflowState state, BlockBuilder out)
    {
        if (state.isNotNull()) {
            if (state.getOverflow() != 0) {
                throwOverflowException();
            }

            long[] decimal = state.getDecimalArray();
            int offset = state.getDecimalArrayOffset();

            long rawLow = decimal[offset];
            long rawHigh = decimal[offset + 1];

            throwIfOverflows(rawLow, rawHigh);
            out.writeLong(rawLow);
            out.writeLong(rawHigh);
            out.closeEntry();
        }
        else {
            out.appendNull();
        }
    }
}
