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
import io.airlift.slice.Slice;
import io.trino.metadata.AggregationFunctionMetadata;
import io.trino.metadata.FunctionArgumentDefinition;
import io.trino.metadata.FunctionBinding;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlAggregationFunction;
import io.trino.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.trino.operator.aggregation.state.LongDecimalWithOverflowAndLongState;
import io.trino.operator.aggregation.state.LongDecimalWithOverflowAndLongStateFactory;
import io.trino.operator.aggregation.state.LongDecimalWithOverflowAndLongStateSerializer;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.UnscaledDecimal128Arithmetic;

import java.lang.invoke.MethodHandle;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.metadata.FunctionKind.AGGREGATE;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static io.trino.operator.aggregation.AggregationUtils.generateAggregationName;
import static io.trino.spi.type.Decimals.MAX_PRECISION;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static io.trino.spi.type.Decimals.writeBigDecimal;
import static io.trino.spi.type.Decimals.writeShortDecimal;
import static io.trino.spi.type.TypeSignatureParameter.typeVariable;
import static io.trino.spi.type.UnscaledDecimal128Arithmetic.UNSCALED_DECIMAL_128_SLICE_LENGTH;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.util.Reflection.methodHandle;
import static java.math.BigDecimal.ROUND_HALF_UP;

public class DecimalAverageAggregation
        extends SqlAggregationFunction
{
    public static final DecimalAverageAggregation DECIMAL_AVERAGE_AGGREGATION = new DecimalAverageAggregation();

    // Constant references for short/long decimal types for use in operations that only manipulate unscaled values
    private static final DecimalType LONG_DECIMAL_TYPE = DecimalType.createDecimalType(MAX_PRECISION, 0);
    private static final DecimalType SHORT_DECIMAL_TYPE = DecimalType.createDecimalType(MAX_SHORT_PRECISION, 0);

    private static final String NAME = "avg";
    private static final MethodHandle SHORT_DECIMAL_INPUT_FUNCTION = methodHandle(DecimalAverageAggregation.class, "inputShortDecimal", LongDecimalWithOverflowAndLongState.class, Block.class, int.class);
    private static final MethodHandle LONG_DECIMAL_INPUT_FUNCTION = methodHandle(DecimalAverageAggregation.class, "inputLongDecimal", LongDecimalWithOverflowAndLongState.class, Block.class, int.class);

    private static final MethodHandle SHORT_DECIMAL_OUTPUT_FUNCTION = methodHandle(DecimalAverageAggregation.class, "outputShortDecimal", DecimalType.class, LongDecimalWithOverflowAndLongState.class, BlockBuilder.class);
    private static final MethodHandle LONG_DECIMAL_OUTPUT_FUNCTION = methodHandle(DecimalAverageAggregation.class, "outputLongDecimal", DecimalType.class, LongDecimalWithOverflowAndLongState.class, BlockBuilder.class);

    private static final MethodHandle COMBINE_FUNCTION = methodHandle(DecimalAverageAggregation.class, "combine", LongDecimalWithOverflowAndLongState.class, LongDecimalWithOverflowAndLongState.class);

    private static final BigInteger TWO = new BigInteger("2");
    private static final BigInteger OVERFLOW_MULTIPLIER = TWO.shiftLeft(UNSCALED_DECIMAL_128_SLICE_LENGTH * 8 - 2);

    public DecimalAverageAggregation()
    {
        super(
                new FunctionMetadata(
                        new Signature(
                                NAME,
                                new TypeSignature("decimal", typeVariable("p"), typeVariable("s")),
                                ImmutableList.of(new TypeSignature("decimal", typeVariable("p"), typeVariable("s")))),
                        true,
                        ImmutableList.of(new FunctionArgumentDefinition(false)),
                        false,
                        true,
                        "Calculates the average value",
                        AGGREGATE),
                new AggregationFunctionMetadata(
                        false,
                        VARBINARY.getTypeSignature()));
    }

    @Override
    public InternalAggregationFunction specialize(FunctionBinding functionBinding)
    {
        Type type = getOnlyElement(functionBinding.getBoundSignature().getArgumentTypes());
        return generateAggregation(type);
    }

    private static InternalAggregationFunction generateAggregation(Type type)
    {
        checkArgument(type instanceof DecimalType, "type must be Decimal");
        DynamicClassLoader classLoader = new DynamicClassLoader(DecimalAverageAggregation.class.getClassLoader());
        List<Type> inputTypes = ImmutableList.of(type);
        MethodHandle inputFunction;
        MethodHandle outputFunction;
        Class<? extends AccumulatorState> stateInterface = LongDecimalWithOverflowAndLongState.class;
        AccumulatorStateSerializer<?> stateSerializer = new LongDecimalWithOverflowAndLongStateSerializer();

        if (((DecimalType) type).isShort()) {
            inputFunction = SHORT_DECIMAL_INPUT_FUNCTION;
            outputFunction = SHORT_DECIMAL_OUTPUT_FUNCTION;
        }
        else {
            inputFunction = LONG_DECIMAL_INPUT_FUNCTION;
            outputFunction = LONG_DECIMAL_OUTPUT_FUNCTION;
        }
        outputFunction = outputFunction.bindTo(type);

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, type.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(type),
                inputFunction,
                Optional.empty(),
                COMBINE_FUNCTION,
                outputFunction,
                ImmutableList.of(new AccumulatorStateDescriptor(
                        stateInterface,
                        stateSerializer,
                        new LongDecimalWithOverflowAndLongStateFactory())),
                type);

        Type intermediateType = stateSerializer.getSerializedType();
        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, ImmutableList.of(intermediateType), type, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type type)
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(BLOCK_INPUT_CHANNEL, type), new ParameterMetadata(BLOCK_INDEX));
    }

    public static void inputShortDecimal(LongDecimalWithOverflowAndLongState state, Block block, int position)
    {
        state.addLong(1); // row counter

        Slice sum = state.getLongDecimal();
        if (sum == null) {
            sum = UnscaledDecimal128Arithmetic.unscaledDecimal();
            state.setLongDecimal(sum);
        }
        long overflow = UnscaledDecimal128Arithmetic.addWithOverflow(sum, UnscaledDecimal128Arithmetic.unscaledDecimal(SHORT_DECIMAL_TYPE.getLong(block, position)), sum);
        state.addOverflow(overflow);
    }

    public static void inputLongDecimal(LongDecimalWithOverflowAndLongState state, Block block, int position)
    {
        state.addLong(1); // row counter

        Slice sum = state.getLongDecimal();
        if (sum == null) {
            sum = UnscaledDecimal128Arithmetic.unscaledDecimal();
            state.setLongDecimal(sum);
        }
        long overflow = UnscaledDecimal128Arithmetic.addWithOverflow(sum, LONG_DECIMAL_TYPE.getSlice(block, position), sum);
        state.addOverflow(overflow);
    }

    public static void combine(LongDecimalWithOverflowAndLongState state, LongDecimalWithOverflowAndLongState otherState)
    {
        state.addLong(otherState.getLong()); // row counter

        long overflow = otherState.getOverflow();

        Slice sum = state.getLongDecimal();
        if (sum == null) {
            state.setLongDecimal(otherState.getLongDecimal());
        }
        else {
            overflow += UnscaledDecimal128Arithmetic.addWithOverflow(sum, otherState.getLongDecimal(), sum);
        }

        state.addOverflow(overflow);
    }

    public static void outputShortDecimal(DecimalType type, LongDecimalWithOverflowAndLongState state, BlockBuilder out)
    {
        if (state.getLong() == 0) {
            out.appendNull();
        }
        else {
            writeShortDecimal(out, average(state, type).unscaledValue().longValueExact());
        }
    }

    public static void outputLongDecimal(DecimalType type, LongDecimalWithOverflowAndLongState state, BlockBuilder out)
    {
        if (state.getLong() == 0) {
            out.appendNull();
        }
        else {
            writeBigDecimal(type, out, average(state, type));
        }
    }

    @VisibleForTesting
    public static BigDecimal average(LongDecimalWithOverflowAndLongState state, DecimalType type)
    {
        BigDecimal sum = new BigDecimal(Decimals.decodeUnscaledValue(state.getLongDecimal()), type.getScale());
        BigDecimal count = BigDecimal.valueOf(state.getLong());

        long overflow = state.getOverflow();
        if (overflow != 0) {
            sum = sum.add(new BigDecimal(OVERFLOW_MULTIPLIER.multiply(BigInteger.valueOf(overflow))));
        }
        return sum.divide(count, type.getScale(), ROUND_HALF_UP);
    }
}
