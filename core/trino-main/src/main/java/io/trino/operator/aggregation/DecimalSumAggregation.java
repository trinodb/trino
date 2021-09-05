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
import io.airlift.slice.Slice;
import io.trino.metadata.AggregationFunctionMetadata;
import io.trino.metadata.FunctionArgumentDefinition;
import io.trino.metadata.FunctionBinding;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlAggregationFunction;
import io.trino.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.trino.operator.aggregation.state.LongDecimalWithOverflowState;
import io.trino.operator.aggregation.state.LongDecimalWithOverflowStateFactory;
import io.trino.operator.aggregation.state.LongDecimalWithOverflowStateSerializer;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.UnscaledDecimal128Arithmetic;

import java.lang.invoke.MethodHandle;
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
import static io.trino.spi.type.TypeSignatureParameter.numericParameter;
import static io.trino.spi.type.TypeSignatureParameter.typeVariable;
import static io.trino.spi.type.UnscaledDecimal128Arithmetic.throwIfOverflows;
import static io.trino.spi.type.UnscaledDecimal128Arithmetic.throwOverflowException;
import static io.trino.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimal;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.util.Reflection.methodHandle;

public class DecimalSumAggregation
        extends SqlAggregationFunction
{
    // Constant references for short/long decimal types for use in operations that only manipulate unscaled values
    private static final DecimalType LONG_DECIMAL_TYPE = DecimalType.createDecimalType(MAX_PRECISION, 0);
    private static final DecimalType SHORT_DECIMAL_TYPE = DecimalType.createDecimalType(MAX_SHORT_PRECISION, 0);

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
                        true,
                        ImmutableList.of(new FunctionArgumentDefinition(false)),
                        false,
                        true,
                        "Calculates the sum over the input values",
                        AGGREGATE),
                new AggregationFunctionMetadata(
                        false,
                        VARBINARY.getTypeSignature()));
    }

    @Override
    public InternalAggregationFunction specialize(FunctionBinding functionBinding)
    {
        Type inputType = getOnlyElement(functionBinding.getBoundSignature().getArgumentTypes());
        Type outputType = functionBinding.getBoundSignature().getReturnType();
        return generateAggregation(inputType, outputType);
    }

    private static InternalAggregationFunction generateAggregation(Type inputType, Type outputType)
    {
        checkArgument(inputType instanceof DecimalType, "type must be Decimal");
        DynamicClassLoader classLoader = new DynamicClassLoader(DecimalSumAggregation.class.getClassLoader());
        List<Type> inputTypes = ImmutableList.of(inputType);
        MethodHandle inputFunction;
        Class<? extends AccumulatorState> stateInterface = LongDecimalWithOverflowState.class;
        AccumulatorStateSerializer<?> stateSerializer = new LongDecimalWithOverflowStateSerializer();

        if (((DecimalType) inputType).isShort()) {
            inputFunction = SHORT_DECIMAL_INPUT_FUNCTION;
        }
        else {
            inputFunction = LONG_DECIMAL_INPUT_FUNCTION;
        }

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, outputType.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(inputType),
                inputFunction,
                Optional.empty(),
                COMBINE_FUNCTION,
                LONG_DECIMAL_OUTPUT_FUNCTION,
                ImmutableList.of(new AccumulatorStateDescriptor(
                        stateInterface,
                        stateSerializer,
                        new LongDecimalWithOverflowStateFactory())),
                outputType);

        Type intermediateType = stateSerializer.getSerializedType();
        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, ImmutableList.of(intermediateType), outputType, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type type)
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(BLOCK_INPUT_CHANNEL, type), new ParameterMetadata(BLOCK_INDEX));
    }

    public static void inputShortDecimal(LongDecimalWithOverflowState state, Block block, int position)
    {
        Slice sum = state.getLongDecimal();
        if (sum == null) {
            sum = UnscaledDecimal128Arithmetic.unscaledDecimal();
            state.setLongDecimal(sum);
        }
        long overflow = UnscaledDecimal128Arithmetic.addWithOverflow(sum, unscaledDecimal(SHORT_DECIMAL_TYPE.getLong(block, position)), sum);
        state.addOverflow(overflow);
    }

    public static void inputLongDecimal(LongDecimalWithOverflowState state, Block block, int position)
    {
        Slice sum = state.getLongDecimal();
        if (sum == null) {
            sum = UnscaledDecimal128Arithmetic.unscaledDecimal();
            state.setLongDecimal(sum);
        }
        long overflow = UnscaledDecimal128Arithmetic.addWithOverflow(sum, LONG_DECIMAL_TYPE.getSlice(block, position), sum);
        state.addOverflow(overflow);
    }

    public static void combine(LongDecimalWithOverflowState state, LongDecimalWithOverflowState otherState)
    {
        Slice sum = state.getLongDecimal();
        long overflow = otherState.getOverflow();

        if (sum == null) {
            state.setLongDecimal(otherState.getLongDecimal());
        }
        else {
            overflow += UnscaledDecimal128Arithmetic.addWithOverflow(sum, otherState.getLongDecimal(), sum);
        }

        state.addOverflow(overflow);
    }

    public static void outputLongDecimal(LongDecimalWithOverflowState state, BlockBuilder out)
    {
        Slice decimal = state.getLongDecimal();
        if (decimal == null) {
            out.appendNull();
        }
        else {
            if (state.getOverflow() != 0) {
                throwOverflowException();
            }
            throwIfOverflows(decimal);
            LONG_DECIMAL_TYPE.writeSlice(out, decimal);
        }
    }
}
