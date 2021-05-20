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
package io.trino.operator.aggregation.listagg;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.metadata.FunctionArgumentDefinition;
import io.trino.metadata.FunctionBinding;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlAggregationFunction;
import io.trino.operator.aggregation.AccumulatorCompiler;
import io.trino.operator.aggregation.AggregationMetadata;
import io.trino.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata;
import io.trino.operator.aggregation.GenericAccumulatorFactoryBinder;
import io.trino.operator.aggregation.InternalAggregationFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metadata.FunctionKind.AGGREGATE;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static io.trino.operator.aggregation.AggregationUtils.generateAggregationName;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.QUERY_REJECTED;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.util.Reflection.methodHandle;

public class ListaggAggregationFunction
        extends SqlAggregationFunction
{
    public static final ListaggAggregationFunction LISTAGG = new ListaggAggregationFunction();
    public static final String NAME = "listagg";
    private static final MethodHandle INPUT_FUNCTION = methodHandle(ListaggAggregationFunction.class, "input",
            Type.class, ListaggAggregationState.class, Block.class, Slice.class, boolean.class, Slice.class, boolean.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(ListaggAggregationFunction.class, "combine",
            Type.class, ListaggAggregationState.class, ListaggAggregationState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(ListaggAggregationFunction.class, "output",
            Type.class, ListaggAggregationState.class, BlockBuilder.class);

    private static final int MAX_OUTPUT_LENGTH = DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
    private static final int MAX_TRUNCATION_FILLER_LENGTH = 65_536;

    private ListaggAggregationFunction()
    {
        super(
                new FunctionMetadata(
                        new Signature(
                                NAME,
                                ImmutableList.of(),
                                ImmutableList.of(),
                                VARCHAR.getTypeSignature(),
                                ImmutableList.of(
                                        new TypeSignature(StandardTypes.VARCHAR, TypeSignatureParameter.typeVariable("v")),
                                        new TypeSignature(StandardTypes.VARCHAR, TypeSignatureParameter.typeVariable("d")),
                                        BOOLEAN.getTypeSignature(),
                                        new TypeSignature(StandardTypes.VARCHAR, TypeSignatureParameter.typeVariable("f")),
                                        BOOLEAN.getTypeSignature()),
                                false),
                        true,
                        ImmutableList.of(
                                new FunctionArgumentDefinition(true),
                                new FunctionArgumentDefinition(false),
                                new FunctionArgumentDefinition(false),
                                new FunctionArgumentDefinition(false),
                                new FunctionArgumentDefinition(false)),
                        false,
                        true,
                        "concatenates the input values with the specified separator",
                        AGGREGATE),
                true,
                true);
    }

    @Override
    public List<TypeSignature> getIntermediateTypes(FunctionBinding functionBinding)
    {
        return ImmutableList.of(new ListaggAggregationStateSerializer(VARCHAR).getSerializedType().getTypeSignature());
    }

    @Override
    public InternalAggregationFunction specialize(FunctionBinding functionBinding)
    {
        return generateAggregation(VARCHAR);
    }

    private static InternalAggregationFunction generateAggregation(Type type)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(ListaggAggregationFunction.class.getClassLoader());

        AccumulatorStateSerializer<?> stateSerializer = new ListaggAggregationStateSerializer(type);
        AccumulatorStateFactory<?> stateFactory = new ListaggAggregationStateFactory(type);

        List<Type> inputTypes = ImmutableList.of(VARCHAR, VARCHAR, BOOLEAN, VARCHAR, BOOLEAN);
        Type outputType = VARCHAR;
        Type intermediateType = stateSerializer.getSerializedType();
        List<ParameterMetadata> inputParameterMetadata = ImmutableList.of(
                new ParameterMetadata(STATE),
                new ParameterMetadata(NULLABLE_BLOCK_INPUT_CHANNEL, type),
                new ParameterMetadata(INPUT_CHANNEL, VARCHAR),
                new ParameterMetadata(INPUT_CHANNEL, BOOLEAN),
                new ParameterMetadata(INPUT_CHANNEL, VARCHAR),
                new ParameterMetadata(INPUT_CHANNEL, BOOLEAN),
                new ParameterMetadata(BLOCK_INDEX));

        MethodHandle inputFunction = INPUT_FUNCTION.bindTo(type);
        MethodHandle combineFunction = COMBINE_FUNCTION.bindTo(type);
        MethodHandle outputFunction = OUTPUT_FUNCTION.bindTo(type);
        Class<? extends AccumulatorState> stateInterface = ListaggAggregationState.class;

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, type.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                inputParameterMetadata,
                inputFunction,
                Optional.empty(),
                combineFunction,
                outputFunction,
                ImmutableList.of(new AccumulatorStateDescriptor(
                        stateInterface,
                        stateSerializer,
                        stateFactory)),
                outputType);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, ImmutableList.of(intermediateType), outputType, factory);
    }

    public static void input(Type type, ListaggAggregationState state, Block value, Slice separator, boolean overflowError, Slice overflowTruncationFiller, boolean overflowTruncationCountIndication, int position)
    {
        if (state.isEmpty()) {
            if (overflowTruncationFiller.length() > MAX_TRUNCATION_FILLER_LENGTH) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "truncation filler maximum length exceeded");
            }
            // Set the parameters of the LISTAGG command within the state so that
            // they can be used within the `output` function
            state.setSeparator(separator);
            state.setOverflowError(overflowError);
            state.setOverflowTruncationFiller(overflowTruncationFiller);
            state.setOverflowTruncationCountIndication(overflowTruncationCountIndication);
        }
        state.add(value, position);
    }

    public static void combine(Type type, ListaggAggregationState state, ListaggAggregationState otherState)
    {
        Slice previousSeparator = state.getSeparator();
        if (previousSeparator == null) {
            state.setSeparator(otherState.getSeparator());
            state.setOverflowError(otherState.isOverflowError());
            state.setOverflowTruncationFiller(otherState.getOverflowTruncationFiller());
            state.setOverflowTruncationCountIndication(otherState.isOverflowTruncationCountIndication());
        }

        state.merge(otherState);
    }

    public static void output(Type type, ListaggAggregationState state, BlockBuilder out)
    {
        if (state.isEmpty()) {
            out.appendNull();
        }
        else {
            outputState(state, out, MAX_OUTPUT_LENGTH);
        }
    }

    @VisibleForTesting
    protected static void outputState(ListaggAggregationState state, BlockBuilder out, int maxOutputLength)
    {
        Slice separator = state.getSeparator();
        AtomicLong aggregationLength = new AtomicLong(0L);
        AtomicInteger nonNullValuesCount = new AtomicInteger(0);
        state.forEach((block, position) -> {
            if (!block.isNull(position)) {
                if (aggregationLength.get() > 0) {
                    aggregationLength.addAndGet(separator.length());
                }
                aggregationLength.addAndGet(block.getSliceLength(position));
                nonNullValuesCount.incrementAndGet();
            }
        });
        if (nonNullValuesCount.get() == 0) {
            out.appendNull();
        }
        else {
            if (aggregationLength.get() > maxOutputLength) {
                if (state.isOverflowError()) {
                    throw new TrinoException(QUERY_REJECTED, "Concatenated string is too large");
                }
                else {
                    outputTruncatedState(state, out, nonNullValuesCount.get(), maxOutputLength);
                }
            }
            else {
                AtomicBoolean elementWritten = new AtomicBoolean(false);
                state.forEach((block, position) -> {
                    if (!block.isNull(position)) {
                        if (elementWritten.get()) {
                            out.writeBytes(separator, 0, separator.length());
                        }
                        block.writeBytesTo(position, 0, block.getSliceLength(position), out);
                        if (!elementWritten.get()) {
                            elementWritten.set(true);
                        }
                    }
                });
            }
            out.closeEntry();
        }
    }

    private static void outputTruncatedState(ListaggAggregationState state, BlockBuilder out, int nonNullValuesCount, int maxOutputLength)
    {
        Slice separator = state.getSeparator();
        Slice overflowTruncationFiller = state.getOverflowTruncationFiller();
        AtomicInteger truncatedAggregationLength = new AtomicInteger(0);
        AtomicInteger nonNullValuesWritten = new AtomicInteger(0);

        int overflowTruncationFillerLength = overflowTruncationFiller.length();
        int maxOutputLengthWithoutTruncationFilling = state.isOverflowTruncationCountIndication() ?
                maxOutputLength - overflowTruncationFillerLength - 2 /*()*/ - nonNullValuesCount :
                maxOutputLength - overflowTruncationFillerLength;

        int separatorLength = separator.length();
        AtomicBoolean isMaxOutputLengthReached = new AtomicBoolean(false);
        state.forEach((block, position) -> {
            if (!isMaxOutputLengthReached.get() && !block.isNull(position)) {
                if (nonNullValuesWritten.get() > 0) {
                    if (truncatedAggregationLength.get() + separatorLength <= maxOutputLengthWithoutTruncationFilling) {
                        out.writeBytes(separator, 0, separatorLength);
                        truncatedAggregationLength.addAndGet(separatorLength);
                    }
                    else {
                        // write the separator only if it fits in full length within the max output length
                        int truncatedAggregationWithTruncationFillingLength = computeLengthWithTruncationFilling(truncatedAggregationLength.get(),
                                overflowTruncationFillerLength,
                                state.isOverflowTruncationCountIndication() ? Optional.of(nonNullValuesCount - nonNullValuesWritten.get()) : Optional.empty());
                        if (truncatedAggregationWithTruncationFillingLength + separatorLength <= maxOutputLength) {
                            out.writeBytes(separator, 0, separatorLength);
                            truncatedAggregationLength.addAndGet(separatorLength);
                        }
                        else {
                            outputOverflowTruncationFiller(overflowTruncationFiller,
                                    state.isOverflowTruncationCountIndication() ? Optional.of(nonNullValuesCount - nonNullValuesWritten.get()) : Optional.empty(),
                                    out);
                            isMaxOutputLengthReached.set(true);
                        }
                    }
                }
                if (!isMaxOutputLengthReached.get()) {
                    int sliceLength = block.getSliceLength(position);
                    if (truncatedAggregationLength.get() + sliceLength <= maxOutputLengthWithoutTruncationFilling) {
                        block.writeBytesTo(position, 0, sliceLength, out);
                        nonNullValuesWritten.incrementAndGet();
                        truncatedAggregationLength.addAndGet(sliceLength);
                    }
                    else {
                        int eventualTruncatedAggregationWithTruncationFillingLength = computeLengthWithTruncationFilling(truncatedAggregationLength.get(),
                                overflowTruncationFillerLength,
                                state.isOverflowTruncationCountIndication() ? Optional.of(nonNullValuesCount - nonNullValuesWritten.get() - 1 /* current value */) : Optional.empty());
                        if (eventualTruncatedAggregationWithTruncationFillingLength < maxOutputLength) {
                            int subSliceLength = Math.min(maxOutputLength - eventualTruncatedAggregationWithTruncationFillingLength, sliceLength);
                            block.writeBytesTo(position, 0, subSliceLength, out);
                            nonNullValuesWritten.incrementAndGet();
                            truncatedAggregationLength.addAndGet(subSliceLength);
                        }

                        int truncatedAggregationWithTruncationFillingLength = computeLengthWithTruncationFilling(truncatedAggregationLength.get(),
                                overflowTruncationFillerLength,
                                state.isOverflowTruncationCountIndication() ? Optional.of(nonNullValuesCount - nonNullValuesWritten.get()) : Optional.empty());
                        if (truncatedAggregationWithTruncationFillingLength == maxOutputLength) {
                            outputOverflowTruncationFiller(overflowTruncationFiller,
                                    state.isOverflowTruncationCountIndication() ? Optional.of(nonNullValuesCount - nonNullValuesWritten.get()) : Optional.empty(),
                                    out);
                            isMaxOutputLengthReached.set(true);
                        }
                    }
                }
            }
        });
    }

    private static int computeLengthWithTruncationFilling(int length, int overflowTruncationFillerLength, Optional<Integer> omittedValuesCount)
    {
        int lengthWithTruncationFilling = length + overflowTruncationFillerLength;
        if (omittedValuesCount.isPresent()) {
            lengthWithTruncationFilling += 2 /*()*/ + String.valueOf(omittedValuesCount.get()).length();
        }
        return lengthWithTruncationFilling;
    }

    private static void outputOverflowTruncationFiller(Slice overflowTruncationFiller, Optional<Integer> omittedValuesCount, BlockBuilder out)
    {
        out.writeBytes(overflowTruncationFiller, 0, overflowTruncationFiller.length());
        if (omittedValuesCount.isPresent()) {
            Slice overflowTruncationCountIndication = Slices.utf8Slice("(" + omittedValuesCount.get() + ")");
            out.writeBytes(overflowTruncationCountIndication, 0, overflowTruncationCountIndication.length());
        }
    }
}
