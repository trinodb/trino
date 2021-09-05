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
import io.trino.metadata.AggregationFunctionMetadata;
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

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metadata.FunctionKind.AGGREGATE;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static io.trino.operator.aggregation.AggregationUtils.generateAggregationName;
import static io.trino.spi.StandardErrorCode.EXCEEDED_FUNCTION_MEMORY_LIMIT;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TypeSignature.arrayType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.String.format;

public class ListaggAggregationFunction
        extends SqlAggregationFunction
{
    public static final ListaggAggregationFunction LISTAGG = new ListaggAggregationFunction();
    public static final String NAME = "listagg";
    private static final MethodHandle INPUT_FUNCTION = methodHandle(ListaggAggregationFunction.class, "input", Type.class, ListaggAggregationState.class, Block.class, Slice.class, boolean.class, Slice.class, boolean.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(ListaggAggregationFunction.class, "combine", Type.class, ListaggAggregationState.class, ListaggAggregationState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(ListaggAggregationFunction.class, "output", Type.class, ListaggAggregationState.class, BlockBuilder.class);

    private static final int MAX_OUTPUT_LENGTH = DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
    private static final int MAX_OVERFLOW_FILLER_LENGTH = 65_536;

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
                new AggregationFunctionMetadata(
                        true,
                        VARCHAR.getTypeSignature(),
                        BOOLEAN.getTypeSignature(),
                        VARCHAR.getTypeSignature(),
                        BOOLEAN.getTypeSignature(),
                        arrayType(VARCHAR.getTypeSignature())));
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

    public static void input(Type type, ListaggAggregationState state, Block value, Slice separator, boolean overflowError, Slice overflowFiller, boolean showOverflowEntryCount, int position)
    {
        if (state.isEmpty()) {
            if (overflowFiller.length() > MAX_OVERFLOW_FILLER_LENGTH) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Overflow filler length %d exceeds maximum length %d", overflowFiller.length(), MAX_OVERFLOW_FILLER_LENGTH));
            }
            // Set the parameters of the LISTAGG command within the state so that
            // they can be used within the `output` function
            state.setSeparator(separator);
            state.setOverflowError(overflowError);
            state.setOverflowFiller(overflowFiller);
            state.setShowOverflowEntryCount(showOverflowEntryCount);
        }
        if (!value.isNull(position)) {
            state.add(value, position);
        }
    }

    public static void combine(Type type, ListaggAggregationState state, ListaggAggregationState otherState)
    {
        Slice previousSeparator = state.getSeparator();
        if (previousSeparator == null) {
            state.setSeparator(otherState.getSeparator());
            state.setOverflowError(otherState.isOverflowError());
            state.setOverflowFiller(otherState.getOverflowFiller());
            state.setShowOverflowEntryCount(otherState.showOverflowEntryCount());
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
        int separatorLength = separator.length();
        OutputContext context = new OutputContext();
        state.forEach((block, position) -> {
            int entryLength = block.getSliceLength(position);
            int spaceRequired = entryLength + (context.emittedEntryCount > 0 ? separatorLength : 0);

            if (context.outputLength + spaceRequired > maxOutputLength) {
                context.overflow = true;
                return false;
            }

            if (context.emittedEntryCount > 0) {
                out.writeBytes(separator, 0, separatorLength);
                context.outputLength += separatorLength;
            }

            block.writeBytesTo(position, 0, entryLength, out);
            context.outputLength += entryLength;
            context.emittedEntryCount++;

            return true;
        });

        if (context.overflow) {
            if (state.isOverflowError()) {
                throw new TrinoException(EXCEEDED_FUNCTION_MEMORY_LIMIT, format("Concatenated string has the length in bytes larger than the maximum output length %d", maxOutputLength));
            }

            if (context.emittedEntryCount > 0) {
                out.writeBytes(separator, 0, separatorLength);
            }
            out.writeBytes(state.getOverflowFiller(), 0, state.getOverflowFiller().length());

            if (state.showOverflowEntryCount()) {
                out.writeBytes(Slices.utf8Slice("("), 0, 1);
                Slice count = Slices.utf8Slice(Integer.toString(state.getEntryCount() - context.emittedEntryCount));
                out.writeBytes(count, 0, count.length());
                out.writeBytes(Slices.utf8Slice(")"), 0, 1);
            }
        }

        out.closeEntry();
    }

    private static class OutputContext
    {
        long outputLength;
        int emittedEntryCount;
        boolean overflow;
    }
}
