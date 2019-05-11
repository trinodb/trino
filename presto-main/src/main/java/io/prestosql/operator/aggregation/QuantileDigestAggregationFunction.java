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
package io.prestosql.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.stats.QuantileDigest;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionMetadata;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlAggregationFunction;
import io.prestosql.operator.aggregation.state.QuantileDigestState;
import io.prestosql.operator.aggregation.state.QuantileDigestStateFactory;
import io.prestosql.operator.aggregation.state.QuantileDigestStateSerializer;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.QuantileDigestType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.metadata.FunctionKind.AGGREGATE;
import static io.prestosql.metadata.Signature.comparableTypeParameter;
import static io.prestosql.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static io.prestosql.operator.aggregation.AggregationUtils.generateAggregationName;
import static io.prestosql.operator.aggregation.FloatingPointBitsConverterUtil.doubleToSortableLong;
import static io.prestosql.operator.aggregation.FloatingPointBitsConverterUtil.floatToSortableInt;
import static io.prestosql.operator.scalar.QuantileDigestFunctions.DEFAULT_ACCURACY;
import static io.prestosql.operator.scalar.QuantileDigestFunctions.DEFAULT_WEIGHT;
import static io.prestosql.operator.scalar.QuantileDigestFunctions.verifyAccuracy;
import static io.prestosql.operator.scalar.QuantileDigestFunctions.verifyWeight;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.TypeSignature.parametricType;
import static io.prestosql.util.Reflection.methodHandle;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.insertArguments;

public final class QuantileDigestAggregationFunction
        extends SqlAggregationFunction
{
    public static final QuantileDigestAggregationFunction QDIGEST_AGG = new QuantileDigestAggregationFunction(new TypeSignature("V"));
    public static final QuantileDigestAggregationFunction QDIGEST_AGG_WITH_WEIGHT = new QuantileDigestAggregationFunction(new TypeSignature("V"), BIGINT.getTypeSignature());
    public static final QuantileDigestAggregationFunction QDIGEST_AGG_WITH_WEIGHT_AND_ERROR = new QuantileDigestAggregationFunction(new TypeSignature("V"), BIGINT.getTypeSignature(), DOUBLE.getTypeSignature());
    public static final String NAME = "qdigest_agg";

    private static final MethodHandle INPUT_DOUBLE = methodHandle(QuantileDigestAggregationFunction.class, "inputDouble", QuantileDigestState.class, double.class, long.class, double.class);
    private static final MethodHandle INPUT_REAL = methodHandle(QuantileDigestAggregationFunction.class, "inputReal", QuantileDigestState.class, long.class, long.class, double.class);
    private static final MethodHandle INPUT_BIGINT = methodHandle(QuantileDigestAggregationFunction.class, "inputBigint", QuantileDigestState.class, long.class, long.class, double.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(QuantileDigestAggregationFunction.class, "combineState", QuantileDigestState.class, QuantileDigestState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(QuantileDigestAggregationFunction.class, "evaluateFinal", QuantileDigestStateSerializer.class, QuantileDigestState.class, BlockBuilder.class);

    private QuantileDigestAggregationFunction(TypeSignature... typeSignatures)
    {
        super(new FunctionMetadata(
                new Signature(
                        NAME,
                        AGGREGATE,
                        ImmutableList.of(comparableTypeParameter("V")),
                        ImmutableList.of(),
                        parametricType("qdigest", new TypeSignature("V")),
                        ImmutableList.copyOf(typeSignatures),
                        false),
                false,
                true,
                "Returns a qdigest from the set of reals, bigints or doubles"));
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, Metadata metadata)
    {
        Type valueType = boundVariables.getTypeVariable("V");
        QuantileDigestType outputType = (QuantileDigestType) metadata.getParameterizedType(
                StandardTypes.QDIGEST,
                ImmutableList.of(TypeSignatureParameter.typeParameter(valueType.getTypeSignature())));
        return generateAggregation(valueType, outputType, arity);
    }

    private static InternalAggregationFunction generateAggregation(Type valueType, QuantileDigestType outputType, int arity)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(QuantileDigestAggregationFunction.class.getClassLoader());
        List<Type> inputTypes = getInputTypes(valueType, arity);
        QuantileDigestStateSerializer stateSerializer = new QuantileDigestStateSerializer(valueType);
        Type intermediateType = stateSerializer.getSerializedType();

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, outputType.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(inputTypes),
                getMethodHandle(valueType, arity),
                Optional.empty(),
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION.bindTo(stateSerializer),
                ImmutableList.of(new AccumulatorStateDescriptor(
                        QuantileDigestState.class,
                        stateSerializer,
                        new QuantileDigestStateFactory())),
                outputType);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, ImmutableList.of(intermediateType), outputType, true, true, factory);
    }

    private static List<Type> getInputTypes(Type valueType, int arity)
    {
        switch (arity) {
            case 1:
                // weight and accuracy unspecified
                return ImmutableList.of(valueType);
            case 2:
                // weight specified, accuracy unspecified
                return ImmutableList.of(valueType, BIGINT);
            case 3:
                // weight and accuracy specified
                return ImmutableList.of(valueType, BIGINT, DOUBLE);
            default:
                throw new IllegalArgumentException(format("Unsupported number of arguments: %s", arity));
        }
    }

    private static MethodHandle getMethodHandle(Type valueType, int arity)
    {
        final MethodHandle inputFunction;
        switch (valueType.getDisplayName()) {
            case StandardTypes.DOUBLE:
                inputFunction = INPUT_DOUBLE;
                break;
            case StandardTypes.REAL:
                inputFunction = INPUT_REAL;
                break;
            case StandardTypes.BIGINT:
                inputFunction = INPUT_BIGINT;
                break;
            default:
                throw new IllegalArgumentException(format("Unsupported type %s supplied", valueType.getDisplayName()));
        }

        switch (arity) {
            case 1:
                // weight and accuracy unspecified
                return insertArguments(inputFunction, 2, DEFAULT_WEIGHT, DEFAULT_ACCURACY);
            case 2:
                // weight specified, accuracy unspecified
                return insertArguments(inputFunction, 3, DEFAULT_ACCURACY);
            case 3:
                // weight and accuracy specified
                return inputFunction;
            default:
                throw new IllegalArgumentException(format("Unsupported number of arguments: %s", arity));
        }
    }

    private static List<ParameterMetadata> createInputParameterMetadata(List<Type> valueTypes)
    {
        return ImmutableList.<ParameterMetadata>builder()
                .add(new ParameterMetadata(STATE))
                .addAll(valueTypes.stream().map(valueType -> new ParameterMetadata(INPUT_CHANNEL, valueType)).collect(Collectors.toList()))
                .build();
    }

    public static void inputDouble(QuantileDigestState state, double value, long weight, double accuracy)
    {
        inputBigint(state, doubleToSortableLong(value), weight, accuracy);
    }

    public static void inputReal(QuantileDigestState state, long value, long weight, double accuracy)
    {
        inputBigint(state, floatToSortableInt(intBitsToFloat((int) value)), weight, accuracy);
    }

    public static void inputBigint(QuantileDigestState state, long value, long weight, double accuracy)
    {
        QuantileDigest qdigest = getOrCreateQuantileDigest(state, verifyAccuracy(accuracy));
        state.addMemoryUsage(-qdigest.estimatedInMemorySizeInBytes());
        qdigest.add(value, verifyWeight(weight));
        state.addMemoryUsage(qdigest.estimatedInMemorySizeInBytes());
    }

    private static QuantileDigest getOrCreateQuantileDigest(QuantileDigestState state, double accuracy)
    {
        QuantileDigest qdigest = state.getQuantileDigest();
        if (qdigest == null) {
            qdigest = new QuantileDigest(accuracy);
            state.setQuantileDigest(qdigest);
            state.addMemoryUsage(qdigest.estimatedInMemorySizeInBytes());
        }
        return qdigest;
    }

    public static void combineState(QuantileDigestState state, QuantileDigestState otherState)
    {
        QuantileDigest input = otherState.getQuantileDigest();

        QuantileDigest previous = state.getQuantileDigest();
        if (previous == null) {
            state.setQuantileDigest(input);
            state.addMemoryUsage(input.estimatedInMemorySizeInBytes());
        }
        else {
            state.addMemoryUsage(-previous.estimatedInMemorySizeInBytes());
            previous.merge(input);
            state.addMemoryUsage(previous.estimatedInMemorySizeInBytes());
        }
    }

    public static void evaluateFinal(QuantileDigestStateSerializer serializer, QuantileDigestState state, BlockBuilder out)
    {
        serializer.serialize(state, out);
    }
}
