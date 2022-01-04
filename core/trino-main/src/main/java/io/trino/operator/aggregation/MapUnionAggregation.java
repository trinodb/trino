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
import io.trino.operator.aggregation.state.KeyValuePairStateSerializer;
import io.trino.operator.aggregation.state.KeyValuePairsState;
import io.trino.operator.aggregation.state.KeyValuePairsStateFactory;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.type.BlockTypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionEqual;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.Optional;

import static io.trino.metadata.FunctionKind.AGGREGATE;
import static io.trino.metadata.Signature.comparableTypeParameter;
import static io.trino.metadata.Signature.typeVariable;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.AggregationParameterKind.INPUT_CHANNEL;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.AggregationParameterKind.STATE;
import static io.trino.operator.aggregation.AggregationFunctionAdapter.normalizeInputMethod;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.util.Reflection.methodHandle;
import static java.util.Objects.requireNonNull;

public class MapUnionAggregation
        extends SqlAggregationFunction
{
    public static final String NAME = "map_union";
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(MapUnionAggregation.class, "output", KeyValuePairsState.class, BlockBuilder.class);
    private static final MethodHandle INPUT_FUNCTION = methodHandle(MapUnionAggregation.class,
            "input",
            Type.class,
            BlockPositionEqual.class,
            BlockPositionHashCode.class,
            Type.class,
            KeyValuePairsState.class,
            Block.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(MapUnionAggregation.class, "combine", KeyValuePairsState.class, KeyValuePairsState.class);

    private final BlockTypeOperators blockTypeOperators;

    public MapUnionAggregation(BlockTypeOperators blockTypeOperators)
    {
        super(
                new FunctionMetadata(
                        new Signature(
                                NAME,
                                ImmutableList.of(comparableTypeParameter("K"), typeVariable("V")),
                                ImmutableList.of(),
                                mapType(new TypeSignature("K"), new TypeSignature("V")),
                                ImmutableList.of(mapType(new TypeSignature("K"), new TypeSignature("V"))),
                                false),
                        new FunctionNullability(true, ImmutableList.of(false)),
                        false,
                        true,
                        "Aggregate all the maps into a single map",
                        AGGREGATE),
                new AggregationFunctionMetadata(
                        false,
                        mapType(new TypeSignature("K"), new TypeSignature("V"))));
        this.blockTypeOperators = requireNonNull(blockTypeOperators, "blockTypeOperators is null");
    }

    @Override
    public AggregationMetadata specialize(BoundSignature boundSignature)
    {
        MapType outputType = (MapType) boundSignature.getReturnType();
        Type keyType = outputType.getKeyType();
        BlockPositionEqual keyEqual = blockTypeOperators.getEqualOperator(keyType);
        BlockPositionHashCode keyHashCode = blockTypeOperators.getHashCodeOperator(keyType);

        Type valueType = outputType.getValueType();

        KeyValuePairStateSerializer stateSerializer = new KeyValuePairStateSerializer(outputType, keyEqual, keyHashCode);

        MethodHandle inputFunction = MethodHandles.insertArguments(INPUT_FUNCTION, 0, keyType, keyEqual, keyHashCode, valueType);
        inputFunction = normalizeInputMethod(inputFunction, boundSignature, STATE, INPUT_CHANNEL);

        return new AggregationMetadata(
                inputFunction,
                Optional.empty(),
                Optional.of(COMBINE_FUNCTION),
                OUTPUT_FUNCTION,
                ImmutableList.of(new AccumulatorStateDescriptor<>(
                        KeyValuePairsState.class,
                        stateSerializer,
                        new KeyValuePairsStateFactory(keyType, valueType))));
    }

    public static void input(
            Type keyType,
            BlockPositionEqual keyEqual,
            BlockPositionHashCode keyHashCode,
            Type valueType,
            KeyValuePairsState state,
            Block value)
    {
        KeyValuePairs pairs = state.get();
        if (pairs == null) {
            pairs = new KeyValuePairs(keyType, keyEqual, keyHashCode, valueType);
            state.set(pairs);
        }

        long startSize = pairs.estimatedInMemorySize();
        for (int i = 0; i < value.getPositionCount(); i += 2) {
            pairs.add(value, value, i, i + 1);
        }
        state.addMemoryUsage(pairs.estimatedInMemorySize() - startSize);
    }

    public static void combine(KeyValuePairsState state, KeyValuePairsState otherState)
    {
        MapAggregationFunction.combine(state, otherState);
    }

    public static void output(KeyValuePairsState state, BlockBuilder out)
    {
        MapAggregationFunction.output(state, out);
    }
}
