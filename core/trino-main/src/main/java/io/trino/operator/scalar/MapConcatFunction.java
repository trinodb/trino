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
package io.trino.operator.scalar;

import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BufferedMapValueBuilder;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.gen.VarArgsToArrayAdapterGenerator.MethodHandleAndConstructor;
import io.trino.type.BlockTypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;
import io.trino.type.BlockTypeOperators.BlockPositionIsDistinctFrom;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.sql.gen.VarArgsToArrayAdapterGenerator.generateVarArgsToArrayAdapter;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.Math.min;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public final class MapConcatFunction
        extends SqlScalarFunction
{
    private static final String FUNCTION_NAME = "map_concat";
    private static final String DESCRIPTION = "Concatenates given maps";

    private static final MethodHandle USER_STATE_FACTORY = methodHandle(MapConcatFunction.class, "createMapState", MapType.class);
    private static final MethodHandle METHOD_HANDLE = methodHandle(
            MapConcatFunction.class,
            "mapConcat",
            MapType.class,
            BlockPositionIsDistinctFrom.class,
            BlockPositionHashCode.class,
            Object.class,
            Block[].class);

    private final BlockTypeOperators blockTypeOperators;

    public MapConcatFunction(BlockTypeOperators blockTypeOperators)
    {
        super(FunctionMetadata.scalarBuilder()
                .signature(Signature.builder()
                        .name(FUNCTION_NAME)
                        .typeVariable("K")
                        .typeVariable("V")
                        .returnType(mapType(new TypeSignature("K"), new TypeSignature("V")))
                        .argumentType(mapType(new TypeSignature("K"), new TypeSignature("V")))
                        .variableArity()
                        .build())
                .description(DESCRIPTION)
                .build());
        this.blockTypeOperators = requireNonNull(blockTypeOperators, "blockTypeOperators is null");
    }

    @Override
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        if (boundSignature.getArity() < 2) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "There must be two or more concatenation arguments to " + FUNCTION_NAME);
        }

        MapType mapType = (MapType) boundSignature.getReturnType();
        Type keyType = mapType.getKeyType();
        BlockPositionIsDistinctFrom keysDistinctOperator = blockTypeOperators.getDistinctFromOperator(keyType);
        BlockPositionHashCode keyHashCode = blockTypeOperators.getHashCodeOperator(keyType);

        MethodHandleAndConstructor methodHandleAndConstructor = generateVarArgsToArrayAdapter(
                Block.class,
                Block.class,
                boundSignature.getArity(),
                MethodHandles.insertArguments(METHOD_HANDLE, 0, mapType, keysDistinctOperator, keyHashCode),
                USER_STATE_FACTORY.bindTo(mapType));

        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                nCopies(boundSignature.getArity(), NEVER_NULL),
                methodHandleAndConstructor.getMethodHandle(),
                Optional.of(methodHandleAndConstructor.getConstructor()));
    }

    @UsedByGeneratedCode
    public static Object createMapState(MapType mapType)
    {
        return BufferedMapValueBuilder.createBuffered(mapType);
    }

    @UsedByGeneratedCode
    public static Block mapConcat(MapType mapType, BlockPositionIsDistinctFrom keysDistinctOperator, BlockPositionHashCode keyHashCode, Object state, Block[] maps)
    {
        int maxEntries = 0;
        int lastMapIndex = maps.length - 1;
        int firstMapIndex = lastMapIndex;
        for (int i = 0; i < maps.length; i++) {
            maxEntries += maps[i].getPositionCount() / 2;
            if (maps[i].getPositionCount() > 0) {
                lastMapIndex = i;
                firstMapIndex = min(firstMapIndex, i);
            }
        }
        if (lastMapIndex == firstMapIndex) {
            return maps[lastMapIndex];
        }
        int last = lastMapIndex;
        int first = firstMapIndex;

        BufferedMapValueBuilder mapValueBuilder = (BufferedMapValueBuilder) state;

        Type keyType = mapType.getKeyType();
        Type valueType = mapType.getValueType();
        BlockSet set = new BlockSet(keyType, keysDistinctOperator, keyHashCode, maxEntries);
        return mapValueBuilder.build(maxEntries, (keyBuilder, valueBuilder) -> {
            // the last map
            Block map = maps[last];
            for (int i = 0; i < map.getPositionCount(); i += 2) {
                set.add(map, i);
                keyType.appendTo(map, i, keyBuilder);
                valueType.appendTo(map, i + 1, valueBuilder);
            }
            // the map between the last and the first
            for (int idx = last - 1; idx > first; idx--) {
                map = maps[idx];
                for (int i = 0; i < map.getPositionCount(); i += 2) {
                    if (set.add(map, i)) {
                        keyType.appendTo(map, i, keyBuilder);
                        valueType.appendTo(map, i + 1, valueBuilder);
                    }
                }
            }
            // the first map
            map = maps[first];
            for (int i = 0; i < map.getPositionCount(); i += 2) {
                if (!set.contains(map, i)) {
                    keyType.appendTo(map, i, keyBuilder);
                    valueType.appendTo(map, i + 1, valueBuilder);
                }
            }
        });
    }
}
