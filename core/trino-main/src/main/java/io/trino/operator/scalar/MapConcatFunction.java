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

import com.google.common.collect.ImmutableList;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.BoundSignature;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.FunctionNullability;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlScalarFunction;
import io.trino.operator.aggregation.TypedSet;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
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

import static io.trino.metadata.FunctionKind.SCALAR;
import static io.trino.metadata.Signature.typeVariable;
import static io.trino.operator.aggregation.TypedSet.createDistinctTypedSet;
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
        super(new FunctionMetadata(
                new Signature(
                        FUNCTION_NAME,
                        ImmutableList.of(typeVariable("K"), typeVariable("V")),
                        ImmutableList.of(),
                        mapType(new TypeSignature("K"), new TypeSignature("V")),
                        ImmutableList.of(mapType(new TypeSignature("K"), new TypeSignature("V"))),
                        true),
                new FunctionNullability(false, ImmutableList.of(false)),
                false,
                true,
                DESCRIPTION,
                SCALAR));
        this.blockTypeOperators = requireNonNull(blockTypeOperators, "blockTypeOperators is null");
    }

    @Override
    protected ScalarFunctionImplementation specialize(BoundSignature boundSignature)
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

        return new ChoicesScalarFunctionImplementation(
                boundSignature,
                FAIL_ON_NULL,
                nCopies(boundSignature.getArity(), NEVER_NULL),
                methodHandleAndConstructor.getMethodHandle(),
                Optional.of(methodHandleAndConstructor.getConstructor()));
    }

    @UsedByGeneratedCode
    public static Object createMapState(MapType mapType)
    {
        return new PageBuilder(ImmutableList.of(mapType));
    }

    @UsedByGeneratedCode
    public static Block mapConcat(MapType mapType, BlockPositionIsDistinctFrom keysDistinctOperator, BlockPositionHashCode keyHashCode, Object state, Block[] maps)
    {
        int entries = 0;
        int lastMapIndex = maps.length - 1;
        int firstMapIndex = lastMapIndex;
        for (int i = 0; i < maps.length; i++) {
            entries += maps[i].getPositionCount();
            if (maps[i].getPositionCount() > 0) {
                lastMapIndex = i;
                firstMapIndex = min(firstMapIndex, i);
            }
        }
        if (lastMapIndex == firstMapIndex) {
            return maps[lastMapIndex];
        }

        PageBuilder pageBuilder = (PageBuilder) state;
        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }

        // TODO: we should move TypedSet into user state as well
        Type keyType = mapType.getKeyType();
        Type valueType = mapType.getValueType();
        TypedSet typedSet = createDistinctTypedSet(keyType, keysDistinctOperator, keyHashCode, entries / 2, FUNCTION_NAME);
        BlockBuilder mapBlockBuilder = pageBuilder.getBlockBuilder(0);
        BlockBuilder blockBuilder = mapBlockBuilder.beginBlockEntry();

        // the last map
        Block map = maps[lastMapIndex];
        for (int i = 0; i < map.getPositionCount(); i += 2) {
            typedSet.add(map, i);
            keyType.appendTo(map, i, blockBuilder);
            valueType.appendTo(map, i + 1, blockBuilder);
        }
        // the map between the last and the first
        for (int idx = lastMapIndex - 1; idx > firstMapIndex; idx--) {
            map = maps[idx];
            for (int i = 0; i < map.getPositionCount(); i += 2) {
                if (typedSet.add(map, i)) {
                    keyType.appendTo(map, i, blockBuilder);
                    valueType.appendTo(map, i + 1, blockBuilder);
                }
            }
        }
        // the first map
        map = maps[firstMapIndex];
        for (int i = 0; i < map.getPositionCount(); i += 2) {
            if (!typedSet.contains(map, i)) {
                keyType.appendTo(map, i, blockBuilder);
                valueType.appendTo(map, i + 1, blockBuilder);
            }
        }

        mapBlockBuilder.closeEntry();
        pageBuilder.declarePosition();
        return mapType.getObject(mapBlockBuilder, mapBlockBuilder.getPositionCount() - 1);
    }
}
