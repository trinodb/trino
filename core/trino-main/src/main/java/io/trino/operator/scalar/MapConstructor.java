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
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BufferedMapValueBuilder;
import io.trino.spi.block.DuplicateMapKeyException;
import io.trino.spi.block.SqlMap;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.block.MapHashTables.HashBuildMode.STRICT_NOT_DISTINCT_FROM;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.type.TypeSignature.arrayType;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.util.Failures.checkCondition;
import static io.trino.util.Failures.internalError;
import static io.trino.util.Reflection.constructorMethodHandle;
import static io.trino.util.Reflection.methodHandle;

public final class MapConstructor
        extends SqlScalarFunction
{
    public static final MapConstructor MAP_CONSTRUCTOR = new MapConstructor();

    private static final MethodHandle METHOD_HANDLE = methodHandle(
            MapConstructor.class,
            "createMap",
            MapType.class,
            MethodHandle.class,
            State.class,
            ConnectorSession.class,
            Block.class,
            Block.class);

    private static final String DESCRIPTION = "Constructs a map from the given key/value arrays";

    public MapConstructor()
    {
        super(FunctionMetadata.scalarBuilder("map")
                .signature(Signature.builder()
                        .comparableTypeParameter("K")
                        .typeVariable("V")
                        .returnType(mapType(new TypeSignature("K"), new TypeSignature("V")))
                        .argumentType(arrayType(new TypeSignature("K")))
                        .argumentType(arrayType(new TypeSignature("V")))
                        .build())
                .description(DESCRIPTION)
                .build());
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies()
    {
        return FunctionDependencyDeclaration.builder()
                .addOperatorSignature(HASH_CODE, ImmutableList.of(new TypeSignature("K")))
                .addOperatorSignature(EQUAL, ImmutableList.of(new TypeSignature("K"), new TypeSignature("K")))
                .addOperatorSignature(INDETERMINATE, ImmutableList.of(new TypeSignature("K")))
                .build();
    }

    @Override
    public SpecializedSqlScalarFunction specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        MapType mapType = (MapType) boundSignature.getReturnType();
        MethodHandle keyIndeterminate = functionDependencies.getOperatorImplementation(
                INDETERMINATE,
                ImmutableList.of(mapType.getKeyType()),
                simpleConvention(FAIL_ON_NULL, BLOCK_POSITION)).getMethodHandle();
        MethodHandle instanceFactory = constructorMethodHandle(State.class, MapType.class).bindTo(mapType);

        MethodHandle methodHandle = METHOD_HANDLE.bindTo(mapType).bindTo(keyIndeterminate);
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL, NEVER_NULL),
                methodHandle,
                Optional.of(instanceFactory));
    }

    @UsedByGeneratedCode
    public static SqlMap createMap(
            MapType mapType,
            MethodHandle keyIndeterminate,
            State state,
            ConnectorSession session,
            Block keyBlock,
            Block valueBlock)
    {
        checkCondition(keyBlock.getPositionCount() == valueBlock.getPositionCount(), INVALID_FUNCTION_ARGUMENT, "Key and value arrays must be the same length");
        for (int i = 0; i < keyBlock.getPositionCount(); i++) {
            if (keyBlock.isNull(i)) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "map key cannot be null");
            }
            try {
                if ((boolean) keyIndeterminate.invoke(keyBlock, i)) {
                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "map key cannot be indeterminate: " + mapType.getKeyType().getObjectValue(session, keyBlock, i));
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }

        try {
            return new SqlMap(mapType, STRICT_NOT_DISTINCT_FROM, keyBlock, valueBlock);
        }
        catch (DuplicateMapKeyException e) {
            throw e.withDetailedMessage(mapType.getKeyType(), session);
        }
    }

    public static final class State
    {
        private final BufferedMapValueBuilder mapValueBuilder;

        public State(MapType mapType)
        {
            mapValueBuilder = BufferedMapValueBuilder.createBufferedStrict(mapType);
        }

        public BufferedMapValueBuilder getMapValueBuilder()
        {
            return mapValueBuilder;
        }
    }
}
