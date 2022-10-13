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
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.SingleMapBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.Signature;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.OperatorType.SUBSCRIPT;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.util.Reflection.methodHandle;

public class MapSubscriptOperator
        extends SqlScalarFunction
{
    private static final MethodHandle METHOD_HANDLE_BOOLEAN = methodHandle(MapSubscriptOperator.class, "subscript", MissingKeyExceptionFactory.class, Type.class, Type.class, ConnectorSession.class, Block.class, boolean.class);
    private static final MethodHandle METHOD_HANDLE_LONG = methodHandle(MapSubscriptOperator.class, "subscript", MissingKeyExceptionFactory.class, Type.class, Type.class, ConnectorSession.class, Block.class, long.class);
    private static final MethodHandle METHOD_HANDLE_DOUBLE = methodHandle(MapSubscriptOperator.class, "subscript", MissingKeyExceptionFactory.class, Type.class, Type.class, ConnectorSession.class, Block.class, double.class);
    private static final MethodHandle METHOD_HANDLE_OBJECT = methodHandle(MapSubscriptOperator.class, "subscript", MissingKeyExceptionFactory.class, Type.class, Type.class, ConnectorSession.class, Block.class, Object.class);

    public MapSubscriptOperator()
    {
        super(FunctionMetadata.scalarBuilder()
                .signature(Signature.builder()
                        .operatorType(SUBSCRIPT)
                        .typeVariable("K")
                        .typeVariable("V")
                        .returnType(new TypeSignature("V"))
                        .argumentType(mapType(new TypeSignature("K"), new TypeSignature("V")))
                        .argumentType(new TypeSignature("K"))
                        .build())
                .nullable()
                .build());
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies()
    {
        return FunctionDependencyDeclaration.builder()
                .addOptionalCastSignature(new TypeSignature("K"), VARCHAR.getTypeSignature())
                .build();
    }

    @Override
    public SpecializedSqlScalarFunction specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        MapType mapType = (MapType) boundSignature.getArgumentType(0);
        Type keyType = mapType.getKeyType();
        Type valueType = mapType.getValueType();

        MethodHandle methodHandle;
        if (keyType.getJavaType() == boolean.class) {
            methodHandle = METHOD_HANDLE_BOOLEAN;
        }
        else if (keyType.getJavaType() == long.class) {
            methodHandle = METHOD_HANDLE_LONG;
        }
        else if (keyType.getJavaType() == double.class) {
            methodHandle = METHOD_HANDLE_DOUBLE;
        }
        else {
            methodHandle = METHOD_HANDLE_OBJECT;
        }
        MissingKeyExceptionFactory missingKeyExceptionFactory = new MissingKeyExceptionFactory(functionDependencies, keyType);
        methodHandle = methodHandle.bindTo(missingKeyExceptionFactory).bindTo(keyType).bindTo(valueType);
        methodHandle = methodHandle.asType(methodHandle.type().changeReturnType(Primitives.wrap(valueType.getJavaType())));

        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                NULLABLE_RETURN,
                ImmutableList.of(NEVER_NULL, NEVER_NULL),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static Object subscript(MissingKeyExceptionFactory missingKeyExceptionFactory, Type keyType, Type valueType, ConnectorSession session, Block map, boolean key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition = mapBlock.seekKeyExact(key);
        if (valuePosition == -1) {
            throw missingKeyExceptionFactory.create(session, key);
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    @UsedByGeneratedCode
    public static Object subscript(MissingKeyExceptionFactory missingKeyExceptionFactory, Type keyType, Type valueType, ConnectorSession session, Block map, long key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition = mapBlock.seekKeyExact(key);
        if (valuePosition == -1) {
            throw missingKeyExceptionFactory.create(session, key);
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    @UsedByGeneratedCode
    public static Object subscript(MissingKeyExceptionFactory missingKeyExceptionFactory, Type keyType, Type valueType, ConnectorSession session, Block map, double key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition = mapBlock.seekKeyExact(key);
        if (valuePosition == -1) {
            throw missingKeyExceptionFactory.create(session, key);
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    @UsedByGeneratedCode
    public static Object subscript(MissingKeyExceptionFactory missingKeyExceptionFactory, Type keyType, Type valueType, ConnectorSession session, Block map, Object key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition = mapBlock.seekKeyExact(key);
        if (valuePosition == -1) {
            throw missingKeyExceptionFactory.create(session, key);
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    private static class MissingKeyExceptionFactory
    {
        private final MethodHandle castMethod;

        public MissingKeyExceptionFactory(FunctionDependencies functionDependencies, Type keyType)
        {
            MethodHandle castMethod = null;
            try {
                InvocationConvention invocationConvention = new InvocationConvention(ImmutableList.of(BOXED_NULLABLE), NULLABLE_RETURN, true, false);
                castMethod = functionDependencies.getCastImplementation(keyType, VARCHAR, invocationConvention).getMethodHandle();
                if (!castMethod.type().parameterType(0).equals(ConnectorSession.class)) {
                    castMethod = MethodHandles.dropArguments(castMethod, 0, ConnectorSession.class);
                }
            }
            catch (TrinoException ignored) {
            }
            this.castMethod = castMethod;
        }

        public TrinoException create(ConnectorSession session, Object value)
        {
            String additionalInfo = "";
            if (castMethod != null) {
                Slice varcharValue = null;
                try {
                    varcharValue = (Slice) castMethod.invokeWithArguments(session, value);
                }
                catch (Throwable ignored) {
                }

                additionalInfo = ": " + (varcharValue == null ? "NULL" : varcharValue.toStringUtf8());
            }
            return new TrinoException(INVALID_FUNCTION_ARGUMENT, "Key not present in map" + additionalInfo);
        }
    }
}
