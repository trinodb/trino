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
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.BoundSignature;
import io.trino.metadata.FunctionDependencies;
import io.trino.metadata.FunctionDependencyDeclaration;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.FunctionNullability;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.block.Block;
import io.trino.spi.block.SingleMapBlock;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;

import static io.trino.metadata.FunctionKind.SCALAR;
import static io.trino.metadata.Signature.typeVariable;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.util.Reflection.methodHandle;

public class MapElementAtFunction
        extends SqlScalarFunction
{
    public static final MapElementAtFunction MAP_ELEMENT_AT = new MapElementAtFunction();

    private static final MethodHandle METHOD_HANDLE_BOOLEAN = methodHandle(MapElementAtFunction.class, "elementAt", Type.class, Block.class, boolean.class);
    private static final MethodHandle METHOD_HANDLE_LONG = methodHandle(MapElementAtFunction.class, "elementAt", Type.class, Block.class, long.class);
    private static final MethodHandle METHOD_HANDLE_DOUBLE = methodHandle(MapElementAtFunction.class, "elementAt", Type.class, Block.class, double.class);
    private static final MethodHandle METHOD_HANDLE_OBJECT = methodHandle(MapElementAtFunction.class, "elementAt", Type.class, Block.class, Object.class);

    protected MapElementAtFunction()
    {
        super(new FunctionMetadata(
                new Signature(
                        "element_at",
                        ImmutableList.of(typeVariable("K"), typeVariable("V")),
                        ImmutableList.of(),
                        new TypeSignature("V"),
                        ImmutableList.of(mapType(new TypeSignature("K"), new TypeSignature("V")), new TypeSignature("K")),
                        false),
                new FunctionNullability(true, ImmutableList.of(false, false)),
                false,
                true,
                "Get value for the given key, or null if it does not exist",
                SCALAR));
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies()
    {
        return FunctionDependencyDeclaration.builder()
                .addOperatorSignature(EQUAL, ImmutableList.of(new TypeSignature("K"), new TypeSignature("K")))
                .build();
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
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
        methodHandle = methodHandle.bindTo(valueType);
        methodHandle = methodHandle.asType(methodHandle.type().changeReturnType(Primitives.wrap(valueType.getJavaType())));

        return new ChoicesScalarFunctionImplementation(
                boundSignature,
                NULLABLE_RETURN,
                ImmutableList.of(NEVER_NULL, NEVER_NULL),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static Object elementAt(Type valueType, Block map, boolean key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition = mapBlock.seekKeyExact(key);
        if (valuePosition == -1) {
            return null;
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    @UsedByGeneratedCode
    public static Object elementAt(Type valueType, Block map, long key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition = mapBlock.seekKeyExact(key);
        if (valuePosition == -1) {
            return null;
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    @UsedByGeneratedCode
    public static Object elementAt(Type valueType, Block map, double key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition = mapBlock.seekKeyExact(key);
        if (valuePosition == -1) {
            return null;
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    @UsedByGeneratedCode
    public static Object elementAt(Type valueType, Block map, Object key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition = mapBlock.seekKeyExact(key);
        if (valuePosition == -1) {
            return null;
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }
}
