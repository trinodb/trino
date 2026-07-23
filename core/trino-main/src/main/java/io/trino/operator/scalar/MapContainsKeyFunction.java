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
import io.trino.spi.block.SqlMap;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TypeTemplates.mapType;
import static io.trino.spi.type.TypeTemplates.typeVariable;
import static io.trino.util.Failures.internalError;
import static io.trino.util.Reflection.methodHandle;

public class MapContainsKeyFunction
        extends SqlScalarFunction
{
    public static final MapContainsKeyFunction MAP_CONTAINS_KEY = new MapContainsKeyFunction();

    private static final MethodHandle METHOD_HANDLE_BOOLEAN = methodHandle(MapContainsKeyFunction.class, "containsKey", SqlMap.class, boolean.class);
    private static final MethodHandle METHOD_HANDLE_LONG = methodHandle(MapContainsKeyFunction.class, "containsKey", SqlMap.class, long.class);
    private static final MethodHandle METHOD_HANDLE_DOUBLE = methodHandle(MapContainsKeyFunction.class, "containsKey", SqlMap.class, double.class);
    private static final MethodHandle METHOD_HANDLE_OBJECT = methodHandle(MapContainsKeyFunction.class, "containsKey", MethodHandle.class, SqlMap.class, Object.class);

    private MapContainsKeyFunction()
    {
        super(FunctionMetadata.scalarBuilder("map_contains_key")
                .signature(Signature.builder()
                        .typeVariable("K")
                        .typeVariable("V")
                        .returnType(BOOLEAN)
                        .argumentType(mapType(typeVariable("K"), typeVariable("V")))
                        .argumentType(typeVariable("K"))
                        .build())
                .description("Determines whether the given map contains the given key")
                .build());
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies()
    {
        return FunctionDependencyDeclaration.builder()
                .addOperatorSignature(EQUAL, ImmutableList.of(typeVariable("K"), typeVariable("K")))
                .addOperatorSignature(INDETERMINATE, ImmutableList.of(typeVariable("K")))
                .build();
    }

    @Override
    public SpecializedSqlScalarFunction specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        MapType mapType = (MapType) boundSignature.getArgumentType(0);
        Type keyType = mapType.getKeyType();

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
            MethodHandle keyIndeterminate = functionDependencies.getOperatorImplementation(
                    INDETERMINATE,
                    ImmutableList.of(keyType),
                    simpleConvention(FAIL_ON_NULL, NEVER_NULL)).getMethodHandle();
            methodHandle = METHOD_HANDLE_OBJECT.bindTo(keyIndeterminate);
        }

        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL, NEVER_NULL),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static boolean containsKey(SqlMap sqlMap, boolean key)
    {
        return sqlMap.seekKeyExact(key) != -1;
    }

    @UsedByGeneratedCode
    public static boolean containsKey(SqlMap sqlMap, long key)
    {
        return sqlMap.seekKeyExact(key) != -1;
    }

    @UsedByGeneratedCode
    public static boolean containsKey(SqlMap sqlMap, double key)
    {
        return sqlMap.seekKeyExact(key) != -1;
    }

    @UsedByGeneratedCode
    public static boolean containsKey(MethodHandle keyIndeterminate, SqlMap sqlMap, Object key)
    {
        boolean indeterminate;
        try {
            indeterminate = (boolean) keyIndeterminate.invoke(key);
        }
        catch (Throwable t) {
            throw internalError(t);
        }
        if (indeterminate) {
            // a null-containing key can never be present (map construction rejects such keys), so membership is false.
            // resolving it here keeps the result deterministic rather than depending on hash-bucket collisions in seekKeyExact.
            return false;
        }
        return sqlMap.seekKeyExact(key) != -1;
    }
}
