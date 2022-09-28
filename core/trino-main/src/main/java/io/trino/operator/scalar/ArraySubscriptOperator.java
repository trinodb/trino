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
import io.airlift.slice.Slice;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.OperatorType.SUBSCRIPT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeSignature.arrayType;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ArraySubscriptOperator
        extends SqlScalarFunction
{
    public static final ArraySubscriptOperator ARRAY_SUBSCRIPT = new ArraySubscriptOperator();

    private static final MethodHandle METHOD_HANDLE_BOOLEAN = methodHandle(ArraySubscriptOperator.class, "booleanSubscript", Type.class, Block.class, long.class);
    private static final MethodHandle METHOD_HANDLE_LONG = methodHandle(ArraySubscriptOperator.class, "longSubscript", Type.class, Block.class, long.class);
    private static final MethodHandle METHOD_HANDLE_DOUBLE = methodHandle(ArraySubscriptOperator.class, "doubleSubscript", Type.class, Block.class, long.class);
    private static final MethodHandle METHOD_HANDLE_SLICE = methodHandle(ArraySubscriptOperator.class, "sliceSubscript", Type.class, Block.class, long.class);
    private static final MethodHandle METHOD_HANDLE_OBJECT = methodHandle(ArraySubscriptOperator.class, "objectSubscript", Type.class, Block.class, long.class);

    protected ArraySubscriptOperator()
    {
        super(FunctionMetadata.scalarBuilder()
                .signature(Signature.builder()
                        .operatorType(SUBSCRIPT)
                        .typeVariable("E")
                        .returnType(new TypeSignature("E"))
                        .argumentType(arrayType(new TypeSignature("E")))
                        .argumentType(BIGINT)
                        .build())
                .nullable()
                .build());
    }

    @Override
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        Type elementType = boundSignature.getReturnType();

        MethodHandle methodHandle;
        if (elementType.getJavaType() == boolean.class) {
            methodHandle = METHOD_HANDLE_BOOLEAN;
        }
        else if (elementType.getJavaType() == long.class) {
            methodHandle = METHOD_HANDLE_LONG;
        }
        else if (elementType.getJavaType() == double.class) {
            methodHandle = METHOD_HANDLE_DOUBLE;
        }
        else if (elementType.getJavaType() == Slice.class) {
            methodHandle = METHOD_HANDLE_SLICE;
        }
        else {
            methodHandle = METHOD_HANDLE_OBJECT.asType(
                    METHOD_HANDLE_OBJECT.type().changeReturnType(elementType.getJavaType()));
        }
        methodHandle = methodHandle.bindTo(elementType);
        requireNonNull(methodHandle, "methodHandle is null");
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                NULLABLE_RETURN,
                ImmutableList.of(NEVER_NULL, NEVER_NULL),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static Long longSubscript(Type elementType, Block array, long index)
    {
        checkIndex(array, index);
        int position = toIntExact(index - 1);
        if (array.isNull(position)) {
            return null;
        }

        return elementType.getLong(array, position);
    }

    @UsedByGeneratedCode
    public static Boolean booleanSubscript(Type elementType, Block array, long index)
    {
        checkIndex(array, index);
        int position = toIntExact(index - 1);
        if (array.isNull(position)) {
            return null;
        }

        return elementType.getBoolean(array, position);
    }

    @UsedByGeneratedCode
    public static Double doubleSubscript(Type elementType, Block array, long index)
    {
        checkIndex(array, index);
        int position = toIntExact(index - 1);
        if (array.isNull(position)) {
            return null;
        }

        return elementType.getDouble(array, position);
    }

    @UsedByGeneratedCode
    public static Slice sliceSubscript(Type elementType, Block array, long index)
    {
        checkIndex(array, index);
        int position = toIntExact(index - 1);
        if (array.isNull(position)) {
            return null;
        }

        return elementType.getSlice(array, position);
    }

    @UsedByGeneratedCode
    public static Object objectSubscript(Type elementType, Block array, long index)
    {
        checkIndex(array, index);
        int position = toIntExact(index - 1);
        if (array.isNull(position)) {
            return null;
        }

        return elementType.getObject(array, position);
    }

    public static void checkArrayIndex(long index)
    {
        if (index == 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "SQL array indices start at 1");
        }
        if (index < 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Array subscript is negative: " + index);
        }
    }

    public static void checkIndex(Block array, long index)
    {
        checkArrayIndex(index);
        if (index > array.getPositionCount()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Array subscript must be less than or equal to array length: %s > %s", index, array.getPositionCount()));
        }
    }
}
