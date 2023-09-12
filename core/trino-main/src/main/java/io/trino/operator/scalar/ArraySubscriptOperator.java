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
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.function.OperatorType.READ_VALUE;
import static io.trino.spi.function.OperatorType.SUBSCRIPT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeSignature.arrayType;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.collectArguments;
import static java.lang.invoke.MethodHandles.empty;
import static java.lang.invoke.MethodHandles.explicitCastArguments;
import static java.lang.invoke.MethodHandles.guardWithTest;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodHandles.permuteArguments;
import static java.lang.invoke.MethodType.methodType;

public class ArraySubscriptOperator
        extends SqlScalarFunction
{
    public static final ArraySubscriptOperator ARRAY_SUBSCRIPT = new ArraySubscriptOperator();

    private static final MethodHandle GET_POSITION;
    private static final MethodHandle IS_POSITION_NULL;

    static {
        try {
            GET_POSITION = lookup().findStatic(ArraySubscriptOperator.class, "getPosition", methodType(int.class, Block.class, long.class));
            IS_POSITION_NULL = lookup().findVirtual(Block.class, "isNull", methodType(boolean.class, int.class));
        }
        catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private ArraySubscriptOperator()
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
    public FunctionDependencyDeclaration getFunctionDependencies()
    {
        return FunctionDependencyDeclaration.builder()
                .addOperatorSignature(READ_VALUE, ImmutableList.of(new TypeSignature("E")))
                .build();
    }

    @Override
    public SpecializedSqlScalarFunction specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        Type elementType = boundSignature.getReturnType();
        MethodHandle methodHandle = functionDependencies.getOperatorImplementation(
                READ_VALUE,
                ImmutableList.of(elementType),
                simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL))
                .getMethodHandle();
        Class<?> expectedReturnType = methodType(elementType.getJavaType()).wrap().returnType();
        methodHandle = explicitCastArguments(methodHandle, methodHandle.type().changeReturnType(expectedReturnType));
        methodHandle = guardWithTest(
                IS_POSITION_NULL,
                empty(methodHandle.type()),
                methodHandle);
        methodHandle = collectArguments(methodHandle, 1, GET_POSITION);
        methodHandle = permuteArguments(methodHandle, methodHandle.type().dropParameterTypes(1, 2), 0, 0, 1);

        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                NULLABLE_RETURN,
                ImmutableList.of(NEVER_NULL, NEVER_NULL),
                methodHandle);
    }

    private static int getPosition(Block array, long index)
    {
        checkArrayIndex(index);
        if (index > array.getPositionCount()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Array subscript must be less than or equal to array length: %s > %s", index, array.getPositionCount()));
        }
        int position = toIntExact(index - 1);
        return position;
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
}
