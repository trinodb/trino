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
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.block.Block;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.gen.lambda.BinaryFunctionInterface;
import io.trino.sql.gen.lambda.UnaryFunctionInterface;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FUNCTION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.type.TypeSignature.arrayType;
import static io.trino.spi.type.TypeSignature.functionType;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.util.Reflection.methodHandle;

public final class ArrayReduceFunction
        extends SqlScalarFunction
{
    public static final ArrayReduceFunction ARRAY_REDUCE_FUNCTION = new ArrayReduceFunction();

    private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayReduceFunction.class, "reduce", Type.class, Block.class, Object.class, BinaryFunctionInterface.class, UnaryFunctionInterface.class);

    private ArrayReduceFunction()
    {
        super(FunctionMetadata.scalarBuilder()
                .signature(Signature.builder()
                        .name("reduce")
                        .typeVariable("T")
                        .typeVariable("S")
                        .typeVariable("R")
                        .returnType(new TypeSignature("R"))
                        .argumentType(arrayType(new TypeSignature("T")))
                        .argumentType(new TypeSignature("S"))
                        .argumentType(functionType(new TypeSignature("S"), new TypeSignature("T"), new TypeSignature("S")))
                        .argumentType(functionType(new TypeSignature("S"), new TypeSignature("R")))
                        .build())
                .nullable()
                .argumentNullability(false, true, false, false)
                .nondeterministic()
                .description("Reduce elements of the array into a single value")
                .build());
    }

    @Override
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        ArrayType arrayType = (ArrayType) boundSignature.getArgumentTypes().get(0);
        Type inputType = arrayType.getElementType();
        Type intermediateType = boundSignature.getArgumentTypes().get(1);
        Type outputType = boundSignature.getReturnType();
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(inputType);
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                NULLABLE_RETURN,
                ImmutableList.of(NEVER_NULL, BOXED_NULLABLE, FUNCTION, FUNCTION),
                ImmutableList.of(BinaryFunctionInterface.class, UnaryFunctionInterface.class),
                methodHandle.asType(
                        methodHandle.type()
                                .changeParameterType(1, Primitives.wrap(intermediateType.getJavaType()))
                                .changeReturnType(Primitives.wrap(outputType.getJavaType()))),
                Optional.empty());
    }

    public static Object reduce(
            Type inputType,
            Block block,
            Object initialIntermediateValue,
            BinaryFunctionInterface inputFunction,
            UnaryFunctionInterface outputFunction)
    {
        int positionCount = block.getPositionCount();
        Object intermediateValue = initialIntermediateValue;
        for (int position = 0; position < positionCount; position++) {
            Object input = readNativeValue(inputType, block, position);
            intermediateValue = inputFunction.apply(intermediateValue, input);
        }
        return outputFunction.apply(intermediateValue);
    }
}
