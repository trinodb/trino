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
package io.prestosql.operator.scalar;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;
import io.prestosql.metadata.FunctionArgumentDefinition;
import io.prestosql.metadata.FunctionBinding;
import io.prestosql.metadata.FunctionMetadata;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.gen.lambda.BinaryFunctionInterface;
import io.prestosql.sql.gen.lambda.UnaryFunctionInterface;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.prestosql.metadata.FunctionKind.SCALAR;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.FUNCTION;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.prestosql.spi.type.TypeSignature.arrayType;
import static io.prestosql.spi.type.TypeSignature.functionType;
import static io.prestosql.spi.type.TypeUtils.readNativeValue;
import static io.prestosql.util.Reflection.methodHandle;

public final class ArrayReduceFunction
        extends SqlScalarFunction
{
    public static final ArrayReduceFunction ARRAY_REDUCE_FUNCTION = new ArrayReduceFunction();

    private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayReduceFunction.class, "reduce", Type.class, Block.class, Object.class, BinaryFunctionInterface.class, UnaryFunctionInterface.class);

    private ArrayReduceFunction()
    {
        super(new FunctionMetadata(
                new Signature(
                        "reduce",
                        ImmutableList.of(typeVariable("T"), typeVariable("S"), typeVariable("R")),
                        ImmutableList.of(),
                        new TypeSignature("R"),
                        ImmutableList.of(
                                arrayType(new TypeSignature("T")),
                                new TypeSignature("S"),
                                functionType(new TypeSignature("S"), new TypeSignature("T"), new TypeSignature("S")),
                                functionType(new TypeSignature("S"), new TypeSignature("R"))),
                        false),
                true,
                ImmutableList.of(
                        new FunctionArgumentDefinition(false),
                        new FunctionArgumentDefinition(true),
                        new FunctionArgumentDefinition(false),
                        new FunctionArgumentDefinition(false)),
                false,
                false,
                "Reduce elements of the array into a single value",
                SCALAR));
    }

    @Override
    protected ScalarFunctionImplementation specialize(FunctionBinding functionBinding)
    {
        Type inputType = functionBinding.getTypeVariable("T");
        Type intermediateType = functionBinding.getTypeVariable("S");
        Type outputType = functionBinding.getTypeVariable("R");
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(inputType);
        return new ScalarFunctionImplementation(
                NULLABLE_RETURN,
                ImmutableList.of(NEVER_NULL, BOXED_NULLABLE, FUNCTION, FUNCTION),
                ImmutableList.of(
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(BinaryFunctionInterface.class),
                        Optional.of(UnaryFunctionInterface.class)),
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
