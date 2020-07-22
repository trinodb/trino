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
import io.airlift.slice.Slice;
import io.prestosql.metadata.FunctionArgumentDefinition;
import io.prestosql.metadata.FunctionBinding;
import io.prestosql.metadata.FunctionMetadata;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;

import static io.prestosql.metadata.FunctionKind.SCALAR;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.prestosql.spi.type.TypeSignature.arrayType;
import static io.prestosql.util.Reflection.methodHandle;

public class ArrayToElementConcatFunction
        extends SqlScalarFunction
{
    public static final ArrayToElementConcatFunction ARRAY_TO_ELEMENT_CONCAT_FUNCTION = new ArrayToElementConcatFunction();
    private static final String FUNCTION_NAME = "concat";

    private static final MethodHandle METHOD_HANDLE_BOOLEAN = methodHandle(ArrayConcatUtils.class, "appendElement", Type.class, Block.class, boolean.class);
    private static final MethodHandle METHOD_HANDLE_LONG = methodHandle(ArrayConcatUtils.class, "appendElement", Type.class, Block.class, long.class);
    private static final MethodHandle METHOD_HANDLE_DOUBLE = methodHandle(ArrayConcatUtils.class, "appendElement", Type.class, Block.class, double.class);
    private static final MethodHandle METHOD_HANDLE_SLICE = methodHandle(ArrayConcatUtils.class, "appendElement", Type.class, Block.class, Slice.class);
    private static final MethodHandle METHOD_HANDLE_OBJECT = methodHandle(ArrayConcatUtils.class, "appendElement", Type.class, Block.class, Object.class);

    public ArrayToElementConcatFunction()
    {
        super(new FunctionMetadata(
                new Signature(
                        FUNCTION_NAME,
                        ImmutableList.of(typeVariable("E")),
                        ImmutableList.of(),
                        arrayType(new TypeSignature("E")),
                        ImmutableList.of(arrayType(new TypeSignature("E")), new TypeSignature("E")),
                        false),
                false,
                ImmutableList.of(
                        new FunctionArgumentDefinition(false),
                        new FunctionArgumentDefinition(false)),
                false,
                true,
                "Concatenates an array to an element",
                SCALAR));
    }

    @Override
    protected ScalarFunctionImplementation specialize(FunctionBinding functionBinding)
    {
        Type type = functionBinding.getTypeVariable("E");
        MethodHandle methodHandle;
        if (type.getJavaType() == boolean.class) {
            methodHandle = METHOD_HANDLE_BOOLEAN;
        }
        else if (type.getJavaType() == long.class) {
            methodHandle = METHOD_HANDLE_LONG;
        }
        else if (type.getJavaType() == double.class) {
            methodHandle = METHOD_HANDLE_DOUBLE;
        }
        else if (type.getJavaType() == Slice.class) {
            methodHandle = METHOD_HANDLE_SLICE;
        }
        else {
            methodHandle = METHOD_HANDLE_OBJECT;
        }
        methodHandle = methodHandle.bindTo(type);

        return new ScalarFunctionImplementation(
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL, NEVER_NULL),
                methodHandle);
    }
}
