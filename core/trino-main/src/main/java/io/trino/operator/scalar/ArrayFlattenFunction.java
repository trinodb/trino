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
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.TypeSignature.arrayType;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.Math.toIntExact;

public class ArrayFlattenFunction
        extends SqlScalarFunction
{
    public static final ArrayFlattenFunction ARRAY_FLATTEN_FUNCTION = new ArrayFlattenFunction();
    private static final String FUNCTION_NAME = "flatten";
    private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayFlattenFunction.class, FUNCTION_NAME, Type.class, Type.class, Block.class);

    private ArrayFlattenFunction()
    {
        super(FunctionMetadata.scalarBuilder()
                .signature(Signature.builder()
                        .name(FUNCTION_NAME)
                        .typeVariable("E")
                        .returnType(arrayType(new TypeSignature("E")))
                        .argumentType(arrayType(arrayType(new TypeSignature("E"))))
                        .build())
                .description("Flattens the given array")
                .build());
    }

    @Override
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        ArrayType arrayType = (ArrayType) boundSignature.getReturnType();
        MethodHandle methodHandle = METHOD_HANDLE
                .bindTo(arrayType.getElementType())
                .bindTo(arrayType);
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL),
                methodHandle);
    }

    public static Block flatten(Type type, Type arrayType, Block array)
    {
        if (array.getPositionCount() == 0) {
            return type.createBlockBuilder(null, 0).build();
        }

        BlockBuilder builder = type.createBlockBuilder(null, array.getPositionCount(), toIntExact(array.getSizeInBytes() / array.getPositionCount()));
        for (int i = 0; i < array.getPositionCount(); i++) {
            if (!array.isNull(i)) {
                Block subArray = (Block) arrayType.getObject(array, i);
                for (int j = 0; j < subArray.getPositionCount(); j++) {
                    type.appendTo(subArray, j, builder);
                }
            }
        }
        return builder.build();
    }
}
