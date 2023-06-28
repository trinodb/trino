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

import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BufferedArrayValueBuilder;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.gen.VarArgsToArrayAdapterGenerator;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.TypeSignature.arrayType;
import static io.trino.sql.gen.VarArgsToArrayAdapterGenerator.generateVarArgsToArrayAdapter;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Collections.nCopies;

public final class ArrayConcatFunction
        extends SqlScalarFunction
{
    public static final ArrayConcatFunction ARRAY_CONCAT_FUNCTION = new ArrayConcatFunction();

    private static final String FUNCTION_NAME = "concat";
    private static final String DESCRIPTION = "Concatenates given arrays";

    private static final MethodHandle METHOD_HANDLE;
    private static final MethodHandle USER_STATE_FACTORY;

    static {
        try {
            MethodHandles.Lookup lookup = lookup();
            METHOD_HANDLE = lookup.findStatic(ArrayConcatFunction.class, "concat", methodType(Block.class, Type.class, Object.class, Block[].class));
            USER_STATE_FACTORY = lookup.findStatic(ArrayConcatFunction.class, "createState", methodType(Object.class, ArrayType.class));
        }
        catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private ArrayConcatFunction()
    {
        super(FunctionMetadata.scalarBuilder()
                .signature(Signature.builder()
                        .name(FUNCTION_NAME)
                        .typeVariable("E")
                        .returnType(arrayType(new TypeSignature("E")))
                        .argumentType(arrayType(new TypeSignature("E")))
                        .variableArity()
                        .build())
                .description(DESCRIPTION)
                .build());
    }

    @Override
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        if (boundSignature.getArity() < 2) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "There must be two or more arguments to " + FUNCTION_NAME);
        }

        ArrayType arrayType = (ArrayType) boundSignature.getReturnType();

        VarArgsToArrayAdapterGenerator.MethodHandleAndConstructor methodHandleAndConstructor = generateVarArgsToArrayAdapter(
                Block.class,
                Block.class,
                boundSignature.getArity(),
                METHOD_HANDLE.bindTo(arrayType.getElementType()),
                USER_STATE_FACTORY.bindTo(arrayType));

        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                nCopies(boundSignature.getArity(), NEVER_NULL),
                methodHandleAndConstructor.getMethodHandle(),
                Optional.of(methodHandleAndConstructor.getConstructor()));
    }

    @UsedByGeneratedCode
    public static Object createState(ArrayType arrayType)
    {
        return BufferedArrayValueBuilder.createBuffered(arrayType);
    }

    @UsedByGeneratedCode
    public static Block concat(Type elementType, Object state, Block[] blocks)
    {
        int resultPositionCount = 0;

        // fast path when there is at most one non-empty block
        Block nonEmptyBlock = null;
        for (Block value : blocks) {
            resultPositionCount += value.getPositionCount();
            if (value.getPositionCount() > 0) {
                nonEmptyBlock = value;
            }
        }
        if (nonEmptyBlock == null) {
            return blocks[0];
        }
        if (resultPositionCount == nonEmptyBlock.getPositionCount()) {
            return nonEmptyBlock;
        }

        return ((BufferedArrayValueBuilder) state).build(resultPositionCount, elementBuilder -> {
            for (Block block : blocks) {
                for (int i = 0; i < block.getPositionCount(); i++) {
                    elementType.appendTo(block, i, elementBuilder);
                }
            }
        });
    }
}
