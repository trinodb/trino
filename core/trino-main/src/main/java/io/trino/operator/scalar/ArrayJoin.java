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
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.InvocationConvention.InvocationArgumentConvention;
import io.trino.spi.function.Signature;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.type.UnknownType;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.type.TypeSignature.arrayType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.String.format;

public final class ArrayJoin
        extends SqlScalarFunction
{
    public static final ArrayJoin ARRAY_JOIN = new ArrayJoin();
    public static final ArrayJoinWithNullReplacement ARRAY_JOIN_WITH_NULL_REPLACEMENT = new ArrayJoinWithNullReplacement();

    private static final String FUNCTION_NAME = "array_join";
    private static final String DESCRIPTION = "Concatenates the elements of the given array using a delimiter and an optional string to replace nulls";
    private static final MethodHandle METHOD_HANDLE = methodHandle(
            ArrayJoin.class,
            "arrayJoin",
            MethodHandle.class,
            Object.class,
            ConnectorSession.class,
            Block.class,
            Slice.class);

    private static final MethodHandle STATE_FACTORY = methodHandle(ArrayJoin.class, "createState");

    public static class ArrayJoinWithNullReplacement
            extends SqlScalarFunction
    {
        private static final MethodHandle METHOD_HANDLE = methodHandle(
                ArrayJoin.class,
                "arrayJoin",
                MethodHandle.class,
                Object.class,
                ConnectorSession.class,
                Block.class,
                Slice.class,
                Slice.class);

        public ArrayJoinWithNullReplacement()
        {
            super(FunctionMetadata.scalarBuilder()
                    .signature(Signature.builder()
                            .name(FUNCTION_NAME)
                            .typeVariable("T")
                            .returnType(VARCHAR)
                            .argumentType(arrayType(new TypeSignature("T")))
                            .argumentType(VARCHAR)
                            .argumentType(VARCHAR)
                            .build())
                    .description(DESCRIPTION)
                    .build());
        }

        @Override
        public FunctionDependencyDeclaration getFunctionDependencies()
        {
            return arrayJoinFunctionDependencies();
        }

        @Override
        public SpecializedSqlScalarFunction specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
        {
            return specializeArrayJoin(boundSignature, functionDependencies, METHOD_HANDLE);
        }
    }

    private ArrayJoin()
    {
        super(FunctionMetadata.scalarBuilder()
                .signature(Signature.builder()
                        .name(FUNCTION_NAME)
                        .castableToTypeParameter("T", VARCHAR.getTypeSignature())
                        .returnType(VARCHAR)
                        .argumentType(arrayType(new TypeSignature("T")))
                        .argumentType(VARCHAR)
                        .build())
                .description(DESCRIPTION)
                .build());
    }

    @UsedByGeneratedCode
    public static Object createState()
    {
        return new PageBuilder(ImmutableList.of(VARCHAR));
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies()
    {
        return arrayJoinFunctionDependencies();
    }

    private static FunctionDependencyDeclaration arrayJoinFunctionDependencies()
    {
        return FunctionDependencyDeclaration.builder()
                .addCastSignature(new TypeSignature("T"), VARCHAR.getTypeSignature())
                .build();
    }

    @Override
    public SpecializedSqlScalarFunction specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        return specializeArrayJoin(boundSignature, functionDependencies, METHOD_HANDLE);
    }

    private static ChoicesSpecializedSqlScalarFunction specializeArrayJoin(
            BoundSignature boundSignature,
            FunctionDependencies functionDependencies,
            MethodHandle methodHandle)
    {
        List<InvocationArgumentConvention> argumentConventions = Collections.nCopies(boundSignature.getArity(), NEVER_NULL);

        Type type = ((ArrayType) boundSignature.getArgumentTypes().get(0)).getElementType();
        if (type instanceof UnknownType) {
            return new ChoicesSpecializedSqlScalarFunction(
                    boundSignature,
                    FAIL_ON_NULL,
                    argumentConventions,
                    methodHandle.bindTo(null),
                    Optional.of(STATE_FACTORY));
        }
        try {
            InvocationConvention convention = new InvocationConvention(ImmutableList.of(BLOCK_POSITION), NULLABLE_RETURN, true, false);
            MethodHandle cast = functionDependencies.getCastImplementation(type, VARCHAR, convention).getMethodHandle();

            // if the cast doesn't take a ConnectorSession, create an adapter that drops the provided session
            if (cast.type().parameterArray()[0] != ConnectorSession.class) {
                cast = MethodHandles.dropArguments(cast, 0, ConnectorSession.class);
            }

            MethodHandle target = MethodHandles.insertArguments(methodHandle, 0, cast);
            return new ChoicesSpecializedSqlScalarFunction(
                    boundSignature,
                    FAIL_ON_NULL,
                    argumentConventions,
                    target,
                    Optional.of(STATE_FACTORY));
        }
        catch (TrinoException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Input type %s not supported", type), e);
        }
    }

    @UsedByGeneratedCode
    public static Slice arrayJoin(
            MethodHandle castFunction,
            Object state,
            ConnectorSession session,
            Block arrayBlock,
            Slice delimiter)
    {
        return arrayJoin(castFunction, state, session, arrayBlock, delimiter, null);
    }

    @UsedByGeneratedCode
    public static Slice arrayJoin(
            MethodHandle castFunction,
            Object state,
            ConnectorSession session,
            Block arrayBlock,
            Slice delimiter,
            Slice nullReplacement)
    {
        PageBuilder pageBuilder = (PageBuilder) state;
        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }
        int numElements = arrayBlock.getPositionCount();
        VariableWidthBlockBuilder blockBuilder = (VariableWidthBlockBuilder) pageBuilder.getBlockBuilder(0);
        blockBuilder.buildEntry(valueWriter -> {
            boolean needsDelimiter = false;
            for (int i = 0; i < numElements; i++) {
                Slice value = null;
                if (!arrayBlock.isNull(i)) {
                    try {
                        value = (Slice) castFunction.invokeExact(session, arrayBlock, i);
                    }
                    catch (Throwable throwable) {
                        // Restore pageBuilder into a consistent state
                        pageBuilder.declarePosition();
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error casting array element to VARCHAR", throwable);
                    }
                }

                if (value == null) {
                    value = nullReplacement;
                    if (value == null) {
                        continue;
                    }
                }

                if (needsDelimiter) {
                    valueWriter.writeBytes(delimiter, 0, delimiter.length());
                }
                valueWriter.writeBytes(value, 0, value.length());
                needsDelimiter = true;
            }
        });
        pageBuilder.declarePosition();
        return VARCHAR.getSlice(blockBuilder, blockBuilder.getPositionCount() - 1);
    }
}
