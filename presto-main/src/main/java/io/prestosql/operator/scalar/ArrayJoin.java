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
import io.prestosql.annotation.UsedByGeneratedCode;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionArgumentDefinition;
import io.prestosql.metadata.FunctionMetadata;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.InvocationConvention;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.type.UnknownType;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.metadata.FunctionKind.SCALAR;
import static io.prestosql.metadata.Signature.castableToTypeParameter;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.USE_BOXED_TYPE;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.prestosql.spi.type.TypeSignature.arrayType;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.util.Reflection.methodHandle;
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
            super(new FunctionMetadata(
                    new Signature(
                            FUNCTION_NAME,
                            ImmutableList.of(typeVariable("T")),
                            ImmutableList.of(),
                            VARCHAR.getTypeSignature(),
                            ImmutableList.of(arrayType(new TypeSignature("T")), VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()),
                            false),
                    false,
                    ImmutableList.of(
                            new FunctionArgumentDefinition(false),
                            new FunctionArgumentDefinition(false),
                            new FunctionArgumentDefinition(false)),
                    false,
                    true,
                    DESCRIPTION,
                    SCALAR));
        }

        @Override
        public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, Metadata metadata)
        {
            return specializeArrayJoin(boundVariables, metadata, ImmutableList.of(false, false, false), METHOD_HANDLE);
        }
    }

    public ArrayJoin()
    {
        super(new FunctionMetadata(
                new Signature(
                        FUNCTION_NAME,
                        ImmutableList.of(castableToTypeParameter("T", VARCHAR.getTypeSignature())),
                        ImmutableList.of(),
                        VARCHAR.getTypeSignature(),
                        ImmutableList.of(arrayType(new TypeSignature("T")), VARCHAR.getTypeSignature()),
                        false),
                false,
                ImmutableList.of(
                        new FunctionArgumentDefinition(false),
                        new FunctionArgumentDefinition(false)),
                false,
                true,
                DESCRIPTION,
                SCALAR));
    }

    @UsedByGeneratedCode
    public static Object createState()
    {
        return new PageBuilder(ImmutableList.of(VARCHAR));
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, Metadata metadata)
    {
        return specializeArrayJoin(boundVariables, metadata, ImmutableList.of(false, false), METHOD_HANDLE);
    }

    private static ScalarFunctionImplementation specializeArrayJoin(BoundVariables types, Metadata metadata, List<Boolean> nullableArguments, MethodHandle methodHandle)
    {
        Type type = types.getTypeVariable("T");
        List<ArgumentProperty> argumentProperties = nullableArguments.stream()
                .map(nullable -> nullable
                        ? valueTypeArgumentProperty(USE_BOXED_TYPE)
                        : valueTypeArgumentProperty(RETURN_NULL_ON_NULL))
                .collect(toImmutableList());

        if (type instanceof UnknownType) {
            return new ScalarFunctionImplementation(
                    false,
                    argumentProperties,
                    methodHandle.bindTo(null),
                    Optional.of(STATE_FACTORY));
        }
        else {
            try {
                ResolvedFunction resolvedFunction = metadata.getCoercion(type, VARCHAR);
                InvocationConvention convention = new InvocationConvention(ImmutableList.of(BLOCK_POSITION), NULLABLE_RETURN, true, false);
                MethodHandle cast = metadata.getScalarFunctionInvoker(resolvedFunction, Optional.of(convention)).getMethodHandle();

                // if the cast doesn't take a ConnectorSession, create an adapter that drops the provided session
                if (cast.type().parameterArray()[0] != ConnectorSession.class) {
                    cast = MethodHandles.dropArguments(cast, 0, ConnectorSession.class);
                }

                MethodHandle target = MethodHandles.insertArguments(methodHandle, 0, cast);
                return new ScalarFunctionImplementation(
                        false,
                        argumentProperties,
                        target,
                        Optional.of(STATE_FACTORY));
            }
            catch (PrestoException e) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Input type %s not supported", type), e);
            }
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
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);

        boolean needsDelimiter = false;
        for (int i = 0; i < numElements; i++) {
            Slice value = null;
            if (!arrayBlock.isNull(i)) {
                try {
                    value = (Slice) castFunction.invokeExact(session, arrayBlock, i);
                }
                catch (Throwable throwable) {
                    // Restore pageBuilder into a consistent state
                    blockBuilder.closeEntry();
                    pageBuilder.declarePosition();
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "Error casting array element to VARCHAR", throwable);
                }
            }

            if (value == null) {
                value = nullReplacement;
                if (value == null) {
                    continue;
                }
            }

            if (needsDelimiter) {
                blockBuilder.writeBytes(delimiter, 0, delimiter.length());
            }
            blockBuilder.writeBytes(value, 0, value.length());
            needsDelimiter = true;
        }

        blockBuilder.closeEntry();
        pageBuilder.declarePosition();
        return VARCHAR.getSlice(blockBuilder, blockBuilder.getPositionCount() - 1);
    }
}
