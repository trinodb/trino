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
import io.trino.metadata.BoundSignature;
import io.trino.metadata.FunctionDependencies;
import io.trino.metadata.FunctionDependencyDeclaration;
import io.trino.metadata.FunctionNullability;
import io.trino.metadata.SqlOperator;
import io.trino.operator.aggregation.TypedSet;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.type.BlockTypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;
import io.trino.type.BlockTypeOperators.BlockPositionIsDistinctFrom;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.metadata.Signature.castableToTypeParameter;
import static io.trino.metadata.Signature.typeVariable;
import static io.trino.operator.aggregation.TypedSet.createDistinctTypedSet;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.block.MethodHandleUtil.compose;
import static io.trino.spi.block.MethodHandleUtil.nativeValueWriter;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.util.Failures.internalError;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.invoke.MethodHandles.permuteArguments;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Objects.requireNonNull;

public final class MapToMapCast
        extends SqlOperator
{
    private static final MethodHandle METHOD_HANDLE = methodHandle(
            MapToMapCast.class,
            "mapCast",
            MethodHandle.class,
            MethodHandle.class,
            Type.class,
            BlockPositionIsDistinctFrom.class,
            BlockPositionHashCode.class,
            ConnectorSession.class,
            Block.class);

    private static final MethodHandle CHECK_LONG_IS_NOT_NULL = methodHandle(MapToMapCast.class, "checkLongIsNotNull", Long.class);
    private static final MethodHandle CHECK_DOUBLE_IS_NOT_NULL = methodHandle(MapToMapCast.class, "checkDoubleIsNotNull", Double.class);
    private static final MethodHandle CHECK_BOOLEAN_IS_NOT_NULL = methodHandle(MapToMapCast.class, "checkBooleanIsNotNull", Boolean.class);
    private static final MethodHandle CHECK_SLICE_IS_NOT_NULL = methodHandle(MapToMapCast.class, "checkSliceIsNotNull", Slice.class);
    private static final MethodHandle CHECK_BLOCK_IS_NOT_NULL = methodHandle(MapToMapCast.class, "checkBlockIsNotNull", Block.class);

    private final BlockTypeOperators blockTypeOperators;

    public MapToMapCast(BlockTypeOperators blockTypeOperators)
    {
        super(CAST,
                ImmutableList.of(
                        castableToTypeParameter("FK", new TypeSignature("TK")),
                        castableToTypeParameter("FV", new TypeSignature("TV")),
                        typeVariable("TK"),
                        typeVariable("TV")),
                ImmutableList.of(),
                mapType(new TypeSignature("TK"), new TypeSignature("TV")),
                ImmutableList.of(mapType(new TypeSignature("FK"), new TypeSignature("FV"))),
                true);
        this.blockTypeOperators = requireNonNull(blockTypeOperators, "blockTypeOperators is null");
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies()
    {
        return FunctionDependencyDeclaration.builder()
                .addCastSignature(new TypeSignature("FK"), new TypeSignature("TK"))
                .addCastSignature(new TypeSignature("FV"), new TypeSignature("TV"))
                .build();
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        checkArgument(boundSignature.getArity() == 1, "Expected arity to be 1");
        MapType fromMapType = (MapType) boundSignature.getArgumentType(0);
        Type fromKeyType = fromMapType.getKeyType();
        Type fromValueType = fromMapType.getValueType();
        MapType toMapType = (MapType) boundSignature.getReturnType();
        Type toKeyType = toMapType.getKeyType();
        Type toValueType = toMapType.getValueType();

        MethodHandle keyProcessor = buildProcessor(functionDependencies, fromKeyType, toKeyType, true);
        MethodHandle valueProcessor = buildProcessor(functionDependencies, fromValueType, toValueType, false);
        BlockPositionIsDistinctFrom keyEqual = blockTypeOperators.getDistinctFromOperator(toKeyType);
        BlockPositionHashCode keyHashCode = blockTypeOperators.getHashCodeOperator(toKeyType);
        MethodHandle target = MethodHandles.insertArguments(METHOD_HANDLE, 0, keyProcessor, valueProcessor, toMapType, keyEqual, keyHashCode);
        return new ChoicesScalarFunctionImplementation(boundSignature, NULLABLE_RETURN, ImmutableList.of(NEVER_NULL), target);
    }

    /**
     * The signature of the returned MethodHandle is (Block fromMap, int position, ConnectorSession session, BlockBuilder mapBlockBuilder)void.
     * The processor will get the value from fromMap, cast it and write to toBlock.
     */
    private MethodHandle buildProcessor(FunctionDependencies functionDependencies, Type fromType, Type toType, boolean isKey)
    {
        // Get block position cast, with optional connector session
        FunctionNullability functionNullability = functionDependencies.getCastNullability(fromType, toType);
        InvocationConvention invocationConvention = new InvocationConvention(ImmutableList.of(BLOCK_POSITION), functionNullability.isReturnNullable() ? NULLABLE_RETURN : FAIL_ON_NULL, true, false);
        MethodHandle cast = functionDependencies.getCastInvoker(fromType, toType, invocationConvention).getMethodHandle();
        // Normalize cast to have connector session as first argument
        if (cast.type().parameterArray()[0] != ConnectorSession.class) {
            cast = MethodHandles.dropArguments(cast, 0, ConnectorSession.class);
        }
        // Change cast signature to (Block.class, int.class, ConnectorSession.class):T
        cast = permuteArguments(cast, methodType(cast.type().returnType(), Block.class, int.class, ConnectorSession.class), 2, 0, 1);

        // If the key cast function is nullable, check the result is not null
        if (isKey && functionNullability.isReturnNullable()) {
            cast = compose(nullChecker(cast.type().returnType()), cast);
        }

        // get write method with signature: (T, BlockBuilder.class):void
        MethodHandle writer = nativeValueWriter(toType);
        writer = permuteArguments(writer, methodType(void.class, writer.type().parameterArray()[1], BlockBuilder.class), 1, 0);

        // ensure cast returns type expected by the writer
        cast = cast.asType(methodType(writer.type().parameterType(0), cast.type().parameterArray()));

        return compose(writer, cast);
    }

    /**
     * Returns a null checker MethodHandle that only returns the value when it is not null.
     * <p>
     * The signature of the returned MethodHandle could be one of the following depending on javaType:
     * <ul>
     * <li>(Long value)long
     * <li>(Double value)double
     * <li>(Boolean value)boolean
     * <li>(Slice value)Slice
     * <li>(Block value)Block
     * </ul>
     */
    private MethodHandle nullChecker(Class<?> javaType)
    {
        if (javaType == Long.class) {
            return CHECK_LONG_IS_NOT_NULL;
        }
        else if (javaType == Double.class) {
            return CHECK_DOUBLE_IS_NOT_NULL;
        }
        else if (javaType == Boolean.class) {
            return CHECK_BOOLEAN_IS_NOT_NULL;
        }
        else if (javaType == Slice.class) {
            return CHECK_SLICE_IS_NOT_NULL;
        }
        else if (javaType == Block.class) {
            return CHECK_BLOCK_IS_NOT_NULL;
        }
        else {
            throw new IllegalArgumentException("Unknown java type " + javaType);
        }
    }

    @UsedByGeneratedCode
    public static long checkLongIsNotNull(Long value)
    {
        if (value == null) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "map key is null");
        }
        return value;
    }

    @UsedByGeneratedCode
    public static double checkDoubleIsNotNull(Double value)
    {
        if (value == null) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "map key is null");
        }
        return value;
    }

    @UsedByGeneratedCode
    public static boolean checkBooleanIsNotNull(Boolean value)
    {
        if (value == null) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "map key is null");
        }
        return value;
    }

    @UsedByGeneratedCode
    public static Slice checkSliceIsNotNull(Slice value)
    {
        if (value == null) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "map key is null");
        }
        return value;
    }

    @UsedByGeneratedCode
    public static Block checkBlockIsNotNull(Block value)
    {
        if (value == null) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "map key is null");
        }
        return value;
    }

    @UsedByGeneratedCode
    public static Block mapCast(
            MethodHandle keyProcessFunction,
            MethodHandle valueProcessFunction,
            Type targetType,
            BlockPositionIsDistinctFrom keyDistinctOperator,
            BlockPositionHashCode keyHashCode,
            ConnectorSession session,
            Block fromMap)
    {
        checkState(targetType.getTypeParameters().size() == 2, "Expect two type parameters for targetType");
        Type toKeyType = targetType.getTypeParameters().get(0);
        TypedSet resultKeys = createDistinctTypedSet(toKeyType, keyDistinctOperator, keyHashCode, fromMap.getPositionCount() / 2, "map-to-map cast");

        // Cast the keys into a new block
        BlockBuilder keyBlockBuilder = toKeyType.createBlockBuilder(null, fromMap.getPositionCount() / 2);
        for (int i = 0; i < fromMap.getPositionCount(); i += 2) {
            try {
                keyProcessFunction.invokeExact(fromMap, i, session, keyBlockBuilder);
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }
        Block keyBlock = keyBlockBuilder.build();

        BlockBuilder mapBlockBuilder = targetType.createBlockBuilder(null, 1);
        BlockBuilder blockBuilder = mapBlockBuilder.beginBlockEntry();
        for (int i = 0; i < fromMap.getPositionCount(); i += 2) {
            if (resultKeys.add(keyBlock, i / 2)) {
                toKeyType.appendTo(keyBlock, i / 2, blockBuilder);
                if (fromMap.isNull(i + 1)) {
                    blockBuilder.appendNull();
                    continue;
                }

                try {
                    valueProcessFunction.invokeExact(fromMap, i + 1, session, blockBuilder);
                }
                catch (Throwable t) {
                    throw internalError(t);
                }
            }
            else {
                // if there are duplicated keys, fail it!
                throw new TrinoException(INVALID_CAST_ARGUMENT, "duplicate keys");
            }
        }

        mapBlockBuilder.closeEntry();
        return (Block) targetType.getObject(mapBlockBuilder, mapBlockBuilder.getPositionCount() - 1);
    }
}
