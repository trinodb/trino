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
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DuplicateMapKeyException;
import io.trino.spi.block.SqlMap;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.FunctionNullability;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.Signature;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.type.BlockTypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;
import io.trino.type.BlockTypeOperators.BlockPositionIsIdentical;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.block.MapHashTables.HashBuildMode.STRICT_NOT_DISTINCT_FROM;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.util.Failures.internalError;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.invoke.MethodHandles.dropArguments;
import static java.lang.invoke.MethodHandles.foldArguments;
import static java.lang.invoke.MethodHandles.permuteArguments;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Objects.requireNonNull;

public final class MapToMapCast
        extends SqlScalarFunction
{
    private static final MethodHandle METHOD_HANDLE = methodHandle(
            MapToMapCast.class,
            "mapCast",
            MethodHandle.class,
            MethodHandle.class,
            MapType.class,
            BlockPositionIsIdentical.class,
            BlockPositionHashCode.class,
            ConnectorSession.class,
            SqlMap.class);

    private static final MethodHandle CHECK_LONG_IS_NOT_NULL = methodHandle(MapToMapCast.class, "checkLongIsNotNull", Long.class);
    private static final MethodHandle CHECK_DOUBLE_IS_NOT_NULL = methodHandle(MapToMapCast.class, "checkDoubleIsNotNull", Double.class);
    private static final MethodHandle CHECK_BOOLEAN_IS_NOT_NULL = methodHandle(MapToMapCast.class, "checkBooleanIsNotNull", Boolean.class);
    private static final MethodHandle CHECK_SLICE_IS_NOT_NULL = methodHandle(MapToMapCast.class, "checkSliceIsNotNull", Slice.class);
    private static final MethodHandle CHECK_BLOCK_IS_NOT_NULL = methodHandle(MapToMapCast.class, "checkBlockIsNotNull", Block.class);

    private static final MethodHandle WRITE_LONG = methodHandle(Type.class, "writeLong", BlockBuilder.class, long.class);
    private static final MethodHandle WRITE_DOUBLE = methodHandle(Type.class, "writeDouble", BlockBuilder.class, double.class);
    private static final MethodHandle WRITE_BOOLEAN = methodHandle(Type.class, "writeBoolean", BlockBuilder.class, boolean.class);
    private static final MethodHandle WRITE_OBJECT = methodHandle(Type.class, "writeObject", BlockBuilder.class, Object.class);

    private final BlockTypeOperators blockTypeOperators;

    public MapToMapCast(BlockTypeOperators blockTypeOperators)
    {
        super(FunctionMetadata.operatorBuilder(CAST)
                .signature(Signature.builder()
                        .castableToTypeParameter("FK", new TypeSignature("TK"))
                        .castableToTypeParameter("FV", new TypeSignature("TV"))
                        .typeVariable("TK")
                        .typeVariable("TV")
                        .returnType(mapType(new TypeSignature("TK"), new TypeSignature("TV")))
                        .argumentType(mapType(new TypeSignature("FK"), new TypeSignature("FV")))
                        .build())
                .nullable()
                .build());
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
    public SpecializedSqlScalarFunction specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
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
        BlockPositionIsIdentical keyIdentical = blockTypeOperators.getIdenticalOperator(toKeyType);
        BlockPositionHashCode keyHashCode = blockTypeOperators.getHashCodeOperator(toKeyType);
        MethodHandle target = MethodHandles.insertArguments(METHOD_HANDLE, 0, keyProcessor, valueProcessor, toMapType, keyIdentical, keyHashCode);
        return new ChoicesSpecializedSqlScalarFunction(boundSignature, NULLABLE_RETURN, ImmutableList.of(NEVER_NULL), target);
    }

    /**
     * The signature of the returned MethodHandle is (Block fromMap, int position, ConnectorSession session, BlockBuilder mapBlockBuilder)void.
     * The processor will get the value from fromMap, cast it and write to toBlock.
     */
    private static MethodHandle buildProcessor(FunctionDependencies functionDependencies, Type fromType, Type toType, boolean isKey)
    {
        // Get block position cast, with optional connector session
        FunctionNullability functionNullability = functionDependencies.getCastNullability(fromType, toType);
        InvocationConvention invocationConvention = new InvocationConvention(ImmutableList.of(BLOCK_POSITION), functionNullability.isReturnNullable() ? NULLABLE_RETURN : FAIL_ON_NULL, true, false);
        MethodHandle cast = functionDependencies.getCastImplementation(fromType, toType, invocationConvention).getMethodHandle();
        // Normalize cast to have connector session as first argument
        if (cast.type().parameterArray()[0] != ConnectorSession.class) {
            cast = dropArguments(cast, 0, ConnectorSession.class);
        }
        // Change cast signature to (Block.class, int.class, ConnectorSession.class):T
        cast = permuteArguments(cast, methodType(cast.type().returnType(), Block.class, int.class, ConnectorSession.class), 2, 0, 1);

        // If the key cast function is nullable, check the result is not null
        if (isKey && functionNullability.isReturnNullable()) {
            MethodHandle target = nullChecker(cast.type().returnType());
            cast = foldArguments(dropArguments(target, 1, cast.type().parameterList()), cast);
        }

        // get write method with signature: (T, BlockBuilder.class):void
        MethodHandle writer = nativeValueWriter(toType);
        writer = permuteArguments(writer, methodType(void.class, writer.type().parameterArray()[1], BlockBuilder.class), 1, 0);

        // ensure cast function returns the type expected by the writer
        cast = cast.asType(methodType(writer.type().parameterType(0), cast.type().parameterArray()));

        return foldArguments(dropArguments(writer, 1, cast.type().parameterList()), cast);
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
    private static MethodHandle nullChecker(Class<?> javaType)
    {
        if (javaType == Long.class) {
            return CHECK_LONG_IS_NOT_NULL;
        }
        if (javaType == Double.class) {
            return CHECK_DOUBLE_IS_NOT_NULL;
        }
        if (javaType == Boolean.class) {
            return CHECK_BOOLEAN_IS_NOT_NULL;
        }
        if (javaType == Slice.class) {
            return CHECK_SLICE_IS_NOT_NULL;
        }
        if (javaType == Block.class) {
            return CHECK_BLOCK_IS_NOT_NULL;
        }
        throw new IllegalArgumentException("Unknown java type " + javaType);
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
    public static SqlMap mapCast(
            MethodHandle keyProcessFunction,
            MethodHandle valueProcessFunction,
            MapType toType,
            BlockPositionIsIdentical keyIdenticalOperator,
            BlockPositionHashCode keyHashCode,
            ConnectorSession session,
            SqlMap fromMap)
    {
        int size = fromMap.getSize();
        int rawOffset = fromMap.getRawOffset();
        Block rawKeyBlock = fromMap.getRawKeyBlock();
        Block rawValueBlock = fromMap.getRawValueBlock();

        // Cast the keys into a new block
        Type toKeyType = toType.getKeyType();
        BlockBuilder keyBlockBuilder = toKeyType.createBlockBuilder(null, size);
        for (int i = 0; i < size; i++) {
            try {
                keyProcessFunction.invokeExact(rawKeyBlock, rawOffset + i, session, keyBlockBuilder);
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }
        Block keyBlock = keyBlockBuilder.build();

        // Cast the values into a new block
        Type toValueType = toType.getValueType();
        BlockBuilder valueBlockBuilder = toValueType.createBlockBuilder(null, size);
        for (int i = 0; i < size; i++) {
            if (rawValueBlock.isNull(rawOffset + i)) {
                valueBlockBuilder.appendNull();
                continue;
            }
            try {
                valueProcessFunction.invokeExact(rawValueBlock, rawOffset + i, session, valueBlockBuilder);
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }
        Block valueBlock = valueBlockBuilder.build();

        try {
            return new SqlMap(toType, STRICT_NOT_DISTINCT_FROM, keyBlock, valueBlock);
        }
        catch (DuplicateMapKeyException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "duplicate keys");
        }
    }

    private static MethodHandle nativeValueWriter(Type type)
    {
        Class<?> javaType = type.getJavaType();

        MethodHandle methodHandle;
        if (javaType == long.class) {
            methodHandle = WRITE_LONG;
        }
        else if (javaType == double.class) {
            methodHandle = WRITE_DOUBLE;
        }
        else if (javaType == boolean.class) {
            methodHandle = WRITE_BOOLEAN;
        }
        else if (!javaType.isPrimitive()) {
            methodHandle = WRITE_OBJECT;
        }
        else {
            throw new IllegalArgumentException("Unknown java type " + javaType + " from type " + type);
        }

        return methodHandle.bindTo(type);
    }
}
