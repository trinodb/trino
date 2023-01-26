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
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionDependencyDeclaration.FunctionDependencyDeclarationBuilder;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.QualifiedFunctionName;
import io.trino.spi.function.Signature;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarcharType;

import java.lang.invoke.MethodHandle;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.function.BiFunction;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.mapWithIndex;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.padSpaces;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.Timestamps.roundDiv;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.DateTimes.PICOSECONDS_PER_NANOSECOND;
import static io.trino.type.DateTimes.toLocalDateTime;
import static io.trino.type.DateTimes.toZonedDateTime;
import static io.trino.type.JsonType.JSON;
import static io.trino.type.UnknownType.UNKNOWN;
import static io.trino.util.Failures.internalError;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

public final class FormatFunction
        extends SqlScalarFunction
{
    public static final String NAME = "$format";

    public static final FormatFunction FORMAT_FUNCTION = new FormatFunction();
    private static final MethodHandle METHOD_HANDLE = methodHandle(FormatFunction.class, "sqlFormat", List.class, ConnectorSession.class, Slice.class, Block.class);

    private FormatFunction()
    {
        super(FunctionMetadata.scalarBuilder()
                .signature(Signature.builder()
                        .name(NAME)
                        .variadicTypeParameter("T", "row")
                        .argumentType(VARCHAR.getTypeSignature())
                        .argumentType(new TypeSignature("T"))
                        .returnType(VARCHAR.getTypeSignature())
                        .build())
                .hidden()
                .description("formats the input arguments using a format string")
                .build());
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies(BoundSignature boundSignature)
    {
        FunctionDependencyDeclarationBuilder builder = FunctionDependencyDeclaration.builder();
        boundSignature.getArgumentTypes().get(1).getTypeParameters()
                .forEach(type -> addDependencies(builder, type));
        return builder.build();
    }

    private static void addDependencies(FunctionDependencyDeclarationBuilder builder, Type type)
    {
        if (type.equals(UNKNOWN) ||
                type.equals(BOOLEAN) ||
                type.equals(TINYINT) ||
                type.equals(SMALLINT) ||
                type.equals(INTEGER) ||
                type.equals(BIGINT) ||
                type.equals(REAL) ||
                type.equals(DOUBLE) ||
                type.equals(DATE) ||
                type instanceof TimestampWithTimeZoneType ||
                type instanceof TimestampType ||
                type instanceof TimeType ||
                type instanceof DecimalType ||
                type instanceof VarcharType ||
                type instanceof CharType) {
            return;
        }

        if (type.equals(JSON)) {
            builder.addFunction(QualifiedFunctionName.of("json_format"), ImmutableList.of(JSON));
            return;
        }
        builder.addCast(type, VARCHAR);
    }

    @Override
    public SpecializedSqlScalarFunction specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        Type rowType = boundSignature.getArgumentType(1);

        List<BiFunction<ConnectorSession, Block, Object>> converters = mapWithIndex(
                rowType.getTypeParameters().stream(),
                (type, index) -> converter(functionDependencies, type, toIntExact(index)))
                .collect(toImmutableList());

        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL, NEVER_NULL),
                METHOD_HANDLE.bindTo(converters));
    }

    @UsedByGeneratedCode
    public static Slice sqlFormat(List<BiFunction<ConnectorSession, Block, Object>> converters, ConnectorSession session, Slice slice, Block row)
    {
        Object[] args = new Object[converters.size()];
        for (int i = 0; i < args.length; i++) {
            args[i] = converters.get(i).apply(session, row);
        }

        return sqlFormat(session, slice.toStringUtf8(), args);
    }

    private static Slice sqlFormat(ConnectorSession session, String format, Object[] args)
    {
        try {
            return utf8Slice(format(session.getLocale(), format, args));
        }
        catch (IllegalFormatException e) {
            String message = e.toString().replaceFirst("^java\\.util\\.(\\w+)Exception", "$1");
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Invalid format string: %s (%s)", format, message), e);
        }
    }

    private static BiFunction<ConnectorSession, Block, Object> converter(FunctionDependencies functionDependencies, Type type, int position)
    {
        BiFunction<ConnectorSession, Block, Object> converter = valueConverter(functionDependencies, type, position);
        return (session, block) -> block.isNull(position) ? null : converter.apply(session, block);
    }

    private static BiFunction<ConnectorSession, Block, Object> valueConverter(FunctionDependencies functionDependencies, Type type, int position)
    {
        if (type.equals(UNKNOWN)) {
            return (session, block) -> null;
        }
        if (type.equals(BOOLEAN)) {
            return (session, block) -> type.getBoolean(block, position);
        }
        if (type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER) || type.equals(BIGINT)) {
            return (session, block) -> type.getLong(block, position);
        }
        if (type.equals(REAL)) {
            return (session, block) -> intBitsToFloat(toIntExact(type.getLong(block, position)));
        }
        if (type.equals(DOUBLE)) {
            return (session, block) -> type.getDouble(block, position);
        }
        if (type.equals(DATE)) {
            return (session, block) -> LocalDate.ofEpochDay(type.getLong(block, position));
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            return (session, block) -> toZonedDateTime(timestampWithTimeZoneType, block, position);
        }
        if (type instanceof TimestampType timestampType) {
            return (session, block) -> toLocalDateTime(timestampType, block, position);
        }
        if (type instanceof TimeType) {
            return (session, block) -> toLocalTime(type.getLong(block, position));
        }
        // TODO: support TIME WITH TIME ZONE by https://github.com/trinodb/trino/issues/191 + mapping to java.time.OffsetTime
        if (type.equals(JSON)) {
            MethodHandle handle = functionDependencies.getScalarFunctionImplementation(QualifiedFunctionName.of("json_format"), ImmutableList.of(JSON), simpleConvention(FAIL_ON_NULL, NEVER_NULL)).getMethodHandle();
            return (session, block) -> convertToString(handle, type.getSlice(block, position));
        }
        if (type instanceof DecimalType decimalType) {
            int scale = decimalType.getScale();
            if (decimalType.isShort()) {
                return (session, block) -> BigDecimal.valueOf(type.getLong(block, position), scale);
            }
            return (session, block) -> new BigDecimal(((Int128) type.getObject(block, position)).toBigInteger(), scale);
        }
        if (type instanceof VarcharType) {
            return (session, block) -> type.getSlice(block, position).toStringUtf8();
        }
        if (type instanceof CharType charType) {
            return (session, block) -> padSpaces(type.getSlice(block, position), charType).toStringUtf8();
        }

        BiFunction<ConnectorSession, Block, Object> function;
        if (type.getJavaType() == long.class) {
            function = (session, block) -> type.getLong(block, position);
        }
        else if (type.getJavaType() == double.class) {
            function = (session, block) -> type.getDouble(block, position);
        }
        else if (type.getJavaType() == boolean.class) {
            function = (session, block) -> type.getBoolean(block, position);
        }
        else if (type.getJavaType() == Slice.class) {
            function = (session, block) -> type.getSlice(block, position);
        }
        else {
            function = (session, block) -> type.getObject(block, position);
        }

        MethodHandle handle = functionDependencies.getCastImplementation(type, VARCHAR, simpleConvention(FAIL_ON_NULL, NEVER_NULL)).getMethodHandle();
        return (session, block) -> convertToString(handle, function.apply(session, block));
    }

    private static LocalTime toLocalTime(long value)
    {
        long nanoOfDay = roundDiv(value, PICOSECONDS_PER_NANOSECOND);
        return LocalTime.ofNanoOfDay(nanoOfDay);
    }

    private static Object convertToString(MethodHandle handle, Object value)
    {
        try {
            return ((Slice) handle.invoke(value)).toStringUtf8();
        }
        catch (Throwable t) {
            throw internalError(t);
        }
    }
}
