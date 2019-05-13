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
import io.prestosql.metadata.FunctionMetadata;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.OperatorNotFoundException;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.tree.QualifiedName;

import java.lang.invoke.MethodHandle;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.function.BiFunction;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.mapWithIndex;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.metadata.FunctionKind.SCALAR;
import static io.prestosql.metadata.Signature.withVariadicBound;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.Chars.isCharType;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.Decimals.decodeUnscaledValue;
import static io.prestosql.spi.type.Decimals.isLongDecimal;
import static io.prestosql.spi.type.Decimals.isShortDecimal;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.type.JsonType.JSON;
import static io.prestosql.type.UnknownType.UNKNOWN;
import static io.prestosql.util.Failures.internalError;
import static io.prestosql.util.Reflection.methodHandle;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class FormatFunction
        extends SqlScalarFunction
{
    public static final String NAME = "$format";

    public static final FormatFunction FORMAT_FUNCTION = new FormatFunction();
    private static final MethodHandle METHOD_HANDLE = methodHandle(FormatFunction.class, "sqlFormat", List.class, ConnectorSession.class, Slice.class, Block.class);

    private FormatFunction()
    {
        super(new FunctionMetadata(
                Signature.builder()
                        .kind(SCALAR)
                        .name(NAME)
                        .typeVariableConstraints(withVariadicBound("T", "row"))
                        .argumentTypes(VARCHAR.getTypeSignature(), new TypeSignature("T"))
                        .returnType(VARCHAR.getTypeSignature())
                        .build(),
                true,
                true,
                "formats the input arguments using a format string"));
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, Metadata metadata)
    {
        Type rowType = boundVariables.getTypeVariable("T");

        List<BiFunction<ConnectorSession, Block, Object>> converters = mapWithIndex(
                rowType.getTypeParameters().stream(),
                (type, index) -> converter(metadata, type, toIntExact(index)))
                .collect(toImmutableList());

        return new ScalarFunctionImplementation(
                false,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                METHOD_HANDLE.bindTo(converters));
    }

    public static void validateType(Metadata metadata, Type type)
    {
        valueConverter(metadata, type, 0);
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
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Invalid format string: %s (%s)", format, message), e);
        }
    }

    private static BiFunction<ConnectorSession, Block, Object> converter(Metadata metadata, Type type, int position)
    {
        BiFunction<ConnectorSession, Block, Object> converter = valueConverter(metadata, type, position);
        return (session, block) -> block.isNull(position) ? null : converter.apply(session, block);
    }

    private static BiFunction<ConnectorSession, Block, Object> valueConverter(Metadata metadata, Type type, int position)
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
        if (type.equals(TIMESTAMP_WITH_TIME_ZONE)) {
            return (session, block) -> toZonedDateTime(type.getLong(block, position));
        }
        if (type.equals(TIMESTAMP)) {
            return (session, block) -> toLocalDateTime(session, type.getLong(block, position));
        }
        if (type.equals(TIME)) {
            return (session, block) -> toLocalTime(session, type.getLong(block, position));
        }
        // TODO: support TIME WITH TIME ZONE by making SqlTimeWithTimeZone implement TemporalAccessor
        if (type.equals(JSON)) {
            ResolvedFunction function = metadata.resolveFunction(QualifiedName.of("json_format"), fromTypes(JSON));
            MethodHandle handle = metadata.getScalarFunctionImplementation(function).getMethodHandle();
            return (session, block) -> convertToString(handle, type.getSlice(block, position));
        }
        if (isShortDecimal(type)) {
            int scale = ((DecimalType) type).getScale();
            return (session, block) -> BigDecimal.valueOf(type.getLong(block, position), scale);
        }
        if (isLongDecimal(type)) {
            int scale = ((DecimalType) type).getScale();
            return (session, block) -> new BigDecimal(decodeUnscaledValue(type.getSlice(block, position)), scale);
        }
        if (isVarcharType(type) || isCharType(type)) {
            return (session, block) -> type.getSlice(block, position).toStringUtf8();
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

        MethodHandle handle = castToVarchar(metadata, type);
        if ((handle == null) || (handle.type().parameterCount() != 1)) {
            throw new PrestoException(NOT_SUPPORTED, "Type not supported for formatting: " + type.getDisplayName());
        }

        return (session, block) -> convertToString(handle, function.apply(session, block));
    }

    private static MethodHandle castToVarchar(Metadata metadata, Type type)
    {
        try {
            ResolvedFunction cast = metadata.getCoercion(type, VARCHAR);
            return metadata.getScalarFunctionImplementation(cast).getMethodHandle();
        }
        catch (OperatorNotFoundException e) {
            return null;
        }
    }

    private static ZonedDateTime toZonedDateTime(long value)
    {
        Instant instant = Instant.ofEpochMilli(unpackMillisUtc(value));
        ZoneId zoneId = ZoneId.of(unpackZoneKey(value).getId());
        return ZonedDateTime.ofInstant(instant, zoneId);
    }

    private static LocalDateTime toLocalDateTime(ConnectorSession session, long value)
    {
        Instant instant = Instant.ofEpochMilli(value);
        if (session.isLegacyTimestamp()) {
            ZoneId zoneId = ZoneId.of(session.getTimeZoneKey().getId());
            return LocalDateTime.ofInstant(instant, zoneId);
        }
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private static LocalTime toLocalTime(ConnectorSession session, long value)
    {
        if (session.isLegacyTimestamp()) {
            Instant instant = Instant.ofEpochMilli(value);
            ZoneId zoneId = ZoneId.of(session.getTimeZoneKey().getId());
            return ZonedDateTime.ofInstant(instant, zoneId).toLocalTime();
        }
        return LocalTime.ofNanoOfDay(MILLISECONDS.toNanos(value));
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
