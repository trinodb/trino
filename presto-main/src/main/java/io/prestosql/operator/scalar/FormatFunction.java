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
import io.prestosql.metadata.FunctionArgumentDefinition;
import io.prestosql.metadata.FunctionBinding;
import io.prestosql.metadata.FunctionDependencies;
import io.prestosql.metadata.FunctionDependencyDeclaration;
import io.prestosql.metadata.FunctionDependencyDeclaration.FunctionDependencyDeclarationBuilder;
import io.prestosql.metadata.FunctionMetadata;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.TimeType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.tree.QualifiedName;

import java.lang.invoke.MethodHandle;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.mapWithIndex;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.metadata.FunctionKind.SCALAR;
import static io.prestosql.metadata.Signature.withVariadicBound;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.Chars.padSpaces;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.Decimals.decodeUnscaledValue;
import static io.prestosql.spi.type.Decimals.isLongDecimal;
import static io.prestosql.spi.type.Decimals.isShortDecimal;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.Timestamps.roundDiv;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_NANOSECOND;
import static io.prestosql.type.DateTimes.toLocalDateTime;
import static io.prestosql.type.DateTimes.toZonedDateTime;
import static io.prestosql.type.JsonType.JSON;
import static io.prestosql.type.UnknownType.UNKNOWN;
import static io.prestosql.util.Failures.internalError;
import static io.prestosql.util.Reflection.methodHandle;
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
        super(new FunctionMetadata(
                Signature.builder()
                        .name(NAME)
                        .typeVariableConstraints(withVariadicBound("T", "row"))
                        .argumentTypes(VARCHAR.getTypeSignature(), new TypeSignature("T"))
                        .returnType(VARCHAR.getTypeSignature())
                        .build(),
                false,
                ImmutableList.of(new FunctionArgumentDefinition(false), new FunctionArgumentDefinition(false)),
                true,
                true,
                "formats the input arguments using a format string",
                SCALAR));
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies(FunctionBinding functionBinding)
    {
        FunctionDependencyDeclarationBuilder builder = FunctionDependencyDeclaration.builder();
        functionBinding.getTypeVariable("T").getTypeParameters()
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
                isShortDecimal(type) ||
                isLongDecimal(type) ||
                type instanceof VarcharType ||
                type instanceof CharType) {
            return;
        }

        if (type.equals(JSON)) {
            builder.addFunction(QualifiedName.of("json_format"), ImmutableList.of(JSON));
            return;
        }
        builder.addCast(type, VARCHAR);
    }

    @Override
    public ScalarFunctionImplementation specialize(FunctionBinding functionBinding, FunctionDependencies functionDependencies)
    {
        Type rowType = functionBinding.getTypeVariable("T");

        List<BiFunction<ConnectorSession, Block, Object>> converters = mapWithIndex(
                rowType.getTypeParameters().stream(),
                (type, index) -> converter(functionDependencies, type, toIntExact(index)))
                .collect(toImmutableList());

        return new ScalarFunctionImplementation(
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
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Invalid format string: %s (%s)", format, message), e);
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
        if (type instanceof TimestampWithTimeZoneType) {
            return (session, block) -> toZonedDateTime(((TimestampWithTimeZoneType) type), block, position);
        }
        if (type instanceof TimestampType) {
            return (session, block) -> toLocalDateTime(((TimestampType) type), block, position);
        }
        if (type instanceof TimeType) {
            return (session, block) -> toLocalTime(type.getLong(block, position));
        }
        // TODO: support TIME WITH TIME ZONE by https://github.com/prestosql/presto/issues/191 + mapping to java.time.OffsetTime
        if (type.equals(JSON)) {
            MethodHandle handle = functionDependencies.getFunctionInvoker(QualifiedName.of("json_format"), ImmutableList.of(JSON), Optional.empty()).getMethodHandle();
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
        if (type instanceof VarcharType) {
            return (session, block) -> type.getSlice(block, position).toStringUtf8();
        }
        if (type instanceof CharType) {
            CharType charType = (CharType) type;
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

        MethodHandle handle = functionDependencies.getCastInvoker(type, VARCHAR, Optional.empty()).getMethodHandle();
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
