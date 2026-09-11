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

import com.fasterxml.jackson.core.io.JsonStringEncoder;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionDependencyDeclaration.FunctionDependencyDeclarationBuilder;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.lang.invoke.MethodHandle;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Base64;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.function.BiFunction;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
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
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.roundDiv;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeTemplates.typeVariable;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.DateTimes.toLocalDateTime;
import static io.trino.type.DateTimes.toZonedDateTime;
import static io.trino.type.JsonType.JSON;
import static io.trino.type.UnknownType.UNKNOWN;
import static io.trino.util.Failures.internalError;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.String.format;

public final class FormatFunction
        extends SqlScalarFunction
{
    public static final String FORMAT_FUNCTION_NAME = "$format";

    public static final FormatFunction FORMAT_FUNCTION = new FormatFunction();
    private static final MethodHandle METHOD_HANDLE = methodHandle(FormatFunction.class, "sqlFormat", List.class, ConnectorSession.class, Slice.class, SqlRow.class);
    private static final CatalogSchemaFunctionName JSON_FORMAT_NAME = builtinFunctionName("json_format");
    private static final Pattern JAVA_UTIL_EXCEPTION_PATTERN = Pattern.compile("^java\\.util\\.(\\w+)Exception");

    private FormatFunction()
    {
        super(FunctionMetadata.scalarBuilder(FORMAT_FUNCTION_NAME)
                .signature(Signature.builder()
                        .rowTypeParameter("T")
                        .argumentType(VARCHAR.getTypeDescriptor())
                        .argumentType(typeVariable("T"))
                        .returnType(VARCHAR.getTypeDescriptor())
                        .build())
                .hidden()
                .description("formats the input arguments using a format string")
                .build());
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies(BoundSignature boundSignature)
    {
        FunctionDependencyDeclarationBuilder builder = FunctionDependencyDeclaration.builder();
        ((RowType) boundSignature.getArgumentTypes().get(1)).getFieldTypes()
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
                type instanceof CharType ||
                type instanceof VarbinaryType) {
            return;
        }
        if (type instanceof ArrayType arrayType) {
            addDependencies(builder, arrayType.getElementType());
            return;
        }
        if (type instanceof MapType mapType) {
            addDependencies(builder, mapType.getKeyType());
            addDependencies(builder, mapType.getValueType());
            return;
        }
        if (type instanceof RowType rowType) {
            rowType.getTypeParameters().forEach(t -> addDependencies(builder, t));
            return;
        }

        if (type.equals(JSON)) {
            builder.addFunction(JSON_FORMAT_NAME, ImmutableList.of(JSON));
            return;
        }
        builder.addCast(type, VARCHAR);
    }

    @Override
    public SpecializedSqlScalarFunction specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        RowType rowType = (RowType) boundSignature.getArgumentType(1);

        List<BiFunction<Block, Integer, Object>> converters = rowType.getFieldTypes().stream()
                .map(type -> converter(functionDependencies, type))
                .collect(toImmutableList());

        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL, NEVER_NULL),
                METHOD_HANDLE.bindTo(converters));
    }

    @UsedByGeneratedCode
    public static Slice sqlFormat(List<BiFunction<Block, Integer, Object>> converters, ConnectorSession session, Slice slice, SqlRow row)
    {
        int rawIndex = row.getRawIndex();
        Object[] args = new Object[converters.size()];
        for (int i = 0; i < args.length; i++) {
            args[i] = converters.get(i).apply(row.getRawFieldBlock(i), rawIndex);
        }

        return sqlFormat(session, slice.toStringUtf8(), args);
    }

    private static Slice sqlFormat(ConnectorSession session, String format, Object[] args)
    {
        try {
            return utf8Slice(format(session.getLocale(), format, args));
        }
        catch (IllegalFormatException e) {
            String message = JAVA_UTIL_EXCEPTION_PATTERN.matcher(e.toString()).replaceFirst("$1");
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Invalid format string: %s (%s)", format, message), e);
        }
    }

    private static BiFunction<Block, Integer, Object> converter(FunctionDependencies functionDependencies, Type type)
    {
        BiFunction<Block, Integer, Object> converter = valueConverter(functionDependencies, type);
        return (block, position) -> block.isNull(position) ? null : converter.apply(block, position);
    }

    private static BiFunction<Block, Integer, Object> valueConverter(FunctionDependencies functionDependencies, Type type)
    {
        if (type.equals(UNKNOWN)) {
            return (_, _) -> null;
        }
        if (type.equals(BOOLEAN)) {
            return BOOLEAN::getBoolean;
        }
        if (type.equals(TINYINT)) {
            return (block, position) -> (long) TINYINT.getByte(block, position);
        }
        if (type.equals(SMALLINT)) {
            return (block, position) -> (long) SMALLINT.getShort(block, position);
        }
        if (type.equals(INTEGER)) {
            return (block, position) -> (long) INTEGER.getInt(block, position);
        }
        if (type.equals(BIGINT)) {
            return BIGINT::getLong;
        }
        if (type.equals(REAL)) {
            return REAL::getFloat;
        }
        if (type.equals(DOUBLE)) {
            return DOUBLE::getDouble;
        }
        if (type.equals(DATE)) {
            return (block, position) -> LocalDate.ofEpochDay(DATE.getInt(block, position));
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            return (block, position) -> toZonedDateTime(timestampWithTimeZoneType, block, position);
        }
        if (type instanceof TimestampType timestampType) {
            return (block, position) -> toLocalDateTime(timestampType, block, position);
        }
        if (type instanceof TimeType timeType) {
            return (block, position) -> toLocalTime(timeType.getLong(block, position));
        }
        // TODO: support TIME WITH TIME ZONE by https://github.com/trinodb/trino/issues/191 + mapping to java.time.OffsetTime
        if (type.equals(JSON)) {
            MethodHandle handle = functionDependencies.getScalarFunctionImplementation(JSON_FORMAT_NAME, ImmutableList.of(JSON), simpleConvention(FAIL_ON_NULL, NEVER_NULL)).getMethodHandle();
            return (block, position) -> convertToString(handle, type.getSlice(block, position));
        }
        if (type instanceof DecimalType decimalType) {
            int scale = decimalType.getScale();
            if (decimalType.isShort()) {
                return (block, position) -> BigDecimal.valueOf(decimalType.getLong(block, position), scale);
            }
            return (block, position) -> new BigDecimal(((Int128) decimalType.getObject(block, position)).toBigInteger(), scale);
        }
        if (type instanceof VarcharType varcharType) {
            return (block, position) -> varcharType.getSlice(block, position).toStringUtf8();
        }
        if (type instanceof CharType charType) {
            return (block, position) -> padSpaces(charType.getSlice(block, position), charType).toStringUtf8();
        }
        if (type instanceof RowType rowType) {
            return (block, position) -> rowToString(functionDependencies, rowType, rowType.getObject(block, position));
        }
        if (type instanceof MapType mapType) {
            return (block, position) -> mapToString(functionDependencies, mapType, mapType.getObject(block, position));
        }
        if (type instanceof ArrayType arrayType) {
            return (block, position) -> arrayToString(functionDependencies, arrayType, arrayType.getObject(block, position));
        }
        if (type instanceof VarbinaryType varbinaryType) {
            return (block, position) -> Base64.getEncoder().encodeToString(varbinaryType.getSlice(block, position).getBytes());
        }

        BiFunction<Block, Integer, Object> function;
        if (type.getJavaType() == long.class) {
            function = type::getLong;
        }
        else if (type.getJavaType() == double.class) {
            function = type::getDouble;
        }
        else if (type.getJavaType() == boolean.class) {
            function = type::getBoolean;
        }
        else if (type.getJavaType() == Slice.class) {
            function = type::getSlice;
        }
        else {
            function = type::getObject;
        }

        MethodHandle handle = functionDependencies.getCastImplementation(type, VARCHAR, simpleConvention(FAIL_ON_NULL, NEVER_NULL)).getMethodHandle();
        return (block, position) -> convertToString(handle, function.apply(block, position));
    }

    private static Object quotedValue(FunctionDependencies functionDependencies, Type type, Block block, int position)
    {
        Object value = FormatFunction.converter(functionDependencies, type).apply(block, position);
        if (value != null && (type instanceof VarcharType ||
                type instanceof CharType ||
                type instanceof VarbinaryType ||
                type instanceof UuidType)) {
            return String.format("\"%s\"", new String(JsonStringEncoder.getInstance().quoteAsString((String) value)));
        }
        return value;
    }

    private static String rowToString(FunctionDependencies functionDependencies, RowType rowType, SqlRow row)
    {
        List<RowType.Field> fields = rowType.getFields();
        boolean hasAllFieldNames = fields.stream().allMatch(field -> field.getName().isPresent());
        StringBuilder builder = new StringBuilder(hasAllFieldNames ? "{" : "[");
        int rawIndex = row.getRawIndex();
        for (int i = 0; i < fields.size(); i++) {
            builder.append(i == 0 ? "" : ", ");
            if (hasAllFieldNames) {
                String fieldName = fields.get(i).getName().get();
                builder.append('"').append(new String(JsonStringEncoder.getInstance().quoteAsString(fieldName))).append("\": ");
            }
            builder.append(quotedValue(functionDependencies, fields.get(i).getType(), row.getRawFieldBlock(i), rawIndex));
        }
        return builder.append(hasAllFieldNames ? '}' : ']').toString();
    }

    private static String mapToString(FunctionDependencies functionDependencies, MapType mapType, SqlMap sqlMap)
    {
        StringBuilder builder = new StringBuilder("{");
        Block keys = sqlMap.getRawKeyBlock();
        Block values = sqlMap.getRawValueBlock();
        int rawOffset = sqlMap.getRawOffset();
        for (int i = 0; i < sqlMap.getSize(); i++) {
            builder
                    .append(i == 0 ? "" : ", ")
                    .append(quotedValue(functionDependencies, mapType.getKeyType(), keys, rawOffset + i))
                    .append(": ")
                    .append(quotedValue(functionDependencies, mapType.getValueType(), values, rawOffset + i));
        }
        return builder.append('}').toString();
    }

    private static String arrayToString(FunctionDependencies functionDependencies, ArrayType arrayType, Block elementBlock)
    {
        StringBuilder builder = new StringBuilder("[");
        for (int i = 0; i < elementBlock.getPositionCount(); i++) {
            builder
                    .append(i == 0 ? "" : ", ")
                    .append(quotedValue(functionDependencies, arrayType.getElementType(), elementBlock, i));
        }
        return builder.append(']').toString();
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
