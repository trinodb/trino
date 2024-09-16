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
package io.trino.plugin.bigquery;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.cloud.bigquery.Field.Mode.REPEATED;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.bigquery.BigQueryMetadata.DEFAULT_NUMERIC_TYPE_PRECISION;
import static io.trino.plugin.bigquery.BigQueryMetadata.DEFAULT_NUMERIC_TYPE_SCALE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TimeWithTimeZoneType.DEFAULT_PRECISION;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_SECOND;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Integer.parseInt;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class BigQueryTypeManager
{
    private static final int[] NANO_FACTOR = {
            -1, // 0, no need to multiply
            100_000_000, // 1 digit after the dot
            10_000_000, // 2 digits after the dot
            1_000_000, // 3 digits after the dot
            100_000, // 4 digits after the dot
            10_000, // 5 digits after the dot
            1000, // 6 digits after the dot
            100, // 7 digits after the dot
            10, // 8 digits after the dot
            1, // 9 digits after the dot
    };
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("''HH:mm:ss.SSSSSS''");
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSS").withZone(UTC);

    private final Type jsonType;

    @Inject
    public BigQueryTypeManager(TypeManager typeManager)
    {
        jsonType = requireNonNull(typeManager, "typeManager is null").getType(new TypeSignature(JSON));
    }

    private RowType.Field toRawTypeField(String name, Field field)
    {
        Type trinoType = convertToTrinoType(field).orElseThrow(() -> new IllegalArgumentException("Unsupported column " + field)).type();
        return RowType.field(name, field.getMode() == REPEATED ? new ArrayType(trinoType) : trinoType);
    }

    @VisibleForTesting
    public static LocalDateTime toLocalDateTime(String datetime)
    {
        int dotPosition = datetime.indexOf('.');
        if (dotPosition == -1) {
            // no sub-second element
            return LocalDateTime.from(DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(datetime));
        }
        LocalDateTime result = LocalDateTime.from(DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(datetime.substring(0, dotPosition)));
        // has sub-second element, so convert to nanosecond
        String nanosStr = datetime.substring(dotPosition + 1);
        int nanoOfSecond = parseInt(nanosStr) * NANO_FACTOR[nanosStr.length()];
        return result.withNano(nanoOfSecond);
    }

    public static long toTrinoTimestamp(String datetime)
    {
        Instant instant = toLocalDateTime(datetime).toInstant(UTC);
        return (instant.getEpochSecond() * MICROSECONDS_PER_SECOND) + (instant.getNano() / NANOSECONDS_PER_MICROSECOND);
    }

    private static String floatToStringConverter(Object value)
    {
        return format("CAST('%s' AS float64)", value);
    }

    private static String simpleToStringConverter(Object value)
    {
        return String.valueOf(value);
    }

    @VisibleForTesting
    public static String dateToStringConverter(Object value)
    {
        LocalDate date = LocalDate.ofEpochDay((long) value);
        return "'" + date + "'";
    }

    public static String datetimeToStringConverter(Object value)
    {
        long epochMicros = (long) value;
        long epochSeconds = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
        int nanoAdjustment = floorMod(epochMicros, MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND;
        return formatTimestamp(epochSeconds, nanoAdjustment, UTC);
    }

    @VisibleForTesting
    public static String timeToStringConverter(Object value)
    {
        long time = (long) value;
        verify(0 <= time, "Invalid time value: %s", time);
        long epochSeconds = time / PICOSECONDS_PER_SECOND;
        long nanoAdjustment = (time % PICOSECONDS_PER_SECOND) / PICOSECONDS_PER_NANOSECOND;
        return TIME_FORMATTER.format(toZonedDateTime(epochSeconds, nanoAdjustment, UTC));
    }

    public static String timestampToStringConverter(Object value)
    {
        LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) value;
        long epochMillis = timestamp.getEpochMillis();
        long epochSeconds = floorDiv(epochMillis, MILLISECONDS_PER_SECOND);
        int nanoAdjustment = floorMod(epochMillis, MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND + timestamp.getPicosOfMilli() / PICOSECONDS_PER_NANOSECOND;
        ZoneId zoneId = getTimeZoneKey(timestamp.getTimeZoneKey()).getZoneId();
        return formatTimestamp(epochSeconds, nanoAdjustment, zoneId);
    }

    private static String formatTimestamp(long epochSeconds, long nanoAdjustment, ZoneId zoneId)
    {
        return DATETIME_FORMATTER.format(toZonedDateTime(epochSeconds, nanoAdjustment, zoneId));
    }

    public static ZonedDateTime toZonedDateTime(long epochSeconds, long nanoAdjustment, ZoneId zoneId)
    {
        Instant instant = Instant.ofEpochSecond(epochSeconds, nanoAdjustment);
        return ZonedDateTime.ofInstant(instant, zoneId);
    }

    static String stringToStringConverter(Object value)
    {
        Slice slice = (Slice) value;
        return "'%s'".formatted(slice.toStringUtf8()
                .replace("\\", "\\\\")
                .replace("\n", "\\n")
                .replace("'", "\\'"));
    }

    static String numericToStringConverter(Object value)
    {
        return Decimals.toString((Int128) value, DEFAULT_NUMERIC_TYPE_SCALE);
    }

    static String bytesToStringConverter(Object value)
    {
        Slice slice = (Slice) value;
        return format("FROM_BASE64('%s')", Base64.getEncoder().encodeToString(slice.getBytes()));
    }

    public Field toField(String name, Type type, @Nullable String comment)
    {
        if (type instanceof ArrayType) {
            Type elementType = ((ArrayType) type).getElementType();
            return toInnerField(name, elementType, true, comment);
        }
        return toInnerField(name, type, false, comment);
    }

    private Field toInnerField(String name, Type type, boolean repeated, @Nullable String comment)
    {
        Field.Builder builder;
        if (type instanceof RowType) {
            builder = Field.newBuilder(name, StandardSQLTypeName.STRUCT, toFieldList((RowType) type)).setDescription(comment);
        }
        else {
            builder = Field.newBuilder(name, toStandardSqlTypeName(type)).setDescription(comment);
        }
        if (repeated) {
            builder = builder.setMode(REPEATED);
        }
        return builder.build();
    }

    private FieldList toFieldList(RowType rowType)
    {
        ImmutableList.Builder<Field> fields = ImmutableList.builder();
        for (RowType.Field field : rowType.getFields()) {
            String fieldName = field.getName()
                    .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "ROW type does not have field names declared: " + rowType));
            fields.add(toField(fieldName, field.getType(), null));
        }
        return FieldList.of(fields.build());
    }

    StandardSQLTypeName toStandardSqlTypeName(Type type)
    {
        if (type == BooleanType.BOOLEAN) {
            return StandardSQLTypeName.BOOL;
        }
        if (type == TinyintType.TINYINT || type == SmallintType.SMALLINT || type == IntegerType.INTEGER || type == BigintType.BIGINT) {
            return StandardSQLTypeName.INT64;
        }
        if (type == DoubleType.DOUBLE) {
            return StandardSQLTypeName.FLOAT64;
        }
        if (type instanceof DecimalType) {
            return StandardSQLTypeName.NUMERIC;
        }
        if (type == DateType.DATE) {
            return StandardSQLTypeName.DATE;
        }
        if (type == createTimeWithTimeZoneType(DEFAULT_PRECISION)) {
            return StandardSQLTypeName.TIME;
        }
        if (type == TimestampType.TIMESTAMP_MICROS) {
            return StandardSQLTypeName.DATETIME;
        }
        if (type == TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS) {
            return StandardSQLTypeName.TIMESTAMP;
        }
        if (type instanceof VarcharType) {
            return StandardSQLTypeName.STRING;
        }
        if (type == VarbinaryType.VARBINARY) {
            return StandardSQLTypeName.BYTES;
        }
        if (type instanceof ArrayType) {
            return StandardSQLTypeName.ARRAY;
        }
        if (type instanceof RowType) {
            return StandardSQLTypeName.STRUCT;
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    public static String convertToString(Type type, StandardSQLTypeName bigqueryType, Object value)
    {
        switch (bigqueryType) {
            case BOOL:
                return simpleToStringConverter(value);
            case BYTES:
                return bytesToStringConverter(value);
            case DATE:
                return dateToStringConverter(value);
            case DATETIME:
                return "'%s'".formatted(datetimeToStringConverter(value));
            case FLOAT64:
                return floatToStringConverter(value);
            case INT64:
                return simpleToStringConverter(value);
            case NUMERIC:
            case BIGNUMERIC:
                String bigqueryTypeName = bigqueryType.name();
                DecimalType decimalType = (DecimalType) type;
                if (decimalType.isShort()) {
                    return format("%s '%s'", bigqueryTypeName, Decimals.toString((long) value, ((DecimalType) type).getScale()));
                }
                return format("%s '%s'", bigqueryTypeName, Decimals.toString((Int128) value, ((DecimalType) type).getScale()));
            case STRING:
                return stringToStringConverter(value);
            case TIME:
                return timeToStringConverter(value);
            case TIMESTAMP:
                return "'%s'".formatted(timestampToStringConverter(value));
            default:
                throw new IllegalArgumentException("Unsupported type: " + bigqueryType);
        }
    }

    public Optional<ColumnMapping> toTrinoType(Field field)
    {
        return convertToTrinoType(field)
                .map(columnMapping -> field.getMode() == REPEATED ?
                        new ColumnMapping(new ArrayType(columnMapping.type()), false) :
                        columnMapping);
    }

    private Optional<ColumnMapping> convertToTrinoType(Field field)
    {
        switch (field.getType().getStandardType()) {
            case BOOL:
                return Optional.of(new ColumnMapping(BooleanType.BOOLEAN, true));
            case INT64:
                return Optional.of(new ColumnMapping(BigintType.BIGINT, true));
            case FLOAT64:
                return Optional.of(new ColumnMapping(DoubleType.DOUBLE, true));
            case NUMERIC:
            case BIGNUMERIC:
                Long precision = field.getPrecision();
                Long scale = field.getScale();
                // Unsupported BIGNUMERIC types (precision > 38) are filtered in BigQueryClient.getColumns
                if (precision != null && scale != null) {
                    return Optional.of(new ColumnMapping(createDecimalType(toIntExact(precision), toIntExact(scale)), true));
                }
                if (precision != null) {
                    return Optional.of(new ColumnMapping(createDecimalType(toIntExact(precision)), true));
                }
                return Optional.of(new ColumnMapping(createDecimalType(DEFAULT_NUMERIC_TYPE_PRECISION, DEFAULT_NUMERIC_TYPE_SCALE), true));
            case STRING:
                return Optional.of(new ColumnMapping(createUnboundedVarcharType(), true));
            case BYTES:
                return Optional.of(new ColumnMapping(VarbinaryType.VARBINARY, true));
            case DATE:
                return Optional.of(new ColumnMapping(DateType.DATE, true));
            case DATETIME:
                return Optional.of(new ColumnMapping(TimestampType.TIMESTAMP_MICROS, true));
            case TIME:
                return Optional.of(new ColumnMapping(TimeType.TIME_MICROS, true));
            case TIMESTAMP:
                return Optional.of(new ColumnMapping(TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS, true));
            case GEOGRAPHY:
                return Optional.of(new ColumnMapping(VarcharType.VARCHAR, false));
            case JSON:
                return Optional.of(new ColumnMapping(jsonType, false));
            case STRUCT:
                // create the row
                FieldList subTypes = field.getSubFields();
                checkArgument(!subTypes.isEmpty(), "a record or struct must have sub-fields");
                List<RowType.Field> fields = subTypes.stream().map(subField -> toRawTypeField(subField.getName(), subField)).collect(toList());
                RowType rowType = RowType.from(fields);
                return Optional.of(new ColumnMapping(rowType, false));
            default:
                return Optional.empty();
        }
    }

    public BigQueryColumnHandle toColumnHandle(Field field)
    {
        FieldList subFields = field.getSubFields();
        List<BigQueryColumnHandle> subColumns = subFields == null ?
                Collections.emptyList() :
                subFields.stream()
                        .filter(this::isSupportedType)
                        .map(this::toColumnHandle)
                        .collect(Collectors.toList());
        ColumnMapping columnMapping = toTrinoType(field).orElseThrow(() -> new IllegalArgumentException("Unsupported type: " + field));
        return new BigQueryColumnHandle(
                field.getName(),
                ImmutableList.of(),
                columnMapping.type(),
                field.getType().getStandardType(),
                columnMapping.isPushdownSupported(),
                getMode(field),
                subColumns,
                field.getDescription(),
                false);
    }

    public boolean isSupportedType(Field field)
    {
        LegacySQLTypeName type = field.getType();
        if (type == LegacySQLTypeName.BIGNUMERIC) {
            // Skip BIGNUMERIC without parameters because the precision (77) and scale (38) is too large
            if (field.getPrecision() == null && field.getScale() == null) {
                return false;
            }
            if (field.getPrecision() != null && field.getPrecision() > Decimals.MAX_PRECISION) {
                return false;
            }
        }

        return toTrinoType(field).isPresent();
    }

    public boolean isJsonType(Type type)
    {
        return type.equals(jsonType);
    }

    private static Field.Mode getMode(Field field)
    {
        return firstNonNull(field.getMode(), Field.Mode.NULLABLE);
    }
}
