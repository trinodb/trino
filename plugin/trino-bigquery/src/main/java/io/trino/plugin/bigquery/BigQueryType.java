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
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
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
import java.util.List;
import java.util.Optional;

import static com.google.cloud.bigquery.Field.Mode.REPEATED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.bigquery.BigQueryMetadata.DEFAULT_NUMERIC_TYPE_PRECISION;
import static io.trino.plugin.bigquery.BigQueryMetadata.DEFAULT_NUMERIC_TYPE_SCALE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.DecimalType.createDecimalType;
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
import static java.util.stream.Collectors.toList;

public final class BigQueryType
{
    private BigQueryType() {}

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

    private static RowType.Field toRawTypeField(String name, Field field)
    {
        Type trinoType = convertToTrinoType(field).orElseThrow(() -> new IllegalArgumentException("Unsupported column " + field));
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

    private static String datetimeToStringConverter(Object value)
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

    public static Field toField(String name, Type type, @Nullable String comment)
    {
        if (type instanceof ArrayType) {
            Type elementType = ((ArrayType) type).getElementType();
            return toInnerField(name, elementType, true, comment);
        }
        return toInnerField(name, type, false, comment);
    }

    private static Field toInnerField(String name, Type type, boolean repeated, @Nullable String comment)
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

    private static FieldList toFieldList(RowType rowType)
    {
        ImmutableList.Builder<Field> fields = ImmutableList.builder();
        for (RowType.Field field : rowType.getFields()) {
            String fieldName = field.getName()
                    .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "ROW type does not have field names declared: " + rowType));
            fields.add(toField(fieldName, field.getType(), null));
        }
        return FieldList.of(fields.build());
    }

    private static StandardSQLTypeName toStandardSqlTypeName(Type type)
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

    public static Optional<String> convertToString(Type type, StandardSQLTypeName bigqueryType, Object value)
    {
        if (type instanceof ArrayType) {
            return Optional.empty();
        }
        switch (bigqueryType) {
            case BOOL:
                return Optional.of(simpleToStringConverter(value));
            case BYTES:
                return Optional.of(bytesToStringConverter(value));
            case DATE:
                return Optional.of(dateToStringConverter(value));
            case DATETIME:
                return Optional.of(datetimeToStringConverter(value)).map("'%s'"::formatted);
            case FLOAT64:
                return Optional.of(floatToStringConverter(value));
            case INT64:
                return Optional.of(simpleToStringConverter(value));
            case NUMERIC:
            case BIGNUMERIC:
                String bigqueryTypeName = bigqueryType.name();
                DecimalType decimalType = (DecimalType) type;
                if (decimalType.isShort()) {
                    return Optional.of(format("%s '%s'", bigqueryTypeName, Decimals.toString((long) value, ((DecimalType) type).getScale())));
                }
                return Optional.of(format("%s '%s'", bigqueryTypeName, Decimals.toString((Int128) value, ((DecimalType) type).getScale())));
            case ARRAY:
            case STRUCT:
            case GEOGRAPHY:
                // Or throw an exception?
                return Optional.empty();
            case STRING:
                return Optional.of(stringToStringConverter(value));
            case TIME:
                return Optional.of(timeToStringConverter(value));
            case TIMESTAMP:
                return Optional.of(timestampToStringConverter(value)).map("'%s'"::formatted);
            default:
                throw new IllegalArgumentException("Unsupported type: " + bigqueryType);
        }
    }

    public static Optional<Type> toTrinoType(Field field)
    {
        return convertToTrinoType(field)
                .map(type -> field.getMode() == REPEATED ? new ArrayType(type) : type);
    }

    private static Optional<Type> convertToTrinoType(Field field)
    {
        switch (field.getType().getStandardType()) {
            case BOOL:
                return Optional.of(BooleanType.BOOLEAN);
            case INT64:
                return Optional.of(BigintType.BIGINT);
            case FLOAT64:
                return Optional.of(DoubleType.DOUBLE);
            case NUMERIC:
            case BIGNUMERIC:
                Long precision = field.getPrecision();
                Long scale = field.getScale();
                // Unsupported BIGNUMERIC types (precision > 38) are filtered in BigQueryClient.getColumns
                if (precision != null && scale != null) {
                    return Optional.of(createDecimalType(toIntExact(precision), toIntExact(scale)));
                }
                if (precision != null) {
                    return Optional.of(createDecimalType(toIntExact(precision)));
                }
                return Optional.of(createDecimalType(DEFAULT_NUMERIC_TYPE_PRECISION, DEFAULT_NUMERIC_TYPE_SCALE));
            case STRING:
                return Optional.of(createUnboundedVarcharType());
            case BYTES:
                return Optional.of(VarbinaryType.VARBINARY);
            case DATE:
                return Optional.of(DateType.DATE);
            case DATETIME:
                return Optional.of(TimestampType.TIMESTAMP_MICROS);
            case TIME:
                return Optional.of(TimeType.TIME_MICROS);
            case TIMESTAMP:
                return Optional.of(TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS);
            case GEOGRAPHY:
                return Optional.of(VarcharType.VARCHAR);
            case STRUCT:
                // create the row
                FieldList subTypes = field.getSubFields();
                checkArgument(!subTypes.isEmpty(), "a record or struct must have sub-fields");
                List<RowType.Field> fields = subTypes.stream().map(subField -> toRawTypeField(subField.getName(), subField)).collect(toList());
                RowType rowType = RowType.from(fields);
                return Optional.of(rowType);
            default:
                return Optional.empty();
        }
    }
}
