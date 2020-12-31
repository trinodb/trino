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
package io.prestosql.plugin.bigquery;

import com.google.cloud.bigquery.Field;
import io.airlift.slice.Slice;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DateTimeEncoding;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.TimeWithTimeZoneType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.plugin.bigquery.BigQueryMetadata.NUMERIC_DATA_TYPE_PRECISION;
import static io.prestosql.plugin.bigquery.BigQueryMetadata.NUMERIC_DATA_TYPE_SCALE;
import static io.prestosql.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.time.ZoneOffset.systemDefault;
import static java.util.stream.Collectors.toList;

public enum BigQueryType
{
    BOOLEAN(BooleanType.BOOLEAN, BigQueryType::simpleToStringConverter),
    BYTES(VarbinaryType.VARBINARY, BigQueryType::bytesToStringConverter),
    DATE(DateType.DATE, BigQueryType::dateToStringConverter),
    DATETIME(TimestampType.TIMESTAMP_MILLIS, BigQueryType::datetimeToStringConverter),
    FLOAT(DoubleType.DOUBLE, BigQueryType::simpleToStringConverter),
    GEOGRAPHY(VarcharType.VARCHAR, BigQueryType::stringToStringConverter),
    INTEGER(BigintType.BIGINT, BigQueryType::simpleToStringConverter),
    NUMERIC(DecimalType.createDecimalType(NUMERIC_DATA_TYPE_PRECISION, NUMERIC_DATA_TYPE_SCALE), BigQueryType::numericToStringConverter),
    RECORD(null, BigQueryType::simpleToStringConverter),
    STRING(createUnboundedVarcharType(), BigQueryType::stringToStringConverter),
    TIME(TimeWithTimeZoneType.TIME_WITH_TIME_ZONE, BigQueryType::timeToStringConverter),
    TIMESTAMP(TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS, BigQueryType::timestampToStringConverter);

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
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("''yyyy-MM-dd HH:mm:ss.SSS''");

    private final Type nativeType;
    private final ToStringConverter toStringConverter;

    BigQueryType(Type nativeType, ToStringConverter toStringConverter)
    {
        this.nativeType = nativeType;
        this.toStringConverter = toStringConverter;
    }

    static RowType.Field toRawTypeField(Map.Entry<String, BigQueryType.Adaptor> entry)
    {
        return toRawTypeField(entry.getKey(), entry.getValue());
    }

    static RowType.Field toRawTypeField(String name, BigQueryType.Adaptor typeAdaptor)
    {
        Type prestoType = typeAdaptor.getPrestoType();
        return RowType.field(name, prestoType);
    }

    static LocalDateTime toLocalDateTime(String datetime)
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

    static long toPrestoTimestamp(String datetime)
    {
        return toLocalDateTime(datetime).toInstant(UTC).toEpochMilli() * MICROSECONDS_PER_MILLISECOND;
    }

    static String simpleToStringConverter(Object value)
    {
        return String.valueOf(value);
    }

    static String dateToStringConverter(Object value)
    {
        LocalDate date = LocalDate.ofEpochDay(((Long) value).longValue());
        return quote(date.toString());
    }

    static String datetimeToStringConverter(Object value)
    {
        return formatTimestamp(((Long) value).longValue(), systemDefault());
    }

    static String timeToStringConverter(Object value)
    {
        long longValue = ((Long) value).longValue();
        long millisUtc = DateTimeEncoding.unpackMillisUtc(longValue);
        ZoneId zoneId = ZoneId.of(DateTimeEncoding.unpackZoneKey(longValue).getId());
        LocalTime time = toZonedDateTime(millisUtc, zoneId).toLocalTime();
        return quote(time.toString());
    }

    static String timestampToStringConverter(Object value)
    {
        long longValue = ((Long) value).longValue();
        long millisUtc = DateTimeEncoding.unpackMillisUtc(longValue);
        ZoneId zoneId = ZoneId.of(DateTimeEncoding.unpackZoneKey(longValue).getId());
        return formatTimestamp(millisUtc, zoneId);
    }

    private static String formatTimestamp(long millisUtc, ZoneId zoneId)
    {
        return DATETIME_FORMATTER.format(toZonedDateTime(millisUtc, zoneId));
    }

    private static ZonedDateTime toZonedDateTime(long millisUtc, ZoneId zoneId)
    {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(millisUtc), zoneId);
    }

    static String stringToStringConverter(Object value)
    {
        Slice slice = (Slice) value;
        return quote(slice.toStringUtf8());
    }

    static String numericToStringConverter(Object value)
    {
        Slice slice = (Slice) value;
        return Decimals.toString(slice, NUMERIC_DATA_TYPE_SCALE);
    }

    static String bytesToStringConverter(Object value)
    {
        Slice slice = (Slice) value;
        return format("FROM_BASE64('%s')", Base64.getEncoder().encodeToString(slice.getBytes()));
    }

    private static String quote(String value)
    {
        return "'" + value + "'";
    }

    String convertToString(Object value)
    {
        return toStringConverter.convertToString(value);
    }

    public Type getNativeType(BigQueryType.Adaptor typeAdaptor)
    {
        switch (this) {
            case RECORD:
                // create the row
                Map<String, BigQueryType.Adaptor> subTypes = typeAdaptor.getBigQuerySubTypes();
                checkArgument(!subTypes.isEmpty(), "a record or struct must have sub-fields");
                List<RowType.Field> fields = subTypes.entrySet().stream().map(BigQueryType::toRawTypeField).collect(toList());
                return RowType.from(fields);
            default:
                return nativeType;
        }
    }

    interface Adaptor
    {
        BigQueryType getBigQueryType();

        Map<String, BigQueryType.Adaptor> getBigQuerySubTypes();

        Field.Mode getMode();

        default Type getPrestoType()
        {
            Type rawType = getBigQueryType().getNativeType(this);
            return getMode() == Field.Mode.REPEATED ? new ArrayType(rawType) : rawType;
        }
    }

    @FunctionalInterface
    interface ToStringConverter
    {
        String convertToString(Object value);
    }
}
