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
package io.prestosql.plugin.kafka.encoder.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.kafka.encoder.EncoderColumnHandle;
import io.prestosql.plugin.kafka.encoder.RowEncoder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.SqlDate;
import io.prestosql.spi.type.SqlTime;
import io.prestosql.spi.type.SqlTimeWithTimeZone;
import io.prestosql.spi.type.SqlTimestamp;
import io.prestosql.spi.type.SqlTimestampWithTimeZone;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.Type;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_DATE;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class JsonRowEncoder
        implements RowEncoder
{
    private static final Set<Type> PRIMITIVE_SUPPORTED_TYPES = ImmutableSet.of(
            BIGINT, INTEGER, SMALLINT, TINYINT, DOUBLE, BOOLEAN);
    private static final Set<Type> NON_PARAMETRIC_DATE_TIME_TYPES = ImmutableSet.of(
            DATE, TIME, TIME_WITH_TIME_ZONE);
    private static final Set<String> DATE_TIME_FORMATS = ImmutableSet.of(
            "custom-date-time", "iso8601", "rfc2822", "milliseconds-since-epoch", "seconds-since-epoch");
    private static final String CUSTOM_DATE_TIME_NAME = "custom-date-time";

    public static final String NAME = "json";

    private final ObjectMapper objectMapper;
    private ObjectNode node;

    JsonRowEncoder(ObjectMapper objectMapper, Set<EncoderColumnHandle> columnHandles)
    {
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        this.node = objectMapper.createObjectNode();
        this.parseColumns(columnHandles);
    }

    // performs checks on column handles
    private void parseColumns(Set<EncoderColumnHandle> columnHandles)
    {
        for (EncoderColumnHandle columnHandle : columnHandles) {
            try {
                requireNonNull(columnHandle, "columnHandle is null");
                String columnName = columnHandle.getName();
                Type columnType = columnHandle.getType();

                checkArgument(!columnHandle.isInternal(), "unexpected internal column '%s'", columnName);
                if (!isDateTimeType(columnType)) {
                    checkArgument(columnHandle.getFormatHint() == null, "unexpected format hint '%s' defined for column '%s'", columnHandle.getFormatHint(), columnName);
                    checkArgument(columnHandle.getDataFormat() == null, "unexpected data format '%s' defined for column '%s'", columnHandle.getDataFormat(), columnName);
                }
                else {
                    checkArgument(DATE_TIME_FORMATS.contains(columnHandle.getDataFormat()), "incorrect data format '%s' defined for column '%s'", columnHandle.getDataFormat(), columnName);
                    if (columnHandle.getDataFormat().equals(CUSTOM_DATE_TIME_NAME)) {
                        checkArgument(columnHandle.getFormatHint() != null, "no format hint defined for column '%s'", columnName);
                    }
                    else {
                        checkArgument(columnHandle.getFormatHint() == null, "unexpected format hint '%s' defined for column '%s'", columnHandle.getFormatHint(), columnName);
                    }
                }

                checkArgument(isSupportedType(columnType), "unsupported column type '%s' for column '%s'", columnType, columnName);
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(GENERIC_USER_ERROR, e);
            }
        }
    }

    private boolean isSupportedType(Type type)
    {
        return isVarcharType(type) ||
                type instanceof TimestampType ||
                type instanceof TimestampWithTimeZoneType ||
                PRIMITIVE_SUPPORTED_TYPES.contains(type) ||
                NON_PARAMETRIC_DATE_TIME_TYPES.contains(type);
    }

    private boolean isDateTimeType(Type type)
    {
        return type instanceof TimestampType ||
                type instanceof TimestampWithTimeZoneType ||
                NON_PARAMETRIC_DATE_TIME_TYPES.contains(type);
    }

    @Override
    public byte[] toByteArray()
    {
        try {
            return objectMapper.writeValueAsBytes(node);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void clear()
    {
        this.node = objectMapper.createObjectNode();
    }

    @Override
    public RowEncoder putNullValue(EncoderColumnHandle columnHandle)
    {
        node.putNull(columnHandle.getName());
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, long value)
    {
        node.put(columnHandle.getName(), value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, int value)
    {
        node.put(columnHandle.getName(), value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, short value)
    {
        node.put(columnHandle.getName(), value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, byte value)
    {
        node.put(columnHandle.getName(), value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, double value)
    {
        node.put(columnHandle.getName(), value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, float value)
    {
        node.put(columnHandle.getName(), value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, boolean value)
    {
        node.put(columnHandle.getName(), value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, String value)
    {
        node.put(columnHandle.getName(), value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, ByteBuffer value)
    {
        node.put(columnHandle.getName(), value.array());
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, byte[] value)
    {
        node.put(columnHandle.getName(), value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, SqlDate value)
    {
        node.put(columnHandle.getName(), this.formatDateTime(columnHandle, value));
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, SqlTime value)
    {
        node.put(columnHandle.getName(), this.formatDateTime(columnHandle, value));
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, SqlTimeWithTimeZone value)
    {
        node.put(columnHandle.getName(), this.formatDateTime(columnHandle, value));
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, SqlTimestamp value)
    {
        node.put(columnHandle.getName(), this.formatDateTime(columnHandle, value));
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, SqlTimestampWithTimeZone value)
    {
        node.put(columnHandle.getName(), this.formatDateTime(columnHandle, value));
        return this;
    }

    private org.joda.time.format.DateTimeFormatter getCustomFormatter(String format)
    {
        try {
            return org.joda.time.format.DateTimeFormat.forPattern(format).withLocale(Locale.ENGLISH);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(
                    GENERIC_USER_ERROR,
                    format("invalid joda pattern '%s' passed as format hint", format));
        }
    }

    private DateTimeFormatter getRfcFormatter()
    {
        return DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss X yyyy").withLocale(Locale.ENGLISH).withZone(UTC);
    }

    private String formatDateTime(EncoderColumnHandle columnHandle, SqlDate value)
    {
        switch (columnHandle.getDataFormat()) {
            case "custom-date-time":
                return this.getCustomFormatter(columnHandle.getFormatHint())
                        .print(org.joda.time.Instant.ofEpochMilli(value.getDays()).toDateTime());
            case "rfc2822":
                return Instant.ofEpochMilli(DAYS.toMillis(value.getDays()))
                        .atZone(UTC)
                        .format(this.getRfcFormatter());
            case "iso8601":
                return Instant.ofEpochMilli(DAYS.toMillis(value.getDays()))
                        .atZone(UTC)
                        .toLocalDate()
                        .format(ISO_DATE);
            case "milliseconds-since-epoch":
                return String.valueOf(DAYS.toMillis(value.getDays()));
            case "seconds-since-epoch":
                return String.valueOf(MILLISECONDS.toSeconds(DAYS.toMillis(value.getDays())));
            default:
                throw new PrestoException(GENERIC_USER_ERROR, format("invalid data format '%s' defined for column '%s'", columnHandle.getDataFormat(), columnHandle.getName()));
        }
    }

    private String formatDateTime(EncoderColumnHandle columnHandle, SqlTime value)
    {
        switch (columnHandle.getDataFormat()) {
            case "custom-date-time":
                return this.getCustomFormatter(columnHandle.getFormatHint())
                        .print(org.joda.time.Instant.ofEpochMilli(value.getMillis()).toDateTime());
            case "rfc2822":
                return Instant.ofEpochMilli(value.getMillis())
                        .atZone(UTC)
                        .format(this.getRfcFormatter());
            case "iso8601":
                return Instant.ofEpochMilli(value.getMillis())
                        .atZone(UTC)
                        .toLocalDateTime()
                        .format(ISO_DATE_TIME);
            case "milliseconds-since-epoch":
                return String.valueOf(value.getMillis());
            case "seconds-since-epoch":
                return String.valueOf(MILLISECONDS.toSeconds(value.getMillis()));
            default:
                throw new PrestoException(GENERIC_USER_ERROR, format("invalid data format '%s' defined for column '%s'", columnHandle.getDataFormat(), columnHandle.getName()));
        }
    }

    private String formatDateTime(EncoderColumnHandle columnHandle, SqlTimeWithTimeZone value)
    {
        switch (columnHandle.getDataFormat()) {
            case "custom-date-time":
                return this.getCustomFormatter(columnHandle.getFormatHint())
                        .print(org.joda.time.Instant.ofEpochMilli(value.getMillisUtc()).toDateTime());
            case "rfc2822":
                return Instant.ofEpochMilli(value.getMillisUtc())
                        .atZone(UTC)
                        .format(this.getRfcFormatter());
            case "iso8601":
                return Instant.ofEpochMilli(value.getMillisUtc())
                        .atZone(UTC)
                        .toLocalDateTime()
                        .format(ISO_DATE_TIME);
            case "milliseconds-since-epoch":
                return String.valueOf(value.getMillisUtc());
            case "seconds-since-epoch":
                return String.valueOf(MILLISECONDS.toSeconds(value.getMillisUtc()));
            default:
                throw new PrestoException(GENERIC_USER_ERROR, format("invalid data format '%s' defined for column '%s'", columnHandle.getDataFormat(), columnHandle.getName()));
        }
    }

    private String formatDateTime(EncoderColumnHandle columnHandle, SqlTimestamp value)
    {
        switch (columnHandle.getDataFormat()) {
            case "custom-date-time":
                return this.getCustomFormatter(columnHandle.getFormatHint())
                        .print(org.joda.time.Instant.ofEpochMilli(value.getMillis()).toDateTime());
            case "rfc2822":
                return Instant.ofEpochMilli(value.getMillis())
                        .atZone(UTC)
                        .format(this.getRfcFormatter());
            case "iso8601":
                return Instant.ofEpochMilli(value.getMillis())
                        .atZone(UTC)
                        .toLocalDateTime()
                        .format(ISO_DATE_TIME);
            case "milliseconds-since-epoch":
                return String.valueOf(value.getMillis());
            case "seconds-since-epoch":
                return String.valueOf(MILLISECONDS.toSeconds(value.getMillis()));
            default:
                throw new PrestoException(GENERIC_USER_ERROR, format("invalid data format '%s' defined for column '%s'", columnHandle.getDataFormat(), columnHandle.getName()));
        }
    }

    private String formatDateTime(EncoderColumnHandle columnHandle, SqlTimestampWithTimeZone value)
    {
        switch (columnHandle.getDataFormat()) {
            case "custom-date-time":
                return this.getCustomFormatter(columnHandle.getFormatHint())
                        .print(org.joda.time.Instant.ofEpochMilli(value.getMillisUtc()).toDateTime());
            case "rfc2822":
                return Instant.ofEpochMilli(value.getMillisUtc())
                        .atZone(UTC)
                        .format(this.getRfcFormatter());
            case "iso8601":
                return Instant.ofEpochMilli(value.getMillisUtc())
                        .atZone(UTC)
                        .toLocalDateTime()
                        .format(ISO_DATE_TIME);
            case "milliseconds-since-epoch":
                return String.valueOf(value.getMillisUtc());
            case "seconds-since-epoch":
                return String.valueOf(MILLISECONDS.toSeconds(value.getMillisUtc()));
            default:
                throw new PrestoException(GENERIC_USER_ERROR, format("invalid data format '%s' defined for column '%s'", columnHandle.getDataFormat(), columnHandle.getName()));
        }
    }
}
