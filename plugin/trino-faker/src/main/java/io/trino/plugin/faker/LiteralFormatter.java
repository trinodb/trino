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
package io.trino.plugin.faker;

import com.google.common.net.InetAddresses;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimeWithTimeZone;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import io.trino.type.IntervalDayTimeType;
import io.trino.type.IntervalYearMonthType;
import io.trino.type.IpAddressType;
import io.trino.type.SqlIntervalDayTime;
import io.trino.type.SqlIntervalYearMonth;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDate;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;

public final class LiteralFormatter
{
    private LiteralFormatter() {}

    public static String formatLiteral(Type type, Object value)
    {
        Class<?> javaType = type.getJavaType();
        if (javaType == boolean.class) {
            boolean typedValue = (boolean) value;
            return Boolean.toString(typedValue);
        }

        if (javaType == double.class) {
            double typedValue = (double) value;
            return formatStringLiteral("DOUBLE", Double.toString(typedValue));
        }

        if (javaType == long.class) {
            long typedValue = (long) value;

            if (type == TINYINT) {
                return formatStringLiteral("TINYINT", Byte.toString(SignedBytes.checkedCast(typedValue)));
            }

            if (type == REAL) {
                return formatStringLiteral("REAL", Float.toString(intBitsToFloat(toIntExact(typedValue))));
            }

            if (type == DATE) {
                return formatStringLiteral("DATE", LocalDate.ofEpochDay(typedValue).toString());
            }

            if (type == BIGINT) {
                return formatStringLiteral("BIGINT", Long.toString(typedValue));
            }

            if (type == INTEGER) {
                return formatStringLiteral("INTEGER", Integer.toString(toIntExact(typedValue)));
            }

            if (type instanceof DecimalType decimalType) {
                BigInteger unscaledValue = BigInteger.valueOf(typedValue);
                BigDecimal bigDecimal = new BigDecimal(unscaledValue, decimalType.getScale(), new MathContext(decimalType.getPrecision()));
                return formatStringLiteral("DECIMAL", bigDecimal.toString());
            }

            if (type == SMALLINT) {
                return formatStringLiteral(
                        "SMALLINT",
                        Short.toString(Shorts.checkedCast(typedValue)));
            }

            if (type instanceof TimeType timeType) {
                return formatStringLiteral(
                        "TIME",
                        SqlTime.newInstance(timeType.getPrecision(), typedValue).toString());
            }

            if (type instanceof TimestampType timestampType) {
                if (timestampType.isShort()) {
                    return formatStringLiteral(
                            "TIMESTAMP",
                            SqlTimestamp.newInstance(timestampType.getPrecision(), typedValue, 0).toString());
                }
            }

            if (type instanceof TimeWithTimeZoneType timeWithTimeZoneType) {
                verify(timeWithTimeZoneType.isShort(), "Short TimeWithTimeZoneType was expected");
                return formatStringLiteral("TIME", SqlTimeWithTimeZone.newInstance(
                        timeWithTimeZoneType.getPrecision(),
                        unpackTimeNanos(typedValue) * PICOSECONDS_PER_NANOSECOND,
                        unpackOffsetMinutes(typedValue)).toString());
            }

            if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
                verify(timestampWithTimeZoneType.isShort(), "Short TimestampWithTimezone was expected");
                return formatStringLiteral(
                        "TIMESTAMP",
                        SqlTimestampWithTimeZone.newInstance(
                                timestampWithTimeZoneType.getPrecision(),
                                unpackMillisUtc(typedValue),
                                0,
                                unpackZoneKey(typedValue)).toString());
            }

            if (type instanceof IntervalDayTimeType) {
                return "INTERVAL " + escapeStringLiteral(new SqlIntervalDayTime(typedValue).toString()) + " DAY TO SECOND";
            }

            if (type instanceof IntervalYearMonthType) {
                return "INTERVAL " + escapeStringLiteral(new SqlIntervalYearMonth((int) typedValue).toString()) + " YEAR TO MONTH";
            }
        }

        if (javaType == Slice.class) {
            Slice typedValue = (Slice) value;
            switch (type) {
                case VarcharType _, CharType _ -> {
                    return escapeStringLiteral(typedValue.toStringUtf8());
                }
                case VarbinaryType _ -> {
                    return "x" + escapeStringLiteral(new SqlVarbinary(typedValue.getBytes()).toString());
                }
                case IpAddressType _ -> {
                    try {
                        return formatStringLiteral("IPADDRESS", InetAddresses.toAddrString(InetAddress.getByAddress(typedValue.getBytes())));
                    }
                    catch (UnknownHostException e) {
                        throw new RuntimeException(e);
                    }
                }
                case UuidType _ -> {
                    return formatStringLiteral("UUID", trinoUuidToJavaUuid(typedValue).toString());
                }
                default -> throw new UnsupportedOperationException("Unsupported type " + type + " backed by java " + javaType);
            }
        }

        if (javaType == Int128.class) {
            if (type instanceof DecimalType decimalType) {
                Int128 typedValue = (Int128) value;

                BigInteger unscaledValue = typedValue.toBigInteger();
                BigDecimal bigDecimal = new BigDecimal(unscaledValue, decimalType.getScale(), new MathContext(decimalType.getPrecision()));
                return formatStringLiteral("DECIMAL", bigDecimal.toPlainString());
            }
        }

        switch (type) {
            case TimeWithTimeZoneType timeWithTimeZoneType when javaType == LongTimeWithTimeZone.class -> {
                LongTimeWithTimeZone typedValue = (LongTimeWithTimeZone) value;
                return formatStringLiteral("TIME",
                        SqlTimeWithTimeZone.newInstance(
                                timeWithTimeZoneType.getPrecision(),
                                typedValue.getPicoseconds(),
                                typedValue.getOffsetMinutes()).toString());
            }
            case TimestampType timestampType when javaType == LongTimestamp.class -> {
                LongTimestamp typedValue = (LongTimestamp) value;

                return formatStringLiteral(
                        "TIMESTAMP",
                        SqlTimestamp.newInstance(
                                timestampType.getPrecision(),
                                typedValue.getEpochMicros(),
                                typedValue.getPicosOfMicro()).toString());
            }
            case TimestampWithTimeZoneType timestampWithTimeZoneType when javaType == LongTimestampWithTimeZone.class -> {
                LongTimestampWithTimeZone typedValue = (LongTimestampWithTimeZone) value;
                return formatStringLiteral(
                        "TIMESTAMP",
                        SqlTimestampWithTimeZone.newInstance(
                                timestampWithTimeZoneType.getPrecision(),
                                typedValue.getEpochMillis(),
                                typedValue.getPicosOfMilli(),
                                getTimeZoneKey(typedValue.getTimeZoneKey())).toString());
            }
            default -> throw new UnsupportedOperationException("Unsupported type " + type + " backed by java " + javaType);
        }
    }

    private static String formatStringLiteral(String typeName, String value)
    {
        return typeName + " " + escapeStringLiteral(value);
    }

    private static String escapeStringLiteral(String value)
    {
        return "'" + value.replace("'", "''") + "'";
    }
}
