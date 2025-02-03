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

import com.google.common.base.CharMatcher;
import com.google.common.io.BaseEncoding;
import com.google.common.net.InetAddresses;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
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
import io.trino.sql.tree.IntervalLiteral;
import io.trino.type.IntervalDayTimeType;
import io.trino.type.IntervalYearMonthType;
import io.trino.type.IpAddressType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDate;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.type.DateTimes.parseTime;
import static io.trino.type.DateTimes.parseTimeWithTimeZone;
import static io.trino.type.DateTimes.parseTimestamp;
import static io.trino.type.DateTimes.parseTimestampWithTimeZone;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.trino.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static io.trino.util.DateTimeUtils.parseDate;
import static io.trino.util.DateTimeUtils.parseDayTimeInterval;
import static io.trino.util.DateTimeUtils.parseYearMonthInterval;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.lang.System.arraycopy;
import static java.util.Locale.ENGLISH;

public class Literal
{
    private static final CharMatcher WHITESPACE_MATCHER = CharMatcher.whitespace();
    private static final CharMatcher HEX_DIGIT_MATCHER = CharMatcher.inRange('A', 'F')
            .or(CharMatcher.inRange('0', '9'))
            .precomputed();
    private static final Pattern DECIMAL_PATTERN = Pattern.compile("([+-]?)(\\d(?:_?\\d)*)?(?:\\.(\\d(?:_?\\d)*)?)?");

    private Literal() {}

    public static Object parse(String value, Type type)
    {
        if (value == null) {
            return null;
        }
        if (BIGINT.equals(type) || INTEGER.equals(type) || SMALLINT.equals(type) || TINYINT.equals(type)) {
            return parseLong(value);
        }
        if (BOOLEAN.equals(type)) {
            return parseBoolean(value);
        }
        if (DATE.equals(type)) {
            return (long) parseDate(value);
        }
        if (type instanceof DecimalType decimalType) {
            return parseDecimal(value, decimalType);
        }
        if (REAL.equals(type)) {
            return (long) floatToRawIntBits(Float.parseFloat(value));
        }
        if (DOUBLE.equals(type)) {
            return Double.parseDouble(value);
        }
        // not supported: HYPER_LOG_LOG, QDIGEST, TDIGEST, P4_HYPER_LOG_LOG
        if (INTERVAL_DAY_TIME.equals(type)) {
            return parseDayTimeInterval(value, IntervalLiteral.IntervalField.SECOND, Optional.empty());
        }
        if (INTERVAL_YEAR_MONTH.equals(type)) {
            return parseYearMonthInterval(value, IntervalLiteral.IntervalField.MONTH, Optional.empty());
        }
        if (type instanceof TimestampType timestampType) {
            return parseTimestamp(timestampType.getPrecision(), value);
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            return parseTimestampWithTimeZone(timestampWithTimeZoneType.getPrecision(), value);
        }
        if (type instanceof TimeType) {
            return parseTime(value);
        }
        if (type instanceof TimeWithTimeZoneType timeWithTimeZoneType) {
            return parseTimeWithTimeZone(timeWithTimeZoneType.getPrecision(), value);
        }
        if (VARBINARY.equals(type)) {
            return parseBinary(value);
        }
        if (type instanceof VarcharType || type instanceof CharType) {
            return utf8Slice(value);
        }
        // not supported: ROW, ARRAY, MAP, JSON
        if (type instanceof IpAddressType) {
            return parseIpAddress(value);
        }
        // not supported: GEOMETRY
        if (type instanceof UuidType) {
            return javaUuidToTrinoUuid(UUID.fromString(value));
        }
        throw new IllegalArgumentException("Unsupported literal type: " + type);
    }

    private static long parseLong(String value)
    {
        value = value.replace("_", "");

        if (value.startsWith("0x") || value.startsWith("0X")) {
            return Long.parseLong(value.substring(2), 16);
        }
        else if (value.startsWith("0b") || value.startsWith("0B")) {
            return Long.parseLong(value.substring(2), 2);
        }
        else if (value.startsWith("0o") || value.startsWith("0O")) {
            return Long.parseLong(value.substring(2), 8);
        }
        else {
            return Long.parseLong(value);
        }
    }

    private static Boolean parseBoolean(String value)
    {
        value = value.toLowerCase(ENGLISH);
        checkArgument(value.equals("true") || value.equals("false"));
        return value.equals("true");
    }

    /**
     * Based on {@link io.trino.spi.type.Decimals#parse}, but doesn't infer the type from the value.
     */
    private static Object parseDecimal(String value, DecimalType decimalType)
    {
        Matcher matcher = DECIMAL_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid DECIMAL value '" + value + "'");
        }

        String sign = getMatcherGroup(matcher, 1);
        if (sign.isEmpty()) {
            sign = "+";
        }
        String integralPart = getMatcherGroup(matcher, 2);
        String fractionalPart = getMatcherGroup(matcher, 3);

        if (integralPart.isEmpty() && fractionalPart.isEmpty()) {
            throw new IllegalArgumentException("Invalid DECIMAL value '" + value + "'");
        }

        integralPart = stripLeadingZeros(integralPart.replace("_", ""));
        fractionalPart = fractionalPart.replace("_", "");

        String unscaledValue = sign + (integralPart.isEmpty() ? "0" : "") + integralPart + fractionalPart;
        if (decimalType.isShort()) {
            return Long.parseLong(unscaledValue);
        }
        return Int128.valueOf(new BigInteger(unscaledValue));
    }

    private static String getMatcherGroup(MatchResult matcher, int group)
    {
        String groupValue = matcher.group(group);
        if (groupValue == null) {
            groupValue = "";
        }
        return groupValue;
    }

    private static String stripLeadingZeros(String number)
    {
        for (int i = 0; i < number.length(); i++) {
            if (number.charAt(i) != '0') {
                return number.substring(i);
            }
        }

        return "";
    }

    private static Slice parseBinary(String value)
    {
        String hexString = WHITESPACE_MATCHER.removeFrom(value).toUpperCase(ENGLISH);
        if (!HEX_DIGIT_MATCHER.matchesAllOf(hexString)) {
            throw new IllegalArgumentException("Binary literal can only contain hexadecimal digits");
        }
        if (hexString.length() % 2 != 0) {
            throw new IllegalArgumentException("Binary literal must contain an even number of digits");
        }
        return Slices.wrappedBuffer(BaseEncoding.base16().decode(hexString));
    }

    private static Slice parseIpAddress(String value)
    {
        byte[] address;
        try {
            address = InetAddresses.forString(value).getAddress();
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Cannot cast value to IPADDRESS: " + value);
        }

        byte[] bytes;
        if (address.length == 4) {
            bytes = new byte[16];
            bytes[10] = (byte) 0xff;
            bytes[11] = (byte) 0xff;
            arraycopy(address, 0, bytes, 12, 4);
        }
        else if (address.length == 16) {
            bytes = address;
        }
        else {
            throw new IllegalArgumentException("Invalid InetAddress length: " + address.length);
        }

        return wrappedBuffer(bytes);
    }

    public static String format(Type type, Object value)
    {
        Class<?> javaType = type.getJavaType();
        if (javaType == boolean.class) {
            boolean typedValue = (boolean) value;
            return Boolean.toString(typedValue);
        }

        if (javaType == double.class) {
            double typedValue = (double) value;
            return Double.toString(typedValue);
        }

        if (javaType == long.class) {
            long typedValue = (long) value;

            if (type == TINYINT) {
                return Byte.toString(SignedBytes.checkedCast(typedValue));
            }

            if (type == REAL) {
                return Float.toString(intBitsToFloat(toIntExact(typedValue)));
            }

            if (type == DATE) {
                return LocalDate.ofEpochDay(typedValue).toString();
            }

            if (type == BIGINT) {
                return Long.toString(typedValue);
            }

            if (type == INTEGER) {
                return Integer.toString(toIntExact(typedValue));
            }

            if (type instanceof DecimalType decimalType) {
                BigInteger unscaledValue = BigInteger.valueOf(typedValue);
                BigDecimal bigDecimal = new BigDecimal(unscaledValue, decimalType.getScale(), new MathContext(decimalType.getPrecision()));
                return bigDecimal.toString();
            }

            if (type == SMALLINT) {
                return Short.toString(Shorts.checkedCast(typedValue));
            }

            if (type instanceof TimeType timeType) {
                return SqlTime.newInstance(timeType.getPrecision(), typedValue).toString();
            }

            if (type instanceof TimestampType timestampType) {
                if (timestampType.isShort()) {
                    return SqlTimestamp.newInstance(timestampType.getPrecision(), typedValue, 0).toString();
                }
            }

            if (type instanceof TimeWithTimeZoneType timeWithTimeZoneType) {
                verify(timeWithTimeZoneType.isShort(), "Short TimeWithTimeZoneType was expected");
                return SqlTimeWithTimeZone.newInstance(
                        timeWithTimeZoneType.getPrecision(),
                        unpackTimeNanos(typedValue) * PICOSECONDS_PER_NANOSECOND,
                        unpackOffsetMinutes(typedValue)).toString();
            }

            if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
                verify(timestampWithTimeZoneType.isShort(), "Short TimestampWithTimezone was expected");
                return SqlTimestampWithTimeZone.newInstance(
                        timestampWithTimeZoneType.getPrecision(),
                        unpackMillisUtc(typedValue),
                        0,
                        unpackZoneKey(typedValue)).toString();
            }

            if (type instanceof IntervalDayTimeType) {
                long epochSeconds = floorDiv(typedValue, (long) MILLISECONDS_PER_SECOND);
                long fractionalSecond = floorMod(typedValue, (long) MILLISECONDS_PER_SECOND);
                return "%d.%03d".formatted(epochSeconds, fractionalSecond);
            }

            if (type instanceof IntervalYearMonthType) {
                return Long.toString(typedValue);
            }
        }

        if (javaType == Slice.class) {
            Slice typedValue = (Slice) value;
            switch (type) {
                case VarcharType _, CharType _ -> {
                    return typedValue.toStringUtf8();
                }
                case VarbinaryType _ -> {
                    return new SqlVarbinary(typedValue.getBytes()).toString();
                }
                case IpAddressType _ -> {
                    try {
                        return InetAddresses.toAddrString(InetAddress.getByAddress(typedValue.getBytes()));
                    }
                    catch (UnknownHostException e) {
                        throw new RuntimeException(e);
                    }
                }
                case UuidType _ -> {
                    return trinoUuidToJavaUuid(typedValue).toString();
                }
                default -> throw new UnsupportedOperationException("Unsupported type " + type + " backed by java " + javaType);
            }
        }

        if (javaType == Int128.class) {
            if (type instanceof DecimalType decimalType) {
                Int128 typedValue = (Int128) value;

                BigInteger unscaledValue = typedValue.toBigInteger();
                BigDecimal bigDecimal = new BigDecimal(unscaledValue, decimalType.getScale(), new MathContext(decimalType.getPrecision()));
                return bigDecimal.toPlainString();
            }
        }

        switch (type) {
            case TimeWithTimeZoneType timeWithTimeZoneType when javaType == LongTimeWithTimeZone.class -> {
                LongTimeWithTimeZone typedValue = (LongTimeWithTimeZone) value;
                return SqlTimeWithTimeZone.newInstance(
                        timeWithTimeZoneType.getPrecision(),
                        typedValue.getPicoseconds(),
                        typedValue.getOffsetMinutes()).toString();
            }
            case TimestampType timestampType when javaType == LongTimestamp.class -> {
                LongTimestamp typedValue = (LongTimestamp) value;

                return SqlTimestamp.newInstance(
                        timestampType.getPrecision(),
                        typedValue.getEpochMicros(),
                        typedValue.getPicosOfMicro()).toString();
            }
            case TimestampWithTimeZoneType timestampWithTimeZoneType when javaType == LongTimestampWithTimeZone.class -> {
                LongTimestampWithTimeZone typedValue = (LongTimestampWithTimeZone) value;
                return SqlTimestampWithTimeZone.newInstance(
                        timestampWithTimeZoneType.getPrecision(),
                        typedValue.getEpochMillis(),
                        typedValue.getPicosOfMilli(),
                        getTimeZoneKey(typedValue.getTimeZoneKey())).toString();
            }
            default -> throw new UnsupportedOperationException("Unsupported type " + type + " backed by java " + javaType);
        }
    }
}
