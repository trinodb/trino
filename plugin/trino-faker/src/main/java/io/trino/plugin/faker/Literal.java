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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarcharType;
import io.trino.sql.tree.IntervalLiteral;
import io.trino.type.IpAddressType;

import java.math.BigInteger;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
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
}
