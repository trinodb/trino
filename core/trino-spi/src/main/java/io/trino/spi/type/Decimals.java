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
package io.trino.spi.type;

import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.LongArrayBlockBuilder;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Int128Math.absExact;
import static io.trino.spi.type.Int128Math.powerOfTen;
import static java.lang.Math.abs;
import static java.lang.Math.pow;
import static java.lang.Math.round;
import static java.math.BigInteger.TEN;
import static java.math.RoundingMode.UNNECESSARY;

public final class Decimals
{
    private Decimals() {}

    public static final Int128 MAX_UNSCALED_DECIMAL = Int128.valueOf("99999999999999999999999999999999999999");
    public static final Int128 MIN_UNSCALED_DECIMAL = Int128.valueOf("-99999999999999999999999999999999999999");

    public static final int MAX_PRECISION = 38;
    public static final int MAX_SHORT_PRECISION = 18;

    private static final Pattern DECIMAL_PATTERN = Pattern.compile("([+-]?)(\\d(?:_?\\d)*)?(?:\\.(\\d(?:_?\\d)*)?)?");

    private static final int LONG_POWERS_OF_TEN_TABLE_LENGTH = 19;
    private static final int BIG_INTEGER_POWERS_OF_TEN_TABLE_LENGTH = 100;
    private static final long[] LONG_POWERS_OF_TEN = new long[LONG_POWERS_OF_TEN_TABLE_LENGTH];
    private static final BigInteger[] BIG_INTEGER_POWERS_OF_TEN = new BigInteger[BIG_INTEGER_POWERS_OF_TEN_TABLE_LENGTH];

    static {
        for (int i = 0; i < LONG_POWERS_OF_TEN.length; ++i) {
            // Although this computes using doubles, incidentally, this is exact for all powers of 10 that fit in a long.
            LONG_POWERS_OF_TEN[i] = round(pow(10, i));
        }

        for (int i = 0; i < BIG_INTEGER_POWERS_OF_TEN.length; ++i) {
            BIG_INTEGER_POWERS_OF_TEN[i] = TEN.pow(i);
        }
    }

    public static long longTenToNth(int n)
    {
        return LONG_POWERS_OF_TEN[n];
    }

    public static BigInteger bigIntegerTenToNth(int n)
    {
        return BIG_INTEGER_POWERS_OF_TEN[n];
    }

    public static DecimalParseResult parse(String stringValue)
    {
        Matcher matcher = DECIMAL_PATTERN.matcher(stringValue);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid DECIMAL value '" + stringValue + "'");
        }

        String sign = getMatcherGroup(matcher, 1);
        if (sign.isEmpty()) {
            sign = "+";
        }
        String integralPart = getMatcherGroup(matcher, 2);
        String fractionalPart = getMatcherGroup(matcher, 3);

        if (integralPart.isEmpty() && fractionalPart.isEmpty()) {
            throw new IllegalArgumentException("Invalid DECIMAL value '" + stringValue + "'");
        }

        integralPart = stripLeadingZeros(integralPart.replace("_", ""));
        fractionalPart = fractionalPart.replace("_", "");

        int scale = fractionalPart.length();
        int precision = integralPart.length() + scale;
        if (precision == 0) {
            precision = 1;
        }

        String unscaledValue = sign + (integralPart.isEmpty() ? "0" : "") + integralPart + fractionalPart;
        Object value;
        if (precision <= MAX_SHORT_PRECISION) {
            value = Long.parseLong(unscaledValue);
        }
        else {
            value = Int128.valueOf(new BigInteger(unscaledValue));
        }

        if (precision > MAX_PRECISION) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Value exceeds maximum precision");
        }

        return new DecimalParseResult(value, createDecimalType(precision, scale));
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

    private static String getMatcherGroup(MatchResult matcher, int group)
    {
        String groupValue = matcher.group(group);
        if (groupValue == null) {
            groupValue = "";
        }
        return groupValue;
    }

    public static long encodeShortScaledValue(BigDecimal value, int scale)
    {
        return encodeShortScaledValue(value, scale, UNNECESSARY);
    }

    public static long encodeShortScaledValue(BigDecimal value, int scale, RoundingMode roundingMode)
    {
        if (scale < 0) {
            throw new IllegalArgumentException("scale is negative: " + scale);
        }
        return value.setScale(scale, roundingMode).unscaledValue().longValueExact();
    }

    public static Int128 encodeScaledValue(BigDecimal value, int scale)
    {
        return encodeScaledValue(value, scale, UNNECESSARY);
    }

    public static Int128 encodeScaledValue(BigDecimal value, int scale, RoundingMode roundingMode)
    {
        if (scale < 0) {
            throw new IllegalArgumentException("scale is negative: " + scale);
        }
        return valueOf(value.setScale(scale, roundingMode));
    }

    public static String toString(long unscaledValue, int scale)
    {
        return toString(Long.toString(unscaledValue), scale);
    }

    public static String toString(Int128 unscaledValue, int scale)
    {
        return toString(unscaledValue.toString(), scale);
    }

    public static String toString(BigInteger unscaledValue, int scale)
    {
        return toString(unscaledValue.toString(), scale);
    }

    private static String toString(String unscaledValueString, int scale)
    {
        StringBuilder resultBuilder = new StringBuilder();
        // add sign
        if (unscaledValueString.startsWith("-")) {
            resultBuilder.append("-");
            unscaledValueString = unscaledValueString.substring(1);
        }

        // integral part
        if (unscaledValueString.length() <= scale) {
            resultBuilder.append("0");
        }
        else {
            resultBuilder.append(unscaledValueString, 0, unscaledValueString.length() - scale);
        }

        // fractional part
        if (scale > 0) {
            resultBuilder.append(".");
            if (unscaledValueString.length() < scale) {
                // prepend zeros to fractional part if unscaled value length is shorter than scale
                resultBuilder.append("0".repeat(scale - unscaledValueString.length()));
                resultBuilder.append(unscaledValueString);
            }
            else {
                // otherwise just use scale last digits of unscaled value
                resultBuilder.append(unscaledValueString.substring(unscaledValueString.length() - scale));
            }
        }
        return resultBuilder.toString();
    }

    public static boolean overflows(long value, int precision)
    {
        if (precision > MAX_SHORT_PRECISION) {
            throw new IllegalArgumentException("expected precision to be less than " + MAX_SHORT_PRECISION);
        }
        return abs(value) >= longTenToNth(precision);
    }

    public static boolean overflows(BigInteger value, int precision)
    {
        return value.abs().compareTo(bigIntegerTenToNth(precision)) >= 0;
    }

    public static boolean overflows(BigDecimal value, long precision)
    {
        return value.precision() > precision;
    }

    public static BigDecimal readBigDecimal(DecimalType type, Block block, int position)
    {
        BigInteger unscaledValue = type.isShort()
                ? BigInteger.valueOf(type.getLong(block, position))
                : ((Int128) type.getObject(block, position)).toBigInteger();
        return new BigDecimal(unscaledValue, type.getScale(), new MathContext(type.getPrecision()));
    }

    public static void writeBigDecimal(DecimalType decimalType, BlockBuilder blockBuilder, BigDecimal value)
    {
        decimalType.writeObject(blockBuilder, valueOf(value));
    }

    public static BigDecimal rescale(BigDecimal value, DecimalType type)
    {
        value = value.setScale(type.getScale(), UNNECESSARY);

        if (value.precision() > type.getPrecision()) {
            throw new IllegalArgumentException("decimal precision larger than column precision");
        }
        return value;
    }

    public static void writeShortDecimal(BlockBuilder blockBuilder, long value)
    {
        ((LongArrayBlockBuilder) blockBuilder).writeLong(value);
    }

    public static long rescale(long value, int fromScale, int toScale)
    {
        if (toScale < fromScale) {
            throw new IllegalArgumentException("target scale must be larger than source scale");
        }
        return value * longTenToNth(toScale - fromScale);
    }

    public static BigInteger rescale(BigInteger value, int fromScale, int toScale)
    {
        if (toScale < fromScale) {
            throw new IllegalArgumentException("target scale must be larger than source scale");
        }
        return value.multiply(bigIntegerTenToNth(toScale - fromScale));
    }

    /**
     * Converts {@link BigDecimal} to {@link Int128} representing it for long {@link DecimalType}.
     * It is caller responsibility to ensure that {@code value.scale()} equals to {@link DecimalType#getScale()}.
     */
    public static Int128 valueOf(BigDecimal value)
    {
        return valueOf(value.unscaledValue());
    }

    public static Int128 valueOf(BigInteger value)
    {
        Int128 result;
        try {
            result = Int128.valueOf(value);
        }
        catch (Exception e) {
            throw new ArithmeticException("Decimal overflow");
        }
        throwIfOverflows(result.getHigh(), result.getLow());
        return result;
    }

    public static void throwIfOverflows(long high, long low)
    {
        if (overflows(high, low)) {
            throw new ArithmeticException("Decimal overflow");
        }
    }

    public static boolean overflows(Int128 value)
    {
        return overflows(value.getHigh(), value.getLow());
    }

    public static boolean overflows(long high, long low)
    {
        return Int128.compare(high, low, MAX_UNSCALED_DECIMAL.getHigh(), MAX_UNSCALED_DECIMAL.getLow()) > 0
                || Int128.compare(high, low, MIN_UNSCALED_DECIMAL.getHigh(), MIN_UNSCALED_DECIMAL.getLow()) < 0;
    }

    public static boolean overflows(Int128 value, int precision)
    {
        if (precision > MAX_PRECISION) {
            throw new IllegalArgumentException("precision must be in [1, " + MAX_PRECISION + "] range");
        }

        if (precision == MAX_PRECISION) {
            return overflows(value.getHigh(), value.getLow());
        }

        return absExact(value).compareTo(powerOfTen(precision)) >= 0;
    }
}
