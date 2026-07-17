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
package io.trino.client;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Long.parseLong;
import static java.lang.Math.addExact;
import static java.lang.Math.multiplyExact;
import static java.lang.String.format;

public final class IntervalDayTime
{
    private static final long MICROS_IN_SECOND = 1_000_000;
    private static final long MICROS_IN_MINUTE = 60 * MICROS_IN_SECOND;
    private static final long MICROS_IN_HOUR = 60 * MICROS_IN_MINUTE;
    private static final long MICROS_IN_DAY = 24 * MICROS_IN_HOUR;
    private static final int PICOS_PER_MICRO = 1_000_000;
    private static final int MAX_PRECISION = 12;

    private static final String LONG_MIN_VALUE = "-106751991 04:00:54.775808";

    private static final Pattern FORMAT = Pattern.compile("(\\d+) (\\d+):(\\d+):(\\d+)(?:\\.(\\d+))?");

    // Legacy millisecond API — retained for binary compatibility with clients compiled against the
    // pre-parametric-interval trino-client. The micro/picosecond API above is the current one.
    private static final long MILLIS_IN_SECOND = 1000;
    private static final long MILLIS_IN_MINUTE = 60 * MILLIS_IN_SECOND;
    private static final long MILLIS_IN_HOUR = 60 * MILLIS_IN_MINUTE;
    private static final long MILLIS_IN_DAY = 24 * MILLIS_IN_HOUR;

    private static final String LONG_MIN_VALUE_MILLIS = "-106751991167 07:12:55.808";

    private static final Pattern MILLIS_FORMAT = Pattern.compile("(\\d+) (\\d+):(\\d+):(\\d+).(\\d+)");

    private IntervalDayTime() {}

    public static long toMicros(long day, long hour, long minute, long second, long micros)
    {
        try {
            long value = micros;
            value = addExact(value, multiplyExact(day, MICROS_IN_DAY));
            value = addExact(value, multiplyExact(hour, MICROS_IN_HOUR));
            value = addExact(value, multiplyExact(minute, MICROS_IN_MINUTE));
            value = addExact(value, multiplyExact(second, MICROS_IN_SECOND));
            return value;
        }
        catch (ArithmeticException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static String formatMicros(long micros)
    {
        return formatMicros(micros, 6);
    }

    /// Renders the interval with exactly `fractionalPrecision` sub-second digits (0 to 6).
    public static String formatMicros(long micros, int fractionalPrecision)
    {
        return formatInterval(micros, 0, fractionalPrecision);
    }

    /// Renders the interval — microseconds plus the picoseconds within that microsecond — with exactly
    /// `fractionalPrecision` sub-second digits (0 to 12).
    public static String formatInterval(long micros, int picosOfMicro, int fractionalPrecision)
    {
        if (micros == Long.MIN_VALUE && picosOfMicro == 0) {
            return LONG_MIN_VALUE;
        }
        String sign = "";
        if (micros < 0) {
            sign = "-";
            // negate the (micros, picosOfMicro) pair, where picosOfMicro is a positive increment
            if (picosOfMicro > 0) {
                micros = -(micros + 1);
                picosOfMicro = PICOS_PER_MICRO - picosOfMicro;
            }
            else {
                micros = -micros;
            }
        }

        long day = micros / MICROS_IN_DAY;
        micros %= MICROS_IN_DAY;
        long hour = micros / MICROS_IN_HOUR;
        micros %= MICROS_IN_HOUR;
        long minute = micros / MICROS_IN_MINUTE;
        micros %= MICROS_IN_MINUTE;
        long second = micros / MICROS_IN_SECOND;
        micros %= MICROS_IN_SECOND;

        if (fractionalPrecision == 0) {
            return format("%s%d %02d:%02d:%02d", sign, day, hour, minute, second);
        }
        // combine the microsecond and picosecond fractions into a twelve-digit picosecond fraction, then
        // scale it down to the requested precision
        long picoFraction = micros * PICOS_PER_MICRO + picosOfMicro;
        long divisor = 1;
        for (int i = fractionalPrecision; i < MAX_PRECISION; i++) {
            divisor *= 10;
        }
        return format("%s%d %02d:%02d:%02d.%0" + fractionalPrecision + "d", sign, day, hour, minute, second, picoFraction / divisor);
    }

    public static long parseMicros(String value)
    {
        long[] picos = parseToPicos(value);
        return picos[0];
    }

    /// Parses a day-time interval string into microseconds and the picoseconds within the last
    /// microsecond, preserving a fractional-seconds precision above 6. The fraction is optional, so a
    /// value rendered at precision 0 (no decimal point) parses to zero sub-second components.
    public static long[] parseToPicos(String value)
    {
        if (value.equals(LONG_MIN_VALUE)) {
            return new long[] {Long.MIN_VALUE, 0};
        }

        long signum = 1;
        if (value.startsWith("-")) {
            signum = -1;
            value = value.substring(1);
        }

        Matcher matcher = FORMAT.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid day-time interval: " + value);
        }

        long days = parseLong(matcher.group(1));
        long hours = parseLong(matcher.group(2));
        long minutes = parseLong(matcher.group(3));
        long seconds = parseLong(matcher.group(4));
        long picoFraction = parseFraction(matcher.group(5));
        long micros = toMicros(days, hours, minutes, seconds, picoFraction / PICOS_PER_MICRO);
        int picosOfMicro = (int) (picoFraction % PICOS_PER_MICRO);

        if (signum < 0) {
            // negate the (micros, picosOfMicro) pair, where picosOfMicro is a positive increment
            if (picosOfMicro > 0) {
                micros = -(micros + 1);
                picosOfMicro = PICOS_PER_MICRO - picosOfMicro;
            }
            else {
                micros = -micros;
            }
        }

        return new long[] {micros, picosOfMicro};
    }

    /// Legacy millisecond factory. Retained for binary compatibility; prefer {@link #toMicros}.
    public static long toMillis(long day, long hour, long minute, long second, long millis)
    {
        try {
            long value = millis;
            value = addExact(value, multiplyExact(day, MILLIS_IN_DAY));
            value = addExact(value, multiplyExact(hour, MILLIS_IN_HOUR));
            value = addExact(value, multiplyExact(minute, MILLIS_IN_MINUTE));
            value = addExact(value, multiplyExact(second, MILLIS_IN_SECOND));
            return value;
        }
        catch (ArithmeticException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /// Legacy millisecond renderer (three fractional digits). Retained for binary compatibility; prefer
    /// {@link #formatMicros(long)} or {@link #formatInterval}.
    public static String formatMillis(long millis)
    {
        if (millis == Long.MIN_VALUE) {
            return LONG_MIN_VALUE_MILLIS;
        }
        String sign = "";
        if (millis < 0) {
            sign = "-";
            millis = -millis;
        }

        long day = millis / MILLIS_IN_DAY;
        millis %= MILLIS_IN_DAY;
        long hour = millis / MILLIS_IN_HOUR;
        millis %= MILLIS_IN_HOUR;
        long minute = millis / MILLIS_IN_MINUTE;
        millis %= MILLIS_IN_MINUTE;
        long second = millis / MILLIS_IN_SECOND;
        millis %= MILLIS_IN_SECOND;

        return format("%s%d %02d:%02d:%02d.%03d", sign, day, hour, minute, second, millis);
    }

    /// Legacy millisecond parser. Retained for binary compatibility; prefer {@link #parseMicros} or
    /// {@link #parseToPicos}.
    public static long parseMillis(String value)
    {
        if (value.equals(LONG_MIN_VALUE_MILLIS)) {
            return Long.MIN_VALUE;
        }

        long signum = 1;
        if (value.startsWith("-")) {
            signum = -1;
            value = value.substring(1);
        }

        Matcher matcher = MILLIS_FORMAT.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid day-time interval: " + value);
        }

        long days = parseLong(matcher.group(1));
        long hours = parseLong(matcher.group(2));
        long minutes = parseLong(matcher.group(3));
        long seconds = parseLong(matcher.group(4));
        long millis = parseLong(matcher.group(5));

        return toMillis(days, hours, minutes, seconds, millis) * signum;
    }

    private static long parseFraction(String fraction)
    {
        if (fraction == null) {
            return 0;
        }
        // scale the fractional part to picoseconds (12 digits)
        long value = parseLong(fraction);
        for (int length = fraction.length(); length < MAX_PRECISION; length++) {
            value *= 10;
        }
        for (int length = fraction.length(); length > MAX_PRECISION; length--) {
            value /= 10;
        }
        return value;
    }
}
