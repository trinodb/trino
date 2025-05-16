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
package io.trino.client.uri;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MICROS;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.NANOS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Objects.requireNonNull;

public class DurationUtils
{
    private static final Pattern PATTERN = Pattern.compile("^\\s*(\\d+(?:\\.\\d+)?)\\s*([a-zA-Z]+)\\s*$");

    private DurationUtils() {}

    public static Duration parseDuration(String duration)
    {
        requireNonNull(duration, "duration is null");
        checkArgument(!duration.isEmpty(), "duration is empty");

        Matcher matcher = PATTERN.matcher(duration);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("duration is not a valid data duration string: " + duration);
        }

        double value = Double.parseDouble(matcher.group(1));
        String unitString = matcher.group(2);

        return Duration.of((long) (nanosPerTimeUnit(valueOfTimeUnit(unitString)) * value), NANOS);
    }

    public static String toString(Duration duration)
    {
        ChronoUnit mostSuccinctUnit = mostSuccinctUnit(duration);
        double value = 1.0d * duration.toNanos() / nanosPerTimeUnit(mostSuccinctUnit);

        return String.format(Locale.US, "%.02f%s", value, timeUnitToString(mostSuccinctUnit));
    }

    private static ChronoUnit valueOfTimeUnit(String timeUnitString)
    {
        requireNonNull(timeUnitString, "timeUnitString is null");
        switch (timeUnitString) {
            case "ns":
                return NANOS;
            case "us":
                return MICROS;
            case "ms":
                return MILLIS;
            case "s":
                return SECONDS;
            case "m":
                return MINUTES;
            case "h":
                return HOURS;
            case "d":
                return DAYS;
            default:
                throw new IllegalArgumentException("Unknown time unit: " + timeUnitString);
        }
    }

    private static long nanosPerTimeUnit(ChronoUnit timeUnit)
    {
        switch (timeUnit) {
            case NANOS:
                return 1L;
            case MICROS:
                return 1000L;
            case MILLIS:
                return 1000L * 1000;
            case SECONDS:
                return 1000L * 1000 * 1000;
            case MINUTES:
                return 1000L * 1000 * 1000 * 60;
            case HOURS:
                return 1000L * 1000 * 1000 * 60 * 60;
            case DAYS:
                return 1000L * 1000 * 1000 * 60 * 60 * 24;
            default:
                throw new IllegalArgumentException("Unsupported time unit " + timeUnit);
        }
    }

    private static String timeUnitToString(ChronoUnit timeUnit)
    {
        requireNonNull(timeUnit, "timeUnit is null");
        switch (timeUnit) {
            case NANOS:
                return "ns";
            case MICROS:
                return "us";
            case MILLIS:
                return "ms";
            case SECONDS:
                return "s";
            case MINUTES:
                return "m";
            case HOURS:
                return "h";
            case DAYS:
                return "d";
            default:
                throw new IllegalArgumentException("Unsupported time unit " + timeUnit);
        }
    }

    public static double convertValue(Duration duration, ChronoUnit timeUnit)
    {
        requireNonNull(timeUnit, "timeUnit is null");
        return 1.0d * duration.toNanos() / nanosPerTimeUnit(timeUnit);
    }

    public static ChronoUnit mostSuccinctUnit(Duration duration)
    {
        ChronoUnit unitToUse = NANOS;
        for (ChronoUnit unitToTest : ChronoUnit.values()) {
            if (convertValue(duration, unitToTest) > 0.9999) {
                unitToUse = unitToTest;
            }
            else {
                break;
            }
        }
        return unitToUse;
    }
}
