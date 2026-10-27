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
package io.trino.util;

import com.google.common.collect.ImmutableMap;
import io.airlift.concurrent.ThreadLocalCache;
import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadablePartial;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.DateTimeParserBucket;
import org.joda.time.format.DateTimePrinter;

import java.io.IOException;
import java.io.Writer;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.IsoFields;
import java.util.Locale;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.util.DateTimeZoneIndex.getDateTimeZone;

public final class JodaDateTimeFormat
{
    private static final ThreadLocalCache<String, DateTimeFormatter> FORMATTERS = new ThreadLocalCache<>(100, JodaDateTimeFormat::createFormatter);
    private static final Map<String, DateTimeZone> SHORT_ZONE_NAMES = ImmutableMap.<String, DateTimeZone>builder()
            .put("UT", DateTimeZone.UTC)
            .put("UTC", DateTimeZone.UTC)
            .put("GMT", DateTimeZone.UTC)
            .put("EST", getDateTimeZone(getTimeZoneKey("America/New_York")))
            .put("EDT", getDateTimeZone(getTimeZoneKey("America/New_York")))
            .put("CST", getDateTimeZone(getTimeZoneKey("America/Chicago")))
            .put("CDT", getDateTimeZone(getTimeZoneKey("America/Chicago")))
            .put("MST", getDateTimeZone(getTimeZoneKey("America/Denver")))
            .put("MDT", getDateTimeZone(getTimeZoneKey("America/Denver")))
            .put("PST", getDateTimeZone(getTimeZoneKey("America/Los_Angeles")))
            .put("PDT", getDateTimeZone(getTimeZoneKey("America/Los_Angeles")))
            .buildOrThrow();
    private static final DateTimeParser ZONE_ID_PARSER = createZoneIdParser();
    private static final DateTimePrinter ZONE_ID_PRINTER = new ZoneIdPrinter();

    private JodaDateTimeFormat() {}

    /**
     * Uses Joda-Time pattern syntax, resolving time zone IDs and short names with JDK rules.
     */
    public static DateTimeFormatter forPattern(String pattern)
    {
        return FORMATTERS.get(pattern);
    }

    public static DateTime parse(DateTimeFormatter formatter, String text)
    {
        DateTimeParser parser = formatter.getParser();
        if (parser == null) {
            throw new UnsupportedOperationException("Parsing not supported");
        }
        Chronology chronology = formatter.getChronology();
        if (chronology == null) {
            chronology = ISOChronology.getInstanceUTC();
        }
        if (formatter.getZone() != null) {
            chronology = chronology.withZone(formatter.getZone());
        }
        // DateTimeFormatter.parseDateTime initializes Joda-Time's default zone even
        // when a chronology is supplied. Parse directly into a bucket to avoid that lookup.
        DateTimeParserBucket bucket = new DateTimeParserBucket(0, chronology, formatter.getLocale(), formatter.getPivotYear(), formatter.getDefaultYear());
        long millis = bucket.parseMillis(parser, text);
        if (formatter.isOffsetParsed() && bucket.getOffsetInteger() != null) {
            chronology = chronology.withZone(DateTimeZone.forOffsetMillis(bucket.getOffsetInteger()));
        }
        else if (bucket.getZone() != null) {
            chronology = chronology.withZone(bucket.getZone());
        }
        DateTime dateTime = new DateTime(millis, chronology);
        if (formatter.getZone() != null) {
            dateTime = dateTime.withZone(formatter.getZone());
        }
        return dateTime;
    }

    public static String format(DateTimeFormatter formatter, long millis)
    {
        Chronology chronology = formatter.getChronology();
        if (chronology == null) {
            chronology = ISOChronology.getInstanceUTC();
        }
        // Printing a ReadableInstant avoids the default-zone lookup in print(long).
        return formatter.print(new DateTime(millis, chronology));
    }

    private static DateTimeFormatter createFormatter(String pattern)
    {
        checkArgument(!pattern.isEmpty(), "Invalid pattern specification");
        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
        boolean quoted = false;
        int start = 0;
        for (int index = 0; index < pattern.length(); index++) {
            char character = pattern.charAt(index);
            if (character == '\'') {
                if (index + 1 < pattern.length() && pattern.charAt(index + 1) == '\'') {
                    index++;
                }
                else {
                    quoted = !quoted;
                }
                continue;
            }
            if (quoted || (character != 'z' && character != 'Z' && character != 'x' && character != 'y' && character != 'Y')) {
                continue;
            }
            int end = index + 1;
            while (end < pattern.length() && pattern.charAt(end) == character) {
                end++;
            }
            int length = end - index;
            if ((character == 'Z' && length >= 3) || (character == 'z' && length < 4) || character == 'x' || character == 'y' || character == 'Y') {
                if (start < index) {
                    builder.appendPattern(pattern.substring(start, index));
                }
                if (character == 'Z') {
                    builder.append(ZONE_ID_PRINTER, ZONE_ID_PARSER);
                }
                else if (character == 'z') {
                    // Preserve Joda-Time's short-name aliases, including their daylight saving rules.
                    builder.appendTimeZoneShortName(SHORT_ZONE_NAMES);
                }
                else {
                    appendYear(builder, character, length, nextTokenIsNumeric(pattern, end));
                }
                start = end;
            }
            index = end - 1;
        }
        if (start < pattern.length()) {
            builder.appendPattern(pattern.substring(start));
        }
        return builder.toFormatter();
    }

    private static void appendYear(DateTimeFormatterBuilder builder, char character, int length, boolean nextTokenIsNumeric)
    {
        if (length == 2) {
            // Joda-Time computes the pivot with new DateTime(), loading its default zone.
            LocalDate today = LocalDate.now();
            if (character == 'x') {
                builder.appendTwoDigitWeekyear(today.get(IsoFields.WEEK_BASED_YEAR) - 30, !nextTokenIsNumeric);
            }
            else {
                builder.appendTwoDigitYear(today.getYear() - 30, !nextTokenIsNumeric);
            }
            return;
        }
        int maxDigits = 9;
        if (nextTokenIsNumeric) {
            maxDigits = length;
        }
        switch (character) {
            case 'x' -> builder.appendWeekyear(length, maxDigits);
            case 'y' -> builder.appendYear(length, maxDigits);
            case 'Y' -> builder.appendYearOfEra(length, maxDigits);
            default -> throw new IllegalArgumentException("Unexpected year pattern: " + character);
        }
    }

    private static boolean nextTokenIsNumeric(String pattern, int index)
    {
        if (index == pattern.length()) {
            return false;
        }
        char character = pattern.charAt(index);
        if (character == 'M') {
            int end = index + 1;
            while (end < pattern.length() && pattern.charAt(end) == character) {
                end++;
            }
            return end - index <= 2;
        }
        return "cCxyYdhHmsSeDFwWkK".indexOf(character) >= 0;
    }

    private static DateTimeParser createZoneIdParser()
    {
        ImmutableMap.Builder<String, DateTimeZone> zones = ImmutableMap.builder();
        for (String zoneId : ZoneId.getAvailableZoneIds()) {
            zones.put(zoneId, new JdkDateTimeZone(ZoneId.of(zoneId)));
        }
        return new DateTimeFormatterBuilder()
                .appendTimeZoneName(zones.buildOrThrow())
                .toParser();
    }

    private static final class ZoneIdPrinter
            implements DateTimePrinter
    {
        @Override
        public int estimatePrintedLength()
        {
            return 20;
        }

        @Override
        public void printTo(StringBuffer buffer, long instant, Chronology chronology, int displayOffset, DateTimeZone displayZone, Locale locale)
        {
            if (displayZone != null) {
                buffer.append(displayZone.getID());
            }
        }

        @Override
        public void printTo(Writer writer, long instant, Chronology chronology, int displayOffset, DateTimeZone displayZone, Locale locale)
                throws IOException
        {
            if (displayZone != null) {
                writer.write(displayZone.getID());
            }
        }

        @Override
        public void printTo(StringBuffer buffer, ReadablePartial partial, Locale locale)
        {
            // Partial values have no time zone.
        }

        @Override
        public void printTo(Writer writer, ReadablePartial partial, Locale locale)
        {
            // Partial values have no time zone.
        }
    }
}
