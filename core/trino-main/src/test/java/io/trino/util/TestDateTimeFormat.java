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

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.IsoFields;
import java.util.Locale;

import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDateTimeFormat
{
    @Test
    public void testNamedZones()
    {
        for (String zoneId : ZoneId.getAvailableZoneIds()) {
            ZonedDateTime parsed = parse("yyyy-MM-dd HH:mm ZZZ", "1900-01-01 00:00 " + zoneId);
            assertThat(parsed.toInstant().toEpochMilli())
                    .as("parsed instant in %s", zoneId)
                    .isEqualTo(LocalDateTime.of(1900, 1, 1, 0, 0).atZone(ZoneId.of(zoneId)).toInstant().toEpochMilli());
        }
    }

    @Test
    public void testShortNames()
    {
        ZonedDateTime parsed = parse("yyyy-MM-dd HH:mm z", "2026-07-01 00:00 EST");
        assertThat(parsed.getZone().getId()).isEqualTo("America/New_York");
        assertThat(parsed.toInstant().toEpochMilli())
                .isEqualTo(LocalDateTime.of(2026, 7, 1, 0, 0).atZone(ZoneId.of("America/New_York")).toInstant().toEpochMilli());
        assertThat(parse("yyyy-MM-dd HH:mm z", "2026-07-01 00:00 UTC").getZone().getId())
                .isEqualTo("UTC");
    }

    @Test
    public void testQuotedPatterns()
    {
        assertThat(parse("yyyy-MM-dd 'ZZZ z' HH:mm ZZZ", "2026-07-01 ZZZ z 00:00 Europe/Warsaw").getZone().getId())
                .isEqualTo("Europe/Warsaw");
        assertThat(parse("yyyy-MM-dd ''ZZZ'' HH:mm", "2026-07-01 'Europe/Warsaw' 00:00").getZone().getId())
                .isEqualTo("Europe/Warsaw");
    }

    @Test
    public void testZoneIdPrinting()
    {
        DateTimeFormat formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm ZZZ");
        assertThat(formatter.format(0, ZoneId.of("Europe/Warsaw"), Locale.ENGLISH))
                .isEqualTo("1970-01-01 01:00 Europe/Warsaw");
    }

    @Test
    public void testOffsets()
    {
        assertThat(parse("yyyy-MM-dd HH:mm Z", "2026-07-01 00:00 +0545").getZone().getId())
                .isEqualTo("+05:45");
        assertThat(parse("yyyy-MM-dd HH:mm ZZ", "2026-07-01 00:00 +05:45").getZone().getId())
                .isEqualTo("+05:45");
    }

    @Test
    public void testTwoDigitYears()
    {
        assertThat(parse("yyMMdd", "260701").getYear()).isEqualTo(2026);
        assertThat(parse("YY-MM-dd", "26-07-01").getYear()).isEqualTo(2026);
        assertThat(parse("xx-ww-e", "26-01-1").get(IsoFields.WEEK_BASED_YEAR)).isEqualTo(2026);
        assertThat(parse("yy-MM-dd", "2026-07-01").getYear()).isEqualTo(2026);
    }

    @Test
    public void testInvalidPattern()
    {
        assertThatThrownBy(() -> DateTimeFormat.forPattern(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid pattern specification");
    }

    @Test
    public void testDaylightSavingTransitions()
    {
        assertThatThrownBy(() -> parse("yyyy-MM-dd HH:mm ZZZ", "2026-03-29 02:30 Europe/Warsaw"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(parse("yyyy-MM-dd HH:mm ZZZ", "2026-10-25 02:30 Europe/Warsaw").toInstant().toEpochMilli())
                .isEqualTo(LocalDateTime.of(2026, 10, 25, 2, 30).atZone(ZoneId.of("Europe/Warsaw")).toInstant().toEpochMilli());
    }

    @Test
    public void testEraNames()
    {
        assertThat(parse("G YYYY", "BCE 2013").getYear()).isEqualTo(-2012);
        assertThat(parse("G YYYY", "CE 2013").getYear()).isEqualTo(2013);
        assertThat(parse("G yyyy", "BCE 2013").getYear()).isEqualTo(2013);
    }

    @Test
    public void testEquivalentFieldOrder()
    {
        assertThat(parse("k HH", "24 01").getHour()).isEqualTo(1);
        assertThat(parse("HH k", "01 24").getHour()).isZero();
        assertThat(parse("h K", "12 1").getHour()).isEqualTo(1);
        assertThat(parse("K h", "1 12").getHour()).isZero();
        assertThat(parse("Y y", "2012 2013").getYear()).isEqualTo(2013);
        assertThat(parse("y Y", "2013 2012").getYear()).isEqualTo(2012);
    }

    @Test
    public void testParsedOffsetPrecedence()
    {
        for (ZonedDateTime parsed : new ZonedDateTime[] {
                parse("Z ZZZ", "+0200 UTC"),
                parse("ZZZ Z", "UTC +0200"),
        }) {
            assertThat(parsed.getOffset()).isEqualTo(ZoneOffset.ofHours(2));
            assertThat(parsed.toInstant().toEpochMilli()).isEqualTo(-7_200_000);
        }
    }

    @Test
    public void testFractionalSeconds()
    {
        assertThat(parse("yyyy-MM-dd HH:mm:ss.SSSSSSSSSSSS", "2026-01-01 00:00:00.123456789012").getNano())
                .isEqualTo(123_000_000);
        assertThat(DateTimeFormat.forPattern("SSSSSSSSSSSS").format(123, UTC, Locale.forLanguageTag("ar")))
                .isEqualTo("123000000000");
    }

    @Test
    public void testSubMinuteOffsetFormatting()
    {
        assertThat(DateTimeFormat.forPattern("Z ZZ").format(0, ZoneOffset.ofTotalSeconds(-52), Locale.ENGLISH))
                .isEqualTo("-0000 -00:00");
    }

    @Test
    public void testHistoricalDateLineOverlap()
    {
        assertThat(parse("yyyy-MM-dd HH:mm ZZZ", "1892-07-04 23:59 Pacific/Apia").getOffset())
                .isEqualTo(ZoneOffset.ofHoursMinutesSeconds(-11, -26, -56));
    }

    @Test
    public void testPartialFields()
    {
        assertThat(parse("MM", "5").toLocalDate()).isEqualTo(LocalDate.of(2000, 5, 1));
        assertThat(parse("w", "2").toLocalDate()).isEqualTo(LocalDate.of(2000, 1, 10));
        assertThat(parse("HH:mm", "3:4").toLocalDateTime()).isEqualTo(LocalDateTime.of(1970, 1, 1, 3, 4));
        assertThat(DateTimeFormat.forMysqlPattern("%Y %y").parse("2013 14", UTC, Locale.ENGLISH).getYear())
                .isEqualTo(2014);
        assertThatThrownBy(() -> parse("xxxx-ww-e", "2000-53-1"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testPartialIsoFormats()
    {
        assertThat(DateTimeFormat.parseIsoDate("2001-222")).isEqualTo(LocalDate.of(2001, 8, 10));
        assertThat(DateTimeFormat.parseIsoDate("2001-W34")).isEqualTo(LocalDate.of(2001, 8, 20));
        assertThat(DateTimeFormat.parseIsoTimestamp("2001-08-22T03:04,5+05", UTC).toLocalDateTime())
                .isEqualTo(LocalDateTime.of(2001, 8, 22, 3, 4, 30));
        assertThat(DateTimeFormat.parseIsoTimestamp("2001-08-22T03:04:05.123456789+05:00:00.000", UTC).getNano())
                .isEqualTo(123_000_000);
        assertThatThrownBy(() -> DateTimeFormat.parseIsoTimestamp("T", UTC))
                .isInstanceOf(IllegalArgumentException.class);
        for (String value : new String[] {"TZ", "T+05", "T+02:00"}) {
            assertThatThrownBy(() -> DateTimeFormat.parseIsoTimestamp(value, UTC))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static ZonedDateTime parse(String pattern, String text)
    {
        return DateTimeFormat.forPattern(pattern).parse(text, UTC, Locale.ENGLISH);
    }
}
