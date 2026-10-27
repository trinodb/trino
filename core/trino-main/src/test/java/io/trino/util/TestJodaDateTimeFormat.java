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

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;

import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.util.DateTimeZoneIndex.getDateTimeZone;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestJodaDateTimeFormat
{
    @Test
    public void testNamedZones()
    {
        for (String zoneId : ZoneId.getAvailableZoneIds()) {
            DateTime parsed = parse("yyyy-MM-dd HH:mm ZZZ", "1900-01-01 00:00 " + zoneId);
            assertThat(parsed.getMillis())
                    .as("parsed instant in %s", zoneId)
                    .isEqualTo(LocalDateTime.of(1900, 1, 1, 0, 0).atZone(ZoneId.of(zoneId)).toInstant().toEpochMilli());
        }
    }

    @Test
    public void testShortNames()
    {
        DateTime parsed = parse("yyyy-MM-dd HH:mm z", "2026-07-01 00:00 EST");
        assertThat(parsed.getZone().getID()).isEqualTo("America/New_York");
        assertThat(parsed.getMillis())
                .isEqualTo(LocalDateTime.of(2026, 7, 1, 0, 0).atZone(ZoneId.of("America/New_York")).toInstant().toEpochMilli());
        assertThat(parse("yyyy-MM-dd HH:mm z", "2026-07-01 00:00 UTC").getZone().getID())
                .isEqualTo("UTC");
    }

    @Test
    public void testQuotedPatterns()
    {
        assertThat(parse("yyyy-MM-dd 'ZZZ z' HH:mm ZZZ", "2026-07-01 ZZZ z 00:00 Europe/Warsaw").getZone().getID())
                .isEqualTo("Europe/Warsaw");
        assertThat(parse("yyyy-MM-dd ''ZZZ'' HH:mm", "2026-07-01 'Europe/Warsaw' 00:00").getZone().getID())
                .isEqualTo("Europe/Warsaw");
    }

    @Test
    public void testZoneIdPrinting()
    {
        DateTimeFormatter formatter = JodaDateTimeFormat.forPattern("yyyy-MM-dd HH:mm ZZZ")
                .withZone(getDateTimeZone(getTimeZoneKey("Europe/Warsaw")));
        assertThat(JodaDateTimeFormat.format(formatter, 0))
                .isEqualTo("1970-01-01 01:00 Europe/Warsaw");
    }

    @Test
    public void testOffsets()
    {
        assertThat(parse("yyyy-MM-dd HH:mm Z", "2026-07-01 00:00 +0545").getZone().getID())
                .isEqualTo("+05:45");
        assertThat(parse("yyyy-MM-dd HH:mm ZZ", "2026-07-01 00:00 +05:45").getZone().getID())
                .isEqualTo("+05:45");
    }

    @Test
    public void testTwoDigitYears()
    {
        assertThat(parse("yyMMdd", "260701").getYear()).isEqualTo(2026);
        assertThat(parse("YY-MM-dd", "26-07-01").getYear()).isEqualTo(2026);
        assertThat(parse("xx-ww-e", "26-01-1").getWeekyear()).isEqualTo(2026);
        assertThat(parse("yy-MM-dd", "2026-07-01").getYear()).isEqualTo(2026);
    }

    @Test
    public void testInvalidPattern()
    {
        assertThatThrownBy(() -> JodaDateTimeFormat.forPattern(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid pattern specification");
    }

    @Test
    public void testDaylightSavingTransitions()
    {
        assertThatThrownBy(() -> parse("yyyy-MM-dd HH:mm ZZZ", "2026-03-29 02:30 Europe/Warsaw"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(parse("yyyy-MM-dd HH:mm ZZZ", "2026-10-25 02:30 Europe/Warsaw").getMillis())
                .isEqualTo(LocalDateTime.of(2026, 10, 25, 2, 30).atZone(ZoneId.of("Europe/Warsaw")).toInstant().toEpochMilli());
    }

    private static DateTime parse(String pattern, String text)
    {
        return JodaDateTimeFormat.parse(JodaDateTimeFormat.forPattern(pattern).withOffsetParsed(), text);
    }
}
