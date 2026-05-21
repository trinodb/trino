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
package io.trino.json;

import io.trino.json.ir.TypedValue;
import org.junit.jupiter.api.Test;

import java.time.DateTimeException;

import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.type.DateTimes.parseTimeWithTimeZone;
import static io.trino.type.DateTimes.parseTimestamp;
import static io.trino.type.DateTimes.parseTimestampWithTimeZone;
import static io.trino.util.DateTimeUtils.parseDate;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestJsonDateTimeTemplate
{
    @Test
    public void testTypeInference()
    {
        assertThat(JsonDateTimeTemplate.parse("YYYY-MM-DD").getType()).isEqualTo(DATE);
        assertThat(JsonDateTimeTemplate.parse("HH24:MI:SS.FF3TZH:TZM").getType()).isEqualTo(createTimeWithTimeZoneType(3));
        assertThat(JsonDateTimeTemplate.parse("YYYY-MM-DD HH24:MI:SS.FF3").getType()).isEqualTo(createTimestampType(3));
        assertThat(JsonDateTimeTemplate.parse("YYYY-MM-DD HH24:MI:SS.FF3 TZH:TZM").getType()).isEqualTo(createTimestampWithTimeZoneType(3));
    }

    @Test
    public void testParseValue()
    {
        TypedValue date = JsonDateTimeTemplate.parse("YYYY-MM-DD").parseValue("2024-01-02");
        assertThat(date).isEqualTo(new TypedValue(DATE, (long) parseDate("2024-01-02")));

        TypedValue timeWithTimeZone = JsonDateTimeTemplate.parse("HH24:MI:SS.FF3TZH:TZM").parseValue("12:34:56.789+05:30");
        assertThat(timeWithTimeZone).isEqualTo(TypedValue.fromValueAsObject(createTimeWithTimeZoneType(3), parseTimeWithTimeZone(3, "12:34:56.789 +05:30")));

        TypedValue timestamp = JsonDateTimeTemplate.parse("YYYY-MM-DD HH24:MI:SS.FF3").parseValue("2024-01-02 12:34:56.789");
        assertThat(timestamp).isEqualTo(TypedValue.fromValueAsObject(createTimestampType(3), parseTimestamp(3, "2024-01-02 12:34:56.789")));

        TypedValue timestampWithTimeZone = JsonDateTimeTemplate.parse("YYYY-MM-DD HH24:MI:SS.FF3 TZH:TZM").parseValue("2024-01-02 12:34:56.789 +05:30");
        assertThat(timestampWithTimeZone).isEqualTo(TypedValue.fromValueAsObject(createTimestampWithTimeZoneType(3), parseTimestampWithTimeZone(3, "2024-01-02 12:34:56.789 +05:30")));
    }

    @Test
    public void testNegativeSubHourOffset()
    {
        // Offsets like -00:30 round the signed hour to 0; the sign must be preserved
        // independently of the hour magnitude.
        TypedValue negativeHalfHour = JsonDateTimeTemplate.parse("HH24:MI:SS.FF3TZH:TZM").parseValue("12:34:56.789-00:30");
        assertThat(negativeHalfHour).isEqualTo(TypedValue.fromValueAsObject(createTimeWithTimeZoneType(3), parseTimeWithTimeZone(3, "12:34:56.789 -00:30")));

        TypedValue positiveHalfHour = JsonDateTimeTemplate.parse("HH24:MI:SS.FF3TZH:TZM").parseValue("12:34:56.789+00:30");
        assertThat(positiveHalfHour).isEqualTo(TypedValue.fromValueAsObject(createTimeWithTimeZoneType(3), parseTimeWithTimeZone(3, "12:34:56.789 +00:30")));
    }

    @Test
    public void testInvalidTemplates()
    {
        assertThatThrownBy(() -> JsonDateTimeTemplate.parse("YYYYRR"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("datetime() format template cannot contain both year and rounded year fields");

        assertThatThrownBy(() -> JsonDateTimeTemplate.parse("HH24A.M."))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("datetime() format template with HH24 cannot contain HH, HH12, A.M. or P.M.");

        assertThatThrownBy(() -> JsonDateTimeTemplate.parse("TZM"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("datetime() format template with TZM requires TZH");
    }

    @Test
    public void testQuotedLiteralTemplate()
    {
        // Double-quoted literal text in the template must match verbatim during parsing.
        TypedValue timestamp = JsonDateTimeTemplate.parse("YYYY-MM-DD\"T\"HH24:MI:SS.FF3")
                .parseValue("2024-01-02T12:34:56.789");
        assertThat(timestamp).isEqualTo(TypedValue.fromValueAsObject(createTimestampType(3), parseTimestamp(3, "2024-01-02 12:34:56.789")));

        // Mismatched literal text raises a diagnostic with position information.
        assertThatThrownBy(() -> JsonDateTimeTemplate.parse("YYYY-MM-DD\"T\"HH24")
                .parseValue("2024-01-02X12:34:56"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expected literal 'T'");
    }

    @Test
    public void testFF10To12Precision()
    {
        // FF10..FF12 map to TIMESTAMP precisions above 9, matching Trino's max precision of 12.
        assertThat(JsonDateTimeTemplate.parse("YYYY-MM-DD HH24:MI:SS.FF12").getType())
                .isEqualTo(createTimestampType(12));

        TypedValue ts = JsonDateTimeTemplate.parse("YYYY-MM-DD HH24:MI:SS.FF12")
                .parseValue("2024-01-02 12:34:56.123456789012");
        assertThat(ts).isEqualTo(TypedValue.fromValueAsObject(createTimestampType(12), parseTimestamp(12, "2024-01-02 12:34:56.123456789012")));
    }

    @Test
    public void testAmPmCaseInsensitive()
    {
        // A.M. / P.M. comparisons use case-insensitive matching.
        TypedValue lower = JsonDateTimeTemplate.parse("YYYY-MM-DD HH12:MI:SS a.m.")
                .parseValue("2024-01-02 10:11:12 p.m.");
        assertThat(lower).isEqualTo(TypedValue.fromValueAsObject(createTimestampType(0), parseTimestamp(0, "2024-01-02 22:11:12")));
    }

    @Test
    public void testTemplateIsCanonicalized()
    {
        // Field tokens are matched case-insensitively, so templates that differ only in their
        // case describe the same thing and must be indistinguishable to equals() and toString().
        assertThat(JsonDateTimeTemplate.parse("yyyy-mm-dd").getTemplate()).isEqualTo("YYYY-MM-DD");
        assertThat(JsonDateTimeTemplate.parse("yyyy-mm-dd")).isEqualTo(JsonDateTimeTemplate.parse("YYYY-MM-DD"));
        assertThat(JsonDateTimeTemplate.parse("yyyy-mm-dd")).hasSameHashCodeAs(JsonDateTimeTemplate.parse("YYYY-MM-DD"));

        assertThat(JsonDateTimeTemplate.parse("hh24:mi:ss.ff3 tzh:tzm").getTemplate()).isEqualTo("HH24:MI:SS.FF3 TZH:TZM");
        assertThat(JsonDateTimeTemplate.parse("yyyy-mm-dd hh12:mi:ss a.m.").getTemplate()).isEqualTo("YYYY-MM-DD HH12:MI:SS A.M.");

        // Literal text is matched case-sensitively, so it is preserved verbatim.
        assertThat(JsonDateTimeTemplate.parse("yyyy\"t\"mm").getTemplate()).isEqualTo("YYYY\"t\"MM");
        assertThat(JsonDateTimeTemplate.parse("yyyy\"t\"mm")).isNotEqualTo(JsonDateTimeTemplate.parse("yyyy\"T\"mm"));

        // The canonical form parses back to itself.
        JsonDateTimeTemplate canonical = JsonDateTimeTemplate.parse("yyyy\"t\"mm-dd");
        assertThat(JsonDateTimeTemplate.parse(canonical.getTemplate())).isEqualTo(canonical);
    }

    @Test
    public void testYearCompletion()
    {
        // Templates that specify only the low-order year digits take the remaining digits from a
        // fixed reference year of 1970, so the result does not depend on the current date.
        assertThat(JsonDateTimeTemplate.parse("Y-MM-DD").parseValue("4-01-02"))
                .isEqualTo(new TypedValue(DATE, (long) parseDate("1974-01-02")));
        assertThat(JsonDateTimeTemplate.parse("YY-MM-DD").parseValue("24-01-02"))
                .isEqualTo(new TypedValue(DATE, (long) parseDate("1924-01-02")));
        assertThat(JsonDateTimeTemplate.parse("YYY-MM-DD").parseValue("024-01-02"))
                .isEqualTo(new TypedValue(DATE, (long) parseDate("1024-01-02")));
        assertThat(JsonDateTimeTemplate.parse("YYYY-MM-DD").parseValue("0024-01-02"))
                .isEqualTo(new TypedValue(DATE, (long) parseDate("0024-01-02")));

        // RR rounds to the nearest century instead: with a reference year of 1970, values below 50
        // belong to the next century and values from 50 up to the current one.
        assertThat(JsonDateTimeTemplate.parse("RR-MM-DD").parseValue("24-01-02"))
                .isEqualTo(new TypedValue(DATE, (long) parseDate("2024-01-02")));
        assertThat(JsonDateTimeTemplate.parse("RR-MM-DD").parseValue("74-01-02"))
                .isEqualTo(new TypedValue(DATE, (long) parseDate("1974-01-02")));

        // RRRR is fully specified, so there is nothing to round.
        assertThat(JsonDateTimeTemplate.parse("RRRR-MM-DD").parseValue("2024-01-02"))
                .isEqualTo(new TypedValue(DATE, (long) parseDate("2024-01-02")));
    }

    @Test
    public void testEmptyTemplateRejected()
    {
        assertThatThrownBy(() -> JsonDateTimeTemplate.parse(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty");
    }

    @Test
    public void testInvalidCharacterCarriesPosition()
    {
        // Invalid template characters include their position in the diagnostic.
        assertThatThrownBy(() -> JsonDateTimeTemplate.parse("YYYY@MM"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("position 4");
    }

    @Test
    public void testSecondOfDayOutOfRange()
    {
        // SSSSS is bounds-checked at parse time so out-of-range values produce a clear error,
        // rather than the decomposed (floorDiv/floorMod) values silently building an invalid time.
        assertThatThrownBy(() -> JsonDateTimeTemplate.parse("SSSSS").parseValue("99999"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("second-of-day value out of range")
                .hasMessageContaining("99999");

        // Boundaries are inclusive at 0 / 86399.
        JsonDateTimeTemplate template = JsonDateTimeTemplate.parse("SSSSS");
        template.parseValue("0");
        template.parseValue("86399");
        assertThatThrownBy(() -> template.parseValue("86400"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("second-of-day value out of range");
    }

    @Test
    public void testHour12OutOfRange()
    {
        // HH12 is bounds-checked at parse time. `resolveHour` does `hour12 % 12`, so an
        // out-of-range value (e.g. 13 A.M., 25 P.M., 0 A.M.) would otherwise fold into a
        // valid-looking hour and never reach `parseTime`'s validator.
        JsonDateTimeTemplate template = JsonDateTimeTemplate.parse("HH12:MI:SS A.M.");
        template.parseValue("01:00:00 A.M.");
        template.parseValue("12:00:00 P.M.");
        assertThatThrownBy(() -> template.parseValue("00:00:00 A.M."))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("hour-of-half-day value out of range [1, 12]: 0");
        assertThatThrownBy(() -> template.parseValue("13:00:00 A.M."))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("hour-of-half-day value out of range [1, 12]: 13");
        assertThatThrownBy(() -> template.parseValue("25:00:00 P.M."))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("hour-of-half-day value out of range [1, 12]: 25");
    }

    @Test
    public void testTimeZoneMinuteOutOfRange()
    {
        // TZM is bounds-checked at parse time. `resolveOffset` does `offsetMinute * 60`,
        // so values like 99 carry into the hours (e.g. `+05:99` → `+06:39`) and would
        // never raise an error for clearly-invalid input.
        JsonDateTimeTemplate template = JsonDateTimeTemplate.parse("HH24:MI:SSTZH:TZM");
        template.parseValue("12:34:56+05:30");
        template.parseValue("12:34:56+05:59");
        assertThatThrownBy(() -> template.parseValue("12:34:56+05:60"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("time-zone-minute value out of range [0, 59]: 60");
        assertThatThrownBy(() -> template.parseValue("12:34:56+05:99"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("time-zone-minute value out of range [0, 59]: 99");
    }

    @Test
    public void testMonthAndDayOutOfRange()
    {
        // Unlike the decomposed fields above, MM / DD / DDD are validated by java.time when the
        // calendar date is assembled (LocalDate.of / LocalDate.ofYearDay), which rejects an
        // impossible date outright rather than folding it into a valid-looking one.
        JsonDateTimeTemplate date = JsonDateTimeTemplate.parse("YYYY-MM-DD");
        date.parseValue("2024-01-01");
        date.parseValue("2024-12-31");
        assertThatThrownBy(() -> date.parseValue("2024-13-01"))
                .isInstanceOf(DateTimeException.class);
        assertThatThrownBy(() -> date.parseValue("2024-00-01"))
                .isInstanceOf(DateTimeException.class);
        assertThatThrownBy(() -> date.parseValue("2024-01-32"))
                .isInstanceOf(DateTimeException.class);
        // 2023 is not a leap year, so February has 28 days.
        assertThatThrownBy(() -> date.parseValue("2023-02-29"))
                .isInstanceOf(DateTimeException.class);

        // Day of year is likewise validated against the (possibly leap) year.
        JsonDateTimeTemplate dayOfYear = JsonDateTimeTemplate.parse("YYYY-DDD");
        dayOfYear.parseValue("2024-366");
        assertThatThrownBy(() -> dayOfYear.parseValue("2023-366"))
                .isInstanceOf(DateTimeException.class);
        assertThatThrownBy(() -> dayOfYear.parseValue("2024-000"))
                .isInstanceOf(DateTimeException.class);
        assertThatThrownBy(() -> dayOfYear.parseValue("2024-367"))
                .isInstanceOf(DateTimeException.class);
    }

    @Test
    public void testDelimiterAdjacentToQuotedLiteral()
    {
        // A delimiter next to a quoted literal is unambiguous (the delimiter is one character;
        // the quoted text is verbatim) and must be accepted.
        TypedValue timestamp = JsonDateTimeTemplate.parse("YYYY-\"Q\"MM-DD")
                .parseValue("2024-Q01-02");
        assertThat(timestamp).isEqualTo(new TypedValue(DATE, (long) parseDate("2024-01-02")));

        // Two adjacent DELIMITER characters remain rejected — that combination is ambiguous.
        assertThatThrownBy(() -> JsonDateTimeTemplate.parse("YYYY--MM"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("consecutive delimiters");
    }
}
