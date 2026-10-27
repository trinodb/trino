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
package io.trino.operator.scalar.timestamp;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDateTime;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalAdjusters;

import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTimestampExtraction
{
    @ParameterizedTest
    @ValueSource(longs = {Long.MIN_VALUE, Long.MAX_VALUE, -86_400_000_001L, -86_400_000_000L, -1_000_001, -1_000_000, -1_001, -1_000, -999, -1, 0, 1, 999, 1_000, 1_001, 1_000_000, 86_400_000_000L})
    public void testExtraction(long epochMicros)
    {
        LocalDateTime expected = LocalDateTime.ofEpochSecond(
                floorDiv(epochMicros, MICROSECONDS_PER_SECOND),
                floorMod(epochMicros, MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND,
                UTC);

        assertThat(ExtractYear.extract(epochMicros)).isEqualTo(expected.getYear());
        assertThat(ExtractMonth.extract(epochMicros)).isEqualTo(expected.getMonthValue());
        assertThat(ExtractDay.extract(epochMicros)).isEqualTo(expected.getDayOfMonth());
        assertThat(ExtractDayOfYear.extract(epochMicros)).isEqualTo(expected.getDayOfYear());
        assertThat(ExtractDayOfWeek.extract(epochMicros)).isEqualTo(expected.getDayOfWeek().getValue());
        assertThat(ExtractWeekOfYear.extract(epochMicros)).isEqualTo(expected.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR));
        assertThat(ExtractYearOfWeek.extract(epochMicros)).isEqualTo(expected.get(IsoFields.WEEK_BASED_YEAR));
        assertThat(ExtractQuarter.extract(epochMicros)).isEqualTo(expected.get(IsoFields.QUARTER_OF_YEAR));
        assertThat(ExtractHour.extract(epochMicros)).isEqualTo(expected.getHour());
        assertThat(ExtractMinute.extract(epochMicros)).isEqualTo(expected.getMinute());
        assertThat(ExtractSecond.extract(epochMicros)).isEqualTo(expected.getSecond());
        assertThat(ExtractMillisecond.extract(epochMicros)).isEqualTo(expected.getNano() / 1_000_000);
        assertThat(LastDayOfMonth.lastDayOfMonth(epochMicros))
                .isEqualTo(expected.toLocalDate().with(TemporalAdjusters.lastDayOfMonth()).toEpochDay());
    }
}
