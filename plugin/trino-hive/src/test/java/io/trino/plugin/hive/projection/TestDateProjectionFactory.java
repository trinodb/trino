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
package io.trino.plugin.hive.projection;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.plugin.hive.projection.PartitionProjectionProperties.COLUMN_PROJECTION_FORMAT;
import static io.trino.plugin.hive.projection.PartitionProjectionProperties.COLUMN_PROJECTION_INTERVAL;
import static io.trino.plugin.hive.projection.PartitionProjectionProperties.COLUMN_PROJECTION_INTERVAL_UNIT;
import static io.trino.plugin.hive.projection.PartitionProjectionProperties.COLUMN_PROJECTION_RANGE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.time.temporal.ChronoUnit.WEEKS;
import static java.time.temporal.ChronoUnit.YEARS;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class TestDateProjectionFactory
{
    @Test
    void testTypeSupport()
    {
        new DateProjection("test", VARCHAR, ImmutableMap.of(COLUMN_PROJECTION_FORMAT, "yyyy-MM-dd", COLUMN_PROJECTION_RANGE, ImmutableList.of("2020-01-01", "2020-01-03")));
        new DateProjection("test", DATE, ImmutableMap.of(COLUMN_PROJECTION_FORMAT, "yyyy-MM-dd", COLUMN_PROJECTION_RANGE, ImmutableList.of("2020-01-01", "2020-01-03")));
        new DateProjection("test", TIMESTAMP_SECONDS, ImmutableMap.of(COLUMN_PROJECTION_FORMAT, "yyyy-MM-dd", COLUMN_PROJECTION_RANGE, ImmutableList.of("2020-01-01", "2020-01-03")));
        new DateProjection("test", TIMESTAMP_MICROS, ImmutableMap.of(COLUMN_PROJECTION_FORMAT, "yyyy-MM-dd", COLUMN_PROJECTION_RANGE, ImmutableList.of("2020-01-01", "2020-01-03")));
        assertThatThrownBy(() -> new DateProjection("test", TIMESTAMP_NANOS, ImmutableMap.of(COLUMN_PROJECTION_FORMAT, "yyyy-MM-dd", COLUMN_PROJECTION_RANGE, ImmutableList.of("2020-01-01", "2020-01-03"))))
                .isInstanceOf(InvalidProjectionException.class)
                .hasMessage("Column projection for column 'test' failed. Unsupported column type: timestamp(9)");
        assertThatThrownBy(() -> new DateProjection("test", BIGINT, ImmutableMap.of(COLUMN_PROJECTION_FORMAT, "yyyy-MM-dd", COLUMN_PROJECTION_RANGE, ImmutableList.of("2020-01-01", "2020-01-03"))))
                .isInstanceOf(InvalidProjectionException.class)
                .hasMessage("Column projection for column 'test' failed. Unsupported column type: bigint");
    }

    @Test
    void testCreate()
    {
        Projection projection = new DateProjection("test", VARCHAR, ImmutableMap.of(COLUMN_PROJECTION_FORMAT, "yyyy-MM-dd", COLUMN_PROJECTION_RANGE, ImmutableList.of("2020-01-01", "2020-01-03")));
        assertThat(projection.getProjectedValues(Optional.empty())).containsExactly("2020-01-01", "2020-01-02", "2020-01-03");
        assertThat(projection.getProjectedValues(Optional.of(Domain.all(VARCHAR)))).containsExactly("2020-01-01", "2020-01-02", "2020-01-03");
        assertThat(projection.getProjectedValues(Optional.of(Domain.none(VARCHAR)))).isEmpty();
        assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(VARCHAR, Slices.utf8Slice("2020-01-02"))))).containsExactly("2020-01-02");
        assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(VARCHAR, Slices.utf8Slice("2222-01-01"))))).isEmpty();

        assertThatThrownBy(() -> new DateProjection("test", VARCHAR, ImmutableMap.of("ignored", ImmutableList.of("2020-01-01", "2020-01-02", "2020-01-03"))))
                .isInstanceOf(InvalidProjectionException.class)
                .hasMessage("Column projection for column 'test' failed. Missing required property: 'partition_projection_format'");
    }

    @ParameterizedTest
    @MethodSource("yearMonthFormats")
    void testYearMonthWithSupportedInterval(String pattern)
    {
        assertProjectedPeriods(pattern, LocalDate.of(2020, 1, 1), LocalDate.of(2020, 2, 1), LocalDate.of(2020, 3, 1), LocalDate.of(2020, 4, 1));
    }

    private static Stream<String> yearMonthFormats()
    {
        return Stream.of("yyyy-MM", "'abc'-yyyy-MM", "yyyy-MM-'abc'", "yyyyMM", "yyyy/MM", "MM-yyyy", "uuuu-MM", "yyyy-LL", "'m''d'-yyyy-MM");
    }

    @ParameterizedTest
    @MethodSource("yearFormats")
    void testYearWithSupportedInterval(String pattern)
    {
        // The range includes the leap year 2020
        assertProjectedPeriods(pattern, LocalDate.of(2019, 1, 1), LocalDate.of(2020, 1, 1), LocalDate.of(2021, 1, 1), LocalDate.of(2022, 1, 1));
    }

    private static Stream<String> yearFormats()
    {
        return Stream.of("yyyy", "'abc'-yyyy", "yyyy-'abc'", "uuuu", "'m''d'-yyyy");
    }

    @ParameterizedTest
    @MethodSource("yearMonthFormats")
    void testYearMonthWithUnalignedInterval(String pattern)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern, ENGLISH);
        String january = LocalDate.of(2020, 1, 1).format(formatter);
        String february = LocalDate.of(2020, 2, 1).format(formatter);
        String march = LocalDate.of(2020, 3, 1).format(formatter);
        for (ChronoUnit intervalUnit : ImmutableList.of(HOURS, MINUTES, SECONDS)) {
            Projection projection = new DateProjection("test", VARCHAR, ImmutableMap.of(
                    COLUMN_PROJECTION_FORMAT, pattern,
                    COLUMN_PROJECTION_RANGE, ImmutableList.of(january, march),
                    COLUMN_PROJECTION_INTERVAL, 7,
                    COLUMN_PROJECTION_INTERVAL_UNIT, intervalUnit));
            assertThat(projection.getProjectedValues(Optional.empty())).containsExactly(january, february);
            assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(VARCHAR, Slices.utf8Slice(february)))))
                    .containsExactly(february);
            assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(VARCHAR, Slices.utf8Slice(march))))).isEmpty();
        }
    }

    @ParameterizedTest
    @MethodSource("quarterFormats")
    void testQuarterWithSupportedInterval(String pattern)
    {
        // Start in the third quarter, as the first quarter would also match a value defaulted to January
        assertProjectedPeriods(pattern, LocalDate.of(2020, 7, 1), LocalDate.of(2020, 10, 1), LocalDate.of(2021, 1, 1), LocalDate.of(2020, 4, 1));
    }

    private static Stream<String> quarterFormats()
    {
        return Stream.of("yyyy-'Q'Q", "yyyy-QQQ", "QQQQ yyyy", "uuuu-'q'q");
    }

    private static void assertProjectedPeriods(String pattern, LocalDate first, LocalDate second, LocalDate last, LocalDate outsideRange)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern, ENGLISH);
        String firstValue = first.format(formatter);
        String secondValue = second.format(formatter);
        String lastValue = last.format(formatter);
        for (ChronoUnit intervalUnit : ImmutableList.of(DAYS, HOURS, MINUTES, SECONDS)) {
            Projection projection = new DateProjection("test", VARCHAR, ImmutableMap.of(
                    COLUMN_PROJECTION_FORMAT, pattern,
                    COLUMN_PROJECTION_RANGE, ImmutableList.of(firstValue, lastValue),
                    COLUMN_PROJECTION_INTERVAL_UNIT, intervalUnit));
            assertThat(projection.getProjectedValues(Optional.empty()))
                    .containsExactly(firstValue, secondValue, lastValue);
            assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(VARCHAR, Slices.utf8Slice(secondValue)))))
                    .containsExactly(secondValue);
            assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(VARCHAR, Slices.utf8Slice(outsideRange.format(formatter)))))).isEmpty();
            assertThat(projection.getProjectedValues(Optional.of(Domain.none(VARCHAR)))).isEmpty();
        }
    }

    @Test
    void testTimestampWithHoursInterval()
    {
        Projection projection = new DateProjection("test", TIMESTAMP_MICROS, ImmutableMap.of(
                COLUMN_PROJECTION_FORMAT, "yyyy-MM-dd",
                COLUMN_PROJECTION_RANGE, ImmutableList.of("2020-02-28", "2020-02-29"),
                COLUMN_PROJECTION_INTERVAL, 6,
                COLUMN_PROJECTION_INTERVAL_UNIT, HOURS));
        assertThat(projection.getProjectedValues(Optional.empty())).containsExactly(
                "2020-02-28 00:00:00",
                "2020-02-28 06:00:00",
                "2020-02-28 12:00:00",
                "2020-02-28 18:00:00",
                "2020-02-29 00:00:00");
    }

    @Test
    void testVarcharWithTimeFields()
    {
        Projection projection = new DateProjection("test", VARCHAR, ImmutableMap.of(
                COLUMN_PROJECTION_FORMAT, "yyyy-MM-dd HH:mm:ss",
                COLUMN_PROJECTION_RANGE, ImmutableList.of("2020-02-28 00:00:00", "2020-02-29 00:00:00"),
                COLUMN_PROJECTION_INTERVAL, 6,
                COLUMN_PROJECTION_INTERVAL_UNIT, HOURS));
        assertThat(projection.getProjectedValues(Optional.empty())).containsExactly(
                "2020-02-28 00:00:00",
                "2020-02-28 06:00:00",
                "2020-02-28 12:00:00",
                "2020-02-28 18:00:00",
                "2020-02-29 00:00:00");
    }

    @Test
    void testDateWithHoursInterval()
    {
        Projection projection = new DateProjection("test", VARCHAR, ImmutableMap.of(
                COLUMN_PROJECTION_FORMAT, "yyyy-MM-dd",
                COLUMN_PROJECTION_RANGE, ImmutableList.of("2020-01-01", "2020-01-03"),
                COLUMN_PROJECTION_INTERVAL_UNIT, HOURS));
        assertThat(projection.getProjectedValues(Optional.empty())).containsExactly("2020-01-01", "2020-01-02", "2020-01-03");
        assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(VARCHAR, Slices.utf8Slice("2020-01-02"))))).containsExactly("2020-01-02");
    }

    @Test
    @Timeout(value = 10, threadMode = SEPARATE_THREAD)
    void testDateWithSecondsInterval()
    {
        Projection projection = new DateProjection("test", VARCHAR, ImmutableMap.of(
                COLUMN_PROJECTION_FORMAT, "yyyy-MM-dd",
                COLUMN_PROJECTION_RANGE, ImmutableList.of("1900-01-01", "2100-01-01"),
                COLUMN_PROJECTION_INTERVAL_UNIT, SECONDS));
        assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(VARCHAR, Slices.utf8Slice("2050-01-01")))))
                .containsExactly("2050-01-01");
    }

    @ParameterizedTest
    @CsvSource({
            "yyyy-ww, 2020-01, 2021-01",
            "yyyy-MM-W, 2020-02-1, 2020-03-1",
            "yyyy-MM-EEEE, 2020-02-Saturday, 2020-03-Sunday",
    })
    void testIncompleteDateWithWeekOrWeekday(String pattern, String leftBound, String rightBound)
    {
        assertThatThrownBy(() -> new DateProjection("test", VARCHAR, ImmutableMap.of(
                COLUMN_PROJECTION_FORMAT, pattern,
                COLUMN_PROJECTION_RANGE, ImmutableList.of(leftBound, rightBound),
                COLUMN_PROJECTION_INTERVAL_UNIT, DAYS)))
                .isInstanceOf(InvalidProjectionException.class)
                .hasMessage("Column projection for column 'test' failed. Property: 'partition_projection_format' value '%s' is invalid: a format without a day must have year, quarter, or month precision", pattern);
    }

    @ParameterizedTest
    @MethodSource({"yearMonthFormats", "yearFormats"})
    void testYearOrMonthOnDateOrTimestampColumn(String pattern)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern, ENGLISH);
        List<String> dateRange = ImmutableList.of(LocalDate.of(2020, 1, 1).format(formatter), LocalDate.of(2021, 1, 1).format(formatter));
        List<String> nowRange = ImmutableList.of("NOW-1DAY", "NOW");
        for (List<String> range : ImmutableList.of(dateRange, nowRange)) {
            for (Type type : ImmutableList.of(DATE, TIMESTAMP_SECONDS, TIMESTAMP_MICROS)) {
                assertThatThrownBy(() -> new DateProjection("test", type, ImmutableMap.of(
                        COLUMN_PROJECTION_FORMAT, pattern,
                        COLUMN_PROJECTION_RANGE, range,
                        COLUMN_PROJECTION_INTERVAL_UNIT, DAYS)))
                        .isInstanceOf(InvalidProjectionException.class)
                        .hasMessage("Column projection for column 'test' failed. Date projection with year, quarter, or month precision is only supported for varchar columns");
            }
        }
    }

    @Test
    void testYearMonthWithLargerDaysInterval()
    {
        Projection projection = new DateProjection("test", VARCHAR, ImmutableMap.of(
                COLUMN_PROJECTION_FORMAT, "yyyy-MM",
                COLUMN_PROJECTION_RANGE, ImmutableList.of("2020-01", "2020-04"),
                COLUMN_PROJECTION_INTERVAL, 32,
                COLUMN_PROJECTION_INTERVAL_UNIT, DAYS));
        assertThat(projection.getProjectedValues(Optional.empty())).containsExactly("2020-01", "2020-02", "2020-03");
    }

    @ParameterizedTest
    @MethodSource({"yearMonthFormats", "yearFormats"})
    void testYearOrMonthWithoutIntervalUnit(String pattern)
    {
        assertThatThrownBy(() -> new DateProjection("test", VARCHAR, ImmutableMap.of(
                COLUMN_PROJECTION_FORMAT, pattern,
                COLUMN_PROJECTION_RANGE, ImmutableList.of("NOW-1DAY", "NOW"))))
                .isInstanceOf(InvalidProjectionException.class)
                .hasMessage("Column projection for column 'test' failed. Property: 'partition_projection_interval_unit' value 'Months' is invalid. Available options: [Days, Hours, Minutes, Seconds]");
    }

    @ParameterizedTest
    @MethodSource("yearOrMonthFormatsWithUnsupportedIntervalUnits")
    void testYearOrMonthWithUnsupportedIntervalUnit(String pattern, ChronoUnit intervalUnit)
    {
        assertThatThrownBy(() -> new DateProjection("test", VARCHAR, ImmutableMap.of(
                COLUMN_PROJECTION_FORMAT, pattern,
                COLUMN_PROJECTION_RANGE, ImmutableList.of("NOW-1DAY", "NOW"),
                COLUMN_PROJECTION_INTERVAL_UNIT, intervalUnit)))
                .isInstanceOf(InvalidProjectionException.class)
                .hasMessage("Column projection for column 'test' failed. Property: 'partition_projection_interval_unit' value '%s' is invalid. Available options: [Days, Hours, Minutes, Seconds]", intervalUnit);
    }

    private static Stream<Arguments> yearOrMonthFormatsWithUnsupportedIntervalUnits()
    {
        return Stream.concat(yearMonthFormats(), yearFormats())
                .flatMap(pattern -> Stream.of(MONTHS, YEARS, WEEKS).map(intervalUnit -> arguments(pattern, intervalUnit)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"yyyy-mm", "yyyy-MM HH:mm", "yyyy-MM mm"})
    void testIncompleteDateWithTimeFields(String pattern)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern, ENGLISH);
        assertThatThrownBy(() -> new DateProjection("test", VARCHAR, ImmutableMap.of(
                COLUMN_PROJECTION_FORMAT, pattern,
                COLUMN_PROJECTION_RANGE, ImmutableList.of(
                        LocalDate.of(2020, 1, 1).atStartOfDay().format(formatter),
                        LocalDate.of(2020, 3, 1).atStartOfDay().format(formatter)),
                COLUMN_PROJECTION_INTERVAL_UNIT, DAYS)))
                .isInstanceOf(InvalidProjectionException.class)
                .hasMessageStartingWith("Column projection for column 'test' failed. Property: 'partition_projection_range' needs to be a list of 2 valid dates formatted as '%s'", pattern)
                .hasMessageContaining("Unable to obtain LocalDateTime");
        assertThatThrownBy(() -> new DateProjection("test", VARCHAR, ImmutableMap.of(
                COLUMN_PROJECTION_FORMAT, pattern,
                COLUMN_PROJECTION_RANGE, ImmutableList.of("NOW-1DAY", "NOW"),
                COLUMN_PROJECTION_INTERVAL_UNIT, DAYS)))
                .isInstanceOf(InvalidProjectionException.class)
                .hasMessageStartingWith("Column projection for column 'test' failed. Property: 'partition_projection_format' value '%s' is invalid: Unable to obtain LocalDateTime", pattern);
    }

    @Test
    void testFormatWithZoneOffset()
    {
        assertThatThrownBy(() -> new DateProjection("test", VARCHAR, ImmutableMap.of(
                COLUMN_PROJECTION_FORMAT, "yyyy-MM-dd XXX",
                COLUMN_PROJECTION_RANGE, ImmutableList.of("2020-01-01 Z", "2020-01-03 Z"))))
                .isInstanceOf(InvalidProjectionException.class)
                .hasMessage("Column projection for column 'test' failed. Property: 'partition_projection_format' value 'yyyy-MM-dd XXX' is invalid: Unsupported field: OffsetSeconds");
    }
}
