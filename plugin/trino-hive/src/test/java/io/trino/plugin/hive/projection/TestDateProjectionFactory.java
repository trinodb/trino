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
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
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
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern, ENGLISH);
        String january = LocalDate.of(2020, 1, 1).format(formatter);
        String february = LocalDate.of(2020, 2, 1).format(formatter);
        String march = LocalDate.of(2020, 3, 1).format(formatter);
        String april = LocalDate.of(2020, 4, 1).format(formatter);
        for (ChronoUnit intervalUnit : ImmutableList.of(DAYS, HOURS, MINUTES, SECONDS)) {
            Projection projection = new DateProjection("test", VARCHAR, ImmutableMap.of(
                    COLUMN_PROJECTION_FORMAT, pattern,
                    COLUMN_PROJECTION_RANGE, ImmutableList.of(january, march),
                    COLUMN_PROJECTION_INTERVAL_UNIT, intervalUnit));
            assertThat(projection.getProjectedValues(Optional.empty()))
                    .containsExactly(january, february, march);
            assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(VARCHAR, Slices.utf8Slice(february)))))
                    .containsExactly(february);
            assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(VARCHAR, Slices.utf8Slice(april))))).isEmpty();
            assertThat(projection.getProjectedValues(Optional.of(Domain.none(VARCHAR)))).isEmpty();
        }
    }

    private static Stream<String> yearMonthFormats()
    {
        return Stream.of("yyyy-MM", "'abc'-yyyy-MM", "yyyy-MM-'abc'", "yyyyMM", "yyyy/MM", "MM-yyyy", "uuuu-MM", "yyyy-LL", "'m''d'-yyyy-MM");
    }

    @ParameterizedTest
    @MethodSource("yearFormats")
    void testYearWithSupportedInterval(String pattern)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern, ENGLISH);
        String firstYear = LocalDate.of(2019, 1, 1).format(formatter);
        String leapYear = LocalDate.of(2020, 1, 1).format(formatter);
        String lastYear = LocalDate.of(2021, 1, 1).format(formatter);
        String outsideRange = LocalDate.of(2022, 1, 1).format(formatter);
        for (ChronoUnit intervalUnit : ImmutableList.of(DAYS, HOURS, MINUTES, SECONDS)) {
            Projection projection = new DateProjection("test", VARCHAR, ImmutableMap.of(
                    COLUMN_PROJECTION_FORMAT, pattern,
                    COLUMN_PROJECTION_RANGE, ImmutableList.of(firstYear, lastYear),
                    COLUMN_PROJECTION_INTERVAL_UNIT, intervalUnit));
            assertThat(projection.getProjectedValues(Optional.empty()))
                    .containsExactly(firstYear, leapYear, lastYear);
            assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(VARCHAR, Slices.utf8Slice(leapYear)))))
                    .containsExactly(leapYear);
            assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(VARCHAR, Slices.utf8Slice(outsideRange))))).isEmpty();
            assertThat(projection.getProjectedValues(Optional.of(Domain.none(VARCHAR)))).isEmpty();
        }
    }

    private static Stream<String> yearFormats()
    {
        return Stream.of("yyyy", "'abc'-yyyy", "yyyy-'abc'", "uuuu", "'m''d'-yyyy");
    }

    @Test
    @Timeout(value = 10, threadMode = SEPARATE_THREAD)
    void testYearWithSecondsInterval()
    {
        Projection projection = new DateProjection("test", VARCHAR, ImmutableMap.of(
                COLUMN_PROJECTION_FORMAT, "yyyy",
                COLUMN_PROJECTION_RANGE, ImmutableList.of("1900", "2100"),
                COLUMN_PROJECTION_INTERVAL_UNIT, SECONDS));
        assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(VARCHAR, Slices.utf8Slice("2050")))))
                .containsExactly("2050");
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

    @Test
    void testQuarterWithSecondsInterval()
    {
        Projection projection = new DateProjection("test", VARCHAR, ImmutableMap.of(
                COLUMN_PROJECTION_FORMAT, "yyyy-'Q'Q",
                COLUMN_PROJECTION_RANGE, ImmutableList.of("2020-Q1", "2021-Q1"),
                COLUMN_PROJECTION_INTERVAL_UNIT, SECONDS));
        assertThat(projection.getProjectedValues(Optional.empty())).containsExactly("2020-Q1", "2020-Q2", "2020-Q3", "2020-Q4", "2021-Q1");
    }

    @Test
    void testWeekdayWithSecondsInterval()
    {
        Projection projection = new DateProjection("test", VARCHAR, ImmutableMap.of(
                COLUMN_PROJECTION_FORMAT, "yyyy-MM-EEEE",
                COLUMN_PROJECTION_RANGE, ImmutableList.of("2020-02-Saturday", "2020-03-Sunday"),
                COLUMN_PROJECTION_INTERVAL_UNIT, SECONDS));
        assertThat(projection.getProjectedValues(Optional.empty())).containsExactly(
                "2020-02-Saturday",
                "2020-02-Sunday",
                "2020-02-Monday",
                "2020-02-Tuesday",
                "2020-02-Wednesday",
                "2020-02-Thursday",
                "2020-02-Friday",
                "2020-03-Sunday");
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
    void testIncompleteDateWithQuarter()
    {
        assertThatThrownBy(() -> new DateProjection("test", VARCHAR, ImmutableMap.of(
                COLUMN_PROJECTION_FORMAT, "yyyy-'Q'Q",
                COLUMN_PROJECTION_RANGE, ImmutableList.of("2020-Q4", "2021-Q1"),
                COLUMN_PROJECTION_INTERVAL_UNIT, DAYS)))
                .isInstanceOf(InvalidProjectionException.class)
                .hasMessageContaining("Conflict found: Field QuarterOfYear");
    }

    @ParameterizedTest
    @MethodSource({"yearMonthFormats", "yearFormats"})
    void testYearOrMonthOnDateOrTimestampColumn(String pattern)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern, ENGLISH);
        for (Type type : ImmutableList.of(DATE, TIMESTAMP_SECONDS, TIMESTAMP_MICROS)) {
            assertThatThrownBy(() -> new DateProjection("test", type, ImmutableMap.of(
                    COLUMN_PROJECTION_FORMAT, pattern,
                    COLUMN_PROJECTION_RANGE, ImmutableList.of(
                            LocalDate.of(2020, 1, 1).format(formatter),
                            LocalDate.of(2021, 1, 1).format(formatter)),
                    COLUMN_PROJECTION_INTERVAL_UNIT, DAYS)))
                    .isInstanceOf(InvalidProjectionException.class)
                    .hasMessage("Column projection for column 'test' failed. Date projection with year or month precision is only supported for varchar columns");
        }
    }

    @ParameterizedTest
    @MethodSource({"yearMonthFormats", "yearFormats"})
    void testYearOrMonthWithNowOnDateOrTimestampColumn(String pattern)
    {
        for (Type type : ImmutableList.of(DATE, TIMESTAMP_SECONDS, TIMESTAMP_MICROS)) {
            Projection projection = new DateProjection("test", type, ImmutableMap.of(
                    COLUMN_PROJECTION_FORMAT, pattern,
                    COLUMN_PROJECTION_RANGE, ImmutableList.of("NOW-1DAY", "NOW"),
                    COLUMN_PROJECTION_INTERVAL_UNIT, DAYS));
            assertThatThrownBy(() -> projection.getProjectedValues(Optional.empty()))
                    .isInstanceOf(InvalidProjectionException.class)
                    .hasMessage("Column projection for column 'test' failed. Date projection with year or month precision is only supported for varchar columns");
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
    @MethodSource({"yearMonthFormats", "yearFormats"})
    void testYearOrMonthWithUnsupportedIntervalUnit(String pattern)
    {
        for (ChronoUnit intervalUnit : ImmutableList.of(MONTHS, YEARS, WEEKS)) {
            assertThatThrownBy(() -> new DateProjection("test", VARCHAR, ImmutableMap.of(
                    COLUMN_PROJECTION_FORMAT, pattern,
                    COLUMN_PROJECTION_RANGE, ImmutableList.of("NOW-1DAY", "NOW"),
                    COLUMN_PROJECTION_INTERVAL_UNIT, intervalUnit)))
                    .isInstanceOf(InvalidProjectionException.class)
                    .hasMessage("Column projection for column 'test' failed. Property: 'partition_projection_interval_unit' value '%s' is invalid. Available options: [Days, Hours, Minutes, Seconds]", intervalUnit);
        }
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
                .isInstanceOf(DateTimeException.class);
    }
}
