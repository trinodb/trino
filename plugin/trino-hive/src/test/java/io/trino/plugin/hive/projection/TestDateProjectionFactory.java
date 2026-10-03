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
import org.junit.jupiter.api.Test;

import java.util.Optional;

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
import static java.time.temporal.ChronoUnit.WEEKS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    void testYearFormat()
    {
        Projection projection = new DateProjection("test", VARCHAR, ImmutableMap.of(COLUMN_PROJECTION_FORMAT, "yyyy", COLUMN_PROJECTION_RANGE, ImmutableList.of("2020", "2023")));
        assertThat(projection.getProjectedValues(Optional.empty())).containsExactly("2020", "2021", "2022", "2023");
        assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(VARCHAR, Slices.utf8Slice("2021"))))).containsExactly("2021");
        assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(VARCHAR, Slices.utf8Slice("2019"))))).isEmpty();
    }

    @Test
    void testMonthFormat()
    {
        Projection projection = new DateProjection("test", VARCHAR, ImmutableMap.of(COLUMN_PROJECTION_FORMAT, "yyyy-MM", COLUMN_PROJECTION_RANGE, ImmutableList.of("2022-01", "2022-03")));
        assertThat(projection.getProjectedValues(Optional.empty())).containsExactly("2022-01", "2022-02", "2022-03");
        assertThat(projection.getProjectedValues(Optional.of(Domain.all(VARCHAR)))).containsExactly("2022-01", "2022-02", "2022-03");
        assertThat(projection.getProjectedValues(Optional.of(Domain.none(VARCHAR)))).isEmpty();
        assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(VARCHAR, Slices.utf8Slice("2022-02"))))).containsExactly("2022-02");
        assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(VARCHAR, Slices.utf8Slice("2023-01"))))).isEmpty();
    }

    @Test
    void testWeekInterval()
    {
        Projection projection = new DateProjection("test", VARCHAR, ImmutableMap.of(
                COLUMN_PROJECTION_FORMAT, "yyyy-MM-dd",
                COLUMN_PROJECTION_RANGE, ImmutableList.of("2020-01-01", "2020-01-22"),
                COLUMN_PROJECTION_INTERVAL, 1,
                COLUMN_PROJECTION_INTERVAL_UNIT, WEEKS));
        assertThat(projection.getProjectedValues(Optional.empty())).containsExactly("2020-01-01", "2020-01-08", "2020-01-15", "2020-01-22");
        assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(VARCHAR, Slices.utf8Slice("2020-01-08"))))).containsExactly("2020-01-08");
        assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(VARCHAR, Slices.utf8Slice("2020-01-09"))))).isEmpty();
    }

    @Test
    void testMissingIntervalUnitForSubDayFormat()
    {
        assertThatThrownBy(() -> new DateProjection("test", VARCHAR, ImmutableMap.of(
                COLUMN_PROJECTION_FORMAT, "yyyy-MM-dd HH",
                COLUMN_PROJECTION_RANGE, ImmutableList.of("2020-01-01 00", "2020-01-02 00"))))
                .isInstanceOf(InvalidProjectionException.class)
                .hasMessage("Column projection for column 'test' failed. Property: 'partition_projection_interval_unit' " +
                        "needs to be set when provided 'partition_projection_format' is less than single-day precision. " +
                        "Interval defaults to 1 day, 1 month or 1 year, respectively. Otherwise, interval is required");
    }

    @Test
    void testNowRelativeRangeWithEstimatedUnits()
    {
        // Regression: NOW arithmetic must support estimated units (WEEKS, MONTHS, YEARS) that Instant.plus rejects
        assertThat(new DateProjection("test", VARCHAR, ImmutableMap.of(
                COLUMN_PROJECTION_FORMAT, "yyyy-MM",
                COLUMN_PROJECTION_RANGE, ImmutableList.of("NOW-2MONTHS", "NOW")))
                .getProjectedValues(Optional.empty())).hasSize(3);

        assertThat(new DateProjection("test", VARCHAR, ImmutableMap.of(
                COLUMN_PROJECTION_FORMAT, "yyyy",
                COLUMN_PROJECTION_RANGE, ImmutableList.of("NOW-2YEARS", "NOW")))
                .getProjectedValues(Optional.empty())).hasSize(3);
    }
}
