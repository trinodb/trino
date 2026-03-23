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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.iceberg.ColumnIdentity.primitiveColumnIdentity;
import static io.trino.plugin.iceberg.util.Timestamps.timestampFromNanos;
import static io.trino.plugin.iceberg.util.Timestamps.timestampToNanos;
import static io.trino.plugin.iceberg.util.Timestamps.timestampTzFromMicros;
import static io.trino.plugin.iceberg.util.Timestamps.timestampTzFromNanos;
import static io.trino.plugin.iceberg.util.Timestamps.timestampTzToNanos;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static org.apache.iceberg.expressions.Expression.Operation.GT_EQ;
import static org.apache.iceberg.expressions.Expression.Operation.LT_EQ;
import static org.apache.iceberg.expressions.Expressions.alwaysFalse;
import static org.assertj.core.api.Assertions.assertThat;

public class TestExpressionConverter
{
    private static final long MIN_NANO_EPOCH_MICROS = floorDiv(Long.MIN_VALUE, NANOSECONDS_PER_MICROSECOND);
    private static final int MIN_NANO_OF_MICRO = (int) floorMod(Long.MIN_VALUE, NANOSECONDS_PER_MICROSECOND);
    private static final long MAX_NANO_EPOCH_MICROS = floorDiv(Long.MAX_VALUE, NANOSECONDS_PER_MICROSECOND);
    private static final int MAX_NANO_OF_MICRO = (int) floorMod(Long.MAX_VALUE, NANOSECONDS_PER_MICROSECOND);

    private static final long MIN_NANO_EPOCH_MILLIS = floorDiv(Long.MIN_VALUE, NANOSECONDS_PER_MILLISECOND);
    private static final int MIN_NANO_OF_MILLI = (int) floorMod(Long.MIN_VALUE, NANOSECONDS_PER_MILLISECOND);
    private static final long MAX_NANO_EPOCH_MILLIS = floorDiv(Long.MAX_VALUE, NANOSECONDS_PER_MILLISECOND);
    private static final int MAX_NANO_OF_MILLI = (int) floorMod(Long.MAX_VALUE, NANOSECONDS_PER_MILLISECOND);

    private static final IcebergColumnHandle TIMESTAMP_NANOS_COLUMN = IcebergColumnHandle.optional(primitiveColumnIdentity(1, "ts_nano"))
            .columnType(TIMESTAMP_NANOS)
            .build();
    private static final IcebergColumnHandle TIMESTAMP_TZ_NANOS_COLUMN = IcebergColumnHandle.optional(primitiveColumnIdentity(2, "ts_tz_nano"))
            .columnType(TIMESTAMP_TZ_NANOS)
            .build();

    @Test
    public void testTimestampNanosOutOfRangeSingleValuesAreAlwaysFalse()
    {
        assertThat(toIcebergExpression(TIMESTAMP_NANOS_COLUMN, Domain.singleValue(TIMESTAMP_NANOS, new LongTimestamp(MIN_NANO_EPOCH_MICROS - 1, 0))))
                .isSameAs(alwaysFalse());
        assertThat(toIcebergExpression(TIMESTAMP_NANOS_COLUMN, Domain.singleValue(TIMESTAMP_NANOS, new LongTimestamp(MIN_NANO_EPOCH_MICROS, (MIN_NANO_OF_MICRO - 1) * PICOSECONDS_PER_NANOSECOND))))
                .isSameAs(alwaysFalse());
        assertThat(toIcebergExpression(TIMESTAMP_NANOS_COLUMN, Domain.singleValue(TIMESTAMP_NANOS, new LongTimestamp(MAX_NANO_EPOCH_MICROS, (MAX_NANO_OF_MICRO + 1) * PICOSECONDS_PER_NANOSECOND))))
                .isSameAs(alwaysFalse());
        assertThat(toIcebergExpression(TIMESTAMP_NANOS_COLUMN, Domain.singleValue(TIMESTAMP_NANOS, new LongTimestamp(MAX_NANO_EPOCH_MICROS + 1, 0))))
                .isSameAs(alwaysFalse());
    }

    @Test
    public void testTimestampNanosExactBoundaryValuesAreInRange()
    {
        LongTimestamp minValue = timestampFromNanos(Long.MIN_VALUE);
        assertThat(toIcebergExpression(TIMESTAMP_NANOS_COLUMN, Domain.singleValue(TIMESTAMP_NANOS, minValue)))
                .hasToString(singleValueExpression("ts_nano", minValue));

        LongTimestamp maxValue = timestampFromNanos(Long.MAX_VALUE);
        assertThat(toIcebergExpression(TIMESTAMP_NANOS_COLUMN, Domain.singleValue(TIMESTAMP_NANOS, maxValue)))
                .hasToString(singleValueExpression("ts_nano", maxValue));
    }

    @Test
    public void testTimestampNanosOutOfRangeBoundsAreClipped()
    {
        LongTimestamp upperBound = new LongTimestamp(123_456_789L, 987_000);
        assertThat(toIcebergExpression(
                TIMESTAMP_NANOS_COLUMN,
                Domain.create(ValueSet.ofRanges(Range.range(TIMESTAMP_NANOS, new LongTimestamp(MIN_NANO_EPOCH_MICROS - 1, 0), true, upperBound, true)), false)))
                .hasToString(predicate(LT_EQ, "ts_nano", upperBound));

        LongTimestamp lowerBound = new LongTimestamp(123_456_789L, 654_000);
        assertThat(toIcebergExpression(
                TIMESTAMP_NANOS_COLUMN,
                Domain.create(ValueSet.ofRanges(Range.range(TIMESTAMP_NANOS, lowerBound, true, new LongTimestamp(MAX_NANO_EPOCH_MICROS + 1, 0), true)), false)))
                .hasToString(predicate(GT_EQ, "ts_nano", lowerBound));
    }

    @Test
    public void testTimestampTzNanosOutOfRangeSingleValuesAreAlwaysFalse()
    {
        assertThat(toIcebergExpression(TIMESTAMP_TZ_NANOS_COLUMN, Domain.singleValue(TIMESTAMP_TZ_NANOS, timestampTzFromEpochMicros(MIN_NANO_EPOCH_MICROS - 1))))
                .isSameAs(alwaysFalse());
        assertThat(toIcebergExpression(TIMESTAMP_TZ_NANOS_COLUMN, Domain.singleValue(TIMESTAMP_TZ_NANOS, LongTimestampWithTimeZone.fromEpochMillisAndFraction(MIN_NANO_EPOCH_MILLIS, (MIN_NANO_OF_MILLI - 1) * PICOSECONDS_PER_NANOSECOND, UTC_KEY))))
                .isSameAs(alwaysFalse());
        assertThat(toIcebergExpression(TIMESTAMP_TZ_NANOS_COLUMN, Domain.singleValue(TIMESTAMP_TZ_NANOS, LongTimestampWithTimeZone.fromEpochMillisAndFraction(MAX_NANO_EPOCH_MILLIS, (MAX_NANO_OF_MILLI + 1) * PICOSECONDS_PER_NANOSECOND, UTC_KEY))))
                .isSameAs(alwaysFalse());
        assertThat(toIcebergExpression(TIMESTAMP_TZ_NANOS_COLUMN, Domain.singleValue(TIMESTAMP_TZ_NANOS, timestampTzFromEpochMicros(MAX_NANO_EPOCH_MICROS + 1))))
                .isSameAs(alwaysFalse());
    }

    @Test
    public void testTimestampTzNanosExactBoundaryValuesAreInRange()
    {
        LongTimestampWithTimeZone minValue = timestampTzFromNanos(Long.MIN_VALUE);
        assertThat(toIcebergExpression(TIMESTAMP_TZ_NANOS_COLUMN, Domain.singleValue(TIMESTAMP_TZ_NANOS, minValue)))
                .hasToString(singleValueExpression("ts_tz_nano", minValue));

        LongTimestampWithTimeZone maxValue = timestampTzFromNanos(Long.MAX_VALUE);
        assertThat(toIcebergExpression(TIMESTAMP_TZ_NANOS_COLUMN, Domain.singleValue(TIMESTAMP_TZ_NANOS, maxValue)))
                .hasToString(singleValueExpression("ts_tz_nano", maxValue));
    }

    @Test
    public void testTimestampTzNanosOutOfRangeBoundsAreClipped()
    {
        LongTimestampWithTimeZone upperBound = LongTimestampWithTimeZone.fromEpochMillisAndFraction(123_456L, 789_000, UTC_KEY);
        assertThat(toIcebergExpression(
                TIMESTAMP_TZ_NANOS_COLUMN,
                Domain.create(ValueSet.ofRanges(Range.range(TIMESTAMP_TZ_NANOS, timestampTzFromEpochMicros(MIN_NANO_EPOCH_MICROS - 1), true, upperBound, true)), false)))
                .hasToString(predicate(LT_EQ, "ts_tz_nano", upperBound));

        LongTimestampWithTimeZone lowerBound = LongTimestampWithTimeZone.fromEpochMillisAndFraction(123_456L, 654_000, UTC_KEY);
        assertThat(toIcebergExpression(
                TIMESTAMP_TZ_NANOS_COLUMN,
                Domain.create(ValueSet.ofRanges(Range.range(TIMESTAMP_TZ_NANOS, lowerBound, true, timestampTzFromEpochMicros(MAX_NANO_EPOCH_MICROS + 1), true)), false)))
                .hasToString(predicate(GT_EQ, "ts_tz_nano", lowerBound));
    }

    private static Expression toIcebergExpression(IcebergColumnHandle columnHandle, Domain domain)
    {
        return ExpressionConverter.toIcebergExpression(TupleDomain.withColumnDomains(ImmutableMap.of(columnHandle, domain)));
    }

    private static LongTimestampWithTimeZone timestampTzFromEpochMicros(long epochMicros)
    {
        return timestampTzFromMicros(epochMicros);
    }

    private static String predicate(Expression.Operation operation, String columnName, LongTimestamp timestamp)
    {
        return Expressions.predicate(operation, columnName, Expressions.nanos(timestampToNanos(timestamp))).toString();
    }

    private static String predicate(Expression.Operation operation, String columnName, LongTimestampWithTimeZone timestamp)
    {
        return Expressions.predicate(operation, columnName, Expressions.nanos(timestampTzToNanos(timestamp))).toString();
    }

    private static String singleValueExpression(String columnName, LongTimestamp timestamp)
    {
        return Expressions.in(columnName, ImmutableList.of(timestampToNanos(timestamp))).toString();
    }

    private static String singleValueExpression(String columnName, LongTimestampWithTimeZone timestamp)
    {
        return Expressions.in(columnName, ImmutableList.of(timestampTzToNanos(timestamp))).toString();
    }
}
