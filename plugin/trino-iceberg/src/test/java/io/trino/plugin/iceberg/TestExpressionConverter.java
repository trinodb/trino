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
import io.trino.spi.type.Type;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static io.trino.plugin.iceberg.ColumnIdentity.primitiveColumnIdentity;
import static io.trino.plugin.iceberg.util.Timestamps.timestampFromNanos;
import static io.trino.plugin.iceberg.util.Timestamps.timestampToNanos;
import static io.trino.plugin.iceberg.util.Timestamps.timestampTzFromMicros;
import static io.trino.plugin.iceberg.util.Timestamps.timestampTzFromNanos;
import static io.trino.plugin.iceberg.util.Timestamps.timestampTzToNanos;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.Float.floatToRawIntBits;
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
    public void testFloatingPointMembership()
    {
        for (Type type : List.of(DOUBLE, REAL)) {
            Object one = type.equals(DOUBLE) ? (Object) 1.0 : (long) floatToRawIntBits(1.0f);
            Object nan = type.equals(DOUBLE) ? (Object) Double.NaN : (long) floatToRawIntBits(Float.NaN);
            ValueSet lessThanOne = ValueSet.ofRanges(Range.lessThan(type, one));
            ValueSet nanSet = ValueSet.of(type, nan);
            IcebergColumnHandle column = IcebergColumnHandle.optional(primitiveColumnIdentity(1, "x")).columnType(type).build();
            Schema schema = new Schema(Types.NestedField.optional(1, "x", type.equals(DOUBLE) ? Types.DoubleType.get() : Types.FloatType.get()));
            for (ValueSet values : List.of(
                    ValueSet.all(type),
                    ValueSet.none(type),
                    nanSet,
                    nanSet.complement(),
                    ValueSet.of(type, one),
                    ValueSet.of(type, one).complement(),
                    lessThanOne,
                    lessThanOne.union(nanSet),
                    lessThanOne.complement())) {
                for (boolean nullAllowed : List.of(false, true)) {
                    Domain domain = Domain.create(values, nullAllowed);
                    Evaluator evaluator = new Evaluator(schema.asStruct(), toIcebergExpression(column, domain), true);
                    for (Double value : Arrays.asList(null, Double.NEGATIVE_INFINITY, -1.0, -0.0, 0.0, 1.0, 2.0, Double.POSITIVE_INFINITY, Double.NaN, Double.longBitsToDouble(0x7FF0000000000001L))) {
                        GenericRecord row = GenericRecord.create(schema);
                        Object nativeValue = value;
                        Object icebergValue = value;
                        if (value != null && type.equals(REAL)) {
                            nativeValue = (long) floatToRawIntBits(value.floatValue());
                            icebergValue = value.floatValue();
                        }
                        row.setField("x", icebergValue);
                        assertThat(evaluator.eval(row)).as("%s contains %s", domain, nativeValue).isEqualTo(domain.includesNullableValue(nativeValue));
                    }
                }
            }
        }
    }

    @Test
    public void testFloatingPointZeroBoundaries()
    {
        for (Type type : List.of(DOUBLE, REAL)) {
            IcebergColumnHandle column = IcebergColumnHandle.optional(primitiveColumnIdentity(1, "x")).columnType(type).build();
            org.apache.iceberg.types.Type icebergType = type.equals(DOUBLE) ? Types.DoubleType.get() : Types.FloatType.get();
            Schema schema = new Schema(Types.NestedField.optional(1, "x", icebergType));
            Object nan = type.equals(DOUBLE) ? (Object) Double.NaN : (long) floatToRawIntBits(Float.NaN);
            Object one = type.equals(DOUBLE) ? (Object) 1.0 : (long) floatToRawIntBits(1.0f);
            for (double zero : List.of(-0.0, 0.0)) {
                Object bound = type.equals(DOUBLE) ? (Object) zero : (long) floatToRawIntBits((float) zero);
                for (ValueSet values : List.of(
                        ValueSet.of(type, bound),
                        ValueSet.of(type, bound, one),
                        ValueSet.ofRanges(Range.lessThan(type, bound)),
                        ValueSet.ofRanges(Range.lessThanOrEqual(type, bound)),
                        ValueSet.ofRanges(Range.greaterThan(type, bound)),
                        ValueSet.ofRanges(Range.greaterThanOrEqual(type, bound)))) {
                    for (ValueSet membership : List.of(values, values.complement(), values.union(ValueSet.of(type, nan)))) {
                        for (boolean nullAllowed : List.of(false, true)) {
                            Domain domain = Domain.create(membership, nullAllowed);
                            Expression expression = toIcebergExpression(column, domain);
                            Evaluator evaluator = new Evaluator(schema.asStruct(), expression, true);
                            InclusiveMetricsEvaluator metricsEvaluator = new InclusiveMetricsEvaluator(schema, expression);
                            for (Double value : Arrays.asList(null, -1.0, -0.0, 0.0, 1.0, Double.NaN)) {
                                Object nativeValue = value;
                                Object icebergValue = value;
                                if (value != null && type.equals(REAL)) {
                                    nativeValue = (long) floatToRawIntBits(value.floatValue());
                                    icebergValue = value.floatValue();
                                }
                                GenericRecord row = GenericRecord.create(schema);
                                row.setField("x", icebergValue);
                                boolean matches = domain.includesNullableValue(nativeValue);
                                assertThat(evaluator.eval(row)).as("%s contains %s", domain, nativeValue).isEqualTo(matches);
                                if (value != null && !value.isNaN()) {
                                    DataFile file = DataFiles.builder(PartitionSpec.unpartitioned())
                                            .withPath("test.parquet")
                                            .withFileSizeInBytes(100)
                                            .withMetrics(new Metrics(
                                                    1L,
                                                    ImmutableMap.of(),
                                                    ImmutableMap.of(1, 1L),
                                                    ImmutableMap.of(1, 0L),
                                                    ImmutableMap.of(1, 0L),
                                                    ImmutableMap.of(1, Conversions.toByteBuffer(icebergType, icebergValue)),
                                                    ImmutableMap.of(1, Conversions.toByteBuffer(icebergType, icebergValue))))
                                            .build();
                                    if (matches) {
                                        assertThat(metricsEvaluator.eval(file)).as("%s retains file containing %s", domain, nativeValue).isTrue();
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

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
