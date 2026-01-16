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

import com.google.common.base.VerifyException;
import com.google.common.math.LongMath;
import io.airlift.slice.Slice;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.hive.util.HiveUtil.isStructuralType;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.isMetadataColumnId;
import static io.trino.plugin.iceberg.util.Timestamps.timestampTzToMicros;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.math.RoundingMode.UNNECESSARY;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.expressions.Expressions.alwaysFalse;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;

public final class ExpressionConverter
{
    // Nano timestamp range: Long.MIN_VALUE/MAX_VALUE nanos converted to micros
    private static final long MIN_NANO_EPOCH_MICROS = Long.MIN_VALUE / NANOSECONDS_PER_MICROSECOND;
    private static final long MAX_NANO_EPOCH_MICROS = Long.MAX_VALUE / NANOSECONDS_PER_MICROSECOND;

    private ExpressionConverter() {}

    public static boolean isConvertibleToIcebergExpression(Domain domain)
    {
        if (isStructuralType(domain.getType())) {
            // structural types cannot be used to filter a table scan in Iceberg library.
            return false;
        }

        if (domain.getType() == UUID) {
            // Iceberg orders UUID values differently than Trino (perhaps due to https://bugs.openjdk.org/browse/JDK-7025832), so allow only IS NULL / IS NOT NULL checks
            return domain.isOnlyNull() || domain.getValues().isAll();
        }

        return true;
    }

    public static Expression toIcebergExpression(TupleDomain<IcebergColumnHandle> tupleDomain)
    {
        if (tupleDomain.isAll()) {
            return alwaysTrue();
        }
        if (tupleDomain.isNone()) {
            return alwaysFalse();
        }
        Map<IcebergColumnHandle, Domain> domainMap = tupleDomain.getDomains().get();
        List<Expression> conjuncts = new ArrayList<>();
        for (Map.Entry<IcebergColumnHandle, Domain> entry : domainMap.entrySet()) {
            IcebergColumnHandle columnHandle = entry.getKey();
            checkArgument(!isMetadataColumnId(columnHandle.getId()), "Constraint on an unexpected column %s", columnHandle);
            Domain domain = entry.getValue();
            checkArgument(isConvertibleToIcebergExpression(domain), "Unexpected not convertible domain on column %s: %s", columnHandle, domain);
            conjuncts.add(toIcebergExpression(columnHandle.getQualifiedName(), columnHandle.getType(), domain));
        }
        return and(conjuncts);
    }

    private static Expression toIcebergExpression(String columnName, Type type, Domain domain)
    {
        if (domain.isAll()) {
            return alwaysTrue();
        }
        if (domain.getValues().isNone()) {
            return domain.isNullAllowed() ? isNull(columnName) : alwaysFalse();
        }

        if (domain.getValues().isAll()) {
            return domain.isNullAllowed() ? alwaysTrue() : not(isNull(columnName));
        }

        if (type instanceof ArrayType || type instanceof MapType || type instanceof RowType) {
            // Fail fast. Ignoring expression could lead to data loss in case of deletions.
            throw new UnsupportedOperationException("Unsupported type for expression: " + type);
        }

        if (type.isOrderable()) {
            List<Range> orderedRanges = domain.getValues().getRanges().getOrderedRanges();
            List<Object> icebergValues = new ArrayList<>();
            List<Expression> rangeExpressions = new ArrayList<>();
            for (Range range : orderedRanges) {
                if (range.isSingleValue()) {
                    // skip out-of-range values (they are implicitly false)
                    if (range(type, range.getSingleValue()) == ValueInRange.IN_RANGE) {
                        icebergValues.add(convertTrinoValueToIceberg(type, range.getSingleValue()));
                    }
                }
                else {
                    rangeExpressions.add(toIcebergExpression(columnName, range));
                }
            }
            Expression ranges = or(rangeExpressions);
            Expression values = icebergValues.isEmpty() ? alwaysFalse() : in(columnName, icebergValues);
            Expression nullExpression = domain.isNullAllowed() ? isNull(columnName) : alwaysFalse();
            return or(nullExpression, or(values, ranges));
        }

        throw new VerifyException(format("Unsupported type %s with domain values %s", type, domain));
    }

    private static Expression toIcebergExpression(String columnName, Range range)
    {
        Type type = range.getType();

        if (range.isSingleValue()) {
            return switch (range(type, range.getSingleValue())) {
                case BELOW_RANGE, ABOVE_RANGE -> alwaysFalse();
                case IN_RANGE -> equal(columnName, convertTrinoValueToIceberg(type, range.getSingleValue()));
            };
        }

        List<Expression> conjuncts = new ArrayList<>(2);
        if (!range.isLowUnbounded()) {
            conjuncts.add(switch (range(type, range.getLowBoundedValue())) {
                case ABOVE_RANGE -> alwaysFalse();
                case BELOW_RANGE -> alwaysTrue();
                case IN_RANGE -> {
                    Object icebergLow = convertTrinoValueToIceberg(type, range.getLowBoundedValue());
                    if (range.isLowInclusive()) {
                        yield greaterThanOrEqual(columnName, icebergLow);
                    }
                    else {
                        yield greaterThan(columnName, icebergLow);
                    }
                }
            });
        }

        if (!range.isHighUnbounded()) {
            conjuncts.add(switch (range(type, range.getHighBoundedValue())) {
                case ABOVE_RANGE -> alwaysTrue();
                case BELOW_RANGE -> alwaysFalse();
                case IN_RANGE -> {
                    Object icebergHigh = convertTrinoValueToIceberg(type, range.getHighBoundedValue());
                    if (range.isHighInclusive()) {
                        yield lessThanOrEqual(columnName, icebergHigh);
                    }
                    else {
                        yield lessThan(columnName, icebergHigh);
                    }
                }
            });
        }

        return and(conjuncts);
    }

    private static Expression and(List<Expression> expressions)
    {
        if (expressions.isEmpty()) {
            return alwaysTrue();
        }
        return combine(expressions, Expressions::and);
    }

    private static Expression or(Expression left, Expression right)
    {
        return Expressions.or(left, right);
    }

    private static Expression or(List<Expression> expressions)
    {
        if (expressions.isEmpty()) {
            return alwaysFalse();
        }
        return combine(expressions, Expressions::or);
    }

    private static Expression combine(List<Expression> expressions, BiFunction<Expression, Expression, Expression> combiner)
    {
        // Build balanced tree that preserves the evaluation order of the input expressions.
        //
        // The tree is built bottom up by combining pairs of elements into binary expressions.
        //
        // Example:
        //
        // Initial state:
        //  a b c d e
        //
        // First iteration:
        //
        //  /\    /\   e
        // a  b  c  d
        //
        // Second iteration:
        //
        //    / \    e
        //  /\   /\
        // a  b c  d
        //
        //
        // Last iteration:
        //
        //      / \
        //    / \  e
        //  /\   /\
        // a  b c  d

        Queue<Expression> queue = new ArrayDeque<>(expressions);
        while (queue.size() > 1) {
            Queue<Expression> buffer = new ArrayDeque<>();

            // combine pairs of elements
            while (queue.size() >= 2) {
                buffer.add(combiner.apply(queue.remove(), queue.remove()));
            }

            // if there's and odd number of elements, just append the last one
            if (!queue.isEmpty()) {
                buffer.add(queue.remove());
            }

            // continue processing the pairs that were just built
            queue = buffer;
        }

        return queue.remove();
    }

    private enum ValueInRange
    {
        BELOW_RANGE,
        IN_RANGE,
        ABOVE_RANGE
    }

    private static ValueInRange range(Type type, Object value)
    {
        long epochMicros;
        if (type.equals(TIMESTAMP_NANOS)) {
            LongTimestamp timestamp = (LongTimestamp) value;
            epochMicros = timestamp.getEpochMicros();
        }
        else if (type.equals(TIMESTAMP_TZ_NANOS)) {
            // TIMESTAMP_TZ_NANOS
            LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) value;
            epochMicros = timestamp.getEpochMillis() * 1000 + timestamp.getPicosOfMilli() / 1_000_000;
        }
        else {
            // all other types are in range
            return ValueInRange.IN_RANGE;
        }

        if (epochMicros < MIN_NANO_EPOCH_MICROS) {
            return ValueInRange.BELOW_RANGE;
        }
        else if (epochMicros > MAX_NANO_EPOCH_MICROS) {
            return ValueInRange.ABOVE_RANGE;
        }
        else {
            return ValueInRange.IN_RANGE;
        }
    }

    /**
     * Convert value from Trino representation to Iceberg representation for use in expressions.
     * For nano timestamps, the value must be verified to be in range before calling this method.
     */
    private static Object convertTrinoValueToIceberg(Type type, Object trinoNativeValue)
    {
        requireNonNull(trinoNativeValue, "trinoNativeValue is null");
        // this method should not be used for values outside supported range
        verify(range(type, trinoNativeValue) == ValueInRange.IN_RANGE);

        if (type == BOOLEAN) {
            //noinspection RedundantCast
            return (boolean) trinoNativeValue;
        }

        if (type == INTEGER) {
            return toIntExact((long) trinoNativeValue);
        }

        if (type == BIGINT) {
            //noinspection RedundantCast
            return (long) trinoNativeValue;
        }

        if (type == REAL) {
            return intBitsToFloat(toIntExact((long) trinoNativeValue));
        }

        if (type == DOUBLE) {
            //noinspection RedundantCast
            return (double) trinoNativeValue;
        }

        if (type instanceof DecimalType decimalType) {
            if (decimalType.isShort()) {
                return BigDecimal.valueOf((long) trinoNativeValue).movePointLeft(decimalType.getScale());
            }
            return new BigDecimal(((Int128) trinoNativeValue).toBigInteger(), decimalType.getScale());
        }

        if (type == DATE) {
            return toIntExact((long) trinoNativeValue);
        }

        if (type.equals(TIME_MICROS)) {
            return LongMath.divide((long) trinoNativeValue, PICOSECONDS_PER_MICROSECOND, UNNECESSARY);
        }

        if (type.equals(TIMESTAMP_MICROS)) {
            //noinspection RedundantCast
            return (long) trinoNativeValue;
        }

        if (type.equals(TIMESTAMP_TZ_MICROS)) {
            return timestampTzToMicros((LongTimestampWithTimeZone) trinoNativeValue);
        }

        // The value has been verified to be in range
        if (type.equals(TIMESTAMP_NANOS)) {
            LongTimestamp timestamp = (LongTimestamp) trinoNativeValue;
            return Expressions.nanos(timestamp.getEpochMicros() * NANOSECONDS_PER_MICROSECOND + timestamp.getPicosOfMicro() / 1_000);
        }
        if (type.equals(TIMESTAMP_TZ_NANOS)) {
            LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) trinoNativeValue;
            return Expressions.nanos(timestamp.getEpochMillis() * 1_000_000 + timestamp.getPicosOfMilli() / 1_000);
        }

        if (type instanceof VarcharType) {
            return ((Slice) trinoNativeValue).toStringUtf8();
        }

        if (type instanceof VarbinaryType) {
            return ByteBuffer.wrap(((Slice) trinoNativeValue).getBytes());
        }

        if (type == UUID) {
            return trinoUuidToJavaUuid(((Slice) trinoNativeValue));
        }

        throw new UnsupportedOperationException("Unsupported type: " + type);
    }
}
