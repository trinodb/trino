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
import io.airlift.slice.Slice;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.iceberg.expressions.Expression;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static io.trino.plugin.iceberg.util.Timestamps.timestampTzToMicros;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.expressions.Expressions.alwaysFalse;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.or;

public final class ExpressionConverter
{
    private ExpressionConverter() {}

    public static Expression toIcebergExpression(TupleDomain<IcebergColumnHandle> tupleDomain)
    {
        if (tupleDomain.isAll()) {
            return alwaysTrue();
        }
        if (tupleDomain.getDomains().isEmpty()) {
            return alwaysFalse();
        }
        Map<IcebergColumnHandle, Domain> domainMap = tupleDomain.getDomains().get();
        Expression expression = alwaysTrue();
        for (Map.Entry<IcebergColumnHandle, Domain> entry : domainMap.entrySet()) {
            IcebergColumnHandle columnHandle = entry.getKey();
            Domain domain = entry.getValue();
            expression = and(expression, toIcebergExpression(columnHandle.getName(), columnHandle.getType(), domain));
        }
        return expression;
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

        // Skip structural types. TODO (https://github.com/trinodb/trino/issues/8759) Evaluate Apache Iceberg's support for predicate on structural types
        if (type instanceof ArrayType || type instanceof MapType || type instanceof RowType) {
            // Fail fast. Ignoring expression could lead to data loss in case of deletions.
            throw new UnsupportedOperationException("Unsupported type for expression: " + type);
        }

        ValueSet domainValues = domain.getValues();
        Expression expression = null;
        if (domain.isNullAllowed()) {
            expression = isNull(columnName);
        }

        if (domainValues instanceof SortedRangeSet) {
            List<Range> orderedRanges = ((SortedRangeSet) domainValues).getOrderedRanges();
            expression = firstNonNull(expression, alwaysFalse());

            for (Range range : orderedRanges) {
                expression = or(expression, toIcebergExpression(columnName, range));
            }
            return expression;
        }

        throw new VerifyException("Did not expect a domain value set other than SortedRangeSet but got " + domainValues.getClass().getSimpleName());
    }

    private static Expression toIcebergExpression(String columnName, Range range)
    {
        Type type = range.getType();

        if (range.isSingleValue()) {
            Object icebergValue = getIcebergLiteralValue(type, range.getSingleValue());
            return equal(columnName, icebergValue);
        }

        Expression lowBound;
        if (range.isLowUnbounded()) {
            lowBound = alwaysTrue();
        }
        else {
            Object icebergLow = getIcebergLiteralValue(type, range.getLowBoundedValue());
            if (range.isLowInclusive()) {
                lowBound = greaterThanOrEqual(columnName, icebergLow);
            }
            else {
                lowBound = greaterThan(columnName, icebergLow);
            }
        }

        Expression highBound;
        if (range.isHighUnbounded()) {
            highBound = alwaysTrue();
        }
        else {
            Object icebergHigh = getIcebergLiteralValue(type, range.getHighBoundedValue());
            if (range.isHighInclusive()) {
                highBound = lessThanOrEqual(columnName, icebergHigh);
            }
            else {
                highBound = lessThan(columnName, icebergHigh);
            }
        }

        return and(lowBound, highBound);
    }

    private static Object getIcebergLiteralValue(Type type, Object trinoNativeValue)
    {
        requireNonNull(trinoNativeValue, "trinoNativeValue is null");

        if (type instanceof BooleanType) {
            return (boolean) trinoNativeValue;
        }

        if (type instanceof IntegerType) {
            return toIntExact((long) trinoNativeValue);
        }

        if (type instanceof BigintType) {
            return (long) trinoNativeValue;
        }

        if (type instanceof RealType) {
            return intBitsToFloat(toIntExact((long) trinoNativeValue));
        }

        if (type instanceof DoubleType) {
            return (double) trinoNativeValue;
        }

        // TODO: Remove this conversion once we move to next iceberg version
        if (type instanceof DateType) {
            return toIntExact(((Long) trinoNativeValue));
        }

        if (type.equals(TIME_MICROS)) {
            return ((long) trinoNativeValue) / PICOSECONDS_PER_MICROSECOND;
        }

        if (type.equals(TIMESTAMP_MICROS)) {
            return (long) trinoNativeValue;
        }

        if (type.equals(TIMESTAMP_TZ_MICROS)) {
            return timestampTzToMicros((LongTimestampWithTimeZone) trinoNativeValue);
        }

        if (type instanceof VarcharType) {
            return ((Slice) trinoNativeValue).toStringUtf8();
        }

        if (type instanceof VarbinaryType) {
            return ByteBuffer.wrap(((Slice) trinoNativeValue).getBytes());
        }

        if (type instanceof UuidType) {
            return trinoUuidToJavaUuid(((Slice) trinoNativeValue));
        }

        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            if (Decimals.isShortDecimal(decimalType)) {
                return BigDecimal.valueOf((long) trinoNativeValue).movePointLeft(decimalType.getScale());
            }
            return new BigDecimal(Decimals.decodeUnscaledValue((Slice) trinoNativeValue), decimalType.getScale());
        }

        throw new UnsupportedOperationException("Unsupported type: " + type);
    }
}
