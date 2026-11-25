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
package io.trino.operator.scalar.preimage;

import io.trino.operator.scalar.preimage.CastPreimages.RangeProjection;
import io.trino.spi.function.DomainPreimage;
import io.trino.spi.function.DomainPreimage.Context;
import io.trino.spi.function.PreimageResult;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;

import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.Double.doubleToRawLongBits;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.math.RoundingMode.CEILING;
import static java.math.RoundingMode.FLOOR;

/// Projects casts that round inputs to `DOUBLE` or `REAL` using IEEE 754 round-to-nearest-even.
public final class FloatingPointRoundingCastPreimage
        implements DomainPreimage
{
    private static final BigDecimal HALF = new BigDecimal("0.5");
    private static final double FLOAT_OVERFLOW_THRESHOLD = (double) Float.MAX_VALUE + (double) Math.ulp(Float.MAX_VALUE) / 2.0;
    private static final BigDecimal DOUBLE_OVERFLOW_THRESHOLD_DECIMAL = new BigDecimal(Double.MAX_VALUE)
            .add(new BigDecimal(Math.ulp(Double.MAX_VALUE)).multiply(HALF));
    private static final BigDecimal FLOAT_OVERFLOW_THRESHOLD_DECIMAL = new BigDecimal(FLOAT_OVERFLOW_THRESHOLD);

    @Override
    public Optional<PreimageResult> preimage(Context context, Domain resultDomain)
    {
        if (context.signature().getArgumentTypes().size() != 1) {
            return Optional.empty();
        }
        Type source = context.signature().getArgumentTypes().getFirst();
        Type target = resultDomain.getType();
        if (source.equals(DOUBLE) && target.equals(REAL)) {
            return CastPreimages.orderedRanges(resultDomain, source, FloatingPointRoundingCastPreimage::projectDoubleToReal);
        }
        if (source instanceof DecimalType decimalType && (target.equals(DOUBLE) || target.equals(REAL))) {
            return CastPreimages.orderedRanges(resultDomain, source, (value, lower, inclusive) -> projectDecimalToFloatingPoint(decimalType, target, value, lower, inclusive));
        }
        return Optional.empty();
    }

    private static Optional<RangeProjection> projectDoubleToReal(Object value, boolean lower, boolean inclusive)
    {
        float targetValue = intBitsToFloat(toIntExact((long) value));
        if (Float.isNaN(targetValue)) {
            return Optional.empty();
        }
        boolean lowerEdge = lower == inclusive;
        Optional<Object> neighborValue = lowerEdge ? REAL.getPreviousValue(value) : REAL.getNextValue(value);
        if (neighborValue.isEmpty()) {
            return Optional.of(inclusive ? RangeProjection.of(Range.all(DOUBLE)) : RangeProjection.none());
        }
        float neighbor = intBitsToFloat(toIntExact((long) neighborValue.get()));
        double threshold;
        boolean edgeIncludesFiber;
        if (Float.isInfinite(targetValue)) {
            threshold = targetValue > 0 ? FLOAT_OVERFLOW_THRESHOLD : -FLOAT_OVERFLOW_THRESHOLD;
            edgeIncludesFiber = true;
        }
        else if (Float.isInfinite(neighbor)) {
            threshold = targetValue > 0 ? FLOAT_OVERFLOW_THRESHOLD : -FLOAT_OVERFLOW_THRESHOLD;
            edgeIncludesFiber = false;
        }
        else {
            threshold = ((double) targetValue + (double) neighbor) / 2.0;
            edgeIncludesFiber = (floatToRawIntBits(targetValue) & 1) == 0;
        }
        boolean boundInclusive = inclusive == edgeIncludesFiber;
        return Optional.of(RangeProjection.of(lower
                ? (boundInclusive ? Range.greaterThanOrEqual(DOUBLE, threshold) : Range.greaterThan(DOUBLE, threshold))
                : (boundInclusive ? Range.lessThanOrEqual(DOUBLE, threshold) : Range.lessThan(DOUBLE, threshold))));
    }

    private static Optional<RangeProjection> projectDecimalToFloatingPoint(DecimalType source, Type target, Object value, boolean lower, boolean inclusive)
    {
        double targetValue = toDouble(target, value);
        if (Double.isNaN(targetValue)) {
            return Optional.empty();
        }
        boolean lowerEdge = lower == inclusive;
        Optional<Object> neighborValue = lowerEdge ? target.getPreviousValue(value) : target.getNextValue(value);
        if (neighborValue.isEmpty()) {
            return Optional.of(inclusive ? RangeProjection.of(Range.all(source)) : RangeProjection.none());
        }
        double neighbor = toDouble(target, neighborValue.get());
        BigDecimal threshold;
        boolean edgeIncludesFiber;
        if (Double.isInfinite(targetValue)) {
            BigDecimal overflow = target.equals(DOUBLE) ? DOUBLE_OVERFLOW_THRESHOLD_DECIMAL : FLOAT_OVERFLOW_THRESHOLD_DECIMAL;
            threshold = targetValue > 0 ? overflow : overflow.negate();
            edgeIncludesFiber = true;
        }
        else if (Double.isInfinite(neighbor)) {
            BigDecimal overflow = target.equals(DOUBLE) ? DOUBLE_OVERFLOW_THRESHOLD_DECIMAL : FLOAT_OVERFLOW_THRESHOLD_DECIMAL;
            threshold = targetValue > 0 ? overflow : overflow.negate();
            edgeIncludesFiber = false;
        }
        else {
            threshold = new BigDecimal(targetValue).add(new BigDecimal(neighbor)).multiply(HALF);
            edgeIncludesFiber = isSignificandEven(target, value);
        }
        return Optional.of(decimalBound(source, threshold, lowerEdge, lower, edgeIncludesFiber));
    }

    private static RangeProjection decimalBound(DecimalType source, BigDecimal threshold, boolean lowerEdge, boolean lower, boolean edgeIncludesFiber)
    {
        BigDecimal scaled = threshold.movePointRight(source.getScale());
        Type.Range bounds = source.getRange().orElseThrow();
        BigInteger maxUnscaled = source.isShort() ? BigInteger.valueOf((long) bounds.getMax()) : ((Int128) bounds.getMax()).toBigInteger();
        BigInteger minUnscaled = maxUnscaled.negate();

        if (lowerEdge) {
            // Split at the smallest unscaled decimal belonging to the fiber or above it.
            // Using >= and < at the same split point keeps complementary bounds gapless.
            BigInteger first = edgeIncludesFiber
                    ? scaled.setScale(0, CEILING).toBigIntegerExact()
                    : scaled.setScale(0, FLOOR).toBigIntegerExact().add(BigInteger.ONE);
            if (first.compareTo(maxUnscaled) > 0) {
                return lower ? RangeProjection.none() : RangeProjection.of(Range.all(source));
            }
            if (first.compareTo(minUnscaled) <= 0) {
                return lower ? RangeProjection.of(Range.all(source)) : RangeProjection.none();
            }
            Object split = toDecimalValue(source, first);
            return RangeProjection.of(lower ? Range.greaterThanOrEqual(source, split) : Range.lessThan(source, split));
        }

        // Split at the largest unscaled decimal belonging to the fiber or below it.
        // Using <= and > at the same split point keeps complementary bounds gapless.
        BigInteger last = edgeIncludesFiber
                ? scaled.setScale(0, FLOOR).toBigIntegerExact()
                : scaled.setScale(0, CEILING).toBigIntegerExact().subtract(BigInteger.ONE);
        if (last.compareTo(minUnscaled) < 0) {
            return lower ? RangeProjection.of(Range.all(source)) : RangeProjection.none();
        }
        if (last.compareTo(maxUnscaled) >= 0) {
            return lower ? RangeProjection.none() : RangeProjection.of(Range.all(source));
        }
        Object split = toDecimalValue(source, last);
        return RangeProjection.of(lower ? Range.greaterThan(source, split) : Range.lessThanOrEqual(source, split));
    }

    private static double toDouble(Type floatingType, Object value)
    {
        if (floatingType.equals(DOUBLE)) {
            return (double) value;
        }
        return intBitsToFloat(toIntExact((long) value));
    }

    private static boolean isSignificandEven(Type floatingType, Object value)
    {
        if (floatingType.equals(DOUBLE)) {
            return (doubleToRawLongBits((double) value) & 1L) == 0;
        }
        return (floatToRawIntBits(intBitsToFloat(toIntExact((long) value))) & 1) == 0;
    }

    private static Object toDecimalValue(DecimalType type, BigInteger unscaled)
    {
        if (type.isShort()) {
            return unscaled.longValueExact();
        }
        return Int128.valueOf(unscaled);
    }
}
