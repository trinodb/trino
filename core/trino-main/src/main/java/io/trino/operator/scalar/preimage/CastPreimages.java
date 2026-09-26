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

import io.trino.spi.TrinoException;
import io.trino.spi.function.DomainPreimage.Context;
import io.trino.spi.function.PreimageResult;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.FloatingPointValueSet;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.function.PreimageResult.Exactness.EXACT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TypeUtils.typeHasNaN;

/// Shared range accumulation, cast admission, and round-trip boundary conversion.
final class CastPreimages
{
    private CastPreimages() {}

    static Optional<PreimageResult> compute(Supplier<Optional<PreimageResult>> computation)
    {
        try {
            return computation.get();
        }
        catch (TrinoException e) {
            if (!conversionFailure(e)) {
                throw e;
            }
            return Optional.empty();
        }
    }

    @FunctionalInterface
    interface Boundary
    {
        Optional<ValueSet> project(Object value, boolean lower, boolean inclusive);
    }

    static Optional<PreimageResult> orderedRanges(Domain resultDomain, Type source, Boundary boundary)
    {
        List<Range> inputRanges = new ArrayList<>();
        ValueSet resultValues = resultDomain.getValues();
        if (resultValues instanceof FloatingPointValueSet floatingPoint) {
            resultValues = floatingPoint.getOrderedValues();
        }
        for (Range range : resultValues.getRanges().getOrderedRanges()) {
            Optional<ValueSet> low = range.isLowUnbounded() ? Optional.of(ValueSet.all(source)) : boundary.project(range.getLowBoundedValue(), true, range.isLowInclusive());
            Optional<ValueSet> high = range.isHighUnbounded() ? Optional.of(ValueSet.all(source)) : boundary.project(range.getHighBoundedValue(), false, range.isHighInclusive());
            if (low.isEmpty() || high.isEmpty()) {
                return Optional.empty();
            }
            inputRanges.addAll(low.get().intersect(high.get()).getRanges().getOrderedRanges());
        }
        ValueSet input = ValueSet.copyOfRanges(source, inputRanges);
        if (input instanceof FloatingPointValueSet floatingPoint) {
            input = new FloatingPointValueSet(floatingPoint.getOrderedValues(), false);
        }
        Domain preimage = Domain.create(input, resultDomain.isNullAllowed());
        if (typeHasNaN(source) && typeHasNaN(resultDomain.getType()) && resultDomain.includesNullableValue(FloatingPointValueSet.nanValue(resultDomain.getType()))) {
            preimage = preimage.union(Domain.singleValue(source, FloatingPointValueSet.nanValue(source)));
        }
        return Optional.of(new PreimageResult(preimage, EXACT));
    }

    static Optional<PreimageResult> equality(Domain result, Type source, Function<Object, Optional<ValueSet>> point)
    {
        ValueSet values = result.getValues();
        boolean complement = false;
        if (!values.isDiscreteSet() && !values.isNone()) {
            values = values.complement();
            complement = true;
        }
        if (!values.isDiscreteSet() && !values.isNone()) {
            return Optional.empty();
        }
        List<Range> inputRanges = new ArrayList<>();
        for (Object value : values.isNone() ? List.of() : values.getDiscreteSet()) {
            Optional<ValueSet> mapped = point.apply(value);
            if (mapped.isEmpty()) {
                return Optional.empty();
            }
            inputRanges.addAll(mapped.get().getRanges().getOrderedRanges());
        }
        ValueSet input = ValueSet.copyOfRanges(source, inputRanges);
        return Optional.of(new PreimageResult(Domain.create(complement ? input.complement() : input, result.isNullAllowed()), EXACT));
    }

    static boolean conversionFailure(TrinoException e)
    {
        return e.getErrorCode().equals(INVALID_CAST_ARGUMENT.toErrorCode()) || e.getErrorCode().equals(NUMERIC_VALUE_OUT_OF_RANGE.toErrorCode());
    }

    static boolean eligible(Context context, Type source, Type target)
    {
        if (source instanceof DecimalType sourceDecimal && target instanceof DecimalType targetDecimal) {
            // Increasing scale preserves every successful source value even if reduced
            // integer precision makes the cast fail for other values.
            return targetDecimal.getScale() >= sourceDecimal.getScale();
        }
        if (target instanceof TimeWithTimeZoneType) {
            return false;
        }
        if (target instanceof TimestampWithTimeZoneType && !(source.equals(DATE) || source instanceof TimestampType)) {
            return false;
        }
        if (source instanceof DecimalType decimal && ((target.equals(DOUBLE) && decimal.getPrecision() > 15) || (target.equals(REAL) && decimal.getPrecision() > 7))) {
            return false;
        }
        return context.functions().canCoerce(source, target);
    }

    static boolean injectiveAt(Context context, Type source, Type target, Object value)
    {
        if (IntegralToFloatingPointCastPreimage.supports(source, target)) {
            return IntegralToFloatingPointCastPreimage.injectiveAt(source, target, value);
        }
        if (target instanceof TimestampWithTimeZoneType timestamp) {
            return TimestampWithTimeZoneCastPreimage.injectiveAt(context, timestamp, value);
        }
        return true;
    }

    static Optional<ValueSet> roundTripBound(Type source, Optional<Type.Range> sourceRange, Function<Object, Object> forward, Function<Object, Object> reverse, Comparator<Object> comparison, Object value, boolean lower, boolean inclusive)
    {
        if (sourceRange.isPresent()) {
            Type.Range range = sourceRange.orElseThrow();
            try {
                Object minimum = forward.apply(range.getMin());
                Object maximum = forward.apply(range.getMax());
                if (comparison.compare(value, minimum) < 0 || (comparison.compare(value, minimum) == 0 && inclusive == lower)) {
                    return Optional.of(lower ? ValueSet.all(source) : ValueSet.none(source));
                }
                if (comparison.compare(value, maximum) > 0 || (comparison.compare(value, maximum) == 0 && inclusive != lower)) {
                    return Optional.of(lower ? ValueSet.none(source) : ValueSet.all(source));
                }
            }
            catch (TrinoException e) {
                if (!conversionFailure(e)) {
                    throw e;
                }
            }
        }
        Object input;
        try {
            input = reverse.apply(value);
        }
        catch (TrinoException e) {
            if (!conversionFailure(e)) {
                throw e;
            }
            return Optional.empty();
        }
        return Optional.of(roundTripBound(source, input, comparison.compare(forward.apply(input), value), lower, inclusive));
    }

    static ValueSet roundTripBound(Type source, Object input, int roundTrip, boolean lower, boolean inclusive)
    {
        boolean include = roundTrip == 0 ? inclusive : (lower ? roundTrip > 0 : roundTrip < 0);
        return ValueSet.ofRanges(lower
                ? (include ? Range.greaterThanOrEqual(source, input) : Range.greaterThan(source, input))
                : (include ? Range.lessThanOrEqual(source, input) : Range.lessThan(source, input)));
    }
}
