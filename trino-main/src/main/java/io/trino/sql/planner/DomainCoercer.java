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
package io.prestosql.sql.planner;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.OperatorNotFoundException;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.InvocationConvention;
import io.prestosql.spi.predicate.AllOrNoneValueSet;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Marker;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.Ranges;
import io.prestosql.spi.predicate.SortedRangeSet;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeOperators;
import io.prestosql.sql.InterpretedFunctionInvoker;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.prestosql.spi.function.OperatorType.SATURATED_FLOOR_CAST;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Apply saturated floor casts for implicit coercions on TupleDomain.
 * This class does not handle Float.NaN and Double.NaN because
 * currently it is used only in dynamic filtering where NaNs are not part of TupleDomain.
 */
public final class DomainCoercer
{
    private DomainCoercer() {}

    public static Domain applySaturatedCasts(
            Metadata metadata,
            TypeOperators typeOperators,
            Session session,
            Domain domain,
            Type coercedValueType)
    {
        return new ImplicitCoercer(metadata, typeOperators, session, domain, coercedValueType).applySaturatedCasts();
    }

    private static class ImplicitCoercer
    {
        private final ConnectorSession connectorSession;
        private final InterpretedFunctionInvoker functionInvoker;
        private final ResolvedFunction saturatedFloorCastOperator;
        private final ResolvedFunction castToOriginalTypeOperator;
        private final MethodHandle comparisonOperator;
        private final Domain domain;
        private final Type coercedValueType;

        private ImplicitCoercer(Metadata metadata, TypeOperators typeOperators, Session session, Domain domain, Type coercedValueType)
        {
            this.connectorSession = requireNonNull(session, "session is null").toConnectorSession();
            this.functionInvoker = new InterpretedFunctionInvoker(metadata);
            this.domain = requireNonNull(domain, "domain is null");
            this.coercedValueType = requireNonNull(coercedValueType, "coercedValueType is null");
            Type originalValueType = domain.getType();
            try {
                this.saturatedFloorCastOperator = metadata.getCoercion(SATURATED_FLOOR_CAST, originalValueType, coercedValueType);
            }
            catch (OperatorNotFoundException e) {
                throw new IllegalStateException(
                        format("Saturated floor cast operator not found for coercion from %s to %s", originalValueType, coercedValueType));
            }
            this.castToOriginalTypeOperator = metadata.getCoercion(coercedValueType, originalValueType);
            this.comparisonOperator = typeOperators.getComparisonOperator(originalValueType, InvocationConvention.simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL));
        }

        public Domain applySaturatedCasts()
        {
            if (domain.isNone()) {
                return Domain.none(coercedValueType);
            }
            ValueSet saturatedValueSet = domain.getValues().getValuesProcessor().transform(
                    this::applySaturatedCasts,
                    discreteValues -> ValueSet.all(coercedValueType),
                    allOrNone -> new AllOrNoneValueSet(coercedValueType, allOrNone.isAll()));

            return Domain.create(saturatedValueSet, domain.isNullAllowed());
        }

        private ValueSet applySaturatedCasts(Ranges ranges)
        {
            ImmutableList.Builder<Range> builder = ImmutableList.builder();
            for (Range range : ranges.getOrderedRanges()) {
                Optional<Range> coercedRange = applySaturatedCasts(range);
                if (coercedRange.isEmpty()) {
                    continue;
                }
                if (coercedRange.get().isAll()) {
                    return ValueSet.all(coercedValueType);
                }
                builder.add(coercedRange.get());
            }
            return SortedRangeSet.copyOf(coercedValueType, builder.build());
        }

        private Optional<Range> applySaturatedCasts(Range range)
        {
            if (range.isSingleValue()) {
                Optional<Object> coercedValue = applySaturatedCast(range.getSingleValue());
                return coercedValue.map(value -> Range.equal(coercedValueType, value));
            }

            Marker coercedLow;
            if (range.getLow().isLowerUnbounded()) {
                coercedLow = Marker.lowerUnbounded(coercedValueType);
            }
            else {
                Object originalLowValue = range.getLow().getValue();
                Object coercedLowValue = floorValue(saturatedFloorCastOperator, originalLowValue);
                int originalComparedToCoerced = compareOriginalValueToCoerced(castToOriginalTypeOperator, comparisonOperator, originalLowValue, coercedLowValue);
                boolean coercedValueIsEqualToOriginal = originalComparedToCoerced == 0;
                boolean coercedValueIsLessThanOriginal = originalComparedToCoerced > 0;

                switch (range.getLow().getBound()) {
                    case ABOVE:
                        if (coercedValueIsEqualToOriginal || coercedValueIsLessThanOriginal) {
                            coercedLow = Marker.above(coercedValueType, coercedLowValue);
                        }
                        else {
                            // Coerced domain is narrower than the original domain
                            coercedLow = Marker.lowerUnbounded(coercedValueType);
                        }
                        break;
                    case EXACTLY:
                        if (coercedValueIsEqualToOriginal) {
                            coercedLow = Marker.exactly(coercedValueType, coercedLowValue);
                        }
                        else if (coercedValueIsLessThanOriginal) {
                            coercedLow = Marker.above(coercedValueType, coercedLowValue);
                        }
                        else {
                            coercedLow = Marker.lowerUnbounded(coercedValueType);
                        }
                        break;
                    case BELOW:
                        throw new IllegalStateException("Low Marker should never use BELOW bound: " + range);
                    default:
                        throw new IllegalStateException("Unhandled bound: " + range.getLow().getBound());
                }
            }

            Marker coercedHigh;
            if (range.getHigh().isUpperUnbounded()) {
                coercedHigh = Marker.upperUnbounded(coercedValueType);
            }
            else {
                Object originalHighValue = range.getHigh().getValue();
                Object coercedHighValue = floorValue(saturatedFloorCastOperator, originalHighValue);
                int originalComparedToCoerced = compareOriginalValueToCoerced(castToOriginalTypeOperator, comparisonOperator, originalHighValue, coercedHighValue);
                boolean coercedValueIsEqualToOriginal = originalComparedToCoerced == 0;
                boolean coercedValueIsLessThanOriginal = originalComparedToCoerced > 0;

                switch (range.getHigh().getBound()) {
                    case ABOVE:
                        throw new IllegalStateException("High Marker should never use ABOVE bound: " + range);
                    case EXACTLY:
                        if (coercedValueIsEqualToOriginal || coercedValueIsLessThanOriginal) {
                            coercedHigh = Marker.exactly(coercedValueType, coercedHighValue);
                        }
                        else {
                            // Coerced range is outside the domain of target type
                            return Optional.empty();
                        }
                        break;
                    case BELOW:
                        if (coercedValueIsEqualToOriginal) {
                            coercedHigh = Marker.below(coercedValueType, coercedHighValue);
                        }
                        else if (coercedValueIsLessThanOriginal) {
                            coercedHigh = Marker.exactly(coercedValueType, coercedHighValue);
                        }
                        else {
                            // Coerced range is outside the domain of target type
                            return Optional.empty();
                        }
                        break;
                    default:
                        throw new IllegalStateException("Unhandled bound: " + range.getHigh().getBound());
                }
            }

            if (coercedLow.compareTo(coercedHigh) > 0) {
                // Both high and low are greater than max of target type, resulting in (max, max] after coercion
                return Optional.empty();
            }
            return Optional.of(new Range(coercedLow, coercedHigh));
        }

        private Optional<Object> applySaturatedCast(Object originalValue)
        {
            Object coercedFloorValue = floorValue(saturatedFloorCastOperator, originalValue);
            int originalComparedToCoerced = compareOriginalValueToCoerced(castToOriginalTypeOperator, comparisonOperator, originalValue, coercedFloorValue);
            if (originalComparedToCoerced == 0) {
                return Optional.of(coercedFloorValue);
            }
            else {
                return Optional.empty();
            }
        }

        private int compareOriginalValueToCoerced(ResolvedFunction castToOriginalTypeOperator, MethodHandle comparisonOperator, Object originalValue, Object coercedValue)
        {
            Object coercedValueInOriginalType = functionInvoker.invoke(castToOriginalTypeOperator, connectorSession, coercedValue);
            try {
                return (int) (long) comparisonOperator.invoke(originalValue, coercedValueInOriginalType);
            }
            catch (Throwable throwable) {
                Throwables.throwIfUnchecked(throwable);
                throw new PrestoException(GENERIC_INTERNAL_ERROR, throwable);
            }
        }

        private Object floorValue(ResolvedFunction saturatedFloorCastOperator, Object value)
        {
            return functionInvoker.invoke(saturatedFloorCastOperator, connectorSession, value);
        }
    }
}
