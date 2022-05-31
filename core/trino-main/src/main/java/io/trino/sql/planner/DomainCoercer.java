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
package io.trino.sql.planner;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.OperatorNotFoundException;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.predicate.AllOrNoneValueSet;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.Ranges;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.InterpretedFunctionInvoker;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.SATURATED_FLOOR_CAST;
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
            FunctionManager functionManager,
            TypeOperators typeOperators,
            Session session,
            Domain domain,
            Type coercedValueType)
    {
        return new ImplicitCoercer(metadata, functionManager, typeOperators, session, domain, coercedValueType).applySaturatedCasts();
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

        private ImplicitCoercer(
                Metadata metadata,
                FunctionManager functionManager,
                TypeOperators typeOperators,
                Session session,
                Domain domain,
                Type coercedValueType)
        {
            this.connectorSession = requireNonNull(session, "session is null").toConnectorSession();
            this.functionInvoker = new InterpretedFunctionInvoker(functionManager);
            this.domain = requireNonNull(domain, "domain is null");
            this.coercedValueType = requireNonNull(coercedValueType, "coercedValueType is null");
            Type originalValueType = domain.getType();
            try {
                this.saturatedFloorCastOperator = metadata.getCoercion(session, SATURATED_FLOOR_CAST, originalValueType, coercedValueType);
            }
            catch (OperatorNotFoundException e) {
                throw new IllegalStateException(
                        format("Saturated floor cast operator not found for coercion from %s to %s", originalValueType, coercedValueType));
            }
            this.castToOriginalTypeOperator = metadata.getCoercion(session, coercedValueType, originalValueType);
            // choice of placing unordered values first or last does not matter for this code
            this.comparisonOperator = typeOperators.getComparisonUnorderedLastOperator(originalValueType, InvocationConvention.simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL));
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

            Range coercedLow;
            if (range.isLowUnbounded()) {
                coercedLow = Range.all(coercedValueType);
            }
            else {
                Object originalLowValue = range.getLowBoundedValue();
                Object coercedLowValue = floorValue(saturatedFloorCastOperator, originalLowValue);
                int originalComparedToCoerced = compareOriginalValueToCoerced(castToOriginalTypeOperator, comparisonOperator, originalLowValue, coercedLowValue);
                boolean coercedValueIsEqualToOriginal = originalComparedToCoerced == 0;
                boolean coercedValueIsLessThanOriginal = originalComparedToCoerced > 0;

                if (range.isLowInclusive()) {
                    if (coercedValueIsEqualToOriginal) {
                        coercedLow = Range.greaterThanOrEqual(coercedValueType, coercedLowValue);
                    }
                    else if (coercedValueIsLessThanOriginal) {
                        coercedLow = Range.greaterThan(coercedValueType, coercedLowValue);
                    }
                    else {
                        coercedLow = Range.all(coercedValueType);
                    }
                }
                else {
                    if (coercedValueIsEqualToOriginal || coercedValueIsLessThanOriginal) {
                        coercedLow = Range.greaterThan(coercedValueType, coercedLowValue);
                    }
                    else {
                        // Coerced domain is narrower than the original domain
                        coercedLow = Range.all(coercedValueType);
                    }
                }
            }

            Range coercedHigh;
            if (range.isHighUnbounded()) {
                coercedHigh = Range.all(coercedValueType);
            }
            else {
                Object originalHighValue = range.getHighBoundedValue();
                Object coercedHighValue = floorValue(saturatedFloorCastOperator, originalHighValue);
                int originalComparedToCoerced = compareOriginalValueToCoerced(castToOriginalTypeOperator, comparisonOperator, originalHighValue, coercedHighValue);
                boolean coercedValueIsEqualToOriginal = originalComparedToCoerced == 0;
                boolean coercedValueIsLessThanOriginal = originalComparedToCoerced > 0;

                if (range.isHighInclusive()) {
                    if (coercedValueIsEqualToOriginal || coercedValueIsLessThanOriginal) {
                        coercedHigh = Range.lessThanOrEqual(coercedValueType, coercedHighValue);
                    }
                    else {
                        // Coerced range is outside the domain of target type
                        return Optional.empty();
                    }
                }
                else {
                    if (coercedValueIsEqualToOriginal) {
                        coercedHigh = Range.lessThan(coercedValueType, coercedHighValue);
                    }
                    else if (coercedValueIsLessThanOriginal) {
                        coercedHigh = Range.lessThanOrEqual(coercedValueType, coercedHighValue);
                    }
                    else {
                        // Coerced range is outside the domain of target type
                        return Optional.empty();
                    }
                }
            }

            return coercedLow.intersect(coercedHigh);
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
                throw new TrinoException(GENERIC_INTERNAL_ERROR, throwable);
            }
        }

        private Object floorValue(ResolvedFunction saturatedFloorCastOperator, Object value)
        {
            return functionInvoker.invoke(saturatedFloorCastOperator, connectorSession, value);
        }
    }
}
