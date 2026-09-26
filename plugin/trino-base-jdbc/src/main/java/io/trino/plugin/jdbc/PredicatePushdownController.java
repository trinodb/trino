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
package io.trino.plugin.jdbc;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.FloatingPointValueSet;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.CharType;
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Collection;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.getDomainCompactionThreshold;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public interface PredicatePushdownController
{
    PredicatePushdownController FULL_PUSHDOWN = (session, domain) -> {
        Domain compacted = domain.simplify(getDomainCompactionThreshold(session));
        if (compacted.getValues() instanceof FloatingPointValueSet floatingPoint && !floatingPoint.isAll()) {
            // The generic SQL renderer has no NaN predicate or portable infinity literal.
            // A column mapping with native support can supply its own pushdown controller.
            if (floatingPoint.isNaNAllowed() || floatingPoint.isAllOrderedValues() ||
                    floatingPoint.getRanges().getOrderedRanges().stream().anyMatch(PredicatePushdownController::hasNonFiniteBound)) {
                return new DomainPushdownResult(Domain.create(ValueSet.all(domain.getType()), domain.isNullAllowed()), domain);
            }
        }
        // Backends can order NaN above or below numbers. Ordered comparisons may admit it.
        boolean needsFloatingPointResidual = compacted.getValues() instanceof FloatingPointValueSet floatingPoint &&
                !floatingPoint.isAll() && floatingPoint.getRanges().getOrderedRanges().stream()
                .anyMatch(range -> range.isLowUnbounded() || range.isHighUnbounded());
        return new DomainPushdownResult(compacted, compacted.equals(domain) && !needsFloatingPointResidual ? Domain.all(domain.getType()) : domain);
    };

    /// For column mappings whose storage type cannot contain NaN or infinities.
    PredicatePushdownController FINITE_FLOATING_POINT_PUSHDOWN = (session, domain) -> {
        Domain compacted = domain.simplify(getDomainCompactionThreshold(session));
        if (compacted.getValues() instanceof FloatingPointValueSet floatingPoint && !floatingPoint.isAll()) {
            if (floatingPoint.isAllOrderedValues()) {
                return new DomainPushdownResult(
                        Domain.create(ValueSet.all(domain.getType()), domain.isNullAllowed()),
                        compacted.equals(domain) ? Domain.all(domain.getType()) : domain);
            }
            if (floatingPoint.isNaNAllowed() || floatingPoint.getRanges().getOrderedRanges().stream().anyMatch(PredicatePushdownController::hasNonFiniteBound)) {
                return FULL_PUSHDOWN.apply(session, domain);
            }
        }
        return new DomainPushdownResult(compacted, compacted.equals(domain) ? Domain.all(domain.getType()) : domain);
    };

    /// For mappings that bind infinities and render explicit infinity bounds, as in [FloatingPointQueryBuilder].
    PredicatePushdownController FLOATING_POINT_PUSHDOWN = (session, domain) -> {
        Domain compacted = domain.simplify(getDomainCompactionThreshold(session));
        if (compacted.getValues() instanceof FloatingPointValueSet floatingPoint && !floatingPoint.isAll() && floatingPoint.isNaNAllowed()) {
            return new DomainPushdownResult(Domain.create(ValueSet.all(domain.getType()), domain.isNullAllowed()), domain);
        }
        return new DomainPushdownResult(compacted, compacted.equals(domain) ? Domain.all(domain.getType()) : domain);
    };

    PredicatePushdownController DISABLE_PUSHDOWN = (_, domain) -> new DomainPushdownResult(
            Domain.all(domain.getType()),
            domain);

    PredicatePushdownController CASE_INSENSITIVE_CHARACTER_PUSHDOWN = (session, domain) -> {
        checkArgument(
                domain.getType() instanceof VarcharType || domain.getType() instanceof CharType,
                "CASE_INSENSITIVE_CHARACTER_PUSHDOWN can be used only for chars and varchars");

        if (domain.isOnlyNull() || domain.getValues().isAll()) {
            return FULL_PUSHDOWN.apply(session, domain);
        }

        if (!domain.getValues().isDiscreteSet()) {
            // case insensitive predicate pushdown could return incorrect results for operators like `!=`, `<` or `>`
            return DISABLE_PUSHDOWN.apply(session, domain);
        }

        Domain simplifiedDomain = domain.simplify(getDomainCompactionThreshold(session));
        if (!simplifiedDomain.getValues().isDiscreteSet()) {
            // Domain#simplify can turn a discrete set into a range predicate
            // Push down of range predicate for varchar/char types could lead to incorrect results
            // when the remote database is case insensitive
            return DISABLE_PUSHDOWN.apply(session, domain);
        }
        return new DomainPushdownResult(simplifiedDomain, domain);
    };

    static PredicatePushdownController pushdownDiscreteValues(Type type)
    {
        return (session, domain) -> {
            Optional<Collection<Object>> expandedRange = domain.getValues().tryExpandRanges(getDomainCompactionThreshold(session));
            if (expandedRange.isPresent()) {
                Domain convertedDiscreteDomain = Domain.create(ValueSet.copyOf(type, expandedRange.get()), domain.isNullAllowed());
                return new DomainPushdownResult(convertedDiscreteDomain, Domain.all(domain.getType()));
            }
            return FULL_PUSHDOWN.apply(session, domain);
        };
    }

    DomainPushdownResult apply(ConnectorSession session, Domain domain);

    final class DomainPushdownResult
    {
        private final Domain pushedDown;
        // In some cases, remainingFilter can be the same as pushedDown, e.g. when target database is case insensitive
        private final Domain remainingFilter;

        public DomainPushdownResult(Domain pushedDown, Domain remainingFilter)
        {
            this.pushedDown = requireNonNull(pushedDown, "pushedDown is null");
            this.remainingFilter = requireNonNull(remainingFilter, "remainingFilter is null");
        }

        public Domain getPushedDown()
        {
            return pushedDown;
        }

        public Domain getRemainingFilter()
        {
            return remainingFilter;
        }
    }

    private static boolean hasNonFiniteBound(Range range)
    {
        return (!range.isLowUnbounded() && isNonFinite(range.getType(), range.getLowBoundedValue())) ||
                (!range.isHighUnbounded() && isNonFinite(range.getType(), range.getHighBoundedValue()));
    }

    private static boolean isNonFinite(Type type, Object value)
    {
        if (type.equals(DOUBLE)) {
            return !Double.isFinite((double) value);
        }
        if (type.equals(REAL)) {
            return !Float.isFinite(Float.intBitsToFloat(toIntExact((long) value)));
        }
        if (type.equals(NUMBER)) {
            return !(((TrinoNumber) value).toBigDecimal() instanceof TrinoNumber.BigDecimalValue);
        }
        return true;
    }
}
