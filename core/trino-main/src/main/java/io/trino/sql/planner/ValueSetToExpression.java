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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.PeekingIterator;
import io.trino.metadata.Metadata;
import io.trino.spi.predicate.DiscreteValues;
import io.trino.spi.predicate.FloatingPointValueSet;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.Ranges;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.Logical;
import io.trino.type.CharVarcharCoercion;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterators.peekingIterator;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.typeHasNaN;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.IDENTICAL;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.NOT_EQUAL;
import static io.trino.sql.ir.IrExpressions.comparison;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.ir.IrUtils.combineDisjunctsWithDefault;
import static io.trino.sql.ir.Logical.Operator.AND;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/// Renders non-null value sets over an expression. The caller owns null semantics and operand binding.
public final class ValueSetToExpression
{
    private final Metadata metadata;

    public ValueSetToExpression(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    public Expression toExpression(CharVarcharCoercion charVarcharCoercion, ValueSet values, Expression operand)
    {
        if (values.isNone()) {
            return FALSE;
        }
        if (values.isAll()) {
            return TRUE;
        }
        return combineDisjunctsWithDefault(values.getValuesProcessor().transform(
                ranges -> extractDisjuncts(charVarcharCoercion, values.getType(), ranges, operand),
                discrete -> extractDisjuncts(charVarcharCoercion, values.getType(), discrete, operand),
                _ -> { throw new IllegalStateException("Case should not be reachable"); },
                floatingPoint -> {
                    List<Expression> predicates = new ArrayList<>();
                    if (!floatingPoint.getOrderedValues().isNone()) {
                        predicates.addAll(extractDisjuncts(charVarcharCoercion, values.getType(), new FloatingPointValueSet(floatingPoint.getOrderedValues(), false).getRanges(), operand));
                    }
                    if (floatingPoint.isNaNAllowed()) {
                        predicates.add(comparison(metadata, charVarcharCoercion, IDENTICAL, operand, new Constant(values.getType(), FloatingPointValueSet.nanValue(values.getType()))));
                    }
                    return predicates;
                }), TRUE);
    }

    public static ValueSet simplifyValues(ValueSet values)
    {
        Type type = values.getType();
        ValueSet result = ValueSet.none(type);
        ValueSet orderedValues = values;
        if (values instanceof FloatingPointValueSet floatingPoint) {
            orderedValues = floatingPoint.getOrderedValues();
            if (floatingPoint.isNaNAllowed()) {
                result = ValueSet.of(type, FloatingPointValueSet.nanValue(type));
            }
        }
        for (Range range : orderedValues.getRanges().getOrderedRanges()) {
            if (range.isAll()) {
                return values;
            }
            if (type.getRange().isPresent()) {
                var bounds = type.getRange().orElseThrow();
                if (range.isHighUnbounded() && range.getLowBoundedValue().equals(bounds.getMax()) && range.isLowInclusive()) {
                    result = result.union(ValueSet.of(type, bounds.getMax()));
                    continue;
                }
                if (range.isLowUnbounded() && range.getHighBoundedValue().equals(bounds.getMin()) && range.isHighInclusive()) {
                    result = result.union(ValueSet.of(type, bounds.getMin()));
                    continue;
                }
                if (range.isLowUnbounded() && range.getHighBoundedValue().equals(bounds.getMax()) && !range.isHighInclusive()) {
                    result = result.union(ValueSet.of(type, bounds.getMax()).complement());
                    continue;
                }
                if (range.isHighUnbounded() && range.getLowBoundedValue().equals(bounds.getMin()) && !range.isLowInclusive()) {
                    result = result.union(ValueSet.of(type, bounds.getMin()).complement());
                    continue;
                }
            }
            if (!range.isLowUnbounded() && !range.isHighUnbounded()) {
                var low = range.isLowInclusive() ? Optional.of(range.getLowBoundedValue()) : type.getNextValue(range.getLowBoundedValue());
                var high = range.isHighInclusive() ? Optional.of(range.getHighBoundedValue()) : type.getPreviousValue(range.getHighBoundedValue());
                if (low.isPresent() && high.isPresent()) {
                    if (Range.greaterThan(type, high.get()).contains(Range.equal(type, low.get()))) {
                        continue;
                    }
                    if (low.get().equals(high.get())) {
                        result = result.union(ValueSet.of(type, low.get()));
                        continue;
                    }
                    if (List.of(TINYINT, SMALLINT, INTEGER, BIGINT).contains(type)) {
                        range = Range.range(type, low.get(), true, high.get(), true);
                    }
                }
            }
            result = result.union(ValueSet.ofRanges(range));
        }
        return result;
    }

    private Expression processRange(CharVarcharCoercion charVarcharCoercion, Type type, Range range, Expression reference)
    {
        if (range.isAll()) {
            return TRUE;
        }

        if (isBetween(range)) {
            return new Logical(AND, ImmutableList.of(
                    comparison(metadata, charVarcharCoercion, GREATER_THAN_OR_EQUAL, reference, new Constant(type, range.getLowBoundedValue())),
                    comparison(metadata, charVarcharCoercion, LESS_THAN_OR_EQUAL, reference, new Constant(type, range.getHighBoundedValue()))));
        }

        List<Expression> rangeConjuncts = new ArrayList<>();
        if (!range.isLowUnbounded()) {
            rangeConjuncts.add(comparison(
                    metadata,
                    charVarcharCoercion,
                    range.isLowInclusive() ? GREATER_THAN_OR_EQUAL : GREATER_THAN,
                    reference,
                    new Constant(type, range.getLowBoundedValue())));
        }
        if (!range.isHighUnbounded()) {
            rangeConjuncts.add(comparison(
                    metadata,
                    charVarcharCoercion,
                    range.isHighInclusive() ? LESS_THAN_OR_EQUAL : LESS_THAN,
                    reference,
                    new Constant(type, range.getHighBoundedValue())));
        }
        // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
        checkState(!rangeConjuncts.isEmpty());
        return combineConjuncts(rangeConjuncts);
    }

    private Expression combineRangeWithExcludedPoints(CharVarcharCoercion charVarcharCoercion, Type type, Expression reference, Range range, List<Expression> excludedPoints)
    {
        if (excludedPoints.isEmpty()) {
            return processRange(charVarcharCoercion, type, range, reference);
        }

        Expression excludedPointsExpression = not(metadata, charVarcharCoercion, new In(reference, excludedPoints));
        if (excludedPoints.size() == 1) {
            excludedPointsExpression = comparison(metadata, charVarcharCoercion, NOT_EQUAL, reference, getOnlyElement(excludedPoints));
        }

        return combineConjuncts(processRange(charVarcharCoercion, type, range, reference), excludedPointsExpression);
    }

    private List<Expression> extractDisjuncts(CharVarcharCoercion charVarcharCoercion, Type type, Ranges ranges, Expression reference)
    {
        List<Expression> disjuncts = new ArrayList<>();
        List<Expression> singleValues = new ArrayList<>();
        List<Range> orderedRanges = ranges.getOrderedRanges();

        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(type, orderedRanges);
        SortedRangeSet complement = sortedRangeSet.complement();

        List<Range> singleValueExclusionsList = complement.getOrderedRanges().stream().filter(Range::isSingleValue).collect(toList());
        List<Range> originalUnionSingleValues = SortedRangeSet.copyOf(type, singleValueExclusionsList).union(sortedRangeSet).getOrderedRanges();
        PeekingIterator<Range> singleValueExclusions = peekingIterator(singleValueExclusionsList.iterator());

        /*
        For types including NaN, it is incorrect to introduce range "all" while processing a set of ranges,
        even if the component ranges cover the entire value set.
        This is because partial ranges don't include NaN, while range "all" does.
        Example: ranges (unbounded , 1.0) and (1.0, unbounded) should not be coalesced to (unbounded, unbounded) with excluded point 1.0.
        That result would be further translated to expression "xxx <> 1.0", which is satisfied by NaN.
        To avoid error, in such case the ranges are not optimised.
         */
        if (typeHasNaN(type)) {
            boolean originalRangeIsAll = orderedRanges.stream().anyMatch(Range::isAll);
            boolean coalescedRangeIsAll = originalUnionSingleValues.stream().anyMatch(Range::isAll);
            if (!originalRangeIsAll && coalescedRangeIsAll) {
                for (Range range : orderedRanges) {
                    disjuncts.add(processRange(charVarcharCoercion, type, range, reference));
                }
                return disjuncts;
            }
        }

        for (Range range : originalUnionSingleValues) {
            if (range.isSingleValue()) {
                singleValues.add(new Constant(type, range.getSingleValue()));
                continue;
            }

            // attempt to optimize ranges that can be coalesced as long as single value points are excluded
            List<Expression> singleValuesInRange = new ArrayList<>();
            while (singleValueExclusions.hasNext() && range.contains(singleValueExclusions.peek())) {
                singleValuesInRange.add(new Constant(type, singleValueExclusions.next().getSingleValue()));
            }

            if (!singleValuesInRange.isEmpty()) {
                disjuncts.add(combineRangeWithExcludedPoints(charVarcharCoercion, type, reference, range, singleValuesInRange));
                continue;
            }

            disjuncts.add(processRange(charVarcharCoercion, type, range, reference));
        }

        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            disjuncts.add(comparison(metadata, charVarcharCoercion, EQUAL, reference, getOnlyElement(singleValues)));
        }
        else if (singleValues.size() > 1) {
            disjuncts.add(new In(reference, singleValues));
        }
        return disjuncts;
    }

    private List<Expression> extractDisjuncts(CharVarcharCoercion charVarcharCoercion, Type type, DiscreteValues discreteValues, Expression reference)
    {
        List<Expression> values = discreteValues.getValues().stream()
                .map(object -> new Constant(type, object))
                .collect(toList());

        // If values is empty, then the equatableValues was either ALL or NONE, both of which should already have been checked for
        checkState(!values.isEmpty());

        Expression predicate;
        if (values.size() == 1) {
            predicate = comparison(metadata, charVarcharCoercion, EQUAL, reference, getOnlyElement(values));
        }
        else {
            predicate = new In(reference, values);
        }

        if (!discreteValues.isInclusive()) {
            predicate = not(metadata, charVarcharCoercion, predicate);
        }
        return ImmutableList.of(predicate);
    }

    private static boolean isBetween(Range range)
    {
        // inclusive implies bounded
        return range.isLowInclusive() && range.isHighInclusive();
    }
}
