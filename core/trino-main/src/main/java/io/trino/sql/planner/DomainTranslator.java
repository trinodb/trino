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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.PeekingIterator;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.OperatorNotFoundException;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.ErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.predicate.DiscreteValues;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.Ranges;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.InterpretedFunctionInvoker;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IrUtils;
import io.trino.sql.ir.IrVisitor;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.type.LikeFunctions;
import io.trino.type.LikePattern;
import io.trino.type.TypeCoercion;
import jakarta.annotation.Nullable;

import java.lang.invoke.MethodHandle;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterators.peekingIterator;
import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.airlift.slice.SliceUtf8.getCodePointAt;
import static io.airlift.slice.SliceUtf8.lengthOfCodePoint;
import static io.airlift.slice.SliceUtf8.setCodePointAt;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.function.OperatorType.SATURATED_FLOOR_CAST;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TypeUtils.isFloatingPointNaN;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.IDENTICAL;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.NOT_EQUAL;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.ir.IrUtils.combineDisjunctsWithDefault;
import static io.trino.sql.ir.IrUtils.or;
import static io.trino.type.LikeFunctions.LIKE_FUNCTION_NAME;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class DomainTranslator
{
    private final Metadata metadata;

    public DomainTranslator(Metadata metadata)
    {
        this.metadata = metadata;
    }

    public Expression toPredicate(TupleDomain<Symbol> tupleDomain)
    {
        return IrUtils.combineConjuncts(toPredicateConjuncts(tupleDomain));
    }

    private List<Expression> toPredicateConjuncts(TupleDomain<Symbol> tupleDomain)
    {
        if (tupleDomain.isNone()) {
            return ImmutableList.of(FALSE);
        }

        Map<Symbol, Domain> domains = tupleDomain.getDomains().get();
        return domains.entrySet().stream()
                .sorted(Comparator.comparing(e -> e.getKey().name()))
                .map(entry -> toPredicate(entry.getValue(), entry.getKey().toSymbolReference()))
                .collect(toImmutableList());
    }

    public Expression toPredicate(Domain domain, Reference reference)
    {
        if (domain.getValues().isNone()) {
            return domain.isNullAllowed() ? new IsNull(reference) : FALSE;
        }

        if (domain.getValues().isAll()) {
            return domain.isNullAllowed() ? TRUE : not(metadata, new IsNull(reference));
        }

        List<Expression> disjuncts = new ArrayList<>();

        // Add nullability disjuncts
        if (domain.isNullAllowed()) {
            disjuncts.add(new IsNull(reference));
        }

        disjuncts.addAll(domain.getValues().getValuesProcessor().transform(
                ranges -> extractDisjuncts(domain.getType(), ranges, reference),
                discreteValues -> extractDisjuncts(domain.getType(), discreteValues, reference),
                allOrNone -> {
                    throw new IllegalStateException("Case should not be reachable");
                }));

        return combineDisjunctsWithDefault(disjuncts, TRUE);
    }

    private static Expression processRange(Type type, Range range, Reference reference)
    {
        if (range.isAll()) {
            return TRUE;
        }

        if (isBetween(range)) {
            // specialize the range with BETWEEN expression if possible b/c it is currently more efficient
            return new Between(
                    reference,
                    new Constant(type, range.getLowBoundedValue()),
                    new Constant(type, range.getHighBoundedValue()));
        }

        List<Expression> rangeConjuncts = new ArrayList<>();
        if (!range.isLowUnbounded()) {
            rangeConjuncts.add(new Comparison(
                    range.isLowInclusive() ? GREATER_THAN_OR_EQUAL : GREATER_THAN,
                    reference,
                    new Constant(type, range.getLowBoundedValue())));
        }
        if (!range.isHighUnbounded()) {
            rangeConjuncts.add(new Comparison(
                    range.isHighInclusive() ? LESS_THAN_OR_EQUAL : LESS_THAN,
                    reference,
                    new Constant(type, range.getHighBoundedValue())));
        }
        // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
        checkState(!rangeConjuncts.isEmpty());
        return combineConjuncts(rangeConjuncts);
    }

    private Expression combineRangeWithExcludedPoints(Type type, Reference reference, Range range, List<Expression> excludedPoints)
    {
        if (excludedPoints.isEmpty()) {
            return processRange(type, range, reference);
        }

        Expression excludedPointsExpression = not(metadata, new In(reference, excludedPoints));
        if (excludedPoints.size() == 1) {
            excludedPointsExpression = new Comparison(NOT_EQUAL, reference, getOnlyElement(excludedPoints));
        }

        return combineConjuncts(processRange(type, range, reference), excludedPointsExpression);
    }

    private List<Expression> extractDisjuncts(Type type, Ranges ranges, Reference reference)
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
        if (type instanceof RealType || type instanceof DoubleType) {
            boolean originalRangeIsAll = orderedRanges.stream().anyMatch(Range::isAll);
            boolean coalescedRangeIsAll = originalUnionSingleValues.stream().anyMatch(Range::isAll);
            if (!originalRangeIsAll && coalescedRangeIsAll) {
                for (Range range : orderedRanges) {
                    disjuncts.add(processRange(type, range, reference));
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
                disjuncts.add(combineRangeWithExcludedPoints(type, reference, range, singleValuesInRange));
                continue;
            }

            disjuncts.add(processRange(type, range, reference));
        }

        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            disjuncts.add(new Comparison(EQUAL, reference, getOnlyElement(singleValues)));
        }
        else if (singleValues.size() > 1) {
            disjuncts.add(new In(reference, singleValues));
        }
        return disjuncts;
    }

    private List<Expression> extractDisjuncts(Type type, DiscreteValues discreteValues, Reference reference)
    {
        List<Expression> values = discreteValues.getValues().stream()
                .map(object -> new Constant(type, object))
                .collect(toList());

        // If values is empty, then the equatableValues was either ALL or NONE, both of which should already have been checked for
        checkState(!values.isEmpty());

        Expression predicate;
        if (values.size() == 1) {
            predicate = new Comparison(EQUAL, reference, getOnlyElement(values));
        }
        else {
            predicate = new In(reference, values);
        }

        if (!discreteValues.isInclusive()) {
            predicate = not(metadata, predicate);
        }
        return ImmutableList.of(predicate);
    }

    private static boolean isBetween(Range range)
    {
        // inclusive implies bounded
        return range.isLowInclusive() && range.isHighInclusive();
    }

    /**
     * Convert an Expression predicate into an ExtractionResult consisting of:
     * 1) A successfully extracted TupleDomain
     * 2) An Expression fragment which represents the part of the original Expression that will need to be re-evaluated
     * after filtering with the TupleDomain.
     */
    public static ExtractionResult getExtractionResult(PlannerContext plannerContext, Session session, Expression predicate)
    {
        // This is a limited type analyzer for the simple expressions used in this method
        return new Visitor(plannerContext, session).process(predicate, false);
    }

    private static class Visitor
            extends IrVisitor<ExtractionResult, Boolean>
    {
        private final PlannerContext plannerContext;
        private final Session session;
        private final InterpretedFunctionInvoker functionInvoker;
        private final TypeCoercion typeCoercion;

        private Visitor(PlannerContext plannerContext, Session session)
        {
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.session = requireNonNull(session, "session is null");
            this.functionInvoker = new InterpretedFunctionInvoker(plannerContext.getFunctionManager());
            this.typeCoercion = new TypeCoercion(plannerContext.getTypeManager()::getType);
        }

        private static ValueSet complementIfNecessary(ValueSet valueSet, boolean complement)
        {
            return complement ? valueSet.complement() : valueSet;
        }

        private static Domain complementIfNecessary(Domain domain, boolean complement)
        {
            return complement ? domain.complement() : domain;
        }

        private Expression complementIfNecessary(Expression expression, boolean complement)
        {
            return complement ? not(plannerContext.getMetadata(), expression) : expression;
        }

        @Override
        protected ExtractionResult visitExpression(Expression node, Boolean complement)
        {
            // If we don't know how to process this node, the default response is to say that the TupleDomain is "all"
            return new ExtractionResult(TupleDomain.all(), complementIfNecessary(node, complement));
        }

        @Override
        protected ExtractionResult visitLogical(Logical node, Boolean complement)
        {
            List<ExtractionResult> results = node.terms().stream()
                    .map(term -> process(term, complement))
                    .collect(toImmutableList());

            List<TupleDomain<Symbol>> tupleDomains = results.stream()
                    .map(ExtractionResult::getTupleDomain)
                    .collect(toImmutableList());

            List<Expression> residuals = results.stream()
                    .map(ExtractionResult::getRemainingExpression)
                    .collect(toImmutableList());

            Logical.Operator operator = complement ? node.operator().flip() : node.operator();
            switch (operator) {
                case AND:
                    return new ExtractionResult(
                            TupleDomain.intersect(tupleDomains),
                            combineConjuncts(residuals));

                case OR:
                    TupleDomain<Symbol> columnUnionedTupleDomain = TupleDomain.columnWiseUnion(tupleDomains);

                    // In most cases, the columnUnionedTupleDomain is only a superset of the actual strict union
                    // and so we can return the current node as the remainingExpression so that all bounds will be double checked again at execution time.
                    Expression remainingExpression = complementIfNecessary(node, complement);

                    // However, there are a few cases where the column-wise union is actually equivalent to the strict union, so we if can detect
                    // some of these cases, we won't have to double check the bounds unnecessarily at execution time.

                    // We can only make inferences if the remaining expressions on all terms are equal and deterministic
                    if (Set.copyOf(residuals).size() == 1 && DeterminismEvaluator.isDeterministic(residuals.get(0))) {
                        // NONE are no-op for the purpose of OR
                        tupleDomains = tupleDomains.stream()
                                .filter(domain -> !domain.isNone())
                                .collect(toList());

                        // The column-wise union is equivalent to the strict union if
                        // 1) If all TupleDomains consist of the same exact single column (e.g. one TupleDomain => (a > 0), another TupleDomain => (a < 10))
                        // 2) If one TupleDomain is a superset of the others (e.g. TupleDomain => (a > 0, b > 0 && b < 10) vs TupleDomain => (a > 5, b = 5))
                        boolean matchingSingleSymbolDomains = tupleDomains.stream().allMatch(domain -> domain.getDomains().get().size() == 1);

                        matchingSingleSymbolDomains = matchingSingleSymbolDomains && tupleDomains.stream()
                                .map(tupleDomain -> tupleDomain.getDomains().get().keySet())
                                .distinct()
                                .count() == 1;

                        boolean oneTermIsSuperSet = TupleDomain.maximal(tupleDomains).isPresent();

                        if (oneTermIsSuperSet) {
                            remainingExpression = residuals.get(0);
                        }
                        else if (matchingSingleSymbolDomains) {
                            // Types REAL and DOUBLE require special handling because they include NaN value. In this case, we cannot rely on the union of domains.
                            // That is because domains covering the value set partially might union up to a domain covering the whole value set.
                            // While the component domains didn't include NaN, the resulting domain could be further translated to predicate "TRUE" or "a IS NOT NULL",
                            // which is satisfied by NaN. So during domain union, NaN might be implicitly added.
                            // Example: Let 'a' be a column of type DOUBLE.
                            //          Let left TupleDomain => (a > 0) /false for NaN/, right TupleDomain => (a < 10) /false for NaN/.
                            //          Unioned TupleDomain => "is not null" /true for NaN/
                            // To guard against wrong results, the current node is returned as the remainingExpression.
                            Type type = getOnlyElement(tupleDomains.get(0).getDomains().get().values()).getType();

                            // A Domain of a floating point type contains NaN in the following cases:
                            // 1. When it contains all the values of the type and null.
                            //    In such case the domain is 'all', and if it is the only domain
                            //    in the TupleDomain, the TupleDomain gets normalized to TupleDomain 'all'.
                            // 2. When it contains all the values of the type and doesn't contain null.
                            //    In such case no normalization on the level of TupleDomain takes place,
                            //    and the check for NaN is done by inspecting the Domain's valueSet.
                            //    NaN is included when the valueSet is 'all'.
                            boolean unionedDomainContainsNaN = columnUnionedTupleDomain.isAll() ||
                                    (columnUnionedTupleDomain.getDomains().isPresent() &&
                                            getOnlyElement(columnUnionedTupleDomain.getDomains().get().values()).getValues().isAll());
                            boolean implicitlyAddedNaN = (type instanceof RealType || type instanceof DoubleType) &&
                                    tupleDomains.stream().noneMatch(TupleDomain::isAll) &&
                                    unionedDomainContainsNaN;
                            if (!implicitlyAddedNaN) {
                                remainingExpression = residuals.get(0);
                            }
                        }
                    }

                    return new ExtractionResult(columnUnionedTupleDomain, remainingExpression);
            }
            throw new AssertionError("Unknown operator: " + node.operator());
        }

        @Override
        protected ExtractionResult visitReference(Reference node, Boolean complement)
        {
            if (node.type().equals(BOOLEAN)) {
                Comparison newNode = new Comparison(EQUAL, node, TRUE);
                return visitComparison(newNode, complement);
            }

            return visitExpression(node, complement);
        }

        @Override
        protected ExtractionResult visitComparison(Comparison node, Boolean complement)
        {
            Optional<NormalizedSimpleComparison> optionalNormalized = toNormalizedSimpleComparison(node);
            if (optionalNormalized.isEmpty()) {
                return super.visitComparison(node, complement);
            }
            NormalizedSimpleComparison normalized = optionalNormalized.get();

            Expression symbolExpression = normalized.getSymbolExpression();
            if (symbolExpression instanceof Reference) {
                Symbol symbol = Symbol.from(symbolExpression);
                NullableValue value = normalized.getValue();
                Type type = value.getType(); // common type for symbol and value
                return createComparisonExtractionResult(normalized.getComparisonOperator(), symbol, type, value.getValue(), complement)
                        .orElseGet(() -> super.visitComparison(node, complement));
            }
            if (symbolExpression instanceof Cast castExpression) {
                // type of expression which is then cast to type of value
                Type castSourceType = castExpression.expression().type();
                Type castTargetType = castExpression.type();
                if (castSourceType instanceof VarcharType varcharType && castTargetType == DATE) {
                    Optional<ExtractionResult> result = createVarcharCastToDateComparisonExtractionResult(
                            normalized,
                            varcharType,
                            complement,
                            node);
                    if (result.isPresent()) {
                        return result.get();
                    }
                }
                if (!isImplicitCoercion(castExpression)) {
                    //
                    // we cannot use non-coercion cast to literal_type on symbol side to build tuple domain
                    //
                    // example which illustrates the problem:
                    //
                    // let t be of timestamp type:
                    //
                    // and expression be:
                    // cast(t as date) == date_literal
                    //
                    // after dropping cast we end up with:
                    //
                    // t == date_literal
                    //
                    // if we build tuple domain based coercion of date_literal to timestamp type we would
                    // end up with tuple domain with just one time point (cast(date_literal as timestamp).
                    // While we need range which maps to single date pointed by date_literal.
                    //
                    return super.visitComparison(node, complement);
                }

                // we use saturated floor cast value -> castSourceType to rewrite original expression to new one with one cast peeled off the symbol side
                Optional<Expression> coercedExpression = coerceComparisonWithRounding(
                        castSourceType, castExpression.expression(), normalized.getValue(), normalized.getComparisonOperator());

                if (coercedExpression.isPresent()) {
                    return process(coercedExpression.get(), complement);
                }

                return super.visitComparison(node, complement);
            }
            return super.visitComparison(node, complement);
        }

        /**
         * Extract a normalized simple comparison between a QualifiedNameReference and a native value if possible.
         */
        private Optional<NormalizedSimpleComparison> toNormalizedSimpleComparison(Comparison comparison)
        {
            Expression left = comparison.left();
            Expression right = comparison.right();

            if (left instanceof Constant == right instanceof Constant) {
                // One of the terms must be a constant and the other a non-constant
                return Optional.empty();
            }

            if (left instanceof Constant constant) {
                return Optional.of(new NormalizedSimpleComparison(right, comparison.operator().flip(), new NullableValue(left.type(), constant.value())));
            }
            else {
                return Optional.of(new NormalizedSimpleComparison(left, comparison.operator(), new NullableValue(right.type(), ((Constant) right).value())));
            }
        }

        private boolean isImplicitCoercion(Cast cast)
        {
            return typeCoercion.canCoerce(cast.expression().type(), cast.type());
        }

        private Optional<ExtractionResult> createVarcharCastToDateComparisonExtractionResult(
                NormalizedSimpleComparison comparison,
                VarcharType sourceType,
                boolean complement,
                Comparison originalExpression)
        {
            Expression sourceExpression = ((Cast) comparison.getSymbolExpression()).expression();
            Comparison.Operator operator = comparison.getComparisonOperator();
            NullableValue value = comparison.getValue();

            if (complement || value.isNull()) {
                return Optional.empty();
            }
            if (!(sourceExpression instanceof Reference)) {
                // Calculation is not useful
                return Optional.empty();
            }
            Symbol sourceSymbol = Symbol.from(sourceExpression);

            if (!sourceType.isUnbounded() && sourceType.getBoundedLength() < 10) {
                // too short
                return Optional.empty();
            }

            LocalDate date = LocalDate.ofEpochDay((long) value.getValue());
            if (date.getYear() < 1001 || date.getYear() > 9998) {
                // Edge cases. 1-year margin so that we can go to next/prev year for < or > comparisons
                return Optional.empty();
            }

            // superset of possible values, for the "normal case"
            ValueSet valueSet;
            boolean nullAllowed = false;

            switch (operator) {
                case EQUAL, IDENTICAL:
                    valueSet = dateStringRanges(date, sourceType);
                    nullAllowed = operator == IDENTICAL;
                    break;
                case NOT_EQUAL:
                    if (date.getDayOfMonth() < 10) {
                        // TODO: possible to handle but cumbersome
                        return Optional.empty();
                    }
                    valueSet = ValueSet.all(sourceType).subtract(dateStringRanges(date, sourceType));
                    break;
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                    valueSet = ValueSet.ofRanges(Range.lessThan(sourceType, utf8Slice(Integer.toString(date.getYear() + 1))));
                    break;
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                    valueSet = ValueSet.ofRanges(Range.greaterThan(sourceType, utf8Slice(Integer.toString(date.getYear() - 1))));
                    break;
                default:
                    return Optional.empty();
            }

            // Date representations starting with whitespace, sign or leading zeroes.
            valueSet = valueSet.union(ValueSet.ofRanges(
                    Range.lessThan(sourceType, utf8Slice("1")),
                    Range.greaterThan(sourceType, utf8Slice("9"))));

            return Optional.of(new ExtractionResult(
                    TupleDomain.withColumnDomains(ImmutableMap.of(sourceSymbol, Domain.create(valueSet, nullAllowed))),
                    originalExpression));
        }

        /**
         * @return Date representations of the form 2005-09-09, 2005-09-9, 2005-9-09 and 2005-9-9 expanded to ranges:
         * {@code [2005-09-09, 2005-09-0:), [2005-09-9, 2005-09-:), [2005-9-09, 2005-9-0:), [2005-9-9, 2005-9-:)}
         * (the {@code :} character is the next one after {@code 9}).
         */
        private static SortedRangeSet dateStringRanges(LocalDate date, VarcharType domainType)
        {
            checkArgument(date.getYear() >= 1000 && date.getYear() <= 9999, "Unsupported date: %s", date);

            int month = date.getMonthValue();
            int day = date.getDayOfMonth();
            boolean isMonthSingleDigit = date.getMonthValue() < 10;
            boolean isDaySingleDigit = date.getDayOfMonth() < 10;

            // A specific date value like 2005-09-10 can be a result of a CAST for number of various forms,
            // as the value can have optional sign, leading zeros for the year, and surrounding whitespace,
            // E.g. '  +002005-9-9  '.

            List<Range> valueRanges = new ArrayList<>(4);
            for (boolean useSingleDigitMonth : List.of(true, false)) {
                for (boolean useSingleDigitDay : List.of(true, false)) {
                    if (useSingleDigitMonth && !isMonthSingleDigit) {
                        continue;
                    }
                    if (useSingleDigitDay && !isDaySingleDigit) {
                        continue;
                    }
                    String dateString = date.getYear() +
                            ((!useSingleDigitMonth && isMonthSingleDigit) ? "-0" : "-") + month +
                            ((!useSingleDigitDay && isDaySingleDigit) ? "-0" : "-") + day;
                    String nextStringPrefix = dateString.substring(0, dateString.length() - 1) + (char) (dateString.charAt(dateString.length() - 1) + 1); // cannot overflow
                    verify(dateString.length() <= domainType.getLength().orElse(Integer.MAX_VALUE), "dateString length exceeds type bounds");
                    verify(dateString.length() == nextStringPrefix.length(), "Next string length mismatch");
                    valueRanges.add(Range.range(domainType, utf8Slice(dateString), true, utf8Slice(nextStringPrefix), false));
                }
            }
            return (SortedRangeSet) ValueSet.ofRanges(valueRanges);
        }

        private static Optional<ExtractionResult> createComparisonExtractionResult(Comparison.Operator comparisonOperator, Symbol column, Type type, @Nullable Object value, boolean complement)
        {
            if (value == null) {
                return switch (comparisonOperator) {
                    case EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, NOT_EQUAL -> Optional.of(new ExtractionResult(TupleDomain.none(), TRUE));
                    case IDENTICAL -> {
                        Domain domain = complementIfNecessary(Domain.onlyNull(type), complement);
                        yield Optional.of(new ExtractionResult(
                                TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)),
                                TRUE));
                    }
                };
            }
            if (type.isOrderable()) {
                return extractOrderableDomain(comparisonOperator, type, value, complement)
                        .map(domain -> new ExtractionResult(TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)), TRUE));
            }
            if (type.isComparable()) {
                Domain domain = extractEquatableDomain(comparisonOperator, type, value, complement);
                return Optional.of(new ExtractionResult(
                        TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)),
                        TRUE));
            }
            throw new AssertionError("Type cannot be used in a comparison expression (should have been caught in analysis): " + type);
        }

        private static Optional<Domain> extractOrderableDomain(Comparison.Operator comparisonOperator, Type type, Object value, boolean complement)
        {
            checkArgument(value != null);

            // Handle orderable types which do not have NaN.
            if (!(type instanceof DoubleType) && !(type instanceof RealType)) {
                return switch (comparisonOperator) {
                    case EQUAL -> Optional.of(Domain.create(complementIfNecessary(ValueSet.ofRanges(Range.equal(type, value)), complement), false));
                    case IDENTICAL -> Optional.of(Domain.create(complementIfNecessary(ValueSet.ofRanges(Range.equal(type, value)), complement), complement));
                    case GREATER_THAN -> Optional.of(Domain.create(complementIfNecessary(ValueSet.ofRanges(Range.greaterThan(type, value)), complement), false));
                    case GREATER_THAN_OR_EQUAL -> Optional.of(Domain.create(complementIfNecessary(ValueSet.ofRanges(Range.greaterThanOrEqual(type, value)), complement), false));
                    case LESS_THAN -> Optional.of(Domain.create(complementIfNecessary(ValueSet.ofRanges(Range.lessThan(type, value)), complement), false));
                    case LESS_THAN_OR_EQUAL -> Optional.of(Domain.create(complementIfNecessary(ValueSet.ofRanges(Range.lessThanOrEqual(type, value)), complement), false));
                    case NOT_EQUAL -> Optional.of(Domain.create(complementIfNecessary(ValueSet.ofRanges(Range.lessThan(type, value), Range.greaterThan(type, value)), complement), false));
                };
            }

            // Handle comparisons against NaN
            if (isFloatingPointNaN(type, value)) {
                return switch (comparisonOperator) {
                    case EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL ->
                            Optional.of(Domain.create(complementIfNecessary(ValueSet.none(type), complement), false));
                    case NOT_EQUAL -> Optional.of(Domain.create(complementIfNecessary(ValueSet.all(type), complement), false));
                    case IDENTICAL -> Optional.empty(); // The Domain should be "NaN". It is currently not supported.
                };
            }

            // Handle comparisons against a non-NaN value when the compared value might be NaN
            return switch (comparisonOperator) {
                /*
                 For comparison operators: EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL,
                 the Domain should not contain NaN, but complemented Domain should contain NaN. It is currently not supported.
                 Currently, NaN is only included when ValueSet.isAll().

                 For comparison operators: NOT_EQUAL, IS_DISTINCT_FROM,
                 the Domain should consist of ranges (which do not sum to the whole ValueSet), and NaN.
                 Currently, NaN is only included when ValueSet.isAll().
                  */
                case EQUAL, IDENTICAL -> complement ?
                        Optional.empty() :
                        Optional.of(Domain.create(ValueSet.ofRanges(Range.equal(type, value)), false));
                case GREATER_THAN -> complement ?
                        Optional.empty() :
                        Optional.of(Domain.create(ValueSet.ofRanges(Range.greaterThan(type, value)), false));
                case GREATER_THAN_OR_EQUAL -> complement ?
                        Optional.empty() :
                        Optional.of(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, value)), false));
                case LESS_THAN -> complement ?
                        Optional.empty() :
                        Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(type, value)), false));
                case LESS_THAN_OR_EQUAL -> complement ?
                        Optional.empty() :
                        Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(type, value)), false));
                case NOT_EQUAL -> complement ?
                        Optional.of(Domain.create(ValueSet.ofRanges(Range.equal(type, value)), false)) :
                        Optional.empty();
            };
        }

        private static Domain extractEquatableDomain(Comparison.Operator comparisonOperator, Type type, Object value, boolean complement)
        {
            checkArgument(value != null);
            return switch (comparisonOperator) {
                case EQUAL -> Domain.create(complementIfNecessary(ValueSet.of(type, value), complement), false);
                case NOT_EQUAL -> Domain.create(complementIfNecessary(ValueSet.of(type, value).complement(), complement), false);
                case IDENTICAL -> Domain.create(complementIfNecessary(ValueSet.of(type, value), complement), complement);
                default -> throw new IllegalArgumentException("Unhandled operator: " + comparisonOperator);
            };
        }

        private Optional<Expression> coerceComparisonWithRounding(
                Type symbolExpressionType,
                Expression symbolExpression,
                NullableValue nullableValue,
                Comparison.Operator comparisonOperator)
        {
            requireNonNull(nullableValue, "nullableValue is null");
            if (nullableValue.isNull()) {
                return Optional.empty();
            }
            Type valueType = nullableValue.getType();
            Object value = nullableValue.getValue();
            Optional<Object> floorValueOptional;
            try {
                floorValueOptional = floorValue(valueType, symbolExpressionType, value);
            }
            catch (TrinoException e) {
                ErrorCode errorCode = e.getErrorCode();
                if (INVALID_CAST_ARGUMENT.toErrorCode().equals(errorCode)) {
                    // There's no such value at symbolExpressionType
                    return Optional.of(FALSE);
                }
                throw e;
            }
            return floorValueOptional.map(floorValue -> rewriteComparisonExpression(symbolExpressionType, symbolExpression, valueType, value, floorValue, comparisonOperator));
        }

        private Expression rewriteComparisonExpression(
                Type symbolExpressionType,
                Expression symbolExpression,
                Type valueType,
                Object originalValue,
                Object coercedValue,
                Comparison.Operator comparisonOperator)
        {
            int originalComparedToCoerced = compareOriginalValueToCoerced(valueType, originalValue, symbolExpressionType, coercedValue);
            boolean coercedValueIsEqualToOriginal = originalComparedToCoerced == 0;
            boolean coercedValueIsLessThanOriginal = originalComparedToCoerced > 0;
            boolean coercedValueIsGreaterThanOriginal = originalComparedToCoerced < 0;
            Expression coercedLiteral = new Constant(symbolExpressionType, coercedValue);

            return switch (comparisonOperator) {
                case GREATER_THAN_OR_EQUAL, GREATER_THAN -> {
                    if (coercedValueIsGreaterThanOriginal) {
                        yield new Comparison(GREATER_THAN_OR_EQUAL, symbolExpression, coercedLiteral);
                    }
                    if (coercedValueIsEqualToOriginal) {
                        yield new Comparison(comparisonOperator, symbolExpression, coercedLiteral);
                    }
                    if (coercedValueIsLessThanOriginal) {
                        yield new Comparison(GREATER_THAN, symbolExpression, coercedLiteral);
                    }
                    throw new AssertionError("Unreachable");
                }
                case LESS_THAN_OR_EQUAL, LESS_THAN -> {
                    if (coercedValueIsLessThanOriginal) {
                        yield new Comparison(LESS_THAN_OR_EQUAL, symbolExpression, coercedLiteral);
                    }
                    if (coercedValueIsEqualToOriginal) {
                        yield new Comparison(comparisonOperator, symbolExpression, coercedLiteral);
                    }
                    if (coercedValueIsGreaterThanOriginal) {
                        yield new Comparison(LESS_THAN, symbolExpression, coercedLiteral);
                    }
                    throw new AssertionError("Unreachable");
                }
                case EQUAL -> {
                    if (coercedValueIsEqualToOriginal) {
                        yield new Comparison(EQUAL, symbolExpression, coercedLiteral);
                    }
                    // Return something that is false for all non-null values
                    yield and(new Comparison(GREATER_THAN, symbolExpression, coercedLiteral),
                            new Comparison(LESS_THAN, symbolExpression, coercedLiteral));
                }
                case NOT_EQUAL -> {
                    if (coercedValueIsEqualToOriginal) {
                        yield new Comparison(comparisonOperator, symbolExpression, coercedLiteral);
                    }
                    // Return something that is true for all non-null values
                    yield or(new Comparison(EQUAL, symbolExpression, coercedLiteral),
                            new Comparison(NOT_EQUAL, symbolExpression, coercedLiteral));
                }
                case IDENTICAL -> coercedValueIsEqualToOriginal ?
                        TRUE :
                        new Comparison(comparisonOperator, symbolExpression, coercedLiteral);
            };
        }

        private Optional<Object> floorValue(Type fromType, Type toType, Object value)
        {
            return getSaturatedFloorCastOperator(fromType, toType)
                    .map(operator -> functionInvoker.invoke(operator, session.toConnectorSession(), value));
        }

        private Optional<ResolvedFunction> getSaturatedFloorCastOperator(Type fromType, Type toType)
        {
            try {
                return Optional.of(plannerContext.getMetadata().getCoercion(SATURATED_FLOOR_CAST, fromType, toType));
            }
            catch (OperatorNotFoundException e) {
                return Optional.empty();
            }
        }

        private int compareOriginalValueToCoerced(Type originalValueType, Object originalValue, Type coercedValueType, Object coercedValue)
        {
            requireNonNull(originalValueType, "originalValueType is null");
            requireNonNull(coercedValue, "coercedValue is null");
            ResolvedFunction castToOriginalTypeOperator = plannerContext.getMetadata().getCoercion(coercedValueType, originalValueType);
            Object coercedValueInOriginalType = functionInvoker.invoke(castToOriginalTypeOperator, session.toConnectorSession(), coercedValue);
            // choice of placing unordered values first or last does not matter for this code
            MethodHandle comparisonOperator = plannerContext.getTypeOperators().getComparisonUnorderedLastOperator(originalValueType, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL));
            try {
                return (int) (long) comparisonOperator.invoke(originalValue, coercedValueInOriginalType);
            }
            catch (Throwable throwable) {
                Throwables.throwIfUnchecked(throwable);
                throw new TrinoException(GENERIC_INTERNAL_ERROR, throwable);
            }
        }

        @Override
        protected ExtractionResult visitIn(In node, Boolean complement)
        {
            checkState(!node.valueList().isEmpty(), "InListExpression should never be empty");

            Optional<ExtractionResult> directExtractionResult = processSimpleInPredicate(node, complement);
            if (directExtractionResult.isPresent()) {
                return directExtractionResult.get();
            }

            ImmutableList.Builder<Expression> disjuncts = ImmutableList.builder();
            for (Expression expression : node.valueList()) {
                disjuncts.add(new Comparison(EQUAL, node.value(), expression));
            }
            ExtractionResult extractionResult = process(or(disjuncts.build()), complement);

            // preserve original IN predicate as remaining predicate
            if (extractionResult.tupleDomain.isAll()) {
                Expression originalPredicate = node;
                if (complement) {
                    originalPredicate = not(plannerContext.getMetadata(), originalPredicate);
                }
                return new ExtractionResult(extractionResult.tupleDomain, originalPredicate);
            }
            return extractionResult;
        }

        private Optional<ExtractionResult> processSimpleInPredicate(In node, Boolean complement)
        {
            if (!(node.value() instanceof Reference)) {
                return Optional.empty();
            }
            Symbol symbol = Symbol.from(node.value());
            Type type = node.value().type();
            List<Object> inValues = new ArrayList<>(node.valueList().size());
            List<Expression> excludedExpressions = new ArrayList<>();

            for (Expression expression : node.valueList()) {
                if (expression instanceof Constant constant) {
                    if (constant.value() == null) {
                        if (complement) {
                            // NOT IN is equivalent to NOT(s eq v1) AND NOT(s eq v2). When any right value is NULL, the comparison result is NULL, so AND's result can be at most
                            // NULL (effectively false in predicate context)
                            return Optional.of(new ExtractionResult(TupleDomain.none(), TRUE));
                        }
                        // in case of IN, NULL on the right results with NULL comparison result (effectively false in predicate context), so can be ignored, as the
                        // comparison results are OR-ed
                    }
                    else if (type instanceof RealType || type instanceof DoubleType) {
                        // NaN can be ignored: it always compares to false, as if it was not among IN's values
                        if (!isFloatingPointNaN(type, constant.value())) {
                            if (complement) {
                                // in case of NOT IN with floating point, the NaN on the left passes the test (unless a NULL is found, and we exited earlier)
                                // but this cannot currently be described with a Domain other than Domain.all
                                excludedExpressions.add(expression);
                            }
                            else {
                                inValues.add(constant.value());
                            }
                        }
                    }
                    else {
                        inValues.add(constant.value());
                    }
                }
                else {
                    if (!complement) {
                        // in case of IN, expression on the right side prevents determining the domain: any lhs value can be eligible
                        return Optional.of(new ExtractionResult(TupleDomain.all(), node));
                    }
                    // in case of NOT IN, expression on the right side still allows determining values that are *not* part of the final domain
                    excludedExpressions.add(expression);
                }
            }

            ValueSet valueSet = ValueSet.copyOf(type, inValues);
            if (complement) {
                valueSet = valueSet.complement();
            }
            TupleDomain<Symbol> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(symbol, Domain.create(valueSet, false)));

            Expression remainingExpression;
            if (excludedExpressions.isEmpty()) {
                remainingExpression = TRUE;
            }
            else if (excludedExpressions.size() == 1) {
                remainingExpression = not(plannerContext.getMetadata(), new Comparison(EQUAL, node.value(), getOnlyElement(excludedExpressions)));
            }
            else {
                remainingExpression = not(plannerContext.getMetadata(), new In(node.value(), excludedExpressions));
            }

            return Optional.of(new ExtractionResult(tupleDomain, remainingExpression));
        }

        @Override
        protected ExtractionResult visitBetween(Between node, Boolean complement)
        {
            // Re-write as two comparison expressions
            return process(and(
                    new Comparison(GREATER_THAN_OR_EQUAL, node.value(), node.min()),
                    new Comparison(LESS_THAN_OR_EQUAL, node.value(), node.max())), complement);
        }

        private Optional<ExtractionResult> tryVisitLikeFunction(Call node, Boolean complement)
        {
            Expression value = node.arguments().get(0);
            Expression patternArgument = node.arguments().get(1);

            if (!(value instanceof Reference)) {
                // LIKE not on a symbol
                return Optional.empty();
            }

            Type type = value.type();
            if (!(type instanceof VarcharType varcharType)) {
                // TODO support CharType
                return Optional.empty();
            }

            Symbol symbol = Symbol.from(value);

            if (node.arguments().size() > 2 || !(patternArgument instanceof Constant patternConstant)) {
                // dynamic pattern or escape
                return Optional.empty();
            }

            LikePattern matcher = (LikePattern) patternConstant.value();

            Slice pattern = utf8Slice(matcher.getPattern());
            Optional<Slice> escape = matcher.getEscape()
                    .map(character -> Slices.utf8Slice(character.toString()));

            int patternConstantPrefixBytes = LikeFunctions.patternConstantPrefixBytes(pattern, escape);
            if (patternConstantPrefixBytes == pattern.length()) {
                // This should not actually happen, constant LIKE pattern should be converted to equality predicate before DomainTranslator is invoked.

                Slice literal = LikeFunctions.unescapeLiteralLikePattern(pattern, escape);
                ValueSet valueSet;
                if (varcharType.isUnbounded() || countCodePoints(literal) <= varcharType.getBoundedLength()) {
                    valueSet = ValueSet.of(type, literal);
                }
                else {
                    // impossible to satisfy
                    valueSet = ValueSet.none(type);
                }
                Domain domain = Domain.create(complementIfNecessary(valueSet, complement), false);
                return Optional.of(new ExtractionResult(TupleDomain.withColumnDomains(ImmutableMap.of(symbol, domain)), TRUE));
            }

            if (complement || patternConstantPrefixBytes == 0) {
                // TODO
                return Optional.empty();
            }

            Slice constantPrefix = LikeFunctions.unescapeLiteralLikePattern(pattern.slice(0, patternConstantPrefixBytes), escape);
            return createRangeDomain(type, constantPrefix).map(domain -> new ExtractionResult(TupleDomain.withColumnDomains(ImmutableMap.of(symbol, domain)), node));
        }

        @Override
        protected ExtractionResult visitCall(Call node, Boolean complement)
        {
            CatalogSchemaFunctionName name = node.function().name();
            if (name.equals(builtinFunctionName("starts_with"))) {
                Optional<ExtractionResult> result = tryVisitStartsWithFunction(node, complement);
                if (result.isPresent()) {
                    return result.get();
                }
            }
            else if (name.equals(builtinFunctionName(LIKE_FUNCTION_NAME))) {
                Optional<ExtractionResult> result = tryVisitLikeFunction(node, complement);
                if (result.isPresent()) {
                    return result.get();
                }
            }
            else if (name.equals(builtinFunctionName("$not"))) {
                return process(node.arguments().getFirst(), !complement);
            }
            return visitExpression(node, complement);
        }

        private Optional<ExtractionResult> tryVisitStartsWithFunction(Call node, Boolean complement)
        {
            List<Expression> args = node.arguments();
            if (args.size() != 2) {
                return Optional.empty();
            }

            Expression target = args.get(0);
            if (!(target instanceof Reference)) {
                // Target is not a symbol
                return Optional.empty();
            }

            Expression prefix = args.get(1);
            if (!(prefix instanceof Constant literal && literal.type().equals(VarcharType.VARCHAR))) {
                // dynamic pattern
                return Optional.empty();
            }

            Type type = target.type();
            if (!(type instanceof VarcharType)) {
                // TODO support CharType
                return Optional.empty();
            }
            if (complement) {
                return Optional.empty();
            }

            Symbol symbol = Symbol.from(target);
            Slice constantPrefix = (Slice) literal.value();

            return createRangeDomain(type, constantPrefix).map(domain -> new ExtractionResult(TupleDomain.withColumnDomains(ImmutableMap.of(symbol, domain)), node));
        }

        private Optional<Domain> createRangeDomain(Type type, Slice constantPrefix)
        {
            int lastIncrementable = -1;
            for (int position = 0; position < constantPrefix.length(); position += lengthOfCodePoint(constantPrefix, position)) {
                // Get last ASCII character to increment, so that character length in bytes does not change.
                // Also prefer not to produce non-ASCII if input is all-ASCII, to be on the safe side with connectors.
                // TODO remove those limitations
                if (getCodePointAt(constantPrefix, position) < 127) {
                    lastIncrementable = position;
                }
            }

            if (lastIncrementable == -1) {
                return Optional.empty();
            }

            Slice lowerBound = constantPrefix;
            Slice upperBound = constantPrefix.slice(0, lastIncrementable + lengthOfCodePoint(constantPrefix, lastIncrementable)).copy();
            setCodePointAt(getCodePointAt(constantPrefix, lastIncrementable) + 1, upperBound, lastIncrementable);

            Domain domain = Domain.create(ValueSet.ofRanges(Range.range(type, lowerBound, true, upperBound, false)), false);
            return Optional.of(domain);
        }

        @Override
        protected ExtractionResult visitIsNull(IsNull node, Boolean complement)
        {
            if (!(node.value() instanceof Reference)) {
                return super.visitIsNull(node, complement);
            }

            Symbol symbol = Symbol.from(node.value());
            Type columnType = symbol.type();
            Domain domain = complementIfNecessary(Domain.onlyNull(columnType), complement);
            return new ExtractionResult(
                    TupleDomain.withColumnDomains(ImmutableMap.of(symbol, domain)),
                    TRUE);
        }

        @Override
        protected ExtractionResult visitConstant(Constant node, Boolean complement)
        {
            if (node.value() == null) {
                return new ExtractionResult(TupleDomain.none(), TRUE);
            }

            if (node.type().equals(BOOLEAN)) {
                boolean value = (boolean) node.value();
                value = complement != value;
                return new ExtractionResult(value ? TupleDomain.all() : TupleDomain.none(), TRUE);
            }

            return super.visitConstant(node, complement);
        }
    }

    private static class NormalizedSimpleComparison
    {
        private final Expression symbolExpression;
        private final Comparison.Operator comparisonOperator;
        private final NullableValue value;

        public NormalizedSimpleComparison(Expression symbolExpression, Comparison.Operator comparisonOperator, NullableValue value)
        {
            this.symbolExpression = requireNonNull(symbolExpression, "symbolExpression is null");
            this.comparisonOperator = requireNonNull(comparisonOperator, "comparisonOperator is null");
            this.value = requireNonNull(value, "value is null");
        }

        public Expression getSymbolExpression()
        {
            return symbolExpression;
        }

        public Comparison.Operator getComparisonOperator()
        {
            return comparisonOperator;
        }

        public NullableValue getValue()
        {
            return value;
        }
    }

    public static class ExtractionResult
    {
        private final TupleDomain<Symbol> tupleDomain;
        private final Expression remainingExpression;

        public ExtractionResult(TupleDomain<Symbol> tupleDomain, Expression remainingExpression)
        {
            this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
            this.remainingExpression = requireNonNull(remainingExpression, "remainingExpression is null");
        }

        public TupleDomain<Symbol> getTupleDomain()
        {
            return tupleDomain;
        }

        public Expression getRemainingExpression()
        {
            return remainingExpression;
        }
    }
}
