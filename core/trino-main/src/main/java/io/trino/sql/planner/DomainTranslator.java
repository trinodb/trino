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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.PeekingIterator;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.metadata.AnalyzePropertyManager;
import io.trino.metadata.OperatorNotFoundException;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.SessionPropertyManager;
import io.trino.metadata.TableProceduresPropertyManager;
import io.trino.metadata.TableProceduresRegistry;
import io.trino.metadata.TablePropertyManager;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.TrinoException;
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
import io.trino.sql.analyzer.StatementAnalyzerFactory;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SymbolReference;
import io.trino.type.LikeFunctions;
import io.trino.type.TypeCoercion;

import javax.annotation.Nullable;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterators.peekingIterator;
import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.airlift.slice.SliceUtf8.getCodePointAt;
import static io.airlift.slice.SliceUtf8.lengthOfCodePoint;
import static io.airlift.slice.SliceUtf8.setCodePointAt;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.function.OperatorType.SATURATED_FLOOR_CAST;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TypeUtils.isFloatingPointNaN;
import static io.trino.sql.ExpressionUtils.and;
import static io.trino.sql.ExpressionUtils.combineConjuncts;
import static io.trino.sql.ExpressionUtils.combineDisjunctsWithDefault;
import static io.trino.sql.ExpressionUtils.or;
import static io.trino.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.NOT_EQUAL;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;

public final class DomainTranslator
{
    private final PlannerContext plannerContext;
    private final LiteralEncoder literalEncoder;

    public DomainTranslator(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.literalEncoder = new LiteralEncoder(plannerContext);
    }

    public Expression toPredicate(Session session, TupleDomain<Symbol> tupleDomain)
    {
        if (tupleDomain.isNone()) {
            return FALSE_LITERAL;
        }

        Map<Symbol, Domain> domains = tupleDomain.getDomains().get();
        return domains.entrySet().stream()
                .map(entry -> toPredicate(session, entry.getValue(), entry.getKey().toSymbolReference()))
                .collect(collectingAndThen(toImmutableList(), expressions -> combineConjuncts(plannerContext.getMetadata(), expressions)));
    }

    private Expression toPredicate(Session session, Domain domain, SymbolReference reference)
    {
        if (domain.getValues().isNone()) {
            return domain.isNullAllowed() ? new IsNullPredicate(reference) : FALSE_LITERAL;
        }

        if (domain.getValues().isAll()) {
            return domain.isNullAllowed() ? TRUE_LITERAL : new NotExpression(new IsNullPredicate(reference));
        }

        List<Expression> disjuncts = new ArrayList<>();

        disjuncts.addAll(domain.getValues().getValuesProcessor().transform(
                ranges -> extractDisjuncts(session, domain.getType(), ranges, reference),
                discreteValues -> extractDisjuncts(session, domain.getType(), discreteValues, reference),
                allOrNone -> {
                    throw new IllegalStateException("Case should not be reachable");
                }));

        // Add nullability disjuncts
        if (domain.isNullAllowed()) {
            disjuncts.add(new IsNullPredicate(reference));
        }

        return combineDisjunctsWithDefault(plannerContext.getMetadata(), disjuncts, TRUE_LITERAL);
    }

    private Expression processRange(Session session, Type type, Range range, SymbolReference reference)
    {
        if (range.isAll()) {
            return TRUE_LITERAL;
        }

        if (isBetween(range)) {
            // specialize the range with BETWEEN expression if possible b/c it is currently more efficient
            return new BetweenPredicate(
                    reference,
                    literalEncoder.toExpression(session, range.getLowBoundedValue(), type),
                    literalEncoder.toExpression(session, range.getHighBoundedValue(), type));
        }

        List<Expression> rangeConjuncts = new ArrayList<>();
        if (!range.isLowUnbounded()) {
            rangeConjuncts.add(new ComparisonExpression(
                    range.isLowInclusive() ? GREATER_THAN_OR_EQUAL : GREATER_THAN,
                    reference,
                    literalEncoder.toExpression(session, range.getLowBoundedValue(), type)));
        }
        if (!range.isHighUnbounded()) {
            rangeConjuncts.add(new ComparisonExpression(
                    range.isHighInclusive() ? LESS_THAN_OR_EQUAL : LESS_THAN,
                    reference,
                    literalEncoder.toExpression(session, range.getHighBoundedValue(), type)));
        }
        // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
        checkState(!rangeConjuncts.isEmpty());
        return combineConjuncts(plannerContext.getMetadata(), rangeConjuncts);
    }

    private Expression combineRangeWithExcludedPoints(Session session, Type type, SymbolReference reference, Range range, List<Expression> excludedPoints)
    {
        if (excludedPoints.isEmpty()) {
            return processRange(session, type, range, reference);
        }

        Expression excludedPointsExpression = new NotExpression(new InPredicate(reference, new InListExpression(excludedPoints)));
        if (excludedPoints.size() == 1) {
            excludedPointsExpression = new ComparisonExpression(NOT_EQUAL, reference, getOnlyElement(excludedPoints));
        }

        return combineConjuncts(plannerContext.getMetadata(), processRange(session, type, range, reference), excludedPointsExpression);
    }

    private List<Expression> extractDisjuncts(Session session, Type type, Ranges ranges, SymbolReference reference)
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
                    disjuncts.add(processRange(session, type, range, reference));
                }
                return disjuncts;
            }
        }

        for (Range range : originalUnionSingleValues) {
            if (range.isSingleValue()) {
                singleValues.add(literalEncoder.toExpression(session, range.getSingleValue(), type));
                continue;
            }

            // attempt to optimize ranges that can be coalesced as long as single value points are excluded
            List<Expression> singleValuesInRange = new ArrayList<>();
            while (singleValueExclusions.hasNext() && range.contains(singleValueExclusions.peek())) {
                singleValuesInRange.add(literalEncoder.toExpression(session, singleValueExclusions.next().getSingleValue(), type));
            }

            if (!singleValuesInRange.isEmpty()) {
                disjuncts.add(combineRangeWithExcludedPoints(session, type, reference, range, singleValuesInRange));
                continue;
            }

            disjuncts.add(processRange(session, type, range, reference));
        }

        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            disjuncts.add(new ComparisonExpression(EQUAL, reference, getOnlyElement(singleValues)));
        }
        else if (singleValues.size() > 1) {
            disjuncts.add(new InPredicate(reference, new InListExpression(singleValues)));
        }
        return disjuncts;
    }

    private List<Expression> extractDisjuncts(Session session, Type type, DiscreteValues discreteValues, SymbolReference reference)
    {
        List<Expression> values = discreteValues.getValues().stream()
                .map(object -> literalEncoder.toExpression(session, object, type))
                .collect(toList());

        // If values is empty, then the equatableValues was either ALL or NONE, both of which should already have been checked for
        checkState(!values.isEmpty());

        Expression predicate;
        if (values.size() == 1) {
            predicate = new ComparisonExpression(EQUAL, reference, getOnlyElement(values));
        }
        else {
            predicate = new InPredicate(reference, new InListExpression(values));
        }

        if (!discreteValues.isInclusive()) {
            predicate = new NotExpression(predicate);
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
    public static ExtractionResult getExtractionResult(PlannerContext plannerContext, Session session, Expression predicate, TypeProvider types)
    {
        // This is a limited type analyzer for the simple expressions used in this method
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(
                plannerContext,
                new StatementAnalyzerFactory(
                        plannerContext,
                        new SqlParser(),
                        new AllowAllAccessControl(),
                        user -> ImmutableSet.of(),
                        new TableProceduresRegistry(),
                        new SessionPropertyManager(),
                        new TablePropertyManager(),
                        new AnalyzePropertyManager(),
                        new TableProceduresPropertyManager()));
        return new Visitor(plannerContext, session, types, typeAnalyzer).process(predicate, false);
    }

    private static class Visitor
            extends AstVisitor<ExtractionResult, Boolean>
    {
        private final PlannerContext plannerContext;
        private final LiteralEncoder literalEncoder;
        private final Session session;
        private final TypeProvider types;
        private final InterpretedFunctionInvoker functionInvoker;
        private final TypeAnalyzer typeAnalyzer;
        private final TypeCoercion typeCoercion;

        private Visitor(PlannerContext plannerContext, Session session, TypeProvider types, TypeAnalyzer typeAnalyzer)
        {
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.literalEncoder = new LiteralEncoder(plannerContext);
            this.session = requireNonNull(session, "session is null");
            this.types = requireNonNull(types, "types is null");
            this.functionInvoker = new InterpretedFunctionInvoker(plannerContext.getMetadata());
            this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
            this.typeCoercion = new TypeCoercion(plannerContext.getTypeManager()::getType);
        }

        private Type checkedTypeLookup(Symbol symbol)
        {
            Type type = types.get(symbol);
            checkArgument(type != null, "Types is missing info for symbol: %s", symbol);
            return type;
        }

        private static ValueSet complementIfNecessary(ValueSet valueSet, boolean complement)
        {
            return complement ? valueSet.complement() : valueSet;
        }

        private static Domain complementIfNecessary(Domain domain, boolean complement)
        {
            return complement ? domain.complement() : domain;
        }

        private static Expression complementIfNecessary(Expression expression, boolean complement)
        {
            return complement ? new NotExpression(expression) : expression;
        }

        @Override
        protected ExtractionResult visitExpression(Expression node, Boolean complement)
        {
            // If we don't know how to process this node, the default response is to say that the TupleDomain is "all"
            return new ExtractionResult(TupleDomain.all(), complementIfNecessary(node, complement));
        }

        @Override
        protected ExtractionResult visitLogicalExpression(LogicalExpression node, Boolean complement)
        {
            List<ExtractionResult> results = node.getTerms().stream()
                    .map(term -> process(term, complement))
                    .collect(toImmutableList());

            List<TupleDomain<Symbol>> tupleDomains = results.stream()
                    .map(ExtractionResult::getTupleDomain)
                    .collect(toImmutableList());

            List<Expression> residuals = results.stream()
                    .map(ExtractionResult::getRemainingExpression)
                    .collect(toImmutableList());

            LogicalExpression.Operator operator = complement ? node.getOperator().flip() : node.getOperator();
            switch (operator) {
                case AND:
                    return new ExtractionResult(
                            TupleDomain.intersect(tupleDomains),
                            combineConjuncts(plannerContext.getMetadata(), residuals));

                case OR:
                    TupleDomain<Symbol> columnUnionedTupleDomain = TupleDomain.columnWiseUnion(tupleDomains);

                    // In most cases, the columnUnionedTupleDomain is only a superset of the actual strict union
                    // and so we can return the current node as the remainingExpression so that all bounds will be double checked again at execution time.
                    Expression remainingExpression = complementIfNecessary(node, complement);

                    // However, there are a few cases where the column-wise union is actually equivalent to the strict union, so we if can detect
                    // some of these cases, we won't have to double check the bounds unnecessarily at execution time.

                    // We can only make inferences if the remaining expressions on all terms are equal and deterministic
                    if (Set.copyOf(residuals).size() == 1 && DeterminismEvaluator.isDeterministic(residuals.get(0), plannerContext.getMetadata())) {
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
            throw new AssertionError("Unknown operator: " + node.getOperator());
        }

        @Override
        protected ExtractionResult visitNotExpression(NotExpression node, Boolean complement)
        {
            return process(node.getValue(), !complement);
        }

        @Override
        protected ExtractionResult visitSymbolReference(SymbolReference node, Boolean complement)
        {
            if (types.get(Symbol.from(node)).equals(BOOLEAN)) {
                ComparisonExpression newNode = new ComparisonExpression(EQUAL, node, TRUE_LITERAL);
                return visitComparisonExpression(newNode, complement);
            }

            return visitExpression(node, complement);
        }

        @Override
        protected ExtractionResult visitComparisonExpression(ComparisonExpression node, Boolean complement)
        {
            Optional<NormalizedSimpleComparison> optionalNormalized = toNormalizedSimpleComparison(node);
            if (optionalNormalized.isEmpty()) {
                return super.visitComparisonExpression(node, complement);
            }
            NormalizedSimpleComparison normalized = optionalNormalized.get();

            Expression symbolExpression = normalized.getSymbolExpression();
            if (symbolExpression instanceof SymbolReference) {
                Symbol symbol = Symbol.from(symbolExpression);
                NullableValue value = normalized.getValue();
                Type type = value.getType(); // common type for symbol and value
                return createComparisonExtractionResult(normalized.getComparisonOperator(), symbol, type, value.getValue(), complement)
                        .orElseGet(() -> super.visitComparisonExpression(node, complement));
            }
            if (symbolExpression instanceof Cast) {
                Cast castExpression = (Cast) symbolExpression;
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
                    return super.visitComparisonExpression(node, complement);
                }

                Type castSourceType = typeAnalyzer.getType(session, types, castExpression.getExpression()); // type of expression which is then cast to type of value

                // we use saturated floor cast value -> castSourceType to rewrite original expression to new one with one cast peeled off the symbol side
                Optional<Expression> coercedExpression = coerceComparisonWithRounding(
                        castSourceType, castExpression.getExpression(), normalized.getValue(), normalized.getComparisonOperator());

                if (coercedExpression.isPresent()) {
                    return process(coercedExpression.get(), complement);
                }

                return super.visitComparisonExpression(node, complement);
            }
            return super.visitComparisonExpression(node, complement);
        }

        /**
         * Extract a normalized simple comparison between a QualifiedNameReference and a native value if possible.
         */
        private Optional<NormalizedSimpleComparison> toNormalizedSimpleComparison(ComparisonExpression comparison)
        {
            Map<NodeRef<Expression>, Type> expressionTypes = analyzeExpression(comparison);
            Object left = new ExpressionInterpreter(comparison.getLeft(), plannerContext, session, expressionTypes).optimize(NoOpSymbolResolver.INSTANCE);
            Object right = new ExpressionInterpreter(comparison.getRight(), plannerContext, session, expressionTypes).optimize(NoOpSymbolResolver.INSTANCE);

            Type leftType = expressionTypes.get(NodeRef.of(comparison.getLeft()));
            Type rightType = expressionTypes.get(NodeRef.of(comparison.getRight()));

            // TODO: re-enable this check once we fix the type coercions in the optimizers
            // checkArgument(leftType.equals(rightType), "left and right type do not match in comparison expression (%s)", comparison);

            if (left instanceof Expression == right instanceof Expression) {
                // we expect one side to be expression and other to be value.
                return Optional.empty();
            }

            Expression symbolExpression;
            ComparisonExpression.Operator comparisonOperator;
            NullableValue value;

            if (left instanceof Expression) {
                symbolExpression = comparison.getLeft();
                comparisonOperator = comparison.getOperator();
                value = new NullableValue(rightType, right);
            }
            else {
                symbolExpression = comparison.getRight();
                comparisonOperator = comparison.getOperator().flip();
                value = new NullableValue(leftType, left);
            }

            return Optional.of(new NormalizedSimpleComparison(symbolExpression, comparisonOperator, value));
        }

        private boolean isImplicitCoercion(Cast cast)
        {
            Map<NodeRef<Expression>, Type> expressionTypes = analyzeExpression(cast);
            Type actualType = expressionTypes.get(NodeRef.of(cast.getExpression()));
            Type expectedType = expressionTypes.get(NodeRef.<Expression>of(cast));
            return typeCoercion.canCoerce(actualType, expectedType);
        }

        private Map<NodeRef<Expression>, Type> analyzeExpression(Expression expression)
        {
            return typeAnalyzer.getTypes(session, types, expression);
        }

        private static Optional<ExtractionResult> createComparisonExtractionResult(ComparisonExpression.Operator comparisonOperator, Symbol column, Type type, @Nullable Object value, boolean complement)
        {
            if (value == null) {
                switch (comparisonOperator) {
                    case EQUAL:
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                    case NOT_EQUAL:
                        return Optional.of(new ExtractionResult(TupleDomain.none(), TRUE_LITERAL));

                    case IS_DISTINCT_FROM:
                        Domain domain = complementIfNecessary(Domain.notNull(type), complement);
                        return Optional.of(new ExtractionResult(
                                TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)),
                                TRUE_LITERAL));
                }
                throw new AssertionError("Unhandled operator: " + comparisonOperator);
            }
            if (type.isOrderable()) {
                return extractOrderableDomain(comparisonOperator, type, value, complement)
                        .map(domain -> new ExtractionResult(TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)), TRUE_LITERAL));
            }
            if (type.isComparable()) {
                Domain domain = extractEquatableDomain(comparisonOperator, type, value, complement);
                return Optional.of(new ExtractionResult(
                        TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)),
                        TRUE_LITERAL));
            }
            throw new AssertionError("Type cannot be used in a comparison expression (should have been caught in analysis): " + type);
        }

        private static Optional<Domain> extractOrderableDomain(ComparisonExpression.Operator comparisonOperator, Type type, Object value, boolean complement)
        {
            checkArgument(value != null);

            // Handle orderable types which do not have NaN.
            if (!(type instanceof DoubleType) && !(type instanceof RealType)) {
                switch (comparisonOperator) {
                    case EQUAL:
                        return Optional.of(Domain.create(complementIfNecessary(ValueSet.ofRanges(Range.equal(type, value)), complement), false));
                    case GREATER_THAN:
                        return Optional.of(Domain.create(complementIfNecessary(ValueSet.ofRanges(Range.greaterThan(type, value)), complement), false));
                    case GREATER_THAN_OR_EQUAL:
                        return Optional.of(Domain.create(complementIfNecessary(ValueSet.ofRanges(Range.greaterThanOrEqual(type, value)), complement), false));
                    case LESS_THAN:
                        return Optional.of(Domain.create(complementIfNecessary(ValueSet.ofRanges(Range.lessThan(type, value)), complement), false));
                    case LESS_THAN_OR_EQUAL:
                        return Optional.of(Domain.create(complementIfNecessary(ValueSet.ofRanges(Range.lessThanOrEqual(type, value)), complement), false));
                    case NOT_EQUAL:
                        return Optional.of(Domain.create(complementIfNecessary(ValueSet.ofRanges(Range.lessThan(type, value), Range.greaterThan(type, value)), complement), false));
                    case IS_DISTINCT_FROM:
                        // Need to potential complement the whole domain for IS_DISTINCT_FROM since it is null-aware
                        return Optional.of(complementIfNecessary(Domain.create(ValueSet.ofRanges(Range.lessThan(type, value), Range.greaterThan(type, value)), true), complement));
                }
                throw new AssertionError("Unhandled operator: " + comparisonOperator);
            }

            // Handle comparisons against NaN
            if (isFloatingPointNaN(type, value)) {
                switch (comparisonOperator) {
                    case EQUAL:
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                        return Optional.of(Domain.create(complementIfNecessary(ValueSet.none(type), complement), false));

                    case NOT_EQUAL:
                        return Optional.of(Domain.create(complementIfNecessary(ValueSet.all(type), complement), false));

                    case IS_DISTINCT_FROM:
                        // The Domain should be "all but NaN". It is currently not supported.
                        return Optional.empty();
                }
                throw new AssertionError("Unhandled operator: " + comparisonOperator);
            }

            // Handle comparisons against a non-NaN value when the compared value might be NaN
            switch (comparisonOperator) {
                /*
                 For comparison operators: EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL,
                 the Domain should not contain NaN, but complemented Domain should contain NaN. It is currently not supported.
                 Currently, NaN is only included when ValueSet.isAll().

                 For comparison operators: NOT_EQUAL, IS_DISTINCT_FROM,
                 the Domain should consist of ranges (which do not sum to the whole ValueSet), and NaN.
                 Currently, NaN is only included when ValueSet.isAll().
                  */
                case EQUAL:
                    if (complement) {
                        return Optional.empty();
                    }
                    else {
                        return Optional.of(Domain.create(ValueSet.ofRanges(Range.equal(type, value)), false));
                    }
                case GREATER_THAN:
                    if (complement) {
                        return Optional.empty();
                    }
                    else {
                        return Optional.of(Domain.create(ValueSet.ofRanges(Range.greaterThan(type, value)), false));
                    }
                case GREATER_THAN_OR_EQUAL:
                    if (complement) {
                        return Optional.empty();
                    }
                    else {
                        return Optional.of(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, value)), false));
                    }
                case LESS_THAN:
                    if (complement) {
                        return Optional.empty();
                    }
                    else {
                        return Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThan(type, value)), false));
                    }
                case LESS_THAN_OR_EQUAL:
                    if (complement) {
                        return Optional.empty();
                    }
                    else {
                        return Optional.of(Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(type, value)), false));
                    }
                case NOT_EQUAL:
                    if (complement) {
                        return Optional.of(Domain.create(ValueSet.ofRanges(Range.equal(type, value)), false));
                    }
                    else {
                        return Optional.empty();
                    }
                case IS_DISTINCT_FROM:
                    if (complement) {
                        return Optional.of(Domain.create(ValueSet.ofRanges(Range.equal(type, value)), false));
                    }
                    else {
                        return Optional.empty();
                    }
            }
            throw new AssertionError("Unhandled operator: " + comparisonOperator);
        }

        private static Domain extractEquatableDomain(ComparisonExpression.Operator comparisonOperator, Type type, Object value, boolean complement)
        {
            checkArgument(value != null);
            switch (comparisonOperator) {
                case EQUAL:
                    return Domain.create(complementIfNecessary(ValueSet.of(type, value), complement), false);
                case NOT_EQUAL:
                    return Domain.create(complementIfNecessary(ValueSet.of(type, value).complement(), complement), false);

                case IS_DISTINCT_FROM:
                    // Need to potential complement the whole domain for IS_DISTINCT_FROM since it is null-aware
                    return complementIfNecessary(Domain.create(ValueSet.of(type, value).complement(), true), complement);

                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                    // not applicable to equatable types
                    break;
            }
            throw new AssertionError("Unhandled operator: " + comparisonOperator);
        }

        private Optional<Expression> coerceComparisonWithRounding(
                Type symbolExpressionType,
                Expression symbolExpression,
                NullableValue nullableValue,
                ComparisonExpression.Operator comparisonOperator)
        {
            requireNonNull(nullableValue, "nullableValue is null");
            if (nullableValue.isNull()) {
                return Optional.empty();
            }
            Type valueType = nullableValue.getType();
            Object value = nullableValue.getValue();
            return floorValue(valueType, symbolExpressionType, value)
                    .map(floorValue -> rewriteComparisonExpression(symbolExpressionType, symbolExpression, valueType, value, floorValue, comparisonOperator));
        }

        private Expression rewriteComparisonExpression(
                Type symbolExpressionType,
                Expression symbolExpression,
                Type valueType,
                Object originalValue,
                Object coercedValue,
                ComparisonExpression.Operator comparisonOperator)
        {
            int originalComparedToCoerced = compareOriginalValueToCoerced(valueType, originalValue, symbolExpressionType, coercedValue);
            boolean coercedValueIsEqualToOriginal = originalComparedToCoerced == 0;
            boolean coercedValueIsLessThanOriginal = originalComparedToCoerced > 0;
            boolean coercedValueIsGreaterThanOriginal = originalComparedToCoerced < 0;
            Expression coercedLiteral = literalEncoder.toExpression(session, coercedValue, symbolExpressionType);

            switch (comparisonOperator) {
                case GREATER_THAN_OR_EQUAL:
                case GREATER_THAN:
                    if (coercedValueIsGreaterThanOriginal) {
                        return new ComparisonExpression(GREATER_THAN_OR_EQUAL, symbolExpression, coercedLiteral);
                    }
                    if (coercedValueIsEqualToOriginal) {
                        return new ComparisonExpression(comparisonOperator, symbolExpression, coercedLiteral);
                    }
                    if (coercedValueIsLessThanOriginal) {
                        return new ComparisonExpression(GREATER_THAN, symbolExpression, coercedLiteral);
                    }
                    throw new AssertionError("Unreachable");
                case LESS_THAN_OR_EQUAL:
                case LESS_THAN:
                    if (coercedValueIsLessThanOriginal) {
                        return new ComparisonExpression(LESS_THAN_OR_EQUAL, symbolExpression, coercedLiteral);
                    }
                    if (coercedValueIsEqualToOriginal) {
                        return new ComparisonExpression(comparisonOperator, symbolExpression, coercedLiteral);
                    }
                    if (coercedValueIsGreaterThanOriginal) {
                        return new ComparisonExpression(LESS_THAN, symbolExpression, coercedLiteral);
                    }
                    throw new AssertionError("Unreachable");
                case EQUAL:
                    if (coercedValueIsEqualToOriginal) {
                        return new ComparisonExpression(EQUAL, symbolExpression, coercedLiteral);
                    }
                    // Return something that is false for all non-null values
                    return and(new ComparisonExpression(GREATER_THAN, symbolExpression, coercedLiteral),
                            new ComparisonExpression(LESS_THAN, symbolExpression, coercedLiteral));
                case NOT_EQUAL:
                    if (coercedValueIsEqualToOriginal) {
                        return new ComparisonExpression(comparisonOperator, symbolExpression, coercedLiteral);
                    }
                    // Return something that is true for all non-null values
                    return or(new ComparisonExpression(EQUAL, symbolExpression, coercedLiteral),
                            new ComparisonExpression(NOT_EQUAL, symbolExpression, coercedLiteral));
                case IS_DISTINCT_FROM: {
                    if (coercedValueIsEqualToOriginal) {
                        return new ComparisonExpression(comparisonOperator, symbolExpression, coercedLiteral);
                    }
                    return TRUE_LITERAL;
                }
            }

            throw new IllegalArgumentException("Unhandled operator: " + comparisonOperator);
        }

        private Optional<Object> floorValue(Type fromType, Type toType, Object value)
        {
            return getSaturatedFloorCastOperator(fromType, toType)
                    .map(operator -> functionInvoker.invoke(operator, session.toConnectorSession(), value));
        }

        private Optional<ResolvedFunction> getSaturatedFloorCastOperator(Type fromType, Type toType)
        {
            try {
                return Optional.of(plannerContext.getMetadata().getCoercion(session, SATURATED_FLOOR_CAST, fromType, toType));
            }
            catch (OperatorNotFoundException e) {
                return Optional.empty();
            }
        }

        private int compareOriginalValueToCoerced(Type originalValueType, Object originalValue, Type coercedValueType, Object coercedValue)
        {
            requireNonNull(originalValueType, "originalValueType is null");
            requireNonNull(coercedValue, "coercedValue is null");
            ResolvedFunction castToOriginalTypeOperator = plannerContext.getMetadata().getCoercion(session, coercedValueType, originalValueType);
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
        protected ExtractionResult visitInPredicate(InPredicate node, Boolean complement)
        {
            if (!(node.getValueList() instanceof InListExpression)) {
                return super.visitInPredicate(node, complement);
            }

            InListExpression valueList = (InListExpression) node.getValueList();
            checkState(!valueList.getValues().isEmpty(), "InListExpression should never be empty");

            Optional<ExtractionResult> directExtractionResult = processSimpleInPredicate(node, complement);
            if (directExtractionResult.isPresent()) {
                return directExtractionResult.get();
            }

            ImmutableList.Builder<Expression> disjuncts = ImmutableList.builder();
            for (Expression expression : valueList.getValues()) {
                disjuncts.add(new ComparisonExpression(EQUAL, node.getValue(), expression));
            }
            ExtractionResult extractionResult = process(or(disjuncts.build()), complement);

            // preserve original IN predicate as remaining predicate
            if (extractionResult.tupleDomain.isAll()) {
                Expression originalPredicate = node;
                if (complement) {
                    originalPredicate = new NotExpression(originalPredicate);
                }
                return new ExtractionResult(extractionResult.tupleDomain, originalPredicate);
            }
            return extractionResult;
        }

        private Optional<ExtractionResult> processSimpleInPredicate(InPredicate node, Boolean complement)
        {
            if (!(node.getValue() instanceof SymbolReference)) {
                return Optional.empty();
            }
            Symbol symbol = Symbol.from(node.getValue());
            Map<NodeRef<Expression>, Type> expressionTypes = analyzeExpression(node);
            Type type = expressionTypes.get(NodeRef.of(node.getValue()));
            InListExpression valueList = (InListExpression) node.getValueList();
            List<Object> inValues = new ArrayList<>(valueList.getValues().size());
            List<Expression> excludedExpressions = new ArrayList<>();

            for (Expression expression : valueList.getValues()) {
                Object value = new ExpressionInterpreter(expression, plannerContext, session, expressionTypes)
                        .optimize(NoOpSymbolResolver.INSTANCE);
                if (value == null || value instanceof NullLiteral) {
                    if (!complement) {
                        // in case of IN, NULL on the right results with NULL comparison result (effectively false in predicate context), so can be ignored, as the
                        // comparison results are OR-ed
                        continue;
                    }
                    // NOT IN is equivalent to NOT(s eq v1) AND NOT(s eq v2). When any right value is NULL, the comparison result is NULL, so AND's result can be at most
                    // NULL (effectively false in predicate context)
                    return Optional.of(new ExtractionResult(TupleDomain.none(), TRUE_LITERAL));
                }
                if (value instanceof Expression) {
                    if (!complement) {
                        // in case of IN, expression on the right side prevents determining the domain: any lhs value can be eligible
                        return Optional.of(new ExtractionResult(TupleDomain.all(), node));
                    }
                    // in case of NOT IN, expression on the right side still allows determining values that are *not* part of the final domain
                    excludedExpressions.add(((Expression) value));
                    continue;
                }
                if (isFloatingPointNaN(type, value)) {
                    // NaN can be ignored: it always compares to false, as if it was not among IN's values
                    continue;
                }
                if (complement && (type instanceof RealType || type instanceof DoubleType)) {
                    // in case of NOT IN with floating point, the NaN on the left passes the test (unless a NULL is found, and we exited earlier)
                    // but this cannot currently be described with a Domain other than Domain.all
                    excludedExpressions.add(expression);
                }
                else {
                    inValues.add(value);
                }
            }

            ValueSet valueSet = ValueSet.copyOf(type, inValues);
            if (complement) {
                valueSet = valueSet.complement();
            }
            TupleDomain<Symbol> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(symbol, Domain.create(valueSet, false)));

            Expression remainingExpression;
            if (excludedExpressions.isEmpty()) {
                remainingExpression = TRUE_LITERAL;
            }
            else if (excludedExpressions.size() == 1) {
                remainingExpression = new NotExpression(new ComparisonExpression(EQUAL, node.getValue(), getOnlyElement(excludedExpressions)));
            }
            else {
                remainingExpression = new NotExpression(new InPredicate(node.getValue(), new InListExpression(excludedExpressions)));
            }

            return Optional.of(new ExtractionResult(tupleDomain, remainingExpression));
        }

        @Override
        protected ExtractionResult visitBetweenPredicate(BetweenPredicate node, Boolean complement)
        {
            // Re-write as two comparison expressions
            return process(and(
                    new ComparisonExpression(GREATER_THAN_OR_EQUAL, node.getValue(), node.getMin()),
                    new ComparisonExpression(LESS_THAN_OR_EQUAL, node.getValue(), node.getMax())), complement);
        }

        @Override
        protected ExtractionResult visitLikePredicate(LikePredicate node, Boolean complement)
        {
            Optional<ExtractionResult> result = tryVisitLikePredicate(node, complement);
            return result.orElseGet(() -> super.visitLikePredicate(node, complement));
        }

        private Optional<ExtractionResult> tryVisitLikePredicate(LikePredicate node, Boolean complement)
        {
            if (!(node.getValue() instanceof SymbolReference)) {
                // LIKE not on a symbol
                return Optional.empty();
            }

            if (!(node.getPattern() instanceof StringLiteral)) {
                // dynamic pattern
                return Optional.empty();
            }

            if (node.getEscape().isPresent() && !(node.getEscape().get() instanceof StringLiteral)) {
                // dynamic escape
                return Optional.empty();
            }

            Type type = typeAnalyzer.getType(session, types, node.getValue());
            if (!(type instanceof VarcharType)) {
                // TODO support CharType
                return Optional.empty();
            }
            VarcharType varcharType = (VarcharType) type;

            Symbol symbol = Symbol.from(node.getValue());
            Slice pattern = ((StringLiteral) node.getPattern()).getSlice();
            Optional<Slice> escape = node.getEscape()
                    .map(StringLiteral.class::cast)
                    .map(StringLiteral::getSlice);

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
                return Optional.of(new ExtractionResult(TupleDomain.withColumnDomains(ImmutableMap.of(symbol, domain)), TRUE_LITERAL));
            }

            if (complement || patternConstantPrefixBytes == 0) {
                // TODO
                return Optional.empty();
            }

            Slice constantPrefix = LikeFunctions.unescapeLiteralLikePattern(pattern.slice(0, patternConstantPrefixBytes), escape);
            return createRangeDomain(type, constantPrefix).map(domain -> new ExtractionResult(TupleDomain.withColumnDomains(ImmutableMap.of(symbol, domain)), node));
        }

        @Override
        protected ExtractionResult visitFunctionCall(FunctionCall node, Boolean complement)
        {
            String name = ResolvedFunction.extractFunctionName(node.getName());
            if (name.equals("starts_with")) {
                Optional<ExtractionResult> result = tryVisitStartsWithFunction(node, complement);
                if (result.isPresent()) {
                    return result.get();
                }
            }
            return visitExpression(node, complement);
        }

        private Optional<ExtractionResult> tryVisitStartsWithFunction(FunctionCall node, Boolean complement)
        {
            List<Expression> args = node.getArguments();
            if (args.size() != 2) {
                return Optional.empty();
            }

            Expression target = args.get(0);
            if (!(target instanceof SymbolReference)) {
                // Target is not a symbol
                return Optional.empty();
            }

            Expression prefix = args.get(1);
            if (!(prefix instanceof StringLiteral)) {
                // dynamic pattern
                return Optional.empty();
            }

            Type type = typeAnalyzer.getType(session, types, target);
            if (!(type instanceof VarcharType)) {
                // TODO support CharType
                return Optional.empty();
            }
            if (complement) {
                return Optional.empty();
            }

            Symbol symbol = Symbol.from(target);
            Slice constantPrefix = ((StringLiteral) prefix).getSlice();

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
            Slice upperBound = Slices.copyOf(constantPrefix.slice(0, lastIncrementable + lengthOfCodePoint(constantPrefix, lastIncrementable)));
            setCodePointAt(getCodePointAt(constantPrefix, lastIncrementable) + 1, upperBound, lastIncrementable);

            Domain domain = Domain.create(ValueSet.ofRanges(Range.range(type, lowerBound, true, upperBound, false)), false);
            return Optional.of(domain);
        }

        @Override
        protected ExtractionResult visitIsNullPredicate(IsNullPredicate node, Boolean complement)
        {
            if (!(node.getValue() instanceof SymbolReference)) {
                return super.visitIsNullPredicate(node, complement);
            }

            Symbol symbol = Symbol.from(node.getValue());
            Type columnType = checkedTypeLookup(symbol);
            Domain domain = complementIfNecessary(Domain.onlyNull(columnType), complement);
            return new ExtractionResult(
                    TupleDomain.withColumnDomains(ImmutableMap.of(symbol, domain)),
                    TRUE_LITERAL);
        }

        @Override
        protected ExtractionResult visitIsNotNullPredicate(IsNotNullPredicate node, Boolean complement)
        {
            if (!(node.getValue() instanceof SymbolReference)) {
                return super.visitIsNotNullPredicate(node, complement);
            }

            Symbol symbol = Symbol.from(node.getValue());
            Type columnType = checkedTypeLookup(symbol);

            Domain domain = complementIfNecessary(Domain.notNull(columnType), complement);
            return new ExtractionResult(
                    TupleDomain.withColumnDomains(ImmutableMap.of(symbol, domain)),
                    TRUE_LITERAL);
        }

        @Override
        protected ExtractionResult visitBooleanLiteral(BooleanLiteral node, Boolean complement)
        {
            boolean value = complement ? !node.getValue() : node.getValue();
            return new ExtractionResult(value ? TupleDomain.all() : TupleDomain.none(), TRUE_LITERAL);
        }

        @Override
        protected ExtractionResult visitNullLiteral(NullLiteral node, Boolean complement)
        {
            return new ExtractionResult(TupleDomain.none(), TRUE_LITERAL);
        }
    }

    private static class NormalizedSimpleComparison
    {
        private final Expression symbolExpression;
        private final ComparisonExpression.Operator comparisonOperator;
        private final NullableValue value;

        public NormalizedSimpleComparison(Expression symbolExpression, ComparisonExpression.Operator comparisonOperator, NullableValue value)
        {
            this.symbolExpression = requireNonNull(symbolExpression, "symbolExpression is null");
            this.comparisonOperator = requireNonNull(comparisonOperator, "comparisonOperator is null");
            this.value = requireNonNull(value, "value is null");
        }

        public Expression getSymbolExpression()
        {
            return symbolExpression;
        }

        public ComparisonExpression.Operator getComparisonOperator()
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
