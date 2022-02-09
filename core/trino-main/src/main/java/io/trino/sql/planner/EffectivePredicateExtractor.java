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

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.spi.block.Block;
import io.trino.spi.block.SingleRowBlock;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.DistinctLimitNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SymbolReference;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.type.TypeUtils.isFloatingPointNaN;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.sql.ExpressionUtils.combineConjuncts;
import static io.trino.sql.ExpressionUtils.expressionOrNullSymbols;
import static io.trino.sql.ExpressionUtils.extractConjuncts;
import static io.trino.sql.ExpressionUtils.filterDeterministicConjuncts;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static java.util.Objects.requireNonNull;

/**
 * Computes the effective predicate at the top of the specified PlanNode
 * <p>
 * Note: non-deterministic predicates cannot be pulled up (so they will be ignored)
 */
public class EffectivePredicateExtractor
{
    private static final Predicate<Map.Entry<Symbol, ? extends Expression>> SYMBOL_MATCHES_EXPRESSION =
            entry -> entry.getValue().equals(entry.getKey().toSymbolReference());

    private static final Function<Map.Entry<Symbol, ? extends Expression>, Expression> ENTRY_TO_EQUALITY =
            entry -> {
                SymbolReference reference = entry.getKey().toSymbolReference();
                Expression expression = entry.getValue();
                // TODO: this is not correct with respect to NULLs ('reference IS NULL' would be correct, rather than 'reference = NULL')
                // TODO: switch this to 'IS NOT DISTINCT FROM' syntax when EqualityInference properly supports it
                return new ComparisonExpression(EQUAL, reference, expression);
            };

    private final PlannerContext plannerContext;
    private final DomainTranslator domainTranslator;
    private final boolean useTableProperties;

    public EffectivePredicateExtractor(DomainTranslator domainTranslator, PlannerContext plannerContext, boolean useTableProperties)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.domainTranslator = requireNonNull(domainTranslator, "domainTranslator is null");
        this.useTableProperties = useTableProperties;
    }

    public Expression extract(Session session, PlanNode node, TypeProvider types, TypeAnalyzer typeAnalyzer)
    {
        return node.accept(new Visitor(domainTranslator, plannerContext, session, types, typeAnalyzer, useTableProperties), null);
    }

    private static class Visitor
            extends PlanVisitor<Expression, Void>
    {
        private final DomainTranslator domainTranslator;
        private final PlannerContext plannerContext;
        private final Metadata metadata;
        private final Session session;
        private final TypeProvider types;
        private final TypeAnalyzer typeAnalyzer;
        private final boolean useTableProperties;

        public Visitor(DomainTranslator domainTranslator, PlannerContext plannerContext, Session session, TypeProvider types, TypeAnalyzer typeAnalyzer, boolean useTableProperties)
        {
            this.domainTranslator = requireNonNull(domainTranslator, "domainTranslator is null");
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.metadata = plannerContext.getMetadata();
            this.session = requireNonNull(session, "session is null");
            this.types = requireNonNull(types, "types is null");
            this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
            this.useTableProperties = useTableProperties;
        }

        @Override
        protected Expression visitPlan(PlanNode node, Void context)
        {
            return TRUE_LITERAL;
        }

        @Override
        public Expression visitAggregation(AggregationNode node, Void context)
        {
            // GROUP BY () always produces a group, regardless of whether there's any
            // input (unlike the case where there are group by keys, which produce
            // no output if there's no input).
            // Therefore, we can't say anything about the effective predicate of the
            // output of such an aggregation.
            if (node.getGroupingKeys().isEmpty()) {
                return TRUE_LITERAL;
            }

            Expression underlyingPredicate = node.getSource().accept(this, context);

            return pullExpressionThroughSymbols(underlyingPredicate, node.getGroupingKeys());
        }

        @Override
        public Expression visitFilter(FilterNode node, Void context)
        {
            Expression underlyingPredicate = node.getSource().accept(this, context);

            Expression predicate = node.getPredicate();

            // Remove non-deterministic conjuncts
            predicate = filterDeterministicConjuncts(metadata, predicate);

            return combineConjuncts(metadata, predicate, underlyingPredicate);
        }

        @Override
        public Expression visitExchange(ExchangeNode node, Void context)
        {
            return deriveCommonPredicates(node, source -> {
                Map<Symbol, SymbolReference> mappings = new HashMap<>();
                for (int i = 0; i < node.getInputs().get(source).size(); i++) {
                    mappings.put(
                            node.getOutputSymbols().get(i),
                            node.getInputs().get(source).get(i).toSymbolReference());
                }
                return mappings.entrySet();
            });
        }

        @Override
        public Expression visitProject(ProjectNode node, Void context)
        {
            // TODO: add simple algebraic solver for projection translation (right now only considers identity projections)

            // Clear predicates involving symbols which are keys to non-identity assignments.
            // Assignment such as `s -> x + 1` establishes new semantics for symbol `s`.
            // If symbol `s` was present is the source plan and was included in underlying predicate, the predicate is no more valid.
            // Also, if symbol `s` is present in a project assignment's value, e.g. `s1 -> s + 1`, this assignment should't be used to derive equality.

            Expression underlyingPredicate = node.getSource().accept(this, context);

            List<Map.Entry<Symbol, Expression>> nonIdentityAssignments = node.getAssignments().entrySet().stream()
                    .filter(SYMBOL_MATCHES_EXPRESSION.negate())
                    .collect(toImmutableList());

            Set<Symbol> newlyAssignedSymbols = nonIdentityAssignments.stream()
                    .map(Map.Entry::getKey)
                    .collect(toImmutableSet());

            List<Expression> validUnderlyingEqualities = extractConjuncts(underlyingPredicate).stream()
                    .filter(expression -> Sets.intersection(SymbolsExtractor.extractUnique(expression), newlyAssignedSymbols).isEmpty())
                    .collect(toImmutableList());

            List<Expression> projectionEqualities = nonIdentityAssignments.stream()
                    .filter(assignment -> Sets.intersection(SymbolsExtractor.extractUnique(assignment.getValue()), newlyAssignedSymbols).isEmpty())
                    .map(ENTRY_TO_EQUALITY)
                    .collect(toImmutableList());

            return pullExpressionThroughSymbols(combineConjuncts(
                    metadata,
                    ImmutableList.<Expression>builder()
                            .addAll(projectionEqualities)
                            .addAll(validUnderlyingEqualities)
                            .build()),
                    node.getOutputSymbols());
        }

        @Override
        public Expression visitTopN(TopNNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Expression visitLimit(LimitNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Expression visitAssignUniqueId(AssignUniqueId node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Expression visitDistinctLimit(DistinctLimitNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Expression visitTableScan(TableScanNode node, Void context)
        {
            Map<ColumnHandle, Symbol> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();

            TupleDomain<ColumnHandle> predicate = node.getEnforcedConstraint();
            if (useTableProperties) {
                predicate = metadata.getTableProperties(session, node.getTable()).getPredicate();
            }

            // TODO: replace with metadata.getTableProperties() when table layouts are fully removed
            return domainTranslator.toPredicate(session, predicate.simplify()
                    .filter((columnHandle, domain) -> assignments.containsKey(columnHandle))
                    .transformKeys(assignments::get));
        }

        @Override
        public Expression visitSort(SortNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Expression visitWindow(WindowNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Expression visitPatternRecognition(PatternRecognitionNode node, Void context)
        {
            Expression sourcePredicate = node.getSource().accept(this, context);
            return pullExpressionThroughSymbols(sourcePredicate, node.getOutputSymbols());
        }

        @Override
        public Expression visitUnion(UnionNode node, Void context)
        {
            return deriveCommonPredicates(node, source -> node.outputSymbolMap(source).entries());
        }

        @Override
        public Expression visitUnnest(UnnestNode node, Void context)
        {
            Expression sourcePredicate = node.getSource().accept(this, context);

            switch (node.getJoinType()) {
                case INNER:
                case LEFT:
                    return pullExpressionThroughSymbols(
                            combineConjuncts(metadata, node.getFilter().orElse(TRUE_LITERAL), sourcePredicate),
                            node.getOutputSymbols());
                case RIGHT:
                case FULL:
                    return TRUE_LITERAL;
            }
            throw new UnsupportedOperationException("Unknown UNNEST join type: " + node.getJoinType());
        }

        @Override
        public Expression visitJoin(JoinNode node, Void context)
        {
            Expression leftPredicate = node.getLeft().accept(this, context);
            Expression rightPredicate = node.getRight().accept(this, context);

            List<Expression> joinConjuncts = node.getCriteria().stream()
                    .map(JoinNode.EquiJoinClause::toExpression)
                    .collect(toImmutableList());

            switch (node.getType()) {
                case INNER:
                    return pullExpressionThroughSymbols(combineConjuncts(metadata, ImmutableList.<Expression>builder()
                            .add(leftPredicate)
                            .add(rightPredicate)
                            .add(combineConjuncts(metadata, joinConjuncts))
                            .add(node.getFilter().orElse(TRUE_LITERAL))
                            .build()), node.getOutputSymbols());
                case LEFT:
                    return combineConjuncts(metadata, ImmutableList.<Expression>builder()
                            .add(pullExpressionThroughSymbols(leftPredicate, node.getOutputSymbols()))
                            .addAll(pullNullableConjunctsThroughOuterJoin(extractConjuncts(rightPredicate), node.getOutputSymbols(), node.getRight().getOutputSymbols()::contains))
                            .addAll(pullNullableConjunctsThroughOuterJoin(joinConjuncts, node.getOutputSymbols(), node.getRight().getOutputSymbols()::contains))
                            .build());
                case RIGHT:
                    return combineConjuncts(metadata, ImmutableList.<Expression>builder()
                            .add(pullExpressionThroughSymbols(rightPredicate, node.getOutputSymbols()))
                            .addAll(pullNullableConjunctsThroughOuterJoin(extractConjuncts(leftPredicate), node.getOutputSymbols(), node.getLeft().getOutputSymbols()::contains))
                            .addAll(pullNullableConjunctsThroughOuterJoin(joinConjuncts, node.getOutputSymbols(), node.getLeft().getOutputSymbols()::contains))
                            .build());
                case FULL:
                    return combineConjuncts(metadata, ImmutableList.<Expression>builder()
                            .addAll(pullNullableConjunctsThroughOuterJoin(extractConjuncts(leftPredicate), node.getOutputSymbols(), node.getLeft().getOutputSymbols()::contains))
                            .addAll(pullNullableConjunctsThroughOuterJoin(extractConjuncts(rightPredicate), node.getOutputSymbols(), node.getRight().getOutputSymbols()::contains))
                            .addAll(pullNullableConjunctsThroughOuterJoin(joinConjuncts, node.getOutputSymbols(), node.getLeft().getOutputSymbols()::contains, node.getRight().getOutputSymbols()::contains))
                            .build());
            }
            throw new UnsupportedOperationException("Unknown join type: " + node.getType());
        }

        @Override
        public Expression visitValues(ValuesNode node, Void context)
        {
            if (node.getOutputSymbols().isEmpty()) {
                return TRUE_LITERAL;
            }

            // for each row of Values, get all expressions that will be evaluated:
            // - if the row is of type Row, evaluate fields of the row
            // - otherwise evaluate the whole expression and then analyze fields of the resulting row
            checkState(node.getRows().isPresent(), "rows is empty");
            List<Expression> processedExpressions = node.getRows().get().stream()
                    .flatMap(row -> {
                        if (row instanceof Row) {
                            return ((Row) row).getItems().stream();
                        }
                        return Stream.of(row);
                    })
                    .collect(toImmutableList());

            Map<NodeRef<Expression>, Type> expressionTypes = typeAnalyzer.getTypes(session, types, processedExpressions);

            boolean[] hasNull = new boolean[node.getOutputSymbols().size()];
            boolean[] hasNaN = new boolean[node.getOutputSymbols().size()];
            boolean[] nonDeterministic = new boolean[node.getOutputSymbols().size()];
            ImmutableList.Builder<ImmutableList.Builder<Object>> builders = ImmutableList.builder();
            for (int i = 0; i < node.getOutputSymbols().size(); i++) {
                builders.add(ImmutableList.builder());
            }
            List<ImmutableList.Builder<Object>> valuesBuilders = builders.build();

            for (Expression row : node.getRows().get()) {
                if (row instanceof Row) {
                    for (int i = 0; i < node.getOutputSymbols().size(); i++) {
                        Expression value = ((Row) row).getItems().get(i);
                        if (!DeterminismEvaluator.isDeterministic(value, metadata)) {
                            nonDeterministic[i] = true;
                        }
                        else {
                            ExpressionInterpreter interpreter = new ExpressionInterpreter(value, plannerContext, session, expressionTypes);
                            Object item = interpreter.optimize(NoOpSymbolResolver.INSTANCE);
                            if (item instanceof Expression) {
                                return TRUE_LITERAL;
                            }
                            if (item == null) {
                                hasNull[i] = true;
                            }
                            else {
                                Type type = types.get(node.getOutputSymbols().get(i));
                                if (hasNestedNulls(type, item)) {
                                    // Workaround solution to deal with array and row comparisons don't support null elements currently.
                                    // TODO: remove when comparisons are fixed
                                    return TRUE_LITERAL;
                                }
                                if (isFloatingPointNaN(type, item)) {
                                    hasNaN[i] = true;
                                }
                                valuesBuilders.get(i).add(item);
                            }
                        }
                    }
                }
                else {
                    if (!DeterminismEvaluator.isDeterministic(row, metadata)) {
                        return TRUE_LITERAL;
                    }
                    ExpressionInterpreter interpreter = new ExpressionInterpreter(row, plannerContext, session, expressionTypes);
                    Object evaluated = interpreter.optimize(NoOpSymbolResolver.INSTANCE);
                    if (evaluated instanceof Expression) {
                        return TRUE_LITERAL;
                    }
                    for (int i = 0; i < node.getOutputSymbols().size(); i++) {
                        Type type = types.get(node.getOutputSymbols().get(i));
                        Object item = readNativeValue(type, (SingleRowBlock) evaluated, i);
                        if (item == null) {
                            hasNull[i] = true;
                        }
                        else {
                            if (hasNestedNulls(type, item)) {
                                // Workaround solution to deal with array and row comparisons don't support null elements currently.
                                // TODO: remove when comparisons are fixed
                                return TRUE_LITERAL;
                            }
                            if (isFloatingPointNaN(type, item)) {
                                hasNaN[i] = true;
                            }
                            valuesBuilders.get(i).add(item);
                        }
                    }
                }
            }

            // use aggregated information about columns to build domains
            ImmutableMap.Builder<Symbol, Domain> domains = ImmutableMap.builder();
            for (int i = 0; i < node.getOutputSymbols().size(); i++) {
                Symbol symbol = node.getOutputSymbols().get(i);
                Type type = types.get(symbol);
                if (nonDeterministic[i]) {
                    // We can't describe a predicate for this column because at least
                    // one cell is non-deterministic, so skip it.
                    continue;
                }

                List<Object> values = valuesBuilders.get(i).build();

                Domain domain;
                if (values.isEmpty()) {
                    domain = Domain.none(type);
                }
                else if (hasNaN[i]) {
                    domain = Domain.notNull(type);
                }
                else {
                    domain = Domain.multipleValues(type, values);
                }

                if (hasNull[i]) {
                    domain = domain.union(Domain.onlyNull(type));
                }

                domains.put(symbol, domain);
            }

            // simplify to avoid a large expression if there are many rows in ValuesNode
            return domainTranslator.toPredicate(session, TupleDomain.withColumnDomains(domains.buildOrThrow()).simplify());
        }

        private boolean hasNestedNulls(Type type, Object value)
        {
            if (type instanceof RowType) {
                Block container = (Block) value;
                RowType rowType = (RowType) type;
                for (int i = 0; i < rowType.getFields().size(); i++) {
                    Type elementType = rowType.getFields().get(i).getType();

                    if (container.isNull(i) || elementHasNulls(elementType, container, i)) {
                        return true;
                    }
                }
            }
            else if (type instanceof ArrayType) {
                Block container = (Block) value;
                ArrayType arrayType = (ArrayType) type;
                Type elementType = arrayType.getElementType();

                for (int i = 0; i < container.getPositionCount(); i++) {
                    if (container.isNull(i) || elementHasNulls(elementType, container, i)) {
                        return true;
                    }
                }
            }

            return false;
        }

        private boolean elementHasNulls(Type elementType, Block container, int position)
        {
            if (elementType instanceof RowType || elementType instanceof ArrayType) {
                Block element = (Block) elementType.getObject(container, position);
                return hasNestedNulls(elementType, element);
            }

            return false;
        }

        @SafeVarargs
        private final Iterable<Expression> pullNullableConjunctsThroughOuterJoin(List<Expression> conjuncts, Collection<Symbol> outputSymbols, Predicate<Symbol>... nullSymbolScopes)
        {
            // Conjuncts without any symbol dependencies cannot be applied to the effective predicate (e.g. FALSE literal)
            return conjuncts.stream()
                    .map(expression -> pullExpressionThroughSymbols(expression, outputSymbols))
                    .map(expression -> SymbolsExtractor.extractAll(expression).isEmpty() ? TRUE_LITERAL : expression)
                    .map(expressionOrNullSymbols(nullSymbolScopes))
                    .collect(toImmutableList());
        }

        @Override
        public Expression visitSemiJoin(SemiJoinNode node, Void context)
        {
            // Filtering source does not change the effective predicate over the output symbols
            return node.getSource().accept(this, context);
        }

        @Override
        public Expression visitSpatialJoin(SpatialJoinNode node, Void context)
        {
            Expression leftPredicate = node.getLeft().accept(this, context);
            Expression rightPredicate = node.getRight().accept(this, context);

            switch (node.getType()) {
                case INNER:
                    return combineConjuncts(metadata, ImmutableList.<Expression>builder()
                            .add(pullExpressionThroughSymbols(leftPredicate, node.getOutputSymbols()))
                            .add(pullExpressionThroughSymbols(rightPredicate, node.getOutputSymbols()))
                            .build());
                case LEFT:
                    return combineConjuncts(metadata, ImmutableList.<Expression>builder()
                            .add(pullExpressionThroughSymbols(leftPredicate, node.getOutputSymbols()))
                            .addAll(pullNullableConjunctsThroughOuterJoin(extractConjuncts(rightPredicate), node.getOutputSymbols(), node.getRight().getOutputSymbols()::contains))
                            .build());
            }
            throw new IllegalArgumentException("Unsupported spatial join type: " + node.getType());
        }

        private Expression deriveCommonPredicates(PlanNode node, Function<Integer, Collection<Map.Entry<Symbol, SymbolReference>>> mapping)
        {
            // Find the predicates that can be pulled up from each source
            List<Set<Expression>> sourceOutputConjuncts = new ArrayList<>();
            for (int i = 0; i < node.getSources().size(); i++) {
                Expression underlyingPredicate = node.getSources().get(i).accept(this, null);

                List<Expression> equalities = mapping.apply(i).stream()
                        .filter(SYMBOL_MATCHES_EXPRESSION.negate())
                        .map(ENTRY_TO_EQUALITY)
                        .collect(toImmutableList());

                sourceOutputConjuncts.add(ImmutableSet.copyOf(extractConjuncts(pullExpressionThroughSymbols(combineConjuncts(
                        metadata,
                        ImmutableList.<Expression>builder()
                                .addAll(equalities)
                                .add(underlyingPredicate)
                                .build()),
                        node.getOutputSymbols()))));
            }

            // Find the intersection of predicates across all sources
            // TODO: use a more precise way to determine overlapping conjuncts (e.g. commutative predicates)
            Iterator<Set<Expression>> iterator = sourceOutputConjuncts.iterator();
            Set<Expression> potentialOutputConjuncts = iterator.next();
            while (iterator.hasNext()) {
                potentialOutputConjuncts = Sets.intersection(potentialOutputConjuncts, iterator.next());
            }

            return combineConjuncts(metadata, potentialOutputConjuncts);
        }

        private Expression pullExpressionThroughSymbols(Expression expression, Collection<Symbol> symbols)
        {
            EqualityInference equalityInference = EqualityInference.newInstance(metadata, expression);

            ImmutableList.Builder<Expression> effectiveConjuncts = ImmutableList.builder();
            Set<Symbol> scope = ImmutableSet.copyOf(symbols);
            EqualityInference.nonInferrableConjuncts(metadata, expression).forEach(conjunct -> {
                if (DeterminismEvaluator.isDeterministic(conjunct, metadata)) {
                    Expression rewritten = equalityInference.rewrite(conjunct, scope);
                    if (rewritten != null) {
                        effectiveConjuncts.add(rewritten);
                    }
                }
            });

            effectiveConjuncts.addAll(equalityInference.generateEqualitiesPartitionedBy(scope).getScopeEqualities());

            return combineConjuncts(metadata, effectiveConjuncts.build());
        }
    }
}
