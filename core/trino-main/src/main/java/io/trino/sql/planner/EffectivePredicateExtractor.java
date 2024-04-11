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
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.type.TypeUtils.isFloatingPointNaN;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.ir.IrUtils.expressionOrNullSymbols;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.ir.IrUtils.filterDeterministicConjuncts;
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
                Reference reference = entry.getKey().toSymbolReference();
                Expression expression = entry.getValue();

                if (expression instanceof Constant constant && constant.value() == null) {
                    return new IsNull(reference);
                }

                // TODO: this is not correct with respect to NULLs ('reference IS NULL' would be correct, rather than 'reference = NULL')
                // TODO: switch this to 'IS NOT DISTINCT FROM' syntax when EqualityInference properly supports it
                return new Comparison(EQUAL, reference, expression);
            };

    private final PlannerContext plannerContext;
    private final boolean useTableProperties;

    public EffectivePredicateExtractor(PlannerContext plannerContext, boolean useTableProperties)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.useTableProperties = useTableProperties;
    }

    public Expression extract(Session session, PlanNode node)
    {
        return node.accept(new Visitor(plannerContext, session, useTableProperties), null);
    }

    private static class Visitor
            extends PlanVisitor<Expression, Void>
    {
        private final PlannerContext plannerContext;
        private final Metadata metadata;
        private final Session session;
        private final boolean useTableProperties;

        public Visitor(PlannerContext plannerContext, Session session, boolean useTableProperties)
        {
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.metadata = plannerContext.getMetadata();
            this.session = requireNonNull(session, "session is null");
            this.useTableProperties = useTableProperties;
        }

        @Override
        protected Expression visitPlan(PlanNode node, Void context)
        {
            return TRUE;
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
                return TRUE;
            }

            Expression underlyingPredicate = node.getSource().accept(this, context);

            return pullExpressionThroughSymbols(underlyingPredicate, node.getGroupingKeys());
        }

        @Override
        public Expression visitFilter(FilterNode node, Void context)
        {
            Expression underlyingPredicate = node.getSource().accept(this, context);

            DomainTranslator.ExtractionResult underlying = DomainTranslator.getExtractionResult(plannerContext, session, filterDeterministicConjuncts(underlyingPredicate));

            if (underlying.getTupleDomain().isNone()) {
                // Effective predicate extraction is incorrect in the presence of nulls, which manifests as a NONE domain
                // In that case, ignore it and combine it into the filter directly
                // See EffectivePredicateExtractor#ENTRY_TO_EQUALITY
                // TODO: this should be removed once EffectivePredicate extraction is fixed for null handling
                return combineConjuncts(underlyingPredicate, node.getPredicate());
            }

            DomainTranslator.ExtractionResult current = DomainTranslator.getExtractionResult(plannerContext, session, filterDeterministicConjuncts(node.getPredicate()));
            return combineConjuncts(
                    DomainTranslator.toPredicate(underlying.getTupleDomain().intersect(current.getTupleDomain())),
                    underlying.getRemainingExpression(),
                    current.getRemainingExpression());
        }

        @Override
        public Expression visitExchange(ExchangeNode node, Void context)
        {
            return deriveCommonPredicates(node, source -> {
                Map<Symbol, Reference> mappings = new HashMap<>();
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
                    .filter(assignment -> assignment.getKey().type().isComparable() || assignment.getKey().type().isOrderable())
                    .filter(assignment -> Sets.intersection(SymbolsExtractor.extractUnique(assignment.getValue()), newlyAssignedSymbols).isEmpty())
                    .map(ENTRY_TO_EQUALITY)
                    .collect(toImmutableList());

            return pullExpressionThroughSymbols(combineConjuncts(
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
            return DomainTranslator.toPredicate(predicate.simplify()
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
            return TRUE;
        }

        @Override
        public Expression visitJoin(JoinNode node, Void context)
        {
            Expression leftPredicate = node.getLeft().accept(this, context);
            Expression rightPredicate = node.getRight().accept(this, context);

            List<Expression> joinConjuncts = node.getCriteria().stream()
                    .map(JoinNode.EquiJoinClause::toExpression)
                    .collect(toImmutableList());

            return switch (node.getType()) {
                case INNER -> pullExpressionThroughSymbols(combineConjuncts(ImmutableList.<Expression>builder()
                        .add(leftPredicate)
                        .add(rightPredicate)
                        .add(combineConjuncts(joinConjuncts))
                        .add(node.getFilter().orElse(TRUE))
                        .build()), node.getOutputSymbols());
                case LEFT -> combineConjuncts(ImmutableList.<Expression>builder()
                        .add(pullExpressionThroughSymbols(leftPredicate, node.getOutputSymbols()))
                        .addAll(pullNullableConjunctsThroughOuterJoin(extractConjuncts(rightPredicate), node.getOutputSymbols(), node.getRight().getOutputSymbols()::contains))
                        .addAll(pullNullableConjunctsThroughOuterJoin(joinConjuncts, node.getOutputSymbols(), node.getRight().getOutputSymbols()::contains))
                        .build());
                case RIGHT -> combineConjuncts(ImmutableList.<Expression>builder()
                        .add(pullExpressionThroughSymbols(rightPredicate, node.getOutputSymbols()))
                        .addAll(pullNullableConjunctsThroughOuterJoin(extractConjuncts(leftPredicate), node.getOutputSymbols(), node.getLeft().getOutputSymbols()::contains))
                        .addAll(pullNullableConjunctsThroughOuterJoin(joinConjuncts, node.getOutputSymbols(), node.getLeft().getOutputSymbols()::contains))
                        .build());
                case FULL -> combineConjuncts(ImmutableList.<Expression>builder()
                        .addAll(pullNullableConjunctsThroughOuterJoin(extractConjuncts(leftPredicate), node.getOutputSymbols(), node.getLeft().getOutputSymbols()::contains))
                        .addAll(pullNullableConjunctsThroughOuterJoin(extractConjuncts(rightPredicate), node.getOutputSymbols(), node.getRight().getOutputSymbols()::contains))
                        .addAll(pullNullableConjunctsThroughOuterJoin(joinConjuncts, node.getOutputSymbols(), node.getLeft().getOutputSymbols()::contains, node.getRight().getOutputSymbols()::contains))
                        .build());
            };
        }

        @Override
        public Expression visitValues(ValuesNode node, Void context)
        {
            if (node.getOutputSymbols().isEmpty()) {
                return TRUE;
            }

            // for each row of Values, get all expressions that will be evaluated:
            // - if the row is of type Row, evaluate fields of the row
            // - otherwise evaluate the whole expression and then analyze fields of the resulting row
            checkState(node.getRows().isPresent(), "rows is empty");

            boolean[] hasNull = new boolean[node.getOutputSymbols().size()];
            boolean[] hasNaN = new boolean[node.getOutputSymbols().size()];
            boolean[] nonDeterministic = new boolean[node.getOutputSymbols().size()];
            ImmutableList.Builder<ImmutableList.Builder<Object>> builders = ImmutableList.builder();
            for (int i = 0; i < node.getOutputSymbols().size(); i++) {
                builders.add(ImmutableList.builder());
            }
            List<ImmutableList.Builder<Object>> valuesBuilders = builders.build();

            for (Expression expression : node.getRows().get()) {
                if (expression instanceof Row row) {
                    for (int i = 0; i < node.getOutputSymbols().size(); i++) {
                        Expression value = row.items().get(i);
                        if (!DeterminismEvaluator.isDeterministic(value)) {
                            nonDeterministic[i] = true;
                        }
                        else {
                            Expression item = new IrExpressionInterpreter(value, plannerContext, session).optimize();
                            if (!(item instanceof Constant constant)) {
                                return TRUE;
                            }
                            if (constant.value() == null) {
                                hasNull[i] = true;
                            }
                            else {
                                Type type = node.getOutputSymbols().get(i).type();
                                if (!type.isComparable() && !type.isOrderable()) {
                                    return TRUE;
                                }
                                if (hasNestedNulls(type, ((Constant) item).value())) {
                                    // Workaround solution to deal with array and row comparisons don't support null elements currently.
                                    // TODO: remove when comparisons are fixed
                                    return TRUE;
                                }
                                if (isFloatingPointNaN(type, ((Constant) item).value())) {
                                    hasNaN[i] = true;
                                }
                                valuesBuilders.get(i).add(((Constant) item).value());
                            }
                        }
                    }
                }
                else {
                    if (!DeterminismEvaluator.isDeterministic(expression)) {
                        return TRUE;
                    }
                    Expression evaluated = new IrExpressionInterpreter(expression, plannerContext, session).optimize();
                    if (!(evaluated instanceof Constant constant)) {
                        return TRUE;
                    }
                    SqlRow sqlRow = (SqlRow) constant.value();
                    int rawIndex = sqlRow.getRawIndex();
                    for (int i = 0; i < node.getOutputSymbols().size(); i++) {
                        Type type = node.getOutputSymbols().get(i).type();
                        Block fieldBlock = sqlRow.getRawFieldBlock(i);
                        Object item = readNativeValue(type, fieldBlock, rawIndex);
                        if (item == null) {
                            hasNull[i] = true;
                        }
                        else {
                            if (!type.isComparable() && !type.isOrderable()) {
                                return TRUE;
                            }
                            if (hasNestedNulls(type, item)) {
                                // Workaround solution to deal with array and row comparisons don't support null elements currently.
                                // TODO: remove when comparisons are fixed
                                return TRUE;
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
                Type type = symbol.type();
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
            return DomainTranslator.toPredicate(TupleDomain.withColumnDomains(domains.buildOrThrow()).simplify());
        }

        private boolean hasNestedNulls(Type type, Object value)
        {
            if (type instanceof RowType rowType) {
                SqlRow sqlRow = (SqlRow) value;
                int rawIndex = sqlRow.getRawIndex();
                for (int i = 0; i < rowType.getFields().size(); i++) {
                    Type elementType = rowType.getFields().get(i).getType();
                    Block fieldBlock = sqlRow.getRawFieldBlock(i);
                    if (fieldBlock.isNull(rawIndex) || elementHasNulls(elementType, fieldBlock, rawIndex)) {
                        return true;
                    }
                }
            }
            else if (type instanceof ArrayType arrayType) {
                Block container = (Block) value;
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
            if (elementType instanceof RowType rowType) {
                SqlRow element = rowType.getObject(container, position);
                return hasNestedNulls(elementType, element);
            }
            if (elementType instanceof ArrayType) {
                Block element = (Block) elementType.getObject(container, position);
                return hasNestedNulls(elementType, element);
            }

            return false;
        }

        @SafeVarargs
        private Iterable<Expression> pullNullableConjunctsThroughOuterJoin(List<Expression> conjuncts, Collection<Symbol> outputSymbols, Predicate<Symbol>... nullSymbolScopes)
        {
            // Conjuncts without any symbol dependencies cannot be applied to the effective predicate (e.g. FALSE literal)
            return conjuncts.stream()
                    .map(expression -> pullExpressionThroughSymbols(expression, outputSymbols))
                    .map(expression -> SymbolsExtractor.extractAll(expression).isEmpty() ? TRUE : expression)
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

            return switch (node.getType()) {
                case INNER -> combineConjuncts(ImmutableList.<Expression>builder()
                        .add(pullExpressionThroughSymbols(leftPredicate, node.getOutputSymbols()))
                        .add(pullExpressionThroughSymbols(rightPredicate, node.getOutputSymbols()))
                        .build());
                case LEFT -> combineConjuncts(ImmutableList.<Expression>builder()
                        .add(pullExpressionThroughSymbols(leftPredicate, node.getOutputSymbols()))
                        .addAll(pullNullableConjunctsThroughOuterJoin(extractConjuncts(rightPredicate), node.getOutputSymbols(), node.getRight().getOutputSymbols()::contains))
                        .build());
            };
        }

        private Expression deriveCommonPredicates(PlanNode node, Function<Integer, Collection<Map.Entry<Symbol, Reference>>> mapping)
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

            return combineConjuncts(potentialOutputConjuncts);
        }

        private Expression pullExpressionThroughSymbols(Expression expression, Collection<Symbol> symbols)
        {
            EqualityInference equalityInference = new EqualityInference(expression);

            ImmutableList.Builder<Expression> effectiveConjuncts = ImmutableList.builder();
            Set<Symbol> scope = ImmutableSet.copyOf(symbols);
            EqualityInference.nonInferrableConjuncts(expression).forEach(conjunct -> {
                if (DeterminismEvaluator.isDeterministic(conjunct)) {
                    Expression rewritten = equalityInference.rewrite(conjunct, scope);
                    if (rewritten != null) {
                        effectiveConjuncts.add(rewritten);
                    }
                }
            });

            effectiveConjuncts.addAll(equalityInference.generateEqualitiesPartitionedBy(scope).getScopeEqualities());

            return combineConjuncts(effectiveConjuncts.build());
        }
    }
}
