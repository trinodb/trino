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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.cost.TableStatsProvider;
import io.trino.execution.querystats.PlanOptimizersStatsCollector;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import io.trino.sql.ExpressionUtils;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.QuantifiedComparisonExpression;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.WhenClause;

import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ExpressionUtils.combineConjuncts;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.planner.plan.AggregationNode.globalAggregation;
import static io.trino.sql.planner.plan.AggregationNode.singleAggregation;
import static io.trino.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static io.trino.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.NOT_EQUAL;
import static io.trino.sql.tree.QuantifiedComparisonExpression.Quantifier.ALL;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class TransformQuantifiedComparisonApplyToCorrelatedJoin
        implements PlanOptimizer
{
    private final Metadata metadata;

    public TransformQuantifiedComparisonApplyToCorrelatedJoin(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public PlanNode optimize(
            PlanNode plan,
            Session session,
            TypeProvider types,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator,
            WarningCollector warningCollector,
            PlanOptimizersStatsCollector planOptimizersStatsCollector,
            TableStatsProvider tableStatsProvider)
    {
        return rewriteWith(new Rewriter(idAllocator, types, symbolAllocator, metadata, session), plan, null);
    }

    private static class Rewriter
            extends SimplePlanRewriter<PlanNode>
    {
        private static final QualifiedName MIN = QualifiedName.of("min");
        private static final QualifiedName MAX = QualifiedName.of("max");
        private static final QualifiedName COUNT = QualifiedName.of("count");

        private final PlanNodeIdAllocator idAllocator;
        private final TypeProvider types;
        private final SymbolAllocator symbolAllocator;
        private final Metadata metadata;
        private final Session session;

        public Rewriter(PlanNodeIdAllocator idAllocator, TypeProvider types, SymbolAllocator symbolAllocator, Metadata metadata, Session session)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.types = requireNonNull(types, "types is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.session = session;
        }

        @Override
        public PlanNode visitApply(ApplyNode node, RewriteContext<PlanNode> context)
        {
            if (node.getSubqueryAssignments().size() != 1) {
                return context.defaultRewrite(node);
            }

            Expression expression = getOnlyElement(node.getSubqueryAssignments().getExpressions());
            if (!(expression instanceof QuantifiedComparisonExpression quantifiedComparison)) {
                return context.defaultRewrite(node);
            }

            return rewriteQuantifiedApplyNode(node, quantifiedComparison, context);
        }

        private PlanNode rewriteQuantifiedApplyNode(ApplyNode node, QuantifiedComparisonExpression quantifiedComparison, RewriteContext<PlanNode> context)
        {
            PlanNode subqueryPlan = context.rewrite(node.getSubquery());

            Symbol outputColumn = getOnlyElement(subqueryPlan.getOutputSymbols());
            Type outputColumnType = types.get(outputColumn);
            checkState(outputColumnType.isOrderable(), "Subquery result type must be orderable");

            Symbol minValue = symbolAllocator.newSymbol(MIN.toString(), outputColumnType);
            Symbol maxValue = symbolAllocator.newSymbol(MAX.toString(), outputColumnType);
            Symbol countAllValue = symbolAllocator.newSymbol("count_all", BigintType.BIGINT);
            Symbol countNonNullValue = symbolAllocator.newSymbol("count_non_null", BigintType.BIGINT);

            List<Expression> outputColumnReferences = ImmutableList.of(outputColumn.toSymbolReference());

            subqueryPlan = singleAggregation(
                    idAllocator.getNextId(),
                    subqueryPlan,
                    ImmutableMap.of(
                            minValue, new Aggregation(
                                    metadata.resolveFunction(session, MIN, fromTypes(outputColumnType)),
                                    outputColumnReferences,
                                    false,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()),
                            maxValue, new Aggregation(
                                    metadata.resolveFunction(session, MAX, fromTypes(outputColumnType)),
                                    outputColumnReferences,
                                    false,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()),
                            countAllValue, new Aggregation(
                                    metadata.resolveFunction(session, COUNT, emptyList()),
                                    ImmutableList.of(),
                                    false,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()),
                            countNonNullValue, new Aggregation(
                                    metadata.resolveFunction(session, COUNT, fromTypes(outputColumnType)),
                                    outputColumnReferences,
                                    false,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty())),
                    globalAggregation());

            PlanNode join = new CorrelatedJoinNode(
                    node.getId(),
                    context.rewrite(node.getInput()),
                    subqueryPlan,
                    node.getCorrelation(),
                    CorrelatedJoinNode.Type.INNER,
                    TRUE_LITERAL,
                    node.getOriginSubquery());

            Expression valueComparedToSubquery = rewriteUsingBounds(quantifiedComparison, minValue, maxValue, countAllValue, countNonNullValue);

            Symbol quantifiedComparisonSymbol = getOnlyElement(node.getSubqueryAssignments().getSymbols());

            return projectExpressions(join, Assignments.of(quantifiedComparisonSymbol, valueComparedToSubquery));
        }

        public Expression rewriteUsingBounds(QuantifiedComparisonExpression quantifiedComparison, Symbol minValue, Symbol maxValue, Symbol countAllValue, Symbol countNonNullValue)
        {
            BooleanLiteral emptySetResult;
            Function<List<Expression>, Expression> quantifier;
            if (quantifiedComparison.getQuantifier() == ALL) {
                emptySetResult = TRUE_LITERAL;
                quantifier = expressions -> ExpressionUtils.combineConjuncts(metadata, expressions);
            }
            else {
                emptySetResult = FALSE_LITERAL;
                quantifier = expressions -> ExpressionUtils.combineDisjuncts(metadata, expressions);
            }
            Expression comparisonWithExtremeValue = getBoundComparisons(quantifiedComparison, minValue, maxValue);

            return new SimpleCaseExpression(
                    countAllValue.toSymbolReference(),
                    ImmutableList.of(new WhenClause(
                            new GenericLiteral("bigint", "0"),
                            emptySetResult)),
                    Optional.of(quantifier.apply(ImmutableList.of(
                            comparisonWithExtremeValue,
                            new SearchedCaseExpression(
                                    ImmutableList.of(
                                            new WhenClause(
                                                    new ComparisonExpression(NOT_EQUAL, countAllValue.toSymbolReference(), countNonNullValue.toSymbolReference()),
                                                    new Cast(new NullLiteral(), toSqlType(BOOLEAN)))),
                                    Optional.of(emptySetResult))))));
        }

        private Expression getBoundComparisons(QuantifiedComparisonExpression quantifiedComparison, Symbol minValue, Symbol maxValue)
        {
            if (quantifiedComparison.getOperator() == EQUAL && quantifiedComparison.getQuantifier() == ALL) {
                // A = ALL B <=> min B = max B && A = min B
                return combineConjuncts(
                        metadata,
                        new ComparisonExpression(EQUAL, minValue.toSymbolReference(), maxValue.toSymbolReference()),
                        new ComparisonExpression(EQUAL, quantifiedComparison.getValue(), maxValue.toSymbolReference()));
            }

            if (EnumSet.of(LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL).contains(quantifiedComparison.getOperator())) {
                // A < ALL B <=> A < min B
                // A > ALL B <=> A > max B
                // A < ANY B <=> A < max B
                // A > ANY B <=> A > min B
                Symbol boundValue = shouldCompareValueWithLowerBound(quantifiedComparison) ? minValue : maxValue;
                return new ComparisonExpression(quantifiedComparison.getOperator(), quantifiedComparison.getValue(), boundValue.toSymbolReference());
            }
            throw new IllegalArgumentException("Unsupported quantified comparison: " + quantifiedComparison);
        }

        private static boolean shouldCompareValueWithLowerBound(QuantifiedComparisonExpression quantifiedComparison)
        {
            return switch (quantifiedComparison.getQuantifier()) {
                case ALL -> switch (quantifiedComparison.getOperator()) {
                    case LESS_THAN, LESS_THAN_OR_EQUAL -> true;
                    case GREATER_THAN, GREATER_THAN_OR_EQUAL -> false;
                    default -> throw new IllegalArgumentException("Unexpected value: " + quantifiedComparison.getOperator());
                };
                case ANY, SOME -> switch (quantifiedComparison.getOperator()) {
                    case LESS_THAN, LESS_THAN_OR_EQUAL -> false;
                    case GREATER_THAN, GREATER_THAN_OR_EQUAL -> true;
                    default -> throw new IllegalArgumentException("Unexpected value: " + quantifiedComparison.getOperator());
                };
            };
        }

        private ProjectNode projectExpressions(PlanNode input, Assignments subqueryAssignments)
        {
            Assignments assignments = Assignments.builder()
                    .putIdentities(input.getOutputSymbols())
                    .putAll(subqueryAssignments)
                    .build();
            return new ProjectNode(
                    idAllocator.getNextId(),
                    input,
                    assignments);
        }
    }
}
