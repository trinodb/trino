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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.ResolvedFunction;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.PlanNodeDecorrelator;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.planner.plan.AggregationNode.globalAggregation;
import static io.trino.sql.planner.plan.AggregationNode.singleAggregation;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.Patterns.applyNode;
import static java.util.Objects.requireNonNull;

/**
 * EXISTS is modeled as (if correlated predicates are equality comparisons):
 * <pre>
 *     - Project(exists := COALESCE(subqueryTrue, false))
 *       - CorrelatedJoin(LEFT)
 *         - input
 *         - Project(subqueryTrue := true)
 *           - Limit(count=1)
 *             - subquery
 * </pre>
 * or:
 * <pre>
 *     - CorrelatedJoin(LEFT)
 *       - input
 *       - Project(exists := COALESCE($0, false))
 *         - Aggregation(bool_or(subqueryTrue))
 *           - Project(subqueryTrue := true)
 *             - subquery
 * </pre>
 * otherwise
 */
public class TransformExistsApplyToCorrelatedJoin
        implements Rule<ApplyNode>
{
    private static final Pattern<ApplyNode> PATTERN = applyNode();

    private final PlannerContext plannerContext;

    public TransformExistsApplyToCorrelatedJoin(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    @Override
    public Pattern<ApplyNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ApplyNode parent, Captures captures, Context context)
    {
        if (parent.getSubqueryAssignments().size() != 1) {
            return Result.empty();
        }

        ApplyNode.SetExpression expression = getOnlyElement(parent.getSubqueryAssignments().values());
        if (!(expression instanceof ApplyNode.Exists)) {
            return Result.empty();
        }

        /*
        Empty correlation list indicates that the subquery contains no correlation symbols from the
        immediate outer scope. The subquery might be either not correlated at all, or correlated with
        symbols from further outer scope.
        Currently, the two cases are indistinguishable.
        To support the latter case, the ApplyNode with empty correlation list is rewritten to default
        aggregation, which is inefficient in the rare case of uncorrelated EXISTS subquery,
        but currently allows to successfully decorrelate a correlated EXISTS subquery.

        TODO: remove this condition when exploratory optimizer is implemented or support for decorrelating joins is implemented in PlanNodeDecorrelator
        */
        if (parent.getCorrelation().isEmpty()) {
            return Result.ofPlanNode(rewriteToDefaultAggregation(parent, context));
        }

        Optional<PlanNode> nonDefaultAggregation = rewriteToNonDefaultAggregation(parent, context);
        return nonDefaultAggregation
                .map(Result::ofPlanNode)
                .orElseGet(() -> Result.ofPlanNode(rewriteToDefaultAggregation(parent, context)));
    }

    private Optional<PlanNode> rewriteToNonDefaultAggregation(ApplyNode applyNode, Context context)
    {
        checkState(applyNode.getSubquery().getOutputSymbols().isEmpty(), "Expected subquery output symbols to be pruned");

        Symbol subqueryTrue = context.getSymbolAllocator().newSymbol("subqueryTrue", BOOLEAN);

        PlanNode subquery = new ProjectNode(
                context.getIdAllocator().getNextId(),
                new LimitNode(
                        context.getIdAllocator().getNextId(),
                        applyNode.getSubquery(),
                        1L,
                        false),
                Assignments.of(subqueryTrue, TRUE));

        PlanNodeDecorrelator decorrelator = new PlanNodeDecorrelator(plannerContext, context.getSymbolAllocator(), context.getLookup());
        if (decorrelator.decorrelateFilters(subquery, applyNode.getCorrelation()).isEmpty()) {
            return Optional.empty();
        }

        Symbol exists = getOnlyElement(applyNode.getSubqueryAssignments().keySet());
        Assignments.Builder assignments = Assignments.builder()
                .putIdentities(applyNode.getInput().getOutputSymbols())
                .put(exists, new Coalesce(ImmutableList.of(subqueryTrue.toSymbolReference(), FALSE)));

        return Optional.of(new ProjectNode(context.getIdAllocator().getNextId(),
                new CorrelatedJoinNode(
                        applyNode.getId(),
                        applyNode.getInput(),
                        subquery,
                        applyNode.getCorrelation(),
                        LEFT,
                        TRUE,
                        applyNode.getOriginSubquery()),
                assignments.build()));
    }

    private PlanNode rewriteToDefaultAggregation(ApplyNode applyNode, Context context)
    {
        ResolvedFunction boolOr = plannerContext.getMetadata().resolveBuiltinFunction("bool_or", fromTypes(BOOLEAN));
        Symbol bool = context.getSymbolAllocator().newSymbol("aggrBool", BOOLEAN);

        Symbol exists = getOnlyElement(applyNode.getSubqueryAssignments().keySet());
        Symbol subqueryTrue = context.getSymbolAllocator().newSymbol("subqueryTrue", BOOLEAN);

        return new CorrelatedJoinNode(
                applyNode.getId(),
                applyNode.getInput(),
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        singleAggregation(
                                context.getIdAllocator().getNextId(),
                                new ProjectNode(
                                        context.getIdAllocator().getNextId(),
                                        applyNode.getSubquery(),
                                        Assignments.of(subqueryTrue, TRUE)),
                                ImmutableMap.of(bool, new Aggregation(
                                        boolOr,
                                        ImmutableList.of(subqueryTrue.toSymbolReference()),
                                        false,
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty())),
                                globalAggregation()),
                        Assignments.of(exists, new Coalesce(ImmutableList.of(bool.toSymbolReference(), FALSE)))),
                applyNode.getCorrelation(),
                INNER,
                TRUE,
                applyNode.getOriginSubquery());
    }
}
