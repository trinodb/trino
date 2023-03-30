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
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.QualifiedName;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.matching.Capture.newCapture;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.plan.Patterns.Limit.requiresPreSortedInputs;
import static io.trino.sql.planner.plan.Patterns.limit;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.WindowNode.Frame.DEFAULT_FRAME;
import static java.util.Objects.requireNonNull;

/**
 * Transforms:
 * <pre>
 * - Limit (row count = x, tiesResolvingScheme(a,b,c))
 *    - source
 * </pre>
 * Into:
 * <pre>
 * - Project (prune rank symbol)
 *    - Filter (rank <= x)
 *       - Window (function: rank, order by a,b,c)
 *          - source
 * </pre>
 */
public class ImplementLimitWithTies
        implements Rule<LimitNode>
{
    private static final Capture<PlanNode> CHILD = newCapture();
    private static final Pattern<LimitNode> PATTERN = limit()
            .matching(LimitNode::isWithTies)
            .with(requiresPreSortedInputs().equalTo(false))
            .with(source().capturedAs(CHILD));

    private final Metadata metadata;

    public ImplementLimitWithTies(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<LimitNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(LimitNode parent, Captures captures, Context context)
    {
        PlanNode child = captures.get(CHILD);

        PlanNode rewritten = rewriteLimitWithTies(parent, child, context.getSession(), metadata, context.getIdAllocator(), context.getSymbolAllocator());

        ProjectNode projectNode = new ProjectNode(
                context.getIdAllocator().getNextId(),
                rewritten,
                Assignments.identity(parent.getOutputSymbols()));

        return Result.ofPlanNode(projectNode);
    }

    private static PlanNode rewriteLimitWithTies(LimitNode limitNode, PlanNode source, Session session, Metadata metadata, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
    {
        return rewriteLimitWithTiesWithPartitioning(limitNode, source, session, metadata, idAllocator, symbolAllocator, ImmutableList.of());
    }

    /**
     * Rewrite LimitNode with ties to WindowNode and FilterNode, with partitioning defined by partitionBy.
     * <p>
     * This method does not prune outputs of the rewritten plan. After the rewrite, the output consists of
     * source's output symbols and the newly created rankSymbol.
     * Passing all input symbols is intentional, because this method is used for de-correlation in the scenario
     * where the original LimitNode is in the correlated subquery, and the rewrite result is placed on top of
     * de-correlated join.
     * It is the responsibility of the caller to prune redundant outputs.
     */
    public static PlanNode rewriteLimitWithTiesWithPartitioning(LimitNode limitNode, PlanNode source, Session session, Metadata metadata, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, List<Symbol> partitionBy)
    {
        checkArgument(limitNode.isWithTies(), "Expected LimitNode with ties");

        Symbol rankSymbol = symbolAllocator.newSymbol("rank_num", BIGINT);

        WindowNode.Function rankFunction = new WindowNode.Function(
                metadata.resolveFunction(session, QualifiedName.of("rank"), ImmutableList.of()),
                ImmutableList.of(),
                DEFAULT_FRAME,
                false);

        WindowNode windowNode = new WindowNode(
                idAllocator.getNextId(),
                source,
                new DataOrganizationSpecification(partitionBy, limitNode.getTiesResolvingScheme()),
                ImmutableMap.of(rankSymbol, rankFunction),
                Optional.empty(),
                ImmutableSet.of(),
                0);

        return new FilterNode(
                idAllocator.getNextId(),
                windowNode,
                new ComparisonExpression(
                        ComparisonExpression.Operator.LESS_THAN_OR_EQUAL,
                        rankSymbol.toSymbolReference(),
                        new GenericLiteral("BIGINT", Long.toString(limitNode.getCount()))));
    }
}
