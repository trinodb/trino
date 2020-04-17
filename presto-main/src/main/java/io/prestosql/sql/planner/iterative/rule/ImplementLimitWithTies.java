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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.LimitNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.WindowNode;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.FrameBound;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.WindowFrame;

import java.util.Optional;

import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.planner.plan.Patterns.limit;
import static io.prestosql.sql.planner.plan.Patterns.source;
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
        Symbol rankSymbol = context.getSymbolAllocator().newSymbol("rank_num", BIGINT);

        ResolvedFunction function = metadata.resolveFunction(QualifiedName.of("rank"), ImmutableList.of());

        WindowNode.Frame frame = new WindowNode.Frame(
                WindowFrame.Type.RANGE,
                FrameBound.Type.UNBOUNDED_PRECEDING,
                Optional.empty(),
                FrameBound.Type.CURRENT_ROW,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        WindowNode.Function rankFunction = new WindowNode.Function(
                function,
                ImmutableList.of(),
                frame,
                false);

        WindowNode windowNode = new WindowNode(
                context.getIdAllocator().getNextId(),
                child,
                new WindowNode.Specification(ImmutableList.of(), parent.getTiesResolvingScheme()),
                ImmutableMap.of(rankSymbol, rankFunction),
                Optional.empty(),
                ImmutableSet.of(),
                0);

        FilterNode filterNode = new FilterNode(
                context.getIdAllocator().getNextId(),
                windowNode,
                new ComparisonExpression(
                        ComparisonExpression.Operator.LESS_THAN_OR_EQUAL,
                        rankSymbol.toSymbolReference(),
                        new GenericLiteral("BIGINT", Long.toString(parent.getCount()))));

        ProjectNode projectNode = new ProjectNode(
                context.getIdAllocator().getNextId(),
                filterNode,
                Assignments.identity(parent.getOutputSymbols()));

        return Result.ofPlanNode(projectNode);
    }
}
