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
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.sql.planner.OrderingScheme;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.OffsetNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.RowNumberNode;
import io.prestosql.sql.planner.plan.SortNode;
import io.prestosql.sql.planner.plan.TopNNode;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.GenericLiteral;

import java.util.Optional;

import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.planner.plan.Patterns.offset;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.topN;

/**
 * Transforms:
 * <pre>
 * - Offset (row count = x)
 *    - Project (prunes sort symbols no longer useful)
 *       - TopN (order by a, b, c)
 * </pre>
 * Into:
 * <pre>
 * - Project (prunes rowNumber symbol and sort symbols no longer useful)
 *    - Sort (order by rowNumber)
 *       - Filter (rowNumber > x)
 *          - RowNumber
 *             - TopN (order by a, b, c)
 * </pre>
 */
public class ImplementOffsetOverProjectTopN
        implements Rule<OffsetNode>
{
    private static final Capture<ProjectNode> PROJECT = newCapture();
    private static final Capture<TopNNode> TOPN = newCapture();

    private static final Pattern<OffsetNode> PATTERN = offset()
            .with(source().matching(
                    project().capturedAs(PROJECT).matching(ProjectNode::isIdentity)
                            .with(source().matching(
                                    topN().capturedAs(TOPN)))));

    @Override
    public Pattern<OffsetNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(OffsetNode parent, Captures captures, Context context)
    {
        ProjectNode project = captures.get(PROJECT);
        TopNNode topN = captures.get(TOPN);

        Symbol rowNumberSymbol = context.getSymbolAllocator().newSymbol("row_number", BIGINT);

        RowNumberNode rowNumberNode = new RowNumberNode(
                context.getIdAllocator().getNextId(),
                topN,
                ImmutableList.of(),
                rowNumberSymbol,
                Optional.empty(),
                Optional.empty());

        FilterNode filterNode = new FilterNode(
                context.getIdAllocator().getNextId(),
                rowNumberNode,
                new ComparisonExpression(
                        ComparisonExpression.Operator.GREATER_THAN,
                        rowNumberSymbol.toSymbolReference(),
                        new GenericLiteral("BIGINT", Long.toString(parent.getCount()))));

        SortNode sortNode = new SortNode(
                context.getIdAllocator().getNextId(),
                filterNode,
                new OrderingScheme(ImmutableList.of(rowNumberSymbol), ImmutableMap.of(rowNumberSymbol, SortOrder.ASC_NULLS_FIRST)));

        ProjectNode projectNode = (ProjectNode) project.replaceChildren(ImmutableList.of(sortNode));

        return Result.ofPlanNode(projectNode);
    }
}
