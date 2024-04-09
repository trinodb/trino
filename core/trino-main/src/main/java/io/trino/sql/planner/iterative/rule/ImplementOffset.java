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
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.OffsetNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RowNumberNode;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.planner.plan.Patterns.offset;

/**
 * Transforms:
 * <pre>
 * - Offset (row count = x)
 *    - Source
 * </pre>
 * Into:
 * <pre>
 * - Project (prune rowNumber symbol)
 *    - Filter (rowNumber > x)
 *       - RowNumber
 *          - Source
 * </pre>
 * Relies on RowNumberNode's property of keeping order of its input.
 * If the query contains an ORDER BY clause, the sorted order
 * will be respected when leading rows are removed.
 */
public class ImplementOffset
        implements Rule<OffsetNode>
{
    private static final Pattern<OffsetNode> PATTERN = offset();

    @Override
    public Pattern<OffsetNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(OffsetNode parent, Captures captures, Context context)
    {
        Symbol rowNumberSymbol = context.getSymbolAllocator().newSymbol("row_number", BIGINT);

        RowNumberNode rowNumberNode = new RowNumberNode(
                context.getIdAllocator().getNextId(),
                parent.getSource(),
                ImmutableList.of(),
                true,
                rowNumberSymbol,
                Optional.empty(),
                Optional.empty());

        FilterNode filterNode = new FilterNode(
                context.getIdAllocator().getNextId(),
                rowNumberNode,
                new Comparison(
                        GREATER_THAN,
                        rowNumberSymbol.toSymbolReference(),
                        new Constant(BIGINT, parent.getCount())));

        ProjectNode projectNode = new ProjectNode(
                context.getIdAllocator().getNextId(),
                filterNode,
                Assignments.identity(parent.getOutputSymbols()));

        return Result.ofPlanNode(projectNode);
    }
}
