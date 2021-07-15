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
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.RowNumberNode;

import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.plan.ChildReplacer.replaceChildren;
import static io.trino.sql.planner.plan.Patterns.limit;
import static io.trino.sql.planner.plan.Patterns.rowNumber;
import static io.trino.sql.planner.plan.Patterns.source;
import static java.lang.Math.toIntExact;

public class PushdownLimitIntoRowNumber
        implements Rule<LimitNode>
{
    private static final Capture<RowNumberNode> CHILD = newCapture();
    private static final Pattern<LimitNode> PATTERN = limit()
            .matching(limit -> !limit.isWithTies() && limit.getCount() != 0 && limit.getCount() <= Integer.MAX_VALUE)
            .with(source().matching(rowNumber().capturedAs(CHILD)));

    @Override
    public Pattern<LimitNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(LimitNode node, Captures captures, Context context)
    {
        RowNumberNode source = captures.get(CHILD);
        RowNumberNode rowNumberNode = mergeLimit(source, node);
        if (rowNumberNode.getPartitionBy().isEmpty()) {
            return Result.ofPlanNode(rowNumberNode);
        }
        if (source.getMaxRowCountPerPartition().isPresent()) {
            if (rowNumberNode.getMaxRowCountPerPartition().equals(source.getMaxRowCountPerPartition())) {
                // Source node unchanged
                return Result.empty();
            }
        }
        return Result.ofPlanNode(replaceChildren(node, ImmutableList.of(rowNumberNode)));
    }

    private static RowNumberNode mergeLimit(RowNumberNode node, LimitNode limitNode)
    {
        int newRowCountPerPartition = toIntExact(limitNode.getCount());
        if (node.getMaxRowCountPerPartition().isPresent()) {
            newRowCountPerPartition = Math.min(node.getMaxRowCountPerPartition().get(), newRowCountPerPartition);
        }
        return new RowNumberNode(
                node.getId(),
                node.getSource(),
                node.getPartitionBy(),
                limitNode.requiresPreSortedInputs() || node.isOrderSensitive(),
                node.getRowNumberSymbol(),
                Optional.of(newRowCountPerPartition),
                node.getHashSymbol());
    }
}
