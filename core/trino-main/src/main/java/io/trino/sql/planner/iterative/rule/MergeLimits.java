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
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.SortNode;

import java.util.List;

import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.plan.Patterns.limit;
import static io.trino.sql.planner.plan.Patterns.source;

/**
 * This rule handles both LimitNode with ties and LimitNode without ties.
 * The parent LimitNode is without ties.
 * <p>
 * If the child LimitNode is without ties, both nodes are merged
 * into a single LimitNode with row count being the minimum
 * of their row counts:
 * </p>
 * <pre>
 *    - Limit (3)
 *       - Limit (5)
 * </pre>
 * is transformed into:
 * <pre>
 *     - Limit (3)
 * </pre>
 * <p>
 * If the child LimitNode is with ties, the rule's behavior depends
 * on both nodes' row count.
 * If parent row count is lower or equal to child row count,
 * child node is replaced with a SortNode using its tiesResolvingScheme:
 * </p>
 * <pre>
 *     - Limit (3)
 *        - Limit (5, withTies: a)
 * </pre>
 * is transformed into:
 * <pre>
 *     - Limit (3)
 *        - Sort (a)
 * </pre>
 * <p>
 * If parent row count is greater than child row count, both nodes
 * remain in the plan, rearranged so that the LimitNode with ties is the root,
 * with a SortNode using its tiesResolvingScheme below the inner LimitNode:
 * </p>
 * <pre>
 *     - Limit (5)
 *        - Limit (3, withTies: a)
 * </pre>
 * is transformed into:
 * <pre>
 *     - Limit (3, withTies: a)
 *        - Limit (5)
 *           - Sort (a)
 * </pre>
 */
public class MergeLimits
        implements Rule<LimitNode>
{
    private static final Capture<LimitNode> CHILD = newCapture();

    private static final Pattern<LimitNode> PATTERN = limit()
            .matching(limit -> !limit.isWithTies())
            .with(source().matching(limit().capturedAs(CHILD)));

    @Override
    public Pattern<LimitNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(LimitNode parent, Captures captures, Context context)
    {
        LimitNode child = captures.get(CHILD);

        List<Symbol> sortedInputs = ImmutableList.of();
        if (child.requiresPreSortedInputs()) {
            sortedInputs = child.getPreSortedInputs();
        }
        else if (parent.requiresPreSortedInputs()) {
            sortedInputs = parent.getPreSortedInputs();
        }

        // parent and child are without ties
        if (!child.isWithTies()) {
            return Result.ofPlanNode(
                    new LimitNode(
                            parent.getId(),
                            child.getSource(),
                            Math.min(parent.getCount(), child.getCount()),
                            parent.getTiesResolvingScheme(),
                            parent.isPartial(),
                            sortedInputs));
        }

        // parent is without ties and child is with ties
        if (parent.getCount() > Integer.MAX_VALUE) {
            return Result.empty();
        }

        SortNode sortedSource = new SortNode(
                context.getIdAllocator().getNextId(),
                child.getSource(),
                child.getTiesResolvingScheme().orElseThrow(),
                false);
        LimitNode innerLimit = new LimitNode(
                parent.getId(),
                sortedSource,
                parent.getCount(),
                false);
        if (parent.getCount() > child.getCount()) {
            return Result.ofPlanNode(child.replaceChildren(ImmutableList.of(innerLimit)));
        }

        return Result.ofPlanNode(innerLimit);
    }
}
