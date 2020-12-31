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
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.LimitNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.SortNode;
import io.prestosql.sql.planner.plan.TopNNode;

import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.plan.Patterns.limit;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.sort;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.TopNNode.Step.PARTIAL;
import static io.prestosql.sql.planner.plan.TopNNode.Step.SINGLE;

/**
 * Transforms:
 * <pre>
 * - Limit (limit = x)
 *    - Project (identity, narrowing)
 *       - Sort (order by a, b)
 * </pre>
 * Into:
 * <pre>
 * - Project (identity, narrowing)
 *    - TopN (limit = x, order by a, b)
 * </pre>
 * Applies to LimitNode without ties only.
 */
public class MergeLimitOverProjectWithSort
        implements Rule<LimitNode>
{
    private static final Capture<ProjectNode> PROJECT = newCapture();
    private static final Capture<SortNode> SORT = newCapture();

    private static final Pattern<LimitNode> PATTERN = limit()
            .matching(limit -> !limit.isWithTies())
            .with(source().matching(
                    project().capturedAs(PROJECT).matching(ProjectNode::isIdentity)
                            .with(source().matching(
                                    sort().capturedAs(SORT)))));

    @Override
    public Pattern<LimitNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(LimitNode parent, Captures captures, Context context)
    {
        ProjectNode project = captures.get(PROJECT);
        SortNode sort = captures.get(SORT);

        return Result.ofPlanNode(
                project.replaceChildren(ImmutableList.of(
                        new TopNNode(
                                parent.getId(),
                                sort.getSource(),
                                parent.getCount(),
                                sort.getOrderingScheme(),
                                parent.isPartial() ? PARTIAL : SINGLE))));
    }
}
