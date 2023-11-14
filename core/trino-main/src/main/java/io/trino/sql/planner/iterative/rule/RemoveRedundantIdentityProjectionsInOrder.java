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

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.ProjectNode;

import static io.trino.sql.planner.plan.Patterns.project;

/**
 * Removes projection nodes that only perform non-renaming identity projections
 * without changing output symbols order
 * (same as @{code RemoveRedundantIdentityProjections} but mind output order)
 */
public class RemoveRedundantIdentityProjectionsInOrder
        implements Rule<ProjectNode>
{
    private static final Pattern<ProjectNode> PATTERN = project()
            .matching(ProjectNode::isIdentity)
            .matching(RemoveRedundantIdentityProjectionsInOrder::outputsEqualsSourceOutputs);

    private static boolean outputsEqualsSourceOutputs(ProjectNode node)
    {
        return node.getOutputSymbols().equals(node.getSource().getOutputSymbols());
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ProjectNode project, Captures captures, Context context)
    {
        return Result.ofPlanNode(project.getSource());
    }
}
