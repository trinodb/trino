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

import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.OffsetNode;
import io.trino.sql.planner.plan.ProjectNode;

import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.iterative.rule.Util.transpose;
import static io.trino.sql.planner.plan.Patterns.offset;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;

/**
 * Transforms:
 * <pre>
 * - Offset
 *    - Project (non identity)
 * </pre>
 * Into:
 * <pre>
 * - Project (non identity)
 *    - Offset
 * </pre>
 */
public class PushOffsetThroughProject
        implements Rule<OffsetNode>
{
    private static final Capture<ProjectNode> CHILD = newCapture();

    private static final Pattern<OffsetNode> PATTERN = offset()
            .with(source().matching(
                    project()
                            // do not push offset through identity projection which could be there for column pruning purposes
                            .matching(projectNode -> !projectNode.isIdentity())
                            .capturedAs(CHILD)));

    @Override
    public Pattern<OffsetNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(OffsetNode parent, Captures captures, Context context)
    {
        return Result.ofPlanNode(transpose(parent, captures.get(CHILD)));
    }
}
