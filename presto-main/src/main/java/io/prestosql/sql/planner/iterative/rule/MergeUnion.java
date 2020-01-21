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

import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.SetOperationNode;
import io.prestosql.sql.planner.plan.UnionNode;

import java.util.Optional;

import static io.prestosql.sql.planner.plan.Patterns.union;

/**
 * Transforms:
 * <pre>
 * - Union
 *   - Union
 *     - Relation1
 *     - Relation2
 *   - Union
 *     - Relation3
 *     - Relation4
 * </pre>
 * Into
 * <pre>
 * - Union
 *   - Relation1
 *   - Relation2
 *   - Relation3
 *   - Relation4
 * </pre>
 */
public class MergeUnion
        implements Rule<UnionNode>
{
    private static final Pattern<UnionNode> PATTERN = union();

    @Override
    public Pattern<UnionNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(UnionNode node, Captures captures, Context context)
    {
        SetOperationMerge mergeOperation = new SetOperationMerge(node, context, UnionNode::new);
        Optional<SetOperationNode> result = mergeOperation.merge();
        return result.map(Result::ofPlanNode).orElseGet(Result::empty);
    }
}
