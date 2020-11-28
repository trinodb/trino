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
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.optimizations.QueryCardinalityUtil;
import io.prestosql.sql.planner.plan.ApplyNode;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.tree.ExistsPredicate;
import io.prestosql.sql.tree.Expression;

import static io.prestosql.sql.planner.plan.Patterns.applyNode;
import static io.prestosql.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;

/**
 * Given:
 *
 * <pre>
 * - Apply [X.*, e = EXISTS (true)]
 *   - X
 *   - S with cardinality >= 1
 * </pre>
 *
 * Produces:
 *
 * <pre>
 * - Project [X.*, e = true]
 *   - X
 * </pre>
 *
 * Given:
 *
 * <pre>
 * - Apply [X.*, e = EXISTS (true)]
 *   - X
 *   - S with cardinality = 0
 * </pre>
 *
 * Produces:
 *
 * <pre>
 * - Project [X.*, e = false]
 *   - X
 * </pre>
 */
public class RemoveRedundantExists
        implements Rule<ApplyNode>
{
    private static final Pattern<ApplyNode> PATTERN = applyNode()
            .matching(node -> node.getSubqueryAssignments()
                    .getExpressions().stream()
                    .allMatch(expression -> expression instanceof ExistsPredicate && ((ExistsPredicate) expression).getSubquery().equals(TRUE_LITERAL)));

    @Override
    public Pattern<ApplyNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ApplyNode node, Captures captures, Context context)
    {
        Assignments.Builder assignments = Assignments.builder();
        assignments.putIdentities(node.getInput().getOutputSymbols());

        Expression result;
        if (QueryCardinalityUtil.isEmpty(node.getSubquery(), context.getLookup())) {
            result = FALSE_LITERAL;
        }
        else if (QueryCardinalityUtil.isAtLeastScalar(node.getSubquery(), context.getLookup())) {
            result = TRUE_LITERAL;
        }
        else {
            return Result.empty();
        }

        for (Symbol output : node.getSubqueryAssignments().getOutputs()) {
            assignments.put(output, result);
        }

        return Result.ofPlanNode(new ProjectNode(
                context.getIdAllocator().getNextId(),
                node.getInput(),
                assignments.build()));
    }
}
