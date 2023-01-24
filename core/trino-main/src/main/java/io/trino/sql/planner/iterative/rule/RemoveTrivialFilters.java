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
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NullLiteral;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.util.Collections.emptyList;

public class RemoveTrivialFilters
        implements Rule<FilterNode>
{
    private static final Pattern<FilterNode> PATTERN = filter();

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(FilterNode filterNode, Captures captures, Context context)
    {
        Expression predicate = filterNode.getPredicate();
        checkArgument(!(predicate instanceof NullLiteral), "Unexpected null literal without a cast to boolean");

        if (predicate.equals(TRUE_LITERAL)) {
            return Result.ofPlanNode(filterNode.getSource());
        }

        if (predicate.equals(FALSE_LITERAL) ||
                (predicate instanceof Cast cast && cast.getExpression() instanceof NullLiteral)) {
            return Result.ofPlanNode(new ValuesNode(context.getIdAllocator().getNextId(), filterNode.getOutputSymbols(), emptyList()));
        }

        return Result.empty();
    }
}
