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
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.CorrelatedJoinNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.tree.Expression;

import java.util.Optional;

import static io.prestosql.matching.Pattern.empty;
import static io.prestosql.sql.planner.plan.Patterns.CorrelatedJoin.correlation;
import static io.prestosql.sql.planner.plan.Patterns.correlatedJoin;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;

public class TransformUncorrelatedSubqueryToJoin
        implements Rule<CorrelatedJoinNode>
{
    private static final Pattern<CorrelatedJoinNode> PATTERN = correlatedJoin()
            .with(empty(correlation()));

    @Override
    public Pattern<CorrelatedJoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(CorrelatedJoinNode correlatedJoinNode, Captures captures, Context context)
    {
        return Result.ofPlanNode(new JoinNode(
                context.getIdAllocator().getNextId(),
                correlatedJoinNode.getType().toJoinNodeType(),
                correlatedJoinNode.getInput(),
                correlatedJoinNode.getSubquery(),
                ImmutableList.of(),
                ImmutableList.<Symbol>builder()
                        .addAll(correlatedJoinNode.getInput().getOutputSymbols())
                        .addAll(correlatedJoinNode.getSubquery().getOutputSymbols())
                        .build(),
                filter(correlatedJoinNode.getFilter()),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of()));
    }

    private Optional<Expression> filter(Expression criteria)
    {
        if (criteria.equals(TRUE_LITERAL)) {
            return Optional.empty();
        }

        return Optional.of(criteria);
    }
}
