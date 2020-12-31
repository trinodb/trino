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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.CorrelatedJoinNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.IfExpression;
import io.prestosql.sql.tree.NullLiteral;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.matching.Pattern.empty;
import static io.prestosql.sql.planner.plan.CorrelatedJoinNode.Type.FULL;
import static io.prestosql.sql.planner.plan.CorrelatedJoinNode.Type.INNER;
import static io.prestosql.sql.planner.plan.CorrelatedJoinNode.Type.LEFT;
import static io.prestosql.sql.planner.plan.CorrelatedJoinNode.Type.RIGHT;
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
        // handle INNER and LEFT correlated join
        if (correlatedJoinNode.getType() == INNER || correlatedJoinNode.getType() == LEFT) {
            return Result.ofPlanNode(rewriteToJoin(
                    correlatedJoinNode,
                    correlatedJoinNode.getType().toJoinNodeType(),
                    correlatedJoinNode.getFilter()));
        }

        checkState(
                correlatedJoinNode.getType() == RIGHT || correlatedJoinNode.getType() == FULL,
                "unexpected CorrelatedJoin type: " + correlatedJoinNode.getType());

        // handle RIGHT and FULL correlated join ON TRUE
        JoinNode.Type type;
        if (correlatedJoinNode.getType() == RIGHT) {
            type = JoinNode.Type.INNER;
        }
        else {
            type = JoinNode.Type.LEFT;
        }
        JoinNode joinNode = rewriteToJoin(correlatedJoinNode, type, TRUE_LITERAL);

        if (correlatedJoinNode.getFilter().equals(TRUE_LITERAL)) {
            return Result.ofPlanNode(joinNode);
        }

        // handle RIGHT correlated join on condition other than TRUE
        if (correlatedJoinNode.getType() == RIGHT) {
            Assignments.Builder assignments = Assignments.builder();
            assignments.putIdentities(Sets.intersection(
                    ImmutableSet.copyOf(correlatedJoinNode.getSubquery().getOutputSymbols()),
                    ImmutableSet.copyOf(correlatedJoinNode.getOutputSymbols())));
            for (Symbol inputSymbol : Sets.intersection(
                    ImmutableSet.copyOf(correlatedJoinNode.getInput().getOutputSymbols()),
                    ImmutableSet.copyOf(correlatedJoinNode.getOutputSymbols()))) {
                assignments.put(inputSymbol, new IfExpression(correlatedJoinNode.getFilter(), inputSymbol.toSymbolReference(), new NullLiteral()));
            }
            ProjectNode projectNode = new ProjectNode(
                    context.getIdAllocator().getNextId(),
                    joinNode,
                    assignments.build());

            return Result.ofPlanNode(projectNode);
        }

        // no support for FULL correlated join on condition other than TRUE
        return Result.empty();
    }

    private JoinNode rewriteToJoin(CorrelatedJoinNode parent, JoinNode.Type type, Expression filter)
    {
        return new JoinNode(
                parent.getId(),
                type,
                parent.getInput(),
                parent.getSubquery(),
                ImmutableList.of(),
                parent.getInput().getOutputSymbols(),
                parent.getSubquery().getOutputSymbols(),
                filter.equals(TRUE_LITERAL) ? Optional.empty() : Optional.of(filter),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());
    }
}
