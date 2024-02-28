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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.spi.type.Type;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.NullLiteral;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.matching.Pattern.empty;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.extractCardinality;
import static io.trino.sql.planner.plan.JoinType.FULL;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.JoinType.RIGHT;
import static io.trino.sql.planner.plan.Patterns.CorrelatedJoin.correlation;
import static io.trino.sql.planner.plan.Patterns.correlatedJoin;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;

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
                    correlatedJoinNode.getType(),
                    correlatedJoinNode.getFilter(),
                    context.getLookup()));
        }

        checkState(
                correlatedJoinNode.getType() == RIGHT || correlatedJoinNode.getType() == FULL,
                "unexpected CorrelatedJoin type: " + correlatedJoinNode.getType());

        // handle RIGHT and FULL correlated join ON TRUE
        JoinType type;
        if (correlatedJoinNode.getType() == RIGHT) {
            type = JoinType.INNER;
        }
        else {
            type = JoinType.LEFT;
        }
        JoinNode joinNode = rewriteToJoin(correlatedJoinNode, type, TRUE_LITERAL, context.getLookup());

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
                Type inputType = context.getSymbolAllocator().getTypes().get(inputSymbol);
                assignments.put(inputSymbol, new IfExpression(correlatedJoinNode.getFilter(), inputSymbol.toSymbolReference(), new Cast(new NullLiteral(), toSqlType(inputType))));
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

    private JoinNode rewriteToJoin(CorrelatedJoinNode parent, JoinType type, Expression filter, Lookup lookup)
    {
        if (type == JoinType.LEFT && extractCardinality(parent.getSubquery(), lookup).isAtLeastScalar() && filter.equals(TRUE_LITERAL)) {
            // input rows will always be matched against subquery rows
            type = JoinType.INNER;
        }
        return new JoinNode(
                parent.getId(),
                type,
                parent.getInput(),
                parent.getSubquery(),
                ImmutableList.of(),
                parent.getInput().getOutputSymbols(),
                parent.getSubquery().getOutputSymbols(),
                false,
                filter.equals(TRUE_LITERAL) ? Optional.empty() : Optional.of(filter),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());
    }
}
