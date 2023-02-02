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
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.InPredicate;

import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.matching.Pattern.empty;
import static io.trino.sql.planner.plan.Patterns.Apply.correlation;
import static io.trino.sql.planner.plan.Patterns.applyNode;

/**
 * This optimizers looks for InPredicate expressions in ApplyNodes and replaces the nodes with SemiJoin nodes.
 * <p/>
 * Plan before optimizer:
 * <pre>
 * Filter(a IN b):
 *   Apply
 *     - correlation: []  // empty
 *     - input: some plan A producing symbol a
 *     - subquery: some plan B producing symbol b
 * </pre>
 * <p/>
 * Plan after optimizer:
 * <pre>
 * Filter(semijoinresult):
 *   SemiJoin
 *     - source: plan A
 *     - filteringSource: symbol a
 *     - sourceJoinSymbol: plan B
 *     - filteringSourceJoinSymbol: symbol b
 *     - semiJoinOutput: semijoinresult
 * </pre>
 */
public class TransformUncorrelatedInPredicateSubqueryToSemiJoin
        implements Rule<ApplyNode>
{
    private static final Pattern<ApplyNode> PATTERN = applyNode()
            .with(empty(correlation()));

    @Override
    public Pattern<ApplyNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ApplyNode applyNode, Captures captures, Context context)
    {
        if (applyNode.getSubqueryAssignments().size() != 1) {
            return Result.empty();
        }

        Expression expression = getOnlyElement(applyNode.getSubqueryAssignments().getExpressions());
        if (!(expression instanceof InPredicate inPredicate)) {
            return Result.empty();
        }

        Symbol semiJoinSymbol = getOnlyElement(applyNode.getSubqueryAssignments().getSymbols());

        SemiJoinNode replacement = new SemiJoinNode(context.getIdAllocator().getNextId(),
                applyNode.getInput(),
                applyNode.getSubquery(),
                Symbol.from(inPredicate.getValue()),
                Symbol.from(inPredicate.getValueList()),
                semiJoinSymbol,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        return Result.ofPlanNode(replacement);
    }
}
